#include "net/tcp_conn.h"

namespace hlb {

std::shared_ptr<TcpConn> TcpConn::Adopt(EventLoop* loop, SocketHandle fd, std::string peer) {
  return std::shared_ptr<TcpConn>(new TcpConn(loop, fd, std::move(peer)));
}

std::shared_ptr<TcpConn> TcpConn::Connect(EventLoop* loop, const Endpoint& ep, std::chrono::milliseconds timeout,
                                          ConnectCallback done) {
  bool in_progress = false;
  const SocketHandle fd = ConnectTcpAsync(ep, &in_progress);
  std::shared_ptr<TcpConn> c(new TcpConn(loop, fd, ""));
  c->on_connect_ = std::move(done);
  c->connecting_ = true;
  if (fd == kInvalidSocket || !in_progress) {
    // Report asynchronously so callers never see the callback re-entrantly.
    const bool ok = fd != kInvalidSocket;
    std::weak_ptr<TcpConn> weak = c;
    loop->Post([weak, ok] {
      if (auto self = weak.lock()) self->FinishConnect(ok);
    });
    return c;
  }
  std::weak_ptr<TcpConn> weak = c;
  c->connect_timer_ = loop->RunAfter(timeout, [weak] {
    if (auto self = weak.lock()) {
      self->connect_timer_ = 0;
      self->FinishConnect(false);
    }
  });
  c->UpdateInterest();
  return c;
}

TcpConn::~TcpConn() {
  if (!closed_ && fd_ != kInvalidSocket) {
    if (registered_) loop_->Unwatch(fd_);
    CloseSocket(fd_);
  }
}

void TcpConn::FinishConnect(bool ok) {
  if (closed_ || !connecting_) return;
  connecting_ = false;
  if (connect_timer_ != 0) {
    loop_->Cancel(connect_timer_);
    connect_timer_ = 0;
  }
  ConnectCallback cb = std::move(on_connect_);
  on_connect_ = nullptr;
  if (!ok) {
    if (cb) cb(*this, false);
    Close();
    return;
  }
  UpdateInterest();
  if (cb) cb(*this, true);
}

void TcpConn::StartReading() {
  reading_ = true;
  UpdateInterest();
}

void TcpConn::PauseReading() {
  if (read_paused_) return;
  read_paused_ = true;
  UpdateInterest();
}

void TcpConn::ResumeReading() {
  if (!read_paused_) return;
  read_paused_ = false;
  UpdateInterest();
}

void TcpConn::CloseAfterFlush(std::chrono::milliseconds linger) {
  if (closed_) return;
  on_data_ = nullptr;
  on_eof_ = nullptr;
  on_drain_ = nullptr;
  on_close_ = nullptr;
  reading_ = false;
  if (output_.empty() || connecting_) {
    Close();
    return;
  }
  // Stay alive on our own until the output is written, then close. The timer
  // bounds how long a peer that stops reading can hold the socket.
  closing_after_flush_ = true;
  self_ = shared_from_this();
  std::weak_ptr<TcpConn> weak = self_;
  linger_timer_ = loop_->RunAfter(linger, [weak] {
    if (auto self = weak.lock()) {
      self->linger_timer_ = 0;
      self->Close();
    }
  });
  UpdateInterest();
}

void TcpConn::UpdateInterest() {
  if (closed_ || fd_ == kInvalidSocket) return;
  uint32_t want = 0;
  if (connecting_) {
    want = kEvWrite;
  } else {
    if (reading_ && !read_paused_ && !eof_) want |= kEvRead;
    if (!output_.empty()) want |= kEvWrite;
  }
  // Once the peer has finished sending and we have nothing to write, stop
  // polling entirely: some platforms keep reporting the hang-up (level
  // triggered) and would otherwise spin.
  if (want == 0 && (eof_ || !reading_)) {
    if (registered_) {
      loop_->Unwatch(fd_);
      registered_ = false;
    }
    interest_ = 0;
    return;
  }
  if (!registered_ || want != interest_) {
    loop_->Watch(fd_, this, want);
    registered_ = true;
    interest_ = want;
  }
}

void TcpConn::OnEvents(uint32_t events) {
  if (closed_) return;
  auto self = shared_from_this();  // Callbacks may drop the owner's reference.
  if (connecting_) {
    if (events & (kEvWrite | kEvError)) FinishConnect(SocketError(fd_) == 0 && (events & kEvWrite));
    return;
  }
  if (events & kEvError) {
    if (SocketError(fd_) != 0) {
      Close();  // Reset or other hard error.
      return;
    }
    // A hang-up without an error is the peer's FIN on some platforms (WSAPoll
    // reports half-close as POLLHUP): read to EOF like a normal readable event.
    if (!eof_) HandleRead();
  }
  if (!closed_ && (events & kEvRead) && !read_paused_) HandleRead();
  if (!closed_ && (events & kEvWrite)) HandleWrite();
}

void TcpConn::HandleRead() {
  char buf[64 << 10];
  size_t total = 0;
  bool got_eof = false;
  // Bound the work per event so one busy connection cannot starve the loop.
  while (total < (256u << 10)) {
    const long long n = ReadSome(fd_, buf, sizeof(buf));
    if (n > 0) {
      input_.Append(std::string_view(buf, static_cast<size_t>(n)));
      total += static_cast<size_t>(n);
      // A short read means the socket is drained. With level-triggered
      // polling, anything that arrives later is reported again, so skip the
      // extra recv() that would only return EWOULDBLOCK.
      if (static_cast<size_t>(n) < sizeof(buf)) break;
      continue;
    }
    if (n == 0) got_eof = true;
    if (n == kIoError) {
      if (total > 0 && on_data_) on_data_(*this);
      Close();
      return;
    }
    break;
  }
  if (total > 0 && on_data_) on_data_(*this);
  if (closed_) return;
  if (got_eof && !eof_) {
    eof_ = true;
    UpdateInterest();
    if (on_eof_) on_eof_(*this);
  }
}

void TcpConn::HandleWrite() {
  while (!output_.empty()) {
    const long long n = WriteSome(fd_, output_.data(), output_.size());
    if (n > 0) {
      output_.Consume(static_cast<size_t>(n));
      continue;
    }
    if (n == kIoError) {
      Close();
      return;
    }
    break;  // Would block.
  }
  if (output_.empty() && shutdown_requested_ && !shutdown_done_) {
    shutdown_done_ = true;
    hlb::ShutdownWrite(fd_);
  }
  if (output_.empty() && closing_after_flush_) {
    Close();
    return;
  }
  UpdateInterest();
  if (above_high_water_ && output_.size() < kLowWater) {
    above_high_water_ = false;
    if (on_drain_) on_drain_(*this);
  }
}

void TcpConn::Send(std::string_view data) {
  if (closed_ || shutdown_requested_ || data.empty()) return;
  if (output_.empty() && !connecting_) {
    // Fast path: write straight to the socket, buffering only the remainder.
    while (!data.empty()) {
      const long long n = WriteSome(fd_, data.data(), data.size());
      if (n > 0) {
        data.remove_prefix(static_cast<size_t>(n));
        continue;
      }
      if (n == kIoError) {
        Close();
        return;
      }
      break;
    }
  }
  if (!data.empty()) {
    output_.Append(data);
    if (output_.size() >= kHighWater) above_high_water_ = true;
    UpdateInterest();
  }
}

void TcpConn::ShutdownWrite() {
  if (closed_ || shutdown_requested_) return;
  shutdown_requested_ = true;
  if (output_.empty() && !connecting_) {
    shutdown_done_ = true;
    hlb::ShutdownWrite(fd_);
  }
}

void TcpConn::Close() {
  if (closed_) return;
  closed_ = true;
  if (connect_timer_ != 0) {
    loop_->Cancel(connect_timer_);
    connect_timer_ = 0;
  }
  if (linger_timer_ != 0) {
    loop_->Cancel(linger_timer_);
    linger_timer_ = 0;
  }
  if (fd_ != kInvalidSocket) {
    if (registered_) loop_->Unwatch(fd_);
    registered_ = false;
    CloseSocket(fd_);
  }
  loop_->DeferRelease(shared_from_this());
  self_.reset();
  Callback on_close = std::move(on_close_);
  // Drop every callback so captured owners are released and nothing fires again.
  on_connect_ = nullptr;
  on_data_ = nullptr;
  on_eof_ = nullptr;
  on_close_ = nullptr;
  on_drain_ = nullptr;
  if (on_close) on_close(*this);
}

// ---------------------------------------------------------------------------

Acceptor::Acceptor(EventLoop* loop, SocketHandle listen_fd, OnAccept on_accept)
    : loop_(loop), fd_(listen_fd), on_accept_(std::move(on_accept)) {}

Acceptor::~Acceptor() { Stop(); }

void Acceptor::Start() {
  active_ = true;
  loop_->Watch(fd_, this, kEvRead);
}

void Acceptor::Stop() {
  if (fd_ == kInvalidSocket) return;
  if (active_) loop_->Unwatch(fd_);
  active_ = false;
  CloseSocket(fd_);
  fd_ = kInvalidSocket;
}

void Acceptor::OnEvents(uint32_t) {
  // Accept a bounded batch so a connection flood cannot monopolize the loop.
  for (int i = 0; i < 128 && fd_ != kInvalidSocket; i++) {
    std::string peer;
    const SocketHandle s = AcceptTcp(fd_, &peer);
    if (s == kInvalidSocket) return;
    on_accept_(s, std::move(peer));
  }
}

}  // namespace hlb
