// Layer 4 proxy: splices a client TCP connection to a backend.
//
//  * Failover: if connecting to the chosen backend fails, the failure is
//    recorded (passive health) and the next backend is tried, up to
//    1 + retries attempts. Client bytes that arrive meanwhile are buffered.
//  * Backpressure: when one side's output buffer passes the high-water mark
//    the other side stops reading until it drains, so a slow reader cannot
//    make the proxy buffer unbounded data.
//  * Half-close: a FIN from either side is forwarded as a FIN to the other
//    while the reverse direction keeps flowing; the session ends when both
//    directions are finished.
#include <chrono>
#include <vector>

#include "common/log.h"
#include "lb/worker.h"

namespace hlb {
namespace {

using Clock = std::chrono::steady_clock;

class TcpSession : public Session, public std::enable_shared_from_this<TcpSession> {
 public:
  TcpSession(Worker* worker, ListenerRuntime* listener, std::shared_ptr<TcpConn> client)
      : worker_(worker), listener_(listener), pool_(listener->default_pool), client_(std::move(client)) {}

  void Start() {
    listener_->stats.active++;
    client_->set_on_data([this](TcpConn&) { OnClientData(); });
    client_->set_on_eof([this](TcpConn&) { OnEof(/*from_client=*/true); });
    client_->set_on_close([this](TcpConn&) { Finish(); });
    client_->set_on_drain([this](TcpConn&) {
      // Client caught up: resume reading from the backend.
      if (upstream_ && !finished_) upstream_->ResumeReading();
    });
    client_->StartReading();
    last_activity_ = Clock::now();
    ArmIdleCheck();
    ConnectNext();
  }

  void BeginDrain() override {
    // TCP streams have no request boundary to stop at; let them run until
    // they end or the shutdown timeout aborts them.
  }

  void Abort() override { Finish(); }

 private:
  void ConnectNext() {
    backend_ = pool_->Pick(tried_);
    if (!backend_) {
      LOG_WARN("tcp %s: no available backend for client %s", listener_->config.name.c_str(), client_->peer().c_str());
      listener_->stats.upstream_errors++;
      Finish();
      return;
    }
    tried_.push_back(backend_->id());
    backend_->active++;
    counted_active_ = true;
    upstream_ = TcpConn::Connect(&worker_->loop(), backend_->endpoint(),
                                 std::chrono::milliseconds(listener_->config.connect_timeout_ms),
                                 [this](TcpConn&, bool ok) { OnConnected(ok); });
  }

  void OnConnected(bool ok) {
    if (finished_) return;
    if (!ok) {
      ReleaseBackend();
      if (backend_->RecordFailure(pool_->passive_config(), NowMs())) {
        LOG_WARN("pool %s: backend %s ejected after repeated connect failures", pool_->name().c_str(),
                 backend_->address().ToString().c_str());
      }
      upstream_.reset();
      if (static_cast<int>(tried_.size()) <= listener_->config.retries) {
        listener_->stats.retries++;
        ConnectNext();
      } else {
        listener_->stats.upstream_errors++;
        Finish();
      }
      return;
    }
    backend_->RecordSuccess();
    backend_->connections++;
    connected_ = true;
    upstream_->set_on_data([this](TcpConn&) { Pump(*upstream_, *client_, /*to_client=*/true); });
    upstream_->set_on_eof([this](TcpConn&) { OnEof(/*from_client=*/false); });
    upstream_->set_on_close([this](TcpConn&) { Finish(); });
    upstream_->set_on_drain([this](TcpConn&) {
      if (!finished_) {
        client_->ResumeReading();
        if (!client_->input().empty()) Pump(*client_, *upstream_, false);
      }
    });
    upstream_->StartReading();
    // Forward anything the client sent while we were connecting.
    if (!client_->input().empty()) Pump(*client_, *upstream_, /*to_client=*/false);
    if (client_eof_) upstream_->ShutdownWrite();
  }

  void OnClientData() {
    if (connected_) {
      Pump(*client_, *upstream_, /*to_client=*/false);
    } else if (client_->input().size() > TcpConn::kHighWater) {
      client_->PauseReading();  // Bound what we buffer before a backend is ready.
    }
  }

  void Pump(TcpConn& from, TcpConn& to, bool to_client) {
    if (finished_ || from.input().empty()) return;
    const size_t n = from.input().size();
    to.Send(from.input().view());
    from.input().Clear();
    last_activity_ = Clock::now();
    if (to_client) {
      backend_->bytes_from_backend += n;
      listener_->stats.bytes_out += n;
    } else {
      backend_->bytes_to_backend += n;
      listener_->stats.bytes_in += n;
    }
    if (to.write_blocked()) from.PauseReading();
  }

  void OnEof(bool from_client) {
    if (finished_) return;
    if (from_client) {
      client_eof_ = true;
      if (connected_) upstream_->ShutdownWrite();  // Forward the half-close.
    } else {
      upstream_eof_ = true;
      client_->ShutdownWrite();
    }
    if (client_eof_ && upstream_eof_) Finish();
  }

  void ArmIdleCheck() {
    const int idle_ms = listener_->config.idle_timeout_ms;
    const int period = std::max(50, idle_ms / 4);
    std::weak_ptr<TcpSession> weak = shared_from_this();
    idle_timer_ = worker_->loop().RunAfter(std::chrono::milliseconds(period), [weak, idle_ms] {
      auto self = weak.lock();
      if (!self || self->finished_) return;
      self->idle_timer_ = 0;
      if (Clock::now() - self->last_activity_ > std::chrono::milliseconds(idle_ms)) {
        LOG_DEBUG("tcp session idle for %d ms, closing", idle_ms);
        self->Finish();
      } else {
        self->ArmIdleCheck();
      }
    });
  }

  void ReleaseBackend() {
    if (counted_active_) {
      backend_->active--;
      counted_active_ = false;
    }
  }

  void Finish() {
    if (finished_) return;
    finished_ = true;
    if (idle_timer_ != 0) worker_->loop().Cancel(idle_timer_);
    if (backend_) ReleaseBackend();
    // Flush whatever is already buffered in each direction, then close.
    if (upstream_) upstream_->CloseAfterFlush();
    client_->CloseAfterFlush();
    listener_->stats.active--;
    worker_->RemoveSession(this);
  }

  Worker* worker_;
  ListenerRuntime* listener_;
  BackendPool* pool_;
  std::shared_ptr<TcpConn> client_;
  std::shared_ptr<TcpConn> upstream_;
  std::shared_ptr<Backend> backend_;
  std::vector<uint64_t> tried_;
  bool connected_ = false;
  bool counted_active_ = false;
  bool client_eof_ = false;
  bool upstream_eof_ = false;
  bool finished_ = false;
  Clock::time_point last_activity_;
  EventLoop::TimerId idle_timer_ = 0;
};

}  // namespace

void StartTcpSession(Worker* worker, ListenerRuntime* listener, SocketHandle fd, std::string peer) {
  auto client = TcpConn::Adopt(&worker->loop(), fd, std::move(peer));
  auto session = std::make_shared<TcpSession>(worker, listener, std::move(client));
  worker->AddSession(session);
  session->Start();
}

}  // namespace hlb
