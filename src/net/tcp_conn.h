// Non-blocking TCP connection bound to one event loop.
//
// Output is buffered; Send() never blocks. Owners implement backpressure by
// pausing the producing side once write_blocked() and resuming it from the
// on_drain callback. A peer's half-close is reported through on_eof while the
// connection stays open for writing, which the L4 proxy needs to forward
// half-closes faithfully.
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "net/buffer.h"
#include "net/event_loop.h"
#include "net/socket.h"

namespace hlb {

class TcpConn : public IoHandler, public std::enable_shared_from_this<TcpConn> {
 public:
  using Callback = std::function<void(TcpConn&)>;
  using ConnectCallback = std::function<void(TcpConn&, bool ok)>;

  static constexpr size_t kHighWater = 1u << 20;  // Pause the producer above this.
  static constexpr size_t kLowWater = 256u << 10;  // Resume it (on_drain) below this.

  // Wraps an accepted, already non-blocking socket.
  static std::shared_ptr<TcpConn> Adopt(EventLoop* loop, SocketHandle fd, std::string peer);
  // Starts a non-blocking connect. `done` runs exactly once, on the loop
  // thread, unless the connection is closed first.
  static std::shared_ptr<TcpConn> Connect(EventLoop* loop, const Endpoint& ep, std::chrono::milliseconds timeout,
                                          ConnectCallback done);
  ~TcpConn() override;

  void set_on_data(Callback cb) { on_data_ = std::move(cb); }
  void set_on_eof(Callback cb) { on_eof_ = std::move(cb); }
  void set_on_close(Callback cb) { on_close_ = std::move(cb); }
  void set_on_drain(Callback cb) { on_drain_ = std::move(cb); }
  // Detaches the owner: no callback fires after this, including on Close().
  void ClearCallbacks() {
    on_connect_ = nullptr;
    on_data_ = nullptr;
    on_eof_ = nullptr;
    on_close_ = nullptr;
    on_drain_ = nullptr;
  }

  void StartReading();
  void PauseReading();
  void ResumeReading();

  void Send(std::string_view data);
  // Half-close: sends FIN once all buffered output has been written.
  void ShutdownWrite();
  // Immediate close; buffered output is discarded. Fires on_close once.
  void Close();
  // Graceful close: detaches all callbacks, writes out buffered data, then
  // closes. The connection keeps itself alive until then, so the owner may
  // drop its reference immediately.
  void CloseAfterFlush(std::chrono::milliseconds linger = std::chrono::seconds(30));

  Buffer& input() { return input_; }
  size_t pending_output() const { return output_.size(); }
  bool write_blocked() const { return output_.size() >= kHighWater; }
  bool closed() const { return closed_; }
  bool eof() const { return eof_; }
  bool write_shutdown() const { return shutdown_requested_; }
  bool output_flushed() const { return output_.empty(); }
  const std::string& peer() const { return peer_; }
  EventLoop* loop() const { return loop_; }

  void OnEvents(uint32_t events) override;

 private:
  TcpConn(EventLoop* loop, SocketHandle fd, std::string peer) : loop_(loop), fd_(fd), peer_(std::move(peer)) {}

  void HandleRead();
  void HandleWrite();
  void FinishConnect(bool ok);
  void UpdateInterest();

  EventLoop* loop_;
  SocketHandle fd_;
  std::string peer_;
  Buffer input_;
  Buffer output_;

  bool connecting_ = false;
  bool reading_ = false;
  bool read_paused_ = false;
  bool eof_ = false;
  bool closed_ = false;
  bool shutdown_requested_ = false;
  bool shutdown_done_ = false;
  bool above_high_water_ = false;
  bool registered_ = false;
  bool closing_after_flush_ = false;
  uint32_t interest_ = 0;
  EventLoop::TimerId connect_timer_ = 0;
  EventLoop::TimerId linger_timer_ = 0;
  std::shared_ptr<TcpConn> self_;  // Held only while closing after flush.

  ConnectCallback on_connect_;
  Callback on_data_;
  Callback on_eof_;
  Callback on_close_;
  Callback on_drain_;
};

// Accepts connections on a listening socket and hands each one to `on_accept`.
class Acceptor : public IoHandler {
 public:
  using OnAccept = std::function<void(SocketHandle fd, std::string peer_ip)>;
  Acceptor(EventLoop* loop, SocketHandle listen_fd, OnAccept on_accept);
  ~Acceptor() override;
  void Start();
  void Stop();  // Stops accepting and closes the listening socket.
  void OnEvents(uint32_t events) override;

 private:
  EventLoop* loop_;
  SocketHandle fd_;
  OnAccept on_accept_;
  bool active_ = false;
};

}  // namespace hlb
