// Per-thread runtime: each worker owns an event loop, the proxy sessions
// running on it, and its own pool of idle keep-alive backend connections.
// Nothing here is shared between workers except the BackendPools and the
// atomic statistics, so the data path takes no locks of its own.
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "http/http.h"
#include "lb/backend.h"
#include "lb/config.h"
#include "lb/metrics.h"
#include "net/event_loop.h"
#include "net/tcp_conn.h"

namespace hlb {

struct ListenerStats {
  std::atomic<uint64_t> accepted{0};
  std::atomic<int64_t> active{0};
  std::atomic<uint64_t> requests{0};
  std::atomic<uint64_t> responses_2xx{0};
  std::atomic<uint64_t> responses_3xx{0};
  std::atomic<uint64_t> responses_4xx{0};
  std::atomic<uint64_t> responses_5xx{0};
  std::atomic<uint64_t> retries{0};
  std::atomic<uint64_t> upstream_errors{0};
  std::atomic<uint64_t> bytes_in{0};
  std::atomic<uint64_t> bytes_out{0};
  LatencyHistogram latency;

  void CountStatus(int status);
};

// Admin endpoints are served by the same HTTP session code; this callback
// turns a request into a complete serialized response.
using LocalHandler = std::function<std::string(const HttpHead& request, const std::string& body)>;

struct ListenerRuntime {
  ListenerConfig config;
  Address bound;
  SocketHandle fd = kInvalidSocket;
  BackendPool* default_pool = nullptr;  // TCP listeners, and HTTP fallback.
  std::unique_ptr<Router> router;
  std::unordered_map<std::string, BackendPool*> pools;  // Route targets by name.
  LocalHandler local_handler;  // Set for the admin listener.
  ListenerStats stats;
  std::atomic<uint64_t> next_worker{0};
};

// Idle keep-alive connections to backends, owned by one worker thread.
// Reusing a connection skips the TCP handshake on the next request.
class UpstreamPool {
 public:
  explicit UpstreamPool(EventLoop* loop) : loop_(loop) {}
  ~UpstreamPool() { Clear(); }

  // A live idle connection to `backend_id`, or nullptr.
  std::shared_ptr<TcpConn> Acquire(uint64_t backend_id);
  // Parks a connection after a clean exchange. It is dropped if the backend
  // closes it, sends unsolicited data, or it stays idle past `idle_ms`.
  void Release(uint64_t backend_id, std::shared_ptr<TcpConn> conn, size_t max_idle, int idle_ms);
  void Clear();

  uint64_t reused() const { return reused_.load(std::memory_order_relaxed); }
  size_t idle_count() const;

 private:
  struct Idle {
    std::shared_ptr<TcpConn> conn;
    EventLoop::TimerId timer;
  };
  void Drop(uint64_t backend_id, TcpConn* conn);

  EventLoop* loop_;
  std::unordered_map<uint64_t, std::deque<Idle>> idle_;
  std::atomic<uint64_t> reused_{0};  // Read by the stats endpoint from other threads.
};

class Worker;

// A proxied client connection (L4 or L7) living on one worker.
class Session {
 public:
  virtual ~Session() = default;
  // Graceful shutdown: finish the current exchange, then close.
  virtual void BeginDrain() = 0;
  // Immediate teardown.
  virtual void Abort() = 0;
};

class Worker {
 public:
  Worker(int index, bool force_poll) : index_(index), loop_(force_poll), upstreams_(&loop_) {}
  ~Worker();

  void Start();
  void Stop();  // Stops the loop and joins the thread.

  int index() const { return index_; }
  EventLoop& loop() { return loop_; }
  UpstreamPool& upstreams() { return upstreams_; }

  // Loop thread only.
  void AddSession(std::shared_ptr<Session> s) { sessions_.emplace(s.get(), std::move(s)); }
  void RemoveSession(Session* s);
  void DrainSessions();
  void AbortSessions();
  size_t session_count() const { return sessions_.size(); }

 private:
  int index_;
  // Declaration order matters for teardown: sessions and pooled connections
  // unregister from the loop, so they must be destroyed before it.
  EventLoop loop_;
  UpstreamPool upstreams_;
  std::unordered_map<Session*, std::shared_ptr<Session>> sessions_;
  std::thread thread_;
};

// Starts proxying an accepted connection on `worker` (loop thread only).
void StartTcpSession(Worker* worker, ListenerRuntime* listener, SocketHandle fd, std::string peer);
void StartHttpSession(Worker* worker, ListenerRuntime* listener, SocketHandle fd, std::string peer);

}  // namespace hlb
