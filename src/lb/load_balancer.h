// Top-level runtime: backend pools, worker threads, listeners, health
// checking and the admin API.
//
// Threading: N workers each run an event loop. Worker 0 also runs the
// acceptors and hands each accepted socket to a worker in round-robin order,
// after which the connection lives entirely on that worker.
//
// Admin API (optional admin listener):
//   GET    /stats                                  JSON counters and latency percentiles
//   GET    /healthz                                liveness
//   POST   /pools/<pool>/backends?address=h:p&weight=n   add a backend
//   POST   /pools/<pool>/backends/<h:p>/drain      stop new traffic, remove once idle
//   DELETE /pools/<pool>/backends/<h:p>            remove immediately
#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "lb/config.h"
#include "lb/health_checker.h"
#include "lb/worker.h"

namespace hlb {

class LoadBalancer {
 public:
  explicit LoadBalancer(Config config);
  ~LoadBalancer();
  LoadBalancer(const LoadBalancer&) = delete;
  LoadBalancer& operator=(const LoadBalancer&) = delete;

  bool Start(std::string* error);
  // Graceful: stop accepting, let in-flight requests finish (up to the
  // configured shutdown timeout), then close everything.
  void Stop();

  // Actual bound addresses (useful when binding port 0).
  Address listener_address(std::string_view name) const;
  Address admin_address() const;
  BackendPool* pool(std::string_view name);
  const ListenerStats* listener_stats(std::string_view name) const;
  int worker_count() const { return static_cast<int>(workers_.size()); }
  uint64_t upstream_reuses() const;
  // Fraction of time each worker spent handling events since start.
  std::vector<double> worker_utilization() const;

  std::string StatsJson() const;

 private:
  bool StartListener(const ListenerConfig& cfg, LocalHandler handler, std::string* error);
  void OnAccept(ListenerRuntime* listener, SocketHandle fd, std::string peer);
  std::string HandleAdmin(const HttpHead& request, const std::string& body);
  void RunOnWorker0(std::function<void()> fn);

  Config config_;
  std::map<std::string, std::unique_ptr<BackendPool>, std::less<>> pools_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::unique_ptr<ListenerRuntime>> listeners_;
  std::vector<std::unique_ptr<Acceptor>> acceptors_;  // Owned by worker 0's thread.
  std::unique_ptr<HealthChecker> health_;
  std::chrono::steady_clock::time_point started_at_;
  bool running_ = false;
};

}  // namespace hlb
