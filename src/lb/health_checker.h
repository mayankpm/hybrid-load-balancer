// Active health checking on a background thread.
//
// Each pool with checks enabled is probed on its own interval. Probes of one
// round run in parallel, so a backend that times out does not delay the rest.
// A backend turns unhealthy after `fall` consecutive failed probes and healthy
// again after `rise` consecutive successes (hysteresis against flapping).
#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "lb/backend.h"

namespace hlb {

class HealthChecker {
 public:
  explicit HealthChecker(std::vector<BackendPool*> pools) : pools_(std::move(pools)) {}
  ~HealthChecker() { Stop(); }

  void Start();
  void Stop();

  // Probes one backend synchronously (TCP connect, or an HTTP GET expecting 2xx/3xx).
  static bool Probe(const Backend& backend, const HealthCheckConfig& cfg);

 private:
  void Loop();
  void CheckPool(BackendPool* pool);

  std::vector<BackendPool*> pools_;
  std::map<BackendPool*, int64_t> next_check_ms_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::thread thread_;
};

}  // namespace hlb
