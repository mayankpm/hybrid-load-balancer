// Backends and the pools that schedule traffic across them.
//
// Concurrency model: every worker thread picks backends from the same pool.
//  * Membership is an immutable, copy-on-write snapshot. A std::shared_mutex
//    guards only the pointer to the current snapshot: a pick holds the shared
//    lock just long enough to copy that pointer, then selects with no lock
//    held. Writers (add/remove/sweep) build the next snapshot outside the
//    lock and take the exclusive lock only to swap it in. Keeping the read
//    side this short matters: glibc's shared_mutex prefers readers, and when
//    picks held it for the whole selection, busy workers starved admin
//    updates indefinitely (caught by balancer_test under load on Linux).
//  * Per-backend state that changes on every request (in-flight count,
//    health, draining, ejection deadline, counters) is std::atomic, so picks
//    never block on it and snapshots can be shared.
//  * Smooth weighted round-robin mutates per-backend "current weight" state,
//    so it alone serializes its short selection step with a std::mutex.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "lb/metrics.h"
#include "net/socket.h"

namespace hlb {

enum class Algorithm { kRoundRobin, kLeastConnections, kWeightedRoundRobin };
bool ParseAlgorithm(std::string_view name, Algorithm* out);
const char* AlgorithmName(Algorithm a);

struct HealthCheckConfig {
  enum class Type { kNone, kTcp, kHttp };
  Type type = Type::kNone;
  std::string path = "/health";
  int interval_ms = 1000;
  int timeout_ms = 500;
  int rise = 2;  // Consecutive successes to mark a backend healthy.
  int fall = 2;  // Consecutive failures to mark it unhealthy.
};

// Passive checking: real traffic failures eject a backend for a while, which
// catches a dead backend faster than the next active check would.
struct PassiveConfig {
  int max_fails = 3;
  int eject_ms = 10000;
};

int64_t NowMs();

class Backend {
 public:
  Backend(uint64_t id, Address address, Endpoint endpoint, int weight)
      : id_(id), address_(std::move(address)), endpoint_(endpoint), weight_(weight < 1 ? 1 : weight) {}

  uint64_t id() const { return id_; }
  const Address& address() const { return address_; }
  const Endpoint& endpoint() const { return endpoint_; }
  int weight() const { return weight_; }

  bool Available(int64_t now_ms) const {
    return healthy.load(std::memory_order_relaxed) && !draining.load(std::memory_order_relaxed) &&
           now_ms >= ejected_until_ms.load(std::memory_order_relaxed);
  }

  void RecordSuccess() { consecutive_failures.store(0, std::memory_order_relaxed); }
  // Returns true if this failure ejected the backend.
  bool RecordFailure(const PassiveConfig& cfg, int64_t now_ms);

  // Health verdict from active checks (true until a check says otherwise).
  std::atomic<bool> healthy{true};
  // Draining backends take no new traffic and leave the pool once idle.
  std::atomic<bool> draining{false};
  std::atomic<int64_t> ejected_until_ms{0};
  // In-flight requests (L7) or connections (L4); drives least-connections.
  std::atomic<int64_t> active{0};
  std::atomic<int> consecutive_failures{0};

  std::atomic<uint64_t> requests{0};
  std::atomic<uint64_t> failures{0};
  std::atomic<uint64_t> connections{0};
  std::atomic<uint64_t> ejections{0};
  std::atomic<uint64_t> bytes_to_backend{0};
  std::atomic<uint64_t> bytes_from_backend{0};
  LatencyHistogram latency;

  // Used only by the health checker thread.
  int hc_successes = 0;
  int hc_failures = 0;
  // Used only under the pool's weighted round-robin mutex.
  int64_t wrr_current = 0;

 private:
  const uint64_t id_;
  const Address address_;
  const Endpoint endpoint_;
  const int weight_;
};

class BackendPool {
 public:
  BackendPool(std::string name, Algorithm algorithm, HealthCheckConfig health, PassiveConfig passive)
      : name_(std::move(name)), algorithm_(algorithm), health_(std::move(health)), passive_(passive) {}

  const std::string& name() const { return name_; }
  Algorithm algorithm() const { return algorithm_; }
  const HealthCheckConfig& health_config() const { return health_; }
  const PassiveConfig& passive_config() const { return passive_; }
  // Idle keep-alive connections kept per backend by each worker thread.
  size_t max_idle_per_backend() const { return max_idle_; }
  void set_max_idle_per_backend(size_t n) { max_idle_ = n; }

  // Chooses a backend for new traffic, skipping unavailable backends and any
  // id in `exclude` (backends already tried for this request). Thread-safe.
  std::shared_ptr<Backend> Pick(const std::vector<uint64_t>& exclude = {});

  bool AddBackend(const Address& address, int weight, std::string* error);
  // Takes the backend out immediately; in-flight traffic still completes
  // because sessions hold their own reference.
  bool RemoveBackend(const Address& address);
  // Stops new traffic; the backend leaves the pool once its in-flight count is zero.
  bool DrainBackend(const Address& address);
  // Removes drained backends that have gone idle. Returns how many were removed.
  int SweepDrained();

  std::vector<std::shared_ptr<Backend>> Snapshot() const;
  size_t size() const;

 private:
  using BackendList = std::vector<std::shared_ptr<Backend>>;

  std::shared_ptr<const BackendList> Load() const;
  void Publish(std::shared_ptr<const BackendList> next);

  std::shared_ptr<Backend> PickRoundRobin(const BackendList& list, const std::vector<uint64_t>& exclude, int64_t now);
  std::shared_ptr<Backend> PickLeastConnections(const BackendList& list, const std::vector<uint64_t>& exclude,
                                                int64_t now);
  std::shared_ptr<Backend> PickWeighted(const BackendList& list, const std::vector<uint64_t>& exclude, int64_t now);

  const std::string name_;
  const Algorithm algorithm_;
  const HealthCheckConfig health_;
  const PassiveConfig passive_;
  size_t max_idle_ = 32;

  mutable std::shared_mutex mu_;  // Guards only the snapshot pointer swap.
  std::shared_ptr<const BackendList> backends_{new BackendList()};  // Guarded by mu_.
  std::mutex write_mu_;  // Serializes writers (copy, modify, publish).
  std::atomic<uint64_t> cursor_{0};
  std::mutex wrr_mu_;
};

}  // namespace hlb
