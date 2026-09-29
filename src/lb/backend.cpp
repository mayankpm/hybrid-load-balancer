#include "lb/backend.h"

#include <algorithm>
#include <chrono>
#include <cstdint>

#include "common/log.h"

namespace hlb {
namespace {

std::atomic<uint64_t> g_next_backend_id{1};

bool Excluded(const std::vector<uint64_t>& exclude, uint64_t id) {
  return std::find(exclude.begin(), exclude.end(), id) != exclude.end();
}

}  // namespace

int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

bool ParseAlgorithm(std::string_view name, Algorithm* out) {
  if (name == "round_robin" || name == "rr") {
    *out = Algorithm::kRoundRobin;
  } else if (name == "least_connections" || name == "least_conn") {
    *out = Algorithm::kLeastConnections;
  } else if (name == "weighted_round_robin" || name == "weighted") {
    *out = Algorithm::kWeightedRoundRobin;
  } else {
    return false;
  }
  return true;
}

const char* AlgorithmName(Algorithm a) {
  switch (a) {
    case Algorithm::kRoundRobin: return "round_robin";
    case Algorithm::kLeastConnections: return "least_connections";
    case Algorithm::kWeightedRoundRobin: return "weighted_round_robin";
  }
  return "?";
}

bool Backend::RecordFailure(const PassiveConfig& cfg, int64_t now_ms) {
  failures.fetch_add(1, std::memory_order_relaxed);
  const int n = consecutive_failures.fetch_add(1, std::memory_order_relaxed) + 1;
  if (cfg.max_fails <= 0 || n < cfg.max_fails) return false;
  // Eject; only the thread that crosses the threshold reports it.
  consecutive_failures.store(0, std::memory_order_relaxed);
  ejected_until_ms.store(now_ms + cfg.eject_ms, std::memory_order_relaxed);
  ejections.fetch_add(1, std::memory_order_relaxed);
  return true;
}

std::shared_ptr<const BackendPool::BackendList> BackendPool::Load() const {
  std::shared_lock lock(mu_);
  return backends_;
}

void BackendPool::Publish(std::shared_ptr<const BackendList> next) {
  std::unique_lock lock(mu_);
  backends_ = std::move(next);
}

std::shared_ptr<Backend> BackendPool::Pick(const std::vector<uint64_t>& exclude) {
  const auto list = Load();  // The lock is held only for this pointer copy.
  if (list->empty()) return nullptr;
  auto pick = [&](int64_t now) -> std::shared_ptr<Backend> {
    switch (algorithm_) {
      case Algorithm::kRoundRobin: return PickRoundRobin(*list, exclude, now);
      case Algorithm::kLeastConnections: return PickLeastConnections(*list, exclude, now);
      case Algorithm::kWeightedRoundRobin: return PickWeighted(*list, exclude, now);
    }
    return nullptr;
  };
  if (auto b = pick(NowMs())) return b;
  // Panic mode: if passive ejection has removed every healthy backend, keep
  // sending to the ejected ones rather than failing everything. A burst of
  // errors should not turn into a full outage.
  return pick(INT64_MAX);
}

std::shared_ptr<Backend> BackendPool::PickRoundRobin(const BackendList& list, const std::vector<uint64_t>& exclude,
                                                     int64_t now) {
  const size_t n = list.size();
  const size_t start = static_cast<size_t>(cursor_.fetch_add(1, std::memory_order_relaxed) % n);
  for (size_t i = 0; i < n; i++) {
    const auto& b = list[(start + i) % n];
    if (b->Available(now) && !Excluded(exclude, b->id())) return b;
  }
  return nullptr;
}

std::shared_ptr<Backend> BackendPool::PickLeastConnections(const BackendList& list,
                                                           const std::vector<uint64_t>& exclude, int64_t now) {
  // Weighted least-connections: minimize active/weight. Start the scan at a
  // rotating offset so ties are spread instead of always hitting backend 0.
  const size_t n = list.size();
  const size_t start = static_cast<size_t>(cursor_.fetch_add(1, std::memory_order_relaxed) % n);
  std::shared_ptr<Backend> best;
  int64_t best_active = 0;
  for (size_t i = 0; i < n; i++) {
    const auto& b = list[(start + i) % n];
    if (!b->Available(now) || Excluded(exclude, b->id())) continue;
    const int64_t active = b->active.load(std::memory_order_relaxed);
    // active / weight < best_active / best_weight, without division.
    if (!best || active * best->weight() < best_active * b->weight()) {
      best = b;
      best_active = active;
    }
  }
  return best;
}

std::shared_ptr<Backend> BackendPool::PickWeighted(const BackendList& list, const std::vector<uint64_t>& exclude,
                                                   int64_t now) {
  // Smooth weighted round-robin (the nginx algorithm): each pick adds every
  // candidate's weight to its running score, takes the highest, and subtracts
  // the total from the winner. Weights 5:1:1 give a,a,b,a,c,a,a rather than
  // a,a,a,a,a,b,c, and unavailable backends drop out without skewing the rest.
  std::lock_guard lock(wrr_mu_);
  std::shared_ptr<Backend> best;
  int64_t total = 0;
  for (const auto& b : list) {
    if (!b->Available(now) || Excluded(exclude, b->id())) continue;
    b->wrr_current += b->weight();
    total += b->weight();
    if (!best || b->wrr_current > best->wrr_current) best = b;
  }
  if (best) best->wrr_current -= total;
  return best;
}

bool BackendPool::AddBackend(const Address& address, int weight, std::string* error) {
  Endpoint ep;
  if (!Resolve(address, &ep)) {
    *error = "cannot resolve " + address.host;
    return false;
  }
  std::lock_guard writer(write_mu_);
  const auto current = Load();
  for (const auto& existing : *current) {
    if (existing->address() == address) {
      *error = "backend " + address.ToString() + " already in pool " + name_;
      return false;
    }
  }
  auto next = std::shared_ptr<BackendList>(new BackendList(*current));
  next->push_back(std::make_shared<Backend>(g_next_backend_id.fetch_add(1), address, ep, weight));
  Publish(std::move(next));
  return true;
}

bool BackendPool::RemoveBackend(const Address& address) {
  std::lock_guard writer(write_mu_);
  auto next = std::shared_ptr<BackendList>(new BackendList(*Load()));
  const auto it = std::find_if(next->begin(), next->end(), [&](const auto& b) { return b->address() == address; });
  if (it == next->end()) return false;
  next->erase(it);
  Publish(std::move(next));
  return true;
}

bool BackendPool::DrainBackend(const Address& address) {
  for (const auto& b : *Load()) {
    if (b->address() == address) {
      b->draining = true;  // Atomic flag on the backend: no new snapshot needed.
      return true;
    }
  }
  return false;
}

int BackendPool::SweepDrained() {
  std::vector<std::string> removed;
  {
    std::lock_guard writer(write_mu_);
    const auto current = Load();
    std::shared_ptr<BackendList> next(new BackendList());
    for (const auto& b : *current) {
      if (b->draining && b->active.load() == 0) {
        removed.push_back(b->address().ToString());
      } else {
        next->push_back(b);
      }
    }
    if (!removed.empty()) Publish(std::move(next));
  }
  for (const auto& r : removed) LOG_INFO("pool %s: drained backend %s removed", name_.c_str(), r.c_str());
  return static_cast<int>(removed.size());
}

std::vector<std::shared_ptr<Backend>> BackendPool::Snapshot() const { return *Load(); }

size_t BackendPool::size() const { return Load()->size(); }

}  // namespace hlb
