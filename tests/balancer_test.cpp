#include <atomic>
#include <map>
#include <thread>

#include "lb/backend.h"
#include "lb/metrics.h"
#include "test.h"

using namespace hlb;

namespace {

std::unique_ptr<BackendPool> MakePool(Algorithm algo, std::vector<int> weights) {
  auto pool = std::make_unique<BackendPool>("p", algo, HealthCheckConfig{}, PassiveConfig{});
  std::string err;
  for (size_t i = 0; i < weights.size(); i++) {
    if (!pool->AddBackend(Address{"127.0.0.1", static_cast<uint16_t>(10000 + i)}, weights[i], &err)) {
      throw lbtest::Failure(err);
    }
  }
  return pool;
}

std::map<uint16_t, int> Distribution(BackendPool& pool, int picks) {
  std::map<uint16_t, int> count;
  for (int i = 0; i < picks; i++) {
    auto b = pool.Pick();
    if (!b) throw lbtest::Failure("pick returned no backend");
    count[b->address().port]++;
  }
  return count;
}

}  // namespace

TEST(balancer, round_robin_is_even) {
  auto pool = MakePool(Algorithm::kRoundRobin, {1, 1, 1});
  auto d = Distribution(*pool, 300);
  for (auto& [port, n] : d) CHECK_EQ(n, 100);
}

TEST(balancer, weighted_round_robin_is_exact_and_smooth) {
  auto pool = MakePool(Algorithm::kWeightedRoundRobin, {5, 1, 1});
  // First cycle must interleave the light backends instead of bunching the
  // heavy one (nginx smooth WRR gives a a b a c a a).
  std::string seq;
  for (int i = 0; i < 7; i++) seq += static_cast<char>('a' + pool->Pick()->address().port - 10000);
  CHECK_EQ(seq, std::string("aabacaa"));
  auto d = Distribution(*pool, 700);
  CHECK_EQ(d[10000], 500);
  CHECK_EQ(d[10001], 100);
  CHECK_EQ(d[10002], 100);
}

TEST(balancer, weighted_round_robin_redistributes_when_a_backend_is_down) {
  auto pool = MakePool(Algorithm::kWeightedRoundRobin, {3, 2, 1});
  pool->Snapshot()[0]->healthy = false;
  auto d = Distribution(*pool, 300);
  CHECK_EQ(d.count(10000), 0u);
  CHECK_EQ(d[10001], 200);  // The remaining 2:1 ratio is preserved exactly.
  CHECK_EQ(d[10002], 100);
}

TEST(balancer, least_connections_prefers_the_least_loaded) {
  auto pool = MakePool(Algorithm::kLeastConnections, {1, 1, 1});
  auto backends = pool->Snapshot();
  backends[0]->active = 5;
  backends[1]->active = 1;
  backends[2]->active = 3;
  CHECK_EQ(pool->Pick()->address().port, 10001);
  // Weighted: 4 in flight on a weight-4 backend beats 2 on a weight-1 backend.
  auto weighted = MakePool(Algorithm::kLeastConnections, {4, 1});
  weighted->Snapshot()[0]->active = 4;
  weighted->Snapshot()[1]->active = 2;
  CHECK_EQ(weighted->Pick()->address().port, 10000);
}

TEST(balancer, least_connections_spreads_ties) {
  auto pool = MakePool(Algorithm::kLeastConnections, {1, 1, 1, 1});
  auto d = Distribution(*pool, 400);  // All idle: ties must not all go to one backend.
  for (auto& [port, n] : d) CHECK_EQ(n, 100);
}

TEST(balancer, skips_unhealthy_draining_ejected_and_excluded_backends) {
  for (Algorithm a : {Algorithm::kRoundRobin, Algorithm::kLeastConnections, Algorithm::kWeightedRoundRobin}) {
    auto pool = MakePool(a, {1, 1, 1, 1});
    auto b = pool->Snapshot();
    b[0]->healthy = false;
    b[1]->draining = true;
    b[2]->ejected_until_ms = NowMs() + 60000;
    for (int i = 0; i < 20; i++) CHECK_EQ(pool->Pick()->address().port, 10003);
    // Excluding the only available backend falls back to panic mode, which
    // ignores passive ejection but never health or draining.
    CHECK_EQ(pool->Pick({b[3]->id()})->address().port, 10002);
    b[2]->ejected_until_ms = 0;
    b[3]->healthy = false;
    b[2]->healthy = false;
    CHECK(pool->Pick() == nullptr);
  }
}

TEST(balancer, passive_ejection_after_max_fails_and_recovery) {
  PassiveConfig cfg{3, 200};
  Backend b(1, Address{"127.0.0.1", 1}, Endpoint{}, 1);
  const int64_t now = NowMs();
  CHECK(!b.RecordFailure(cfg, now));
  CHECK(!b.RecordFailure(cfg, now));
  b.RecordSuccess();  // A success resets the streak.
  CHECK(!b.RecordFailure(cfg, now));
  CHECK(!b.RecordFailure(cfg, now));
  CHECK(b.RecordFailure(cfg, now));
  CHECK(!b.Available(now + 100));
  CHECK(b.Available(now + 200));
  CHECK_EQ(b.ejections.load(), 1u);
}

TEST(balancer, drain_removes_backend_once_idle) {
  auto pool = MakePool(Algorithm::kRoundRobin, {1, 1});
  auto b = pool->Snapshot()[0];
  b->active = 1;
  CHECK(pool->DrainBackend(b->address()));
  CHECK_EQ(pool->SweepDrained(), 0);  // Still busy.
  for (int i = 0; i < 10; i++) CHECK_EQ(pool->Pick()->address().port, 10001);
  b->active = 0;
  CHECK_EQ(pool->SweepDrained(), 1);
  CHECK_EQ(pool->size(), 1u);
}

TEST(balancer, concurrent_picks_during_membership_changes) {
  // Pickers on several threads while another thread adds, drains and removes
  // backends. Run under ThreadSanitizer in CI to prove the locking is sound.
  for (Algorithm a : {Algorithm::kRoundRobin, Algorithm::kLeastConnections, Algorithm::kWeightedRoundRobin}) {
    auto pool = MakePool(a, {1, 2, 3});
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> picks{0};
    std::vector<std::thread> pickers;
    for (int t = 0; t < 8; t++) {
      pickers.emplace_back([&] {
        while (!stop) {
          if (auto b = pool->Pick()) {
            b->active++;
            b->requests++;
            b->active--;
            picks++;
          }
        }
      });
    }
    std::string err;
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(300);
    for (int i = 0; std::chrono::steady_clock::now() < until; i++) {
      const Address addr{"127.0.0.1", static_cast<uint16_t>(20000 + i % 5000)};
      pool->AddBackend(addr, 1 + i % 3, &err);
      if (i % 3 == 0) pool->DrainBackend(addr);
      if (i % 2 == 0) pool->RemoveBackend(Address{"127.0.0.1", static_cast<uint16_t>(20000 + (i / 2) % 5000)});
      pool->SweepDrained();
      pool->Snapshot()[0]->healthy = (i % 5 != 0);
    }
    stop = true;
    for (auto& t : pickers) t.join();
    CHECK(picks.load() > 1000);
  }
}

TEST(balancer, latency_histogram_percentiles) {
  LatencyHistogram h;
  for (uint64_t v = 1; v <= 10000; v++) h.Record(v);
  CHECK_EQ(h.Count(), 10000u);
  auto near = [](uint64_t got, uint64_t want) { return got >= want * 85 / 100 && got <= want * 115 / 100; };
  CHECK(near(h.Percentile(0.50), 5000));
  CHECK(near(h.Percentile(0.99), 9900));
  CHECK(h.Percentile(0.0) <= 2);
}
