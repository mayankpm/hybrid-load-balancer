// Fault injection: backends fail, recover, are drained or removed while
// clients keep sending traffic. The bar is zero client-visible errors.
#include <atomic>
#include <map>
#include <mutex>
#include <thread>

#include "harness.h"

using namespace hlb;
using namespace hlb::testkit;
using namespace lbtest;

namespace {

// Keeps `threads` keep-alive clients sending GETs until stopped, recording
// which backend served each request and every failure.
class Load {
 public:
  Load(Address addr, int threads) {
    for (int t = 0; t < threads; t++) {
      threads_.emplace_back([this, addr] {
        HttpClient c(addr, 10000);
        while (!stop_) {
          auto r = c.Get("/x");
          std::lock_guard lock(mu_);
          if (!r.ok || r.status != 200) {
            errors_++;
            last_error_ = r.ok ? "status " + std::to_string(r.status) : r.error;
          } else {
            served_[*r.Header("x-backend")]++;
            ok_++;
          }
        }
      });
    }
  }
  ~Load() { Stop(); }
  void Stop() {
    stop_ = true;
    for (auto& t : threads_) {
      if (t.joinable()) t.join();
    }
  }
  uint64_t served_by(const std::string& id) {
    std::lock_guard lock(mu_);
    return served_[id];
  }
  uint64_t ok() {
    std::lock_guard lock(mu_);
    return ok_;
  }
  uint64_t errors() {
    std::lock_guard lock(mu_);
    return errors_;
  }
  std::string last_error() {
    std::lock_guard lock(mu_);
    return last_error_;
  }

 private:
  std::atomic<bool> stop_{false};
  std::vector<std::thread> threads_;
  std::mutex mu_;
  std::map<std::string, uint64_t> served_;
  uint64_t ok_ = 0;
  uint64_t errors_ = 0;
  std::string last_error_;
};

std::shared_ptr<Backend> Find(LoadBalancer& lb, const std::string& pool, const Address& a) {
  for (auto& b : lb.pool(pool)->Snapshot()) {
    if (b->address() == a) return b;
  }
  return nullptr;
}

const char* kCheckedConfig =
    "threads 2\n"
    "pool web algorithm=round_robin health=http health_path=/health interval_ms=100 timeout_ms=300 rise=2 fall=2\n"
    "listener http mode=http bind=127.0.0.1:0 pool=web retries=2 connect_timeout_ms=300\n";

}  // namespace

TEST(failover, active_health_checks_mark_backends_down_and_up) {
  auto backends = StartHttpBackends(3);
  auto lb = StartLb(MakeConfig(kCheckedConfig, "web", backends));
  auto b1 = Find(*lb, "web", backends[1]->address());

  backends[1]->SetHealthy(false);
  CHECK(WaitFor([&] { return !b1->healthy.load(); }, 3000));
  HttpClient client(lb->listener_address("http"));
  for (int i = 0; i < 30; i++) {
    auto r = client.Get("/x");
    CHECK_EQ(r.status, 200);
    CHECK(*r.Header("x-backend") != "b1");
  }

  backends[1]->SetHealthy(true);
  CHECK(WaitFor([&] { return b1->healthy.load(); }, 3000));
  int to_b1 = 0;
  for (int i = 0; i < 30; i++) to_b1 += *client.Get("/x").Header("x-backend") == "b1" ? 1 : 0;
  CHECK_EQ(to_b1, 10);
}

TEST(failover, killing_a_backend_under_load_causes_no_client_errors) {
  auto backends = StartHttpBackends(3);
  auto lb = StartLb(MakeConfig(kCheckedConfig, "web", backends));
  Load load(lb->listener_address("http"), 8);
  SleepMs(500);

  backends[1]->Kill();  // Crash mid-traffic: in-flight and pooled connections are reset.
  SleepMs(1000);
  const uint64_t b1_during = load.served_by("b1");
  SleepMs(300);
  CHECK_EQ(load.served_by("b1"), b1_during);  // Out of rotation.

  CHECK(backends[1]->Restart());
  auto b1 = Find(*lb, "web", backends[1]->address());
  CHECK(WaitFor([&] { return b1->healthy.load() && load.served_by("b1") > b1_during + 50; }, 5000));
  load.Stop();

  if (load.errors() != 0) throw Failure("client errors: " + std::to_string(load.errors()) + ", last: " + load.last_error());
  CHECK(load.ok() > 1000);
  CHECK(lb->listener_stats("http")->retries.load() > 0);  // Failover really happened.
}

TEST(failover, draining_a_backend_under_load_causes_no_client_errors) {
  auto backends = StartHttpBackends(2);
  auto cfg = MakeConfig(kCheckedConfig, "web", backends);
  cfg.admin = Address{"127.0.0.1", 0};
  auto lb = StartLb(std::move(cfg));
  Load load(lb->listener_address("http"), 8);
  SleepMs(300);

  HttpClient admin(lb->admin_address());
  CHECK_EQ(admin.Request("POST", "/pools/web/backends/" + backends[0]->address().ToString() + "/drain").status, 202);
  // Draining backends leave the pool once their in-flight requests finish.
  CHECK(WaitFor([&] { return lb->pool("web")->size() == 1; }, 3000));
  const uint64_t b0_after = load.served_by("b0");
  SleepMs(300);
  CHECK_EQ(load.served_by("b0"), b0_after);
  load.Stop();
  CHECK_EQ(load.errors(), 0u);
}

TEST(failover, passive_ejection_without_active_checks) {
  auto backends = StartHttpBackends(3);
  auto lb = StartLb(MakeConfig(
      "threads 2\n"
      "pool web algorithm=round_robin max_fails=2 eject_ms=700\n"
      "listener http mode=http bind=127.0.0.1:0 pool=web retries=2 connect_timeout_ms=300\n",
      "web", backends));
  backends[2]->Kill();
  HttpClient client(lb->listener_address("http"));
  for (int i = 0; i < 30; i++) CHECK_EQ(client.Get("/x").status, 200);  // Retries hide the failures.
  auto b2 = Find(*lb, "web", backends[2]->address());
  CHECK(b2->ejections.load() >= 1);

  CHECK(backends[2]->Restart());
  SleepMs(800);  // Ejection expires; real traffic is the probe.
  int to_b2 = 0;
  for (int i = 0; i < 30; i++) to_b2 += *client.Get("/x").Header("x-backend") == "b2" ? 1 : 0;
  CHECK(to_b2 >= 5);
}

TEST(failover, all_backends_down_then_recover) {
  auto backends = StartHttpBackends(2);
  auto lb = StartLb(MakeConfig(kCheckedConfig, "web", backends));
  for (auto& b : backends) b->SetHealthy(false);
  auto pool = lb->pool("web");
  CHECK(WaitFor([&] {
    for (auto& b : pool->Snapshot()) {
      if (b->healthy) return false;
    }
    return true;
  }, 3000));
  HttpClient client(lb->listener_address("http"));
  CHECK_EQ(client.Get("/x").status, 503);
  for (auto& b : backends) b->SetHealthy(true);
  CHECK(WaitFor([&] { return client.Get("/x").status == 200; }, 3000));
}

TEST(failover, graceful_shutdown_finishes_in_flight_requests) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kCheckedConfig, "web", backends));
  const Address addr = lb->listener_address("http");
  HttpResult slow;
  std::thread t([&] {
    HttpClient c(addr);
    slow = c.Get("/delay/600");
  });
  SleepMs(150);
  lb->Stop();  // Blocks until the in-flight request completes.
  t.join();
  CHECK(slow.ok);
  CHECK_EQ(slow.status, 200);
  // No longer accepting.
  const SocketHandle s = ConnectTcpBlocking(addr, 300);
  CHECK(s == kInvalidSocket);
}
