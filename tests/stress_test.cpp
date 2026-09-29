// Concurrency stress: many clients at once, with per-response verification
// and counter consistency checks. Run under ThreadSanitizer in CI.
#include <atomic>
#include <thread>

#include "harness.h"

using namespace hlb;
using namespace hlb::testkit;
using namespace lbtest;

TEST(stress, concurrent_keep_alive_clients_get_their_own_responses) {
  auto backends = StartHttpBackends(4);
  auto lb = StartLb(MakeConfig(
      "threads 4\npool web algorithm=least_connections\nlistener http mode=http bind=127.0.0.1:0 pool=web\n", "web",
      backends));
  const int kThreads = 32, kRequests = 150;
  std::atomic<int> mismatches{0}, failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&, t] {
      HttpClient c(lb->listener_address("http"));
      for (int i = 0; i < kRequests; i++) {
        const std::string id = std::to_string(t) + "-" + std::to_string(i);
        auto r = c.Request("POST", "/echo", "payload-" + id, {{"X-Client-Id", id}});
        if (!r.ok || r.status != 200) {
          failures++;
          continue;
        }
        // A response routed to the wrong client would carry someone else's id.
        if (r.body.find("x-client-id=" + id + "\n") == std::string::npos ||
            r.body.find("body=payload-" + id) == std::string::npos) {
          mismatches++;
        }
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(failures.load(), 0);
  CHECK_EQ(mismatches.load(), 0);
  const uint64_t total = kThreads * kRequests;
  uint64_t backend_sum = 0;
  for (auto& b : lb->pool("web")->Snapshot()) {
    backend_sum += b->requests;
    CHECK_EQ(b->active.load(), 0);  // In-flight counters return to zero.
    CHECK(b->requests.load() > total / 8);  // Least-connections used every backend.
  }
  CHECK_EQ(backend_sum, total);
  CHECK_EQ(lb->listener_stats("http")->requests.load(), total);
  CHECK_EQ(lb->listener_stats("http")->responses_2xx.load(), total);
}

TEST(stress, connection_churn) {
  auto backends = StartHttpBackends(2);
  auto lb = StartLb(MakeConfig(
      "threads 2\npool web\nlistener http mode=http bind=127.0.0.1:0 pool=web\n", "web", backends));
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; t++) {
    threads.emplace_back([&] {
      for (int i = 0; i < 100; i++) {
        HttpClient c(lb->listener_address("http"));
        auto r = c.Request("GET", "/x", "", {{"Connection", "close"}});
        if (!r.ok || r.status != 200) failures++;
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(failures.load(), 0);
  CHECK(WaitFor([&] { return lb->listener_stats("http")->active.load() == 0; }));
  CHECK_EQ(lb->listener_stats("http")->accepted.load(), 800u);
}

TEST(stress, parallel_tcp_sessions) {
  auto backends = StartEchoBackends(3);
  auto lb = StartLb(MakeConfig(
      "threads 4\npool echo algorithm=least_connections\nlistener tcp mode=tcp bind=127.0.0.1:0 pool=echo\n", "echo",
      backends));
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 32; t++) {
    threads.emplace_back([&, t] {
      const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
      if (s == kInvalidSocket) {
        failures++;
        return;
      }
      SetRecvTimeout(s, 10000);
      std::string hello;
      char c;
      while (RecvSome(s, &c, 1) == 1 && c != '\n') hello.push_back(c);
      const std::string payload(256 << 10, static_cast<char>('a' + t % 26));
      std::string echoed;
      std::thread reader([&] {
        char buf[65536];
        while (echoed.size() < payload.size()) {
          const long long n = RecvSome(s, buf, sizeof(buf));
          if (n <= 0) break;
          echoed.append(buf, static_cast<size_t>(n));
        }
      });
      SendAll(s, payload);
      reader.join();
      if (echoed != payload) failures++;
      CloseSocket(s);
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(failures.load(), 0);
  CHECK(WaitFor([&] { return lb->listener_stats("tcp")->active.load() == 0; }));
}

TEST(stress, slow_reader_gets_complete_response) {
  // The client reads far slower than the backend writes, so the proxy must
  // pause the backend (backpressure) instead of buffering everything.
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig("threads 1\npool web\nlistener http mode=http bind=127.0.0.1:0 pool=web\n", "web",
                               backends));
  const SocketHandle s = ConnectTcpBlocking(lb->listener_address("http"), 2000);
  SetRecvTimeout(s, 10000);
  CHECK(SendAll(s, "GET /size/8000000 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"));
  std::string all;
  char buf[65536];
  long long n;
  int reads = 0;
  while ((n = RecvSome(s, buf, sizeof(buf))) > 0) {
    all.append(buf, static_cast<size_t>(n));
    if (++reads % 8 == 0) SleepMs(5);
  }
  CloseSocket(s);
  const size_t body = all.find("\r\n\r\n");
  CHECK(body != std::string::npos);
  CHECK_EQ(all.size() - body - 4, 8000000u);
}
