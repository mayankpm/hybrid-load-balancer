// Layer 4 (TCP) proxying through a real load balancer to real echo backends.
#include <map>
#include <random>
#include <thread>

#include "harness.h"

using namespace hlb;
using namespace lbtest;

namespace {

const char* kTcpConfig =
    "threads 2\n"
    "pool echo algorithm=round_robin\n"
    "listener tcp mode=tcp bind=127.0.0.1:0 pool=echo connect_timeout_ms=500\n";

// Connects through the proxy and reads the backend's "<id>\n" greeting.
std::string Hello(SocketHandle s) {
  std::string line;
  char c;
  while (line.size() < 64) {
    if (RecvSome(s, &c, 1) != 1) return "";
    if (c == '\n') return line;
    line.push_back(c);
  }
  return line;
}

bool ReadExactly(SocketHandle s, size_t n, std::string* out) {
  char buf[65536];
  while (out->size() < n) {
    const long long got = RecvSome(s, buf, std::min(sizeof(buf), n - out->size()));
    if (got <= 0) return false;
    out->append(buf, static_cast<size_t>(got));
  }
  return true;
}

}  // namespace

TEST(l4, proxies_and_round_robins_connections) {
  auto backends = StartEchoBackends(3);
  auto lb = StartLb(MakeConfig(kTcpConfig, "echo", backends));
  std::map<std::string, int> seen;
  for (int i = 0; i < 30; i++) {
    const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
    CHECK(s != kInvalidSocket);
    SetRecvTimeout(s, 5000);
    const std::string id = Hello(s);
    seen[id]++;
    const std::string msg = "ping-" + std::to_string(i);
    CHECK(SendAll(s, msg));
    std::string echo;
    CHECK(ReadExactly(s, msg.size(), &echo));
    CHECK_EQ(echo, msg);
    CloseSocket(s);
  }
  CHECK_EQ(seen.size(), 3u);
  for (auto& [id, n] : seen) CHECK_EQ(n, 10);
}

TEST(l4, large_transfer_is_intact_under_backpressure) {
  auto backends = StartEchoBackends(1);
  auto lb = StartLb(MakeConfig(kTcpConfig, "echo", backends));
  const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
  SetRecvTimeout(s, 10000);
  CHECK_EQ(Hello(s), std::string("e0"));

  // 16 MB of pseudo-random data, far more than any socket or proxy buffer, so
  // the proxy has to apply backpressure in both directions.
  std::string payload(16u << 20, '\0');
  std::mt19937 rng(7);
  for (char& c : payload) c = static_cast<char>(rng());
  std::string echoed;
  std::thread reader([&] { ReadExactly(s, payload.size(), &echoed); });
  CHECK(SendAll(s, payload));
  reader.join();
  CHECK(echoed == payload);
  CloseSocket(s);
}

TEST(l4, half_close_is_forwarded_in_both_directions) {
  auto backends = StartEchoBackends(1);
  auto lb = StartLb(MakeConfig(kTcpConfig, "echo", backends));
  const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
  SetRecvTimeout(s, 5000);
  CHECK_EQ(Hello(s), std::string("e0"));
  CHECK(SendAll(s, "request-then-fin"));
  ShutdownWrite(s);  // Client is done sending but still expects a reply.
  std::string rest;
  char buf[256];
  long long n;
  while ((n = RecvSome(s, buf, sizeof(buf))) > 0) rest.append(buf, static_cast<size_t>(n));
  CHECK_EQ(n, 0LL);  // The backend's FIN arrives as a clean EOF.
  CHECK_EQ(rest, std::string("request-then-fin"));
  CloseSocket(s);
}

TEST(l4, fails_over_when_a_backend_is_down) {
  auto backends = StartEchoBackends(3);
  auto lb = StartLb(MakeConfig(kTcpConfig, "echo", backends));
  backends[1]->Kill();  // Its port now refuses connections.
  int ok = 0;
  for (int i = 0; i < 30; i++) {
    const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
    SetRecvTimeout(s, 5000);
    const std::string id = Hello(s);
    CHECK(id == "e0" || id == "e2");
    ok++;
    CloseSocket(s);
  }
  CHECK_EQ(ok, 30);
  // Passive health checking ejected the dead backend after repeated failures.
  bool ejected = false;
  for (const auto& b : lb->pool("echo")->Snapshot()) {
    if (b->address() == backends[1]->address()) ejected = b->ejections.load() > 0;
  }
  CHECK(ejected);
}

TEST(l4, least_connections_balances_long_lived_connections) {
  auto backends = StartEchoBackends(3);
  auto cfg = MakeConfig(
      "threads 2\npool echo algorithm=least_connections\nlistener tcp mode=tcp bind=127.0.0.1:0 pool=echo\n", "echo",
      backends);
  auto lb = StartLb(std::move(cfg));
  std::vector<SocketHandle> open;
  std::map<std::string, int> seen;
  for (int i = 0; i < 12; i++) {
    const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
    SetRecvTimeout(s, 5000);
    seen[Hello(s)]++;
    open.push_back(s);  // Keep them open: each adds to its backend's load.
  }
  for (auto& [id, n] : seen) CHECK_EQ(n, 4);
  for (SocketHandle s : open) CloseSocket(s);
  CHECK(WaitFor([&] { return lb->listener_stats("tcp")->active.load() == 0; }));
}

TEST(l4, closes_client_when_no_backend_is_reachable) {
  auto backends = StartEchoBackends(2);
  auto lb = StartLb(MakeConfig(kTcpConfig, "echo", backends));
  for (auto& b : backends) b->Kill();
  const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
  SetRecvTimeout(s, 5000);
  char c;
  CHECK(RecvSome(s, &c, 1) <= 0);  // Closed, not hung.
  CloseSocket(s);
}
