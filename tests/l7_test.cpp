// Layer 7 (HTTP) proxying through a real load balancer to real backends.
#include <map>
#include <thread>

#include "harness.h"

using namespace hlb;
using namespace hlb::testkit;
using namespace lbtest;

namespace {

const char* kHttpConfig =
    "threads 2\n"
    "pool web algorithm=round_robin\n"
    "listener http mode=http bind=127.0.0.1:0 pool=web response_timeout_ms=1500\n";

std::string Field(const std::string& body, const std::string& key) {
  const size_t at = body.find(key + "=");
  if (at == std::string::npos) return "";
  const size_t start = at + key.size() + 1;
  return body.substr(start, body.find('\n', start) - start);
}

}  // namespace

TEST(l7, proxies_get_and_post_and_round_robins) {
  auto backends = StartHttpBackends(3);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  std::map<std::string, int> seen;
  for (int i = 0; i < 30; i++) {
    auto r = client.Get("/hello?i=" + std::to_string(i));
    CHECK(r.ok);
    CHECK_EQ(r.status, 200);
    seen[*r.Header("x-backend")]++;
  }
  CHECK_EQ(seen.size(), 3u);
  for (auto& [id, n] : seen) CHECK_EQ(n, 10);

  const std::string payload(100000, 'p');
  auto r = client.Request("POST", "/echo", payload);
  CHECK_EQ(r.status, 200);
  CHECK_EQ(Field(r.body, "method"), std::string("POST"));
  CHECK(r.body.size() > payload.size());
  CHECK_EQ(r.body.substr(r.body.size() - payload.size()), payload);
  CHECK_EQ(client.connects(), 1);  // Everything above used one keep-alive connection.
}

TEST(l7, routes_by_host_prefix_and_header) {
  auto api = StartHttpBackends(1);
  auto web = StartHttpBackends(1);
  auto canary = StartHttpBackends(1);
  Config cfg;
  std::string err;
  const std::string text =
      "threads 2\npool api\npool web\npool canary\n"
      "backend api " + api[0]->address().ToString() + "\n"
      "backend web " + web[0]->address().ToString() + "\n"
      "backend canary " + canary[0]->address().ToString() + "\n"
      "listener http mode=http bind=127.0.0.1:0\n"
      "route http header=X-Canary:yes pool=canary\n"
      "route http host=api.local prefix=/v1/ pool=api\n"
      "route http pool=web\n";
  CHECK(Config::Parse(text, &cfg, &err));
  cfg.force_poll = ForcePoll();
  auto lb = StartLb(std::move(cfg));
  HttpClient client(lb->listener_address("http"));
  auto get = [&](const std::string& target, std::vector<Header> headers) {
    auto r = client.Request("GET", target, "", headers);
    CHECK_EQ(r.status, 200);
    return r.body;
  };
  // All three backends are named b0, so identify them by port via the echo.
  CHECK(lb->pool("api")->Snapshot()[0]->requests == 0);
  get("/v1/users", {{"Host", "api.local"}});
  get("/v1/users", {{"Host", "other.local"}});
  get("/anything", {{"X-Canary", "yes"}});
  CHECK(WaitFor([&] { return lb->pool("api")->Snapshot()[0]->requests == 1; }));
  CHECK_EQ(lb->pool("web")->Snapshot()[0]->requests.load(), 1u);
  CHECK_EQ(lb->pool("canary")->Snapshot()[0]->requests.load(), 1u);
}

TEST(l7, adds_forwarding_headers_and_strips_hop_by_hop) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  auto r = client.Request("GET", "/echo", "",
                          {{"Connection", "keep-alive, X-Secret-Hop"}, {"X-Secret-Hop", "leak"},
                           {"X-Forwarded-For", "203.0.113.9"}});
  CHECK_EQ(r.status, 200);
  CHECK_EQ(Field(r.body, "x-forwarded-for"), std::string("203.0.113.9, 127.0.0.1"));
  CHECK_EQ(Field(r.body, "x-forwarded-proto"), std::string("http"));
  CHECK_EQ(Field(r.body, "via"), std::string("1.1 hybridlb"));
  CHECK_EQ(Field(r.body, "x-request-id").size(), 16u);
  CHECK(Field(r.body, "x-secret-hop").empty());  // Named in Connection: must not be forwarded.
  CHECK(Field(r.body, "connection").empty());
}

TEST(l7, reuses_backend_connections) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig("threads 1\npool web\nlistener http mode=http bind=127.0.0.1:0 pool=web\n", "web",
                               backends));
  for (int c = 0; c < 5; c++) {
    HttpClient client(lb->listener_address("http"));  // New client connection each round.
    for (int i = 0; i < 20; i++) CHECK_EQ(client.Get("/x").status, 200);
  }
  // 100 requests from 5 client connections, but the proxy kept one pooled
  // connection to the backend and reused it.
  CHECK_EQ(backends[0]->requests(), 100u);
  CHECK_EQ(backends[0]->connections(), 1u);
  CHECK_EQ(lb->upstream_reuses(), 99u);
}

TEST(l7, streams_large_and_chunked_responses) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  auto big = client.Get("/size/20000000");
  CHECK_EQ(big.status, 200);
  CHECK_EQ(big.body.size(), 20000000u);
  auto chunked = client.Get("/chunked/500");
  CHECK_EQ(chunked.status, 200);
  CHECK_EQ(*chunked.Header("transfer-encoding"), std::string("chunked"));
  CHECK(chunked.body.substr(0, 8) == "chunk-0;");
  CHECK(chunked.body.find("chunk-499;") != std::string::npos);
  // The connection is still usable afterwards.
  CHECK_EQ(client.Get("/x").status, 200);
  CHECK_EQ(client.connects(), 1);
}

TEST(l7, head_204_and_until_close_responses) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  auto head = client.Request("HEAD", "/size/1000");
  CHECK_EQ(head.status, 200);
  CHECK_EQ(*head.Header("content-length"), std::string("1000"));
  CHECK(head.body.empty());
  CHECK_EQ(client.Get("/status/204").status, 204);
  CHECK_EQ(client.connects(), 1);
  // A body delimited by connection close forces the proxy to close the client too.
  auto until_close = client.Get("/until-close/5000");
  CHECK_EQ(until_close.status, 200);
  CHECK_EQ(until_close.body.size(), 5000u);
  CHECK_EQ(*until_close.Header("connection"), std::string("close"));
}

TEST(l7, expect_100_continue_and_pipelining) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  auto r = client.Request("POST", "/echo", "data", {{"Expect", "100-continue"}});
  CHECK_EQ(r.status, 200);  // The client skips the interim 100 response.
  CHECK_EQ(Field(r.body, "body"), std::string("data"));

  // Three pipelined requests in one write must be answered in order.
  const SocketHandle s = ConnectTcpBlocking(lb->listener_address("http"), 2000);
  SetRecvTimeout(s, 5000);
  CHECK(SendAll(s, "GET /p1 HTTP/1.1\r\nHost: x\r\n\r\nGET /p2 HTTP/1.1\r\nHost: x\r\n\r\n"
                   "GET /p3 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"));
  std::string all;
  char buf[4096];
  long long n;
  while ((n = RecvSome(s, buf, sizeof(buf))) > 0) all.append(buf, static_cast<size_t>(n));
  CloseSocket(s);
  const size_t p1 = all.find("path=/p1"), p2 = all.find("path=/p2"), p3 = all.find("path=/p3");
  CHECK(p1 != std::string::npos && p2 != std::string::npos && p3 != std::string::npos);
  CHECK(p1 < p2 && p2 < p3);
}

TEST(l7, rejects_bad_requests) {
  auto backends = StartHttpBackends(1);
  auto cfg = MakeConfig(kHttpConfig, "web", backends);
  cfg.listeners[0].max_body_bytes = 1000;
  auto lb = StartLb(std::move(cfg));
  {
    HttpClient c(lb->listener_address("http"));
    CHECK_EQ(c.Raw("GARBAGE\r\n\r\n").status, 400);
  }
  {
    HttpClient c(lb->listener_address("http"));
    CHECK_EQ(c.Raw("POST / HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n").status, 400);
  }
  {
    HttpClient c(lb->listener_address("http"));
    CHECK_EQ(c.Request("POST", "/echo", std::string(5000, 'x')).status, 413);
  }
  CHECK_EQ(backends[0]->requests(), 0u);  // None of these reached the backend.
}

TEST(l7, returns_503_502_and_504) {
  auto backends = StartHttpBackends(1);
  auto lb = StartLb(MakeConfig(kHttpConfig, "web", backends));
  HttpClient client(lb->listener_address("http"));
  // Slower than response_timeout_ms (1500): gateway timeout.
  auto slow = client.Get("/delay/3000");
  CHECK_EQ(slow.status, 504);
  // Backend gone: connect failures exhaust retries.
  backends[0]->Kill();
  auto gone = client.Get("/x");
  CHECK(gone.status == 502 || gone.status == 503);
  // Marked unhealthy by the active checker: nothing to choose at all.
  lb->pool("web")->Snapshot()[0]->healthy = false;
  CHECK_EQ(client.Get("/x").status, 503);
}

TEST(l7, admin_api_reports_stats_and_manages_backends) {
  auto backends = StartHttpBackends(2);
  auto cfg = MakeConfig(kHttpConfig, "web", std::vector<HttpServer*>{backends[0].get()});
  cfg.admin = Address{"127.0.0.1", 0};
  auto lb = StartLb(std::move(cfg));
  HttpClient client(lb->listener_address("http"));
  HttpClient admin(lb->admin_address());
  for (int i = 0; i < 5; i++) CHECK_EQ(client.Get("/x").status, 200);

  auto stats = admin.Get("/stats");
  CHECK_EQ(stats.status, 200);
  CHECK(stats.body.find("\"requests\":5") != std::string::npos);
  CHECK(stats.body.find("\"algorithm\":\"round_robin\"") != std::string::npos);

  const std::string second = backends[1]->address().ToString();
  CHECK_EQ(admin.Request("POST", "/pools/web/backends?address=" + second + "&weight=1").status, 201);
  CHECK_EQ(admin.Request("POST", "/pools/web/backends?address=" + second).status, 409);  // Duplicate.
  for (int i = 0; i < 10; i++) CHECK_EQ(client.Get("/x").status, 200);
  CHECK(backends[1]->requests() >= 4);  // The new backend is in rotation.

  CHECK_EQ(admin.Request("DELETE", "/pools/web/backends/" + second).status, 200);
  const uint64_t before = backends[1]->requests();
  for (int i = 0; i < 10; i++) CHECK_EQ(client.Get("/x").status, 200);
  CHECK_EQ(backends[1]->requests(), before);  // Removed: no more traffic.
  CHECK_EQ(admin.Get("/nope").status, 404);
}
