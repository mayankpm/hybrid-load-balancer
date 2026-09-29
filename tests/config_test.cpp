#include "lb/config.h"
#include "test.h"

using namespace hlb;

namespace {

HttpHead Req(const std::string& target, const std::string& host, std::vector<Header> extra = {}) {
  HttpHead h;
  h.method = "GET";
  h.target = target;
  if (!host.empty()) h.Add("Host", host);
  for (auto& e : extra) h.headers.push_back(e);
  return h;
}

}  // namespace

TEST(config, parses_full_example) {
  const char* text = R"(
    threads 4
    admin 127.0.0.1:9901          # admin API
    pool api algorithm=least_connections health=http health_path=/healthz interval_ms=500 rise=3 fall=2 max_fails=5 eject_ms=2000
    backend api 10.0.0.1:8080 weight=3
    backend api 10.0.0.2:8080
    pool db algorithm=weighted_round_robin health=tcp
    backend db 10.0.0.3:5432 weight=2
    listener web mode=http bind=0.0.0.0:8080 retries=1 response_timeout_ms=2000
    route web host=*.example.com prefix=/v1 pool=api
    route web pool=api
    listener sql mode=tcp bind=0.0.0.0:6000 pool=db
  )";
  Config c;
  std::string err;
  if (!Config::Parse(text, &c, &err)) throw lbtest::Failure(err);
  CHECK_EQ(c.threads, 4);
  CHECK(c.admin.has_value());
  CHECK_EQ(c.admin->port, 9901);
  CHECK_EQ(c.pools.size(), 2u);
  const PoolConfig& api = c.pools[0];
  CHECK(api.algorithm == Algorithm::kLeastConnections);
  CHECK(api.health.type == HealthCheckConfig::Type::kHttp);
  CHECK_EQ(api.health.path, std::string("/healthz"));
  CHECK_EQ(api.health.rise, 3);
  CHECK_EQ(api.passive.max_fails, 5);
  CHECK_EQ(api.backends.size(), 2u);
  CHECK_EQ(api.backends[0].weight, 3);
  CHECK(c.pools[1].health.type == HealthCheckConfig::Type::kTcp);
  CHECK_EQ(c.listeners.size(), 2u);
  CHECK_EQ(c.listeners[0].routes.size(), 2u);
  CHECK_EQ(c.listeners[0].retries, 1);
  CHECK(c.listeners[1].mode == ListenerConfig::Mode::kTcp);
}

TEST(config, reports_errors_with_line_numbers) {
  struct Case {
    const char* text;
    const char* expect;
  } cases[] = {
      {"pool a\nbackend b 1.2.3.4:5\n", "line 2: backend refers to unknown pool b"},
      {"pool a algorithm=random\n", "line 1: unknown algorithm random"},
      {"pool a\nlistener l mode=udp bind=:1\n", "line 2: listener needs mode=tcp or mode=http"},
      {"pool a\nlistener l mode=tcp bind=:1 pool=nope\n", "tcp listener l needs pool= naming a defined pool"},
      {"pool a\nlistener l mode=http bind=:1\nroute l pool=missing\n", "route on l refers to unknown pool missing"},
      {"pool a bogus=1\n", "line 1: unknown pool option bogus"},
      {"pool a\npool a\n", "line 2: duplicate pool a"},
      {"frobnicate\n", "line 1: unknown directive frobnicate"},
  };
  for (const Case& c : cases) {
    Config cfg;
    std::string err;
    CHECK(!Config::Parse(c.text, &cfg, &err));
    CHECK_EQ(err, std::string(c.expect));
  }
}

TEST(config, router_matches_host_prefix_and_header_in_order) {
  Router r({
      {"api.example.com", "/v2", "", "", "api-v2"},
      {"*.example.com", "", "", "", "wildcard"},
      {"", "/static/", "", "", "static"},
      {"", "", "X-Canary", "1", "canary"},
      {"", "", "", "", "default"},
  });
  auto pool = [&](const HttpHead& h) { return r.Match(h)->pool; };
  CHECK_EQ(pool(Req("/v2/users", "api.example.com:8080")), std::string("api-v2"));
  CHECK_EQ(pool(Req("/v1/users", "API.Example.com")), std::string("wildcard"));
  CHECK_EQ(pool(Req("/x", "shop.example.com")), std::string("wildcard"));
  CHECK_EQ(pool(Req("/x", "example.com")), std::string("default"));  // Wildcard needs a subdomain.
  CHECK_EQ(pool(Req("/static/app.js", "other.org")), std::string("static"));
  CHECK_EQ(pool(Req("/x", "other.org", {{"x-canary", "1"}})), std::string("canary"));
  CHECK_EQ(pool(Req("/x", "other.org", {{"x-canary", "0"}})), std::string("default"));
  // Absolute-form target carries its own host.
  CHECK_EQ(pool(Req("http://api.example.com/v2/x", "")), std::string("api-v2"));
}
