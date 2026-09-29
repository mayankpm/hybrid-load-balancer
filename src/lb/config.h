// Load balancer configuration and the L7 router.
//
// File format: one directive per line, "key=value" options, '#' comments.
//
//   threads 4
//   admin 127.0.0.1:9901
//   pool api algorithm=least_connections health=http health_path=/health interval_ms=1000
//   backend api 127.0.0.1:9001 weight=2
//   listener web mode=http bind=0.0.0.0:8080 retries=2
//   route web host=api.example.com prefix=/v1 pool=api
//   route web header=X-Canary:1 pool=canary
//   route web pool=api                      # default route
//   listener db mode=tcp bind=0.0.0.0:6000 pool=api
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "http/http.h"
#include "lb/backend.h"
#include "net/socket.h"

namespace hlb {

struct BackendConfig {
  Address address;
  int weight = 1;
};

struct PoolConfig {
  std::string name;
  Algorithm algorithm = Algorithm::kRoundRobin;
  HealthCheckConfig health;
  PassiveConfig passive;
  int max_idle_per_backend = 32;  // Keep-alive upstream connections, per worker thread.
  std::vector<BackendConfig> backends;
};

struct RouteConfig {
  std::string host;          // Exact, or "*.example.com" suffix match; empty matches any.
  std::string path_prefix;   // Empty matches any.
  std::string header_name;   // With header_value: exact header match.
  std::string header_value;
  std::string pool;
};

struct ListenerConfig {
  enum class Mode { kTcp, kHttp };
  std::string name;
  Mode mode = Mode::kHttp;
  Address bind;
  std::string pool;  // TCP listeners: the pool to proxy to.
  std::vector<RouteConfig> routes;
  int connect_timeout_ms = 1000;
  int response_timeout_ms = 30000;  // Max silence from a backend mid-exchange.
  int idle_timeout_ms = 60000;      // Keep-alive client idle time (and L4 session idle time).
  int upstream_idle_ms = 30000;     // How long pooled backend connections stay open.
  int retries = 2;                  // Extra attempts on other backends.
  size_t max_body_bytes = 8u << 20;
};

struct Config {
  int threads = 0;  // 0 = hardware concurrency.
  bool force_poll = false;
  std::optional<Address> admin;
  int shutdown_timeout_ms = 10000;
  std::vector<PoolConfig> pools;
  std::vector<ListenerConfig> listeners;

  static bool Parse(std::string_view text, Config* out, std::string* error);
  static bool Load(const std::string& path, Config* out, std::string* error);
  bool Validate(std::string* error) const;
  PoolConfig* FindPool(std::string_view name);
  ListenerConfig* FindListener(std::string_view name);
};

class Router {
 public:
  explicit Router(std::vector<RouteConfig> routes) : routes_(std::move(routes)) {}
  // First matching rule wins. Returns nullptr when nothing matches.
  const RouteConfig* Match(const HttpHead& request) const;

 private:
  std::vector<RouteConfig> routes_;
};

}  // namespace hlb
