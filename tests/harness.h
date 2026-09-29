// Helpers for integration tests: start real backends and a real load
// balancer on ephemeral loopback ports.
#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "lb/load_balancer.h"
#include "test.h"
#include "testkit/servers.h"

namespace lbtest {

inline bool ForcePoll() {
  const char* v = std::getenv("HLB_FORCE_POLL");
  return v != nullptr && v[0] == '1';
}

// Starts `n` HTTP backends named b0..b(n-1).
inline std::vector<std::unique_ptr<hlb::testkit::HttpServer>> StartHttpBackends(int n) {
  std::vector<std::unique_ptr<hlb::testkit::HttpServer>> out;
  for (int i = 0; i < n; i++) {
    auto s = std::make_unique<hlb::testkit::HttpServer>("b" + std::to_string(i));
    if (!s->Start()) throw Failure("backend failed to start");
    out.push_back(std::move(s));
  }
  return out;
}

inline std::vector<std::unique_ptr<hlb::testkit::EchoServer>> StartEchoBackends(int n) {
  std::vector<std::unique_ptr<hlb::testkit::EchoServer>> out;
  for (int i = 0; i < n; i++) {
    auto s = std::make_unique<hlb::testkit::EchoServer>("e" + std::to_string(i));
    if (!s->Start()) throw Failure("backend failed to start");
    out.push_back(std::move(s));
  }
  return out;
}

// Builds a config from text, pointing pool `pool` at the given backends.
template <typename Servers>
hlb::Config MakeConfig(const std::string& text, const std::string& pool, const Servers& backends) {
  hlb::Config cfg;
  std::string err;
  if (!hlb::Config::Parse(text, &cfg, &err)) throw Failure("config: " + err);
  hlb::PoolConfig* p = cfg.FindPool(pool);
  if (p == nullptr) throw Failure("config has no pool " + pool);
  for (const auto& b : backends) p->backends.push_back({b->address(), 1});
  cfg.force_poll = ForcePoll();
  return cfg;
}

inline std::unique_ptr<hlb::LoadBalancer> StartLb(hlb::Config cfg) {
  auto lb = std::make_unique<hlb::LoadBalancer>(std::move(cfg));
  std::string err;
  if (!lb->Start(&err)) throw Failure("load balancer failed to start: " + err);
  return lb;
}

// Polls `cond` until it holds or `timeout_ms` passes.
template <typename Fn>
bool WaitFor(Fn cond, int timeout_ms = 5000) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (cond()) return true;
    SleepMs(10);
  }
  return cond();
}

}  // namespace lbtest
