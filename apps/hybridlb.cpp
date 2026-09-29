// hybridlb: the load balancer.
//
//   hybridlb --config lb.conf [--log-level debug|info|warn|error]
//
// SIGINT/SIGTERM trigger a graceful shutdown: stop accepting, finish
// in-flight requests (bounded by shutdown_timeout_ms), then exit.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>

#include "common/log.h"
#include "common/platform.h"
#include "lb/load_balancer.h"

namespace {

std::atomic<bool> g_stop{false};
void OnSignal(int) { g_stop = true; }

}  // namespace

int main(int argc, char** argv) {
  hlb::EnableHighResolutionTimers();
  std::string config_path;
  for (int i = 1; i < argc; i++) {
    const std::string a = argv[i];
    if (a == "--config" && i + 1 < argc) {
      config_path = argv[++i];
    } else if (a == "--log-level" && i + 1 < argc) {
      hlb::LogLevel level;
      if (!hlb::ParseLogLevel(argv[++i], &level)) {
        std::fprintf(stderr, "unknown log level\n");
        return 2;
      }
      hlb::SetLogLevel(level);
    } else {
      std::fprintf(stderr, "usage: hybridlb --config FILE [--log-level debug|info|warn|error]\n");
      return 2;
    }
  }
  if (config_path.empty()) {
    std::fprintf(stderr, "usage: hybridlb --config FILE [--log-level debug|info|warn|error]\n");
    return 2;
  }

  hlb::Config config;
  std::string err;
  if (!hlb::Config::Load(config_path, &config, &err)) {
    std::fprintf(stderr, "hybridlb: %s: %s\n", config_path.c_str(), err.c_str());
    return 1;
  }
  hlb::LoadBalancer lb(std::move(config));
  if (!lb.Start(&err)) {
    std::fprintf(stderr, "hybridlb: %s\n", err.c_str());
    return 1;
  }
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  LOG_INFO("shutting down gracefully");
  lb.Stop();
  return 0;
}
