// Runs every registered test, or only those whose name contains one of the
// command-line arguments (e.g. `lbtests l7.` or `lbtests failover.kill`).
// Set HLB_FORCE_POLL=1 to run the proxy on the portable poll() backend
// instead of epoll on Linux; CI runs the suite both ways.
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>

#include "common/log.h"
#include "common/platform.h"
#include "net/socket.h"
#include "test.h"

int main(int argc, char** argv) {
  hlb::EnableHighResolutionTimers();
  hlb::NetInit();
  hlb::LogLevel level = hlb::LogLevel::kError;
  if (const char* env = std::getenv("HLB_LOG")) hlb::ParseLogLevel(env, &level);
  hlb::SetLogLevel(level);

  std::vector<std::string> filters(argv + 1, argv + argc);
  int passed = 0, failed = 0;
  std::vector<std::string> failures;

  for (const auto& t : lbtest::Registry()) {
    if (!filters.empty()) {
      bool match = false;
      for (const auto& f : filters) match = match || t.name.find(f) != std::string::npos;
      if (!match) continue;
    }
    std::printf("[ RUN  ] %s\n", t.name.c_str());
    std::fflush(stdout);
    const auto start = std::chrono::steady_clock::now();
    try {
      t.fn();
      const auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
      std::printf("[  OK  ] %s (%lld ms)\n", t.name.c_str(), static_cast<long long>(ms));
      passed++;
    } catch (const std::exception& e) {
      std::printf("[ FAIL ] %s\n         %s\n", t.name.c_str(), e.what());
      failures.push_back(t.name);
      failed++;
    }
    std::fflush(stdout);
  }

  std::printf("\n%d passed, %d failed\n", passed, failed);
  for (const auto& f : failures) std::printf("  FAILED: %s\n", f.c_str());
  return failed == 0 ? 0 : 1;
}
