#include "common/log.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <mutex>

namespace hlb {
namespace {

std::atomic<int> g_level{static_cast<int>(LogLevel::kInfo)};
std::mutex g_mu;

}  // namespace

void SetLogLevel(LogLevel level) { g_level = static_cast<int>(level); }
LogLevel GetLogLevel() { return static_cast<LogLevel>(g_level.load()); }

bool ParseLogLevel(const char* name, LogLevel* out) {
  static const struct {
    const char* name;
    LogLevel level;
  } kLevels[] = {{"debug", LogLevel::kDebug}, {"info", LogLevel::kInfo}, {"warn", LogLevel::kWarn},
                 {"error", LogLevel::kError}, {"off", LogLevel::kOff}};
  for (const auto& l : kLevels) {
    if (std::strcmp(name, l.name) == 0) {
      *out = l.level;
      return true;
    }
  }
  return false;
}

void Log(LogLevel level, const char* fmt, ...) {
  static const char* kNames[] = {"DEBUG", "INFO", "WARN", "ERROR", "OFF"};
  const auto now = std::chrono::system_clock::now();
  const std::time_t t = std::chrono::system_clock::to_time_t(now);
  const int ms = static_cast<int>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
  std::tm tm{};
#ifdef _WIN32
  localtime_s(&tm, &t);
#else
  localtime_r(&t, &tm);
#endif
  char msg[1024];
  va_list args;
  va_start(args, fmt);
  std::vsnprintf(msg, sizeof(msg), fmt, args);
  va_end(args);
  std::lock_guard lock(g_mu);
  std::fprintf(stderr, "%02d:%02d:%02d.%03d %-5s %s\n", tm.tm_hour, tm.tm_min, tm.tm_sec, ms,
               kNames[static_cast<int>(level)], msg);
}

}  // namespace hlb
