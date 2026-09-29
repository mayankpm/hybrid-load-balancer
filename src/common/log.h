// Minimal leveled logger: timestamped lines to stderr.
#pragma once

#include <atomic>
#include <cstdarg>

namespace hlb {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3, kOff = 4 };

void SetLogLevel(LogLevel level);
LogLevel GetLogLevel();
bool ParseLogLevel(const char* name, LogLevel* out);

#if defined(__GNUC__)
__attribute__((format(printf, 2, 3)))
#endif
void Log(LogLevel level, const char* fmt, ...);

#define HLB_LOG(level, ...)                                                   \
  do {                                                                        \
    if (static_cast<int>(level) >= static_cast<int>(::hlb::GetLogLevel())) \
      ::hlb::Log(level, __VA_ARGS__);                                         \
  } while (0)

#define LOG_DEBUG(...) HLB_LOG(::hlb::LogLevel::kDebug, __VA_ARGS__)
#define LOG_INFO(...) HLB_LOG(::hlb::LogLevel::kInfo, __VA_ARGS__)
#define LOG_WARN(...) HLB_LOG(::hlb::LogLevel::kWarn, __VA_ARGS__)
#define LOG_ERROR(...) HLB_LOG(::hlb::LogLevel::kError, __VA_ARGS__)

}  // namespace hlb
