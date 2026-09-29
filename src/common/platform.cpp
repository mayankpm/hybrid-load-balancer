#include "common/platform.h"

#ifdef _WIN32
#if !defined(_WIN32_WINNT) || _WIN32_WINNT < 0x0A00
#undef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00  // SetProcessInformation / power throttling (Windows 10+).
#endif
#include <windows.h>
#include <timeapi.h>
#else
#include <chrono>
#include <thread>
#endif

namespace hlb {

void EnableHighResolutionTimers() {
#ifdef _WIN32
  // Windows 11 ignores timer resolution requests from processes without a
  // visible window (e.g. a server started in the background) unless they opt
  // out of this power-throttling behavior.
  PROCESS_POWER_THROTTLING_STATE state{};
  state.Version = PROCESS_POWER_THROTTLING_CURRENT_VERSION;
  state.ControlMask = 0x4;  // PROCESS_POWER_THROTTLING_IGNORE_TIMER_RESOLUTION
  state.StateMask = 0;
  SetProcessInformation(GetCurrentProcess(), ProcessPowerThrottling, &state, sizeof(state));
  timeBeginPeriod(1);
#endif
}

void SleepMicros(int64_t us) {
  if (us <= 0) return;
#ifdef _WIN32
  Sleep(static_cast<DWORD>((us + 999) / 1000));
#else
  std::this_thread::sleep_for(std::chrono::microseconds(us));
#endif
}

}  // namespace hlb
