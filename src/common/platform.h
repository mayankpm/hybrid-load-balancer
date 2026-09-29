// Timer precision helpers.
//
// With MinGW on Windows, std::this_thread::sleep_for and timed condition
// variable waits both round up to the 15.6ms scheduler tick, even after the
// process asks for 1ms resolution. Raft heartbeats/timeouts and the network
// simulator's delays need better than that, so timer loops use SleepMicros.
#pragma once

#include <cstdint>

namespace hlb {

// Requests 1ms timer resolution for the process on Windows (no-op elsewhere).
// Call once at startup.
void EnableHighResolutionTimers();

// Sleeps for about `us` microseconds with ~1ms precision on every platform.
void SleepMicros(int64_t us);

}  // namespace hlb
