// Readiness notification backends. Level triggered, so a handler that leaves
// data unread is simply called again on the next iteration.
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "net/socket.h"

namespace hlb {

enum : uint32_t {
  kEvRead = 1,
  kEvWrite = 2,
  kEvError = 4,  // Error or hang-up; reported even when not requested.
};

struct PollEvent {
  SocketHandle fd;
  uint32_t events;
};

class Poller {
 public:
  // epoll on Linux, poll()/WSAPoll elsewhere. `force_poll` selects the
  // portable backend on Linux too (used to test both).
  static std::unique_ptr<Poller> Create(bool force_poll = false);
  virtual ~Poller() = default;

  // Registers `fd` or changes its interest set.
  virtual void Set(SocketHandle fd, uint32_t events) = 0;
  virtual void Remove(SocketHandle fd) = 0;
  // Blocks up to `timeout_ms` (-1 = forever) and appends ready sockets to `out`.
  virtual void Wait(int timeout_ms, std::vector<PollEvent>* out) = 0;
  virtual const char* Name() const = 0;
};

}  // namespace hlb
