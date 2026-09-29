// Single-threaded reactor: waits for socket readiness and timers and runs
// callbacks. Each worker thread owns one loop; other threads talk to it only
// through Post().
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "net/poller.h"

namespace hlb {

class IoHandler {
 public:
  virtual ~IoHandler() = default;
  virtual void OnEvents(uint32_t events) = 0;
};

class EventLoop {
 public:
  using Fn = std::function<void()>;
  using Clock = std::chrono::steady_clock;
  using TimerId = uint64_t;

  explicit EventLoop(bool force_poll = false);
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // Runs until Stop(). Call from the thread that will own the loop.
  void Run();
  // Thread-safe.
  void Stop();
  // Thread-safe: runs `fn` on the loop thread.
  void Post(Fn fn);
  bool InLoopThread() const { return std::this_thread::get_id() == thread_id_.load(); }

  // Loop thread only. Watch() both registers and updates interest.
  void Watch(SocketHandle fd, IoHandler* handler, uint32_t events);
  void Unwatch(SocketHandle fd);

  // Loop thread only.
  TimerId RunAfter(std::chrono::milliseconds delay, Fn fn);
  void Cancel(TimerId id);

  // Keeps `p` alive until the current round of callbacks has finished, so an
  // object may safely drop the last reference to itself from its own callback.
  void DeferRelease(std::shared_ptr<void> p) { graveyard_.push_back(std::move(p)); }

  const char* poller_name() const { return poller_->Name(); }
  // Nanoseconds spent handling events vs blocked waiting for them. Their
  // ratio is the loop's utilization: near 100% means this thread is saturated.
  uint64_t busy_ns() const { return busy_ns_.load(std::memory_order_relaxed); }
  uint64_t idle_ns() const { return idle_ns_.load(std::memory_order_relaxed); }

 private:
  void RunTimers();
  int NextTimeoutMs() const;
  void DrainWakeup();

  std::unique_ptr<Poller> poller_;
  std::unordered_map<SocketHandle, IoHandler*> handlers_;
  SocketHandle wake_read_ = kInvalidSocket;
  SocketHandle wake_write_ = kInvalidSocket;

  std::mutex mu_;
  std::vector<Fn> pending_;
  bool wake_pending_ = false;
  std::atomic<bool> stop_{false};
  std::atomic<std::thread::id> thread_id_;

  TimerId next_timer_ = 1;
  std::multimap<Clock::time_point, TimerId> timer_queue_;
  std::unordered_map<TimerId, std::pair<Clock::time_point, Fn>> timers_;

  std::vector<std::shared_ptr<void>> graveyard_;
  std::vector<PollEvent> ready_;
  std::atomic<uint64_t> busy_ns_{0};
  std::atomic<uint64_t> idle_ns_{0};
};

}  // namespace hlb
