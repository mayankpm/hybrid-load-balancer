#include "net/event_loop.h"

#include <cstdio>
#include <stdexcept>

namespace hlb {
namespace {

// A connected loopback TCP pair: the portable stand-in for eventfd/pipe that
// also works on Windows, where poll() only accepts sockets.
void MakeWakeupPair(SocketHandle* read_end, SocketHandle* write_end) {
  std::string err;
  Address bound;
  const SocketHandle listener = ListenTcp(Address{"127.0.0.1", 0}, 1, &bound, &err);
  if (listener == kInvalidSocket) throw std::runtime_error("wakeup pair: " + err);
  SetNonBlocking(listener, false);
  *write_end = ConnectTcpBlocking(bound, 5000);
  *read_end = AcceptTcp(listener, nullptr);
  CloseSocket(listener);
  if (*write_end == kInvalidSocket || *read_end == kInvalidSocket) throw std::runtime_error("wakeup pair failed");
  SetNonBlocking(*read_end, true);
  SetNonBlocking(*write_end, true);
}

}  // namespace

EventLoop::EventLoop(bool force_poll) : poller_(Poller::Create(force_poll)), thread_id_(std::this_thread::get_id()) {
  NetInit();
  MakeWakeupPair(&wake_read_, &wake_write_);
  poller_->Set(wake_read_, kEvRead);
}

EventLoop::~EventLoop() {
  poller_->Remove(wake_read_);
  CloseSocket(wake_read_);
  CloseSocket(wake_write_);
}

void EventLoop::Post(Fn fn) {
  bool need_wake;
  {
    std::lock_guard lock(mu_);
    pending_.push_back(std::move(fn));
    need_wake = !wake_pending_;
    wake_pending_ = true;
  }
  if (need_wake) WriteSome(wake_write_, "x", 1);
}

void EventLoop::Stop() {
  stop_ = true;
  Post([] {});
}

void EventLoop::DrainWakeup() {
  char buf[256];
  while (ReadSome(wake_read_, buf, sizeof(buf)) > 0) {
  }
}

void EventLoop::Watch(SocketHandle fd, IoHandler* handler, uint32_t events) {
  handlers_[fd] = handler;
  poller_->Set(fd, events);
}

void EventLoop::Unwatch(SocketHandle fd) {
  if (handlers_.erase(fd) != 0) poller_->Remove(fd);
}

EventLoop::TimerId EventLoop::RunAfter(std::chrono::milliseconds delay, Fn fn) {
  const TimerId id = next_timer_++;
  const auto when = Clock::now() + delay;
  timers_.emplace(id, std::make_pair(when, std::move(fn)));
  timer_queue_.emplace(when, id);
  return id;
}

void EventLoop::Cancel(TimerId id) {
  auto it = timers_.find(id);
  if (it == timers_.end()) return;
  auto range = timer_queue_.equal_range(it->second.first);
  for (auto q = range.first; q != range.second; ++q) {
    if (q->second == id) {
      timer_queue_.erase(q);
      break;
    }
  }
  timers_.erase(it);
}

int EventLoop::NextTimeoutMs() const {
  if (timer_queue_.empty()) return 1000;  // Bounded so a missed wakeup can never hang the loop.
  const auto delta = timer_queue_.begin()->first - Clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
  if (ms <= 0) return 0;
  return static_cast<int>(std::min<long long>(ms + 1, 1000));
}

void EventLoop::RunTimers() {
  const auto now = Clock::now();
  while (!timer_queue_.empty() && timer_queue_.begin()->first <= now) {
    const TimerId id = timer_queue_.begin()->second;
    timer_queue_.erase(timer_queue_.begin());
    auto it = timers_.find(id);
    if (it == timers_.end()) continue;
    Fn fn = std::move(it->second.second);
    timers_.erase(it);
    fn();
  }
}

void EventLoop::Run() {
  thread_id_ = std::this_thread::get_id();
  while (!stop_) {
    ready_.clear();
    const auto wait_start = Clock::now();
    poller_->Wait(NextTimeoutMs(), &ready_);
    const auto work_start = Clock::now();
    idle_ns_.fetch_add(static_cast<uint64_t>((work_start - wait_start).count()), std::memory_order_relaxed);
    for (const PollEvent& ev : ready_) {
      if (ev.fd == wake_read_) {
        DrainWakeup();
        continue;
      }
      // Look the handler up per event: an earlier callback in this batch may
      // have closed this socket.
      auto it = handlers_.find(ev.fd);
      if (it != handlers_.end()) it->second->OnEvents(ev.events);
    }
    RunTimers();

    std::vector<Fn> pending;
    {
      std::lock_guard lock(mu_);
      pending.swap(pending_);
      wake_pending_ = false;
    }
    for (Fn& fn : pending) fn();
    graveyard_.clear();
    busy_ns_.fetch_add(static_cast<uint64_t>((Clock::now() - work_start).count()), std::memory_order_relaxed);
  }
  // Run anything posted during shutdown so resources are released in order.
  std::vector<Fn> pending;
  {
    std::lock_guard lock(mu_);
    pending.swap(pending_);
  }
  for (Fn& fn : pending) fn();
  graveyard_.clear();
}

}  // namespace hlb
