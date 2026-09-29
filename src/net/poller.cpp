#include "net/poller.h"

#include <unordered_map>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <poll.h>
#endif
#ifdef __linux__
#include <sys/epoll.h>
#include <unistd.h>
#endif

namespace hlb {
namespace {

#ifdef _WIN32
using PollFd = WSAPOLLFD;
constexpr short kIn = POLLRDNORM;   // WSAPoll rejects POLLIN's POLLRDBAND/POLLPRI bits.
constexpr short kOut = POLLWRNORM;
inline int DoPoll(PollFd* fds, size_t n, int timeout) { return WSAPoll(fds, static_cast<ULONG>(n), timeout); }
#else
using PollFd = pollfd;
constexpr short kIn = POLLIN;
constexpr short kOut = POLLOUT;
inline int DoPoll(PollFd* fds, size_t n, int timeout) { return ::poll(fds, static_cast<nfds_t>(n), timeout); }
#endif

// Portable backend: O(n) per wait, which is fine for the connection counts a
// single worker handles in tests; Linux production use gets epoll.
class PollPoller : public Poller {
 public:
  void Set(SocketHandle fd, uint32_t events) override {
    short mask = 0;
    if (events & kEvRead) mask |= kIn;
    if (events & kEvWrite) mask |= kOut;
    auto it = index_.find(fd);
    if (it == index_.end()) {
      PollFd p{};
      p.fd = static_cast<decltype(p.fd)>(fd);
      p.events = mask;
      index_[fd] = fds_.size();
      fds_.push_back(p);
    } else {
      fds_[it->second].events = mask;
    }
  }

  void Remove(SocketHandle fd) override {
    auto it = index_.find(fd);
    if (it == index_.end()) return;
    const size_t i = it->second;
    index_.erase(it);
    if (i != fds_.size() - 1) {
      fds_[i] = fds_.back();
      index_[static_cast<SocketHandle>(fds_[i].fd)] = i;
    }
    fds_.pop_back();
  }

  void Wait(int timeout_ms, std::vector<PollEvent>* out) override {
    if (DoPoll(fds_.data(), fds_.size(), timeout_ms) <= 0) return;
    for (const PollFd& p : fds_) {
      if (p.revents == 0) continue;
      uint32_t ev = 0;
      if (p.revents & kIn) ev |= kEvRead;
      if (p.revents & kOut) ev |= kEvWrite;
      if (p.revents & (POLLERR | POLLHUP | POLLNVAL)) ev |= kEvError;
      out->push_back({static_cast<SocketHandle>(p.fd), ev});
    }
  }

  const char* Name() const override { return "poll"; }

 private:
  std::vector<PollFd> fds_;
  std::unordered_map<SocketHandle, size_t> index_;
};

#ifdef __linux__
class EpollPoller : public Poller {
 public:
  EpollPoller() : epfd_(epoll_create1(EPOLL_CLOEXEC)) {}
  ~EpollPoller() override { close(epfd_); }

  void Set(SocketHandle fd, uint32_t events) override {
    epoll_event e{};
    if (events & kEvRead) e.events |= EPOLLIN | EPOLLRDHUP;
    if (events & kEvWrite) e.events |= EPOLLOUT;
    e.data.fd = fd;
    const bool known = registered_.count(fd) != 0;
    epoll_ctl(epfd_, known ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &e);
    registered_[fd] = true;
  }

  void Remove(SocketHandle fd) override {
    if (registered_.erase(fd) == 0) return;
    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
  }

  void Wait(int timeout_ms, std::vector<PollEvent>* out) override {
    epoll_event events[256];
    const int n = epoll_wait(epfd_, events, 256, timeout_ms);
    for (int i = 0; i < n; i++) {
      uint32_t ev = 0;
      if (events[i].events & (EPOLLIN | EPOLLRDHUP)) ev |= kEvRead;
      if (events[i].events & EPOLLOUT) ev |= kEvWrite;
      if (events[i].events & (EPOLLERR | EPOLLHUP)) ev |= kEvError;
      out->push_back({events[i].data.fd, ev});
    }
  }

  const char* Name() const override { return "epoll"; }

 private:
  int epfd_;
  std::unordered_map<int, bool> registered_;
};
#endif

}  // namespace

std::unique_ptr<Poller> Poller::Create(bool force_poll) {
#ifdef __linux__
  if (!force_poll) return std::make_unique<EpollPoller>();
#endif
  (void)force_poll;
  return std::make_unique<PollPoller>();
}

}  // namespace hlb
