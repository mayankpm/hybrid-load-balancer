#include "net/socket.h"

#include <cerrno>
#include <cstring>
#include <mutex>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <csignal>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace hlb {

#ifdef _WIN32
const SocketHandle kInvalidSocket = static_cast<SocketHandle>(INVALID_SOCKET);
using NativeSocket = SOCKET;
#else
const SocketHandle kInvalidSocket = -1;
using NativeSocket = int;
#endif

namespace {

inline NativeSocket N(SocketHandle s) { return static_cast<NativeSocket>(s); }

bool LastErrorWouldBlock() {
#ifdef _WIN32
  const int e = WSAGetLastError();
  return e == WSAEWOULDBLOCK || e == WSAEINPROGRESS;
#else
  return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS;
#endif
}

bool LastErrorInterrupted() {
#ifdef _WIN32
  return WSAGetLastError() == WSAEINTR;
#else
  return errno == EINTR;
#endif
}

sockaddr_in ToSockaddr(const Endpoint& ep) {
  sockaddr_in sa;
  std::memset(&sa, 0, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = ep.ip_be;
  sa.sin_port = ep.port_be;
  return sa;
}

}  // namespace

bool NetInit() {
  static std::once_flag once;
  static bool ok = false;
  std::call_once(once, [] {
#ifdef _WIN32
    WSADATA data;
    ok = WSAStartup(MAKEWORD(2, 2), &data) == 0;
#else
    std::signal(SIGPIPE, SIG_IGN);
    ok = true;
#endif
  });
  return ok;
}

bool Address::Parse(std::string_view text, Address* out) {
  const size_t colon = text.rfind(':');
  if (colon == std::string_view::npos || colon + 1 >= text.size()) return false;
  unsigned long port = 0;
  for (char c : text.substr(colon + 1)) {
    if (c < '0' || c > '9') return false;
    port = port * 10 + static_cast<unsigned long>(c - '0');
    if (port > 65535) return false;
  }
  out->host = std::string(text.substr(0, colon));
  if (out->host.empty() || out->host == "*") out->host = "0.0.0.0";
  out->port = static_cast<uint16_t>(port);
  return true;
}

bool Resolve(const Address& addr, Endpoint* out) {
  NetInit();
  out->port_be = htons(addr.port);
  in_addr ip{};
  if (inet_pton(AF_INET, addr.host.c_str(), &ip) == 1) {
    out->ip_be = ip.s_addr;
    return true;
  }
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(addr.host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) return false;
  out->ip_be = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr.s_addr;
  freeaddrinfo(res);
  return true;
}

SocketHandle ListenTcp(const Address& addr, int backlog, Address* bound, std::string* error) {
  NetInit();
  Endpoint ep;
  if (!Resolve(addr, &ep)) {
    *error = "cannot resolve " + addr.host;
    return kInvalidSocket;
  }
  const auto s = static_cast<SocketHandle>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
  if (s == kInvalidSocket) {
    *error = "socket() failed";
    return kInvalidSocket;
  }
#ifndef _WIN32
  // Skip TIME_WAIT on restart. (On Windows SO_REUSEADDR would allow two
  // processes to share a port, so it is left off there.)
  int one = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#endif
  sockaddr_in sa = ToSockaddr(ep);
  if (bind(N(s), reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0 || listen(N(s), backlog) != 0) {
    *error = "cannot listen on " + addr.ToString();
    CloseSocket(s);
    return kInvalidSocket;
  }
  sockaddr_in actual{};
  socklen_t len = sizeof(actual);
  getsockname(N(s), reinterpret_cast<sockaddr*>(&actual), &len);
  if (bound != nullptr) {
    char ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &actual.sin_addr, ip, sizeof(ip));
    bound->host = ip;
    bound->port = ntohs(actual.sin_port);
  }
  SetNonBlocking(s, true);
  return s;
}

SocketHandle AcceptTcp(SocketHandle listener, std::string* peer_ip) {
  sockaddr_in peer{};
  socklen_t len = sizeof(peer);
  const auto s = static_cast<SocketHandle>(accept(N(listener), reinterpret_cast<sockaddr*>(&peer), &len));
  if (s == kInvalidSocket) return kInvalidSocket;
  if (peer_ip != nullptr) {
    char ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
    *peer_ip = ip;
  }
  SetNonBlocking(s, true);
  SetNoDelay(s);
  return s;
}

SocketHandle ConnectTcpAsync(const Endpoint& ep, bool* in_progress) {
  NetInit();
  const auto s = static_cast<SocketHandle>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
  if (s == kInvalidSocket) return kInvalidSocket;
  SetNonBlocking(s, true);
  SetNoDelay(s);
  sockaddr_in sa = ToSockaddr(ep);
  *in_progress = false;
  if (connect(N(s), reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
    if (!LastErrorWouldBlock()) {
      CloseSocket(s);
      return kInvalidSocket;
    }
    *in_progress = true;
  }
  return s;
}

int SocketError(SocketHandle s) {
  int err = 0;
  socklen_t len = sizeof(err);
  if (getsockopt(N(s), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &len) != 0) return -1;
  return err;
}

long long ReadSome(SocketHandle s, char* buf, size_t n) {
  while (true) {
#ifdef _WIN32
    const int got = recv(N(s), buf, static_cast<int>(n), 0);
#else
    const ssize_t got = recv(s, buf, n, 0);
#endif
    if (got >= 0) return got;
    if (LastErrorInterrupted()) continue;
    return LastErrorWouldBlock() ? kWouldBlock : kIoError;
  }
}

long long WriteSome(SocketHandle s, const char* buf, size_t n) {
  while (true) {
#ifdef _WIN32
    const int sent = send(N(s), buf, static_cast<int>(n), 0);
#elif defined(MSG_NOSIGNAL)
    const ssize_t sent = send(s, buf, n, MSG_NOSIGNAL);
#else
    const ssize_t sent = send(s, buf, n, 0);
#endif
    if (sent >= 0) return sent;
    if (LastErrorInterrupted()) continue;
    return LastErrorWouldBlock() ? kWouldBlock : kIoError;
  }
}

void SetNonBlocking(SocketHandle s, bool on) {
#ifdef _WIN32
  u_long mode = on ? 1 : 0;
  ioctlsocket(N(s), FIONBIO, &mode);
#else
  const int flags = fcntl(s, F_GETFL, 0);
  fcntl(s, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
#endif
}

void SetNoDelay(SocketHandle s) {
  int one = 1;
  setsockopt(N(s), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
}

void ShutdownWrite(SocketHandle s) {
#ifdef _WIN32
  shutdown(N(s), SD_SEND);
#else
  shutdown(s, SHUT_WR);
#endif
}

void ShutdownBoth(SocketHandle s) {
#ifdef _WIN32
  shutdown(N(s), SD_BOTH);
#else
  shutdown(s, SHUT_RDWR);
#endif
}

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
  closesocket(N(s));
#else
  close(s);
#endif
}

SocketHandle ConnectTcpBlocking(const Address& addr, int timeout_ms) {
  Endpoint ep;
  if (!Resolve(addr, &ep)) return kInvalidSocket;
  bool in_progress = false;
  const SocketHandle s = ConnectTcpAsync(ep, &in_progress);
  if (s == kInvalidSocket) return kInvalidSocket;
  if (in_progress) {
#ifdef _WIN32
    WSAPOLLFD pfd{N(s), POLLWRNORM, 0};
    const int rc = WSAPoll(&pfd, 1, timeout_ms);
#else
    pollfd pfd{s, POLLOUT, 0};
    const int rc = poll(&pfd, 1, timeout_ms);
#endif
    if (rc <= 0 || SocketError(s) != 0) {
      CloseSocket(s);
      return kInvalidSocket;
    }
  }
  SetNonBlocking(s, false);
  return s;
}

bool SetRecvTimeout(SocketHandle s, int timeout_ms) {
#ifdef _WIN32
  DWORD tv = static_cast<DWORD>(timeout_ms);
#else
  timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
#endif
  return setsockopt(N(s), SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv)) == 0;
}

bool SendAll(SocketHandle s, std::string_view data) {
  while (!data.empty()) {
    const long long n = WriteSome(s, data.data(), data.size());
    if (n <= 0) return false;
    data.remove_prefix(static_cast<size_t>(n));
  }
  return true;
}

long long RecvSome(SocketHandle s, char* buf, size_t n) {
  const long long got = ReadSome(s, buf, n);
  return got == kWouldBlock ? kIoError : got;
}

}  // namespace hlb
