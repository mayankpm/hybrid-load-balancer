// Portable socket layer over Winsock and BSD sockets. The proxy data path
// uses the non-blocking calls; the blocking helpers exist for health checks,
// tests and the benchmark client.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hlb {

#ifdef _WIN32
using SocketHandle = uintptr_t;
#else
using SocketHandle = int;
#endif

extern const SocketHandle kInvalidSocket;

// Must run before any other socket call (idempotent). Also ignores SIGPIPE on
// POSIX so a write to a closed peer fails with an error instead of killing us.
bool NetInit();

struct Address {
  std::string host;
  uint16_t port = 0;

  static bool Parse(std::string_view text, Address* out);  // "host:port"
  std::string ToString() const { return host + ":" + std::to_string(port); }
  bool operator==(const Address& o) const { return host == o.host && port == o.port; }
};

// Resolved IPv4 endpoint, cached so connects on the hot path skip DNS.
struct Endpoint {
  uint32_t ip_be = 0;    // Network byte order.
  uint16_t port_be = 0;  // Network byte order.
};
bool Resolve(const Address& addr, Endpoint* out);

// Result codes for non-blocking reads and writes.
constexpr long long kWouldBlock = -1;
constexpr long long kIoError = -2;

// Non-blocking listener. Pass port 0 for an ephemeral port; `bound` receives the real one.
SocketHandle ListenTcp(const Address& addr, int backlog, Address* bound, std::string* error);
// Non-blocking accept. Returns kInvalidSocket when nothing is pending.
SocketHandle AcceptTcp(SocketHandle listener, std::string* peer_ip);
// Starts a non-blocking connect. `*in_progress` is set when completion must be
// awaited (the socket becomes writable); otherwise the connect finished at once.
SocketHandle ConnectTcpAsync(const Endpoint& ep, bool* in_progress);
// Pending error on a socket (0 if none); used to finish a non-blocking connect.
int SocketError(SocketHandle s);

// >0 bytes transferred, 0 on EOF (reads only), kWouldBlock, or kIoError.
long long ReadSome(SocketHandle s, char* buf, size_t n);
long long WriteSome(SocketHandle s, const char* buf, size_t n);

void SetNonBlocking(SocketHandle s, bool on);
void SetNoDelay(SocketHandle s);
void ShutdownWrite(SocketHandle s);
void ShutdownBoth(SocketHandle s);
void CloseSocket(SocketHandle s);

// Blocking helpers.
SocketHandle ConnectTcpBlocking(const Address& addr, int timeout_ms);
bool SetRecvTimeout(SocketHandle s, int timeout_ms);
bool SendAll(SocketHandle s, std::string_view data);
// Blocking read: >0 bytes, 0 on EOF, negative on error or timeout.
long long RecvSome(SocketHandle s, char* buf, size_t n);

}  // namespace hlb
