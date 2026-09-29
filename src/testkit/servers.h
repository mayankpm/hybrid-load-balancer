// Blocking, thread-per-connection backend servers for tests, the benchmark
// and the demo. Deliberately simple and independent of the proxy's event
// loop, so a bug in the proxy cannot hide behind the same bug in the backend.
#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "http/http.h"
#include "net/socket.h"

namespace hlb::testkit {

class ServerBase {
 public:
  virtual ~ServerBase();
  // Binds host:port (port 0 = ephemeral) and starts accepting.
  bool Start(const std::string& host = "127.0.0.1", uint16_t port = 0);
  // Simulates a crash: closes the listener and resets every open connection.
  void Kill();
  // Starts again on the same port after Kill().
  bool Restart();

  Address address() const { return bound_; }
  uint64_t connections() const { return connections_; }

 protected:
  virtual void Serve(SocketHandle s) = 0;

 private:
  struct Conn {
    std::thread thread;
    std::atomic<bool> done{false};
  };
  void AcceptLoop();
  void Reap(bool all);

  Address bound_;
  SocketHandle listener_ = kInvalidSocket;
  std::thread accept_thread_;
  std::atomic<bool> running_{false};
  std::mutex mu_;
  std::set<SocketHandle> open_;
  std::list<std::unique_ptr<Conn>> conns_;
  std::atomic<uint64_t> connections_{0};

  friend class ConnGuard;
};

// HTTP backend. Every response carries "X-Backend: <id>". Endpoints:
//   /health               200, or 503 after SetHealthy(false)
//   /echo                 request line, selected headers and body echoed back
//   /delay/<ms>           responds after sleeping
//   /size/<n>             n-byte body with Content-Length
//   /chunked/<n>          n chunks, chunked transfer encoding
//   /until-close/<n>      n bytes, no length: ends by closing the connection
//   /status/<code>        empty response with that status
//   /close                responds with Connection: close
//   anything else         "backend=<id> path=<target>"
class HttpServer : public ServerBase {
 public:
  explicit HttpServer(std::string id) : id_(std::move(id)) {}
  ~HttpServer() override { Kill(); }
  const std::string& id() const { return id_; }
  void SetHealthy(bool healthy) { healthy_ = healthy; }
  void SetExtraDelayMs(int ms) { delay_ms_ = ms; }
  uint64_t requests() const { return requests_; }

 protected:
  void Serve(SocketHandle s) override;

 private:
  std::string Handle(const HttpHead& req, const std::string& body, bool* close);

  std::string id_;
  std::atomic<bool> healthy_{true};
  std::atomic<int> delay_ms_{0};
  std::atomic<uint64_t> requests_{0};
};

// TCP backend: sends "<id>\n" on connect, then echoes everything back and
// mirrors half-close (FIN in, FIN out).
class EchoServer : public ServerBase {
 public:
  explicit EchoServer(std::string id) : id_(std::move(id)) {}
  ~EchoServer() override { Kill(); }
  const std::string& id() const { return id_; }

 protected:
  void Serve(SocketHandle s) override;

 private:
  std::string id_;
};

// Decodes a raw chunked body into its payload (tests only).
std::string DecodeChunked(std::string_view raw);

struct HttpResult {
  bool ok = false;
  int status = 0;
  HttpHead head;
  std::string body;  // Decoded.
  std::string error;
  const std::string* Header(std::string_view name) const { return head.Find(name); }
};

// Blocking HTTP/1.1 client with keep-alive. Like real clients, it reconnects
// once if a reused connection turns out to be closed before any response.
class HttpClient {
 public:
  explicit HttpClient(Address addr, int timeout_ms = 10000) : addr_(std::move(addr)), timeout_ms_(timeout_ms) {}
  ~HttpClient() { Close(); }
  HttpResult Request(std::string_view method, std::string_view target, std::string_view body = "",
                     const std::vector<Header>& headers = {});
  HttpResult Get(std::string_view target) { return Request("GET", target); }
  // Sends raw bytes and reads one response (for malformed-request tests).
  HttpResult Raw(std::string_view bytes, std::string_view method = "GET");
  void Close();
  int connects() const { return connects_; }

 private:
  bool EnsureConnected();
  HttpResult ReadResponse(std::string_view method);

  Address addr_;
  int timeout_ms_;
  SocketHandle fd_ = kInvalidSocket;
  std::string pending_;
  int connects_ = 0;
};

}  // namespace hlb::testkit
