#include "testkit/servers.h"

#include <chrono>
#include <cstdlib>

namespace hlb::testkit {

// ---------------------------------------------------------------------------
// ServerBase

ServerBase::~ServerBase() = default;

bool ServerBase::Start(const std::string& host, uint16_t port) {
  std::string err;
  listener_ = ListenTcp(Address{host, port}, 256, &bound_, &err);
  if (listener_ == kInvalidSocket) return false;
  SetNonBlocking(listener_, false);
  running_ = true;
  accept_thread_ = std::thread([this] { AcceptLoop(); });
  return true;
}

bool ServerBase::Restart() {
  for (int i = 0; i < 50; i++) {
    if (Start(bound_.host, bound_.port)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

void ServerBase::AcceptLoop() {
  while (running_) {
    const SocketHandle s = AcceptTcp(listener_, nullptr);
    if (s == kInvalidSocket) {
      if (!running_) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    SetNonBlocking(s, false);
    connections_++;
    std::lock_guard lock(mu_);
    if (!running_) {
      CloseSocket(s);
      break;
    }
    Reap(false);
    open_.insert(s);
    auto c = std::make_unique<Conn>();
    Conn* raw = c.get();
    c->thread = std::thread([this, s, raw] {
      Serve(s);
      {
        std::lock_guard l(mu_);
        if (open_.erase(s)) CloseSocket(s);
      }
      raw->done = true;
    });
    conns_.push_back(std::move(c));
  }
}

void ServerBase::Reap(bool all) {
  for (auto it = conns_.begin(); it != conns_.end();) {
    if (all || (*it)->done) {
      if ((*it)->thread.joinable()) (*it)->thread.join();
      it = conns_.erase(it);
    } else {
      ++it;
    }
  }
}

void ServerBase::Kill() {
  if (!running_.exchange(false)) return;
  ShutdownBoth(listener_);
  CloseSocket(listener_);
  // Wake a blocked accept() by connecting to ourselves.
  const SocketHandle poke = ConnectTcpBlocking(bound_, 200);
  if (poke != kInvalidSocket) CloseSocket(poke);
  accept_thread_.join();
  std::list<std::unique_ptr<Conn>> conns;
  {
    std::lock_guard lock(mu_);
    for (SocketHandle s : open_) ShutdownBoth(s);  // Unblocks the serving threads.
    conns.swap(conns_);
  }
  for (auto& c : conns) c->thread.join();
  std::lock_guard lock(mu_);
  for (SocketHandle s : open_) CloseSocket(s);
  open_.clear();
}

// ---------------------------------------------------------------------------
// HttpServer

namespace {

int NumberAfter(std::string_view target, std::string_view prefix) {
  return std::atoi(std::string(target.substr(prefix.size())).c_str());
}

}  // namespace

std::string HttpServer::Handle(const HttpHead& req, const std::string& body, bool* close) {
  requests_++;
  if (const int d = delay_ms_.load(); d > 0) std::this_thread::sleep_for(std::chrono::milliseconds(d));
  const std::string_view t = req.target;
  std::vector<Header> extra = {{"X-Backend", id_}};
  auto respond = [&](int status, std::string_view b) {
    return MakeResponse(status, b, "text/plain", *close, extra);
  };

  if (t == "/health") return respond(healthy_ ? 200 : 503, healthy_ ? "ok" : "unhealthy");
  if (t == "/close") {
    *close = true;
    return respond(200, "closing");
  }
  if (t.substr(0, 7) == "/delay/") {
    std::this_thread::sleep_for(std::chrono::milliseconds(NumberAfter(t, "/delay/")));
    return respond(200, "delayed");
  }
  if (t.substr(0, 6) == "/size/") return respond(200, std::string(static_cast<size_t>(NumberAfter(t, "/size/")), 'x'));
  if (t.substr(0, 8) == "/status/") return respond(NumberAfter(t, "/status/"), "");
  if (t.substr(0, 9) == "/chunked/") {
    const int n = NumberAfter(t, "/chunked/");
    HttpHead h;
    h.status = 200;
    h.Add("Transfer-Encoding", "chunked");
    h.Add("X-Backend", id_);
    std::string out = h.SerializeResponse();
    for (int i = 0; i < n; i++) {
      const std::string chunk = "chunk-" + std::to_string(i) + ";";
      char size[16];
      std::snprintf(size, sizeof(size), "%zx", chunk.size());
      out += std::string(size) + "\r\n" + chunk + "\r\n";
    }
    out += "0\r\nX-Trailer: done\r\n\r\n";
    return out;
  }
  if (t.substr(0, 13) == "/until-close/") {
    *close = true;
    HttpHead h;
    h.status = 200;
    h.Add("X-Backend", id_);
    h.Add("Connection", "close");
    return h.SerializeResponse() + std::string(static_cast<size_t>(NumberAfter(t, "/until-close/")), 'u');
  }
  if (t.substr(0, 5) == "/echo") {
    std::string out = "backend=" + id_ + "\nmethod=" + req.method + "\ntarget=" + req.target + "\n";
    for (const char* name : {"x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "x-request-id", "via",
                             "connection", "keep-alive", "x-secret-hop", "host", "x-client-id"}) {
      if (const std::string* v = req.Find(name)) out += std::string(name) + "=" + *v + "\n";
    }
    out += "body=" + body;
    return respond(200, out);
  }
  return respond(200, "backend=" + id_ + " path=" + req.target);
}

void HttpServer::Serve(SocketHandle s) {
  HttpParser parser(HttpParser::Kind::kRequest);
  std::string pending;
  std::string body;
  char buf[16384];
  while (true) {
    // Parse as much as possible from what we already have.
    while (!pending.empty()) {
      const bool had_head = parser.head_complete();
      const size_t n = parser.Feed(pending);
      if (had_head) body.append(pending, 0, n);
      pending.erase(0, n);
      if (parser.state() == HttpParser::State::kError) {
        SendAll(s, MakeResponse(400, "bad request", "text/plain", true));
        return;
      }
      if (parser.done()) {
        std::string raw_body = parser.body_kind() == BodyKind::kChunked ? DecodeChunked(body) : body;
        bool close = !parser.keep_alive();
        const std::string response = Handle(parser.head(), raw_body, &close);
        if (!SendAll(s, response) || close) return;
        parser.Reset();
        body.clear();
        continue;
      }
      if (n == 0) break;
    }
    const long long n = RecvSome(s, buf, sizeof(buf));
    if (n <= 0) return;
    pending.append(buf, static_cast<size_t>(n));
  }
}

// ---------------------------------------------------------------------------
// EchoServer

void EchoServer::Serve(SocketHandle s) {
  if (!SendAll(s, id_ + "\n")) return;
  char buf[65536];
  while (true) {
    const long long n = RecvSome(s, buf, sizeof(buf));
    if (n <= 0) {
      if (n == 0) ShutdownWrite(s);  // Mirror the client's half-close.
      return;
    }
    if (!SendAll(s, std::string_view(buf, static_cast<size_t>(n)))) return;
  }
}

// ---------------------------------------------------------------------------
// Client helpers

std::string DecodeChunked(std::string_view raw) {
  std::string out;
  while (!raw.empty()) {
    const size_t eol = raw.find("\r\n");
    if (eol == std::string_view::npos) break;
    const size_t size = std::strtoul(std::string(raw.substr(0, eol)).c_str(), nullptr, 16);
    raw.remove_prefix(eol + 2);
    if (size == 0) break;
    out.append(raw.substr(0, size));
    raw.remove_prefix(std::min(raw.size(), size + 2));
  }
  return out;
}

void HttpClient::Close() {
  if (fd_ != kInvalidSocket) CloseSocket(fd_);
  fd_ = kInvalidSocket;
  pending_.clear();
}

bool HttpClient::EnsureConnected() {
  if (fd_ != kInvalidSocket) return true;
  fd_ = ConnectTcpBlocking(addr_, timeout_ms_);
  if (fd_ == kInvalidSocket) return false;
  SetRecvTimeout(fd_, timeout_ms_);
  connects_++;
  return true;
}

HttpResult HttpClient::Request(std::string_view method, std::string_view target, std::string_view body,
                               const std::vector<Header>& headers) {
  HttpHead h;
  h.method = std::string(method);
  h.target = std::string(target);
  for (const Header& x : headers) h.Add(x.name, x.value);
  if (!h.Find("host")) h.Add("Host", addr_.ToString());
  if (!body.empty() || method == "POST" || method == "PUT") h.Add("Content-Length", std::to_string(body.size()));
  std::string bytes = h.SerializeRequest();
  bytes.append(body.data(), body.size());

  for (int attempt = 0; attempt < 2; attempt++) {
    const bool reused = fd_ != kInvalidSocket;
    if (!EnsureConnected()) return HttpResult{false, 0, {}, {}, "connect failed"};
    if (!SendAll(fd_, bytes)) {
      Close();
      if (reused) continue;
      return HttpResult{false, 0, {}, {}, "send failed"};
    }
    HttpResult r = ReadResponse(method);
    if (!r.ok && r.status == 0 && reused && r.error == "closed before response") {
      Close();
      continue;  // Stale keep-alive connection: retry once on a new one.
    }
    return r;
  }
  return HttpResult{false, 0, {}, {}, "retry failed"};
}

HttpResult HttpClient::Raw(std::string_view bytes, std::string_view method) {
  if (!EnsureConnected()) return HttpResult{false, 0, {}, {}, "connect failed"};
  if (!SendAll(fd_, bytes)) return HttpResult{false, 0, {}, {}, "send failed"};
  return ReadResponse(method);
}

HttpResult HttpClient::ReadResponse(std::string_view method) {
  HttpResult r;
  HttpParser parser(HttpParser::Kind::kResponse);
  parser.set_request_method(method);
  std::string raw_body;
  bool got_any = false;
  char buf[65536];
  while (true) {
    while (!pending_.empty() && !parser.done()) {
      const bool had_head = parser.head_complete();
      const size_t n = parser.Feed(pending_);
      if (had_head) raw_body.append(pending_, 0, n);
      pending_.erase(0, n);
      if (parser.state() == HttpParser::State::kError) {
        r.error = "parse error: " + parser.error();
        Close();
        return r;
      }
      if (parser.head_complete() && parser.head().status >= 100 && parser.head().status < 200) {
        parser.Reset();  // Skip interim responses.
        parser.set_request_method(method);
        continue;
      }
      if (n == 0) break;
    }
    if (parser.done()) break;
    const long long n = RecvSome(fd_, buf, sizeof(buf));
    if (n <= 0) {
      parser.OnEof();
      if (parser.done()) break;
      r.error = got_any ? "connection closed mid-response" : "closed before response";
      Close();
      return r;
    }
    got_any = true;
    pending_.append(buf, static_cast<size_t>(n));
  }
  r.ok = true;
  r.head = parser.head();
  r.status = r.head.status;
  r.body = parser.body_kind() == BodyKind::kChunked ? DecodeChunked(raw_body) : raw_body;
  if (!parser.keep_alive() || parser.body_kind() == BodyKind::kUntilClose) Close();
  return r;
}

}  // namespace hlb::testkit
