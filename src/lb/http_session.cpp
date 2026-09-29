// Layer 7 proxy: one session per client connection, handling a sequence of
// HTTP/1.1 exchanges (keep-alive and pipelining).
//
// Per request:
//  1. Read and validate the full request (bounded by max_body_bytes). The
//     request is buffered so it can be replayed on another backend.
//  2. Route it (host, path prefix, header rules) to a pool, rewrite headers
//     (strip hop-by-hop, add X-Forwarded-*, Via, X-Request-Id).
//  3. Send it over a pooled keep-alive connection to the chosen backend, or a
//     new one. On failure before any response byte arrives, record the
//     failure and retry on a different backend if that is safe: always when
//     the request never reached a backend, otherwise only for idempotent
//     methods.
//  4. Stream the response back as it arrives (bodies are never buffered
//     whole), pausing the backend when the client falls behind.
//  5. Return the backend connection to the pool if the response framing
//     allows reuse, and wait for the client's next request.
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

#include "common/log.h"
#include "lb/worker.h"

namespace hlb {
namespace {

using Clock = std::chrono::steady_clock;

bool IsIdempotent(std::string_view method) {
  return method == "GET" || method == "HEAD" || method == "PUT" || method == "DELETE" || method == "OPTIONS" ||
         method == "TRACE";
}

std::string NewRequestId() {
  thread_local std::mt19937_64 rng(std::random_device{}() ^
                                   static_cast<uint64_t>(Clock::now().time_since_epoch().count()));
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(rng()));
  return buf;
}

class HttpSession : public Session, public std::enable_shared_from_this<HttpSession> {
 public:
  HttpSession(Worker* worker, ListenerRuntime* listener, std::shared_ptr<TcpConn> client)
      : worker_(worker), listener_(listener), cfg_(listener->config), client_(std::move(client)) {}

  void Start() {
    listener_->stats.active++;
    client_->set_on_data([this](TcpConn&) { OnClientData(); });
    client_->set_on_eof([this](TcpConn&) { OnClientEof(); });
    client_->set_on_close([this](TcpConn&) { Finish(); });
    client_->set_on_drain([this](TcpConn&) {
      if (upstream_ && !finished_) upstream_->ResumeReading();
    });
    client_->StartReading();
    ArmTimer(cfg_.idle_timeout_ms, [this] { Finish(); });
  }

  void BeginDrain() override {
    draining_ = true;
    // Idle between requests: nothing in flight, close now.
    if (phase_ == Phase::kReadRequest && req_parser_.state() == HttpParser::State::kHead &&
        client_->input().empty()) {
      CloseClientGracefully();
    }
  }

  void Abort() override { Finish(); }

 private:
  enum class Phase { kReadRequest, kUpstream };

  // ---- client side -------------------------------------------------------

  void OnClientData() {
    if (finished_) return;
    if (phase_ != Phase::kReadRequest) {
      // Pipelined bytes while a request is in flight: keep them queued, but
      // stop reading if the client keeps sending.
      if (client_->input().size() > (64u << 10)) client_->PauseReading();
      return;
    }
    Buffer& in = client_->input();
    while (!finished_ && phase_ == Phase::kReadRequest && !in.empty()) {
      const bool had_head = req_parser_.head_complete();
      const size_t n = req_parser_.Feed(in.view());
      if (had_head) {
        body_.append(in.data(), n);
        if (body_.size() > cfg_.max_body_bytes) {
          in.Consume(n);
          SendLocalError(413, "request body too large\n", /*close=*/true);
          return;
        }
      }
      in.Consume(n);
      if (req_parser_.state() == HttpParser::State::kError) {
        SendLocalError(req_parser_.error_status(), req_parser_.error() + "\n", /*close=*/true);
        return;
      }
      if (!had_head && req_parser_.head_complete() && !OnRequestHead()) return;
      if (req_parser_.done()) {
        ProcessRequest();
        return;
      }
      if (n == 0) break;
    }
  }

  // Validates a just-parsed request head. Returns false if the session already responded.
  bool OnRequestHead() {
    const HttpHead& h = req_parser_.head();
    if (req_parser_.body_kind() == BodyKind::kLength && req_parser_.content_length() > cfg_.max_body_bytes) {
      SendLocalError(413, "request body too large\n", /*close=*/true);
      return false;
    }
    // We buffer the body ourselves, so answer "Expect: 100-continue" here
    // instead of making the client wait for the backend.
    if (h.HasToken("expect", "100-continue") && req_parser_.body_kind() != BodyKind::kNone) {
      client_->Send("HTTP/1.1 100 Continue\r\n\r\n");
    }
    return true;
  }

  void OnClientEof() {
    client_eof_ = true;
    if (phase_ == Phase::kReadRequest) {
      // Clean close between requests, or a truncated request: either way we are done.
      CloseClientGracefully();
    }
    // Mid-exchange: finish sending the response, then close (see CompleteExchange).
  }

  void ProcessRequest() {
    CancelTimer();
    phase_ = Phase::kUpstream;
    listener_->stats.requests++;
    request_start_ = Clock::now();

    HttpHead head = req_parser_.head();
    method_ = head.method;
    client_keep_alive_ = req_parser_.keep_alive() && !draining_;
    client_http10_ = head.version_minor == 0;

    if (listener_->local_handler) {
      const std::string response = listener_->local_handler(head, body_);
      // Local responses carry their own status line; count it for stats.
      int status = 200;
      if (response.size() > 12) status = std::atoi(response.c_str() + 9);
      FinishLocalExchange(status, response);
      return;
    }

    const RouteConfig* route = listener_->router ? listener_->router->Match(head) : nullptr;
    pool_ = route ? listener_->pools[route->pool] : listener_->default_pool;
    if (pool_ == nullptr) {
      SendLocalError(404, "no route for this request\n", false);
      return;
    }

    // Rewrite for the backend.
    StripHopByHop(&head);
    head.Remove("expect");
    const std::string* xff = head.Find("x-forwarded-for");
    head.Set("X-Forwarded-For", xff ? *xff + ", " + client_->peer() : client_->peer());
    if (!head.Find("x-forwarded-proto")) head.Add("X-Forwarded-Proto", "http");
    if (const std::string* host = head.Find("host"); host && !head.Find("x-forwarded-host")) {
      head.Add("X-Forwarded-Host", *host);
    }
    if (!head.Find("x-request-id")) head.Add("X-Request-Id", NewRequestId());
    const std::string* via = head.Find("via");
    head.Set("Via", via ? *via + ", 1.1 hybridlb" : std::string("1.1 hybridlb"));
    if (req_parser_.body_kind() == BodyKind::kNone && head.Find("content-length") == nullptr &&
        (method_ == "POST" || method_ == "PUT" || method_ == "PATCH")) {
      head.Add("Content-Length", "0");
    }
    request_bytes_ = head.SerializeRequest();
    request_bytes_ += body_;

    tried_.clear();
    attempts_ = 0;
    Dispatch();
  }

  // ---- upstream side -----------------------------------------------------

  void Dispatch() {
    backend_ = pool_->Pick(tried_);
    if (!backend_) {
      listener_->stats.upstream_errors++;
      if (tried_.empty()) {
        SendLocalError(503, "no healthy backend available\n", false);
      } else {
        SendLocalError(502, "all backends failed\n", false);
      }
      return;
    }
    attempts_++;
    tried_.push_back(backend_->id());
    backend_->active++;
    counted_active_ = true;
    response_started_ = false;
    request_sent_ = false;

    if (auto conn = worker_->upstreams().Acquire(backend_->id())) {
      upstream_reused_ = true;
      AttachUpstream(std::move(conn));
      SendRequest();
      return;
    }
    upstream_reused_ = false;
    upstream_ = TcpConn::Connect(&worker_->loop(), backend_->endpoint(),
                                 std::chrono::milliseconds(cfg_.connect_timeout_ms), [this](TcpConn& c, bool ok) {
                                   if (finished_ || &c != upstream_.get()) return;
                                   if (!ok) {
                                     UpstreamFailed("connect failed", /*backend_fault=*/true);
                                     return;
                                   }
                                   backend_->connections++;
                                   AttachUpstream(upstream_);
                                   SendRequest();
                                 });
    ArmTimer(cfg_.connect_timeout_ms + cfg_.response_timeout_ms, [this] { UpstreamTimedOut(); });
  }

  void AttachUpstream(std::shared_ptr<TcpConn> conn) {
    upstream_ = std::move(conn);
    upstream_->set_on_data([this](TcpConn&) { OnUpstreamData(); });
    upstream_->set_on_eof([this](TcpConn&) { OnUpstreamEof(); });
    upstream_->set_on_close([this](TcpConn&) { OnUpstreamClosed(); });
    upstream_->set_on_drain(nullptr);
    upstream_->StartReading();
    upstream_->ResumeReading();
    resp_parser_.Reset();
    resp_parser_.set_request_method(method_);
  }

  void SendRequest() {
    request_sent_ = true;
    backend_->bytes_to_backend += request_bytes_.size();
    listener_->stats.bytes_in += request_bytes_.size();
    ArmTimer(cfg_.response_timeout_ms, [this] { UpstreamTimedOut(); });
    upstream_->Send(request_bytes_);
  }

  void OnUpstreamData() {
    if (finished_ || phase_ != Phase::kUpstream) return;
    ArmTimer(cfg_.response_timeout_ms, [this] { UpstreamTimedOut(); });
    Buffer& in = upstream_->input();
    while (!in.empty()) {
      if (!resp_parser_.head_complete()) {
        const size_t n = resp_parser_.Feed(in.view());
        in.Consume(n);
        if (resp_parser_.state() == HttpParser::State::kError) {
          out_.clear();
          if (!response_started_) {
            UpstreamFailed(("bad response: " + resp_parser_.error()).c_str(), true);
          } else {
            AbortExchange();
          }
          return;
        }
        if (!resp_parser_.head_complete()) {
          FlushToClient();  // Need more bytes.
          return;
        }
        if (!OnResponseHead()) continue;  // Interim 1xx: parse the next head.
        if (resp_parser_.done()) {
          FlushToClient();
          CompleteExchange();
          return;
        }
        continue;
      }
      const size_t n = resp_parser_.Feed(in.view());
      if (n > 0) {
        out_.append(in.data(), n);
        backend_->bytes_from_backend += n;
        listener_->stats.bytes_out += n;
        in.Consume(n);
      }
      if (resp_parser_.state() == HttpParser::State::kError) {
        out_.clear();
        AbortExchange();
        return;
      }
      if (resp_parser_.done()) {
        FlushToClient();
        CompleteExchange();
        return;
      }
      if (n == 0) break;
    }
    FlushToClient();
    // Backpressure: stop reading from the backend while the client is behind.
    if (client_->write_blocked()) upstream_->PauseReading();
  }

  // Everything forwarded from one upstream read (head plus body bytes) goes
  // out in a single write: one send() per read instead of one per piece,
  // which matters because the proxy is bound by syscalls, not parsing.
  void FlushToClient() {
    if (out_.empty()) return;
    client_->Send(out_);
    out_.clear();
  }

  // Forwards the response head. Returns false for an interim (1xx) response,
  // after which the final response head follows.
  bool OnResponseHead() {
    HttpHead head = resp_parser_.head();
    if (head.status >= 100 && head.status < 200) {
      // Interim response (e.g. 103 Early Hints): forward and keep waiting.
      if (head.status != 101) out_ += head.SerializeResponse();
      resp_parser_.Reset();
      resp_parser_.set_request_method(method_);
      return false;
    }
    status_ = head.status;
    StripHopByHop(&head);
    // A read-until-close body can only be delimited for the client by closing.
    close_client_after_ = !client_keep_alive_ || client_eof_ || resp_parser_.body_kind() == BodyKind::kUntilClose;
    if (close_client_after_) {
      head.Set("Connection", "close");
    } else if (client_http10_) {
      head.Set("Connection", "keep-alive");
    }
    response_started_ = true;
    const size_t before = out_.size();
    out_ += head.SerializeResponse();
    listener_->stats.bytes_out += out_.size() - before;
    return true;
  }

  void OnUpstreamEof() {
    if (finished_ || phase_ != Phase::kUpstream) return;
    resp_parser_.OnEof();
    if (resp_parser_.done()) {
      CompleteExchange();  // Read-until-close body finished.
    } else if (!response_started_) {
      // A pooled connection the backend had already closed is not the
      // backend's fault; a fresh connection closing on us is.
      UpstreamFailed("backend closed connection before responding", !upstream_reused_);
    } else {
      AbortExchange();
    }
  }

  void OnUpstreamClosed() {
    if (finished_ || phase_ != Phase::kUpstream) return;
    if (!response_started_) {
      UpstreamFailed("backend connection reset", !upstream_reused_);
    } else {
      AbortExchange();
    }
  }

  void UpstreamTimedOut() {
    timer_ = 0;
    if (finished_ || phase_ != Phase::kUpstream) return;
    LOG_WARN("%s %s: backend %s timed out", method_.c_str(), req_parser_.head().target.c_str(),
             backend_->address().ToString().c_str());
    listener_->stats.upstream_errors++;
    backend_->RecordFailure(pool_->passive_config(), NowMs());
    DropUpstream();
    ReleaseBackend();
    if (response_started_) {
      AbortExchange();
      return;
    }
    // A slow backend may still be working on it, so never replay: answer 504.
    SendLocalError(504, "backend timed out\n", false);
  }

  void UpstreamFailed(const char* why, bool backend_fault) {
    CancelTimer();
    // Even on a stale pooled connection the request may have reached the
    // backend, so the idempotency rule below still applies.
    const bool was_sent = request_sent_;
    if (backend_fault) {
      LOG_DEBUG("backend %s failed: %s", backend_->address().ToString().c_str(), why);
      if (backend_->RecordFailure(pool_->passive_config(), NowMs())) {
        LOG_WARN("pool %s: backend %s ejected after repeated failures (%s)", pool_->name().c_str(),
                 backend_->address().ToString().c_str(), why);
      }
    } else {
      // Stale pooled connection: retry the same backend on a fresh connection.
      tried_.pop_back();
      attempts_--;
    }
    DropUpstream();
    ReleaseBackend();
    // Replaying is safe if the backend never got the request, or if repeating
    // it cannot change the outcome.
    const bool may_retry = !was_sent || IsIdempotent(method_);
    if (attempts_ < 1 + cfg_.retries && may_retry) {
      listener_->stats.retries++;
      Dispatch();
      return;
    }
    listener_->stats.upstream_errors++;
    SendLocalError(502, std::string("bad gateway: ") + why + "\n", false);
  }

  void CompleteExchange() {
    CancelTimer();
    const auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - request_start_).count();
    backend_->latency.Record(static_cast<uint64_t>(micros));
    listener_->stats.latency.Record(static_cast<uint64_t>(micros));
    backend_->requests++;
    backend_->RecordSuccess();
    listener_->stats.CountStatus(status_);
    ReleaseBackend();

    const bool reusable = resp_parser_.body_kind() != BodyKind::kUntilClose && resp_parser_.keep_alive() &&
                          upstream_->input().empty() && !upstream_->eof() && !upstream_->closed();
    auto conn = std::move(upstream_);
    upstream_.reset();
    if (reusable) {
      worker_->upstreams().Release(backend_->id(), std::move(conn), pool_->max_idle_per_backend(),
                                   cfg_.upstream_idle_ms);
    } else {
      conn->ClearCallbacks();  // Our own close must not be reported back to us as a failure.
      conn->Close();
    }
    NextRequest();
  }

  // ---- shared helpers ----------------------------------------------------

  void NextRequest() {
    phase_ = Phase::kReadRequest;
    if (close_client_after_ || client_eof_ || draining_) {
      CloseClientGracefully();
      return;
    }
    req_parser_.Reset();
    body_.clear();
    request_bytes_.clear();
    close_client_after_ = false;
    ArmTimer(cfg_.idle_timeout_ms, [this] { Finish(); });
    client_->ResumeReading();
    if (!client_->input().empty()) {
      // A pipelined request is already queued. Go through the loop rather
      // than recursing, so a long pipeline of locally answered requests
      // cannot grow the stack.
      std::weak_ptr<HttpSession> weak = shared_from_this();
      worker_->loop().Post([weak] {
        if (auto self = weak.lock(); self && !self->finished_) self->OnClientData();
      });
    }
  }

  void FinishLocalExchange(int status, const std::string& response) {
    listener_->stats.CountStatus(status);
    close_client_after_ = !client_keep_alive_;
    client_->Send(response);
    NextRequest();
  }

  void SendLocalError(int status, const std::string& message, bool close) {
    if (response_started_) {
      AbortExchange();
      return;
    }
    CancelTimer();
    if (phase_ != Phase::kUpstream) listener_->stats.requests++;
    listener_->stats.CountStatus(status);
    close_client_after_ = close || !client_keep_alive_ || req_parser_.state() == HttpParser::State::kError;
    client_->Send(MakeResponse(status, message, "text/plain", close_client_after_));
    phase_ = Phase::kUpstream;  // So NextRequest resets parser state.
    NextRequest();
  }

  void AbortExchange() {
    // The response was partly sent: there is no way to signal an error in
    // band, so close the client connection to show it was truncated.
    listener_->stats.upstream_errors++;
    if (backend_) backend_->failures++;
    DropUpstream();
    ReleaseBackend();
    Finish();
  }

  void DropUpstream() {
    if (upstream_) {
      auto conn = std::move(upstream_);
      upstream_.reset();
      conn->ClearCallbacks();  // Deliberate close: do not re-enter OnUpstreamClosed.
      conn->Close();
    }
  }

  void ReleaseBackend() {
    if (counted_active_) {
      backend_->active--;
      counted_active_ = false;
    }
  }

  void CloseClientGracefully() {
    if (finished_) return;
    client_->CloseAfterFlush();
    Finish();
  }

  void ArmTimer(int ms, std::function<void()> fn) {
    CancelTimer();
    std::weak_ptr<HttpSession> weak = shared_from_this();
    timer_ = worker_->loop().RunAfter(std::chrono::milliseconds(ms), [weak, fn = std::move(fn)] {
      auto self = weak.lock();
      if (!self || self->finished_) return;
      self->timer_ = 0;
      fn();
    });
  }

  void CancelTimer() {
    if (timer_ != 0) {
      worker_->loop().Cancel(timer_);
      timer_ = 0;
    }
  }

  void Finish() {
    if (finished_) return;
    finished_ = true;
    CancelTimer();
    DropUpstream();
    if (backend_) ReleaseBackend();
    client_->CloseAfterFlush();
    listener_->stats.active--;
    worker_->RemoveSession(this);
  }

  Worker* worker_;
  ListenerRuntime* listener_;
  const ListenerConfig& cfg_;
  std::shared_ptr<TcpConn> client_;
  std::shared_ptr<TcpConn> upstream_;
  std::shared_ptr<Backend> backend_;
  BackendPool* pool_ = nullptr;

  HttpParser req_parser_{HttpParser::Kind::kRequest};
  HttpParser resp_parser_{HttpParser::Kind::kResponse};
  std::string body_;
  std::string request_bytes_;
  std::string out_;  // Response bytes queued during one upstream read.
  std::string method_;
  std::vector<uint64_t> tried_;
  int attempts_ = 0;
  int status_ = 0;
  Clock::time_point request_start_;

  Phase phase_ = Phase::kReadRequest;
  bool client_keep_alive_ = true;
  bool client_http10_ = false;
  bool client_eof_ = false;
  bool close_client_after_ = false;
  bool response_started_ = false;
  bool request_sent_ = false;
  bool upstream_reused_ = false;
  bool counted_active_ = false;
  bool draining_ = false;
  bool finished_ = false;
  EventLoop::TimerId timer_ = 0;
};

}  // namespace

void StartHttpSession(Worker* worker, ListenerRuntime* listener, SocketHandle fd, std::string peer) {
  auto client = TcpConn::Adopt(&worker->loop(), fd, std::move(peer));
  auto session = std::make_shared<HttpSession>(worker, listener, std::move(client));
  worker->AddSession(session);
  session->Start();
}

}  // namespace hlb
