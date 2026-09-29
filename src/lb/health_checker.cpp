#include "lb/health_checker.h"

#include <future>

#include "common/log.h"
#include "http/http.h"

namespace hlb {

void HealthChecker::Start() {
  thread_ = std::thread([this] { Loop(); });
}

void HealthChecker::Stop() {
  {
    std::lock_guard lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

bool HealthChecker::Probe(const Backend& backend, const HealthCheckConfig& cfg) {
  const SocketHandle s = ConnectTcpBlocking(backend.address(), cfg.timeout_ms);
  if (s == kInvalidSocket) return false;
  if (cfg.type == HealthCheckConfig::Type::kTcp) {
    CloseSocket(s);
    return true;
  }
  SetRecvTimeout(s, cfg.timeout_ms);
  HttpHead req;
  req.method = "GET";
  req.target = cfg.path;
  req.Add("Host", backend.address().ToString());
  req.Add("User-Agent", "hybridlb-health-check");
  req.Add("Connection", "close");
  bool ok = SendAll(s, req.SerializeRequest());
  HttpParser parser(HttpParser::Kind::kResponse);
  char buf[4096];
  while (ok && !parser.head_complete()) {
    const long long n = RecvSome(s, buf, sizeof(buf));
    if (n <= 0) {
      ok = false;
      break;
    }
    std::string_view data(buf, static_cast<size_t>(n));
    while (!data.empty() && !parser.head_complete() && parser.state() != HttpParser::State::kError) {
      data.remove_prefix(parser.Feed(data));
    }
    if (parser.state() == HttpParser::State::kError) ok = false;
  }
  CloseSocket(s);
  return ok && parser.head().status >= 200 && parser.head().status < 400;
}

void HealthChecker::CheckPool(BackendPool* pool) {
  const HealthCheckConfig& cfg = pool->health_config();
  const auto backends = pool->Snapshot();
  std::vector<std::future<bool>> results;
  results.reserve(backends.size());
  for (const auto& b : backends) {
    results.push_back(std::async(std::launch::async, [b, &cfg] { return Probe(*b, cfg); }));
  }
  for (size_t i = 0; i < backends.size(); i++) {
    Backend& b = *backends[i];
    if (results[i].get()) {
      b.hc_failures = 0;
      if (++b.hc_successes >= cfg.rise && !b.healthy) {
        b.healthy = true;
        b.consecutive_failures = 0;
        b.ejected_until_ms = 0;
        LOG_INFO("pool %s: backend %s is healthy again", pool->name().c_str(), b.address().ToString().c_str());
      }
    } else {
      b.hc_successes = 0;
      if (++b.hc_failures >= cfg.fall && b.healthy) {
        b.healthy = false;
        LOG_WARN("pool %s: backend %s failed %d health checks, marked down", pool->name().c_str(),
                 b.address().ToString().c_str(), b.hc_failures);
      }
    }
  }
}

void HealthChecker::Loop() {
  std::unique_lock lock(mu_);
  while (!stop_) {
    const int64_t now = NowMs();
    for (BackendPool* pool : pools_) {
      pool->SweepDrained();
      if (pool->health_config().type == HealthCheckConfig::Type::kNone) continue;
      int64_t& next = next_check_ms_[pool];
      if (now < next) continue;
      next = now + pool->health_config().interval_ms;
      lock.unlock();
      CheckPool(pool);
      lock.lock();
      if (stop_) return;
    }
    cv_.wait_for(lock, std::chrono::milliseconds(50));
  }
}

}  // namespace hlb
