#include "lb/load_balancer.h"

#include <cstdio>
#include <future>
#include <sstream>
#include <thread>

#include "common/log.h"

namespace hlb {
namespace {

std::string JsonEscape(std::string_view s) {
  std::string out;
  for (char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

std::string LatencyJson(const LatencyHistogram& h) {
  char buf[160];
  std::snprintf(buf, sizeof(buf), "{\"count\":%llu,\"mean\":%.0f,\"p50\":%llu,\"p90\":%llu,\"p99\":%llu}",
                static_cast<unsigned long long>(h.Count()), h.MeanMicros(),
                static_cast<unsigned long long>(h.Percentile(0.50)),
                static_cast<unsigned long long>(h.Percentile(0.90)),
                static_cast<unsigned long long>(h.Percentile(0.99)));
  return buf;
}

std::vector<std::string> SplitPath(std::string_view path) {
  std::vector<std::string> parts;
  while (!path.empty()) {
    if (path[0] == '/') {
      path.remove_prefix(1);
      continue;
    }
    const size_t slash = path.find('/');
    parts.emplace_back(path.substr(0, slash));
    if (slash == std::string_view::npos) break;
    path.remove_prefix(slash);
  }
  return parts;
}

std::string QueryParam(std::string_view query, std::string_view key) {
  while (!query.empty()) {
    const size_t amp = query.find('&');
    const std::string_view kv = query.substr(0, amp);
    const size_t eq = kv.find('=');
    if (kv.substr(0, eq) == key) return eq == std::string_view::npos ? "" : std::string(kv.substr(eq + 1));
    if (amp == std::string_view::npos) break;
    query.remove_prefix(amp + 1);
  }
  return "";
}

std::string JsonResponse(int status, const std::string& json) {
  return MakeResponse(status, json + "\n", "application/json");
}

}  // namespace

LoadBalancer::LoadBalancer(Config config) : config_(std::move(config)) { NetInit(); }

LoadBalancer::~LoadBalancer() { Stop(); }

bool LoadBalancer::Start(std::string* error) {
  if (running_) return true;
  if (!config_.Validate(error)) return false;
  started_at_ = std::chrono::steady_clock::now();

  for (const PoolConfig& pc : config_.pools) {
    auto pool = std::make_unique<BackendPool>(pc.name, pc.algorithm, pc.health, pc.passive);
    pool->set_max_idle_per_backend(static_cast<size_t>(pc.max_idle_per_backend));
    for (const BackendConfig& bc : pc.backends) {
      if (!pool->AddBackend(bc.address, bc.weight, error)) return false;
    }
    pools_[pc.name] = std::move(pool);
  }

  int n = config_.threads > 0 ? config_.threads : static_cast<int>(std::thread::hardware_concurrency());
  if (n <= 0) n = 1;
  for (int i = 0; i < n; i++) {
    workers_.push_back(std::make_unique<Worker>(i, config_.force_poll));
    workers_.back()->Start();
  }
  running_ = true;

  for (const ListenerConfig& lc : config_.listeners) {
    if (!StartListener(lc, nullptr, error)) {
      Stop();
      return false;
    }
  }
  if (config_.admin) {
    ListenerConfig admin;
    admin.name = "admin";
    admin.mode = ListenerConfig::Mode::kHttp;
    admin.bind = *config_.admin;
    if (!StartListener(admin, [this](const HttpHead& h, const std::string& b) { return HandleAdmin(h, b); },
                       error)) {
      Stop();
      return false;
    }
  }

  std::vector<BackendPool*> pools;
  for (auto& [name, p] : pools_) pools.push_back(p.get());
  health_ = std::make_unique<HealthChecker>(std::move(pools));
  health_->Start();

  for (const auto& l : listeners_) {
    LOG_INFO("listener %s (%s) on %s", l->config.name.c_str(),
             l->config.mode == ListenerConfig::Mode::kTcp ? "tcp" : "http", l->bound.ToString().c_str());
  }
  LOG_INFO("started %d worker threads using %s", n, workers_[0]->loop().poller_name());
  return true;
}

bool LoadBalancer::StartListener(const ListenerConfig& cfg, LocalHandler handler, std::string* error) {
  auto l = std::make_unique<ListenerRuntime>();
  l->config = cfg;
  l->local_handler = std::move(handler);
  l->fd = ListenTcp(cfg.bind, 1024, &l->bound, error);
  if (l->fd == kInvalidSocket) return false;
  if (!cfg.pool.empty()) l->default_pool = pools_.at(cfg.pool).get();
  if (!cfg.routes.empty()) {
    l->router = std::make_unique<Router>(cfg.routes);
    for (const RouteConfig& r : cfg.routes) l->pools[r.pool] = pools_.at(r.pool).get();
  }
  ListenerRuntime* raw = l.get();
  listeners_.push_back(std::move(l));
  RunOnWorker0([this, raw] {
    auto acceptor = std::make_unique<Acceptor>(&workers_[0]->loop(), raw->fd, [this, raw](SocketHandle fd, std::string peer) {
      OnAccept(raw, fd, std::move(peer));
    });
    acceptor->Start();
    acceptors_.push_back(std::move(acceptor));
  });
  return true;
}

void LoadBalancer::RunOnWorker0(std::function<void()> fn) {
  std::promise<void> done;
  workers_[0]->loop().Post([&] {
    fn();
    done.set_value();
  });
  done.get_future().wait();
}

void LoadBalancer::OnAccept(ListenerRuntime* listener, SocketHandle fd, std::string peer) {
  listener->stats.accepted++;
  Worker* w = workers_[listener->next_worker.fetch_add(1) % workers_.size()].get();
  const bool tcp = listener->config.mode == ListenerConfig::Mode::kTcp;
  w->loop().Post([w, listener, fd, peer = std::move(peer), tcp]() mutable {
    if (tcp) {
      StartTcpSession(w, listener, fd, std::move(peer));
    } else {
      StartHttpSession(w, listener, fd, std::move(peer));
    }
  });
}

void LoadBalancer::Stop() {
  if (!running_) return;
  running_ = false;
  // 1. Stop accepting.
  RunOnWorker0([this] { acceptors_.clear(); });
  // 2. Ask every session to finish its current exchange and close.
  for (auto& w : workers_) {
    Worker* raw = w.get();
    raw->loop().Post([raw] { raw->DrainSessions(); });
  }
  // 3. Wait for in-flight work, bounded by the shutdown timeout.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.shutdown_timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    int64_t active = 0;
    for (const auto& l : listeners_) {
      // Keep-alive admin connections should not hold up shutdown.
      if (!l->local_handler) active += l->stats.active.load();
    }
    if (active == 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (health_) health_->Stop();
  // 4. Abort whatever is left and stop the loops.
  for (auto& w : workers_) w->Stop();
  workers_.clear();
  LOG_INFO("stopped");
}

Address LoadBalancer::listener_address(std::string_view name) const {
  for (const auto& l : listeners_) {
    if (l->config.name == name) return l->bound;
  }
  return {};
}

Address LoadBalancer::admin_address() const { return listener_address("admin"); }

BackendPool* LoadBalancer::pool(std::string_view name) {
  auto it = pools_.find(name);
  return it == pools_.end() ? nullptr : it->second.get();
}

const ListenerStats* LoadBalancer::listener_stats(std::string_view name) const {
  for (const auto& l : listeners_) {
    if (l->config.name == name) return &l->stats;
  }
  return nullptr;
}

std::vector<double> LoadBalancer::worker_utilization() const {
  std::vector<double> out;
  for (const auto& w : workers_) {
    const double busy = static_cast<double>(w->loop().busy_ns());
    const double total = busy + static_cast<double>(w->loop().idle_ns());
    out.push_back(total > 0 ? busy / total : 0.0);
  }
  return out;
}

uint64_t LoadBalancer::upstream_reuses() const {
  uint64_t n = 0;
  for (const auto& w : workers_) n += w->upstreams().reused();
  return n;
}

std::string LoadBalancer::StatsJson() const {
  std::ostringstream o;
  const auto uptime =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - started_at_).count();
  o << "{\"uptime_s\":" << uptime << ",\"workers\":" << workers_.size() << ",\"poller\":\""
    << (workers_.empty() ? "" : workers_[0]->loop().poller_name()) << "\",\"upstream_reuses\":" << upstream_reuses()
    << ",\"worker_utilization\":[";
  const auto util = worker_utilization();
  for (size_t i = 0; i < util.size(); i++) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%s%.3f", i ? "," : "", util[i]);
    o << buf;
  }
  o << "],\"listeners\":[";
  bool first = true;
  for (const auto& l : listeners_) {
    const ListenerStats& s = l->stats;
    o << (first ? "" : ",") << "{\"name\":\"" << JsonEscape(l->config.name) << "\",\"mode\":\""
      << (l->config.mode == ListenerConfig::Mode::kTcp ? "tcp" : "http") << "\",\"bind\":\""
      << l->bound.ToString() << "\",\"accepted\":" << s.accepted << ",\"active\":" << s.active
      << ",\"requests\":" << s.requests << ",\"responses\":{\"2xx\":" << s.responses_2xx
      << ",\"3xx\":" << s.responses_3xx << ",\"4xx\":" << s.responses_4xx << ",\"5xx\":" << s.responses_5xx
      << "},\"retries\":" << s.retries << ",\"upstream_errors\":" << s.upstream_errors
      << ",\"bytes_in\":" << s.bytes_in << ",\"bytes_out\":" << s.bytes_out
      << ",\"latency_us\":" << LatencyJson(s.latency) << "}";
    first = false;
  }
  o << "],\"pools\":[";
  first = true;
  const int64_t now = NowMs();
  for (const auto& [name, pool] : pools_) {
    o << (first ? "" : ",") << "{\"name\":\"" << JsonEscape(name) << "\",\"algorithm\":\""
      << AlgorithmName(pool->algorithm()) << "\",\"backends\":[";
    bool bfirst = true;
    for (const auto& b : pool->Snapshot()) {
      o << (bfirst ? "" : ",") << "{\"address\":\"" << b->address().ToString() << "\",\"weight\":" << b->weight()
        << ",\"healthy\":" << (b->healthy ? "true" : "false") << ",\"draining\":" << (b->draining ? "true" : "false")
        << ",\"ejected\":" << (now < b->ejected_until_ms ? "true" : "false") << ",\"active\":" << b->active
        << ",\"requests\":" << b->requests << ",\"failures\":" << b->failures
        << ",\"connections\":" << b->connections << ",\"ejections\":" << b->ejections
        << ",\"latency_us\":" << LatencyJson(b->latency) << "}";
      bfirst = false;
    }
    o << "]}";
    first = false;
  }
  o << "]}";
  return o.str();
}

std::string LoadBalancer::HandleAdmin(const HttpHead& req, const std::string& /*body*/) {
  std::string_view target = req.target;
  std::string_view query;
  if (const size_t q = target.find('?'); q != std::string_view::npos) {
    query = target.substr(q + 1);
    target = target.substr(0, q);
  }
  const auto parts = SplitPath(target);

  if (req.method == "GET" && parts.size() == 1 && parts[0] == "stats") return JsonResponse(200, StatsJson());
  if (req.method == "GET" && parts.size() == 1 && parts[0] == "healthz") return MakeResponse(200, "ok\n");

  if (parts.size() >= 3 && parts[0] == "pools" && parts[2] == "backends") {
    BackendPool* p = pool(parts[1]);
    if (!p) return JsonResponse(404, "{\"error\":\"unknown pool\"}");

    if (req.method == "POST" && parts.size() == 3) {
      Address addr;
      if (!Address::Parse(QueryParam(query, "address"), &addr)) {
        return JsonResponse(400, "{\"error\":\"address=host:port required\"}");
      }
      const std::string w = QueryParam(query, "weight");
      const int weight = w.empty() ? 1 : std::max(1, std::atoi(w.c_str()));
      std::string err;
      if (!p->AddBackend(addr, weight, &err)) return JsonResponse(409, "{\"error\":\"" + JsonEscape(err) + "\"}");
      LOG_INFO("admin: added backend %s to pool %s", addr.ToString().c_str(), p->name().c_str());
      return JsonResponse(201, "{\"added\":\"" + addr.ToString() + "\"}");
    }

    Address addr;
    if (parts.size() >= 4 && Address::Parse(parts[3], &addr)) {
      if (req.method == "POST" && parts.size() == 5 && parts[4] == "drain") {
        if (!p->DrainBackend(addr)) return JsonResponse(404, "{\"error\":\"unknown backend\"}");
        LOG_INFO("admin: draining backend %s in pool %s", addr.ToString().c_str(), p->name().c_str());
        return JsonResponse(202, "{\"draining\":\"" + addr.ToString() + "\"}");
      }
      if (req.method == "DELETE" && parts.size() == 4) {
        if (!p->RemoveBackend(addr)) return JsonResponse(404, "{\"error\":\"unknown backend\"}");
        LOG_INFO("admin: removed backend %s from pool %s", addr.ToString().c_str(), p->name().c_str());
        return JsonResponse(200, "{\"removed\":\"" + addr.ToString() + "\"}");
      }
    }
  }
  return JsonResponse(404, "{\"error\":\"not found\"}");
}

}  // namespace hlb
