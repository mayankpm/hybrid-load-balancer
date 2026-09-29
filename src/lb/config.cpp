#include "lb/config.h"

#include <cctype>
#include <cstdio>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

namespace hlb {
namespace {

bool ParseInt(const std::string& s, long long* out) {
  if (s.empty()) return false;
  long long v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
    if (v > (1LL << 40)) return false;
  }
  *out = v;
  return true;
}

// Splits "k=v" tokens into a map; bare tokens go to `positional`.
void SplitOptions(std::istringstream& in, std::vector<std::string>* positional,
                  std::map<std::string, std::string>* options) {
  std::string tok;
  while (in >> tok) {
    const size_t eq = tok.find('=');
    if (eq == std::string::npos) {
      positional->push_back(tok);
    } else {
      (*options)[tok.substr(0, eq)] = tok.substr(eq + 1);
    }
  }
}

std::string ToLower(std::string_view s) {
  std::string out(s);
  for (char& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return out;
}

}  // namespace

PoolConfig* Config::FindPool(std::string_view name) {
  for (auto& p : pools) {
    if (p.name == name) return &p;
  }
  return nullptr;
}

ListenerConfig* Config::FindListener(std::string_view name) {
  for (auto& l : listeners) {
    if (l.name == name) return &l;
  }
  return nullptr;
}

bool Config::Parse(std::string_view text, Config* out, std::string* error) {
  *out = Config{};
  std::istringstream lines{std::string(text)};
  std::string line;
  int lineno = 0;
  auto fail = [&](const std::string& msg) {
    *error = "line " + std::to_string(lineno) + ": " + msg;
    return false;
  };

  while (std::getline(lines, line)) {
    lineno++;
    if (const size_t hash = line.find('#'); hash != std::string::npos) line.resize(hash);
    std::istringstream in(line);
    std::string directive;
    if (!(in >> directive)) continue;
    std::vector<std::string> pos;
    std::map<std::string, std::string> opt;
    SplitOptions(in, &pos, &opt);

    // Integer options with validation. Unknown keys are errors, not ignored.
    auto take_int = [&](const char* key, int* dst, long long min) {
      auto it = opt.find(key);
      if (it == opt.end()) return true;
      long long v;
      if (!ParseInt(it->second, &v) || v < min) return false;
      *dst = static_cast<int>(v);
      opt.erase(it);
      return true;
    };
    auto leftover = [&]() { return opt.empty() ? std::string() : opt.begin()->first; };

    if (directive == "threads") {
      long long v;
      if (pos.size() != 1 || !ParseInt(pos[0], &v) || v < 0 || v > 256) return fail("threads needs a count");
      out->threads = static_cast<int>(v);
    } else if (directive == "poller") {
      if (pos.size() != 1 || (pos[0] != "auto" && pos[0] != "poll")) return fail("poller must be auto or poll");
      out->force_poll = pos[0] == "poll";
    } else if (directive == "shutdown_timeout_ms") {
      long long v;
      if (pos.size() != 1 || !ParseInt(pos[0], &v)) return fail("shutdown_timeout_ms needs a number");
      out->shutdown_timeout_ms = static_cast<int>(v);
    } else if (directive == "admin") {
      Address a;
      if (pos.size() != 1 || !Address::Parse(pos[0], &a)) return fail("admin needs host:port");
      out->admin = a;
    } else if (directive == "pool") {
      if (pos.size() != 1) return fail("pool needs a name");
      if (out->FindPool(pos[0])) return fail("duplicate pool " + pos[0]);
      PoolConfig p;
      p.name = pos[0];
      if (auto it = opt.find("algorithm"); it != opt.end()) {
        if (!ParseAlgorithm(it->second, &p.algorithm)) return fail("unknown algorithm " + it->second);
        opt.erase(it);
      }
      if (auto it = opt.find("health"); it != opt.end()) {
        if (it->second == "none") p.health.type = HealthCheckConfig::Type::kNone;
        else if (it->second == "tcp") p.health.type = HealthCheckConfig::Type::kTcp;
        else if (it->second == "http") p.health.type = HealthCheckConfig::Type::kHttp;
        else return fail("health must be none, tcp or http");
        opt.erase(it);
      }
      if (auto it = opt.find("health_path"); it != opt.end()) {
        if (it->second.empty() || it->second[0] != '/') return fail("health_path must start with /");
        p.health.path = it->second;
        opt.erase(it);
      }
      if (!take_int("interval_ms", &p.health.interval_ms, 10) || !take_int("timeout_ms", &p.health.timeout_ms, 1) ||
          !take_int("rise", &p.health.rise, 1) || !take_int("fall", &p.health.fall, 1) ||
          !take_int("max_fails", &p.passive.max_fails, 0) || !take_int("eject_ms", &p.passive.eject_ms, 0) ||
          !take_int("max_idle", &p.max_idle_per_backend, 0)) {
        return fail("invalid numeric option");
      }
      if (!opt.empty()) return fail("unknown pool option " + leftover());
      out->pools.push_back(std::move(p));
    } else if (directive == "backend") {
      if (pos.size() != 2) return fail("backend needs a pool and host:port");
      PoolConfig* p = out->FindPool(pos[0]);
      if (!p) return fail("backend refers to unknown pool " + pos[0]);
      BackendConfig b;
      if (!Address::Parse(pos[1], &b.address)) return fail("bad backend address " + pos[1]);
      if (!take_int("weight", &b.weight, 1)) return fail("weight must be a positive integer");
      if (!opt.empty()) return fail("unknown backend option " + leftover());
      p->backends.push_back(b);
    } else if (directive == "listener") {
      if (pos.size() != 1) return fail("listener needs a name");
      if (out->FindListener(pos[0])) return fail("duplicate listener " + pos[0]);
      ListenerConfig l;
      l.name = pos[0];
      auto mode = opt.find("mode");
      if (mode == opt.end() || (mode->second != "tcp" && mode->second != "http")) {
        return fail("listener needs mode=tcp or mode=http");
      }
      l.mode = mode->second == "tcp" ? ListenerConfig::Mode::kTcp : ListenerConfig::Mode::kHttp;
      opt.erase(mode);
      auto bind = opt.find("bind");
      if (bind == opt.end() || !Address::Parse(bind->second, &l.bind)) return fail("listener needs bind=host:port");
      opt.erase(bind);
      if (auto it = opt.find("pool"); it != opt.end()) {
        l.pool = it->second;
        opt.erase(it);
      }
      int max_body = static_cast<int>(l.max_body_bytes);
      if (!take_int("connect_timeout_ms", &l.connect_timeout_ms, 1) ||
          !take_int("response_timeout_ms", &l.response_timeout_ms, 1) ||
          !take_int("idle_timeout_ms", &l.idle_timeout_ms, 1) ||
          !take_int("upstream_idle_ms", &l.upstream_idle_ms, 1) || !take_int("retries", &l.retries, 0) ||
          !take_int("max_body_bytes", &max_body, 0)) {
        return fail("invalid numeric option");
      }
      l.max_body_bytes = static_cast<size_t>(max_body);
      if (!opt.empty()) return fail("unknown listener option " + leftover());
      out->listeners.push_back(std::move(l));
    } else if (directive == "route") {
      if (pos.size() != 1) return fail("route needs a listener name");
      ListenerConfig* l = out->FindListener(pos[0]);
      if (!l) return fail("route refers to unknown listener " + pos[0]);
      if (l->mode != ListenerConfig::Mode::kHttp) return fail("routes only apply to http listeners");
      RouteConfig r;
      if (auto it = opt.find("host"); it != opt.end()) {
        r.host = ToLower(it->second);
        opt.erase(it);
      }
      if (auto it = opt.find("prefix"); it != opt.end()) {
        if (it->second.empty() || it->second[0] != '/') return fail("prefix must start with /");
        r.path_prefix = it->second;
        opt.erase(it);
      }
      if (auto it = opt.find("header"); it != opt.end()) {
        const size_t colon = it->second.find(':');
        if (colon == std::string::npos || colon == 0) return fail("header must be Name:Value");
        r.header_name = it->second.substr(0, colon);
        r.header_value = it->second.substr(colon + 1);
        opt.erase(it);
      }
      auto pool = opt.find("pool");
      if (pool == opt.end()) return fail("route needs pool=");
      r.pool = pool->second;
      opt.erase(pool);
      if (!opt.empty()) return fail("unknown route option " + leftover());
      l->routes.push_back(std::move(r));
    } else {
      return fail("unknown directive " + directive);
    }
  }
  return out->Validate(error);
}

bool Config::Load(const std::string& path, Config* out, std::string* error) {
  std::ifstream f(path, std::ios::binary);
  if (!f) {
    *error = "cannot read " + path;
    return false;
  }
  std::stringstream ss;
  ss << f.rdbuf();
  return Parse(ss.str(), out, error);
}

bool Config::Validate(std::string* error) const {
  std::set<std::string> pool_names;
  for (const auto& p : pools) pool_names.insert(p.name);
  if (listeners.empty()) {
    *error = "no listeners configured";
    return false;
  }
  for (const auto& l : listeners) {
    if (l.mode == ListenerConfig::Mode::kTcp) {
      if (!pool_names.count(l.pool)) {
        *error = "tcp listener " + l.name + " needs pool= naming a defined pool";
        return false;
      }
    } else {
      if (l.routes.empty() && l.pool.empty()) {
        *error = "http listener " + l.name + " has no routes and no default pool";
        return false;
      }
      if (!l.pool.empty() && !pool_names.count(l.pool)) {
        *error = "listener " + l.name + " refers to unknown pool " + l.pool;
        return false;
      }
      for (const auto& r : l.routes) {
        if (!pool_names.count(r.pool)) {
          *error = "route on " + l.name + " refers to unknown pool " + r.pool;
          return false;
        }
      }
    }
  }
  return true;
}

const RouteConfig* Router::Match(const HttpHead& req) const {
  // Absolute-form targets ("http://host/path") carry the host themselves.
  std::string_view target = req.target;
  std::string host;
  if (target.substr(0, 7) == "http://" || target.substr(0, 8) == "https://") {
    const size_t start = target.find("//") + 2;
    const size_t slash = target.find('/', start);
    host = std::string(target.substr(start, slash == std::string_view::npos ? std::string_view::npos : slash - start));
    target = slash == std::string_view::npos ? std::string_view("/") : target.substr(slash);
  } else if (const std::string* h = req.Find("host")) {
    host = *h;
  }
  if (const size_t colon = host.rfind(':'); colon != std::string::npos && host.find(']') == std::string::npos) {
    host.resize(colon);
  }
  host = ToLower(host);

  for (const RouteConfig& r : routes_) {
    if (!r.host.empty()) {
      if (r.host.size() > 2 && r.host[0] == '*' && r.host[1] == '.') {
        const std::string_view suffix = std::string_view(r.host).substr(1);  // ".example.com"
        if (host.size() <= suffix.size() || host.compare(host.size() - suffix.size(), suffix.size(), suffix) != 0) {
          continue;
        }
      } else if (host != r.host) {
        continue;
      }
    }
    if (!r.path_prefix.empty() && target.substr(0, r.path_prefix.size()) != r.path_prefix) continue;
    if (!r.header_name.empty()) {
      const std::string* v = req.Find(r.header_name);
      if (v == nullptr || *v != r.header_value) continue;
    }
    return &r;
  }
  return nullptr;
}

}  // namespace hlb
