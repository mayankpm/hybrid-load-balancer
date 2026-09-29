// lbbench: synthetic traffic generator and benchmarks. Everything (backends,
// load balancer, clients) runs in this process over loopback TCP.
//
//   lbbench http     [--clients 64] [--seconds 5] [--backends 4] [--threads 4] [--size 128]
//   lbbench tcp      [--mb 512]
//   lbbench failover [--clients 32] [--threads 4]
//
// "http" measures closed-loop request latency directly against a backend and
// through the balancer, so the difference is the latency the proxy adds.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/log.h"
#include "common/platform.h"
#include "lb/load_balancer.h"
#include "testkit/servers.h"

using namespace hlb;
using namespace hlb::testkit;
using Clock = std::chrono::steady_clock;

namespace {

struct Args {
  std::string mode = "http";
  int clients = 64;
  int seconds = 5;
  int backends = 4;
  int threads = 4;
  int size = 128;
  int mb = 512;
};

struct RunResult {
  double seconds = 0;
  uint64_t ok = 0;
  uint64_t errors = 0;
  std::vector<double> latencies_us;
};

double Pct(std::vector<double>& v, double q) {
  if (v.empty()) return 0;
  return v[static_cast<size_t>(q * static_cast<double>(v.size() - 1))];
}

// Closed-loop load: each client sends its next request as soon as the last returns.
RunResult RunHttpLoad(const Address& target, int clients, int seconds, const std::string& path) {
  std::atomic<bool> stop{false};
  std::mutex mu;
  RunResult result;
  std::vector<std::thread> threads;
  for (int c = 0; c < clients; c++) {
    threads.emplace_back([&] {
      HttpClient client(target, 10000);
      std::vector<double> lat;
      lat.reserve(200000);
      uint64_t ok = 0, errors = 0;
      while (!stop) {
        const auto t0 = Clock::now();
        auto r = client.Get(path);
        const double us = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
        if (r.ok && r.status == 200) {
          ok++;
          lat.push_back(us);
        } else {
          errors++;
        }
      }
      std::lock_guard lock(mu);
      result.ok += ok;
      result.errors += errors;
      result.latencies_us.insert(result.latencies_us.end(), lat.begin(), lat.end());
    });
  }
  const auto start = Clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  stop = true;
  for (auto& t : threads) t.join();
  result.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  std::sort(result.latencies_us.begin(), result.latencies_us.end());
  return result;
}

void Print(const char* label, RunResult& r) {
  std::printf("  %-26s %9.0f req/s   p50 %7.1f us   p99 %7.1f us   p99.9 %7.1f us   errors %llu\n", label,
              static_cast<double>(r.ok) / r.seconds, Pct(r.latencies_us, 0.50), Pct(r.latencies_us, 0.99),
              Pct(r.latencies_us, 0.999), static_cast<unsigned long long>(r.errors));
}

std::vector<std::unique_ptr<HttpServer>> StartBackends(int n) {
  std::vector<std::unique_ptr<HttpServer>> out;
  for (int i = 0; i < n; i++) {
    out.push_back(std::make_unique<HttpServer>("b" + std::to_string(i)));
    if (!out.back()->Start()) {
      std::fprintf(stderr, "backend failed to start\n");
      std::exit(1);
    }
  }
  return out;
}

std::unique_ptr<LoadBalancer> StartLb(const std::string& text, const std::vector<Address>& backends) {
  Config cfg;
  std::string err;
  if (!Config::Parse(text, &cfg, &err)) {
    std::fprintf(stderr, "config: %s\n", err.c_str());
    std::exit(1);
  }
  for (const Address& a : backends) cfg.pools[0].backends.push_back({a, 1});
  auto lb = std::make_unique<LoadBalancer>(std::move(cfg));
  if (!lb->Start(&err)) {
    std::fprintf(stderr, "lb: %s\n", err.c_str());
    std::exit(1);
  }
  return lb;
}

int BenchHttp(const Args& a) {
  auto backends = StartBackends(a.backends);
  std::vector<Address> addrs;
  for (auto& b : backends) addrs.push_back(b->address());
  auto lb = StartLb("threads " + std::to_string(a.threads) +
                        "\npool web algorithm=least_connections\n"
                        "listener http mode=http bind=127.0.0.1:0 pool=web\n",
                    addrs);
  const std::string path = "/size/" + std::to_string(a.size);
  std::printf("HTTP: %d closed-loop keep-alive clients, %d-byte responses, %d backends, %d LB threads, %ds per run\n",
              a.clients, a.size, a.backends, a.threads, a.seconds);

  // Warm up connection pools.
  RunHttpLoad(lb->listener_address("http"), a.clients, 1, path);
  auto direct = RunHttpLoad(addrs[0], a.clients, a.seconds, path);
  auto proxied = RunHttpLoad(lb->listener_address("http"), a.clients, a.seconds, path);
  Print("direct to one backend", direct);
  Print("through hybridlb", proxied);
  std::printf("  added latency at p50: %.1f us, at p99: %.1f us\n",
              Pct(proxied.latencies_us, 0.50) - Pct(direct.latencies_us, 0.50),
              Pct(proxied.latencies_us, 0.99) - Pct(direct.latencies_us, 0.99));
  std::printf("  upstream connection reuses: %llu\n", static_cast<unsigned long long>(lb->upstream_reuses()));
  std::printf("  LB worker utilization since start:");
  for (double u : lb->worker_utilization()) std::printf(" %.0f%%", u * 100);
  std::printf("\n");
  lb->Stop();
  return 0;
}

double TransferMBps(const Address& target, size_t bytes) {
  const SocketHandle s = ConnectTcpBlocking(target, 2000);
  if (s == kInvalidSocket) return 0;
  SetRecvTimeout(s, 30000);
  char c;
  while (RecvSome(s, &c, 1) == 1 && c != '\n') {
  }
  const std::string chunk(1 << 20, 'd');
  std::atomic<size_t> received{0};
  const auto t0 = Clock::now();
  std::thread reader([&] {
    std::vector<char> buf(1 << 20);
    while (received < bytes) {
      const long long n = RecvSome(s, buf.data(), buf.size());
      if (n <= 0) break;
      received += static_cast<size_t>(n);
    }
  });
  for (size_t sent = 0; sent < bytes; sent += chunk.size()) SendAll(s, chunk);
  reader.join();
  const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  CloseSocket(s);
  return static_cast<double>(received) / (1 << 20) / secs;
}

int BenchTcp(const Args& a) {
  EchoServer echo("e0");
  echo.Start();
  auto lb = StartLb("threads 2\npool echo\nlistener tcp mode=tcp bind=127.0.0.1:0 pool=echo\n", {echo.address()});
  const size_t bytes = static_cast<size_t>(a.mb) << 20;
  std::printf("TCP: %d MB echoed through one connection (both directions)\n", a.mb);
  std::printf("  direct to backend        %8.0f MB/s\n", TransferMBps(echo.address(), bytes));
  std::printf("  through hybridlb (L4)    %8.0f MB/s\n", TransferMBps(lb->listener_address("tcp"), bytes));

  // New-connection rate through the L4 proxy.
  const int kConns = 2000;
  const auto t0 = Clock::now();
  for (int i = 0; i < kConns; i++) {
    const SocketHandle s = ConnectTcpBlocking(lb->listener_address("tcp"), 2000);
    SetRecvTimeout(s, 5000);
    char c;
    while (RecvSome(s, &c, 1) == 1 && c != '\n') {
    }
    CloseSocket(s);
  }
  const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  std::printf("  sequential connect+first byte through L4: %.0f conn/s (%.0f us each)\n", kConns / secs,
              secs * 1e6 / kConns);
  lb->Stop();
  return 0;
}

int BenchFailover(const Args& a) {
  auto backends = StartBackends(3);
  std::vector<Address> addrs;
  for (auto& b : backends) addrs.push_back(b->address());
  auto lb = StartLb("threads " + std::to_string(a.threads) +
                        "\npool web algorithm=round_robin health=http interval_ms=200 timeout_ms=300\n"
                        "listener http mode=http bind=127.0.0.1:0 pool=web retries=2 connect_timeout_ms=300\n",
                    addrs);
  std::printf("Failover: %d clients for 6s; backend b1 is killed at 2s and restarted at 4s\n", a.clients);
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> ok{0}, errors{0};
  std::vector<std::thread> threads;
  for (int c = 0; c < a.clients; c++) {
    threads.emplace_back([&] {
      HttpClient client(lb->listener_address("http"), 10000);
      while (!stop) {
        auto r = client.Get("/x");
        (r.ok && r.status == 200 ? ok : errors)++;
      }
    });
  }
  uint64_t last_ok = 0;
  for (int window = 1; window <= 12; window++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const char* event = "";
    if (window == 4) {
      backends[1]->Kill();
      event = "  <- b1 killed";
    } else if (window == 8) {
      backends[1]->Restart();
      event = "  <- b1 restarted";
    }
    const uint64_t now_ok = ok;
    std::printf("  t=%4.1fs  %8.0f req/s  errors so far: %llu%s\n", window * 0.5,
                static_cast<double>(now_ok - last_ok) * 2, static_cast<unsigned long long>(errors.load()), event);
    last_ok = now_ok;
  }
  stop = true;
  for (auto& t : threads) t.join();
  std::printf("  total: %llu requests, %llu errors, %llu retries\n", static_cast<unsigned long long>(ok.load()),
              static_cast<unsigned long long>(errors.load()),
              static_cast<unsigned long long>(lb->listener_stats("http")->retries.load()));
  lb->Stop();
  return errors == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  EnableHighResolutionTimers();
  NetInit();
  SetLogLevel(LogLevel::kError);
  Args a;
  if (argc >= 2) a.mode = argv[1];
  for (int i = 2; i + 1 < argc; i += 2) {
    const std::string f = argv[i];
    const int v = std::atoi(argv[i + 1]);
    if (f == "--clients") a.clients = v;
    else if (f == "--seconds") a.seconds = v;
    else if (f == "--backends") a.backends = v;
    else if (f == "--threads") a.threads = v;
    else if (f == "--size") a.size = v;
    else if (f == "--mb") a.mb = v;
  }
  if (a.mode == "http") return BenchHttp(a);
  if (a.mode == "tcp") return BenchTcp(a);
  if (a.mode == "failover") return BenchFailover(a);
  std::fprintf(stderr, "usage: lbbench http|tcp|failover [options]\n");
  return 2;
}
