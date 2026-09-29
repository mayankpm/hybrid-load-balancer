// demo_backend: a small HTTP or TCP echo backend for trying the balancer out.
//
//   demo_backend --port 9001 --id b1 [--mode http|tcp] [--delay-ms N] [--host 127.0.0.1]
//
// HTTP endpoints are listed in src/testkit/servers.h (/health, /echo,
// /delay/<ms>, /size/<n>, ...). Every response carries X-Backend: <id>.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "common/platform.h"
#include "testkit/servers.h"

namespace {
std::atomic<bool> g_stop{false};
void OnSignal(int) { g_stop = true; }
}  // namespace

int main(int argc, char** argv) {
  hlb::EnableHighResolutionTimers();
  hlb::NetInit();
  std::string id = "backend", mode = "http", host = "127.0.0.1";
  int port = 0, delay_ms = 0;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string a = argv[i], v = argv[i + 1];
    if (a == "--port") port = std::atoi(v.c_str());
    else if (a == "--id") id = v;
    else if (a == "--mode") mode = v;
    else if (a == "--delay-ms") delay_ms = std::atoi(v.c_str());
    else if (a == "--host") host = v;
  }
  if (port <= 0 || (mode != "http" && mode != "tcp")) {
    std::fprintf(stderr, "usage: demo_backend --port N --id NAME [--mode http|tcp] [--delay-ms N] [--host H]\n");
    return 2;
  }
  std::unique_ptr<hlb::testkit::ServerBase> server;
  if (mode == "http") {
    auto s = std::make_unique<hlb::testkit::HttpServer>(id);
    s->SetExtraDelayMs(delay_ms);
    server = std::move(s);
  } else {
    server = std::make_unique<hlb::testkit::EchoServer>(id);
  }
  if (!server->Start(host, static_cast<uint16_t>(port))) {
    std::fprintf(stderr, "demo_backend: cannot listen on %s:%d\n", host.c_str(), port);
    return 1;
  }
  std::printf("demo_backend %s (%s) listening on %s\n", id.c_str(), mode.c_str(), server->address().ToString().c_str());
  std::fflush(stdout);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  server->Kill();
  return 0;
}
