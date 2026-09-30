# Hybrid L4/L7 Load Balancer

A multi-threaded, event-driven TCP (layer 4) and HTTP/1.1 (layer 7) load balancer written from scratch with
no third-party dependencies. It proxies raw TCP streams and HTTP requests across backend pools with round-robin,
least-connections and smooth weighted round-robin scheduling, active and passive health checking, automatic failover,
keep-alive connection pooling, and zero-downtime backend changes through an admin API.

```
             clients
                |
      +---------+----------+           worker 0 accepts, then hands each connection
      |  hybridlb          |           to a worker; it lives on that thread from then on
      |  +--------------+  |
      |  | worker loops |  |  epoll (Linux) / poll (portable), one per thread
      |  | L4 sessions  |  |  TCP splice with backpressure and half-close
      |  | L7 sessions  |  |  HTTP parse, route, rewrite, retry, stream
      |  | upstream pool|  |  keep-alive backend connections, per thread
      |  +--------------+  |
      |  backend pools     |  copy-on-write membership, atomic per-backend state
      |  health checker    |  active HTTP/TCP probes with rise/fall hysteresis
      |  admin API         |  /stats JSON, add/drain/remove backends live
      +---------+----------+
                |
      backend   backend   backend
```

## Features

**Layer 4 (TCP)**
* Bidirectional splicing with backpressure: when one side's output buffer passes 1 MB, the other side stops reading
  until it drains, so a slow peer cannot make the proxy buffer unbounded data.
* Faithful half-close: a FIN from either side is forwarded while the other direction keeps flowing.
* Connect failover: a failed connect is recorded against the backend and the next backend is tried; bytes the client
  sent meanwhile are buffered and delivered.

**Layer 7 (HTTP/1.1)**
* Incremental parser with Content-Length, chunked encoding (extensions and trailers) and read-until-close framing.
  Bodies are forwarded as raw bytes and streamed, never re-encoded or held whole.
* Rejects request smuggling vectors: both Transfer-Encoding and Content-Length, conflicting lengths, obsolete line
  folding, malformed chunk sizes; limits on header size (431) and body size (413).
* Routing by host (exact or `*.example.com`), path prefix and header value.
* Header rewriting: strips hop-by-hop headers (including those named in `Connection`), appends `X-Forwarded-For`,
  adds `X-Forwarded-Proto`, `X-Forwarded-Host`, `Via` and `X-Request-Id`.
* Keep-alive on both sides, pipelining, `Expect: 100-continue`, interim 1xx responses, HEAD/204/304.
* Per-worker pools of keep-alive backend connections: a request normally skips the TCP handshake entirely.
* Retries on a different backend when a request fails before any response byte arrives: always if the request never
  reached a backend, otherwise only for idempotent methods, so a POST is never replayed.
* 502/503/504 responses for failed, missing and slow backends.

**Scheduling and health**
* Round-robin, weighted least-connections, and smooth weighted round-robin (the nginx algorithm: weights 5:1:1 give
  `a a b a c a a`, not `a a a a a b c`).
* Active health checks (TCP connect or HTTP GET expecting 2xx/3xx) with rise/fall thresholds.
* Passive health checks: repeated connection failures eject a backend for a configurable time. If every backend is
  ejected, traffic still flows to them (panic mode) rather than failing everything.
* Zero-downtime changes: add, drain (stop new traffic, remove when idle) or remove backends through the admin API.
* Graceful shutdown: stop accepting, let in-flight requests finish, then exit.

**Concurrency**
* One event loop per worker thread (epoll on Linux, poll/WSAPoll elsewhere). A connection lives on one thread, so the
  data path takes no locks.
* Backend pool membership is a copy-on-write snapshot behind a `std::shared_mutex`: a pick holds the shared lock only
  to copy a pointer, and writers swap in a new snapshot. Per-backend state (in-flight count, health, ejection deadline,
  counters) is `std::atomic`. The first version held the shared lock for the whole pick; the stress test showed that
  under constant load glibc's reader-preferring lock starved admin updates indefinitely, which led to this design.

## Testing

`lbtests` runs 48 tests against real sockets: HTTP parsing (every body split at every byte boundary, smuggling
cases), scheduling algorithms, configuration, L4 and L7 proxying end to end, fault injection and concurrency stress.

| Fault-injection test | What it checks |
|---|---|
| Kill a backend under load | 8 keep-alive clients: zero client errors while in-flight and pooled connections are reset; the backend rejoins after restart |
| Drain a backend under load | Admin drain: zero errors, no new requests reach it, it leaves the pool once idle |
| Active health checks | Unhealthy backend taken out of rotation and returned after recovery |
| Passive ejection | With no active checks, connection failures eject the backend; traffic resumes after the ejection period |
| All backends down | 503, then recovery |
| Graceful shutdown | An in-flight request completes after shutdown begins; new connections are refused |

| Stress test | What it checks |
|---|---|
| 32 clients x 150 requests | Every response carries its own client's id and body; per-backend and listener counters add up exactly; in-flight counters return to zero |
| Connection churn | 800 connections with `Connection: close`, all succeed, no leaked sessions |
| 32 parallel TCP sessions | 256 KB echoed per session, byte-for-byte |
| Slow reader | 8 MB response delivered intact to a client reading slowly (backpressure) |

The suite passes on Linux with epoll and with the portable poll backend (`HLB_FORCE_POLL=1`), under ThreadSanitizer
(no data races reported), under AddressSanitizer with UndefinedBehaviorSanitizer and leak detection, and on Windows
(MinGW). CI runs all of these.

## Benchmarks

`lbbench` runs backends, the balancer and closed-loop clients in one process over loopback. Linux (Docker, 16 vCPUs of
an AMD Ryzen 9 5900HS), Release build, 128-byte responses. Clients, backends and the balancer share the same cores, so
treat absolute numbers as indicative.

| Scenario | Result |
|---|---|
| Added latency, 8 clients, 4 threads | p50 58 us, p99 105 us over going direct |
| Throughput, 64 clients, 8 threads | 127,000 requests/s (p50 428 us, p99 2.2 ms) |
| L4 bulk transfer, one connection echoed both ways | 991 MB/s (direct: 1,304 MB/s) |
| Failover run: backend killed and restarted under 32 clients | 0 errors across 314,000 requests (17 transparent retries) |

Profiling (`perf`) shows the workers spend almost all their time in the kernel's TCP stack rather than in parsing or
scheduling, so the main optimization was fewer syscalls: each upstream read produces one client write (head and body
coalesced), and a short read skips the extra `recv` that would only return `EWOULDBLOCK`.

On Windows the portable backend uses `WSAPoll`, which costs about 0.25 us per registered socket on every call (30 us
at 128 sockets, measured). That caps a worker at roughly 17,000 requests/s there; epoll does not have this cost.

## Build and run

Requires CMake 3.20+ and a C++20 compiler (GCC 11+ or Clang 14+; MinGW on Windows).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/lbtests                        # all tests
./build/lbtests l7. failover.          # or filter by name
cmake -S . -B build-tsan -DHLB_SANITIZE=thread && cmake --build build-tsan --target lbtests
```

Try it with demo backends and the example config:

```sh
scripts/demo.sh                        # Windows: scripts\demo.ps1
curl localhost:8080/echo               # shows the headers the backend received
curl -H "X-Canary: 1" localhost:8080/  # header-based routing
curl localhost:9901/stats              # JSON counters and latency percentiles
curl -X POST "localhost:9901/pools/web/backends/127.0.0.1:9002/drain"
./build/lbbench http --clients 64 --threads 8
./build/lbbench failover
```

Configuration ([examples/lb.conf](examples/lb.conf)):

```
threads 4
admin 127.0.0.1:9901
pool web algorithm=least_connections health=http health_path=/health interval_ms=1000 rise=2 fall=2 max_fails=3 eject_ms=10000
backend web 127.0.0.1:9001 weight=2
listener http mode=http bind=0.0.0.0:8080 retries=2 connect_timeout_ms=1000 response_timeout_ms=30000
route http host=api.example.com prefix=/v1 pool=web
route http header=X-Canary:1 pool=canary
route http pool=web
listener tcp mode=tcp bind=0.0.0.0:9000 pool=echo
```

| Admin endpoint | Effect |
|---|---|
| `GET /stats` | Listener and backend counters, latency percentiles, worker utilization |
| `POST /pools/<pool>/backends?address=h:p&weight=n` | Add a backend |
| `POST /pools/<pool>/backends/<h:p>/drain` | Stop new traffic; remove once idle |
| `DELETE /pools/<pool>/backends/<h:p>` | Remove immediately |

## Layout

```
src/net       sockets, epoll/poll poller, event loop, non-blocking connections
src/http      HTTP/1.1 message heads and incremental parser
src/lb        backend pools and scheduling, health checks, config and routing,
              L4 and L7 sessions, upstream connection pools, admin API
src/testkit   blocking test backends and client (tests, benchmark, demo)
apps          hybridlb, demo_backend, lbbench
tests         unit, integration, fault-injection and stress tests
```

## Limitations

* HTTP/1.1 only: no HTTP/2, TLS termination or WebSocket upgrades.
* IPv4 only.
* Requests are buffered whole (up to `max_body_bytes`, default 8 MB) so they can be retried; responses stream.
* A non-idempotent request is not retried after it may have reached a backend, so if a backend closes a pooled
  keep-alive connection at the exact moment a POST is sent on it, the client gets a 502.
* The portable poll backend scales linearly with open sockets (see the Windows note above); use Linux for production.
