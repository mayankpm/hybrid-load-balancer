#include "lb/worker.h"

#include <string>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#endif

namespace hlb {

void ListenerStats::CountStatus(int status) {
  if (status < 300) responses_2xx++;
  else if (status < 400) responses_3xx++;
  else if (status < 500) responses_4xx++;
  else responses_5xx++;
}

// ---------------------------------------------------------------------------
// UpstreamPool

std::shared_ptr<TcpConn> UpstreamPool::Acquire(uint64_t backend_id) {
  auto it = idle_.find(backend_id);
  if (it == idle_.end()) return nullptr;
  auto& list = it->second;
  while (!list.empty()) {
    // Most recently used first: least likely to have hit the backend's own idle timeout.
    Idle idle = std::move(list.back());
    list.pop_back();
    loop_->Cancel(idle.timer);
    TcpConn& c = *idle.conn;
    c.set_on_data(nullptr);
    c.set_on_eof(nullptr);
    c.set_on_close(nullptr);
    if (!c.closed() && !c.eof() && c.input().empty()) {
      reused_++;
      return std::move(idle.conn);
    }
    c.Close();
  }
  return nullptr;
}

void UpstreamPool::Release(uint64_t backend_id, std::shared_ptr<TcpConn> conn, size_t max_idle, int idle_ms) {
  auto& list = idle_[backend_id];
  if (conn->closed() || list.size() >= max_idle) {
    conn->Close();
    return;
  }
  TcpConn* raw = conn.get();
  // An idle backend connection should be silent. Data or EOF means the
  // backend is closing it (or misbehaving), so it must not be reused.
  auto drop = [this, backend_id, raw](TcpConn&) { Drop(backend_id, raw); };
  conn->set_on_data(drop);
  conn->set_on_eof(drop);
  conn->set_on_close([this, backend_id, raw](TcpConn&) { Drop(backend_id, raw); });
  conn->set_on_drain(nullptr);
  conn->ResumeReading();
  const EventLoop::TimerId timer =
      loop_->RunAfter(std::chrono::milliseconds(idle_ms), [this, backend_id, raw] { Drop(backend_id, raw); });
  list.push_back({std::move(conn), timer});
}

void UpstreamPool::Drop(uint64_t backend_id, TcpConn* conn) {
  auto it = idle_.find(backend_id);
  if (it == idle_.end()) return;
  auto& list = it->second;
  for (auto i = list.begin(); i != list.end(); ++i) {
    if (i->conn.get() == conn) {
      std::shared_ptr<TcpConn> c = std::move(i->conn);
      loop_->Cancel(i->timer);
      list.erase(i);
      c->set_on_close(nullptr);
      c->Close();
      return;
    }
  }
}

void UpstreamPool::Clear() {
  auto idle = std::move(idle_);
  idle_.clear();
  for (auto& [id, list] : idle) {
    for (auto& i : list) {
      loop_->Cancel(i.timer);
      i.conn->set_on_close(nullptr);
      i.conn->Close();
    }
  }
}

size_t UpstreamPool::idle_count() const {
  size_t n = 0;
  for (const auto& [id, list] : idle_) n += list.size();
  return n;
}

// ---------------------------------------------------------------------------
// Worker

Worker::~Worker() { Stop(); }

void Worker::Start() {
  thread_ = std::thread([this] {
#ifdef __linux__
    // Named threads make per-worker profiling (perf --comm) and top -H readable.
    const std::string name = "hlb-w" + std::to_string(index_);
    pthread_setname_np(pthread_self(), name.c_str());
#endif
    loop_.Run();
  });
}

void Worker::Stop() {
  if (!thread_.joinable()) return;
  loop_.Post([this] {
    AbortSessions();
    upstreams_.Clear();
  });
  loop_.Stop();
  thread_.join();
}

void Worker::RemoveSession(Session* s) {
  auto it = sessions_.find(s);
  if (it == sessions_.end()) return;
  // The session may be running this very call; destroy it after the callback.
  loop_.DeferRelease(std::move(it->second));
  sessions_.erase(it);
}

void Worker::DrainSessions() {
  std::vector<Session*> all;
  for (auto& [ptr, s] : sessions_) all.push_back(ptr);
  for (Session* s : all) {
    if (sessions_.count(s)) s->BeginDrain();
  }
}

void Worker::AbortSessions() {
  std::vector<Session*> all;
  for (auto& [ptr, s] : sessions_) all.push_back(ptr);
  for (Session* s : all) {
    if (sessions_.count(s)) s->Abort();
  }
}

}  // namespace hlb
