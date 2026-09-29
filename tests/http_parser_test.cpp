#include "http/http.h"
#include "test.h"
#include "testkit/servers.h"

using namespace hlb;

namespace {

// Feeds `data` in pieces of `step` bytes; returns bytes consumed overall.
size_t FeedInSteps(HttpParser& p, std::string_view data, size_t step, std::string* body = nullptr) {
  size_t used = 0;
  while (used < data.size() && !p.done() && p.state() != HttpParser::State::kError) {
    const size_t len = std::min(step, data.size() - used);
    std::string_view piece = data.substr(used, len);
    size_t used_here = 0;
    // Keep feeding the piece until the parser stops making progress.
    while (used_here < piece.size() && !p.done() && p.state() != HttpParser::State::kError) {
      const bool had_head = p.head_complete();
      const size_t n = p.Feed(piece.substr(used_here));
      if (had_head && body) body->append(piece.substr(used_here, n));
      used_here += n;
      if (n == 0) break;
    }
    used += used_here;
    if (used_here < piece.size() && !p.done() && p.state() != HttpParser::State::kError) break;
  }
  return used;
}

}  // namespace

TEST(http, parses_request_line_and_headers) {
  HttpParser p(HttpParser::Kind::kRequest);
  const std::string req = "GET /a/b?x=1 HTTP/1.1\r\nHost: example.com\r\nX-Pad:   spaced value  \r\n\r\n";
  CHECK_EQ(FeedInSteps(p, req, req.size()), req.size());
  CHECK(p.done());
  CHECK_EQ(p.head().method, std::string("GET"));
  CHECK_EQ(p.head().target, std::string("/a/b?x=1"));
  CHECK_EQ(*p.head().Find("HOST"), std::string("example.com"));
  CHECK_EQ(*p.head().Find("x-pad"), std::string("spaced value"));
  CHECK(p.keep_alive());
}

TEST(http, content_length_body_split_at_every_byte) {
  const std::string req = "POST /upload HTTP/1.1\r\nContent-Length: 11\r\n\r\nhello world";
  for (size_t step = 1; step <= req.size(); step++) {
    HttpParser p(HttpParser::Kind::kRequest);
    std::string body;
    CHECK_EQ(FeedInSteps(p, req, step, &body), req.size());
    CHECK(p.done());
    CHECK_EQ(body, std::string("hello world"));
  }
}

TEST(http, chunked_body_with_extensions_and_trailers_split_at_every_byte) {
  const std::string body = "5;ext=1\r\nhello\r\n6\r\n world\r\n0\r\nX-Checksum: abc\r\n\r\n";
  const std::string req = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n" + body;
  for (size_t step = 1; step <= req.size(); step++) {
    HttpParser p(HttpParser::Kind::kRequest);
    std::string raw;
    CHECK_EQ(FeedInSteps(p, req + "GET /next HTTP/1.1\r\n\r\n", step, &raw), req.size());
    CHECK(p.done());
    CHECK_EQ(p.body_kind(), BodyKind::kChunked);
    CHECK_EQ(raw, body);  // Raw bytes are passed through untouched.
    CHECK_EQ(testkit::DecodeChunked(raw), std::string("hello world"));
  }
}

TEST(http, pipelined_requests_are_separated) {
  const std::string two = "GET /1 HTTP/1.1\r\n\r\nPOST /2 HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcGET /3 HTTP/1.1\r\n\r\n";
  std::string_view rest = two;
  std::vector<std::string> targets;
  HttpParser p(HttpParser::Kind::kRequest);
  while (!rest.empty()) {
    const size_t n = p.Feed(rest);
    rest.remove_prefix(n);
    if (p.done()) {
      targets.push_back(p.head().target);
      p.Reset();
    }
  }
  CHECK_EQ(targets.size(), 3u);
  CHECK_EQ(targets[1], std::string("/2"));
  CHECK_EQ(targets[2], std::string("/3"));
}

TEST(http, rejects_request_smuggling_and_malformed_input) {
  struct Case {
    const char* request;
    int status;
  } cases[] = {
      {"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n", 400},
      {"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n", 400},
      {"POST / HTTP/1.1\r\nContent-Length: 5, 6\r\n\r\n", 400},
      {"POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n", 400},
      {"POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", 501},
      {"GET / HTTP/1.1\r\nX-Folded: a\r\n b\r\n\r\n", 400},
      {"GET / HTTP/1.1\r\nBad Header: x\r\n\r\n", 400},
      {"GET /\r\n\r\n", 400},
      {"GET / HTTP/2.0\r\n\r\n", 505},
      {"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", 400},
  };
  for (const Case& c : cases) {
    HttpParser p(HttpParser::Kind::kRequest);
    std::string_view data = c.request;
    while (!data.empty() && p.state() != HttpParser::State::kError) {
      const size_t n = p.Feed(data);
      if (n == 0) break;
      data.remove_prefix(n);
    }
    if (p.state() != HttpParser::State::kError) throw lbtest::Failure(std::string("accepted: ") + c.request);
    CHECK_EQ(p.error_status(), c.status);
  }
}

TEST(http, oversized_head_is_rejected_with_431) {
  HttpParser p(HttpParser::Kind::kRequest, 1024);
  std::string req = "GET / HTTP/1.1\r\nX-Big: " + std::string(4000, 'a') + "\r\n\r\n";
  p.Feed(req);
  CHECK(p.state() == HttpParser::State::kError);
  CHECK_EQ(p.error_status(), 431);
}

TEST(http, response_framing_rules) {
  {
    HttpParser p(HttpParser::Kind::kResponse);
    p.set_request_method("HEAD");
    p.Feed("HTTP/1.1 200 OK\r\nContent-Length: 500\r\n\r\n");
    CHECK(p.done());  // HEAD responses have no body despite Content-Length.
  }
  for (const char* status : {"204 No Content", "304 Not Modified", "100 Continue"}) {
    HttpParser p(HttpParser::Kind::kResponse);
    p.Feed(std::string("HTTP/1.1 ") + status + "\r\n\r\n");
    CHECK(p.done());
  }
  {
    HttpParser p(HttpParser::Kind::kResponse);
    const std::string head = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
    CHECK_EQ(p.Feed(head), head.size());
    CHECK_EQ(p.body_kind(), BodyKind::kUntilClose);
    CHECK_EQ(p.Feed("streaming bytes"), 15u);
    CHECK(!p.done());
    p.OnEof();
    CHECK(p.done());
  }
  {
    HttpParser p(HttpParser::Kind::kResponse);
    p.Feed("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n");
    p.Feed("short");
    p.OnEof();
    CHECK(p.state() == HttpParser::State::kError);  // Truncated.
  }
}

TEST(http, keep_alive_rules) {
  auto keep_alive = [](const std::string& req) {
    HttpParser p(HttpParser::Kind::kRequest);
    p.Feed(req);
    return p.keep_alive();
  };
  CHECK(keep_alive("GET / HTTP/1.1\r\n\r\n"));
  CHECK(!keep_alive("GET / HTTP/1.1\r\nConnection: close\r\n\r\n"));
  CHECK(!keep_alive("GET / HTTP/1.0\r\n\r\n"));
  CHECK(keep_alive("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"));
}

TEST(http, strip_hop_by_hop_headers) {
  HttpHead h;
  h.Add("Connection", "keep-alive, X-Secret-Hop");
  h.Add("Keep-Alive", "timeout=5");
  h.Add("X-Secret-Hop", "1");
  h.Add("Upgrade", "websocket");
  h.Add("Transfer-Encoding", "chunked");
  h.Add("X-Kept", "yes");
  StripHopByHop(&h);
  CHECK(h.Find("connection") == nullptr);
  CHECK(h.Find("keep-alive") == nullptr);
  CHECK(h.Find("x-secret-hop") == nullptr);
  CHECK(h.Find("upgrade") == nullptr);
  CHECK(h.Find("transfer-encoding") != nullptr);  // Body framing is forwarded as is.
  CHECK(h.Find("x-kept") != nullptr);
}
