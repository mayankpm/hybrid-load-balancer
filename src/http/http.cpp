#include "http/http.h"

#include <algorithm>
#include <cctype>

namespace hlb {
namespace {

char Lower(char c) { return static_cast<char>(std::tolower(static_cast<unsigned char>(c))); }

std::string_view Trim(std::string_view s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
  return s;
}

bool IsTokenChar(char c) {
  if (std::isalnum(static_cast<unsigned char>(c))) return true;
  switch (c) {
    case '!': case '#': case '$': case '%': case '&': case '\'': case '*': case '+':
    case '-': case '.': case '^': case '_': case '`': case '|': case '~':
      return true;
    default:
      return false;
  }
}

bool IsToken(std::string_view s) {
  return !s.empty() && std::all_of(s.begin(), s.end(), IsTokenChar);
}

bool ParseDecimal(std::string_view s, uint64_t* out) {
  if (s.empty() || s.size() > 18) return false;
  uint64_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + static_cast<uint64_t>(c - '0');
  }
  *out = v;
  return true;
}

// Splits "a, b ,c" into trimmed, non-empty items.
template <typename Fn>
void ForEachListItem(std::string_view list, Fn fn) {
  while (!list.empty()) {
    const size_t comma = list.find(',');
    const std::string_view item = Trim(list.substr(0, comma));
    if (!item.empty()) fn(item);
    if (comma == std::string_view::npos) break;
    list.remove_prefix(comma + 1);
  }
}

}  // namespace

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); i++) {
    if (Lower(a[i]) != Lower(b[i])) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// HttpHead

const std::string* HttpHead::Find(std::string_view name) const {
  for (const Header& h : headers) {
    if (EqualsIgnoreCase(h.name, name)) return &h.value;
  }
  return nullptr;
}

void HttpHead::Set(std::string_view name, std::string_view value) {
  Remove(name);
  Add(name, value);
}

void HttpHead::Remove(std::string_view name) {
  headers.erase(std::remove_if(headers.begin(), headers.end(),
                               [&](const Header& h) { return EqualsIgnoreCase(h.name, name); }),
                headers.end());
}

bool HttpHead::HasToken(std::string_view name, std::string_view token) const {
  bool found = false;
  for (const Header& h : headers) {
    if (!EqualsIgnoreCase(h.name, name)) continue;
    ForEachListItem(h.value, [&](std::string_view item) { found = found || EqualsIgnoreCase(item, token); });
  }
  return found;
}

std::string HttpHead::SerializeRequest() const {
  std::string out;
  out.reserve(256);
  out += method;
  out += ' ';
  out += target;
  out += " HTTP/1.1\r\n";
  for (const Header& h : headers) {
    out += h.name;
    out += ": ";
    out += h.value;
    out += "\r\n";
  }
  out += "\r\n";
  return out;
}

std::string HttpHead::SerializeResponse() const {
  std::string out;
  out.reserve(256);
  out += "HTTP/1.1 ";
  out += std::to_string(status);
  out += ' ';
  out += reason.empty() ? ReasonPhrase(status) : reason;
  out += "\r\n";
  for (const Header& h : headers) {
    out += h.name;
    out += ": ";
    out += h.value;
    out += "\r\n";
  }
  out += "\r\n";
  return out;
}

void StripHopByHop(HttpHead* head) {
  std::vector<std::string> named;
  for (const Header& h : head->headers) {
    if (EqualsIgnoreCase(h.name, "connection")) {
      ForEachListItem(h.value, [&](std::string_view item) {
        if (!EqualsIgnoreCase(item, "close") && !EqualsIgnoreCase(item, "keep-alive")) named.emplace_back(item);
      });
    }
  }
  for (const char* h : {"connection", "keep-alive", "proxy-connection", "te", "trailer", "upgrade",
                        "proxy-authorization", "proxy-authenticate"}) {
    head->Remove(h);
  }
  for (const std::string& h : named) {
    if (!EqualsIgnoreCase(h, "transfer-encoding") && !EqualsIgnoreCase(h, "content-length")) head->Remove(h);
  }
}

const char* ReasonPhrase(int status) {
  switch (status) {
    case 100: return "Continue";
    case 200: return "OK";
    case 201: return "Created";
    case 202: return "Accepted";
    case 204: return "No Content";
    case 301: return "Moved Permanently";
    case 302: return "Found";
    case 304: return "Not Modified";
    case 400: return "Bad Request";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 408: return "Request Timeout";
    case 413: return "Content Too Large";
    case 431: return "Request Header Fields Too Large";
    case 500: return "Internal Server Error";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    case 504: return "Gateway Timeout";
    default: return "Unknown";
  }
}

std::string MakeResponse(int status, std::string_view body, std::string_view content_type, bool close,
                         const std::vector<Header>& extra) {
  HttpHead h;
  h.status = status;
  h.Add("Content-Type", content_type);
  h.Add("Content-Length", std::to_string(body.size()));
  if (close) h.Add("Connection", "close");
  for (const Header& e : extra) h.Add(e.name, e.value);
  std::string out = h.SerializeResponse();
  out.append(body.data(), body.size());
  return out;
}

// ---------------------------------------------------------------------------
// HttpParser

HttpParser::HttpParser(Kind kind, size_t max_head_bytes) : kind_(kind), max_head_bytes_(max_head_bytes) {}

void HttpParser::Reset() {
  state_ = State::kHead;
  head_buf_.clear();
  head_ = HttpHead{};
  request_method_.clear();
  body_kind_ = BodyKind::kNone;
  content_length_ = remaining_ = 0;
  chunk_state_ = ChunkState::kSizeLine;
  line_.clear();
  trailer_bytes_ = 0;
  error_status_ = 0;
  error_.clear();
}

void HttpParser::Fail(int status, std::string message) {
  state_ = State::kError;
  error_status_ = status;
  error_ = std::move(message);
}

bool HttpParser::keep_alive() const {
  if (head_.HasToken("connection", "close")) return false;
  if (head_.version_minor == 0) return head_.HasToken("connection", "keep-alive");
  return true;
}

size_t HttpParser::Feed(std::string_view data) {
  if (state_ == State::kHead) return FeedHead(data);
  if (state_ == State::kBody) return FeedBody(data);
  return 0;
}

size_t HttpParser::FeedHead(std::string_view data) {
  // Tolerate stray CRLFs between pipelined messages (RFC 9112 2.2).
  size_t skipped = 0;
  if (head_buf_.empty()) {
    while (skipped < data.size() && (data[skipped] == '\r' || data[skipped] == '\n')) skipped++;
    data.remove_prefix(skipped);
  }
  const size_t before = head_buf_.size();
  const size_t search_from = before >= 3 ? before - 3 : 0;
  head_buf_.append(data.data(), std::min(data.size(), max_head_bytes_ + 4 - std::min(before, max_head_bytes_)));
  const size_t end = head_buf_.find("\r\n\r\n", search_from);
  if (end == std::string::npos) {
    if (head_buf_.size() > max_head_bytes_) {
      Fail(431, "header section too large");
      return skipped + data.size();
    }
    return skipped + data.size();
  }
  const size_t head_len = end + 4;
  const size_t used_here = head_len - before;
  if (!ParseHead(std::string_view(head_buf_).substr(0, end)) || !DetermineBody()) return skipped + used_here;
  head_buf_.clear();
  return skipped + used_here;
}

bool HttpParser::ParseHead(std::string_view text) {
  size_t eol = text.find("\r\n");
  const std::string_view first = text.substr(0, eol);
  if (kind_ == Kind::kRequest) {
    const size_t sp1 = first.find(' ');
    const size_t sp2 = first.rfind(' ');
    if (sp1 == std::string_view::npos || sp2 == sp1) {
      Fail(400, "malformed request line");
      return false;
    }
    head_.method = std::string(first.substr(0, sp1));
    head_.target = std::string(first.substr(sp1 + 1, sp2 - sp1 - 1));
    const std::string_view version = first.substr(sp2 + 1);
    if (!IsToken(head_.method) || head_.target.empty() || head_.target.find(' ') != std::string::npos) {
      Fail(400, "malformed request line");
      return false;
    }
    if (version == "HTTP/1.1") {
      head_.version_minor = 1;
    } else if (version == "HTTP/1.0") {
      head_.version_minor = 0;
    } else {
      Fail(version.substr(0, 5) == "HTTP/" ? 505 : 400, "unsupported HTTP version");
      return false;
    }
  } else {
    if (first.size() < 12 || first.substr(0, 7) != "HTTP/1." || (first[7] != '0' && first[7] != '1') ||
        first[8] != ' ') {
      Fail(502, "malformed status line");
      return false;
    }
    head_.version_minor = first[7] - '0';
    uint64_t status = 0;
    if (!ParseDecimal(first.substr(9, 3), &status) || status < 100 || status > 999) {
      Fail(502, "malformed status code");
      return false;
    }
    head_.status = static_cast<int>(status);
    if (first.size() > 13) head_.reason = std::string(first.substr(13));
  }

  size_t count = 0;
  while (eol != std::string_view::npos) {
    const size_t start = eol + 2;
    eol = text.find("\r\n", start);
    const std::string_view line = text.substr(start, eol == std::string_view::npos ? std::string_view::npos : eol - start);
    if (line.empty()) continue;
    if (line[0] == ' ' || line[0] == '\t') {
      Fail(kind_ == Kind::kRequest ? 400 : 502, "obsolete header folding");
      return false;
    }
    const size_t colon = line.find(':');
    if (colon == std::string_view::npos || !IsToken(line.substr(0, colon))) {
      Fail(kind_ == Kind::kRequest ? 400 : 502, "malformed header");
      return false;
    }
    if (++count > 100) {
      Fail(431, "too many headers");
      return false;
    }
    head_.headers.push_back({std::string(line.substr(0, colon)), std::string(Trim(line.substr(colon + 1)))});
  }
  return true;
}

bool HttpParser::DetermineBody() {
  const int bad = kind_ == Kind::kRequest ? 400 : 502;
  const std::string* te = head_.Find("transfer-encoding");

  // Content-Length: every value (across repeated headers or a list) must agree.
  bool has_length = false;
  uint64_t length = 0;
  for (const Header& h : head_.headers) {
    if (!EqualsIgnoreCase(h.name, "content-length")) continue;
    bool ok = true;
    ForEachListItem(h.value, [&](std::string_view item) {
      uint64_t v = 0;
      if (!ParseDecimal(item, &v) || (has_length && v != length)) ok = false;
      has_length = true;
      length = v;
    });
    if (!ok) {
      Fail(bad, "invalid or conflicting Content-Length");
      return false;
    }
  }

  if (kind_ == Kind::kResponse) {
    const int s = head_.status;
    if (EqualsIgnoreCase(request_method_, "HEAD") || (s >= 100 && s < 200) || s == 204 || s == 304) {
      body_kind_ = BodyKind::kNone;
      state_ = State::kDone;
      return true;
    }
  }

  if (te != nullptr) {
    // A message with both headers is a classic request smuggling vector.
    if (has_length && kind_ == Kind::kRequest) {
      Fail(400, "both Transfer-Encoding and Content-Length");
      return false;
    }
    std::string last;
    for (const Header& h : head_.headers) {
      if (EqualsIgnoreCase(h.name, "transfer-encoding")) {
        ForEachListItem(h.value, [&](std::string_view item) { last = std::string(item); });
      }
    }
    if (EqualsIgnoreCase(last, "chunked")) {
      body_kind_ = BodyKind::kChunked;
      chunk_state_ = ChunkState::kSizeLine;
      state_ = State::kBody;
      return true;
    }
    if (kind_ == Kind::kRequest) {
      Fail(501, "unsupported transfer coding");
      return false;
    }
    body_kind_ = BodyKind::kUntilClose;
    state_ = State::kBody;
    return true;
  }

  if (has_length) {
    content_length_ = remaining_ = length;
    body_kind_ = length == 0 ? BodyKind::kNone : BodyKind::kLength;
    state_ = length == 0 ? State::kDone : State::kBody;
    return true;
  }
  if (kind_ == Kind::kRequest) {
    body_kind_ = BodyKind::kNone;
    state_ = State::kDone;
  } else {
    body_kind_ = BodyKind::kUntilClose;
    state_ = State::kBody;
  }
  return true;
}

size_t HttpParser::FeedBody(std::string_view data) {
  switch (body_kind_) {
    case BodyKind::kNone:
      state_ = State::kDone;
      return 0;
    case BodyKind::kUntilClose:
      return data.size();
    case BodyKind::kLength: {
      const size_t n = static_cast<size_t>(std::min<uint64_t>(remaining_, data.size()));
      remaining_ -= n;
      if (remaining_ == 0) state_ = State::kDone;
      return n;
    }
    case BodyKind::kChunked:
      break;
  }

  const int bad = kind_ == Kind::kRequest ? 400 : 502;
  size_t i = 0;
  while (i < data.size() && state_ == State::kBody) {
    switch (chunk_state_) {
      case ChunkState::kSizeLine:
      case ChunkState::kTrailerLine: {
        const char c = data[i++];
        if (chunk_state_ == ChunkState::kTrailerLine && ++trailer_bytes_ > 16384) {
          Fail(bad, "chunked trailer too large");
          return i;
        }
        if (c != '\n') {
          if (line_.size() > 4096) {
            Fail(bad, "chunk size line too long");
            return i;
          }
          line_.push_back(c);
          break;
        }
        if (line_.empty() || line_.back() != '\r') {
          Fail(bad, "bare LF in chunked body");
          return i;
        }
        line_.pop_back();
        if (chunk_state_ == ChunkState::kTrailerLine) {
          if (line_.empty()) state_ = State::kDone;  // Blank line ends the trailers.
          line_.clear();
          break;
        }
        // Chunk size in hex, optionally followed by ";extensions".
        const std::string_view size_text = Trim(std::string_view(line_).substr(0, line_.find(';')));
        uint64_t size = 0;
        if (size_text.empty() || size_text.size() > 15) {
          Fail(bad, "invalid chunk size");
          return i;
        }
        for (char h : size_text) {
          int d;
          if (h >= '0' && h <= '9') d = h - '0';
          else if (h >= 'a' && h <= 'f') d = h - 'a' + 10;
          else if (h >= 'A' && h <= 'F') d = h - 'A' + 10;
          else {
            Fail(bad, "invalid chunk size");
            return i;
          }
          size = size * 16 + static_cast<uint64_t>(d);
        }
        line_.clear();
        if (size == 0) {
          chunk_state_ = ChunkState::kTrailerLine;
        } else {
          remaining_ = size;
          content_length_ += size;
          chunk_state_ = ChunkState::kData;
        }
        break;
      }
      case ChunkState::kData: {
        const size_t n = static_cast<size_t>(std::min<uint64_t>(remaining_, data.size() - i));
        i += n;
        remaining_ -= n;
        if (remaining_ == 0) chunk_state_ = ChunkState::kDataCr;
        break;
      }
      case ChunkState::kDataCr:
        if (data[i++] != '\r') {
          Fail(bad, "missing CRLF after chunk");
          return i;
        }
        chunk_state_ = ChunkState::kDataLf;
        break;
      case ChunkState::kDataLf:
        if (data[i++] != '\n') {
          Fail(bad, "missing CRLF after chunk");
          return i;
        }
        chunk_state_ = ChunkState::kSizeLine;
        break;
    }
  }
  return i;
}

void HttpParser::OnEof() {
  if (state_ == State::kBody && body_kind_ == BodyKind::kUntilClose) {
    state_ = State::kDone;
  } else if (state_ == State::kHead && head_buf_.empty()) {
    // Clean close between messages: nothing lost.
  } else if (state_ != State::kDone && state_ != State::kError) {
    Fail(kind_ == Kind::kRequest ? 400 : 502, "connection closed mid-message");
  }
}

}  // namespace hlb
