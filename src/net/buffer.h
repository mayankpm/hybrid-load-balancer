#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace hlb {

// Byte queue with a read offset: appends at the back, consumes from the front,
// and compacts lazily so consuming is O(1).
class Buffer {
 public:
  size_t size() const { return data_.size() - off_; }
  bool empty() const { return size() == 0; }
  const char* data() const { return data_.data() + off_; }
  std::string_view view() const { return {data(), size()}; }

  void Append(std::string_view s) { data_.append(s.data(), s.size()); }
  void Consume(size_t n) {
    off_ += n;
    if (off_ >= data_.size()) {
      Clear();
    } else if (off_ > (64u << 10) && off_ * 2 > data_.size()) {
      data_.erase(0, off_);
      off_ = 0;
    }
  }
  void Clear() {
    data_.clear();
    off_ = 0;
  }

 private:
  std::string data_;
  size_t off_ = 0;
};

}  // namespace hlb
