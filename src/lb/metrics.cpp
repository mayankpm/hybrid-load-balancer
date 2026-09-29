#include "lb/metrics.h"

namespace hlb {

int LatencyHistogram::BucketFor(uint64_t v) {
  if (v < kSubBuckets) return static_cast<int>(v);
  int msb = 63;
  while (!(v >> msb)) msb--;
  // Top three bits below the leading one pick the sub-bucket.
  const int sub = static_cast<int>((v >> (msb - 3)) & (kSubBuckets - 1));
  return (msb - 2) * kSubBuckets + sub;
}

uint64_t LatencyHistogram::BucketMidpoint(int bucket) {
  if (bucket < kSubBuckets) return static_cast<uint64_t>(bucket);
  const int msb = bucket / kSubBuckets + 2;
  const uint64_t sub = static_cast<uint64_t>(bucket % kSubBuckets);
  const uint64_t lo = (uint64_t{1} << msb) | (sub << (msb - 3));
  const uint64_t width = uint64_t{1} << (msb - 3);
  return lo + width / 2;
}

void LatencyHistogram::Record(uint64_t micros) {
  buckets_[static_cast<size_t>(BucketFor(micros))].fetch_add(1, std::memory_order_relaxed);
  count_.fetch_add(1, std::memory_order_relaxed);
  sum_.fetch_add(micros, std::memory_order_relaxed);
}

uint64_t LatencyHistogram::Count() const { return count_.load(std::memory_order_relaxed); }

double LatencyHistogram::MeanMicros() const {
  const uint64_t n = Count();
  return n == 0 ? 0.0 : static_cast<double>(sum_.load(std::memory_order_relaxed)) / static_cast<double>(n);
}

uint64_t LatencyHistogram::Percentile(double q) const {
  uint64_t total = 0;
  for (const auto& b : buckets_) total += b.load(std::memory_order_relaxed);
  if (total == 0) return 0;
  const uint64_t rank = static_cast<uint64_t>(q * static_cast<double>(total - 1)) + 1;
  uint64_t seen = 0;
  for (int i = 0; i < kBuckets; i++) {
    seen += buckets_[static_cast<size_t>(i)].load(std::memory_order_relaxed);
    if (seen >= rank) return BucketMidpoint(i);
  }
  return BucketMidpoint(kBuckets - 1);
}

}  // namespace hlb
