// Lock-free latency histogram: log-linear buckets (8 per power of two, so
// about 12% relative error) over atomic counters. Every worker thread records
// into the same histogram without contention beyond one atomic add.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace hlb {

class LatencyHistogram {
 public:
  void Record(uint64_t micros);
  uint64_t Count() const;
  // Approximate value at quantile q in [0, 1], in microseconds.
  uint64_t Percentile(double q) const;
  double MeanMicros() const;

 private:
  static constexpr int kSubBuckets = 8;
  static constexpr int kBuckets = 64 * kSubBuckets;
  static int BucketFor(uint64_t v);
  static uint64_t BucketMidpoint(int bucket);

  std::array<std::atomic<uint64_t>, kBuckets> buckets_{};
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> sum_{0};
};

}  // namespace hlb
