#pragma once

#include <algorithm>
#include <chrono>
#include <vector>

namespace ycsbr {

class FrozenMeter;

class Meter {
 public:
  Meter(size_t num_entries_hint = 100000) : bytes_(0) {
    latencies_.reserve(num_entries_hint);
  }

  void Record(std::chrono::nanoseconds run_time, size_t bytes) {
    latencies_.push_back(run_time);
    bytes_ += bytes;
  }

  FrozenMeter Freeze();

 private:
  friend class FrozenMeter;
  size_t bytes_;
  std::vector<std::chrono::nanoseconds> latencies_;
};

class FrozenMeter {
 public:
  FrozenMeter() : bytes_(0) {}
  size_t TotalBytes() const { return bytes_; }
  size_t NumOperations() const { return latencies_.size(); }

 private:
  friend class Meter;
  FrozenMeter(Meter meter)
      : bytes_(std::move(meter.bytes_)),
        latencies_(std::move(meter.latencies_)) {}
  const size_t bytes_;
  const std::vector<std::chrono::nanoseconds> latencies_;
};

inline FrozenMeter Meter::Freeze() {
  std::sort(latencies_.begin(), latencies_.end());
  return FrozenMeter(*this);
}

}  // namespace ycsbr
