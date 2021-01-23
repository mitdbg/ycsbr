#pragma once

#include <chrono>
#include <cstdlib>
#include <vector>

#include "../benchmark.h"
#include "../meter.h"

namespace ycsbr {
namespace impl {

class MetricsTracker {
 public:
  MetricsTracker(size_t num_reads_hint = 100000,
                 size_t num_writes_hint = 100000, size_t num_scans_hint = 1000)
      : reads_(num_reads_hint),
        writes_(num_writes_hint),
        scans_(num_scans_hint),
        read_xor_(0) {}

  void RecordRead(std::chrono::nanoseconds run_time, size_t read_bytes) {
    reads_.Record(run_time, read_bytes);
  }

  void RecordWrite(std::chrono::nanoseconds run_time, size_t write_bytes) {
    writes_.Record(run_time, write_bytes);
  }

  void RecordScan(std::chrono::nanoseconds run_time, size_t scanned_bytes,
                  size_t scanned_amount) {
    scans_.RecordMultiple(run_time, scanned_bytes, scanned_amount);
  }

  void SetReadXOR(uint32_t value) { read_xor_ = value; }

  BenchmarkResult Finalize(std::chrono::nanoseconds total_run_time) {
    return BenchmarkResult(
        total_run_time, read_xor_, std::move(reads_).Freeze(),
        std::move(writes_).Freeze(), std::move(scans_).Freeze());
  }

  static BenchmarkResult FinalizeGroup(std::chrono::nanoseconds total_run_time,
                                       std::vector<MetricsTracker> trackers) {
    std::vector<Meter> reads, writes, scans;
    uint32_t read_xor = 0;
    reads.reserve(trackers.size());
    writes.reserve(trackers.size());
    scans.reserve(trackers.size());

    for (auto& tracker : trackers) {
      reads.emplace_back(std::move(tracker.reads_));
      writes.emplace_back(std::move(tracker.writes_));
      scans.emplace_back(std::move(tracker.scans_));
      read_xor ^= tracker.read_xor_;
    }

    return BenchmarkResult(total_run_time, read_xor,
                           Meter::FreezeGroup(std::move(reads)),
                           Meter::FreezeGroup(std::move(writes)),
                           Meter::FreezeGroup(std::move(scans)));
  }

 private:
  Meter reads_, writes_, scans_;
  uint32_t read_xor_;
};

}  // namespace impl
}  // namespace ycsbr
