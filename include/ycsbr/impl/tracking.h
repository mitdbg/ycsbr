#pragma once

#include <chrono>
#include <cstdlib>
#include <optional>
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
        failed_reads_(0),
        failed_writes_(0),
        failed_scans_(0),
        read_xor_(0) {}

  void RecordRead(std::optional<std::chrono::nanoseconds> run_time,
                  size_t read_bytes, bool succeeded) {
    if (succeeded) {
      reads_.Record(run_time, read_bytes);
    } else {
      ++failed_reads_;
    }
  }

  void RecordWrite(std::optional<std::chrono::nanoseconds> run_time,
                   size_t write_bytes, bool succeeded) {
    if (succeeded) {
      writes_.Record(run_time, write_bytes);
    } else {
      ++failed_writes_;
    }
  }

  void RecordScan(std::optional<std::chrono::nanoseconds> run_time,
                  size_t scanned_bytes, size_t scanned_amount, bool succeeded) {
    if (succeeded) {
      scans_.RecordMultiple(run_time, scanned_bytes, scanned_amount);
    } else {
      ++failed_scans_;
    }
  }

  void SetReadXOR(uint32_t value) { read_xor_ = value; }

  BenchmarkResult Finalize(std::chrono::nanoseconds total_run_time) {
    return BenchmarkResult(
        total_run_time, read_xor_, std::move(reads_).Freeze(),
        std::move(writes_).Freeze(), std::move(scans_).Freeze(), failed_reads_,
        failed_writes_, failed_scans_);
  }

  static BenchmarkResult FinalizeGroup(std::chrono::nanoseconds total_run_time,
                                       std::vector<MetricsTracker> trackers) {
    std::vector<Meter> reads, writes, scans;
    size_t failed_reads = 0, failed_writes = 0, failed_scans = 0;
    uint32_t read_xor = 0;
    reads.reserve(trackers.size());
    writes.reserve(trackers.size());
    scans.reserve(trackers.size());

    for (auto& tracker : trackers) {
      reads.emplace_back(std::move(tracker.reads_));
      writes.emplace_back(std::move(tracker.writes_));
      scans.emplace_back(std::move(tracker.scans_));
      read_xor ^= tracker.read_xor_;
      failed_reads += tracker.failed_reads_;
      failed_writes += tracker.failed_writes_;
      failed_scans += tracker.failed_scans_;
    }

    return BenchmarkResult(total_run_time, read_xor,
                           Meter::FreezeGroup(std::move(reads)),
                           Meter::FreezeGroup(std::move(writes)),
                           Meter::FreezeGroup(std::move(scans)), failed_reads,
                           failed_writes, failed_scans);
  }

 private:
  Meter reads_, writes_, scans_;
  size_t failed_reads_, failed_writes_, failed_scans_;
  uint32_t read_xor_;
};

}  // namespace impl
}  // namespace ycsbr
