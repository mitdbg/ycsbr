#pragma once

#include <chrono>
#include <cstdlib>
#include <vector>

#include "ycsbr/benchmark.h"
#include "ycsbr/meter.h"

namespace ycsbr {
namespace impl {

class MetricsTracker {
 public:
  MetricsTracker(size_t num_reads_hint = 100000,
                 size_t num_writes_hint = 100000, size_t num_scans_hint = 1000)
      : reads_(num_reads_hint),
        writes_(num_writes_hint),
        scans_(num_scans_hint) {}

  void RecordRead(std::chrono::nanoseconds run_time, size_t read_bytes) {
    reads_.Record(run_time, read_bytes);
  }

  void RecordWrite(std::chrono::nanoseconds run_time, size_t write_bytes) {
    writes_.Record(run_time, write_bytes);
  }

  void RecordScan(std::chrono::nanoseconds run_time, size_t scanned_bytes, size_t scanned_amount) {
    scans_.RecordMultiple(run_time, scanned_bytes, scanned_amount);
  }

  BenchmarkResult Finalize(std::chrono::nanoseconds total_run_time) {
    return BenchmarkResult(total_run_time, reads_.Freeze(), writes_.Freeze(),
                           scans_.Freeze());
  }

 private:
  Meter reads_, writes_, scans_;
};

}  // namespace impl
}  // namespace ycsbr
