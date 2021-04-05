#pragma once

#include <chrono>
#include <iostream>

#include "meter.h"

namespace ycsbr {

class BenchmarkResult {
 public:
  BenchmarkResult(std::chrono::nanoseconds total_run_time);
  BenchmarkResult(std::chrono::nanoseconds total_run_time, uint32_t read_xor,
                  FrozenMeter reads, FrozenMeter writes, FrozenMeter scans,
                  size_t failed_reads, size_t failed_writes,
                  size_t failed_scans);

  template <typename Units>
  Units RunTime() const;

  double ThroughputMopsPerSecond() const;
  double ThroughputReadMiBPerSecond() const;
  double ThroughputWriteMiBPerSecond() const;

  const FrozenMeter& Reads() const { return reads_; }
  const FrozenMeter& Writes() const { return writes_; }
  const FrozenMeter& Scans() const { return scans_; }

  size_t NumFailedReads() { return failed_reads_; }
  size_t NumFailedWrites() { return failed_writes_; }
  size_t NumFailedScans() { return failed_scans_; }

  static void PrintCSVHeader(std::ostream& out);
  void PrintAsCSV(std::ostream& out, bool print_header = true) const;

 private:
  friend std::ostream& operator<<(std::ostream& out,
                                  const BenchmarkResult& res);
  const std::chrono::nanoseconds run_time_;
  const FrozenMeter reads_, writes_, scans_;
  const size_t failed_reads_, failed_writes_, failed_scans_;
  const uint32_t read_xor_;
};

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res);

}  // namespace ycsbr

#include "impl/benchmark_result-inl.h"
