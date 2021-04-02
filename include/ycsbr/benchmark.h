#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "data.h"
#include "meter.h"

namespace ycsbr {

class BenchmarkResult;

struct BenchmarkOptions {
  // Number of threads used to run the workload (must be at least 1). The
  // requests in the workload will be evenly divided among the worker threads.
  size_t num_threads = 1;

  // Used to specify how to pin the worker threads to physical cores, if
  // desired. The vector's size must be equal to `num_threads`, and
  // `pin_to_core_map[i]` must hold the core id that thread `i` should be pinned
  // to. If `pin_to_core_map` is not of size `num_threads`, the worker threads
  // will not be pinned to any cores.
  std::vector<unsigned> pin_to_core_map;
};

// Runs the specified (timed) workload. If `load` is provided, this function
// will run the bulk load workload before starting the timed workload.
//
// NOTE: Only running the workload is timed. Loading the records is performed by
// calling `BulkLoad()` on the specified `DatabaseInterface`. The bulk load
// always runs on a single thread.
template <class DatabaseInterface>
BenchmarkResult RunTimedWorkload(
    DatabaseInterface& db, 
    const Workload& workload,
    const BulkLoadWorkload* load = nullptr,
    const BenchmarkOptions& options = BenchmarkOptions());

// Measures the time it takes to load the specified records using bulk load.
// NOTE: The bulk load always runs on a single thread.
template <class DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const BulkLoadWorkload& load);

class BenchmarkResult {
 public:
  BenchmarkResult(std::chrono::nanoseconds total_run_time);
  BenchmarkResult(std::chrono::nanoseconds total_run_time, uint32_t read_xor,
                  FrozenMeter reads, FrozenMeter writes, FrozenMeter scans);

  template <typename Units>
  Units RunTime() const;

  double ThroughputMopsPerSecond() const;
  double ThroughputReadMiBPerSecond() const;
  double ThroughputWriteMiBPerSecond() const;

  const FrozenMeter& Reads() const { return reads_; }
  const FrozenMeter& Writes() const { return writes_; }
  const FrozenMeter& Scans() const { return scans_; }

  static void PrintCSVHeader(std::ostream& out);
  void PrintAsCSV(std::ostream& out, bool print_header = true) const;

 private:
  friend std::ostream& operator<<(std::ostream& out,
                                  const BenchmarkResult& res);
  const std::chrono::nanoseconds run_time_;
  const FrozenMeter reads_, writes_, scans_;
  const uint32_t read_xor_;
};

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res);

}  // namespace ycsbr

#include "impl/benchmark-inl.h"
