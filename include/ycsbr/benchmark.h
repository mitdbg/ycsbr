#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "benchmark_result.h"
#include "trace.h"

namespace ycsbr {

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

  // Used to configure latency sampling. Sampling is done by individual workers,
  // and all workers will share the same sampling configuration. If this is set
  // to 1, a worker will measure the latency of all of its requests. If set to
  // some value `n`, a worker will measure every `n`-th request's latency.
  size_t latency_sample_period = 1;

  // If set to true, the benchmark will fail if any request fails. This should
  // only be used if you expect all requests to succeed (e.g., there are no
  // negative lookups and no updates of non-existent keys).
  bool expect_request_success = false;

  // If set to true, the benchmark will fail if any scan requests return fewer
  // (or more) records than requested. This should only be used if you expect
  // all scan amounts to be "valid".
  bool expect_scan_amount_found = false;
};

// Replays the specified trace (as a benchmark). If `load` is provided, this
// function will run the bulk load trace before starting the timed trace.
//
// NOTE: Only running the trace is timed. Loading the records is performed by
// calling `BulkLoad()` on the specified `DatabaseInterface`. The bulk load
// always runs on a single thread.
template <class DatabaseInterface>
BenchmarkResult ReplayTrace(
    DatabaseInterface& db, const Trace& trace,
    const BulkLoadTrace* load = nullptr,
    const BenchmarkOptions& options = BenchmarkOptions());

// Measures the time it takes to load the specified records using bulk load.
// NOTE: The bulk load always runs on a single thread.
template <class DatabaseInterface>
BenchmarkResult ReplayTrace(DatabaseInterface& db, const BulkLoadTrace& load);

}  // namespace ycsbr

#include "impl/benchmark-inl.h"
