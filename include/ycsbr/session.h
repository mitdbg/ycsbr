#pragma once

#include <memory>
#include <vector>

#include "benchmark_result.h"
#include "impl/thread_pool.h"
#include "trace.h"

namespace ycsbr {

// Options used to configure Session-based trace replays and workload runs.
struct RunOptions {
  // Used to configure latency sampling. Sampling is done by individual workers,
  // and all workers will share the same sampling configuration. If this is set
  // to 1, a worker will measure the latency of all of its requests. If set to
  // some value `n`, a worker will measure every `n`-th request's latency.
  size_t latency_sample_period = 10;

  // If set to true, the benchmark will fail if any request fails. This should
  // only be used if you expect all requests to succeed (e.g., there are no
  // negative lookups and no updates of non-existent keys).
  bool expect_request_success = false;

  // If set to true, the benchmark will fail if any scan requests return fewer
  // (or more) records than requested. This should only be used if you expect
  // all scan amounts to be "valid".
  bool expect_scan_amount_found = false;
};

template <class DatabaseInterface>
class Session {
 public:
  // Starts a benchmark session that will run workloads with `num_threads`
  // threads. If a core map is provided, the threads will be pinned to the cores
  // specified in `core_map`. All worker threads will call
  // `DatabaseInterface::InitializeWorker()` when they start up. After the
  // worker threads start up, `DatabaseInterface::InitializeDatabase()` will be
  // called once.
  Session(size_t num_threads,
          const std::vector<size_t>& core_map = std::vector<size_t>());

  // Calls `DatabaseInterface::DeleteDatabase()` and then terminates the worker
  // threads. All worker threads will call `DatabaseInterface::ShutdownWorker()`
  // before terminating. Once a session has been terminated, it cannot be
  // restarted.
  void Terminate();

  ~Session();
  Session(Session&&) = default;
  Session& operator=(Session&&) = default;

  DatabaseInterface& db();
  const DatabaseInterface& db() const;

  // Replays the provided bulk load trace. Note that bulk loads always run on
  // one thread.
  BenchmarkResult ReplayBulkLoadTrace(const BulkLoadTrace& load);

  // Replays the provided trace. The trace's requests will be split among all
  // the worker threads.
  BenchmarkResult ReplayTrace(const Trace& trace,
                              const RunOptions& options = RunOptions());

  // Runs a custom workload against the database.
  template <class CustomWorkload>
  BenchmarkResult RunWorkload(const CustomWorkload& workload,
                              const RunOptions& options = RunOptions());

 private:
  DatabaseInterface db_;
  std::unique_ptr<impl::ThreadPool> threads_;
  size_t num_threads_;
};

}  // namespace ycsbr

#include "impl/session-inl.h"
