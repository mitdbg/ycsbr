#pragma once

#include <memory>
#include <vector>

#include "benchmark_result.h"
#include "impl/thread_pool.h"
#include "run_options.h"
#include "trace.h"

namespace ycsbr {

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
