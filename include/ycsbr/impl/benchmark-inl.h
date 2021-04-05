// Implementation of declarations in yscbr/benchmark.h. Do not include this
// header!
#include <functional>
#include <memory>
#include <thread>

#include "flag.h"
#include "tracking.h"
#include "worker.h"

namespace ycsbr {
namespace impl {

// Used to ensure the database is "deleted" if an exception is thrown.
class CallOnExit {
 public:
  explicit CallOnExit(std::function<void()> fn)
      : fn_(std::move(fn)), cancelled_(false) {}

  void Cancel() { cancelled_ = true; }

  ~CallOnExit() {
    if (!cancelled_) {
      fn_();
    }
  }

 private:
  std::function<void()> fn_;
  bool cancelled_;
};

template <class DatabaseInterface>
inline BenchmarkResult ReplayTraceImpl(DatabaseInterface& db,
                                       const Trace& trace,
                                       const BulkLoadTrace* load,
                                       const BenchmarkOptions& options) {
  if (options.num_threads == 0) {
    throw std::invalid_argument("Must use at least 1 thread.");
  }

  Flag start_running;
  std::vector<std::unique_ptr<Worker<DatabaseInterface>>> workers;
  workers.reserve(options.num_threads);

  // Split up the requests.
  const size_t min_requests_per_worker = trace.size() / options.num_threads;
  size_t leftover_requests = trace.size() % options.num_threads;
  size_t next_offset = 0;
  for (size_t worker_id = 0; worker_id < options.num_threads; ++worker_id) {
    size_t num_requests = min_requests_per_worker;
    if (leftover_requests > 0) {
      ++num_requests;
      --leftover_requests;
    }
    std::optional<unsigned> core =
        options.pin_to_core_map.size() == options.num_threads
            ? std::optional<unsigned>(options.pin_to_core_map[worker_id])
            : std::optional<unsigned>();
    workers.emplace_back(std::make_unique<Worker<DatabaseInterface>>(
        &db, &trace, next_offset, num_requests, &start_running, core,
        options.latency_sample_period, options.expect_request_success,
        options.expect_scan_amount_found));
    next_offset += num_requests;
  }

  // Busy wait for the workers to start up.
  for (const auto& worker : workers) {
    while (!worker->IsReady()) {
    }
  }

  if (load != nullptr) {
    // The bulk load will run on this thread, so we need to call
    // `InitializeWorker()` here.
    db.InitializeWorker(std::this_thread::get_id());
  }

  // Initialize the database before starting the trace replay.
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });

  // Run the bulk load.
  if (load != nullptr) {
    db.BulkLoad(*load);
  }

  // Run the trace replay.
  const auto start = std::chrono::steady_clock::now();
  start_running.Raise();
  for (auto& worker : workers) {
    worker->Wait();
  }
  db.DeleteDatabase();
  guard.Cancel();
  const auto end = std::chrono::steady_clock::now();

  if (load != nullptr) {
    db.ShutdownWorker(std::this_thread::get_id());
  }

  // Retrieve the results.
  std::vector<MetricsTracker> results;
  results.reserve(options.num_threads);
  for (auto& worker : workers) {
    results.emplace_back(std::move(*worker).GetResults());
  }

  return MetricsTracker::FinalizeGroup(end - start, std::move(results));
}

}  // namespace impl

template <class DatabaseInterface>
inline BenchmarkResult ReplayTrace(DatabaseInterface& db, const Trace& trace,
                                   const BulkLoadTrace* load,
                                   const BenchmarkOptions& options) {
  return impl::ReplayTraceImpl(db, trace, load, options);
}

template <class DatabaseInterface>
inline BenchmarkResult ReplayTrace(DatabaseInterface& db,
                                   const BulkLoadTrace& load) {
  db.InitializeWorker(std::this_thread::get_id());
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  const auto start = std::chrono::steady_clock::now();
  db.BulkLoad(load);
  db.DeleteDatabase();
  guard.Cancel();
  const auto end = std::chrono::steady_clock::now();
  db.ShutdownWorker(std::this_thread::get_id());

  const auto run_time = end - start;
  Meter loading;
  loading.RecordMultiple(run_time, load.DatasetSizeBytes(), load.size());
  return BenchmarkResult(run_time, 0, FrozenMeter(),
                         std::move(loading).Freeze(), FrozenMeter(), 0, 0, 0);
}

}  // namespace ycsbr
