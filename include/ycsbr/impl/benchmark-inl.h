// Implementation of declarations in yscbr/benchmark.h. Do not include this
// header!
#include <functional>
#include <memory>

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
inline BenchmarkResult RunTimedWorkloadImpl(DatabaseInterface& db,
                                            const Workload& workload,
                                            const std::optional<const BulkLoadWorkload>& load,
                                            const BenchmarkOptions& options) {
  if (options.num_threads == 0) {
    throw std::invalid_argument("Must use at least 1 thread.");
  }

  Flag start_running;
  std::vector<std::unique_ptr<Worker<DatabaseInterface>>> workers;
  workers.reserve(options.num_threads);

  // Split up the requests.
  const size_t min_requests_per_worker = workload.size() / options.num_threads;
  size_t leftover_requests = workload.size() % options.num_threads;
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
        &db, &workload, next_offset, num_requests, &start_running, core));
    next_offset += num_requests;
  }

  // Busy wait for the workers to start up.
  for (const auto& worker : workers) {
    while (!worker->IsReady()) {
    }
  }

  if (load.has_value()) {
    // The bulk load will run on this thread, so we need to call
    // `InitializeWorker()` here.
    db.InitializeWorker();
  }

  // Initialize the database before starting the workload.
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });

  // Run the bulk load.
  if (load.has_value()) {
    db.BulkLoad(*load);
  }

  // Run the workload.
  const auto start = std::chrono::steady_clock::now();
  start_running.Raise();
  for (auto& worker : workers) {
    worker->Wait();
  }
  db.DeleteDatabase();
  guard.Cancel();
  const auto end = std::chrono::steady_clock::now();

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
inline BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                        const Workload& workload,
                                        const std::optional<const BulkLoadWorkload>& load,
                                        const BenchmarkOptions& options) {
  return impl::RunTimedWorkloadImpl(db, workload, load, options);
}

template <class DatabaseInterface>
inline BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                        const BulkLoadWorkload& load) {
  db.InitializeWorker();
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  const auto start = std::chrono::steady_clock::now();
  db.BulkLoad(load);
  db.DeleteDatabase();
  guard.Cancel();
  const auto end = std::chrono::steady_clock::now();

  const auto run_time = end - start;
  Meter loading;
  loading.RecordMultiple(run_time, load.DatasetSizeBytes(), load.size());
  return BenchmarkResult(run_time, 0, FrozenMeter(),
                         std::move(loading).Freeze(), FrozenMeter());
}

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time)
    : BenchmarkResult(total_run_time, 0, FrozenMeter(), FrozenMeter(),
                      FrozenMeter()) {}

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time,
                                        uint32_t read_xor, FrozenMeter reads,
                                        FrozenMeter writes, FrozenMeter scans)
    : run_time_(total_run_time),
      reads_(reads),
      writes_(writes),
      scans_(scans),
      read_xor_(read_xor) {}

template <typename Units>
inline Units BenchmarkResult::RunTime() const {
  return std::chrono::duration_cast<Units>(run_time_);
}

inline double BenchmarkResult::ThroughputMopsPerSecond() const {
  uint64_t total_ops =
      reads_.NumOperations() + writes_.NumOperations() + scans_.NumOperations();
  return total_ops /
         (double)std::chrono::duration_cast<std::chrono::microseconds>(
             run_time_)
             .count();
}

inline double BenchmarkResult::ThroughputReadMiBPerSecond() const {
  size_t total_read = reads_.TotalBytes() + scans_.TotalBytes();
  double read_mib = total_read / 1024.0 / 1024.0;
  return read_mib / RunTime<std::chrono::duration<double>>().count();
}

inline double BenchmarkResult::ThroughputWriteMiBPerSecond() const {
  double write_mib = writes_.TotalBytes() / 1024.0 / 1024.0;
  return write_mib / RunTime<std::chrono::duration<double>>().count();
}

inline std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res) {
  out << "Total run time (us):       "
      << res.RunTime<std::chrono::microseconds>().count() << std::endl;
  out << "Total reads:               " << res.Reads().NumOperations()
      << std::endl;
  out << "Total writes:              " << res.Writes().NumOperations()
      << std::endl;
  out << "Total scanned keys:        " << res.Scans().NumOperations()
      << std::endl;
  out << "Throughput (Mops/s):       " << res.ThroughputMopsPerSecond()
      << std::endl;
  out << "Read Throughput (MiB/s):   " << res.ThroughputReadMiBPerSecond()
      << std::endl;
  out << "Write Throughput (MiB/s):  " << res.ThroughputWriteMiBPerSecond()
      << std::endl;
  out << "Read XOR (ignore):         " << res.read_xor_;
  return out;
}

inline void BenchmarkResult::PrintCSVHeader(std::ostream& out) {
  out << "num_reads,num_writes,num_scanned_keys,reads_ns_p99,reads_ns_p50,"
         "writes_ns_p99,writes_ns_p50,mops_per_s,read_mib_per_s,write_mib_per_s"
      << std::endl;
}

inline void BenchmarkResult::PrintAsCSV(std::ostream& out,
                                        bool print_header) const {
  using nanoseconds = std::chrono::nanoseconds;
  if (print_header) {
    PrintCSVHeader(out);
  }
  out << Reads().NumOperations() << ",";
  out << Writes().NumOperations() << ",";
  out << Scans().NumOperations() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << ThroughputMopsPerSecond() << ",";
  out << ThroughputReadMiBPerSecond() << ",";
  out << ThroughputWriteMiBPerSecond() << std::endl;
}

}  // namespace ycsbr
