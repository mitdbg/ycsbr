// Implementation of declarations in yscbr/benchmark.h. Do not include this
// header!
#include <functional>

#include "tracking.h"

namespace ycsbr {
namespace impl {

class CallOnExit {
 public:
  explicit CallOnExit(std::function<void()> fn) : fn_(std::move(fn)) {}
  ~CallOnExit() { fn_(); }

 private:
  std::function<void()> fn_;
};

template <typename Callable>
inline std::pair<bool, std::chrono::nanoseconds> MeasureRunTime(
    Callable callable) {
  const auto start = std::chrono::steady_clock::now();
  bool succeeded = callable();
  const auto end = std::chrono::steady_clock::now();
  return {succeeded, end - start};
}

template <class DatabaseInterface>
inline BenchmarkResult RunTimedWorkloadImpl(DatabaseInterface& db,
                                            const Workload& workload) {
  MetricsTracker tracker;

  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  auto start = std::chrono::steady_clock::now();
  for (const auto& req : workload) {
    switch (req.op) {
      case Request::Operation::kRead: {
        value_out.clear();
        auto res = MeasureRunTime(
            [&db, &req, &value_out]() { return db.Read(req.key, &value_out); });
        if (!res.first) {
          throw std::runtime_error(
              "Failed to read a key requested by the benchmark!");
        }
        tracker.RecordRead(res.second, value_out.size());
        break;
      }

      case Request::Operation::kInsert: {
        auto res = MeasureRunTime([&db, &req]() {
          return db.Insert(req.key, req.value, req.value_size);
        });
        if (!res.first) {
          throw std::runtime_error(
              "Failed to insert a key requested by the benchmark!");
        }
        // Inserts count the whole record size, since this should be the first
        // time the entire record is written to the DB.
        tracker.RecordWrite(res.second, req.value_size + sizeof(req.key));
        break;
      }

      case Request::Operation::kUpdate: {
        auto res = MeasureRunTime([&db, &req]() {
          return db.Update(req.key, req.value, req.value_size);
        });
        if (!res.first) {
          throw std::runtime_error(
              "Failed to update a key requested by the benchmark!");
        }
        // Updates only record the value size, since the key should already
        // exist in the DB.
        tracker.RecordWrite(res.second, req.value_size);
        break;
      }

      case Request::Operation::kScan: {
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        auto res = MeasureRunTime([&db, &req, &scan_out]() {
          return db.Scan(req.key, req.scan_amount, &scan_out);
        });
        if (!res.first || scan_out.size() != req.scan_amount) {
          throw std::runtime_error(
              "Failed to scan a key range requested by the benchmark!");
        }
        size_t scanned_bytes = 0;
        for (const auto& entry : scan_out) {
          scanned_bytes += entry.second.size();
        }
        // NOTE: Will need to revisit scan metrics.
        tracker.RecordScan(res.second, scanned_bytes);
        break;
      }

      default:
        throw std::runtime_error("Unrecognized request operation!");
    }
  }
  auto end = std::chrono::steady_clock::now();
  return tracker.Finalize(end - start);
}

}  // namespace impl

template <class DatabaseInterface>
inline BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                        const Workload& workload) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  return impl::RunTimedWorkloadImpl(db, workload);
}

template <class DatabaseInterface>
inline BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                        const BulkLoadWorkload& load,
                                        const Workload& workload) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  db.BulkLoad(load);
  return impl::RunTimedWorkloadImpl(db, workload);
}

template <class DatabaseInterface>
inline BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                        const BulkLoadWorkload& load) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  auto start = std::chrono::steady_clock::now();
  db.BulkLoad(load);
  auto end = std::chrono::steady_clock::now();
  return BenchmarkResult(end - start);
}

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time)
    : BenchmarkResult(total_run_time, FrozenMeter(), FrozenMeter(),
                      FrozenMeter()) {}

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time,
                                        FrozenMeter reads, FrozenMeter writes,
                                        FrozenMeter scans)
    : run_time_(total_run_time),
      reads_(reads),
      writes_(writes),
      scans_(scans) {}

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

inline size_t BenchmarkResult::NumReads() const {
  return reads_.NumOperations();
}

inline size_t BenchmarkResult::NumWrites() const {
  return writes_.NumOperations();
}

inline std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res) {
  out << "Total run time (us):  "
      << res.RunTime<std::chrono::microseconds>().count() << std::endl;
  out << "Total reads:          " << res.NumReads() << std::endl;
  out << "Total writes:         " << res.NumWrites() << std::endl;
  out << "Throughput (Mops/s):  " << res.ThroughputMopsPerSecond();
  return out;
}

}  // namespace ycsbr
