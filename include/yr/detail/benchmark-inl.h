// Implementation of declarations in benchmark.h. Do not include this header!
#include <functional>

#include "yr/benchmark.h"

namespace {

class CallOnExit {
 public:
  explicit CallOnExit(std::function<void()> fn) : fn_(std::move(fn)) {}
  ~CallOnExit() { fn_(); }

 private:
  std::function<void()> fn_;
};

template <typename DatabaseInterface>
yr::BenchmarkResult RunTimedWorkloadImpl(DatabaseInterface& db,
                                          const yr::Workload& workload) {
  uint64_t reads = 0;
  uint64_t writes = 0;
  yr::Record::Value out_value;

  auto start = std::chrono::steady_clock::now();
  for (const auto& req : workload) {
    if (req.op == yr::Request::Op::kRead) {
      if (!db.Read(req.key, &out_value)) {
        throw std::runtime_error(
            "Failed to read a key requested by the benchmark!");
      }
      reads += 1;

    } else if (req.op == yr::Request::Op::kUpdate) {
      // NOTE: We are using a "dummy" value here.
      if (!db.Update(req.key, writes)) {
        throw std::runtime_error(
            "Failed to update a key given by the benchmark!");
      }
      writes += 1;

    } else {
      throw std::runtime_error("Unhandled request operation!");
    }
  }
  auto end = std::chrono::steady_clock::now();

  return {start, end, reads, writes};
}

}  // namespace

namespace yr {

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const Workload& workload) {
  db.InitializeDatabase();
  CallOnExit guard([&db]() { db.DeleteDatabase(); });
  return RunTimedWorkloadImpl(db, workload);
}

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const RecordsToLoad& records,
                                 const Workload& workload) {
  db.InitializeDatabase();
  CallOnExit guard([&db]() { db.DeleteDatabase(); });
  db.BulkLoad(records);
  return RunTimedWorkloadImpl(db, workload);
}

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const RecordsToLoad& records) {
  db.InitializeDatabase();
  CallOnExit guard([&db]() { db.DeleteDatabase(); });
  auto start = std::chrono::steady_clock::now();
  db.BulkLoad(records);
  auto end = std::chrono::steady_clock::now();
  return {start, end, 0, records.size()};
}

BenchmarkResult::BenchmarkResult()
    : run_time_(std::chrono::nanoseconds(0)), reads_(0), writes_(0) {}

BenchmarkResult::BenchmarkResult(BenchmarkResult::TimePoint start,
                                 BenchmarkResult::TimePoint end, uint64_t reads,
                                 uint64_t writes)
    : run_time_(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)),
      reads_(reads),
      writes_(writes) {}

template <typename Units>
Units BenchmarkResult::RunTime() const {
  return std::chrono::duration_cast<Units>(run_time_);
}

double BenchmarkResult::ThroughputMopsPerSecond() const {
  uint64_t total_ops = reads_ + writes_;
  return total_ops /
         (double)std::chrono::duration_cast<std::chrono::microseconds>(
             run_time_)
             .count();
}

uint64_t BenchmarkResult::NumReads() const { return reads_; }

uint64_t BenchmarkResult::NumWrites() const { return writes_; }

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res) {
  out << "Total run time (us):  "
      << res.RunTime<std::chrono::microseconds>().count() << std::endl;
  out << "Total reads:          " << res.NumReads() << std::endl;
  out << "Total writes:         " << res.NumWrites() << std::endl;
  out << "Throughput (Mops/s):  " << res.ThroughputMopsPerSecond();
  return out;
}

}  // namespace yr
