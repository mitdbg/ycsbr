// Implementation of declarations in benchmark.h. Do not include this header!
#include <functional>

namespace yr {
namespace impl {

class CallOnExit {
 public:
  explicit CallOnExit(std::function<void()> fn) : fn_(std::move(fn)) {}
  ~CallOnExit() { fn_(); }

 private:
  std::function<void()> fn_;
};

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkloadImpl(DatabaseInterface& db,
                                     const Workload& workload) {
  uint64_t reads = 0;
  uint64_t writes = 0;
  size_t read_xor = 0;
  size_t scan_xor = 0;

  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  auto start = std::chrono::steady_clock::now();
  for (const auto& req : workload) {
    switch (req.op) {
      case Request::Operation::kRead:
        value_out.clear();
        if (!db.Read(req.key, &value_out)) {
          throw std::runtime_error(
              "Failed to read a key requested by the benchmark!");
        }
        read_xor ^= value_out.size();
        reads += 1;
        break;

      case Request::Operation::kInsert:
        value_out.clear();
        if (!db.Insert(req.key, req.value, req.value_size)) {
          throw std::runtime_error(
              "Failed to insert a key requested by the benchmark!");
        }
        writes += 1;
        break;

      case Request::Operation::kUpdate:
        value_out.clear();
        if (!db.Update(req.key, req.value, req.value_size)) {
          throw std::runtime_error(
              "Failed to update a key requested by the benchmark!");
        }
        writes += 1;
        break;

      case Request::Operation::kScan:
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        if (!db.Scan(req.key, req.scan_amount, &scan_out) ||
            scan_out.size() != req.scan_amount) {
          throw std::runtime_error(
              "Failed to scan a key range requested by the benchmark!");
        }
        scan_xor ^= scan_out.size();
        reads += scan_out.size();
        break;

      default:
        throw std::runtime_error("Unrecognized request operation!");
    }
  }
  auto end = std::chrono::steady_clock::now();

  return {start, end, reads, writes, read_xor ^ scan_xor};
}

}  // namespace impl

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const Workload& workload) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  return impl::RunTimedWorkloadImpl(db, workload);
}

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const BulkLoadWorkload& load,
                                 const Workload& workload) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  db.BulkLoad(load);
  return impl::RunTimedWorkloadImpl(db, workload);
}

template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const BulkLoadWorkload& load) {
  db.InitializeDatabase();
  impl::CallOnExit guard([&db]() { db.DeleteDatabase(); });
  auto start = std::chrono::steady_clock::now();
  db.BulkLoad(load);
  auto end = std::chrono::steady_clock::now();
  return {start, end, 0, load.size(), 0};
}

BenchmarkResult::BenchmarkResult()
    : run_time_(std::chrono::nanoseconds(0)),
      reads_(0),
      writes_(0),
      read_xor_(0) {}

BenchmarkResult::BenchmarkResult(BenchmarkResult::TimePoint start,
                                 BenchmarkResult::TimePoint end, size_t reads,
                                 size_t writes, size_t read_xor)
    : run_time_(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)),
      reads_(reads),
      writes_(writes),
      read_xor_(read_xor) {}

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

size_t BenchmarkResult::NumReads() const { return reads_; }

size_t BenchmarkResult::NumWrites() const { return writes_; }

size_t BenchmarkResult::ReadRequestChecksum() const { return read_xor_; }

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res) {
  out << "Total run time (us):  "
      << res.RunTime<std::chrono::microseconds>().count() << std::endl;
  out << "Total reads:          " << res.NumReads() << std::endl;
  out << "Total writes:         " << res.NumWrites() << std::endl;
  out << "Read checksum:        " << res.ReadRequestChecksum() << std::endl;
  out << "Throughput (Mops/s):  " << res.ThroughputMopsPerSecond();
  return out;
}

}  // namespace yr
