#include <atomic>

#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "ycsbr/ycsbr.h"

namespace {

using namespace ycsbr;

class TestDatabaseInterface {
 public:
  void InitializeWorker(const std::thread::id& worker_id) {
    ++initialize_worker_calls;
  }
  void ShutdownWorker(const std::thread::id& worker_id) {
    ++shutdown_worker_calls;
  }
  void InitializeDatabase() { ++initialize_calls; }
  void DeleteDatabase() { ++delete_calls; }

  void BulkLoad(const BulkLoadTrace& load) { ++bulk_load_calls; }

  bool Update(Request::Key key, const char* value, size_t value_size) {
    ++update_calls;
    return true;
  }

  bool Insert(Request::Key key, const char* value, size_t value_size) {
    ++insert_calls;
    return true;
  }

  bool Read(Request::Key key, std::string* value_out) {
    ++read_calls;
    return true;
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(Request::Key key, size_t amount,
            std::vector<std::pair<Request::Key, std::string>>* scan_out) {
    ++scan_calls;
    for (size_t i = 0; i < amount; ++i) {
      scan_out->emplace_back(static_cast<Request::Key>(key), "");
    }
    return true;
  }

  std::atomic<size_t> initialize_calls = 0;
  std::atomic<size_t> delete_calls = 0;
  std::atomic<size_t> bulk_load_calls = 0;
  std::atomic<size_t> update_calls = 0;
  std::atomic<size_t> insert_calls = 0;
  std::atomic<size_t> read_calls = 0;
  std::atomic<size_t> scan_calls = 0;
  std::atomic<size_t> initialize_worker_calls = 0;
  std::atomic<size_t> shutdown_worker_calls = 0;
};

TEST_F(TraceLoadA, BenchBulkLoad) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  for (const auto& req : load) {
    ASSERT_NE(req.value, nullptr);
    ASSERT_EQ(req.value_size, options.value_size);
  }

  TestDatabaseInterface db;
  auto res = ReplayTrace(db, load);
  ASSERT_EQ(db.initialize_worker_calls, 1);
  ASSERT_EQ(db.shutdown_worker_calls, 1);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 1);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_EQ(db.scan_calls, 0);
}

TEST_F(TraceLoadA, BenchLoadRunA) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  const Trace trace = BulkLoadTrace::LoadFromFile(trace_file, options);
  ASSERT_EQ(load.size(), trace_size);
  ASSERT_EQ(trace.size(), trace_size);

  TestDatabaseInterface db;
  auto res = ReplayTrace(db, trace, &load);
  // The bulk load and workload run on different threads.
  ASSERT_EQ(db.initialize_worker_calls, 2);
  ASSERT_EQ(db.shutdown_worker_calls, 2);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 1);
  ASSERT_EQ(db.insert_calls, trace_size);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_EQ(db.scan_calls, 0);
}

TEST_F(TraceReplayA, BenchRunA) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  TestDatabaseInterface db;
  auto res = ReplayTrace(db, trace);
  ASSERT_EQ(db.initialize_worker_calls, 1);
  ASSERT_EQ(db.shutdown_worker_calls, 1);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_TRUE(db.update_calls > 0);
  ASSERT_TRUE(db.read_calls > 0);
  ASSERT_EQ(db.scan_calls, 0);
  ASSERT_EQ(db.update_calls + db.read_calls, trace_size);
}

TEST_F(TraceReplayE, BenchRunE) {
  // Workload E is a range scan workload with some inserts.
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  TestDatabaseInterface db;
  auto res = ReplayTrace(db, trace);
  ASSERT_EQ(db.initialize_worker_calls, 1);
  ASSERT_EQ(db.shutdown_worker_calls, 1);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_TRUE(db.insert_calls > 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_TRUE(db.scan_calls > 0);
}

TEST_F(TraceReplayA, MultithreadedRun) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  BenchmarkOptions boptions;
  boptions.num_threads = 5;
  TestDatabaseInterface db;
  auto res = ReplayTrace(db, trace, nullptr, boptions);
  ASSERT_EQ(db.initialize_worker_calls, 5);
  ASSERT_EQ(db.shutdown_worker_calls, 5);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_TRUE(db.update_calls > 0);
  ASSERT_TRUE(db.read_calls > 0);
  ASSERT_EQ(db.scan_calls, 0);
  ASSERT_EQ(db.update_calls + db.read_calls, trace_size);
  ASSERT_EQ(res.Reads().NumOperations(), db.read_calls);
  ASSERT_EQ(res.Writes().NumOperations(), db.update_calls);
}

TEST_F(TraceReplayA, NoThreads) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  BenchmarkOptions boptions;
  boptions.num_threads = 0;
  TestDatabaseInterface db;
  ASSERT_THROW(ReplayTrace(db, trace, nullptr, boptions),
               std::invalid_argument);
  ASSERT_EQ(db.initialize_worker_calls, 0);
  ASSERT_EQ(db.shutdown_worker_calls, 0);
  ASSERT_EQ(db.initialize_calls, 0);
  ASSERT_EQ(db.delete_calls, 0);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_EQ(db.scan_calls, 0);
}

TEST_F(TraceLoadA, NoThreads) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  BenchmarkOptions boptions;
  boptions.num_threads = 0;
  TestDatabaseInterface db;

  ASSERT_THROW(ReplayTrace(db, trace, nullptr, boptions),
               std::invalid_argument);
  ASSERT_EQ(db.initialize_worker_calls, 0);
  ASSERT_EQ(db.shutdown_worker_calls, 0);
  ASSERT_EQ(db.initialize_calls, 0);
  ASSERT_EQ(db.delete_calls, 0);
  ASSERT_EQ(db.bulk_load_calls, 0);

  ASSERT_THROW(ReplayTrace(db, trace, &load, boptions), std::invalid_argument);
  ASSERT_EQ(db.initialize_worker_calls, 0);
  ASSERT_EQ(db.shutdown_worker_calls, 0);
  ASSERT_EQ(db.initialize_calls, 0);
  ASSERT_EQ(db.delete_calls, 0);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_EQ(db.scan_calls, 0);
}

}  // namespace
