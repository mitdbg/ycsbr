#include <atomic>

#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "ycsbr/ycsbr.h"

#include "db_interface.h"

namespace {

using namespace ycsbr;

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
