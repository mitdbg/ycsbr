#include <atomic>

#include "db_interface.h"
#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "ycsbr/ycsbr.h"

namespace {

using namespace ycsbr;

TEST_F(TraceLoadA, BenchBulkLoad) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  for (const auto& req : load) {
    ASSERT_NE(req.value, nullptr);
    ASSERT_EQ(req.value_size, options.value_size);
  }

  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  auto res = session.ReplayBulkLoadTrace(load);
  session.Terminate();

  ASSERT_EQ(res.Writes().NumOperations(), load.size());
  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().insert_calls, 0);
  ASSERT_EQ(session.db().update_calls, 0);
  ASSERT_EQ(session.db().read_calls, 0);
  ASSERT_EQ(session.db().scan_calls, 0);

  // Make sure the simplified interface works.
  auto res2 = ReplayTrace<TestDatabaseInterface>(load);
  ASSERT_EQ(res2.Writes().NumOperations(), load.size());
}

TEST_F(TraceLoadA, BenchLoadRunA) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  const Trace trace = BulkLoadTrace::LoadFromFile(trace_file, options);
  ASSERT_EQ(load.size(), trace_size);
  ASSERT_EQ(trace.size(), trace_size);

  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(load);
  session.ReplayTrace(trace);
  session.Terminate();
  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().insert_calls, trace_size);
  ASSERT_EQ(session.db().update_calls, 0);
  ASSERT_EQ(session.db().read_calls, 0);
  ASSERT_EQ(session.db().scan_calls, 0);

  // Make sure the simplified interface works.
  auto res = ReplayTrace<TestDatabaseInterface>(trace, &load);
  ASSERT_EQ(res.Writes().NumOperations(), trace_size);
}

TEST_F(TraceReplayA, BenchRunA) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  auto res = ReplayTrace<TestDatabaseInterface>(trace);
  ASSERT_EQ(res.Writes().NumOperations() + res.Reads().NumOperations(),
            trace_size);
}

TEST_F(TraceReplayE, BenchRunE) {
  // Workload E is a range scan workload with some inserts.
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayTrace(trace);
  session.Terminate();

  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 0);
  ASSERT_TRUE(session.db().insert_calls > 0);
  ASSERT_EQ(session.db().update_calls, 0);
  ASSERT_EQ(session.db().read_calls, 0);
  ASSERT_TRUE(session.db().scan_calls > 0);
}

TEST_F(TraceReplayA, MultithreadedRun) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  Session<TestDatabaseInterface> session(5);
  session.Initialize();
  auto res = session.ReplayTrace(trace);
  session.Terminate();

  ASSERT_EQ(session.db().initialize_worker_calls, 5);
  ASSERT_EQ(session.db().shutdown_worker_calls, 5);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 0);
  ASSERT_EQ(session.db().insert_calls, 0);
  ASSERT_TRUE(session.db().update_calls > 0);
  ASSERT_TRUE(session.db().read_calls > 0);
  ASSERT_EQ(session.db().scan_calls, 0);
  ASSERT_EQ(session.db().update_calls + session.db().read_calls, trace_size);
  ASSERT_EQ(res.Reads().NumOperations(), session.db().read_calls);
  ASSERT_EQ(res.Writes().NumOperations(), session.db().update_calls);

  BenchmarkOptions<TestDatabaseInterface> boptions;
  boptions.num_threads = 5;
  auto res2 = ReplayTrace(trace, nullptr, boptions);
  ASSERT_EQ(res.Reads().NumOperations(), res2.Reads().NumOperations());
  ASSERT_EQ(res.Writes().NumOperations(), res2.Writes().NumOperations());
}

TEST_F(TraceReplayA, NoThreads) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  BenchmarkOptions<TestDatabaseInterface> boptions;
  boptions.num_threads = 0;
  ASSERT_THROW(ReplayTrace(trace, nullptr, boptions), std::invalid_argument);
}

TEST_F(TraceLoadA, NoThreads) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  BenchmarkOptions<TestDatabaseInterface> boptions;
  boptions.num_threads = 0;

  ASSERT_THROW(ReplayTrace(trace, nullptr, boptions), std::invalid_argument);
  ASSERT_THROW(ReplayTrace(trace, &load, boptions), std::invalid_argument);
}

TEST_F(TraceReplayA, PreRunHook) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);

  bool called = false;
  BenchmarkOptions<TestDatabaseInterface> boptions;
  boptions.pre_run_hook = [&called](TestDatabaseInterface& db) { called = true; };
  ReplayTrace(trace, nullptr, boptions);
  ASSERT_TRUE(called);
}

}  // namespace
