#include <chrono>

#include "db_interface.h"
#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "ycsbr/ycsbr.h"

namespace {

using namespace ycsbr;

TEST_F(TraceReplayA, SessionSimpleTest) {
  Session<TestDatabaseInterface> session(1);
  session.Terminate();
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
}

TEST_F(TraceReplayA, SessionSimpleRun) {
  const Trace trace = Trace::LoadFromFile(trace_file, Trace::Options());
  Session<TestDatabaseInterface> session(1);
  const BenchmarkResult result = session.ReplayTrace(trace);
  session.Terminate();
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  // ASSERT_TRUE(session.db().read_calls > 0);
  // ASSERT_TRUE(session.db().update_calls > 0);
  // ASSERT_EQ(session.db().read_calls + session.db().update_calls, kTraceSize);
}

TEST_F(TraceLoadA, SessionBulkLoad) {
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, Trace::Options());
  Session<TestDatabaseInterface> session(1);
  const auto result = session.ReplayBulkLoadTrace(load);
  session.Terminate();
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_TRUE(result.RunTime<std::chrono::nanoseconds>().count() > 0);
}

}  // namespace
