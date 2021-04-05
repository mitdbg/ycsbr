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

}  // namespace
