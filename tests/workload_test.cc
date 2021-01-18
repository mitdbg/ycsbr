#include <stdexcept>

#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "yr/data.h"

namespace {

using namespace yr;

TEST_F(WorkloadLoadA, LoadBulkLoad) {
  const Workload::Options options;
  const BulkLoadWorkload load =
      BulkLoadWorkload::LoadFromFile(workload_file, options);
  ASSERT_EQ(load.size(), workload_size);

  // Can also load as a regular workload.
  const Workload as_workload = Workload::LoadFromFile(workload_file, options);
  ASSERT_EQ(as_workload.size(), workload_size);
}

TEST_F(WorkloadRunA, InvalidLoadBulkLoad) {
  const Workload::Options options;
  ASSERT_THROW(BulkLoadWorkload::LoadFromFile(workload_file, options),
               std::invalid_argument);
}

TEST_F(WorkloadRunA, LoadWorkload) {
  const Workload::Options options;
  const Workload workload = Workload::LoadFromFile(workload_file, options);
  ASSERT_EQ(workload.size(), workload_size);
}

TEST_F(WorkloadRunE, InvalidLoadBulkLoad) {
  const Workload::Options options;
  ASSERT_THROW(BulkLoadWorkload::LoadFromFile(workload_file, options),
               std::invalid_argument);
}

TEST_F(WorkloadRunE, LoadWorkload) {
  const Workload::Options options;
  const Workload workload = Workload::LoadFromFile(workload_file, options);
  ASSERT_EQ(workload.size(), workload_size);

  // Workload E contains scans.
  bool found_scan = false;
  for (const auto& req : workload) {
    if (req.op == Request::Operation::kScan) {
      found_scan = true;
      break;
    }
  }
  ASSERT_TRUE(found_scan);
}

}  // namespace
