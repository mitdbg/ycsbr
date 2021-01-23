#include <algorithm>
#include <stdexcept>

#include "gtest/gtest.h"
#include "workloads/fixtures.h"
#include "ycsbr/ycsbr.h"

namespace {

using namespace ycsbr;

bool SystemIsLittleEndian() {
  int one = 1;
  return *reinterpret_cast<const char*>(&one) == 1;
}

TEST_F(WorkloadLoadA, LoadBulkLoad) {
  const Workload::Options options;
  const BulkLoadWorkload load =
      BulkLoadWorkload::LoadFromFile(workload_file, options);
  ASSERT_EQ(load.size(), workload_size);

  // Can also load as a regular workload.
  const Workload as_workload = Workload::LoadFromFile(workload_file, options);
  ASSERT_EQ(as_workload.size(), workload_size);
}

TEST_F(WorkloadLoadA, SmallValueInvalid) {
  Workload::Options options;
  options.value_size = 1;
  ASSERT_THROW(Workload::LoadFromFile(workload_file, options),
               std::invalid_argument);
  ASSERT_THROW(BulkLoadWorkload::LoadFromFile(workload_file, options),
               std::invalid_argument);
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

TEST_F(WorkloadLoadA, SwapBytesMinMax) {
  Workload::Options non_swap;
  non_swap.swap_key_bytes = false;
  const Workload workload = Workload::LoadFromFile(workload_file, non_swap);
  ASSERT_EQ(workload.size(), workload_size);

  // Use numeric comparison to extract the min and max key.
  const auto compare = [](const Request& r1, const Request& r2) {
    return r1.key < r2.key;
  };
  Request::Key min_numeric_key =
      std::min_element(workload.begin(), workload.end(), compare)->key;
  Request::Key max_numeric_key =
      std::max_element(workload.begin(), workload.end(), compare)->key;

  // Reload the workload, this time swapping bytes on little endian machines.
  Workload::Options swap_if_needed;
  swap_if_needed.swap_key_bytes = SystemIsLittleEndian();
  const Workload lexicographic =
      Workload::LoadFromFile(workload_file, swap_if_needed);
  const Workload::MinMaxKeys range = lexicographic.GetKeyRange();

  // Make sure the key range is as expected, based on endianness and byte
  // swapping.
  if (SystemIsLittleEndian()) {
    min_numeric_key = __builtin_bswap64(min_numeric_key);
    max_numeric_key = __builtin_bswap64(max_numeric_key);
  }
  ASSERT_EQ(min_numeric_key, range.min);
  ASSERT_EQ(max_numeric_key, range.max);
}

TEST_F(WorkloadRunA, SortRequests) {
  Workload::Options non_swap;
  non_swap.swap_key_bytes = false;
  const Workload numeric = Workload::LoadFromFile(workload_file, non_swap);
  ASSERT_EQ(numeric.size(), workload_size);

  std::vector<Request::Key> numeric_keys;
  numeric_keys.reserve(numeric.size());
  for (const auto& req : numeric) {
    numeric_keys.push_back(req.key);
  }

  // Sort numerically.
  std::sort(numeric_keys.begin(), numeric_keys.end());

  // Load and sort lexicographically (swapping to big endian if needed).
  Workload::Options swap_if_needed;
  swap_if_needed.swap_key_bytes = SystemIsLittleEndian();
  swap_if_needed.sort_requests = true;
  const Workload lexicographic =
      Workload::LoadFromFile(workload_file, swap_if_needed);

  ASSERT_EQ(lexicographic.size(), numeric.size());

  // Convert numerically sorted keys into big endian if needed.
  if (SystemIsLittleEndian()) {
    std::transform(numeric_keys.begin(), numeric_keys.end(),
                   numeric_keys.begin(),
                   [](Request::Key key) { return __builtin_bswap64(key); });
  }

  // Verify sort order.
  for (size_t i = 0; i < numeric_keys.size(); ++i) {
    ASSERT_EQ(lexicographic[i].key, numeric_keys[i]);
  }
}

}  // namespace
