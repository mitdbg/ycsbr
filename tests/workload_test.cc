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

TEST_F(TraceLoadA, LoadBulkLoad) {
  const Trace::Options options;
  const BulkLoadTrace load = BulkLoadTrace::LoadFromFile(trace_file, options);
  ASSERT_EQ(load.size(), trace_size);

  // Can also load as a regular trace.
  const Trace as_trace = Trace::LoadFromFile(trace_file, options);
  ASSERT_EQ(as_trace.size(), trace_size);
}

TEST_F(TraceLoadA, SmallValueInvalid) {
  Trace::Options options;
  options.value_size = 1;
  ASSERT_THROW(Trace::LoadFromFile(trace_file, options), std::invalid_argument);
  ASSERT_THROW(BulkLoadTrace::LoadFromFile(trace_file, options),
               std::invalid_argument);
}

TEST_F(TraceReplayA, InvalidLoadBulkLoad) {
  const Trace::Options options;
  ASSERT_THROW(BulkLoadTrace::LoadFromFile(trace_file, options),
               std::invalid_argument);
}

TEST_F(TraceReplayA, LoadWorkload) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);
  ASSERT_EQ(trace.size(), trace_size);
}

TEST_F(TraceReplayE, InvalidLoadBulkLoad) {
  const Trace::Options options;
  ASSERT_THROW(BulkLoadTrace::LoadFromFile(trace_file, options),
               std::invalid_argument);
}

TEST_F(TraceReplayE, LoadWorkload) {
  const Trace::Options options;
  const Trace trace = Trace::LoadFromFile(trace_file, options);
  ASSERT_EQ(trace.size(), trace_size);

  // Workload E contains scans.
  bool found_scan = false;
  for (const auto& req : trace) {
    if (req.op == Request::Operation::kScan) {
      found_scan = true;
      break;
    }
  }
  ASSERT_TRUE(found_scan);
}

TEST_F(TraceLoadA, SwapBytesMinMaxV1) {
  Trace::Options non_swap;
  non_swap.use_v1_semantics = true;
  non_swap.swap_key_bytes = false;
  const Trace trace = Trace::LoadFromFile(trace_file, non_swap);
  ASSERT_EQ(trace.size(), trace_size);

  // Use numeric comparison to extract the min and max key.
  const auto compare = [](const Request& r1, const Request& r2) {
    return r1.key < r2.key;
  };
  Request::Key min_numeric_key =
      std::min_element(trace.begin(), trace.end(), compare)->key;
  Request::Key max_numeric_key =
      std::max_element(trace.begin(), trace.end(), compare)->key;

  // Reload the workload, this time swapping bytes on little endian machines.
  Trace::Options swap_if_needed;
  swap_if_needed.use_v1_semantics = true;
  swap_if_needed.swap_key_bytes = SystemIsLittleEndian();
  const Trace lexicographic = Trace::LoadFromFile(trace_file, swap_if_needed);
  const Trace::MinMaxKeys range = lexicographic.GetKeyRange();

  // Make sure the key range is as expected, based on endianness and byte
  // swapping.
  if (SystemIsLittleEndian()) {
    min_numeric_key = __builtin_bswap64(min_numeric_key);
    max_numeric_key = __builtin_bswap64(max_numeric_key);
  }
  ASSERT_EQ(min_numeric_key, range.min);
  ASSERT_EQ(max_numeric_key, range.max);
}

TEST_F(TraceReplayA, SortRequestsV1) {
  Trace::Options non_swap;
  non_swap.use_v1_semantics = true;
  non_swap.swap_key_bytes = false;
  const Trace numeric = Trace::LoadFromFile(trace_file, non_swap);
  ASSERT_EQ(numeric.size(), trace_size);

  std::vector<Request::Key> numeric_keys;
  numeric_keys.reserve(numeric.size());
  for (const auto& req : numeric) {
    numeric_keys.push_back(req.key);
  }

  // Sort numerically.
  std::sort(numeric_keys.begin(), numeric_keys.end());

  // Load and sort lexicographically (swapping to big endian if needed).
  Trace::Options swap_if_needed;
  swap_if_needed.use_v1_semantics = true;
  swap_if_needed.swap_key_bytes = SystemIsLittleEndian();
  swap_if_needed.sort_requests = true;
  const Trace lexicographic = Trace::LoadFromFile(trace_file, swap_if_needed);

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

TEST_F(TraceReplayA, IgnoreSwap) {
  Trace::Options with_swap;
  with_swap.use_v1_semantics = false;
  with_swap.swap_key_bytes =
      true;  // Should be a no-op because of non-v1 semantics.
  const Trace t_with_swap = Trace::LoadFromFile(trace_file, with_swap);
  ASSERT_EQ(t_with_swap.size(), trace_size);

  // Load and sort lexicographically (swapping to big endian if needed).
  Trace::Options no_swap;
  no_swap.use_v1_semantics = false;
  no_swap.swap_key_bytes = false;
  const Trace t_no_swap = Trace::LoadFromFile(trace_file, no_swap);

  ASSERT_EQ(t_no_swap.size(), t_with_swap.size());

  // Verify identical.
  for (size_t i = 0; i < t_with_swap.size(); ++i) {
    ASSERT_EQ(t_with_swap[i].key, t_no_swap[i].key);
  }
}

TEST_F(TraceReplayA, SortRequests) {
  Trace::Options no_sort;
  no_sort.sort_requests = false;
  const Trace t_rand = Trace::LoadFromFile(trace_file, no_sort);
  ASSERT_EQ(t_rand.size(), trace_size);

  std::vector<Request::Key> numeric_keys;
  numeric_keys.reserve(t_rand.size());
  for (const auto& req : t_rand) {
    numeric_keys.push_back(req.key);
  }

  // Sort it ourselves.
  std::sort(numeric_keys.begin(), numeric_keys.end());

  // Load and sort.
  Trace::Options sort;
  sort.sort_requests = true;
  const Trace t_sorted = Trace::LoadFromFile(trace_file, sort);

  ASSERT_EQ(t_rand.size(), t_sorted.size());

  // Verify sort order.
  for (size_t i = 0; i < numeric_keys.size(); ++i) {
    ASSERT_EQ(t_sorted[i].key, numeric_keys[i]);
  }
}

TEST(TraceTest, BulkLoadFromKeys) {
  const Trace::Options options;
  std::vector<Request::Key> keys = {0, 1, 2, 3, 4, 5};
  const BulkLoadTrace load = BulkLoadTrace::LoadFromKeys(keys, options);
  ASSERT_EQ(load.size(), keys.size());
  for (size_t i = 0; i < load.size(); ++i) {
    ASSERT_EQ(keys[i], load[i].key);
    ASSERT_EQ(load[i].op, Request::Operation::kInsert);
    ASSERT_EQ(load[i].value_size, options.value_size);
    ASSERT_NE(load[i].value, nullptr);
  }
}

}  // namespace
