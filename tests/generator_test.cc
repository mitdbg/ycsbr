#include <algorithm>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "../generator/hotspot_keygen.h"
#include "../generator/latest_chooser.h"
#include "../generator/linspace_keygen.h"
#include "../generator/sampling.h"
#include "../generator/uniform_keygen.h"
#include "../generator/zipfian_chooser.h"
#include "db_interface.h"
#include "gtest/gtest.h"
#include "ycsbr/gen.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

TEST(GeneratorTest, FloydSample) {
  constexpr size_t num_samples = 100;
  constexpr size_t start_index = 10;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 500000000;
  PRNG prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  FloydSample<uint64_t, PRNG>(num_samples, Range<uint64_t>(min, max), &samples,
                              start_index, prng);
  ASSERT_EQ(samples.size(), num_samples + 20);

  std::unordered_set<uint64_t> seen;
  seen.reserve(num_samples);
  for (size_t i = 0; i < samples.size(); ++i) {
    if (i >= start_index && i < start_index + num_samples) {
      const uint64_t val = samples.at(i);
      ASSERT_TRUE(val >= min);
      ASSERT_TRUE(val <= max);
      // `res.second` is false if the item was already in the set.
      auto res = seen.insert(val);
      ASSERT_TRUE(res.second);
    } else {
      // Should be unchanged.
      ASSERT_EQ(samples.at(i), 0);
    }
  }
}

TEST(GeneratorTest, FisherYates) {
  constexpr size_t num_samples = 100;
  constexpr size_t start_index = 10;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 500000000;
  PRNG prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  FisherYatesSample<uint64_t, PRNG>(num_samples, Range<uint64_t>(min, max),
                                    &samples, start_index, prng);
  ASSERT_EQ(samples.size(), num_samples + 20);

  std::unordered_set<uint64_t> seen;
  seen.reserve(num_samples);
  for (size_t i = 0; i < samples.size(); ++i) {
    if (i >= start_index && i < start_index + num_samples) {
      const uint64_t val = samples.at(i);
      ASSERT_TRUE(val >= min);
      ASSERT_TRUE(val <= max);
      // `res.second` is false if the item was already in the set.
      auto res = seen.insert(val);
      ASSERT_TRUE(res.second);
    } else {
      ASSERT_EQ(samples.at(i), 0);
    }
  }
}

TEST(GeneratorTest, SelectionSample) {
  constexpr size_t num_samples = 100;
  constexpr size_t start_index = 10;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 100000;
  PRNG prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  SelectionSample<uint64_t, PRNG>(num_samples, Range<uint64_t>(min, max),
                                  &samples, start_index, prng);
  ASSERT_EQ(samples.size(), num_samples + 20);

  std::unordered_set<uint64_t> seen;
  seen.reserve(num_samples);
  for (size_t i = 0; i < samples.size(); ++i) {
    if (i >= start_index && i < start_index + num_samples) {
      const uint64_t val = samples.at(i);
      ASSERT_TRUE(val >= min);
      ASSERT_TRUE(val <= max);
      // `res.second` is false if the item was already in the set.
      auto res = seen.insert(val);
      ASSERT_TRUE(res.second);
    } else {
      ASSERT_EQ(samples.at(i), 0);
    }
  }
}

TEST(GeneratorTest, UniformGenerator) {
  constexpr size_t num_samples = 1000;
  constexpr Request::Key min = 10;
  constexpr Request::Key max = 10000;

  PRNG prng(42);
  UniformGenerator generator(num_samples, KeyRange(min, max));
  std::vector<Request::Key> dest(num_samples + 10, 0);
  generator.Generate(prng, &dest, 10);

  // These assertions are mostly just a sanity check.
  for (size_t i = 0; i < dest.size(); ++i) {
    if (i < 10) {
      ASSERT_EQ(dest[i], 0);
    } else {
      ASSERT_GE(dest[i], min);
      ASSERT_LE(dest[i], max);
    }
  }
}

TEST(GeneratorTest, HotspotGenerator) {
  constexpr size_t num_samples = 100;
  constexpr uint32_t hot_pct = 90;
  const KeyRange overall(1, 100000);
  const KeyRange hot(1, 100);
  constexpr size_t offset = 5;
  constexpr size_t repetitions = 3;

  PRNG prng(42);
  HotspotGenerator generator(num_samples, hot_pct, overall, hot);

  for (size_t rep = 0; rep < repetitions; ++rep) {
    std::vector<Request::Key> dest(num_samples + offset, 0);
    generator.Generate(prng, &dest, offset);

    size_t hot_count = 0;
    for (size_t i = 0; i < dest.size(); ++i) {
      if (i < offset) {
        ASSERT_EQ(dest[i], 0);
      } else {
        ASSERT_GE(dest[i], overall.min());
        ASSERT_LE(dest[i], overall.max());
        if (dest[i] >= hot.min() && dest[i] <= hot.max()) {
          ++hot_count;
        }
      }
    }

    // Make sure the hot range has the expected number of samples.
    const size_t expected_hot_keys = num_samples * (hot_pct / 100.0);
    ASSERT_EQ(hot_count, expected_hot_keys);
  }
}

TEST(GeneratorTest, RequestProportions) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 100\n"
      "  read:\n"
      "    proportion_pct: 25\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 25\n"
      "    distribution:\n"
      "      type: zipfian\n"
      "      theta: 0.90\n"
      "  scan:\n"
      "    max_length: 100\n"
      "    proportion_pct: 25\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  insert:\n"
      "    proportion_pct: 25\n"
      "    distribution:\n"
      "      type: hotspot\n"
      "      range_min: 10\n"
      "      range_max: 2000000\n"
      "      hot_proportion_pct: 90\n"
      "      hot_range_min: 10\n"
      "      hot_range_max: 500000\n";
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().insert_calls, 25);

  // Overall there should be 100 requests. They should be evenly split among the
  // three request types (i.e., 25 each). However, since which request to
  // perform is selected using a PRNG, we can't assert that they will each be
  // exactly 25. Instead, we just check that the request counts sum to 75.
  const size_t num_other_requests = session.db().read_calls +
                                    session.db().update_calls +
                                    session.db().scan_calls;
  ASSERT_EQ(num_other_requests, 75);
}

TEST(GeneratorTest, Linspace) {
  std::mt19937 prng(42);
  std::vector<Request::Key> dest(100, 0);

  // Simple case: generate dense keys from 0 to 9 inclusive.
  gen::LinspaceGenerator gen1(/*num_keys=*/10, /*start_key=*/0,
                              /*step_size=*/1);
  gen1.Generate(prng, &dest, 0);
  std::sort(dest.begin(), dest.begin() + 10);
  for (size_t i = 0; i < dest.size(); ++i) {
    if (i < 10) {
      ASSERT_EQ(dest[i], i);
    } else {
      ASSERT_EQ(dest[i], 0);
    }
  }

  // Larger key list - ensure all diffs are the same.
  gen::LinspaceGenerator gen2(/*num_keys=*/100, /*start_key=*/100,
                              /*step_size=*/123);
  gen2.Generate(prng, &dest, 0);
  std::sort(dest.begin(), dest.end());
  ASSERT_EQ(dest[0], 100);
  for (size_t i = 1; i < dest.size(); ++i) {
    const int diff = static_cast<int>(dest[i]) - static_cast<int>(dest[i - 1]);
    ASSERT_EQ(diff, 123);
    ASSERT_GE(dest[i], 100);
  }
}

TEST(GeneratorTest, ReadModifyWrite) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 100\n"
      "  readmodifywrite:\n"
      "    proportion_pct: 90\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  scan:\n"
      "    max_length: 100\n"
      "    proportion_pct: 10\n"
      "    distribution:\n"
      "      type: uniform\n";
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().insert_calls, 0);
  ASSERT_TRUE(session.db().scan_calls > 0);

  // Read-modify-write implies a read paired with an update.
  ASSERT_EQ(session.db().read_calls, session.db().update_calls);

  // 100 requests in total, each read-modify-write is counted as 1 logical
  // request.
  ASSERT_EQ(session.db().read_calls + session.db().scan_calls, 100);

  // 90% of the requests should be read-modify-writes. We allow +/- 5 requests
  // to account for the PRNG (which is used to choose which operation to do.)
  ASSERT_GE(session.db().read_calls, 85);
  ASSERT_LE(session.db().read_calls, 95);
}

TEST(GeneratorTest, CustomDataset) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 100\n"
      "  read:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: uniform\n";
  std::vector<Request::Key> dataset = {10, 12, 15, 16, 2000};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  workload->SetCustomLoadDataset(dataset);
  BulkLoadTrace trace = workload->GetLoadTrace();
  ASSERT_EQ(dataset.size(), trace.size());

  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(trace);
  session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().initialize_worker_calls, 1);
  ASSERT_EQ(session.db().shutdown_worker_calls, 1);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().read_calls, 100);
}

TEST(GeneratorTest, CustomDatasetKeyTooLarge) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 100\n"
      "  read:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: uniform\n";
  std::vector<Request::Key> dataset = {1ULL << 60};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  ASSERT_THROW(workload->SetCustomLoadDataset(dataset), std::invalid_argument);
}

TEST(GeneratorTest, NegativeLookups) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 100\n"
      "  read:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  negativeread:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: zipfian\n"
      "      theta: 0.99\n";
  std::vector<Request::Key> dataset = {10, 12, 15, 16, 2000};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  workload->SetCustomLoadDataset(dataset);

  for (auto& key : dataset) {
    // The generator reserves the lower 16 bits for phase/thread IDs.
    key <<= 16;
  }

  Session<NegativeLookupInterface> session(1);
  session.db().keys.insert(dataset.begin(), dataset.end());
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  const auto result = session.RunWorkload(*workload);
  session.Terminate();

  const size_t num_succeeded_reads = result.Reads().NumRequests();
  const size_t num_failed_reads = result.NumFailedReads();
  ASSERT_EQ(num_succeeded_reads + num_failed_reads, 100);
  // Expect 50% of the reads to fail with a +/- 5% margin of error
  ASSERT_GE(num_failed_reads, 45);
  ASSERT_LE(num_failed_reads, 55);
}

TEST(GeneratorTest, ZipfianSalt) {
  constexpr size_t kItemCount = 100;
  constexpr double kTheta = 0.99;
  std::mt19937 prng(42);

  ScatteredZipfianChooser zipf1(kItemCount, kTheta, 0);
  ScatteredZipfianChooser zipf2(kItemCount, kTheta, 12345);
  // Should produce the same distribution as `zipf1`. The selection frequencies
  // may not be exactly the same, but the hot keys should be very similar (e.g.,
  // the hottest keys should be the same).
  ScatteredZipfianChooser zipf3(kItemCount, kTheta, 0);

  // Key -> Selection Frequency
  std::unordered_map<size_t, size_t> zipf1_count, zipf2_count, zipf3_count;

  // Count key selection frequency
  for (size_t i = 0; i < 1000; ++i) {
    ++zipf1_count[zipf1.Next(prng)];
    ++zipf2_count[zipf2.Next(prng)];
    ++zipf3_count[zipf3.Next(prng)];
  }

  const auto compare = [](const auto& pair1, const auto& pair2) {
    return pair1.second < pair2.second;
  };

  const size_t zipf1_max_key =
      std::max_element(zipf1_count.begin(), zipf1_count.end(), compare)->first;
  const size_t zipf2_max_key =
      std::max_element(zipf2_count.begin(), zipf2_count.end(), compare)->first;
  const size_t zipf3_max_key =
      std::max_element(zipf3_count.begin(), zipf3_count.end(), compare)->first;

  // Should choose different "hot" keys if the salts are different.
  ASSERT_EQ(zipf1_max_key, zipf3_max_key);
  ASSERT_NE(zipf1_max_key, zipf2_max_key);
}

TEST(GeneratorTest, LatestChooser) {
  constexpr size_t kItemCount = 100;
  constexpr double kTheta = 0.99;
  std::mt19937 prng(42);

  LatestChooser latest(kItemCount, kTheta);

  // Index -> Selection Frequency
  std::unordered_map<size_t, size_t> sel_count;

  // Count key selection frequency
  for (size_t i = 0; i < 1000; ++i) {
    ++sel_count[latest.Next(prng)];
  }

  const auto compare = [](const auto& pair1, const auto& pair2) {
    return pair1.second < pair2.second;
  };

  const size_t max_index100 =
      std::max_element(sel_count.begin(), sel_count.end(), compare)->first;

  // Most frequently chosen index should be the "latest" index (99).
  ASSERT_EQ(max_index100, 99);

  // Now add new items.
  latest.IncreaseItemCountBy(/*delta=*/100);

  // Generate some more selections.
  for (size_t i = 0; i < 2000; ++i) {
    ++sel_count[latest.Next(prng)];
  }

  const size_t max_index200 =
      std::max_element(sel_count.begin(), sel_count.end(), compare)->first;

  // Most frequently chosen index should be the "latest" index (now 199).
  ASSERT_EQ(max_index200, 199);
}

TEST(GeneratorTest, InsertOnly) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 100\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000\n"
      "run:\n"
      "- num_requests: 100\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: uniform\n"
      "      range_min: 100\n"
      "      range_max: 100000\n";
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);

  Session<TestDatabaseInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().insert_calls, 100);
}

TEST(GeneratorTest, BufferedWorkload) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 100\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000\n"
      "run:\n"
      "- num_requests: 100\n"
      "  insert:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n"
      "      range_min: 100\n"
      "      range_max: 100000\n"
      "  read:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  BufferedWorkload<PhasedWorkload> bworkload(*workload);

  constexpr size_t num_workers = 2;
  Session<TestDatabaseInterface> session(num_workers);
  session.Initialize();
  session.ReplayBulkLoadTrace(bworkload.workload().GetLoadTrace());
  session.RunWorkload(bworkload);  // Using the buffered workload here.
  session.Terminate();

  ASSERT_EQ(session.db().read_calls, 50);
  ASSERT_EQ(session.db().insert_calls, 50);

  ASSERT_EQ(session.db().initialize_worker_calls, num_workers);
  ASSERT_EQ(session.db().shutdown_worker_calls, num_workers);
  ASSERT_EQ(session.db().initialize_calls, 1);
  ASSERT_EQ(session.db().shutdown_calls, 1);
  ASSERT_EQ(session.db().bulk_load_calls, 1);
  ASSERT_EQ(session.db().scan_calls, 0);
  ASSERT_EQ(session.db().update_calls, 0);
}

TEST(GeneratorTest, ClusteredZipfian) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 1000\n"
      "  read:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: zipfian_clustered\n"
      "      theta: 0.99\n";
  std::vector<Request::Key> dataset = {15, 12, 16, 2000, 10};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  workload->SetCustomLoadDataset(dataset);

  for (auto& key : dataset) {
    // The generator reserves the lower 16 bits for phase/thread IDs.
    key <<= 16;
  }

  Session<KeyFrequencyInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  const auto result = session.RunWorkload(*workload);
  session.Terminate();

  std::vector<Request::Key> dataset_copy(dataset);

  // Sort dataset by access frequency in descending order.
  std::sort(dataset.begin(), dataset.end(),
            [&session](const Request::Key& a, const Request::Key& b) {
              return session.db().key_freqs[a] > session.db().key_freqs[b];
            });
  // Sort the dataset_copy in ascending order by value.
  std::sort(dataset_copy.begin(), dataset_copy.end());

  // The smallest key should be the most frequently accessed, followed by the
  // second smallest, and so on. So the two vectors should be equal.
  ASSERT_EQ(dataset, dataset_copy);
}

TEST(GeneratorTest, CustomInserts) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 3\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: custom\n"
      "      name: testing\n";
  const std::vector<Request::Key> load_dataset = {15, 12, 16, 2000, 10};
  std::vector<Request::Key> insert_dataset = {1, 2, 3};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  workload->SetCustomLoadDataset(load_dataset);
  workload->AddCustomInsertList("testing", insert_dataset);

  for (auto& key : insert_dataset) {
    // The generator reserves the lower 16 bits for phase/thread IDs. The
    // inserts are made by thread 1 in phase 1 (thread 0 phase 0 represents the
    // bulk load).
    key = (key << 16) | (1 << 8) | 1;
  }

  Session<InsertTraceInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  const auto result = session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().insert_trace.size(), insert_dataset.size());
  for (size_t i = 0; i < insert_dataset.size(); ++i) {
    ASSERT_EQ(session.db().insert_trace[i], insert_dataset[i]);
  }
}

TEST(GeneratorTest, CustomInsertsWithOffset) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  distribution:\n"
      "    type: custom\n"
      "run:\n"
      "- num_requests: 3\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: custom\n"
      "      name: testing\n"
      "      offset: 3\n";
  const std::vector<Request::Key> load_dataset = {15, 12, 16, 2000, 10};
  std::vector<Request::Key> insert_dataset = {1, 2, 3, 4, 5, 6};
  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);
  workload->SetCustomLoadDataset(load_dataset);
  workload->AddCustomInsertList("testing", insert_dataset);

  for (auto& key : insert_dataset) {
    // The generator reserves the lower 16 bits for phase/thread IDs. The
    // inserts are made by thread 1 in phase 1 (thread 0 phase 0 represents the
    // bulk load).
    key = (key << 16) | (1 << 8) | 1;
  }

  Session<InsertTraceInterface> session(1);
  session.Initialize();
  session.ReplayBulkLoadTrace(workload->GetLoadTrace());
  const auto result = session.RunWorkload(*workload);
  session.Terminate();

  ASSERT_EQ(session.db().insert_trace.size(), insert_dataset.size() - 3);
  for (size_t i = 0; i < insert_dataset.size() - 3; ++i) {
    ASSERT_EQ(session.db().insert_trace[i], insert_dataset[i + 3]);
  }
}

}  // namespace
