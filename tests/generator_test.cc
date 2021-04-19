#include <cstdint>
#include <random>
#include <unordered_set>

#include "../generator/hotspot_keygen.h"
#include "../generator/sampling.h"
#include "../generator/uniform_keygen.h"
#include "gtest/gtest.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

TEST(GeneratorTest, FloydSample) {
  constexpr size_t num_samples = 100;
  constexpr size_t start_index = 10;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 500000000;
  std::mt19937 prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  FloydSample<uint64_t, std::mt19937>(num_samples, min, max, &samples,
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
  std::mt19937 prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  FisherYatesSample<uint64_t, std::mt19937>(num_samples, min, max, &samples,
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
      ASSERT_EQ(samples.at(i), 0);
    }
  }
}

TEST(GeneratorTest, SelectionSample) {
  constexpr size_t num_samples = 100;
  constexpr size_t start_index = 10;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 100000;
  std::mt19937 prng(42);
  std::vector<uint64_t> samples(num_samples + 20, 0);

  SelectionSample<uint64_t, std::mt19937>(num_samples, min, max, &samples,
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
      ASSERT_EQ(samples.at(i), 0);
    }
  }
}

TEST(GeneratorTest, UniformGenerator) {
  constexpr size_t num_samples = 1000;
  constexpr Request::Key min = 10;
  constexpr Request::Key max = 10000;

  std::mt19937 prng(42);
  UniformGenerator generator(num_samples, min, max);
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

  std::mt19937 prng(42);
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

}  // namespace
