#include <cstdint>
#include <random>
#include <unordered_set>

#include "../generator/sampling.h"
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

}  // namespace
