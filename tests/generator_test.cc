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
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 500000000;
  std::mt19937 prng(42);
  std::vector<uint64_t> samples(num_samples, 0);

  FloydSample<uint64_t, std::mt19937>(num_samples, min, max, &samples, 0, prng);
  ASSERT_EQ(samples.size(), num_samples);

  std::unordered_set<uint64_t> seen;
  seen.reserve(num_samples);
  for (auto& val : samples) {
    ASSERT_TRUE(val >= min);
    ASSERT_TRUE(val <= max);
    // `res.second` is false if the item was already in the set.
    auto res = seen.insert(val);
    ASSERT_TRUE(res.second);
  }
}

TEST(GeneratorTest, FisherYates) {
  constexpr size_t num_samples = 100;
  constexpr uint64_t min = 1;
  constexpr uint64_t max = 500000000;
  std::mt19937 prng(42);
  std::vector<uint64_t> samples(num_samples, 0);

  FisherYatesSample<uint64_t, std::mt19937>(num_samples, min, max, &samples, 0,
                                            prng);
  ASSERT_EQ(samples.size(), num_samples);

  std::unordered_set<uint64_t> seen;
  seen.reserve(num_samples);
  for (auto& val : samples) {
    ASSERT_TRUE(val >= min);
    ASSERT_TRUE(val <= max);
    // `res.second` is false if the item was already in the set.
    auto res = seen.insert(val);
    ASSERT_TRUE(res.second);
  }
}

}  // namespace
