#include <random>
#include <unordered_set>

#include "../generator/uniform_keygen.h"
#include "gtest/gtest.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

TEST(GeneratorTest, Uniform) {
  constexpr size_t num_samples = 1000;
  constexpr Request::Key min = 1;
  constexpr Request::Key max = 500000000;
  std::mt19937 prng(42);
  std::vector<Request::Key> samples(num_samples, 0);

  UniformGenerator gen(num_samples, min, max);
  gen.Generate(prng, &samples, 0);
  ASSERT_EQ(samples.size(), num_samples);

  std::unordered_set<Request::Key> seen;
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
