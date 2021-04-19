#include "../generator/zipfian_chooser.h"

#include <random>

#include "gtest/gtest.h"

namespace {

using namespace ycsbr::gen;

TEST(ZipfianTest, Simple) {
  constexpr size_t item_count = 1000000;
  constexpr size_t repetitions = 100000;
  constexpr size_t epsilon = 100;

  std::mt19937 prng(42);
  ZipfianChooser zipf(item_count, 0.99);
  std::vector<size_t> freq(item_count, 0);
  for (size_t i = 0; i < repetitions; ++i) {
    ++freq[zipf.Next(prng)];
  }
  for (size_t i = 1; i < item_count; ++i) {
    ASSERT_LE(freq[i], freq[i - 1] + epsilon);
  }
}

TEST(ZipfianTest, CheckCaching) {
  constexpr size_t item_count = 1002000;
  constexpr size_t repetitions = 100000;
  constexpr size_t epsilon = 100;

  std::mt19937 prng(42);
  ZipfianChooser zipf(item_count, 0.99);
  std::vector<size_t> freq(item_count, 0);
  for (size_t i = 0; i < repetitions; ++i) {
    ++freq[zipf.Next(prng)];
  }
  for (size_t i = 1; i < item_count; ++i) {
    ASSERT_LE(freq[i], freq[i - 1] + epsilon);
  }
}

}  // namespace
