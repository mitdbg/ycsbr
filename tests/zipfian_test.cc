#include "ycsbr/gen/zipfian.h"
#include "gtest/gtest.h"

namespace {

using namespace ycsbr::gen;

TEST(ZipfianTest, Simple) {
  constexpr size_t item_count = 1000000;
  constexpr size_t repetitions = 100000;
  constexpr size_t epsilon = 100;

  Zipfian zipf(item_count, 0.99, 42);
  std::vector<size_t> freq(item_count, 0);
  for (size_t i = 0; i < repetitions; ++i) {
    ++freq[zipf()];
  }
  for (size_t i = 1; i < item_count; ++i) {
    ASSERT_LE(freq[i], freq[i - 1] + epsilon);
  }
}

}  // namespace
