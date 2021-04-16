#include "uniform_keygen.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <unordered_set>

#include "sampling.h"

namespace ycsbr {
namespace gen {

UniformGenerator::UniformGenerator(size_t num_keys, Request::Key range_min,
                                   Request::Key range_max)
    : num_keys_(num_keys), range_min_(range_min), range_max_(range_max) {
  if ((range_min_ > range_max_) || (range_max_ - range_min_ + 1 < num_keys_)) {
    throw std::invalid_argument(
        "UniformGenerator: Range is invalid or too small.");
  }
}

void UniformGenerator::Generate(std::mt19937& prng,
                                std::vector<Request::Key>* dest,
                                const size_t start_index) const {
  SampleWithoutReplacement<Request::Key, std::mt19937>(
      num_keys_, range_min_, range_max_, dest, start_index, prng);
  // Shuffle the samples to be sure the order is not biased.
  std::shuffle(dest->begin() + start_index,
               dest->begin() + start_index + num_keys_, prng);
}

}  // namespace gen
}  // namespace ycsbr
