#include "uniform_keygen.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <unordered_set>

#include "sampling.h"

namespace ycsbr {
namespace gen {

UniformGenerator::UniformGenerator(const size_t num_keys, KeyRange range)
    : num_keys_(num_keys), range_(std::move(range)) {
  if (range_.size() < num_keys_) {
    throw std::invalid_argument("UniformGenerator: Range is too small.");
  }
}

void UniformGenerator::Generate(PRNG& prng, std::vector<Request::Key>* dest,
                                const size_t start_index) const {
  SampleWithoutReplacement<Request::Key, PRNG>(num_keys_, range_, dest,
                                               start_index, prng);
  // Shuffle the samples to be sure the order is not biased.
  std::shuffle(dest->begin() + start_index,
               dest->begin() + start_index + num_keys_, prng);
}

}  // namespace gen
}  // namespace ycsbr
