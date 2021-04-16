#include "uniform_keygen.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <unordered_set>

namespace ycsbr {
namespace gen {

UniformGenerator::UniformGenerator(size_t num_keys, Request::Key range_min,
                                   Request::Key range_max)
    : num_keys_(num_keys), range_min_(range_min), range_max_(range_max) {
  assert(range_min_ <= range_max_);
  assert(range_max_ - range_min_ + 1 >= num_keys_);
}

void UniformGenerator::Generate(std::mt19937& prng,
                                std::vector<Request::Key>* dest,
                                const size_t start_index) const {
  assert(start_index < dest->size());
  assert(start_index + num_keys_ <= dest->size());
  // We use Floyd sampling. For more details, see:
  // https://www.nowherenearithaca.com/2013/05/robert-floyds-tiny-and-beautiful.html
  std::unordered_set<Request::Key> samples;
  samples.reserve(num_keys_);
  for (Request::Key curr = range_max_ - num_keys_ + 1; curr <= range_max_;
       ++curr) {
    std::uniform_int_distribution<Request::Key> dist(range_min_, curr);
    const Request::Key next = dist(prng);
    auto res = samples.insert(next);
    if (!res.second) {
      samples.insert(curr);
    }
  }
  assert(samples.size() == num_keys_);

  // Copy samples into the destination vector.
  size_t i = start_index;
  for (auto val : samples) {
    (*dest)[i++] = val;
  }

  // Shuffle the samples to be sure the order is not biased.
  std::shuffle(dest->begin() + start_index,
               dest->begin() + start_index + num_keys_, prng);
}

}  // namespace gen
}  // namespace ycsbr
