#include "linspace_keygen.h"

#include <algorithm>
#include <cassert>

namespace ycsbr {
namespace gen {

LinspaceGenerator::LinspaceGenerator(size_t num_keys, Request::Key start_key,
                                     Request::Key step_size)
    : num_keys_(num_keys), start_key_(start_key), step_size_(step_size) {
  assert(step_size > 0);
  assert(num_keys > 0);
}

void LinspaceGenerator::Generate(PRNG& prng, std::vector<Request::Key>* dest,
                                 const size_t start_index) const {
  const Request::Key max_key = start_key_ + (num_keys_ - 1) * step_size_;
  size_t i = start_index;
  for (Request::Key key = start_key_; key <= max_key; key += step_size_) {
    (*dest)[i++] = key;
  }
  assert(i - start_index == num_keys_);
  std::shuffle(dest->begin() + start_index,
               dest->begin() + start_index + num_keys_, prng);
}

}  // namespace gen
}  // namespace ycsbr
