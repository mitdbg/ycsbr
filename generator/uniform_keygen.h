#pragma once

#include <vector>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/keyrange.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

class UniformGenerator : public Generator {
 public:
  // Uniformly select `num_keys` from [range.min(), range.max()].
  UniformGenerator(size_t num_keys, KeyRange range);

  void Generate(PRNG& prng, std::vector<Request::Key>* dest,
                size_t start_index) const override;

 private:
  size_t num_keys_;
  KeyRange range_;
};

}  // namespace gen
}  // namespace ycsbr
