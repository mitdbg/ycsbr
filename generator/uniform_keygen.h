#pragma once

#include <random>
#include <vector>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

class UniformGenerator : public Generator {
 public:
  // Uniformly select `num_keys` from [range_min, range_max].
  UniformGenerator(size_t num_keys, Request::Key range_min,
                   Request::Key range_max);

  void Generate(std::mt19937& prng, std::vector<Request::Key>* dest,
                size_t start_index) const override;

 private:
  size_t num_keys_;
  Request::Key range_min_;
  Request::Key range_max_;
};

}  // namespace gen
}  // namespace ycsbr
