#pragma once

#include <vector>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

class LinspaceGenerator : public Generator {
 public:
  // Generates keys that are evenly spaced.
  LinspaceGenerator(size_t num_keys, Request::Key start_key, Request::Key step_size);

  void Generate(PRNG& prng, std::vector<Request::Key>* dest,
                size_t start_index) const override;

 private:
  size_t num_keys_;
  Request::Key start_key_;
  Request::Key step_size_;
};

}  // namespace gen
}  // namespace ycsbr
