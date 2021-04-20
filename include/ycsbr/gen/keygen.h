#pragma once

#include <vector>

#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Generates keys.
// Used to generate keys for inserts.
class Generator {
 public:
  virtual ~Generator() = default;

  // The generated keys must be in the range [0, 2^48 - 1] (i.e., only the least
  // significant 48 bits may be used to represent a key). The number of
  // generated keys is stored by the `Generator` instance.
  virtual void Generate(PRNG& prng, std::vector<Request::Key>* dest,
                        size_t start_index) const = 0;
};

}  // namespace gen
}  // namespace ycsbr
