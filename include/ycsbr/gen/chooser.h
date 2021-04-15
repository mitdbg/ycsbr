#pragma once

#include <cstdint>
#include <cstring>

#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Chooses values from a 0-based dense range.
// Used to select existing keys for read/update/scan operations.
class Chooser {
 public:
  virtual ~Chooser() = default;
  virtual size_t Next(uint32_t rand) = 0;
};

// Generates keys.
// Used to generate keys for inserts.
class Generator {
 public:
  virtual ~Generator() = default;

  // The returned key must be in the range [0, 2^48 - 1] (i.e., only the least
  // significant 48 bits may be used to represent a key).
  virtual Request::Key Next(uint32_t rand) = 0;
};

}  // namespace gen
}  // namespace ycsbr
