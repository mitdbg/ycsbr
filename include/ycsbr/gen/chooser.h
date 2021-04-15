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

}  // namespace gen
}  // namespace ycsbr
