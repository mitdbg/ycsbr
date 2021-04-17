#pragma once

#include <cstdint>
#include <cstring>
#include <random>

#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Chooses values from a 0-based dense range.
// Used to select existing keys for read/update/scan operations.
class Chooser {
 public:
  virtual ~Chooser() = default;
  virtual size_t Next(std::mt19937& prng) = 0;
  virtual void IncreaseItemCount(size_t new_item_count) = 0;
};

}  // namespace gen
}  // namespace ycsbr
