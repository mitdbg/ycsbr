#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <random>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Chooses values uniformly from a 0-based dense range.
// Used to select existing keys for read/update/scan operations.
class UniformChooser : public Chooser {
 public:
  UniformChooser(size_t item_count) : dist_(0, item_count - 1) {
    assert(item_count > 0);
  }

  size_t Next(std::mt19937& prng) override { return dist_(prng); }

  void IncreaseItemCount(size_t new_item_count) override {
    assert(new_item_count > 0);
    dist_ = std::uniform_int_distribution<size_t>(0, new_item_count - 1);
  }

 private:
  std::uniform_int_distribution<size_t> dist_;
};

}  // namespace gen
}  // namespace ycsbr
