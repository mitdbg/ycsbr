#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <random>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Chooses values uniformly from a 0-based dense range.
// Used to select existing keys for read/update/scan operations.
class UniformChooser : public Chooser {
 public:
  UniformChooser(size_t item_count)
      : item_count_(item_count), dist_(0, item_count - 1) {
    assert(item_count > 0);
  }

  size_t Next(PRNG& prng) override { return dist_(prng); }

  void SetItemCount(const size_t item_count) override {
    item_count_ = item_count;
    UpdateDistribution();
  }

  void IncreaseItemCountBy(size_t delta) override {
    item_count_ += delta;
    UpdateDistribution();
  }

 private:
  void UpdateDistribution() {
    dist_ = std::uniform_int_distribution<size_t>(0, item_count_ - 1);
  }

  size_t item_count_;
  std::uniform_int_distribution<size_t> dist_;
};

}  // namespace gen
}  // namespace ycsbr
