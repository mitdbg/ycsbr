#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <random>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"
#include "zipfian_chooser.h"

namespace ycsbr {
namespace gen {

// Selects values in the range [0, item_count) with a skew towards "latest"
// values. The latest value is assumed to be `item_count - 1`, the second latest
// is `item_count - 2` and so on.
class LatestChooser : public Chooser {
 public:
  LatestChooser(size_t item_count, double theta)
      : item_count_(item_count), zipf_(item_count, theta) {
    assert(item_count > 0);
  }

  size_t Next(PRNG& prng) override {
    // The `ZipfianChooser` selects 0 as the most popular item, followed by 1,
    // then 2, etc.
    const size_t choice = zipf_.Next(prng);
    return item_count_ - 1 - choice;
  }

  void SetItemCount(const size_t item_count) override {
    item_count_ = item_count;
    zipf_.SetItemCount(item_count);
  }

  void IncreaseItemCountBy(size_t delta) override {
    item_count_ += delta;
    zipf_.IncreaseItemCountBy(delta);
  }

 private:
  size_t item_count_;
  ZipfianChooser zipf_;
};

}  // namespace gen
}  // namespace ycsbr
