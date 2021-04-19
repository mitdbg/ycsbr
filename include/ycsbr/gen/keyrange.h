#pragma once

#include <cassert>
#include <limits>

#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Represents an inclusive interval `[min, max]`.
template <typename T>
class Range {
 public:
  Range(const T min, const T max) : min_(min), max_(max) {
    assert(min_ <= max_);
  }

  T min() const { return min_; }
  T max() const { return max_; }

  // The number of values in this `Range`.
  size_t size() const { return max_ - min_ + 1; }

  // Returns true if `inner` is contained within this `Range`.
  bool Contains(const Range<T>& inner) const {
    // Since `inner.min_ <= inner.max_` must be true, we don't need to also
    // check `inner.min_ <= max_` and `inner.max_ >= min_`.
    return min_ <= inner.min_ && inner.max_ <= max_;
  }

  // Returns the "before" and "after" ranges that result from subtracting the
  // given `inner` range from this range.
  std::pair<std::optional<Range<T>>, std::optional<Range<T>>> SubtractContained(
      const Range<T>& inner) const {
    // Only supported for ranges completely contained by this range.
    assert(Contains(inner));
    std::optional<Range<T>> before, after;

    // Handle the before range. A before range cannot exist if the inner lower
    // bound is the smallest possible value.
    if (inner.min_ > std::numeric_limits<T>::min()) {
      if (inner.min_ - 1 >= min_) {
        before = Range<T>(min_, inner.min_ - 1);
      }
    }

    // Handle the after range. An after range cannot exist if the inner upper
    // bound is the largest possible value.
    if (inner.max_ < std::numeric_limits<T>::max()) {
      if (inner.max_ + 1 <= max_) {
        after = Range<T>(inner.max_ + 1, max_);
      }
    }

    return std::make_pair(std::move(before), std::move(after));
  }

 private:
  T min_, max_;
};

using KeyRange = Range<Request::Key>;

}  // namespace gen
}  // namespace ycsbr
