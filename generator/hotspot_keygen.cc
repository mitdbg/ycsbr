#include "hotspot_keygen.h"

#include <algorithm>
#include <stdexcept>

#include "sampling.h"

namespace ycsbr {
namespace gen {

HotspotGenerator::HotspotGenerator(const size_t num_keys,
                                   const uint32_t hot_proportion_pct,
                                   const KeyRange overall, const KeyRange hot)
    : num_hot_keys_(0),
      hot_(std::move(hot)),
      num_cold_before_keys_(0),
      num_cold_after_keys_(0),
      cold_before_(),
      cold_after_() {
  if (!overall.Contains(hot_)) {
    throw std::invalid_argument(
        "Hotspot: The hot range must be inside the overall range.");
  }
  if (hot_proportion_pct > 100) {
    throw std::invalid_argument(
        "Hotspot: The hot proportion percentage cannot be more than 100%.");
  }

  num_hot_keys_ = static_cast<size_t>(num_keys * (hot_proportion_pct / 100.0));
  if (hot_.size() < num_hot_keys_) {
    throw std::invalid_argument(
        "Hotspot: The hot range is not large enough to generate enough unique "
        "values.");
  }

  // Build the disjoint cold ranges.
  auto res = overall.SubtractContained(hot_);
  cold_before_ = std::move(res.first);
  cold_after_ = std::move(res.second);

  // Compute the cold range sizes.
  size_t cold_before_range_size = 0, cold_after_range_size = 0;
  if (cold_before_.has_value()) {
    cold_before_range_size = cold_before_->size();
  }
  if (cold_after_.has_value()) {
    cold_after_range_size = cold_after_->size();
  }
  const size_t total_cold_range_size =
      cold_before_range_size + cold_after_range_size;

  // Figure out how many keys we need to generate in each cold range.
  const size_t remaining_keys = num_keys - num_hot_keys_;
  num_cold_before_keys_ = static_cast<size_t>(
      remaining_keys *
      (cold_before_range_size / static_cast<double>(total_cold_range_size)));
  num_cold_after_keys_ = remaining_keys - num_cold_before_keys_;

  // Make sure the cold key ranges are large enough.
  if (num_cold_before_keys_ > 0) {
    if (!cold_before_.has_value() ||
        cold_before_->size() < num_cold_before_keys_) {
      throw std::invalid_argument(
          "Hotspot: The cold range (below the hot values) is not large enough "
          "to generate enough unique values.");
    }
  }
  if (num_cold_after_keys_ > 0) {
    if (!cold_after_.has_value() ||
        cold_after_->size() < num_cold_after_keys_) {
      throw std::invalid_argument(
          "Hotspot: The cold range (above the hot values) is not large enough "
          "to generate enough unique values.");
    }
  }
}

void HotspotGenerator::Generate(PRNG& prng, std::vector<Request::Key>* dest,
                                const size_t start_index) const {
  size_t curr_index = start_index;
  // Before.
  if (num_cold_before_keys_ > 0) {
    SelectionSample<Request::Key, PRNG>(num_cold_before_keys_, *cold_before_,
                                        dest, curr_index, prng);
    curr_index += num_cold_before_keys_;
  }

  // Hot.
  SelectionSample<Request::Key, PRNG>(num_hot_keys_, hot_, dest, curr_index,
                                      prng);
  curr_index += num_hot_keys_;

  // After.
  if (num_cold_after_keys_ > 0) {
    SelectionSample<Request::Key, PRNG>(num_cold_after_keys_, *cold_after_,
                                        dest, curr_index, prng);
    curr_index += num_cold_after_keys_;
  }

  // Sanity check.
  assert(curr_index == (start_index + num_cold_before_keys_ + num_hot_keys_ +
                        num_cold_after_keys_));

  // Shuffle the samples to be sure the order is not biased.
  std::shuffle(dest->begin() + start_index, dest->begin() + curr_index, prng);
}

}  // namespace gen
}  // namespace ycsbr
