#pragma once

#include <cassert>
#include <cmath>
#include <cstdint>
#include <random>

namespace ycsbr {
namespace impl {

// Returns Zipfian-distributed values in the range [0, item_count). This
// implementation is based on the YCSB driver's Zipfian implementation, which in
// turn uses the algorithm presented in
//   J. Gray et al. Quickly generating billion-record synthetic databases. In
//   SIGMOD'94.
class Zipfian {
 public:
  Zipfian(size_t item_count, double theta, uint64_t seed);

  // Get a sample from the distribution. The returned value will be in the range
  // [0, item_count).
  size_t operator()();

  // This requires some computation and can be slow if the difference between
  // `new_item_count` and the current item count is large.
  void IncreaseItemCount(size_t new_item_count);

 private:
  static double ComputeZetaN(size_t item_count, double theta,
                             size_t prev_item_count = 0,
                             double prev_zeta_n = 0.0);
  void UpdateComputedConstants(size_t prev_item_count = 0,
                               double prev_zeta_n = 0.0);

  size_t item_count_;
  double theta_;
  double alpha_;
  double thres_;
  double zeta2theta_;
  double zeta_n_;
  double eta_;

  std::mt19937 prng_;
  std::uniform_real_distribution<double> dist_;
};

// Implementation details follow.

inline Zipfian::Zipfian(const size_t item_count, const double theta,
                        const uint64_t seed)
    : item_count_(item_count),
      theta_(theta),
      alpha_(1.0 / (1.0 - theta)),
      thres_(1.0 + std::pow(0.5, theta)),
      zeta2theta_(ComputeZetaN(2, theta)),
      zeta_n_(0.0),
      eta_(0.0),
      prng_(seed),
      dist_(0.0, 1.0) {
  UpdateComputedConstants();
}

inline size_t Zipfian::operator()() {
  const double u = dist_(prng_);
  const double uz = u * zeta_n_;
  if (uz < 1.0) return 0;
  if (uz < thres_) return 1;
  return static_cast<size_t>(
      (item_count_)*std::pow(eta_ * u - eta_ + 1, alpha_));
}

inline void Zipfian::IncreaseItemCount(const size_t new_item_count) {
  assert(new_item_count > item_count_);
  const size_t prev_item_count = item_count_;
  const double prev_zeta_n = zeta_n_;
  item_count_ = new_item_count;
  UpdateComputedConstants(prev_item_count, prev_zeta_n);
}

inline double Zipfian::ComputeZetaN(const size_t item_count, const double theta,
                                    const size_t prev_item_count,
                                    const double prev_zeta_n) {
  assert(item_count > prev_item_count);
  size_t item_count_so_far = prev_item_count;
  double zeta_so_far = prev_zeta_n;
  for (; item_count_so_far < item_count; ++item_count_so_far) {
    zeta_so_far +=
        1.0 / std::pow(static_cast<double>(item_count_so_far + 1), theta);
  }
  return zeta_so_far;
}

inline void Zipfian::UpdateComputedConstants(const size_t prev_item_count,
                                             const double prev_zeta_n) {
  zeta_n_ = ComputeZetaN(item_count_, theta_, prev_item_count, prev_zeta_n);
  eta_ = (1 - std::pow(2.0 / item_count_, 1.0 - theta_)) /
         (1.0 - zeta2theta_ / zeta_n_);
}

}  // namespace impl
}  // namespace ycsbr
