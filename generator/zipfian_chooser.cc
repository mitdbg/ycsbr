#include "zipfian_chooser.h"

#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <utility>

namespace {

// A thread-safe `zeta(n)` cache (to reduce recomputation latency for large item
// counts).
class ZetaCache {
 public:
  static ZetaCache& Instance() {
    static ZetaCache instance;
    return instance;
  }

  // Finds a `zeta(n)` value for a given `item_count` (or for a smaller
  // `item_count` if the exact `item_count` is not in the cache).
  std::optional<std::pair<size_t, double>> FindStartingPoint(
      const size_t item_count) const {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cache_.size() == 0) {
      return std::optional<std::pair<size_t, double>>();
    }

    const auto it = cache_.lower_bound(item_count);
    if (it == cache_.end()) {
      return *std::prev(cache_.end());
    } else {
      if (it->first == item_count) {
        // Exact match.
        return *it;
      } else if (it == cache_.begin()) {
        // No previous values.
        return std::optional<std::pair<size_t, double>>();
      } else {
        // Not an exact match, so the starting point should be the first zeta
        // computed with a smaller item count.
        return *std::prev(cache_.end());
      }
    }
  }

  void Add(size_t item_count, double zeta) {
    std::unique_lock<std::mutex> lock(mutex_);
    // N.B. If an entry for `item_count` already exists, this insert will be an
    // effective no-op.
    cache_.insert(std::make_pair(item_count, zeta));
  }

  ZetaCache(ZetaCache&) = delete;
  ZetaCache& operator=(ZetaCache&) = delete;

 private:
  // Singleton class - use `ZetaCache::Instance()` instead.
  ZetaCache() = default;

  mutable std::mutex mutex_;

  // Caches (item_count, zeta) pairs.
  std::map<size_t, double> cache_;
};

}  // namespace

namespace ycsbr {
namespace gen {

void ZipfianChooser::UpdateZetaNWithCaching() {
  ZetaCache& cache = ZetaCache::Instance();
  auto result = cache.FindStartingPoint(item_count_);
  if (result.has_value() && result->first == item_count_) {
    // We computed zeta(n) for this `item_count` before.
    zeta_n_ = result->second;
    return;
  }
  size_t prev_item_count = 0;
  double prev_zeta_n = 0.0;
  if (result.has_value()) {
    prev_item_count = result->first;
    prev_zeta_n = result->second;
    assert(prev_item_count < item_count_);
  }
  zeta_n_ = ComputeZetaN(item_count_, theta_, prev_item_count, prev_zeta_n);
  // N.B. Multiple threads may end up computing zeta(n) for the same
  // `item_count`, but we consider this case acceptable because it cannot lead
  // to incorrect zeta(n) values.
  cache.Add(item_count_, zeta_n_);
}

}  // namespace gen
}  // namespace ycsbr
