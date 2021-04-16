#include <cassert>
#include <limits>
#include <random>
#include <unordered_set>

namespace ycsbr {
namespace gen {

template <typename T, class RNG>
inline void FloydSample(const size_t num_samples, const T range_min,
                        const T range_max, std::vector<T>* dest,
                        const size_t start_index, RNG& rng) {
  assert(range_min <= range_max);
  assert(range_max - range_min >= num_samples);
  assert(start_index < dest->size());
  assert(start_index + num_samples <= dest->size());

  std::unordered_set<T> samples;
  samples.reserve(num_samples);
  for (T curr = range_max - num_samples + 1; curr <= range_max; ++curr) {
    std::uniform_int_distribution<T> dist(range_min, curr);
    const T next = dist(rng);
    auto res = samples.insert(next);
    if (!res.second) {
      samples.insert(curr);
    }
  }
  assert(samples.size() == num_samples);

  // Copy samples into the destination vector.
  size_t i = start_index;
  for (auto val : samples) {
    (*dest)[i++] = val;
  }
}

template <typename T, class RNG>
void SelectionSample(const size_t num_samples, const T range_min,
                     const T range_max, std::vector<T>* dest,
                     const size_t start_index, RNG& rng) {
  assert(range_min <= range_max);
  assert(range_max - range_min >= num_samples);
  assert(start_index < dest->size());
  assert(start_index + num_samples <= dest->size());

  std::uniform_real_distribution<double> dist(0.0, 1.0);
  const T interval = range_max - range_min + 1;
  size_t samples = 0;
  size_t curr = 0;
  while (samples < num_samples) {
    const double u = dist(rng);
    if ((interval - curr) * u < num_samples - samples) {
      (*dest)[samples++] = range_min + curr;
    }
    ++curr;
  }
}

}  // namespace gen
}  // namespace ycsbr
