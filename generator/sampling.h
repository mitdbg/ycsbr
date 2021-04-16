#pragma once

#include <vector>

namespace ycsbr {
namespace gen {

// The functions in this header implement sampling without replacement. They
// select a uniform sample of `num_samples` values without replacement from the
// range `[range_min, range_max]`.

// An implementation of Floyd sampling. For more details, see:
// https://www.nowherenearithaca.com/2013/05/robert-floyds-tiny-and-beautiful.html
template <typename T, class RNG>
void FloydSample(size_t num_samples, T range_min, T range_max,
                 std::vector<T>* dest, size_t start_index, RNG& rng);

// An implementation of selection sampling. For more details, see:
// https://stackoverflow.com/a/311716
template <typename T, class RNG>
void SelectionSample(size_t num_samples, T range_min, T range_max,
                     std::vector<T>* dest, size_t start_index, RNG& rng);

// Sampling based on the Fisher-Yates shuffle algorithm. For more details, see:
// https://en.wikipedia.org/wiki/Fisher–Yates_shuffle
template <typename T, class RNG>
void FisherYatesSample(size_t num_samples, T range_min, T range_max,
                       std::vector<T>* dest, size_t start_index, RNG& rng);

// Selects which of the above sampling algorithms to run (for performance
// reasons) using heuristics based on the input parameters.
template <typename T, class RNG>
void SampleWithoutReplacement(size_t num_samples, T range_min, T range_max,
                              std::vector<T>* dest, size_t start_index,
                              RNG& rng);

}  // namespace gen
}  // namespace ycsbr

#include "sampling-inl.h"
