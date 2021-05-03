#pragma once

#include <cstdint>
#include <random>

namespace ycsbr {
namespace gen {

using PhaseID = uint64_t;
using ProducerID = uint64_t;
using PRNG = std::mt19937;

// The workload runner reserves 16 bits for the phase ID and producer ID (helps
// us ensure inserts are always new keys.)
inline constexpr uint64_t kMaxKey = (1ULL << 48) - 1;

}  // namespace gen
}  // namespace ycsbr
