#pragma once

#include <cstdint>
#include <random>

namespace ycsbr {
namespace gen {

using PhaseID = uint64_t;
using ProducerID = uint64_t;
using PRNG = std::mt19937;

}  // namespace gen
}  // namespace ycsbr
