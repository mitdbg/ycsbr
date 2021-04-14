#pragma once

#include <cstdint>

namespace ycsbr {
namespace gen {

enum class Distribution : uint8_t {
  kUniform = 0,
  kZipfian = 1,
  kHotspot = 2
};

}  // namespace gen
}  // namespace ycsbr
