#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <random>

namespace ycsbr {
namespace impl {

inline std::unique_ptr<char[]> GetRandomBytes(const size_t size,
                                              std::mt19937& prng) {
  assert(size >= sizeof(uint32_t));
  std::unique_ptr<char[]> values = std::make_unique<char[]>(size);
  const size_t num_u32 = size / sizeof(uint32_t);
  uint32_t* start = reinterpret_cast<uint32_t*>(values.get());
  for (size_t i = 0; i < num_u32; ++i, ++start) {
    *start = prng();
  }
  return values;
}

}  // namespace impl
}  // namespace ycsbr
