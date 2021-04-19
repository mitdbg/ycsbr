#pragma once

#include <optional>
#include <random>
#include <vector>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/keyrange.h"

namespace ycsbr {
namespace gen {

class HotspotGenerator : public Generator {
 public:
  HotspotGenerator(size_t num_keys, uint32_t hot_proportion_pct,
                   KeyRange overall, KeyRange hot);

  void Generate(std::mt19937& prng, std::vector<Request::Key>* dest,
                size_t start_index) const override;

 private:
  size_t num_hot_keys_;
  KeyRange hot_;

  size_t num_cold_before_keys_, num_cold_after_keys_;
  std::optional<KeyRange> cold_before_, cold_after_;
};

}  // namespace gen
}  // namespace ycsbr
