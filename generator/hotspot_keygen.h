#pragma once

#include <optional>
#include <vector>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/keyrange.h"
#include "ycsbr/gen/types.h"

namespace ycsbr {
namespace gen {

class HotspotGenerator : public Generator {
 public:
  HotspotGenerator(size_t num_keys, uint32_t hot_proportion_pct,
                   KeyRange overall, KeyRange hot);

  void Generate(PRNG& prng, std::vector<Request::Key>* dest,
                size_t start_index) const override;

 private:
  size_t num_hot_keys_;
  KeyRange hot_;

  size_t num_cold_before_keys_, num_cold_after_keys_;
  std::optional<KeyRange> cold_before_, cold_after_;
};

}  // namespace gen
}  // namespace ycsbr
