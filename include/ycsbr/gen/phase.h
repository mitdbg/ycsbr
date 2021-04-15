#pragma once

#include <memory>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

class Phase {
 public:
  bool HasNext() const { return num_operations_left_ > 0; }

 private:
  size_t num_inserts_;
  size_t num_total_operations_;
  size_t num_operations_left_;

  uint32_t read_thres_, scan_thres_, update_thres_;
  std::unique_ptr<Chooser> read_chooser_;
  std::unique_ptr<Chooser> scan_chooser_;
  std::unique_ptr<Chooser> update_chooser_;
};

}  // namespace gen
}  // namespace ycsbr
