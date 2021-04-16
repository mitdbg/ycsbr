#pragma once

#include <memory>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Tracks the current state of a workload phase.
// This is meant for internal use only.
struct Phase {
  Phase()
      : num_inserts(0),
        num_total_requests(0),
        num_requests_left(0),
        read_thres(0),
        scan_thres(0),
        update_thres(0) {}

  bool HasNext() const { return num_requests_left > 0; }

  size_t num_inserts;
  size_t num_total_requests;
  size_t num_requests_left;

  uint32_t read_thres, scan_thres, update_thres;
  std::unique_ptr<Chooser> read_chooser;
  std::unique_ptr<Chooser> scan_chooser;
  std::unique_ptr<Chooser> update_chooser;
};

}  // namespace gen
}  // namespace ycsbr
