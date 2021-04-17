#pragma once

#include <memory>

#include "ycsbr/gen/chooser.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"

namespace ycsbr {
namespace gen {

// Tracks the current state of a workload phase.
// This is meant for internal use only.
struct Phase {
  explicit Phase(PhaseID phase_id)
      : phase_id(phase_id),
        num_inserts(0),
        num_inserts_left(0),
        num_requests(0),
        num_requests_left(0),
        read_thres(0),
        scan_thres(0),
        update_thres(0),
        max_scan_length(0) {}

  bool HasNext() const { return num_requests_left > 0; }

  void SetItemCount(const size_t item_count) {
    if (read_chooser != nullptr) {
      read_chooser->SetItemCount(item_count);
    }
    if (scan_chooser != nullptr) {
      scan_chooser->SetItemCount(item_count);
    }
    if (update_chooser != nullptr) {
      update_chooser->SetItemCount(item_count);
    }
  }

  void IncreaseItemCountBy(const size_t delta) {
    if (read_chooser != nullptr) {
      read_chooser->IncreaseItemCountBy(delta);
    }
    if (scan_chooser != nullptr) {
      scan_chooser->IncreaseItemCountBy(delta);
    }
    if (update_chooser != nullptr) {
      update_chooser->IncreaseItemCountBy(delta);
    }
  }

  PhaseID phase_id;

  size_t num_inserts, num_inserts_left;
  size_t num_requests, num_requests_left;

  uint32_t read_thres, scan_thres, update_thres;
  size_t max_scan_length;
  std::unique_ptr<Chooser> read_chooser;
  std::unique_ptr<Chooser> scan_chooser;
  std::unique_ptr<Chooser> scan_length_chooser;
  std::unique_ptr<Chooser> update_chooser;
};

}  // namespace gen
}  // namespace ycsbr
