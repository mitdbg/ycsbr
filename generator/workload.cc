#include "ycsbr/gen/workload.h"

namespace ycsbr {
namespace gen {

using Producer = PhasedWorkload::Producer;

std::shared_ptr<PhasedWorkload> PhasedWorkload::LoadFrom(
    const std::filesystem::path& config_file) {
  // 1. Load config file
  // 2. Validate config
  // 3. Pass to constructor
  return nullptr;
}

PhasedWorkload::PhasedWorkload() {
  // 1. Generate load keys
  // 2. Store config
}

std::vector<Producer> PhasedWorkload::GetProducers(
    const size_t num_producers) const {
  return std::vector<Producer>();
}

void Producer::Prepare() {
  // 1. Initialize choosers/distributions
  // 2. Compute indices and offsets
}

Request Producer::Next() {
  // Advance based on the phase
  return Request();
}

}  // namespace gen
}  // namespace ycsbr
