#include "ycsbr/gen/workload.h"

namespace ycsbr {
namespace gen {

using Producer = PhasedWorkload::Producer;

std::shared_ptr<PhasedWorkload> PhasedWorkload::LoadFrom(
    const std::filesystem::path& config_file) {
  std::unique_ptr<WorkloadConfig> config = WorkloadConfig::LoadFrom(config_file);
  if (config == nullptr) {
    return nullptr;
  }
  return std::make_shared<PhasedWorkload>(std::move(config));
}

PhasedWorkload::PhasedWorkload(std::unique_ptr<WorkloadConfig> config)
    : config_(std::move(config)) {
  // TODO: Generate load keys
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
