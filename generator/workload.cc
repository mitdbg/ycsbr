#include "ycsbr/gen/workload.h"

#include "ycsbr/gen/types.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

void ApplyPhaseAndProducerIDs(std::vector<Request::Key>* keys,
                              const PhaseID phase_id,
                              const ProducerID producer_id) {
  for (auto& key : *keys) {
    key = (key << 16) | ((phase_id & 0xFF) << 8) | (producer_id & 0xFF);
  }
}

}  // namespace

namespace ycsbr {
namespace gen {

using Producer = PhasedWorkload::Producer;

std::shared_ptr<PhasedWorkload> PhasedWorkload::LoadFrom(
    const std::filesystem::path& config_file, const uint32_t prng_seed) {
  return std::make_shared<PhasedWorkload>(WorkloadConfig::LoadFrom(config_file),
                                          prng_seed);
}

PhasedWorkload::PhasedWorkload(std::unique_ptr<WorkloadConfig> config,
                               const uint32_t prng_seed)
    : prng_(prng_seed),
      config_(std::move(config)),
      load_keys_(config_->GetNumLoadRecords(), 0) {
  auto load_gen = config_->GetLoadGenerator();
  load_gen->Generate(prng_, &load_keys_, 0);
  ApplyPhaseAndProducerIDs(&load_keys_, /*phase_id=*/0, /*producer_id=*/0);
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
