#include "ycsbr/gen/workload.h"

#include <cassert>

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

PhasedWorkload::PhasedWorkload(std::shared_ptr<WorkloadConfig> config,
                               const uint32_t prng_seed)
    : prng_(prng_seed),
      prng_seed_(prng_seed),
      config_(std::move(config)),
      load_keys_(config_->GetNumLoadRecords(), 0) {
  auto load_gen = config_->GetLoadGenerator();
  load_gen->Generate(prng_, &load_keys_, 0);
  ApplyPhaseAndProducerIDs(&load_keys_, /*phase_id=*/0, /*producer_id=*/0);
}

BulkLoadTrace PhasedWorkload::GetLoadTrace() const {
  Trace::Options options;
  options.value_size = config_->GetRecordSizeBytes() - sizeof(Request::Key);
  return BulkLoadTrace::LoadFromKeys(load_keys_, options);
}

std::vector<Producer> PhasedWorkload::GetProducers(
    const size_t num_producers) const {
  std::vector<Producer> producers;
  producers.reserve(num_producers);
  for (ProducerID id = 0; id < num_producers; ++id) {
    producers.push_back(
        Producer(shared_from_this(), id, num_producers, prng_seed_ ^ id));
  }
  return producers;
}

Producer::Producer(std::shared_ptr<const PhasedWorkload> workload,
                   const ProducerID id, const size_t num_producers,
                   const uint32_t prng_seed)
    : id_(id),
      num_producers_(num_producers),
      workload_(std::move(workload)),
      prng_(prng_seed),
      current_phase_(0),
      next_insert_key_index_(0) {}

void Producer::Prepare() {
  // Set up the workload phases.
  const size_t num_phases = workload_->config_->GetNumPhases();
  phases_.reserve(num_phases);
  for (PhaseID phase_id = 0; phase_id < num_phases; ++phase_id) {
    phases_.push_back(
        workload_->config_->GetPhase(phase_id, id_, num_producers_));
  }

  // Generate the inserts.
  size_t insert_index = 0;
  for (auto& phase : phases_) {
    if (phase.num_inserts == 0) continue;
    auto generator = workload_->config_->GetGeneratorForPhase(phase);
    assert(generator != nullptr);
    insert_keys_.resize(insert_keys_.size() + phase.num_inserts);
    generator->Generate(prng_, &insert_keys_, insert_index);
    insert_index = insert_keys_.size();
  }

  // Update the phase chooser item counts.
  size_t count = workload_->load_keys_.size();
  bool first_iter = true;
  for (auto& phase : phases_) {
    if (!first_iter) {
      if (phase.read_chooser != nullptr) {
        phase.read_chooser->IncreaseItemCount(count);
      }
      if (phase.scan_chooser != nullptr) {
        phase.scan_chooser->IncreaseItemCount(count);
      }
      if (phase.update_chooser != nullptr) {
        phase.update_chooser->IncreaseItemCount(count);
      }
    }
    first_iter = false;
    count += phase.num_inserts;
  }
}

Request Producer::Next() {
  // Advance based on the phase
  return Request();
}

}  // namespace gen
}  // namespace ycsbr
