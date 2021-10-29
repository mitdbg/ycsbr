#include "ycsbr/gen/workload.h"

#include <cassert>

#include "ycsbr/buffered_workload.h"
#include "ycsbr/gen/types.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

// Producers will cycle through this many unique values (when inserting or
// making updates).
constexpr size_t kNumUniqueValues = 100;

void ApplyPhaseAndProducerIDs(std::vector<Request::Key>::iterator begin,
                              std::vector<Request::Key>::iterator end,
                              const PhaseID phase_id,
                              const ProducerID producer_id) {
  for (auto it = begin; it != end; ++it) {
    *it = (*it << 16) | ((phase_id & 0xFF) << 8) | (producer_id & 0xFF);
  }
}

}  // namespace

namespace ycsbr {

// For convenience, instantiate a `BufferedWorkload` for `PhasedWorkload`.
template class BufferedWorkload<gen::PhasedWorkload>;

namespace gen {

using Producer = PhasedWorkload::Producer;

std::unique_ptr<PhasedWorkload> PhasedWorkload::LoadFrom(
    const std::filesystem::path& config_file, const uint32_t prng_seed,
    const size_t set_record_size_bytes) {
  return std::make_unique<PhasedWorkload>(
      WorkloadConfig::LoadFrom(config_file, set_record_size_bytes), prng_seed);
}

std::unique_ptr<PhasedWorkload> PhasedWorkload::LoadFromString(
    const std::string& raw_config, const uint32_t prng_seed,
    const size_t set_record_size_bytes) {
  return std::make_unique<PhasedWorkload>(
      WorkloadConfig::LoadFromString(raw_config, set_record_size_bytes),
      prng_seed);
}

PhasedWorkload::PhasedWorkload(std::shared_ptr<WorkloadConfig> config,
                               const uint32_t prng_seed)
    : prng_(prng_seed),
      prng_seed_(prng_seed),
      config_(std::move(config)),
      load_keys_(nullptr) {
  // If we're using a custom dataset, the user will call SetCustomLoadDataset()
  // to configure `load_keys_`.
  if (config_->UsingCustomDataset()) return;

  load_keys_ = std::make_shared<std::vector<Request::Key>>(
      config_->GetNumLoadRecords(), 0);
  auto load_gen = config_->GetLoadGenerator();
  load_gen->Generate(prng_, load_keys_.get(), 0);
  ApplyPhaseAndProducerIDs(load_keys_->begin(), load_keys_->end(),
                           /*phase_id=*/0,
                           /*producer_id=*/0);
}

void PhasedWorkload::SetCustomLoadDataset(std::vector<Request::Key> dataset) {
  assert(dataset.size() > 0);
  if (*std::max_element(dataset.begin(), dataset.end()) > kMaxKey) {
    throw std::invalid_argument("The maximum supported key is 2^48 - 1.");
  }
  load_keys_ = std::make_shared<std::vector<Request::Key>>(std::move(dataset));
  ApplyPhaseAndProducerIDs(load_keys_->begin(), load_keys_->end(),
                           /*phase_id=*/0, /*producer_id=*/0);
  std::shuffle(load_keys_->begin(), load_keys_->end(), prng_);
}

size_t PhasedWorkload::GetRecordSizeBytes() const {
  return config_->GetRecordSizeBytes();
}

BulkLoadTrace PhasedWorkload::GetLoadTrace() const {
  Trace::Options options;
  options.value_size = config_->GetRecordSizeBytes() - sizeof(Request::Key);
  return BulkLoadTrace::LoadFromKeys(*load_keys_, options);
}

std::vector<Producer> PhasedWorkload::GetProducers(
    const size_t num_producers) const {
  std::vector<Producer> producers;
  producers.reserve(num_producers);
  for (ProducerID id = 0; id < num_producers; ++id) {
    producers.push_back(
        // Each Producer's workload should be deterministic, but we want each
        // Producer to produce different requests from each other. So we include
        // the producer ID in its seed.
        Producer(config_, load_keys_, id, num_producers, prng_seed_ ^ id));
  }
  return producers;
}

Producer::Producer(std::shared_ptr<const WorkloadConfig> config,
                   std::shared_ptr<const std::vector<Request::Key>> load_keys,
                   const ProducerID id, const size_t num_producers,
                   const uint32_t prng_seed)
    : id_(id),
      num_producers_(num_producers),
      config_(std::move(config)),
      prng_(prng_seed),
      current_phase_(0),
      load_keys_(std::move(load_keys)),
      num_load_keys_(load_keys_->size()),
      next_insert_key_index_(0),
      valuegen_(config_->GetRecordSizeBytes() - sizeof(Request::Key),
                kNumUniqueValues, prng_),
      op_dist_(0, 99) {}

void Producer::Prepare() {
  // Set up the workload phases.
  const size_t num_phases = config_->GetNumPhases();
  phases_.reserve(num_phases);
  for (PhaseID phase_id = 0; phase_id < num_phases; ++phase_id) {
    phases_.push_back(config_->GetPhase(phase_id, id_, num_producers_));
  }

  // Generate the inserts.
  size_t insert_index = 0;
  for (auto& phase : phases_) {
    if (phase.num_inserts == 0) continue;
    auto generator = config_->GetGeneratorForPhase(phase);
    assert(generator != nullptr);
    insert_keys_.resize(insert_keys_.size() + phase.num_inserts);
    generator->Generate(prng_, &insert_keys_, insert_index);
    ApplyPhaseAndProducerIDs(
        insert_keys_.begin() + insert_index,
        insert_keys_.begin() + insert_index + phase.num_inserts,
        // We add 1 because ID 0 is reserved for the initial load.
        phase.phase_id + 1, id_ + 1);
    insert_index = insert_keys_.size();
  }

  // Set the phase chooser item counts based on the number of inserts the
  // producer will make in each phase.
  size_t count = load_keys_->size();
  for (auto& phase : phases_) {
    phase.SetItemCount(count);
    count += phase.num_inserts;
  }
}

Request::Key Producer::ChooseKey(const std::unique_ptr<Chooser>& chooser) {
  const size_t index = chooser->Next(prng_);
  if (index < num_load_keys_) {
    return (*load_keys_)[index];
  }
  return insert_keys_[index - num_load_keys_];
}

Request Producer::Next() {
  assert(HasNext());
  Phase& this_phase = phases_[current_phase_];

  Request::Operation next_op = Request::Operation::kInsert;

  // If there are more requests left than inserts, we can randomly decide what
  // request to do next. Otherwise we must do an insert. Note that we adjust
  // `op_dist_` as needed to ensure that we do not generate an insert once
  // `this_phase.num_inserts_left == 0`.
  if (this_phase.num_inserts_left < this_phase.num_requests_left) {
    // Decide what operation to do.
    const uint32_t choice = op_dist_(prng_);
    if (choice < this_phase.read_thres) {
      next_op = Request::Operation::kRead;
    } else if (choice < this_phase.rmw_thres) {
      next_op = Request::Operation::kReadModifyWrite;
    } else if (choice < this_phase.negativeread_thres) {
      next_op = Request::Operation::kNegativeRead;
    } else if (choice < this_phase.scan_thres) {
      next_op = Request::Operation::kScan;
    } else if (choice < this_phase.update_thres) {
      next_op = Request::Operation::kUpdate;
    } else {
      next_op = Request::Operation::kInsert;
      assert(this_phase.num_inserts_left > 0);
    }
  }

  Request to_return;
  switch (next_op) {
    case Request::Operation::kRead: {
      to_return = Request(Request::Operation::kRead,
                          ChooseKey(this_phase.read_chooser), 0, nullptr, 0);
      break;
    }

    case Request::Operation::kReadModifyWrite: {
      to_return = Request(Request::Operation::kReadModifyWrite,
                          ChooseKey(this_phase.rmw_chooser), 0,
                          valuegen_.NextValue(), valuegen_.value_size());
      break;
    }

    case Request::Operation::kNegativeRead: {
      Request::Key to_read = ChooseKey(this_phase.negativeread_chooser);
      to_read |= (0xFF << 8);
      to_return =
          Request(Request::Operation::kNegativeRead, to_read, 0, nullptr, 0);
      break;
    }

    case Request::Operation::kScan: {
      // We add 1 to the chosen scan length because `Chooser` instances always
      // return values in a 0-based range.
      to_return =
          Request(Request::Operation::kScan, ChooseKey(this_phase.scan_chooser),
                  this_phase.scan_length_chooser->Next(prng_) + 1, nullptr, 0);
      break;
    }

    case Request::Operation::kUpdate: {
      to_return = Request(Request::Operation::kUpdate,
                          ChooseKey(this_phase.update_chooser), 0,
                          valuegen_.NextValue(), valuegen_.value_size());
      break;
    }

    case Request::Operation::kInsert: {
      to_return = Request(Request::Operation::kInsert,
                          insert_keys_[next_insert_key_index_], 0,
                          valuegen_.NextValue(), valuegen_.value_size());
      ++next_insert_key_index_;
      --this_phase.num_inserts_left;
      this_phase.IncreaseItemCountBy(1);
      if (this_phase.num_inserts_left == 0) {
        // No more inserts left. We adjust the operation selection distribution
        // to make sure we no longer select inserts during this phase. Note that
        // the bounds used below are inclusive.
        if (this_phase.update_thres > 0) {
          op_dist_ = std::uniform_int_distribution<uint32_t>(
              0, this_phase.update_thres - 1);
        } else {
          // This case should only occur if the workload is insert-only. However
          // this means that this was the last request (we decrement the
          // requests counter below).
          assert(this_phase.num_requests_left == 1);
        }
      }
      break;
    }
  }

  // Advance to the next request.
  --this_phase.num_requests_left;
  if (this_phase.num_requests_left == 0) {
    ++current_phase_;
    // Reset the operation selection distribution.
    op_dist_ = std::uniform_int_distribution<uint32_t>(0, 99);
  }
  return to_return;
}

}  // namespace gen
}  // namespace ycsbr
