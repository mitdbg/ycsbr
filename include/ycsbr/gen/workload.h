#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <vector>

#include "ycsbr/gen/config.h"
#include "ycsbr/gen/phase.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/request.h"
#include "ycsbr/workload/trace.h"

namespace ycsbr {
namespace gen {

class PhasedWorkload {
 public:
  static std::shared_ptr<PhasedWorkload> LoadFrom(
      const std::filesystem::path& config_file, uint32_t prng_seed = 42);

  BulkLoadTrace GetLoadTrace() const;

  class Producer;
  std::vector<Producer> GetProducers(size_t num_producers) const;

  // Not intended to be used directly. Use `LoadFrom()` instead.
  PhasedWorkload(std::unique_ptr<WorkloadConfig> config, uint32_t prng_seed);

 private:
  std::mt19937 prng_;
  std::unique_ptr<WorkloadConfig> config_;
  std::vector<Request::Key> load_keys_;
};

class PhasedWorkload::Producer {
 public:
  void Prepare();

  bool HasNext() const {
    return current_phase_ < phases_.size() && phases_[current_phase_].HasNext();
  }
  Request Next();

 private:
  ProducerID id_;
  std::shared_ptr<PhasedWorkload> workload_;

  std::vector<Phase> phases_;
  PhaseID current_phase_;

  // Stores all the keys this producer will eventually insert.
  std::vector<Request::Key> insert_keys_;
  size_t next_insert_key_index_;
};

}  // namespace gen
}  // namespace ycsbr
