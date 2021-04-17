#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <vector>

#include "ycsbr/gen/config.h"
#include "ycsbr/gen/phase.h"
#include "ycsbr/gen/types.h"
#include "ycsbr/gen/valuegen.h"
#include "ycsbr/request.h"
#include "ycsbr/workload/trace.h"

namespace ycsbr {
namespace gen {

class PhasedWorkload {
 public:
  static std::unique_ptr<PhasedWorkload> LoadFrom(
      const std::filesystem::path& config_file, uint32_t prng_seed = 42);
  static std::unique_ptr<PhasedWorkload> LoadFromString(
      const std::string& raw_config, uint32_t prng_seed = 42);

  BulkLoadTrace GetLoadTrace() const;

  class Producer;
  std::vector<Producer> GetProducers(size_t num_producers) const;

  // Not intended to be used directly. Use `LoadFrom()` instead.
  PhasedWorkload(std::shared_ptr<WorkloadConfig> config, uint32_t prng_seed);

 private:
  std::mt19937 prng_;
  uint32_t prng_seed_;
  std::shared_ptr<WorkloadConfig> config_;
  std::shared_ptr<std::vector<Request::Key>> load_keys_;
};

class PhasedWorkload::Producer {
 public:
  void Prepare();

  bool HasNext() const {
    return current_phase_ < phases_.size() && phases_[current_phase_].HasNext();
  }
  Request Next();

 private:
  friend class PhasedWorkload;
  Producer(std::shared_ptr<const WorkloadConfig> config,
           std::shared_ptr<const std::vector<Request::Key>> load_keys,
           ProducerID id, size_t num_producers, uint32_t prng_seed);

  Request::Key ChooseKey(const std::unique_ptr<Chooser>& chooser);

  ProducerID id_;
  size_t num_producers_;
  std::shared_ptr<const WorkloadConfig> config_;
  std::mt19937 prng_;

  std::vector<Phase> phases_;
  PhaseID current_phase_;

  // The keys that were loaded.
  std::shared_ptr<const std::vector<Request::Key>> load_keys_;
  size_t num_load_keys_;

  // Stores all the keys this producer will eventually insert.
  std::vector<Request::Key> insert_keys_;
  size_t next_insert_key_index_;

  ValueGenerator valuegen_;

  std::uniform_int_distribution<uint32_t> op_dist_;
};

}  // namespace gen
}  // namespace ycsbr
