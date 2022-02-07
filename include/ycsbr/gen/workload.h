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
#include "ycsbr/trace_workload.h"

namespace ycsbr {
namespace gen {

// Represents a customizable workload with "phases". The workload configuration
// must be specified in a YAML file. See `tests/workloads/custom.yml` for an
// example.
class PhasedWorkload {
 public:
  // Creates a `PhasedWorkload` from the configuration in the provided file.
  // Set the `prng_seed` to ensure reproducibility. Setting
  // `set_record_size_bytes` to a positive value will override the record size
  // specified in the workload file, if any.
  static std::unique_ptr<PhasedWorkload> LoadFrom(
      const std::filesystem::path& config_file, uint32_t prng_seed = 42,
      const size_t set_record_size_bytes = 0);

  // Creates a `PhasedWorkload` from a configuration stored in a string. This
  // method is mainly useful for testing purposes. Setting
  // `set_record_size_bytes` to a positive value will override the record size
  // specified in the workload file, if any.
  static std::unique_ptr<PhasedWorkload> LoadFromString(
      const std::string& raw_config, uint32_t prng_seed = 42,
      const size_t set_record_size_bytes = 0);

  // Sets the "load dataset" that should be used. This method should be used
  // when you want to use a custom dataset. Note that the workload config file's
  // "load" section must specify that the distribution is "custom".
  void SetCustomLoadDataset(std::vector<Request::Key> dataset);

  // Retrieve the size of the records in the workload, in bytes.
  size_t GetRecordSizeBytes() const;

  // Get a load trace that can be used to load a database with the records used
  // in this workload.
  //
  // If `sort_requests` is set to true, the records in the trace will be sorted
  // in ascending order by key. If `sort_requests` is false, there are no
  // guarantees on the order of the records in the trace.
  //
  // NOTE: If a custom dataset is used, `SetCustomLoadDataset()` must be called
  // first before this method.
  BulkLoadTrace GetLoadTrace(bool sort_requests = false) const;

  class Producer;
  // Used by the workload runner to prepare the workload for execution. You
  // generally do not need to call this method.
  std::vector<Producer> GetProducers(size_t num_producers) const;

  // Not intended to be used directly. Use `LoadFrom()` instead.
  PhasedWorkload(std::shared_ptr<WorkloadConfig> config, uint32_t prng_seed);

 private:
  PRNG prng_;
  uint32_t prng_seed_;
  std::shared_ptr<WorkloadConfig> config_;
  std::shared_ptr<std::vector<Request::Key>> load_keys_;
};

// Used by the workload runner to actually execute the workload. This class
// generally does not need to be used directly.
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
  PRNG prng_;

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
