#pragma once

#include <mutex>

#include "yaml-cpp/yaml.h"
#include "ycsbr/gen/config.h"
#include "ycsbr/gen/types.h"

namespace ycsbr {
namespace gen {

class WorkloadConfigImpl : public WorkloadConfig {
 public:
  WorkloadConfigImpl(YAML::Node raw_config,
                     const size_t set_record_size_bytes = 0);

  bool UsingCustomDataset() const override;
  size_t GetNumLoadRecords() const override;
  size_t GetRecordSizeBytes() const override;
  std::unique_ptr<Generator> GetLoadGenerator() const override;

  size_t GetNumPhases() const override;
  Phase GetPhase(PhaseID phase_id, ProducerID producer_id,
                 size_t num_producers) const override;
  std::unique_ptr<Generator> GetGeneratorForPhase(
      const Phase& phase) const override;
  std::optional<WorkloadConfig::CustomInserts> GetCustomInsertsForPhase(
      const Phase& phase) const override;

 private:
  bool UsingCustomDatasetImpl() const;
  size_t GetNumLoadRecordsImpl() const;

  // If the workload file did not specify the record size already, then it is
  // set to `set_record_size_bytes_` if it non-zero. Otherwise, an exception is
  // thrown.
  const size_t set_record_size_bytes_;

  // The config can be accessed concurrently. Even though all our methods are
  // `const`, it turns out that some `const` methods on `YAML::Node` are not
  // thread-safe. To be safe, we guard `raw_config_` with a mutex. This config
  // is not meant to be accessed during the workload, so this mutex will not
  // impact workload scalability.
  //
  // See https://github.com/jbeder/yaml-cpp/issues/419
  mutable std::mutex mutex_;
  const YAML::Node raw_config_;
};

}  // namespace gen
}  // namespace ycsbr
