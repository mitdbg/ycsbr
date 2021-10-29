#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/phase.h"
#include "ycsbr/gen/types.h"

namespace ycsbr {
namespace gen {

class WorkloadConfig {
 public:
  // Setting `set_record_size_bytes` to a positive value will override the
  // record size specified in the workload file, if any.
  static std::shared_ptr<WorkloadConfig> LoadFrom(
      const std::filesystem::path& config_file,
      const size_t set_record_size_bytes = 0);
  static std::shared_ptr<WorkloadConfig> LoadFromString(
      const std::string& raw_config, const size_t set_record_size_bytes = 0);

  virtual bool UsingCustomDataset() const = 0;
  virtual size_t GetNumLoadRecords() const = 0;
  virtual size_t GetRecordSizeBytes() const = 0;
  virtual std::unique_ptr<Generator> GetLoadGenerator() const = 0;

  virtual size_t GetNumPhases() const = 0;
  virtual Phase GetPhase(PhaseID phase_id, ProducerID producer_id,
                         size_t num_producers) const = 0;
  virtual std::unique_ptr<Generator> GetGeneratorForPhase(
      const Phase& phase) const = 0;

  virtual ~WorkloadConfig() = default;
};

}  // namespace gen
}  // namespace ycsbr
