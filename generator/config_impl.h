#pragma once

#include "yaml-cpp/yaml.h"
#include "ycsbr/gen/config.h"
#include "ycsbr/gen/types.h"

namespace ycsbr {
namespace gen {

class WorkloadConfigImpl : public WorkloadConfig {
 public:
  WorkloadConfigImpl(YAML::Node raw_config);

  size_t GetNumLoadRecords() const override;
  size_t GetRecordSizeBytes() const override;
  std::unique_ptr<Generator> GetLoadGenerator() const override;

  size_t GetNumPhases() const override;
  Phase GetPhase(PhaseID phase_id, ProducerID producer_id,
                 size_t num_producers) const override;
  std::unique_ptr<Generator> GetGeneratorForPhase(
      const Phase& phase) const override;

 private:
  YAML::Node raw_config_;
};

}  // namespace gen
}  // namespace ycsbr
