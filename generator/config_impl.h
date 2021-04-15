#pragma once

#include "yaml-cpp/yaml.h"
#include "ycsbr/gen/config.h"

namespace ycsbr {
namespace gen {

class WorkloadConfigImpl : public WorkloadConfig {
 public:
  WorkloadConfigImpl(YAML::Node raw_config);

  size_t GetNumLoadRecords() const override;
  std::unique_ptr<Generator> GetLoadGenerator() const override;

  size_t GetNumPhases() const override;
  Phase GetPhase(size_t phase_id) const override;
  std::unique_ptr<Generator> GetPhaseGenerator(size_t phase_id) const override;

 private:
  YAML::Node raw_config_;
};

}  // namespace gen
}  // namespace ycsbr
