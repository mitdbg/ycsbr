#pragma once

#include "yaml-cpp/yaml.h"
#include "ycsbr/gen/config.h"

namespace ycsbr {
namespace gen {

class WorkloadConfigImpl : public WorkloadConfig {
 public:
  WorkloadConfigImpl(YAML::Node raw_config);

 private:
  YAML::Node raw_config_;
};

}  // namespace gen
}  // namespace ycsbr
