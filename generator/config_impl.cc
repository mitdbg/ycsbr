#include "config_impl.h"

#include <iostream>

#include "yaml-cpp/yaml.h"

namespace {

const std::string kLoadConfigKey = "load";
const std::string kRunConfigKey = "run";

// Only does a quick high-level structural validation. The semantic validation
// is done when phases are retrieved.
bool ValidateConfig(const YAML::Node& raw_config) {
  if (!raw_config.IsMap()) {
    std::cerr << "ERROR: Workload config needs to be a YAML map." << std::endl;
    return false;
  }
  if (!raw_config[kLoadConfigKey]) {
    std::cerr << "ERROR: Missing workload config '" << kLoadConfigKey
              << "' section." << std::endl;
    return false;
  }
  if (!raw_config[kRunConfigKey]) {
    std::cerr << "ERROR: Missing workload config '" << kRunConfigKey
              << "' section." << std::endl;
    return false;
  }
  if (!raw_config[kRunConfigKey].IsSequence()) {
    std::cerr << "ERROR: The workload config's '" << kRunConfigKey
              << "' section should be a list of phases." << std::endl;
    return false;
  }
  for (const auto& raw_phase : raw_config[kRunConfigKey]) {
    if (!raw_phase.IsMap()) {
      std::cerr
          << "ERROR: Each phase in the workload config should be a YAML map."
          << std::endl;
      return false;
    }
  }

  return true;
}

}  // namespace

namespace ycsbr {
namespace gen {

std::unique_ptr<WorkloadConfig> WorkloadConfig::LoadFrom(
    const std::filesystem::path& config_file) {
  try {
    YAML::Node node = YAML::LoadFile(config_file);
    if (!ValidateConfig(node)) {
      return nullptr;
    }
    return std::make_unique<WorkloadConfigImpl>(std::move(node));

  } catch (const YAML::BadFile&) {
    return nullptr;
  }
}

WorkloadConfigImpl::WorkloadConfigImpl(YAML::Node raw_config)
    : raw_config_(std::move(raw_config)) {}

size_t WorkloadConfigImpl::GetNumLoadRecords() const { return 0; }

std::unique_ptr<Generator> WorkloadConfigImpl::GetLoadGenerator() const {
  return nullptr;
}

size_t WorkloadConfigImpl::GetNumPhases() const { return 0; }

Phase WorkloadConfigImpl::GetPhase(size_t phase_id) const { return Phase(); }

std::unique_ptr<Generator> WorkloadConfigImpl::GetPhaseGenerator(
    size_t phase_id) const {
  return nullptr;
}

}  // namespace gen
}  // namespace ycsbr
