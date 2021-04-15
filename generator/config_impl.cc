#include "config_impl.h"

#include <iostream>

#include "yaml-cpp/yaml.h"

namespace {

bool ValidateConfig(const YAML::Node& raw_config) { return true; }

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

}  // namespace gen
}  // namespace ycsbr
