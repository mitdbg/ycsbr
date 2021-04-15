#pragma once

#include <filesystem>
#include <memory>

namespace ycsbr {
namespace gen {

class WorkloadConfig {
 public:
  static std::unique_ptr<WorkloadConfig> LoadFrom(
      const std::filesystem::path& config_file);
  
  virtual ~WorkloadConfig() = default;
};

}  // namespace gen
}  // namespace ycsbr
