#pragma once

#include <filesystem>
#include <memory>

#include "ycsbr/gen/keygen.h"
#include "ycsbr/gen/phase.h"

namespace ycsbr {
namespace gen {

class WorkloadConfig {
 public:
  static std::unique_ptr<WorkloadConfig> LoadFrom(
      const std::filesystem::path& config_file);
 
  virtual size_t GetNumLoadRecords() const = 0;
  virtual std::unique_ptr<Generator> GetLoadGenerator() const = 0;

  virtual size_t GetNumPhases() const = 0;
  virtual Phase GetPhase(size_t phase_id) const = 0;
  virtual std::unique_ptr<Generator> GetPhaseGenerator(size_t phase_id) const = 0;

  virtual ~WorkloadConfig() = default;
};

}  // namespace gen
}  // namespace ycsbr
