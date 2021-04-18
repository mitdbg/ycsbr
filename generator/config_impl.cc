#include "config_impl.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

#include "uniform_chooser.h"
#include "uniform_keygen.h"
#include "yaml-cpp/yaml.h"

namespace {

// Top-level keys.
const std::string kLoadConfigKey = "load";
const std::string kRunConfigKey = "run";
const std::string kRecordSizeBytesKey = "record_size_bytes";

// Operation keys.
const std::string kReadOpKey = "read";
const std::string kScanOpKey = "scan";
const std::string kUpdateOpKey = "update";
const std::string kInsertOpKey = "insert";

// Assorted keys.
const std::string kNumRecordsKey = "num_records";
const std::string kNumRequestsKey = "num_requests";
const std::string kDistributionKey = "distribution";
const std::string kDistributionTypeKey = "type";
const std::string kProportionKey = "proportion_pct";
const std::string kScanMaxLengthKey = "max_length";

// Distribution names and keys.
const std::string kUniformDist = "uniform";
const std::string kZipfianDist = "zipfian";
const std::string kHotspotDist = "hotspot";

const std::string kRangeMinKey = "range_min";
const std::string kRangeMaxKey = "range_max";
const std::string kZipfianThetaKey = "theta";
const std::string kHotspotProportionKey = "hot_proportion_pct";
const std::string kHotRangeMinKey = "hot_range_min";
const std::string kHotRangeMaxKey = "hot_range_max";

// Only does a quick high-level structural validation. The semantic validation
// is done when phases are retrieved.
bool ValidateConfig(const YAML::Node& raw_config) {
  if (!raw_config.IsMap()) {
    std::cerr << "ERROR: Workload config needs to be a YAML map." << std::endl;
    return false;
  }
  if (!raw_config[kRecordSizeBytesKey]) {
    std::cerr << "ERROR: Missing workload config '" << kRecordSizeBytesKey
              << "' value." << std::endl;
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

std::shared_ptr<WorkloadConfig> WorkloadConfig::LoadFrom(
    const std::filesystem::path& config_file) {
  try {
    YAML::Node node = YAML::LoadFile(config_file);
    if (!ValidateConfig(node)) {
      throw std::invalid_argument("Invalid workload configuration file.");
    }
    return std::make_shared<WorkloadConfigImpl>(std::move(node));
  } catch (const YAML::BadFile&) {
    throw std::invalid_argument(
        "Could not parse the workload configuration file.");
  }
}

std::shared_ptr<WorkloadConfig> WorkloadConfig::LoadFromString(
    const std::string& raw_config) {
  YAML::Node node = YAML::Load(raw_config);
  if (!ValidateConfig(node)) {
    throw std::invalid_argument("Invalid workload configuration string.");
  }
  return std::make_shared<WorkloadConfigImpl>(std::move(node));
}

WorkloadConfigImpl::WorkloadConfigImpl(YAML::Node raw_config)
    : raw_config_(std::move(raw_config)) {}

size_t WorkloadConfigImpl::GetNumLoadRecords() const {
  return raw_config_[kLoadConfigKey][kNumRecordsKey].as<size_t>();
}

size_t WorkloadConfigImpl::GetRecordSizeBytes() const {
  const size_t record_size_bytes =
      raw_config_[kRecordSizeBytesKey].as<size_t>();
  if (record_size_bytes < 9) {
    throw std::invalid_argument("Record sizes must be at least 9 bytes.");
  }
  return record_size_bytes;
}

std::unique_ptr<Generator> WorkloadConfigImpl::GetLoadGenerator() const {
  const YAML::Node& load_dist = raw_config_[kLoadConfigKey][kDistributionKey];
  if (!load_dist) {
    throw std::invalid_argument("Missing load distribution configuration.");
  }
  const std::string dist_type =
      load_dist[kDistributionTypeKey].as<std::string>();
  if (dist_type == kUniformDist) {
    const size_t num_records = GetNumLoadRecords();
    const Request::Key range_min = load_dist[kRangeMinKey].as<Request::Key>();
    const Request::Key range_max = load_dist[kRangeMaxKey].as<Request::Key>();
    return std::make_unique<UniformGenerator>(num_records, range_min,
                                              range_max);

  } else {
    throw std::invalid_argument("Unsupported load distribution: " + dist_type);
  }
}

size_t WorkloadConfigImpl::GetNumPhases() const {
  const size_t num_phases = raw_config_[kRunConfigKey].size();
  if (num_phases > std::numeric_limits<uint8_t>::max() - 1) {
    throw std::invalid_argument(
        "Too many workload phases (only 255 are supported).");
  }
  return num_phases;
}

Phase WorkloadConfigImpl::GetPhase(const PhaseID phase_id,
                                   const ProducerID producer_id,
                                   const size_t num_producers) const {
  const YAML::Node& phase_config = raw_config_[kRunConfigKey][phase_id];
  const size_t num_load_records = GetNumLoadRecords();
  Phase phase(phase_id);

  // Compute the number of requests for this producer.
  const size_t total_requests = phase_config[kNumRequestsKey].as<size_t>();
  phase.num_requests = total_requests / num_producers;
  const size_t remainder = total_requests % num_producers;
  if (producer_id < remainder) {
    ++phase.num_requests;
  }
  phase.num_requests_left = phase.num_requests;

  // Load the request proportions and validate them.
  uint32_t insert_pct = 0;
  if (phase_config[kReadOpKey]) {
    phase.read_thres = phase_config[kReadOpKey][kProportionKey].as<uint32_t>();

    // Create the read key chooser.
    const auto& dist = phase_config[kReadOpKey][kDistributionKey];
    const std::string& dist_type = dist[kDistributionTypeKey].as<std::string>();
    if (dist_type == kUniformDist) {
      phase.read_chooser = std::make_unique<UniformChooser>(num_load_records);
    } else {
      throw std::invalid_argument("Unsupported read distribution: " +
                                  dist_type);
    }
  }
  if (phase_config[kScanOpKey]) {
    phase.scan_thres = phase_config[kScanOpKey][kProportionKey].as<uint32_t>();
    phase.max_scan_length =
        phase_config[kScanOpKey][kScanMaxLengthKey].as<size_t>();
    if (phase.max_scan_length == 0) {
      throw std::invalid_argument(
          "The maximum scan length must be at least 1.");
    }

    // Create the scan key chooser.
    const auto& dist = phase_config[kScanOpKey][kDistributionKey];
    const std::string& dist_type = dist[kDistributionTypeKey].as<std::string>();
    if (dist_type == kUniformDist) {
      phase.scan_chooser = std::make_unique<UniformChooser>(num_load_records);
    } else {
      throw std::invalid_argument("Unsupported scan distribution: " +
                                  dist_type);
    }
    // We need to add 1 because the UniformChooser returns values in a 0-based
    // exclusive upper range.
    phase.scan_length_chooser =
        std::make_unique<UniformChooser>(phase.max_scan_length + 1);
  }
  if (phase_config[kUpdateOpKey]) {
    phase.update_thres =
        phase_config[kUpdateOpKey][kProportionKey].as<uint32_t>();

    // Create the update key chooser.
    const auto& dist = phase_config[kUpdateOpKey][kDistributionKey];
    const std::string& dist_type = dist[kDistributionTypeKey].as<std::string>();
    if (dist_type == kUniformDist) {
      phase.update_chooser = std::make_unique<UniformChooser>(num_load_records);
    } else {
      throw std::invalid_argument("Unsupported update distribution: " +
                                  dist_type);
    }
  }
  if (phase_config[kInsertOpKey]) {
    insert_pct = phase_config[kInsertOpKey][kProportionKey].as<uint32_t>();
  }
  if (insert_pct + phase.read_thres + phase.scan_thres + phase.update_thres !=
      100) {
    throw std::invalid_argument(
        "Request proportions must sum to exactly 100%.");
  }

  // Compute the number of inserts we should expect to do.
  phase.num_inserts =
      static_cast<size_t>(phase.num_requests * (insert_pct / 100.0));
  phase.num_inserts_left = phase.num_inserts;

  // Set the thresholds appropriately to allow for comparsion against a random
  // integer generated in the range [0, 100).
  phase.scan_thres += phase.read_thres;
  phase.update_thres += phase.scan_thres;

  return phase;
}

std::unique_ptr<Generator> WorkloadConfigImpl::GetGeneratorForPhase(
    const Phase& phase) const {
  const YAML::Node& phase_config = raw_config_[kRunConfigKey][phase.phase_id];
  if (!phase_config) {
    throw std::invalid_argument("Nonexistent phase id: " + phase.phase_id);
  }
  if (!phase_config[kInsertOpKey] || phase.num_inserts == 0) {
    // There are no inserts.
    return nullptr;
  }

  const YAML::Node& dist = phase_config[kInsertOpKey][kDistributionKey];
  const std::string dist_type = dist[kDistributionTypeKey].as<std::string>();
  if (dist_type == kUniformDist) {
    const Request::Key range_min = dist[kRangeMinKey].as<Request::Key>();
    const Request::Key range_max = dist[kRangeMaxKey].as<Request::Key>();
    return std::make_unique<UniformGenerator>(phase.num_inserts, range_min,
                                              range_max);
  } else {
    throw std::invalid_argument("Unsupported insert distribution: " +
                                dist_type);
  }
}

}  // namespace gen
}  // namespace ycsbr
