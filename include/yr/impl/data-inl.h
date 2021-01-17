// Implementation of declarations in yr/data.h. Do not include this header!
#include <fstream>
#include <random>

#include "yr/data.h"

namespace yr {

Workload Workload::LoadFromFile(const std::string& file,
                                const Options& options) {
  if (options.value_size == 0) {
    throw std::invalid_argument("Options::value_size must be at least 1.");
  }
  std::ifstream input(file, std::ios::in | std::ios::binary);
  if (!input) {
    throw std::runtime_error(
        "Failed to load workload from file. Error opening: " + file);
  }
  input.exceptions(std::ifstream::failbit | std::ifstream::badbit);

  size_t num_writes = 0;
  std::vector<Request> workload_raw;
  Request::Encoded encoded;
  while (!input.eof()) {
    uint32_t scan_amount = 0;
    input.read(reinterpret_cast<char*>(&encoded), sizeof(encoded));
    if (encoded.op == Request::Operation::kScan) {
      input.read(reinterpret_cast<char*>(&scan_amount), sizeof(scan_amount));
    }
    if (encoded.op == Request::Operation::kInsert ||
        encoded.op == Request::Operation::kUpdate) {
      num_writes += 1;
    }
    workload_raw.emplace_back(encoded.op, encoded.key, scan_amount, nullptr, 0);
  }

  // Create the values and initialize them.
  size_t total_value_size = num_writes * options.value_size;
  std::unique_ptr<char[]> values = std::make_unique<char[]>(total_value_size);
  std::mt19937 rng(options.rng_seed);
  size_t num_u32 = total_value_size / sizeof(uint32_t);
  uint32_t* start = reinterpret_cast<uint32_t*>(values.get());
  for (size_t i = 0; i < num_u32; ++i, ++start) {
    *start = rng();
  }

  std::vector<Request> workload;
  workload.reserve(workload_raw.size());
  size_t value_index = 0;
  for (size_t i = 0; i < workload_raw.size(); ++i) {
    const auto& raw = workload_raw[i];
    if (raw.op == Request::Operation::kInsert ||
        raw.op == Request::Operation::kUpdate) {
      workload.emplace_back(raw.op, raw.key, raw.scan_amount,
                            &values[value_index * options.value_size],
                            options.value_size);
      value_index += 1;
    } else {
      workload.emplace_back(raw);
    }
  }

  return Workload(std::move(workload), std::move(values));
}

BulkLoadWorkload BulkLoadWorkload::LoadFromFile(
    const std::string& file, const Workload::Options& options) {
  Workload workload = Workload::LoadFromFile(file, options);
  for (const auto& request : workload) {
    if (request.op != Request::Operation::kInsert) {
      throw std::invalid_argument(
          "This workload is not a bulk load workload (it contains non-insert "
          "requests).");
    }
  }
  return BulkLoadWorkload(std::move(workload));
}

}  // namespace yr
