// Implementation of declarations in ycsbr/data.h. Do not include this header!
#include <algorithm>
#include <cstring>
#include <fstream>
#include <random>
#include <stdexcept>

namespace ycsbr {

// We recycle values in the synthetic dataset to avoid having to allocate too
// much memory for very large bulk loads.
constexpr size_t kNumUniqueValues = 1024;

inline Trace Trace::LoadFromFile(const std::string& file,
                                 const Options& options) {
  if (options.value_size < 4) {
    throw std::invalid_argument("Options::value_size must be at least 4.");
  }
  std::ifstream input(file, std::ios::in | std::ios::binary);
  if (!input) {
    throw std::runtime_error(
        "Failed to load workload from file. Error opening: " + file);
  }
  input.exceptions(std::ifstream::badbit);

  size_t num_writes = 0;
  std::vector<Request> trace_raw;
  Request::Encoded encoded;
  while (true) {
    uint32_t scan_amount = 0;
    input.read(reinterpret_cast<char*>(&encoded), sizeof(encoded));
    if (input.eof()) {
      break;
    }

    if (encoded.op == Request::Operation::kScan) {
      input.read(reinterpret_cast<char*>(&scan_amount), sizeof(scan_amount));
    }
    if (encoded.op == Request::Operation::kInsert ||
        encoded.op == Request::Operation::kUpdate) {
      num_writes += 1;
    }
    trace_raw.emplace_back(encoded.op,
                           options.use_v1_semantics && options.swap_key_bytes
                               ? __builtin_bswap64(encoded.key)
                               : encoded.key,
                           scan_amount, nullptr, 0);
  }

  if (options.sort_requests) {
    if (options.use_v1_semantics) {
      std::sort(trace_raw.begin(), trace_raw.end(),
                [](const Request& r1, const Request& r2) {
                  return memcmp(&r1.key, &r2.key, sizeof(r1.key)) < 0;
                });
    } else {
      std::sort(trace_raw.begin(), trace_raw.end());
    }
  }

  // Create the values and initialize them.
  size_t total_value_size = kNumUniqueValues * options.value_size;
  std::unique_ptr<char[]> values = std::make_unique<char[]>(total_value_size);
  std::mt19937 rng(options.rng_seed);
  size_t num_u32 = total_value_size / sizeof(uint32_t);
  uint32_t* start = reinterpret_cast<uint32_t*>(values.get());
  for (size_t i = 0; i < num_u32; ++i, ++start) {
    *start = rng();
  }

  std::vector<Request> trace;
  trace.reserve(trace_raw.size());
  size_t value_index = 0;
  for (size_t i = 0; i < trace_raw.size(); ++i) {
    const auto& raw = trace_raw[i];
    if (raw.op == Request::Operation::kInsert ||
        raw.op == Request::Operation::kUpdate) {
      trace.emplace_back(
          raw.op, raw.key, raw.scan_amount,
          &values[(value_index % kNumUniqueValues) * options.value_size],
          options.value_size);
      value_index += 1;
    } else {
      trace.emplace_back(raw);
    }
  }

  return Trace(std::move(trace), std::move(values), options.use_v1_semantics);
}

inline Trace::MinMaxKeys Trace::GetKeyRange() const {
  Request::Key min = begin()->key;
  Request::Key max = begin()->key;
  for (const auto& req : *this) {
    if (use_v1_semantics_) {
      if (memcmp(&req.key, &min, sizeof(Request::Key)) < 0) {
        min = req.key;
      }
      if (memcmp(&req.key, &max, sizeof(Request::Key)) > 0) {
        max = req.key;
      }

    } else {
      if (req.key < min) {
        min = req.key;
      }
      if (req.key > max) {
        max = req.key;
      }
    }
  }
  return MinMaxKeys(min, max);
}

inline BulkLoadTrace BulkLoadTrace::LoadFromFile(
    const std::string& file, const Trace::Options& options) {
  Trace workload = Trace::LoadFromFile(file, options);
  for (const auto& request : workload) {
    if (request.op != Request::Operation::kInsert) {
      throw std::invalid_argument(
          "This workload is not a bulk load workload (it contains non-insert "
          "requests).");
    }
  }
  return BulkLoadTrace(std::move(workload));
}

inline size_t BulkLoadTrace::DatasetSizeBytes() const {
  size_t total_size = 0;
  for (const auto& request : *this) {
    total_size += sizeof(request.key) + request.value_size;
  }
  return total_size;
}

}  // namespace ycsbr
