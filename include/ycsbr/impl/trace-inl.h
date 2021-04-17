// Implementation of declarations in ycsbr/trace.h. Do not include this header!
#include <algorithm>
#include <cstring>
#include <fstream>
#include <random>
#include <stdexcept>

#include "util.h"

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

  return ProcessRawTrace(std::move(trace_raw), options);
}

inline Trace Trace::ProcessRawTrace(std::vector<Request> raw_trace,
                                    const Options& options) {
  if (options.sort_requests) {
    if (options.use_v1_semantics) {
      std::sort(raw_trace.begin(), raw_trace.end(),
                [](const Request& r1, const Request& r2) {
                  return memcmp(&r1.key, &r2.key, sizeof(r1.key)) < 0;
                });
    } else {
      std::sort(raw_trace.begin(), raw_trace.end());
    }
  }

  // Create the values and initialize them.
  size_t total_value_size = kNumUniqueValues * options.value_size;
  std::mt19937 rng(options.rng_seed);
  std::unique_ptr<char[]> values = impl::GetRandomBytes(total_value_size, rng);

  std::vector<Request> trace;
  trace.reserve(raw_trace.size());
  size_t value_index = 0;
  for (size_t i = 0; i < raw_trace.size(); ++i) {
    const auto& raw = raw_trace[i];
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

inline BulkLoadTrace BulkLoadTrace::LoadFromKeys(
    const std::vector<Request::Key>& keys, const Trace::Options& options) {
  std::vector<Request> raw_trace;
  raw_trace.reserve(keys.size());
  for (const auto& key : keys) {
    // All operations are inserts.
    raw_trace.emplace_back(Request::Operation::kInsert,
                           options.use_v1_semantics && options.swap_key_bytes
                               ? __builtin_bswap64(key)
                               : key,
                           0, nullptr, 0);
  }
  return BulkLoadTrace(Trace::ProcessRawTrace(std::move(raw_trace), options));
}

inline size_t BulkLoadTrace::DatasetSizeBytes() const {
  size_t total_size = 0;
  for (const auto& request : *this) {
    total_size += sizeof(request.key) + request.value_size;
  }
  return total_size;
}

}  // namespace ycsbr
