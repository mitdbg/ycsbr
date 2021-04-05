#pragma once

#include <filesystem>
#include <fstream>

// These are hardcoded raw extracted YCSB workloads.
// We use them to test deserialization.
#include "wa_load.h"
#include "wa_run.h"
#include "we_run.h"

// All serialized traces have 25 operations.
inline constexpr size_t kTraceSize = 25;

enum class WorkloadType { kLoadA, kRunA, kRunE };

template <WorkloadType Type>
std::filesystem::path CreateWorkloadFile() {
  unsigned char* data = nullptr;
  size_t data_size = 0;
  if constexpr (Type == WorkloadType::kLoadA) {
    data = wa_load_ycsb;
    data_size = wa_load_ycsb_len;
  } else if constexpr (Type == WorkloadType::kRunA) {
    data = wa_run_ycsb;
    data_size = wa_run_ycsb_len;
  } else if constexpr (Type == WorkloadType::kRunE) {
    data = we_run_ycsb;
    data_size = we_run_ycsb_len;
  }

  auto path = std::filesystem::temp_directory_path();
  std::filesystem::path trace_file = path / "trace.ycsb";

  std::ofstream output(trace_file.string(), std::ios::out | std::ios::binary);
  for (size_t i = 0; i < data_size; ++i, ++data) {
    output.write(reinterpret_cast<const char*>(data), 1);
  }

  return trace_file;
}
