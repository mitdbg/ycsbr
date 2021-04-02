#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "gtest/gtest.h"

// These are hardcoded raw extracted YCSB workloads.
// We use them to test deserialization.
#include "wa_load.h"
#include "wa_run.h"
#include "we_run.h"

enum class WorkloadType { kLoadA, kRunA, kRunE };

template <WorkloadType Type>
class TraceFile : public testing::Test {
 protected:
  void SetUp() override {
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
    trace_file = path / "trace.ycsb";

    std::ofstream output(trace_file.string(), std::ios::out | std::ios::binary);
    for (size_t i = 0; i < data_size; ++i, ++data) {
      output.write(reinterpret_cast<const char*>(data), 1);
    }
  }

  void TearDown() override { std::filesystem::remove(trace_file); }

  // All serialized traces have 25 operations.
  const size_t trace_size = 25;
  std::filesystem::path trace_file;
};

using TraceLoadA = TraceFile<WorkloadType::kLoadA>;
using TraceReplayA = TraceFile<WorkloadType::kRunA>;
using TraceReplayE = TraceFile<WorkloadType::kRunE>;
