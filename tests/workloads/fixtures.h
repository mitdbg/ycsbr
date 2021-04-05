#pragma once

#include <filesystem>

#include "create_workload.h"
#include "gtest/gtest.h"

template <WorkloadType Type>
class TraceFile : public testing::Test {
 protected:
  void SetUp() override {
    trace_file = CreateWorkloadFile<Type>();
  }

  void TearDown() override { std::filesystem::remove(trace_file); }

  const size_t trace_size = kTraceSize;
  std::filesystem::path trace_file;
};

using TraceLoadA = TraceFile<WorkloadType::kLoadA>;
using TraceReplayA = TraceFile<WorkloadType::kRunA>;
using TraceReplayE = TraceFile<WorkloadType::kRunE>;
