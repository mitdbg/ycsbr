#pragma once

// --- YCSBR: Your Customizable Synthetic Benchmark Runner ---
// Include this header to get access to the YCSBR library. This header only
// includes the benchmark runner (and associated utilities). In your CMake
// project, "link" to the `ycsbr` interface library target.
//
// If you want to also use the workload generator (i.e., anything in the
// `ycsbr::gen` namespace), use `#include "ycsbr/gen.h"` instead. The `gen.h`
// header also includes everything this header includes. You will need to link
// to the `ycsbr-gen` CMake library target. Note that the `ycsbr-gen` library is
// not a header-only library.

#include "benchmark_result.h"
#include "benchmark.h"
#include "db_example.h"
#include "meter.h"
#include "request.h"
#include "run_options.h"
#include "session.h"
#include "trace_workload.h"
#include "trace.h"
#include "workload_example.h"
