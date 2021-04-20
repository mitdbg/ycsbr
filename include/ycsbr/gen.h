#pragma once

// --- YCSBR: Your Customizable Synthetic Benchmark Runner ---
// Include this header to get access to the YCSBR library and workload
// generator. In your CMake project, link to the `ycsbr-gen` library target.
//
// If you do not need to use the workload generator (i.e., you do not use
// anything in the `ycsbr::gen` namespace), use `#include "ycsbr/ycsbr.h"`
// instead. The `ycsbr.h` header includes just the benchmark runner, and is a
// header-only library. You will need to "link" to the `ycsbr` CMake interface
// library target.

#include "ycsbr.h"
#include "gen/chooser.h"
#include "gen/config.h"
#include "gen/keygen.h"
#include "gen/keyrange.h"
#include "gen/phase.h"
#include "gen/types.h"
#include "gen/valuegen.h"
#include "gen/workload.h"
