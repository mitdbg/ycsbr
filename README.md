# YCSBR: Your Customizable Synthetic Benchmark Runner
This repository contains a header-only library and utilities that help with
running synthetic key value store benchmark workloads. This workload runner
only supports 64-bit unsigned integer keys.

## Compilation and Usage
This project is configured with CMake. The key value workload runner is
intended to be used as a subproject within a CMake project. You can either
(i) include this repository as a submodule, or (ii) use CMake's
`FetchContent` or `ExternalProject` modules to checkout this repository
during configuration/compilation.

To use the key value workload runner library in your code, link to the
`ycsbr` CMake target.
```cmake
target_link_libraries(your_target ycsbr)
```

In your source code, you only need to include the `ycsbr/ycsbr.h` header.
```cpp
#include "ycsbr/ycsbr.h"
```
The public library methods and classes can be found under `include/ycsbr` and
are documented.

## Database Interface
YCSBR interacts with the database being benchmarked through a
`DatabaseInterface` class. You need to define one of these classes for each
database you plan to benchmark. See `ycsbr/db.h` for an example class.

## Running Benchmark Workloads
There are two ways to run benchmark workloads against a `DatabaseInterface`.
The single trace replay functions and the `Session` methods.

### Single Benchmark
To run a single trace, you can use the `ReplayTrace<DatabaseInterface>()`
functions. See `ycsbr/benchmark.h` for more documentation. These functions
only allow you to run a single trace and have limited customizability.

### Session-Based Benchmarking
For more fine-grained control over the benchmarking setup, you can use a
benchmark `Session`. See `ycsbr/session.h` for more documentation.

## Custom Workloads
YCSBR also supports custom key-value workloads. To define a custom workload,
implement classes with the same methods as shown in
`ycsbr/workload/workload.h`. To run the workload, you need to use
`Session`-based benchmarking (you will need to call
`Session::RunWorkload()`).

## YCSB Workload Extractor
This project contains a tool that extracts a YCSB workload and serializes it
as a file that can then be loaded by the workload runner library. To build
the tool, you need to set the `YR_BUILD_EXTRACTOR` option when configuring
the project.

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DYR_BUILD_EXTRACTOR=ON ..
make
```

After building, you will find the `ycsbextractor` executable. The extractor
tool works by processing the official YCSB benchmark driver's output when
using the `basic` "database". It accepts the benchmark driver's output on
standard in and writes to a file that you specify when launching the program.

### Extraction Examples

**Load 1 million records**
```bash
ycsb load basic -P workloads/workloada \
  -p recordcount=1000000 \
  | ./build/ycsbextractor ycsb-a-load-1M
```

**Workload A, 1 million record database size, 100k operations**
```bash
ycsb run basic -P workloads/workloada \
  -p recordcount=1000000 \
  -p operationcount=100000 \
  | ./build/ycsbextractor ycsb-a-run-1M-100k
```

## Running Tests and Microbenchmarks
To build the YCSBR tests and microbenchmarks, you need to set the
`YR_BUILD_TESTS` option when configuring this project. Note that you need to
also build the extractor if you want to build the tests.

```bash
cmake -DCMAKE_BUILD_TYPE=Release -DYR_BUILD_TESTS=ON -DYR_BUILD_EXTRACTOR=ON ..
```

Then, after compiling, run `./tests/test_runner` inside the build directory.
You can run the microbenchmarks by running `./tests/benchmark_runner`.

The benchmark runner adds an overhead of ~20 nanoseconds to each request.
This overhead will be part of the workload throughput measurement, but will
*not* be part of the latency measurements.
