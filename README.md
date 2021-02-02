# YCSB Workload Runner
This repository contains a header-only library and utilities that help with
running key value store benchmark workloads.

## Compilation and Usage
This project is configured with CMake. The key value workload runner is
intended to be used as a subproject within a CMake project. You can either
(i) include this repository as a submodule, or (ii) use CMake's
`FetchContent` or `ExternalProject` modules to checkout this repository
during configuration/compilation.

To use the key value workload runner library in your code, link to the
`ycsbrunner` CMake target.
```cmake
target_link_libraries(your_target ycsbrunner)
```

In your source code, you only need to include the `ycsbr/ycsbr.h` header.
```cpp
#include "ycsbr/ycsbr.h"
```
The public library methods and classes can be found under `include/ycsbr` and
are documented.

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
