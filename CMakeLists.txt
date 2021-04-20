cmake_minimum_required(VERSION 3.17)
project(ycsbr LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

option(YR_BUILD_GENERATOR "Set to build the YCSBR workload generator." ON)
option(YR_BUILD_EXTRACTOR "Set to build the YCSBR workload extractors." OFF)
option(YR_BUILD_TESTS "Set to build the YCSBR tests." OFF)

# The YCSBR library is a header-only library.
add_library(ycsbr INTERFACE)
target_include_directories(ycsbr INTERFACE include)
set(srcdir include/ycsbr)
target_sources(ycsbr
  INTERFACE
    ${srcdir}/impl/affinity.h
    ${srcdir}/impl/benchmark_result-inl.h
    ${srcdir}/impl/benchmark-inl.h
    ${srcdir}/impl/executor.h
    ${srcdir}/impl/flag.h
    ${srcdir}/impl/session-inl.h
    ${srcdir}/impl/thread_pool-inl.h
    ${srcdir}/impl/thread_pool.h
    ${srcdir}/impl/trace-inl.h
    ${srcdir}/impl/tracking.h
    ${srcdir}/impl/util.h
    ${srcdir}/benchmark_result.h
    ${srcdir}/benchmark.h
    ${srcdir}/db_example.h
    ${srcdir}/meter.h
    ${srcdir}/request.h
    ${srcdir}/run_options.h
    ${srcdir}/session.h
    ${srcdir}/trace_workload.h
    ${srcdir}/trace.h
    ${srcdir}/workload_example.h
    ${srcdir}/ycsbr.h)

# Link against ycsbr-gen if you want to use the workload generator. Note that
# ycsbr-gen is not a header-only library.
if(YR_BUILD_GENERATOR)
  add_library(ycsbr-gen)
  target_sources(ycsbr-gen
    PUBLIC
      ${srcdir}/gen/chooser.h
      ${srcdir}/gen/config.h
      ${srcdir}/gen/keygen.h
      ${srcdir}/gen/keyrange.h
      ${srcdir}/gen/phase.h
      ${srcdir}/gen/types.h
      ${srcdir}/gen/valuegen.h
      ${srcdir}/gen/workload.h
      ${srcdir}/gen.h)
  target_link_libraries(ycsbr-gen PUBLIC ycsbr)
  add_subdirectory(generator)
endif()

if(YR_BUILD_EXTRACTOR)
  add_executable(ycsbextractor extractors/ycsb.cc)
  target_link_libraries(ycsbextractor PRIVATE ycsbr)
endif()

if(YR_BUILD_TESTS AND NOT YR_BUILD_EXTRACTOR)
  message(FATAL_ERROR "YR_BUILD_EXTRACTOR must also be set to ON when building tests.")
endif()

if(YR_BUILD_TESTS AND NOT YR_BUILD_GENERATOR)
  message(FATAL_ERROR "YR_BUILD_GENERATOR must also be set to ON when building tests.")
endif()

if(YR_BUILD_TESTS)
  add_subdirectory(tests)
endif()
