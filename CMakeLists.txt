cmake_minimum_required(VERSION 3.17)
project(ycsb-runner LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

option(YR_BUILD_EXTRACTOR "Set to build the YCSB workload extractor." OFF)
option(YR_BUILD_TESTS "Set to build the YCSB Runner tests." OFF)

# The YCSB Runner library is a header-only library.
add_library(ycsbrunner INTERFACE)
target_include_directories(ycsbrunner INTERFACE include)
set(srcdir include/ycsbr)
target_sources(ycsbrunner
  INTERFACE
    ${srcdir}/impl/benchmark-inl.h
    ${srcdir}/impl/trace-inl.h
    ${srcdir}/impl/flag.h
    ${srcdir}/impl/tracking.h
    ${srcdir}/impl/worker.h
    ${srcdir}/benchmark.h
    ${srcdir}/db.h
    ${srcdir}/meter.h
    ${srcdir}/request.h
    ${srcdir}/trace.h
    ${srcdir}/ycsbr.h)

if(YR_BUILD_EXTRACTOR)
  add_executable(ycsbextractor extractors/ycsb.cc)
  target_link_libraries(ycsbextractor PRIVATE ycsbrunner)
endif()

if(YR_BUILD_TESTS AND NOT YR_BUILD_EXTRACTOR)
  message(FATAL_ERROR "YR_BUILD_EXTRACTOR must also be set to ON when building tests.")
endif()

if(YR_BUILD_TESTS)
  add_subdirectory(tests)
endif()
