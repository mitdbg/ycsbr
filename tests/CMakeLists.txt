include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.10.0
)
FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark
    GIT_TAG        v1.5.2
  )
set(BENCHMARK_ENABLE_TESTING OFF)
set(BENCHMARK_ENABLE_INSTALL OFF)
FetchContent_MakeAvailable(googletest benchmark)

add_executable(test_runner
  benchmark_test.cc
  meter_test.cc
  session_test.cc
  workload_test.cc
  zipfian_test.cc)
target_link_libraries(test_runner PRIVATE ycsbr-gen gtest gtest_main)

add_executable(benchmark_runner overhead_benchmark.cc generator_benchmark.cc)
target_link_libraries(benchmark_runner
  PRIVATE
    ycsbr-gen
    benchmark::benchmark_main)
