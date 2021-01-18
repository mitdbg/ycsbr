include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.10.0
)
FetchContent_MakeAvailable(googletest)

add_executable(test_runner
  benchmark_test.cc
  meter_test.cc
  workload_test.cc)
target_link_libraries(test_runner PRIVATE ycsbrunner gtest gtest_main)
