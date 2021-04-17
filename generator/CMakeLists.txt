# ycsbr-gen relies on yaml-cpp to parse workload definition files.
include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        yaml-cpp-0.6.3
)
set(YAML_CPP_BUILD_CONTRIB OFF)
set(YAML_CPP_BUILD_TOOLS OFF)
set(YAML_BUILD_SHARED_LIBS OFF)
set(YAML_CPP_BUILD_TESTS OFF)
set(YAML_CPP_INSTALL OFF)
FetchContent_MakeAvailable(yaml-cpp)

target_sources(ycsbr-gen
  PRIVATE
    config_impl.cc
    config_impl.h
    sampling-inl.h
    sampling.h
    uniform_chooser.h
    uniform_keygen.cc
    uniform_keygen.h
    workload.cc
    zipfian.h)
target_link_libraries(ycsbr-gen PRIVATE yaml-cpp)
