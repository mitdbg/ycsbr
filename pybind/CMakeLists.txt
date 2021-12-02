include(FetchContent)
FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG        v2.6.2
)
FetchContent_MakeAvailable(pybind11)

pybind11_add_module(ycsbr_py
  db_interface.h
  delegating_db.h
  python_db.cc
  python_db.h
  ycsbr.cc)
target_link_libraries(ycsbr_py PRIVATE ycsbr-gen)
