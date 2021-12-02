#include "python_db.h"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace ycsbr {
namespace py {

void PythonDatabase::InitializeDatabase() {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(void, DatabaseInterface, "initialize_database",
                              InitializeDatabase);
}

void PythonDatabase::ShutdownDatabase() {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(void, DatabaseInterface, "shutdown_database",
                              ShutdownDatabase);
}

void PythonDatabase::BulkLoad(const BulkLoadTrace& load) {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(void, DatabaseInterface, "bulk_load", BulkLoad,
                              load);
}

bool PythonDatabase::Update(Request::Key key, const std::string& value) {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(bool, DatabaseInterface, "update", Update, key,
                              pybind11::bytes(value));
}

bool PythonDatabase::Insert(Request::Key key, const std::string& value) {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(bool, DatabaseInterface, "insert", Insert, key,
                              pybind11::bytes(value));
}

std::optional<std::string> PythonDatabase::Read(Request::Key key) {
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(std::optional<std::string>, DatabaseInterface,
                              "read", Read, key);
}

std::vector<std::pair<Request::Key, std::string>> PythonDatabase::Scan(
    Request::Key key, size_t amount) {
  using RecordBatch = std::vector<std::pair<Request::Key, std::string>>;
  pybind11::gil_scoped_acquire acquire;
  PYBIND11_OVERRIDE_PURE_NAME(RecordBatch, DatabaseInterface, "scan", Scan, key,
                              amount);
}

}  // namespace py
}  // namespace ycsbr
