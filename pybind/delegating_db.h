#pragma once

#include <memory>
#include <string>

#include "db_interface.h"

namespace ycsbr {
namespace py {

// A wrapper class for internal use with `ycsbr::Session`. See `db_interface.h`
// for documentation.
class DelegatingDB {
 public:
  DelegatingDB() : db_(nullptr) {}

  // Must be called once before any other methods are called.
  void SetDatabase(std::shared_ptr<DatabaseInterface> db) {
    db_ = std::move(db);
  }

  void InitializeWorker(const std::thread::id& worker_id) {}

  void ShutdownWorker(const std::thread::id& worker_id) {}

  void InitializeDatabase() { db_->InitializeDatabase(); }

  void ShutdownDatabase() { db_->ShutdownDatabase(); }

  void BulkLoad(const BulkLoadTrace& load) { db_->BulkLoad(load); }

  bool Update(Request::Key key, const char* value, size_t value_size) {
    // N.B. We need to use `std::string` so that pybind can expose raw bytes to
    // Python. This means we need to make an extra copy of `value` here.
    return db_->Update(key, std::string(value, value_size));
  }

  bool Insert(Request::Key key, const char* value, size_t value_size) {
    return db_->Insert(key, std::string(value, value_size));
  }

  bool Read(Request::Key key, std::string* value_out) {
    auto result = db_->Read(key);
    if (!result.has_value()) {
      return false;
    }
    value_out->assign(*result);
    return true;
  }

  bool Scan(Request::Key key, size_t amount,
            std::vector<std::pair<Request::Key, std::string>>* scan_out) {
    *scan_out = db_->Scan(key, amount);
    return true;
  }

 private:
  std::shared_ptr<DatabaseInterface> db_;
};

}  // namespace py
}  // namespace ycsbr
