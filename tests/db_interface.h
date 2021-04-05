#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "ycsbr/request.h"
#include "ycsbr/trace.h"

namespace ycsbr {

class TestDatabaseInterface {
 public:
  void InitializeWorker(const std::thread::id& worker_id) {
    ++initialize_worker_calls;
  }
  void ShutdownWorker(const std::thread::id& worker_id) {
    ++shutdown_worker_calls;
  }
  void InitializeDatabase() { ++initialize_calls; }
  void DeleteDatabase() { ++delete_calls; }

  void BulkLoad(const BulkLoadTrace& load) { ++bulk_load_calls; }

  bool Update(Request::Key key, const char* value, size_t value_size) {
    ++update_calls;
    return true;
  }

  bool Insert(Request::Key key, const char* value, size_t value_size) {
    ++insert_calls;
    return true;
  }

  bool Read(Request::Key key, std::string* value_out) {
    ++read_calls;
    return true;
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(Request::Key key, size_t amount,
            std::vector<std::pair<Request::Key, std::string>>* scan_out) {
    ++scan_calls;
    for (size_t i = 0; i < amount; ++i) {
      scan_out->emplace_back(static_cast<Request::Key>(key), "");
    }
    return true;
  }

  std::atomic<size_t> initialize_calls = 0;
  std::atomic<size_t> delete_calls = 0;
  std::atomic<size_t> bulk_load_calls = 0;
  std::atomic<size_t> update_calls = 0;
  std::atomic<size_t> insert_calls = 0;
  std::atomic<size_t> read_calls = 0;
  std::atomic<size_t> scan_calls = 0;
  std::atomic<size_t> initialize_worker_calls = 0;
  std::atomic<size_t> shutdown_worker_calls = 0;
};

}  // namespace ycsbr
