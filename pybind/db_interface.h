#pragma once

#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ycsbr/ycsbr.h"

namespace ycsbr {
namespace py {

class DatabaseInterface {
 public:
  // The class must be default constructible.
  DatabaseInterface() = default;

  // The class must have a public destructor.
  virtual ~DatabaseInterface() = default;

  // Called once before the benchmark.
  // Put any needed initialization code in here.
  virtual void InitializeDatabase() = 0;

  // Called once if `InitializeDatabase()` has been called.
  // Put any needed clean up code in here.
  virtual void ShutdownDatabase() = 0;

  // Load the records into the database.
  virtual void BulkLoad(const BulkLoadTrace& load) = 0;

  // Update the value at the specified key. Return true if the update succeeded.
  virtual bool Update(Request::Key key, const std::string& value) = 0;

  // Insert the specified key value pair. Return true if the insert succeeded.
  virtual bool Insert(Request::Key key, const std::string& value) = 0;

  // Read and return the value at the specified key. Return none if the read
  // failed.
  virtual std::optional<std::string> Read(Request::Key key) = 0;

  // Scan the key range starting from `key` for `amount` records.
  virtual std::vector<std::pair<Request::Key, std::string>> Scan(
      Request::Key key, size_t amount) = 0;
};

}  // namespace py
}  // namespace ycsbr
