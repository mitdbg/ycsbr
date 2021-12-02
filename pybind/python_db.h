#pragma once

#include <string>

#include "db_interface.h"
#include "ycsbr/ycsbr.h"

namespace ycsbr {
namespace py {

class PythonDatabase : public DatabaseInterface {
 public:
  using DatabaseInterface::DatabaseInterface;

  // Called once before the benchmark.
  // Put any needed initialization code in here.
  void InitializeDatabase() override;

  // Called once if `InitializeDatabase()` has been called.
  // Put any needed clean up code in here.
  void ShutdownDatabase() override;

  // Load the records into the database.
  void BulkLoad(const BulkLoadTrace& load) override;

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(Request::Key key, const std::string& value) override;

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(Request::Key key, const std::string& value) override;

  // Read the value at the specified key. Return true if the read succeeded.
  std::optional<std::string> Read(Request::Key key) override;

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  std::vector<std::pair<Request::Key, std::string>> Scan(
      Request::Key key, size_t amount) override;
};

}  // namespace py
}  // namespace ycsbr
