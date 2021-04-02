#pragma once

#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "data.h"

namespace ycsbr {

// This is an example `DatabaseInterface` - you need to implement all of these
// methods to be able to use `ReplayTrace()`.
//
// **Do not subclass this class.** Just implement the same methods with the same
// signatures in a concrete class. We use templates in ReplayTrace() to
// avoid vtable overheads.
class ExampleDatabaseInterface final {
 public:
  // Called once by each worker thread **before** the database is initialized.
  // Note that this method will be called concurrently by each worker thread.
  virtual void InitializeWorker(const std::thread::id& worker_id) = 0;

  // Called once by each worker thread after it is done running. This method is
  // called concurrently by each worker thread and may run concurrently with
  // `DeleteDatabase()`.
  virtual void ShutdownWorker(const std::thread::id& worker_id) = 0;

  // Called once before the benchmark.
  // Put any needed initialization code in here.
  virtual void InitializeDatabase() = 0;

  // Called once if `InitializeDatabase()` has been called.
  // Put any needed clean up code in here.
  virtual void DeleteDatabase() = 0;

  // Load the records into the database.
  virtual void BulkLoad(const BulkLoadWorkload& load) = 0;

  // Update the value at the specified key. Return true if the update succeeded.
  virtual bool Update(Request::Key key, const char* value,
                      size_t value_size) = 0;

  // Insert the specified key value pair. Return true if the insert succeeded.
  virtual bool Insert(Request::Key key, const char* value,
                      size_t value_size) = 0;

  // Read the value at the specified key. Return true if the read succeeded.
  virtual bool Read(Request::Key key, std::string* value_out) = 0;

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  virtual bool Scan(
      Request::Key key, size_t amount,
      std::vector<std::pair<Request::Key, std::string>>* scan_out) = 0;
};

}  // namespace ycsbr
