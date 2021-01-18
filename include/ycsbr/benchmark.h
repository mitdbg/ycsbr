#pragma once

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "ycsbr/data.h"
#include "ycsbr/meter.h"

namespace ycsbr {

class BenchmarkResult;

// Runs the specified workload.
//
// NOTE: This assumes that the database already has an appropriate dataset
// loaded. If the database does not, use the overload with `BulkLoadWorkload`
// instead.
template <class DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const Workload& workload);

// Loads the specified records into the database and then runs the specified
// (timed) workload.
//
// NOTE: Only running the workload is timed. Loading the records is performed by
// calling `BulkLoad()` on the specified `DatabaseInterface`.
template <class DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const BulkLoadWorkload& load,
                                 const Workload& workload);

// Measures the time it takes to load the specified records using bulk load.
template <class DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const BulkLoadWorkload& load);

class BenchmarkResult {
 public:
  BenchmarkResult(std::chrono::nanoseconds total_run_time);
  BenchmarkResult(std::chrono::nanoseconds total_run_time, FrozenMeter reads,
                  FrozenMeter writes, FrozenMeter scans);

  template <typename Units>
  Units RunTime() const;
  double ThroughputMopsPerSecond() const;

  size_t NumReads() const;
  size_t NumWrites() const;

 private:
  const std::chrono::nanoseconds run_time_;
  const FrozenMeter reads_, writes_, scans_;
};

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res);

// This is an example `DatabaseInterface` - you need to implement all of these
// methods to be able to use `RunTimedWorkload()` above.
//
// **Do not subclass this class.** Just implement the same methods with the same
// signatures in a concrete class. We use templates in RunTimedWorkload() to
// avoid vtable overheads.
class ExampleDatabaseInterface final {
 public:
  // Called once before the benchmark.
  // Put any needed initialization code in here.
  virtual void InitializeDatabase() = 0;

  // Called exactly once if `InitializeDatabase()` has been called.
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

#include "impl/benchmark-inl.h"
