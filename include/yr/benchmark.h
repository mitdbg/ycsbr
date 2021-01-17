#pragma once

#include <chrono>

#include "yr/data.h"

namespace yr {

class BenchmarkResult;

// Runs the specified workload.
//
// NOTE: This assumes that the database already has an appropriate dataset
// loaded. If the database does not, use the overload with RecordsToLoad
// instead.
template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const Workload& workload);

// Loads the specified records into the database and then runs the specified
// (timed) workload.
//
// NOTE: Only running the workload is timed. Loading the records is performed by
// calling BulkLoad() on the specified DatabaseInterface.
template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const RecordsToLoad& records,
                                 const Workload& workload);

// Measures the time it takes to load the specified records using bulk load.
template <typename DatabaseInterface>
BenchmarkResult RunTimedWorkload(DatabaseInterface& db,
                                 const RecordsToLoad& records);

class BenchmarkResult {
 private:
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

 public:
  BenchmarkResult();
  BenchmarkResult(TimePoint start, TimePoint end, uint64_t reads,
                  uint64_t writes);

  template <typename Units>
  Units RunTime() const;
  double ThroughputMopsPerSecond() const;

  uint64_t NumReads() const;
  uint64_t NumWrites() const;

 private:
  std::chrono::nanoseconds run_time_;
  uint64_t reads_;
  uint64_t writes_;
};

std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res);

// This is an example DatabaseInterface - you need to implement all of these
// methods to be able to use RunTimedWorkload() above.
//
// **Do not subclass this class.** Just implement the same methods with the same
// signatures in a concrete class. We use templates in RunTimedWorkload() to
// avoid vtable overheads.
class ExampleDatabaseInterface final {
 public:
  // Called once before the benchmark.
  // Put any needed initialization code in here.
  virtual void InitializeDatabase() = 0;

  // Called exactly once if InitializeDatabase() has been called.
  // Put any needed clean up code in here.
  virtual void DeleteDatabase() = 0;

  // Load the records into the database.
  virtual void BulkLoad(const RecordsToLoad& records) = 0;

  // Update the value at the specified key. Return true if the update succeeded.
  virtual bool Update(Record::Key key, Record::Value value) = 0;

  // Read the value at the specified key. Return true if the read succeeded.
  virtual bool Read(Record::Key key, Record::Value* out_value) = 0;
};

}  // namespace yr

#include "detail/benchmark-inl.h"
