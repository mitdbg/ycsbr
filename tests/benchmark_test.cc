#include "ycsbr/benchmark.h"

#include "gtest/gtest.h"
#include "ycsbr/data.h"
#include "workloads/fixtures.h"

namespace {

using namespace ycsbr;

class TestDatabaseInterface {
 public:
  void InitializeDatabase() { ++initialize_calls; }
  void DeleteDatabase() { ++delete_calls; }

  void BulkLoad(const BulkLoadWorkload& load) {
    ++bulk_load_calls;
  }

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

  size_t initialize_calls = 0;
  size_t delete_calls = 0;
  size_t bulk_load_calls = 0;
  size_t update_calls = 0;
  size_t insert_calls = 0;
  size_t read_calls = 0;
  size_t scan_calls = 0;
};

TEST_F(WorkloadLoadA, BenchBulkLoad) {
  const Workload::Options options;
  const BulkLoadWorkload load =
      BulkLoadWorkload::LoadFromFile(workload_file, options);
  for (const auto& req : load) {
    ASSERT_NE(req.value, nullptr);
    ASSERT_EQ(req.value_size, options.value_size);
  }

  TestDatabaseInterface db;
  auto res = RunTimedWorkload(db, load);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 1);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_EQ(db.scan_calls, 0);
}

TEST_F(WorkloadRunA, BenchRunA) {
  const Workload::Options options;
  const Workload load =
      Workload::LoadFromFile(workload_file, options);

  TestDatabaseInterface db;
  auto res = RunTimedWorkload(db, load);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_EQ(db.insert_calls, 0);
  ASSERT_TRUE(db.update_calls > 0);
  ASSERT_TRUE(db.read_calls > 0);
  ASSERT_EQ(db.scan_calls, 0);
}

TEST_F(WorkloadRunE, BenchRunE) {
  // Workload E is a range scan workload with some inserts.
  const Workload::Options options;
  const Workload load =
      Workload::LoadFromFile(workload_file, options);

  TestDatabaseInterface db;
  auto res = RunTimedWorkload(db, load);
  ASSERT_EQ(db.initialize_calls, 1);
  ASSERT_EQ(db.delete_calls, 1);
  ASSERT_EQ(db.bulk_load_calls, 0);
  ASSERT_TRUE(db.insert_calls > 0);
  ASSERT_EQ(db.update_calls, 0);
  ASSERT_EQ(db.read_calls, 0);
  ASSERT_TRUE(db.scan_calls > 0);
}

}  // namespace
