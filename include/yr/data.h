#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace yr {

struct Record {
  using Key = uint64_t;
  using Value = uint64_t;
  Record(Key key, Value value) : key(key), value(value) {}
  Key key;
  Value value;
};

struct Request {
  enum class Op : uint32_t { kRead = 0, kUpdate = 1 };

  Request(Op op, Record::Key key) : op(op), key(key) {}
  Op op;
  Record::Key key;
};

class Workload {
 public:
  static Workload LoadFromFile(const std::string& file);

  std::vector<Request>::iterator begin() { return requests_.begin(); }
  std::vector<Request>::iterator end() { return requests_.end(); }
  std::vector<Request>::const_iterator begin() const {
    return requests_.begin();
  }
  std::vector<Request>::const_iterator end() const { return requests_.end(); }
  size_t size() const { return requests_.size(); }

 private:
  explicit Workload(std::vector<Request> requests)
      : requests_(std::move(requests)) {}
  std::vector<Request> requests_;
};

class RecordsToLoad {
 public:
  static RecordsToLoad LoadFromFile(const std::string& file);

  size_t TotalDataSizeBytes() const {
    return records_.size() * sizeof(Record::Key) * 2;
  }
  std::vector<Record>::iterator begin() { return records_.begin(); }
  std::vector<Record>::iterator end() { return records_.end(); }
  std::vector<Record>::const_iterator begin() const { return records_.begin(); }
  std::vector<Record>::const_iterator end() const { return records_.end(); }
  size_t size() const { return records_.size(); }

 private:
  explicit RecordsToLoad(std::vector<Record> records)
      : records_(std::move(records)) {}
  std::vector<Record> records_;
};

}  // namespace yr

#include "detail/data-inl.h"
