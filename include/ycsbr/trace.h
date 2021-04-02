#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ycsbr {

struct Request {
  enum class Operation : uint8_t {
    kInsert = 0,
    kRead = 1,
    kUpdate = 2,
    kScan = 3
  };
  using Key = uint64_t;

  struct Encoded {
    Encoded() : Encoded(Operation::kRead, 0) {}
    Encoded(Operation op, Key key) : op(op), key(key) {}

    const Operation op;
    const Key key;
    // To save space, the `scan_amount` is only encoded for requests with
    // Operation::kScan. The `scan_amount` is encoded directly following the
    // request in the file.
  } __attribute__((packed));

  Request() : Request(Operation::kRead, 0, 0, nullptr, 0) {}
  Request(Operation op, Key key, uint32_t scan_amount, char* value,
          size_t value_size)
      : op(op),
        key(key),
        scan_amount(scan_amount),
        value(value),
        value_size(value_size) {}

  bool operator<(const Request& other) const {
    return memcmp(&key, &other.key, sizeof(key)) < 0;
  }

  Operation op;
  Key key;

  // Number of keys to scan; non-zero only if `op` is `Operation::kScan`.
  uint32_t scan_amount;

  // Value to write; non-null only if `op` is `Operation::kInsert` or
  // `Operation::kUpdate`.
  char* value;

  // Size of the value to write in bytes; non-zero only if `op` is
  // `Operation::kInsert` or `Operation::kUpdate`.
  size_t value_size;
};

class Workload {
 public:
  struct Options {
    // The workload's keys are encoded as 64-bit integers. On little endian
    // machines, swapping the key's bytes ensures that they retain their integer
    // ordering when compared lexicographically.
    bool swap_key_bytes = true;

    // If true, the requests will be sorted in ascending order lexicographically
    // by key.
    bool sort_requests = false;

    // The size of the values for insert and update requests, in bytes.
    size_t value_size = 1024;
    int rng_seed = 42;
  };
  static Workload LoadFromFile(const std::string& file, const Options& options);

  using const_iterator = std::vector<Request>::const_iterator;
  const_iterator begin() const { return requests_.begin(); }
  const_iterator end() const { return requests_.end(); }
  size_t size() const { return requests_.size(); }

  const Request& at(size_t index) const { return requests_.at(index); }
  const Request& operator[](size_t index) const { return requests_[index]; }

  struct MinMaxKeys {
    MinMaxKeys() : MinMaxKeys(0, 0) {}
    MinMaxKeys(Request::Key min, Request::Key max) : min(min), max(max) {}
    const Request::Key min, max;
  };
  // Get the minimum and maximum key in this `Workload`. Keys are compared
  // lexicographically.
  MinMaxKeys GetKeyRange() const;

 protected:
  Workload(std::vector<Request> requests, std::unique_ptr<char[]> values)
      : requests_(std::move(requests)), values_(std::move(values)) {}

 private:
  std::vector<Request> requests_;
  // All values stored contiguously.
  std::unique_ptr<char[]> values_;
};

class BulkLoadWorkload : public Workload {
 public:
  static BulkLoadWorkload LoadFromFile(const std::string& file,
                                       const Workload::Options& options);
  size_t DatasetSizeBytes() const;

 private:
  BulkLoadWorkload(Workload workload) : Workload(std::move(workload)) {}
};

}  // namespace ycsbr

#include "impl/trace-inl.h"
