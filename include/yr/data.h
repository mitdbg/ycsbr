#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace yr {

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

  const Operation op;
  const Key key;

  // Number of keys to scan; non-zero only if `op` is `Operation::kScan`.
  const uint32_t scan_amount;

  // Value to write; non-null only if `op` is `Operation::kInsert` or
  // `Operation::kUpdate`.
  const char* value;

  // Size of the value to write in bytes; non-zero only if `op` is
  // `Operation::kInsert` or `Operation::kUpdate`.
  const size_t value_size;
};

class Workload {
 public:
  struct Options {
    // The size of the values for insert and update requests, in bytes.
    size_t value_size = 1024;
    int rng_seed = 42;
  };
  inline static Workload LoadFromFile(const std::string& file,
                                      const Options& options);

  using iterator = std::vector<Request>::iterator;
  using const_iterator = std::vector<Request>::const_iterator;

  iterator begin() { return requests_.begin(); }
  iterator end() { return requests_.end(); }
  const_iterator begin() const { return requests_.begin(); }
  const_iterator end() const { return requests_.end(); }
  size_t size() const { return requests_.size(); }

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
  inline static BulkLoadWorkload LoadFromFile(const std::string& file,
                                              const Workload::Options& options);

 private:
  BulkLoadWorkload(Workload workload) : Workload(std::move(workload)) {}
};

}  // namespace yr

#include "impl/data-inl.h"
