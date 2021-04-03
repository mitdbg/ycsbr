#pragma once

#include <cstdint>
#include <cstring>

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

  bool operator<(const Request& other) const { return key < other.key; }

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

}  // namespace ycsbr
