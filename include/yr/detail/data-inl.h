// Implementation of declarations in yr/data.h. Do not include this header!
#include <fstream>

#include "yr/data.h"

namespace yr {

Workload Workload::LoadFromFile(const std::string& file) {
  std::ifstream input(file, std::ios::in | std::ios::binary);
  if (!input) {
    throw std::runtime_error(
        "Failed to load workload from file. Error opening: " + file);
  }

  std::vector<Request> workload;
  uint32_t op_mode;
  uint64_t key;
  for (;;) {
    input.read(reinterpret_cast<char*>(&op_mode), sizeof(op_mode));
    if (input.eof()) {
      break;
    }
    input.read(reinterpret_cast<char*>(&key), sizeof(key));
    if (input.eof()) {
      break;
    }

    workload.emplace_back(op_mode == static_cast<uint32_t>(Request::Op::kRead)
                              ? Request::Op::kRead
                              : Request::Op::kUpdate,
                          key);
  }

  return Workload(std::move(workload));
}

RecordsToLoad RecordsToLoad::LoadFromFile(const std::string& file) {
  std::ifstream input(file, std::ios::in | std::ios::binary);
  if (!input) {
    throw std::runtime_error(
        "Failed to load records from file. Error opening: " + file);
  }

  std::vector<Record> records;
  uint64_t key;
  uint64_t value = 0;
  for (uint64_t idx = 0;; idx++) {
    input.read(reinterpret_cast<char*>(&key), sizeof(key));
    if (input.eof()) {
      break;
    }
    records.emplace_back(key, value);
    value += 1;
  }

  return RecordsToLoad(std::move(records));
}

}  // namespace yr
