#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>

#include "ycsbr/gen/workload.h"

using namespace ycsbr;
using namespace ycsbr::gen;

namespace {

void PrintRequest(const Request& req) {
  switch (req.op) {
    case Request::Operation::kInsert:
      std::cerr << "[INSERT]    Key: 0x" << std::hex << req.key << std::dec
                << "  Value Size: " << req.value_size << std::endl;
      break;
    case Request::Operation::kRead:
      std::cerr << "[READ]      Key: 0x" << std::hex << req.key << std::dec
                << std::endl;
      break;
    case Request::Operation::kReadModifyWrite:
      std::cerr << "[R-M-W]     Key: 0x" << std::hex << req.key << std::dec
                << "  Value Size: " << req.value_size << std::endl;
      break;
    case Request::Operation::kNegativeRead:
      std::cerr << "[NEG-READ]  Key: 0x" << std::hex << req.key << std::dec
                << std::endl;
      break;
    case Request::Operation::kScan:
      std::cerr << "[SCAN]      Key: 0x" << std::hex << req.key << std::dec
                << "  Length: " << req.scan_amount << std::endl;
      break;
    case Request::Operation::kUpdate:
      std::cerr << "[UPDATE]    Key: 0x" << std::hex << req.key << std::dec
                << "  Value Size: " << req.value_size << std::endl;
      break;
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "ERROR: Please provide a workload configuration file."
              << std::endl;
    return 1;
  }
  const std::filesystem::path config_file(argv[1]);

  uint32_t seed = 42;
  if (argc >= 3) {
    seed = std::atoi(argv[2]);
  }

  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFrom(config_file, seed);
  BulkLoadTrace load = workload->GetLoadTrace();

  std::cerr << "Load" << std::endl;
  std::cerr << "====" << std::endl;
  for (const auto& req : load) {
    std::cerr << "Key: 0x" << std::hex << req.key << std::dec << std::endl;
  }

  std::vector<PhasedWorkload::Producer> producers = workload->GetProducers(1);
  for (auto& prod : producers) {
    prod.Prepare();
  }

  std::cerr << std::endl;
  std::cerr << "Workload" << std::endl;
  std::cerr << "========" << std::endl;

  PhasedWorkload::Producer& prod(producers[0]);
  while (prod.HasNext()) {
    PrintRequest(prod.Next());
  }

  return 0;
}
