#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>

#include "ycsbr/gen/workload.h"

using namespace ycsbr;
using namespace ycsbr::gen;

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

  std::shared_ptr<PhasedWorkload> workload =
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

  return 0;
}
