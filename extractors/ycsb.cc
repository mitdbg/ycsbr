#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "ycsbr/trace.h"

namespace fs = std::filesystem;

namespace {

using Op = ycsbr::Request::Operation;

const std::unordered_map<std::string, Op> kStringToOp = {
    {"INSERT", Op::kInsert},
    {"READ", Op::kRead},
    {"UPDATE", Op::kUpdate},
    {"SCAN", Op::kScan}};

void ExtractYCSBTrace(const std::string& output_file) {
  std::ofstream output(output_file, std::ios::out | std::ios::binary);
  if (!output) {
    throw std::runtime_error(
        "Failed to load workload from file. Error opening: " + output_file);
  }
  output.exceptions(std::ofstream::failbit | std::ofstream::badbit);

  std::string line;
  while (std::getline(std::cin, line)) {
    std::istringstream iss(line);
    iss.exceptions(std::istringstream::failbit | std::istringstream::badbit);

    std::string op_candidate;
    iss >> op_candidate;

    const auto& it = kStringToOp.find(op_candidate);
    if (it == kStringToOp.end()) {
      // Not a request output.
      continue;
    }
    const Op operation = it->second;

    std::string key_string, discard;
    iss >> discard;
    iss >> key_string;

    const ycsbr::Request::Key key =
        strtoull(key_string.c_str() + 4, nullptr, 10);
    const ycsbr::Request::Encoded encoded(operation, key);
    output.write(reinterpret_cast<const char*>(&encoded), sizeof(encoded));

    // Encode the scan amount if the request is a SCAN operation
    if (operation != Op::kScan) {
      continue;
    }

    std::string scan_amount_string;
    iss >> scan_amount_string;

    const uint32_t scan_amount =
        strtoul(scan_amount_string.c_str(), nullptr, 10);
    output.write(reinterpret_cast<const char*>(&scan_amount),
                 sizeof(scan_amount));
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output file>" << std::endl;
    return 1;
  }

  const std::string output_file(argv[1]);
  if (fs::exists(output_file)) {
    std::cerr
        << "ERROR: Output file already exists. Aborting to avoid overwriting."
        << std::endl;
    return 1;
  }

  ExtractYCSBTrace(output_file);

  return 0;
}
