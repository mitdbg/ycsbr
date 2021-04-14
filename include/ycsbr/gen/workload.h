#pragma once

#include <vector>
#include <memory>

#include "ycsbr/request.h"
#include "ycsbr/workload/trace.h"

namespace ycsbr {
namespace gen {

class PhasedWorkload {
 public:
  static std::shared_ptr<PhasedWorkload> LoadFrom(const std::filesystem::path& config_file);

  BulkLoadTrace GetLoadTrace();

  class Producer;
  std::vector<Producer> GetProducers() const;

 private:
  std::vector<Request::Key> load_keys_;
};

class PhasedWorkload::Producer {
 private:
  std::shared_ptr<PhasedWorkload> workload_;
  std::vector<Request::Key> insert_keys_;
  size_t next_insert_key_index_;
};

}  // namespace gen
}  // namespace ycsbr
