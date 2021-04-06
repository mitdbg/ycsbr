#pragma once

#include <vector>

#include "../request.h"

namespace ycsbr {

// An example of the methods (and inner classes) that need to exist in a
// `CustomWorkload`.
class ExampleCustomWorkload final {
 public:
  virtual ~ExampleCustomWorkload() = default;
  class Producer;
  virtual std::vector<Producer> GetProducers(size_t num_producers) = 0;
};

class ExampleCustomWorkload::Producer final {
 public:
  virtual ~Producer() = default;
  virtual bool HasNext() = 0;
  virtual Request Next() = 0;
};

}  // namespace ycsbr
