#pragma once

#include <vector>

#include "ycsbr/request.h"

namespace ycsbr {

// An example of the methods (and inner classes) that need to exist in a
// `CustomWorkload`.
class ExampleCustomWorkload final {
 public:
  virtual ~ExampleCustomWorkload() = default;

  // A `Producer` instance is passed to each thread running the benchmark.
  // `Producer`s are meant to generate requests that are specific to a thread.
  class Producer;
  virtual std::vector<Producer> GetProducers(size_t num_producers) = 0;
};

class ExampleCustomWorkload::Producer final {
 public:
  virtual ~Producer() = default;

  // Called once after this `Producer` is created.
  // Useful for running setup code.
  virtual void Prepare() = 0;

  // Return true if there are still more requests.
  virtual bool HasNext() = 0;

  // This method may also return a `const Request&`.
  virtual Request Next() = 0;
};

}  // namespace ycsbr
