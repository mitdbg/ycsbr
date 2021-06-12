#include <vector>

#include "request.h"

namespace ycsbr {

// This class wraps an existing workload and buffers all of the workload's
// requests in memory. It does this by "running" the entire workload during the
// producers' prepare phase and recording the requests that are generated.
//
// The purpose of this warpper is to help avoid the runtime overhead of
// generating the workload. The trade-off is that more memory will be used (to
// store all the requests).
template <class Workload>
class BufferedWorkload {
 public:
  BufferedWorkload(const Workload& workload);

  // Get a reference to the wrapped workload.
  const Workload& workload() const;

  class Producer;
  std::vector<Producer> GetProducers(size_t num_producers) const;

 private:
  const Workload& workload_;
};

template <class Workload>
class BufferedWorkload<Workload>::Producer {
 public:
  Producer(typename Workload::Producer producer);
  void Prepare();
  bool HasNext() const;
  const Request& Next();

 private:
  typename Workload::Producer producer_;

  std::vector<Request> requests_;
  size_t next_request_;
};

}  // namespace ycsbr

#include "impl/buffered_workload-inl.h"
