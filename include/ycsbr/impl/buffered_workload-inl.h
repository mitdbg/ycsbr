// Implementation of declarations in buffered_workload.h. Do not include this
// header!

namespace ycsbr {

template <class Workload>
inline BufferedWorkload<Workload>::BufferedWorkload(const Workload& workload)
    : workload_(workload) {}

template <class Workload>
inline const Workload& BufferedWorkload<Workload>::workload() const {
  return workload_;
}

template <class Workload>
inline std::vector<typename BufferedWorkload<Workload>::Producer>
BufferedWorkload<Workload>::GetProducers(const size_t num_producers) const {
  // Get the actual producers.
  std::vector<typename Workload::Producer> producers =
      workload_.GetProducers(num_producers);

  // Construct the wrapper producers.
  std::vector<typename BufferedWorkload<Workload>::Producer> wrapper_producers;
  wrapper_producers.reserve(producers.size());
  for (auto& producer : producers) {
    wrapper_producers.emplace_back(std::move(producer));
  }

  return wrapper_producers;
}

template <class Workload>
inline BufferedWorkload<Workload>::Producer::Producer(
    typename Workload::Producer producer)
    : producer_(std::move(producer)), next_request_(0) {}

template <class Workload>
inline void BufferedWorkload<Workload>::Producer::Prepare() {
  producer_.Prepare();

  // Record all generated requests.
  while (producer_.HasNext()) {
    requests_.push_back(producer_.Next());
  }

  // Always reset the next request counter, even though producers are not
  // supposed to be prepared and used more than once.
  next_request_ = 0;
}

template <class Workload>
inline bool BufferedWorkload<Workload>::Producer::HasNext() const {
  return next_request_ < requests_.size();
}

template <class Workload>
inline const Request& BufferedWorkload<Workload>::Producer::Next() {
  return requests_[next_request_++];
}

}  // namespace ycsbr
