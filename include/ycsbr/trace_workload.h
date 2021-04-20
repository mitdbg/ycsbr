#pragma once

#include <memory>
#include <vector>

#include "ycsbr/request.h"
#include "ycsbr/trace.h"

namespace ycsbr {

class TraceWorkload {
 public:
  TraceWorkload(const Trace* trace) : trace_(trace) {}

  class Producer;
  std::vector<Producer> GetProducers(size_t num_producers) const;

 private:
  const Trace* trace_;
};

class TraceWorkload::Producer {
 public:
  void Prepare() {}
  bool HasNext() const { return index_ < stop_before_; }
  Request Next() { return (*trace_)[index_++]; }

 private:
  friend class TraceWorkload;
  Producer(const Trace* trace, size_t start_index, size_t num_requests)
      : trace_(trace),
        index_(start_index),
        stop_before_(start_index + num_requests) {}
  const Trace* trace_;
  size_t index_;
  size_t stop_before_;
};

inline std::vector<TraceWorkload::Producer> TraceWorkload::GetProducers(
    size_t num_producers) const {
  std::vector<Producer> producers;
  producers.reserve(num_producers);

  // Split up the requests.
  const size_t min_requests_per_producer = trace_->size() / num_producers;
  size_t leftover_requests = trace_->size() % num_producers;
  size_t next_offset = 0;
  for (size_t producer_id = 0; producer_id < num_producers; ++producer_id) {
    size_t num_requests = min_requests_per_producer;
    if (leftover_requests > 0) {
      ++num_requests;
      --leftover_requests;
    }
    producers.push_back(Producer(trace_, next_offset, num_requests));
    next_offset += num_requests;
  }

  return producers;
}

}  // namespace ycsbr
