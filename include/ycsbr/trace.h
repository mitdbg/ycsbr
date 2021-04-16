#pragma once

#include <memory>
#include <string>
#include <vector>

#include "request.h"

namespace ycsbr {

class Trace {
 public:
  struct Options {
    // The trace's deserialization semantics (related to key sort order) have
    // changed since v1. Set this to true to use the v1 semantics instead.
    bool use_v1_semantics = false;

    // DEPRECATED: This option is only meaningful if `use_v1_semantics` is set
    // to true. Otherwise it is ignored.
    //
    // The trace's keys are encoded as 64-bit integers. On little endian
    // machines, swapping the key's bytes ensures that they retain their integer
    // ordering when compared lexicographically.
    bool swap_key_bytes = true;

    // If true, the requests will be sorted in ascending order by key.
    // If `use_v1_semantics` is set to true, the sort will be lexicographic.
    bool sort_requests = false;

    // The size of the values for insert and update requests, in bytes.
    size_t value_size = 1024;
    int rng_seed = 42;
  };
  static Trace LoadFromFile(const std::string& file, const Options& options);

  using const_iterator = std::vector<Request>::const_iterator;
  const_iterator begin() const { return requests_.begin(); }
  const_iterator end() const { return requests_.end(); }
  size_t size() const { return requests_.size(); }

  const Request& at(size_t index) const { return requests_.at(index); }
  const Request& operator[](size_t index) const { return requests_[index]; }

  struct MinMaxKeys {
    MinMaxKeys() : MinMaxKeys(0, 0) {}
    MinMaxKeys(Request::Key min, Request::Key max) : min(min), max(max) {}
    const Request::Key min, max;
  };
  // Get the minimum and maximum key in this `Workload`. Keys are compared
  // lexicographically.
  MinMaxKeys GetKeyRange() const;

 protected:
  static Trace ProcessRawTrace(std::vector<Request> raw_trace,
                               const Options& options);
  Trace(std::vector<Request> requests, std::unique_ptr<char[]> values,
        bool use_v1_semantics)
      : requests_(std::move(requests)),
        values_(std::move(values)),
        use_v1_semantics_(use_v1_semantics) {}

 private:
  std::vector<Request> requests_;
  // All values stored contiguously.
  std::unique_ptr<char[]> values_;
  bool use_v1_semantics_;
};

class BulkLoadTrace : public Trace {
 public:
  static BulkLoadTrace LoadFromFile(const std::string& file,
                                    const Trace::Options& options);
  static BulkLoadTrace LoadFromKeys(const std::vector<Request::Key>& keys,
                                    const Trace::Options& options);
  size_t DatasetSizeBytes() const;

 private:
  BulkLoadTrace(Trace trace) : Trace(std::move(trace)) {}
};

}  // namespace ycsbr

#include "impl/trace-inl.h"
