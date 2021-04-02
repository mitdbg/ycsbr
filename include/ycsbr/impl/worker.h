#pragma once

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../trace.h"
#include "flag.h"
#include "tracking.h"

namespace ycsbr {
namespace impl {

// Runs a contiguous slice of a `Trace` (i.e., a contiguous slice of
// requests) in a separate thread.
template <class DatabaseInterface>
class Worker {
 public:
  Worker(DatabaseInterface* db, const Trace* trace, size_t offset,
         size_t num_requests, const Flag* can_start,
         std::optional<unsigned> pin_to_core, size_t latency_sample_period,
         bool expect_request_success, bool expect_scan_amount_found);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  bool IsReady() const;
  void Wait();
  MetricsTracker&& GetResults() &&;

 private:
  void WorkerMain();

  std::thread thread_;
  std::atomic<bool> ready_;
  Flag done_;
  DatabaseInterface* db_;
  const Trace* trace_;
  size_t offset_, num_requests_;
  const Flag* can_start_;
  std::optional<unsigned> pin_to_core_;
  MetricsTracker tracker_;

  size_t latency_sample_period_;
  size_t sampling_counter_;

  bool expect_request_success_;
  bool expect_scan_amount_found_;
};

// Implementation details follow.

template <class DatabaseInterface>
inline Worker<DatabaseInterface>::Worker(
    DatabaseInterface* db, const Trace* trace, size_t offset,
    size_t num_requests, const Flag* can_start,
    std::optional<unsigned> pin_to_core, size_t latency_sample_period,
    bool expect_request_success, bool expect_scan_amount_found)
    : ready_(false),
      done_(),
      db_(db),
      trace_(trace),
      offset_(offset),
      num_requests_(num_requests),
      can_start_(can_start),
      pin_to_core_(pin_to_core),
      latency_sample_period_(latency_sample_period),
      sampling_counter_(0),
      expect_request_success_(expect_request_success),
      expect_scan_amount_found_(expect_scan_amount_found) {
  thread_ = std::thread(&Worker<DatabaseInterface>::WorkerMain, this);
}

template <class DatabaseInterface>
inline Worker<DatabaseInterface>::~Worker() {
  Wait();
  if (thread_.joinable()) {
    thread_.join();
  }
}

template <class DatabaseInterface>
inline bool Worker<DatabaseInterface>::IsReady() const {
  return ready_.load(std::memory_order_acquire);
}

template <class DatabaseInterface>
inline void Worker<DatabaseInterface>::Wait() {
  done_.Wait();
}

template <class DatabaseInterface>
inline MetricsTracker&& Worker<DatabaseInterface>::GetResults() && {
  Wait();
  return std::move(tracker_);
}

template <typename Callable>
inline std::optional<std::chrono::nanoseconds> MeasurementHelper(
    Callable&& callable, bool measure_latency) {
  if (!measure_latency) {
    callable();
    return std::optional<std::chrono::nanoseconds>();
  }

  const auto start = std::chrono::steady_clock::now();
  callable();
  const auto end = std::chrono::steady_clock::now();
  return end - start;
}

template <class DatabaseInterface>
inline void Worker<DatabaseInterface>::WorkerMain() {
  // Pin the thread if asked.
  if (pin_to_core_.has_value()) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(pin_to_core_.value(), &cpuset);

    pthread_t thread = pthread_self();
    pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
  }

  // Initialize state needed for the trace replay.
  uint32_t read_xor = 0;
  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  // Run any needed per-worker initialization.
  db_->InitializeWorker(std::this_thread::get_id());

  // Now ready to proceed; wait until we're told to start.
  ready_.store(true, std::memory_order_release);
  can_start_->Wait();

  // Run our trace slice.
  const size_t stop_before = offset_ + num_requests_;
  for (size_t i = offset_; i < stop_before; ++i) {
    const auto& req = (*trace_)[i];

    bool measure_latency = false;
    if (++sampling_counter_ >= latency_sample_period_) {
      measure_latency = true;
      sampling_counter_ = 0;
    }

    switch (req.op) {
      case Request::Operation::kRead: {
        bool succeeded = false;
        value_out.clear();
        const auto run_time = MeasurementHelper(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              succeeded = db_->Read(req.key, &value_out);
              // Force a read of the extracted value. We want to count this time
              // against the read latency too.
              read_xor ^= *reinterpret_cast<const uint32_t*>(value_out.c_str());
              return succeeded;
            },
            measure_latency);
        tracker_.RecordRead(run_time, value_out.size(), succeeded);
        if (!succeeded && expect_request_success_) {
          throw std::runtime_error(
              "Failed to read a key that was expected to be found.");
        }
        break;
      }

      case Request::Operation::kInsert: {
        // Inserts count the whole record size, since this should be the first
        // time the entire record is written to the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Insert(req.key, req.value, req.value_size);
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size + sizeof(req.key),
                             succeeded);
        if (!succeeded && expect_request_success_) {
          throw std::runtime_error(
              "Failed to insert a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kUpdate: {
        // Updates only record the value size, since the key should already
        // exist in the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Update(req.key, req.value, req.value_size);
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size, succeeded);
        if (!succeeded && expect_request_success_) {
          throw std::runtime_error(
              "Failed to update a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kScan: {
        bool succeeded = false;
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        const auto run_time = MeasurementHelper(
            [this, &req, &scan_out, &read_xor, &succeeded]() {
              succeeded = db_->Scan(req.key, req.scan_amount, &scan_out);
              // Force a read of the first extracted value. We want to count
              // this time against the read latency too.
              read_xor ^= *reinterpret_cast<const uint32_t*>(
                  scan_out.front().second.c_str());
            },
            measure_latency);
        size_t scanned_bytes = 0;
        for (const auto& entry : scan_out) {
          scanned_bytes += sizeof(entry.first) + entry.second.size();
        }
        tracker_.RecordScan(run_time, scanned_bytes, scan_out.size(),
                            succeeded);
        if (!succeeded && expect_request_success_) {
          throw std::runtime_error(
              "Failed to run a range scan (expected to succeed).");
        }
        if (expect_scan_amount_found_ && scan_out.size() < req.scan_amount) {
          throw std::runtime_error(
              "A range scan returned too few (or too many) records.");
        }
        break;
      }

      default:
        throw std::runtime_error("Unrecognized request operation!");
    }
  }

  // Used to prevent optimizing away reads.
  tracker_.SetReadXOR(read_xor);

  // Notify others that we are done.
  done_.Raise();

  // Run any needed shutdown code.
  db_->ShutdownWorker(std::this_thread::get_id());
}

}  // namespace impl
}  // namespace ycsbr
