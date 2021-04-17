#pragma once

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../request.h"
#include "../run_options.h"
#include "flag.h"
#include "tracking.h"

namespace ycsbr {
namespace impl {

template <class DatabaseInterface, typename WorkloadProducer>
class Executor {
 public:
  Executor(DatabaseInterface* db, WorkloadProducer producer,
           const Flag* can_start, const RunOptions& options);

  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;

  // Runs the workload produced by the producer.
  void operator()();

  bool IsReady() const;
  void Wait();
  MetricsTracker&& GetResults() &&;

  // Meant for use by YCSBR's internal microbenchmarks.
  void BM_WorkloadLoop();

 private:
  void WorkloadLoop();

  std::atomic<bool> ready_;
  const Flag* can_start_;
  Flag done_;

  DatabaseInterface* db_;
  WorkloadProducer producer_;
  MetricsTracker tracker_;

  const RunOptions options_;
  size_t sampling_counter_;
};

// Implementation details follow.

template <class DatabaseInterface, typename WorkloadProducer>
inline Executor<DatabaseInterface, WorkloadProducer>::Executor(
    DatabaseInterface* db, WorkloadProducer producer, const Flag* can_start,
    const RunOptions& options)
    : ready_(false),
      can_start_(can_start),
      done_(),
      db_(db),
      producer_(std::move(producer)),
      tracker_(),
      options_(options),
      sampling_counter_(0) {}

template <class DatabaseInterface, typename WorkloadProducer>
inline bool Executor<DatabaseInterface, WorkloadProducer>::IsReady() const {
  return ready_.load(std::memory_order_acquire);
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::Wait() {
  done_.Wait();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline MetricsTracker&&
Executor<DatabaseInterface, WorkloadProducer>::GetResults() && {
  Wait();
  return std::move(tracker_);
}

template <typename Callable>
inline std::optional<std::chrono::nanoseconds> MeasurementHelper2(
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

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::operator()() {
  // Run any needed preparation code.
  producer_.Prepare();

  // Now ready to proceed; wait until we're told to start.
  ready_.store(true, std::memory_order_release);
  can_start_->Wait();

  // Run the job.
  WorkloadLoop();

  // Notify others that we are done.
  done_.Raise();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WorkloadLoop() {
  // Initialize state needed for the replay.
  uint32_t read_xor = 0;
  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  // Run our trace slice.
  while (producer_.HasNext()) {
    const auto& req = producer_.Next();

    bool measure_latency = false;
    if (++sampling_counter_ >= options_.latency_sample_period) {
      measure_latency = true;
      sampling_counter_ = 0;
    }

    switch (req.op) {
      case Request::Operation::kRead: {
        bool succeeded = false;
        value_out.clear();
        const auto run_time = MeasurementHelper2(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              succeeded = db_->Read(req.key, &value_out);
              if (succeeded) {
                // Force a read of the extracted value. We want to count this
                // time against the read latency too.
                read_xor ^=
                    *reinterpret_cast<const uint32_t*>(value_out.c_str());
              }
              return succeeded;
            },
            measure_latency);
        tracker_.RecordRead(run_time, value_out.size(), succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to read a key that was expected to be found.");
        }
        break;
      }

      case Request::Operation::kInsert: {
        // Inserts count the whole record size, since this should be the first
        // time the entire record is written to the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper2(
            [this, &req, &succeeded]() {
              succeeded = db_->Insert(req.key, req.value, req.value_size);
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size + sizeof(req.key),
                             succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to insert a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kUpdate: {
        // Updates only record the value size, since the key should already
        // exist in the DB.
        bool succeeded = false;
        const auto run_time = MeasurementHelper2(
            [this, &req, &succeeded]() {
              succeeded = db_->Update(req.key, req.value, req.value_size);
            },
            measure_latency);
        tracker_.RecordWrite(run_time, req.value_size, succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to update a record (expected to succeed).");
        }
        break;
      }

      case Request::Operation::kScan: {
        bool succeeded = false;
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        const auto run_time = MeasurementHelper2(
            [this, &req, &scan_out, &read_xor, &succeeded]() {
              succeeded = db_->Scan(req.key, req.scan_amount, &scan_out);
              if (succeeded && scan_out.size() > 0) {
                // Force a read of the first extracted value. We want to count
                // this time against the read latency too.
                read_xor ^= *reinterpret_cast<const uint32_t*>(
                    scan_out.front().second.c_str());
              }
            },
            measure_latency);
        size_t scanned_bytes = 0;
        for (const auto& entry : scan_out) {
          scanned_bytes += sizeof(entry.first) + entry.second.size();
        }
        tracker_.RecordScan(run_time, scanned_bytes, scan_out.size(),
                            succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to run a range scan (expected to succeed).");
        }
        if (options_.expect_scan_amount_found &&
            scan_out.size() < req.scan_amount) {
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
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::BM_WorkloadLoop() {
  WorkloadLoop();
}

}  // namespace impl
}  // namespace ycsbr
