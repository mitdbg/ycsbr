#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
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
  Executor(DatabaseInterface* db, WorkloadProducer producer, size_t id,
           const Flag* can_start, const RunOptions& options);

  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;

  // Runs the workload produced by the producer.
  void operator()();

  void WaitForReady() const;
  void WaitForCompletion() const;
  MetricsTracker&& GetResults() &&;

  // Meant for use by YCSBR's internal microbenchmarks.
  void BM_WorkloadLoop();

 private:
  void WorkloadLoop();
  void SetupOutputFileIfNeeded();

  Flag ready_;
  const Flag* can_start_;
  Flag done_;

  DatabaseInterface* db_;
  WorkloadProducer producer_;
  MetricsTracker tracker_;
  size_t id_;

  const RunOptions options_;
  size_t latency_sampling_counter_;
  size_t throughput_sampling_counter_;

  // Used to print out throughput samples, if requested.
  std::ofstream throughput_output_file_;
};

// Implementation details follow.

template <class DatabaseInterface, typename WorkloadProducer>
inline Executor<DatabaseInterface, WorkloadProducer>::Executor(
    DatabaseInterface* db, WorkloadProducer producer, const size_t id,
    const Flag* can_start, const RunOptions& options)
    : ready_(),
      can_start_(can_start),
      done_(),
      db_(db),
      producer_(std::move(producer)),
      tracker_(),
      id_(id),
      options_(options),
      latency_sampling_counter_(0),
      throughput_sampling_counter_(0),
      throughput_output_file_() {}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WaitForReady()
    const {
  return ready_.Wait();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WaitForCompletion()
    const {
  done_.Wait();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline MetricsTracker&&
Executor<DatabaseInterface, WorkloadProducer>::GetResults() && {
  WaitForCompletion();
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

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::operator()() {
  // Run any needed preparation code.
  producer_.Prepare();

  // Sets up the throughput sample output file, if needed.
  SetupOutputFileIfNeeded();

  // Now ready to proceed; wait until we're told to start.
  ready_.Raise();
  can_start_->Wait();

  // Run the job.
  WorkloadLoop();

  // Notify others that we are done.
  done_.Raise();
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void
Executor<DatabaseInterface, WorkloadProducer>::SetupOutputFileIfNeeded() {
  if (options_.throughput_sample_period == 0) return;
  const auto filename =
      options_.output_dir /
      (options_.throughput_output_file_prefix + std::to_string(id_) + ".csv");
  throughput_output_file_ = std::ofstream(filename);
  if (throughput_output_file_.fail()) {
    throw std::invalid_argument("Failed to create output file: " +
                                filename.string());
  }
  throughput_output_file_ << "mrecords_per_s,elapsed_ns" << std::endl;
}

template <class DatabaseInterface, typename WorkloadProducer>
inline void Executor<DatabaseInterface, WorkloadProducer>::WorkloadLoop() {
  // Initialize state needed for the replay.
  uint32_t read_xor = 0;
  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  tracker_.ResetSample();

  // Run our trace slice.
  while (producer_.HasNext()) {
    const auto& req = producer_.Next();

    bool measure_latency = false;
    if (++latency_sampling_counter_ >= options_.latency_sample_period) {
      measure_latency = true;
      latency_sampling_counter_ = 0;
    }

    switch (req.op) {
      case Request::Operation::kRead:
      case Request::Operation::kNegativeRead: {
        bool succeeded = false;
        value_out.clear();
        const auto run_time = MeasurementHelper(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              succeeded = db_->Read(req.key, &value_out);
              if (succeeded) {
                // Force a read of the extracted value. We want to count this
                // time against the read latency too.
                read_xor ^=
                    *reinterpret_cast<const uint32_t*>(value_out.c_str());
              }
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
        const auto run_time = MeasurementHelper(
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
        const auto run_time = MeasurementHelper(
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
        const auto run_time = MeasurementHelper(
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

      case Request::Operation::kReadModifyWrite: {
        bool succeeded = false;

        // First, do the read.
        const auto read_run_time = MeasurementHelper(
            [this, &req, &value_out, &read_xor, &succeeded]() {
              // Do the read.
              succeeded = db_->Read(req.key, &value_out);
              if (!succeeded) return;
              // Force a read of the extracted value. We want to count this
              // time against the read latency too.
              read_xor ^= *reinterpret_cast<const uint32_t*>(value_out.c_str());
            },
            measure_latency);
        tracker_.RecordRead(read_run_time, value_out.size(), succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to read a record during a read-modify-write (expected to "
              "succeed).");
        }
        // Skip the write if the read failed.
        if (!succeeded) break;

        // Now do the write.
        const auto write_run_time = MeasurementHelper(
            [this, &req, &succeeded]() {
              succeeded = db_->Update(req.key, req.value, req.value_size);
            },
            measure_latency);
        tracker_.RecordWrite(write_run_time, req.value_size, succeeded);
        if (!succeeded && options_.expect_request_success) {
          throw std::runtime_error(
              "Failed to update a record during a read-modify-write (expected "
              "to succeed).");
        }
        break;
      }

      default:
        throw std::runtime_error("Unrecognized request operation!");
    }

    if (options_.throughput_sample_period > 0 &&
        ++throughput_sampling_counter_ >= options_.throughput_sample_period) {
      auto sample = tracker_.GetSample();
      throughput_output_file_ << sample.MRecordsPerSecond() << ","
                              << sample.ElapsedTimeNanos().count() << std::endl;
      throughput_sampling_counter_ = 0;
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
