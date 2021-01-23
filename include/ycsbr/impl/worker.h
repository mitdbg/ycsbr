#pragma once

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../data.h"
#include "flag.h"
#include "tracking.h"

namespace ycsbr {
namespace impl {

// Runs a contiguous slice of a `Workload` (i.e., a contiguous slice of
// requests) in a separate thread.
template <class DatabaseInterface>
class Worker {
 public:
  Worker(DatabaseInterface* db, const Workload* workload, size_t offset,
         size_t num_requests, const Flag* can_start,
         std::optional<unsigned> pin_to_core);
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
  DatabaseInterface* db_;
  const Workload* workload_;
  size_t offset_, num_requests_;
  const Flag* can_start_;
  std::optional<unsigned> pin_to_core_;
  MetricsTracker tracker_;
};

// Implementation details follow.

template <class DatabaseInterface>
inline Worker<DatabaseInterface>::Worker(DatabaseInterface* db,
                                         const Workload* workload,
                                         size_t offset, size_t num_requests,
                                         const Flag* can_start,
                                         std::optional<unsigned> pin_to_core)
    : ready_(false),
      db_(db),
      workload_(workload),
      offset_(offset),
      num_requests_(num_requests),
      can_start_(can_start),
      pin_to_core_(pin_to_core) {
  thread_ = std::thread(&Worker<DatabaseInterface>::WorkerMain, this);
}

template <class DatabaseInterface>
inline Worker<DatabaseInterface>::~Worker() {
  Wait();
}

template <class DatabaseInterface>
inline bool Worker<DatabaseInterface>::IsReady() const {
  return ready_.load(std::memory_order_acquire);
}

template <class DatabaseInterface>
inline void Worker<DatabaseInterface>::Wait() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

template <class DatabaseInterface>
inline MetricsTracker&& Worker<DatabaseInterface>::GetResults() && {
  Wait();
  return std::move(tracker_);
}

template <typename Callable>
inline std::pair<bool, std::chrono::nanoseconds> MeasureRunTime(
    Callable callable) {
  const auto start = std::chrono::steady_clock::now();
  bool succeeded = callable();
  const auto end = std::chrono::steady_clock::now();
  return {succeeded, end - start};
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

  // Initialize state needed for the workload run.
  uint32_t read_xor = 0;
  std::string value_out;
  std::vector<std::pair<Request::Key, std::string>> scan_out;

  // Now ready to proceed; wait until we're told to start.
  ready_.store(true, std::memory_order_release);
  can_start_->Wait();

  // Run our workload slice.
  const size_t stop_before = offset_ + num_requests_;
  for (size_t i = offset_; i < stop_before; ++i) {
    const auto& req = (*workload_)[i];

    switch (req.op) {
      case Request::Operation::kRead: {
        value_out.clear();
        auto res = MeasureRunTime([this, &req, &value_out, &read_xor]() {
          bool succeeded = db_->Read(req.key, &value_out);
          if (!succeeded) {
            throw std::runtime_error(
                "Failed to read a key requested by the benchmark!");
          }
          // Force a read of the extracted value. We want to count this time
          // against the read latency too.
          read_xor ^= *reinterpret_cast<const uint32_t*>(value_out.c_str());
          return succeeded;
        });
        tracker_.RecordRead(res.second, value_out.size());
        break;
      }

      case Request::Operation::kInsert: {
        auto res = MeasureRunTime([this, &req]() {
          return db_->Insert(req.key, req.value, req.value_size);
        });
        if (!res.first) {
          throw std::runtime_error(
              "Failed to insert a key requested by the benchmark!");
        }
        // Inserts count the whole record size, since this should be the first
        // time the entire record is written to the DB.
        tracker_.RecordWrite(res.second, req.value_size + sizeof(req.key));
        break;
      }

      case Request::Operation::kUpdate: {
        auto res = MeasureRunTime([this, &req]() {
          return db_->Update(req.key, req.value, req.value_size);
        });
        if (!res.first) {
          throw std::runtime_error(
              "Failed to update a key requested by the benchmark!");
        }
        // Updates only record the value size, since the key should already
        // exist in the DB.
        tracker_.RecordWrite(res.second, req.value_size);
        break;
      }

      case Request::Operation::kScan: {
        scan_out.clear();
        scan_out.reserve(req.scan_amount);
        auto res = MeasureRunTime([this, &req, &scan_out, &read_xor]() {
          bool succeeded = db_->Scan(req.key, req.scan_amount, &scan_out);
          if (!succeeded || scan_out.size() != req.scan_amount) {
            throw std::runtime_error(
                "Failed to scan a key range requested by the benchmark!");
          }
          // Force a read of the first extracted value. We want to count this
          // time against the read latency too.
          read_xor ^= *reinterpret_cast<const uint32_t*>(
              scan_out.front().second.c_str());
          return succeeded;
        });
        size_t scanned_bytes = 0;
        for (const auto& entry : scan_out) {
          scanned_bytes += entry.second.size();
        }
        tracker_.RecordScan(res.second, scanned_bytes, scan_out.size());
        break;
      }

      default:
        throw std::runtime_error("Unrecognized request operation!");
    }
  }

  // Used to prevent optimizing away reads.
  tracker_.SetReadXOR(read_xor);
}

}  // namespace impl
}  // namespace ycsbr
