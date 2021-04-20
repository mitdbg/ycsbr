#include <cassert>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

#include "../meter.h"
#include "../trace_workload.h"
#include "executor.h"

namespace ycsbr {

template <class DatabaseInterface>
inline Session<DatabaseInterface>::Session(size_t num_threads,
                                           const std::vector<size_t>& core_map)
    : threads_(core_map.size() == num_threads
                   ? (std::make_unique<impl::ThreadPool>(
                         num_threads, core_map,
                         [this]() {
                           db_.InitializeWorker(std::this_thread::get_id());
                         },
                         [this]() {
                           db_.ShutdownWorker(std::this_thread::get_id());
                         }))
                   : (std::make_unique<impl::ThreadPool>(
                         num_threads,
                         [this]() {
                           db_.InitializeWorker(std::this_thread::get_id());
                         },
                         [this]() {
                           db_.ShutdownWorker(std::this_thread::get_id());
                         }))),
      num_threads_(num_threads),
      initialized_(false) {
  if (num_threads == 0) {
    throw std::invalid_argument("Must use at least 1 thread.");
  }
}

template <class DatabaseInterface>
inline Session<DatabaseInterface>::~Session() {
  Terminate();
}

template <class DatabaseInterface>
inline void Session<DatabaseInterface>::Initialize() {
  if (initialized_ || threads_ == nullptr) return;
  threads_->Submit([this]() { db_.InitializeDatabase(); }).get();
  initialized_ = true;
}

template <class DatabaseInterface>
inline void Session<DatabaseInterface>::Terminate() {
  if (threads_ == nullptr) return;
  if (initialized_) {
    threads_->Submit([this]() { db_.ShutdownDatabase(); }).get();
  }
  threads_.reset(nullptr);
}

template <class DatabaseInterface>
inline DatabaseInterface& Session<DatabaseInterface>::db() {
  return db_;
}

template <class DatabaseInterface>
inline const DatabaseInterface& Session<DatabaseInterface>::db() const {
  return db_;
}

template <class DatabaseInterface>
inline BenchmarkResult Session<DatabaseInterface>::ReplayBulkLoadTrace(
    const BulkLoadTrace& load) {
  std::chrono::steady_clock::time_point start, end;
  threads_
      ->Submit([this, &load, &start, &end]() {
        start = std::chrono::steady_clock::now();
        db_.BulkLoad(load);
        end = std::chrono::steady_clock::now();
      })
      .get();

  const auto run_time = end - start;
  Meter load_meter;
  load_meter.RecordMultiple(run_time, load.DatasetSizeBytes(), load.size());
  return BenchmarkResult(run_time, 0, FrozenMeter(),
                         std::move(load_meter).Freeze(), FrozenMeter(), 0, 0,
                         0);
}

template <class DatabaseInterface>
inline BenchmarkResult Session<DatabaseInterface>::ReplayTrace(
    const Trace& trace, const RunOptions& options) {
  const TraceWorkload workload(&trace);
  return RunWorkload<TraceWorkload>(workload, options);
}

template <class DatabaseInterface>
template <class CustomWorkload>
inline BenchmarkResult Session<DatabaseInterface>::RunWorkload(
    const CustomWorkload& workload, const RunOptions& options) {
  using Runner =
      impl::Executor<DatabaseInterface, typename CustomWorkload::Producer>;

  auto producers = workload.GetProducers(num_threads_);
  assert(producers.size() == num_threads_);

  impl::Flag can_start;
  std::vector<std::unique_ptr<Runner>> executors;
  executors.reserve(num_threads_);
  for (auto& producer : producers) {
    executors.push_back(std::make_unique<Runner>(&db_, std::move(producer),
                                                 &can_start, options));
    threads_->SubmitNoWait([exec = executors.back().get()]() { (*exec)(); });
  }

  // Wait for the executors to finish performing their startup work.
  for (const auto& executor : executors) {
    executor->WaitForReady();
  }

  // Start the workload and the timer.
  const auto start = std::chrono::steady_clock::now();
  can_start.Raise();
  for (auto& executor : executors) {
    executor->WaitForCompletion();
  }
  const auto end = std::chrono::steady_clock::now();

  // Retrieve the results.
  std::vector<impl::MetricsTracker> results;
  results.reserve(num_threads_);
  for (auto& executor : executors) {
    results.emplace_back(std::move(*executor).GetResults());
  }

  return impl::MetricsTracker::FinalizeGroup(end - start, std::move(results));
}

}  // namespace ycsbr
