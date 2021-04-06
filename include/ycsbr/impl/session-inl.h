#include <cassert>
#include <chrono>
#include <thread>

#include "../meter.h"
#include "../workload/trace.h"

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
      num_threads_(num_threads) {
  threads_->Submit([this]() { db_.InitializeDatabase(); }).get();
}

template <class DatabaseInterface>
inline Session<DatabaseInterface>::~Session() {
  Terminate();
}

template <class DatabaseInterface>
inline void Session<DatabaseInterface>::Terminate() {
  if (threads_ == nullptr) return;
  threads_->Submit([this]() { db_.DeleteDatabase(); }).get();
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
  auto producers = workload.GetProducers(num_threads_);
  assert(producers.size() == num_threads);
  return BenchmarkResult(std::chrono::nanoseconds(0));
}

}  // namespace ycsbr
