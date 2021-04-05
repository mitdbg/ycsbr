#include <thread>

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
                         }))) {
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
    const BulkLoadTrace& load) const {
  return BenchmarkResult();
}

template <class DatabaseInterface>
inline BenchmarkResult Session<DatabaseInterface>::ReplayTrace(
    const Trace& trace) const {
  return BenchmarkResult();
}

template <class DatabaseInterface>
template <class CustomWorkload>
inline BenchmarkResult Session<DatabaseInterface>::RunWorkload(
    const CustomWorkload& workload) const {
  return BenchmarkResult();
}

}  // namespace ycsbr
