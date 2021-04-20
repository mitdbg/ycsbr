#include <cassert>

#include "affinity.h"

namespace ycsbr {
namespace impl {

inline ThreadPool::ThreadPool(size_t num_threads,
                              std::function<void()> on_start,
                              std::function<void()> on_shutdown)
    : shutdown_(false),
      on_start_(std::move(on_start)),
      on_shutdown_(std::move(on_shutdown)) {
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&ThreadPool::ThreadMain, this);
  }
}

inline ThreadPool::ThreadPool(size_t num_threads,
                              const std::vector<size_t>& thread_to_core,
                              std::function<void()> on_start,
                              std::function<void()> on_shutdown)
    : shutdown_(false),
      on_start_(std::move(on_start)),
      on_shutdown_(std::move(on_shutdown)) {
  assert(num_threads == thread_to_core.size());
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&ThreadPool::ThreadMainOnCore, this,
                          thread_to_core[i]);
  }
}

inline ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
  for (auto& thread : threads_) {
    thread.join();
  }
}

inline void ThreadPool::ThreadMainOnCore(size_t core_id) {
  PinToCore(core_id);
  ThreadMain();
}

inline void ThreadPool::ThreadMain() {
  on_start_();
  std::unique_ptr<Task> next_job = nullptr;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      // Need a loop here to handle spurious wakeup
      while (!shutdown_ && work_queue_.empty()) {
        cv_.wait(lock);
      }
      if (shutdown_ && work_queue_.empty()) break;
      next_job.reset(work_queue_.front().release());
      work_queue_.pop();
    }
    (*next_job)();
    next_job.reset(nullptr);
  }
  on_shutdown_();
}

}  // namespace impl
}  // namespace ycsbr
