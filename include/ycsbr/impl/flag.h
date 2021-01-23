#pragma once

#include <future>

namespace ycsbr {
namespace impl {

// A thread synchronization object representing a "flag" that can be raised (but
// never lowered). Threads can wait for the flag to be raised, and one thread is
// allowed to "raise" the flag to notify the waiting threads.
class Flag {
 public:
  Flag() : future_(flag_.get_future().share()) {}

  // "Raises" this flag, allowing any threads that have called `Wait()` or will
  // call it in the future to proceed.
  //
  // NOTE: Caller must guarantee that this method is called **at most once**.
  void Raise() { flag_.set_value(); }

  // Wait for this flag to be raised. Threads will be blocked until the flag has
  // been raised. Threads that call this method after the flag has been raised
  // will proceed without blocking.
  //
  // This method can be called concurrently by multiple threads without mutual
  // exclusion.
  void Wait() const { future_.get(); }

 private:
  std::promise<void> flag_;
  std::shared_future<void> future_;
};

}  // namespace impl
}  // namespace ycsbr
