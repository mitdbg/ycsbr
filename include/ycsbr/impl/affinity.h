#pragma once

#include <pthread.h>

namespace ycsbr {
namespace impl {

// Pin the calling thread to `core`. This function returns true if the pin was
// successful.
inline bool PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);

  pthread_t thread = pthread_self();
  return pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset) == 0;
}

}  // namespace impl
}  // namespace ycsbr
