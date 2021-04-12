// Implementation of declarations in yscbr/benchmark.h. Do not include this
// header!
#include <chrono>

#include "../run_options.h"
#include "../session.h"

namespace ycsbr {

template <class DatabaseInterface>
inline BenchmarkResult ReplayTrace(
    const Trace& trace, const BulkLoadTrace* load,
    const BenchmarkOptions<DatabaseInterface>& options) {
  Session<DatabaseInterface> session(options.num_threads,
                                     options.pin_to_core_map);
  if (options.pre_run_hook) {
    options.pre_run_hook(session.db());
  }
  session.Initialize();
  if (load != nullptr) {
    session.ReplayBulkLoadTrace(*load);
  }
  RunOptions roptions;
  roptions.latency_sample_period = options.latency_sample_period;
  roptions.expect_request_success = options.expect_request_success;
  roptions.expect_scan_amount_found = options.expect_scan_amount_found;
  return session.ReplayTrace(trace, roptions);
  // `Session::Terminate()` called by the `Session` destructor.
}

template <class DatabaseInterface>
inline BenchmarkResult ReplayTrace(
    const BulkLoadTrace& load,
    std::function<void(DatabaseInterface&)> pre_run_hook) {
  Session<DatabaseInterface> session(/*num_threads=*/1);
  if (pre_run_hook) {
    pre_run_hook(session.db());
  }
  session.Initialize();
  return session.ReplayBulkLoadTrace(load);
  // `Session::Terminate()` called by the `Session` destructor.
}

}  // namespace ycsbr
