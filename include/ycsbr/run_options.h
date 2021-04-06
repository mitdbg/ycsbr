#pragma once

#include <cstring>

namespace ycsbr {

// Options used to configure Session-based trace replays and workload runs.
struct RunOptions {
  // Used to configure latency sampling. Sampling is done by individual workers,
  // and all workers will share the same sampling configuration. If this is set
  // to 1, a worker will measure the latency of all of its requests. If set to
  // some value `n`, a worker will measure every `n`-th request's latency.
  size_t latency_sample_period = 10;

  // If set to true, the benchmark will fail if any request fails. This should
  // only be used if you expect all requests to succeed (e.g., there are no
  // negative lookups and no updates of non-existent keys).
  bool expect_request_success = false;

  // If set to true, the benchmark will fail if any scan requests return fewer
  // (or more) records than requested. This should only be used if you expect
  // all scan amounts to be "valid".
  bool expect_scan_amount_found = false;
};

}  // namespace ycsbr
