#include <filesystem>
#include <memory>

#include "benchmark/benchmark.h"
#include "workloads/create_workload.h"
#include "ycsbr/impl/flag.h"
#include "ycsbr/impl/worker.h"
#include "ycsbr/request.h"
#include "ycsbr/trace.h"

namespace {

using namespace ycsbr;

// All operations are intentionally no-ops.
class NoOpInterface {
 public:
  void InitializeWorker(const std::thread::id& worker_id) {}
  void ShutdownWorker(const std::thread::id& worker_id) {}
  void InitializeDatabase() {}
  void DeleteDatabase() {}
  void BulkLoad(const BulkLoadTrace& load) {}
  bool Update(Request::Key key, const char* value, size_t value_size) {
    return true;
  }
  bool Insert(Request::Key key, const char* value, size_t value_size) {
    return true;
  }
  bool Read(Request::Key key, std::string* value_out) { return true; }
  bool Scan(Request::Key key, size_t amount,
            std::vector<std::pair<Request::Key, std::string>>* scan_out) {
    return true;
  }
};

template <WorkloadType Type>
void BM_WorkerLoopOverhead(benchmark::State& state) {
  const std::filesystem::path trace_file = CreateWorkloadFile<Type>();
  NoOpInterface db;
  impl::Flag can_start;
  Trace::Options options;
  options.value_size = 16;
  const Trace trace = Trace::LoadFromFile(trace_file.string(), options);
  std::unique_ptr<impl::Worker<NoOpInterface>> worker =
      std::make_unique<impl::Worker<NoOpInterface>>(
          &db, &trace, 0, kTraceSize, &can_start, std::optional<unsigned>(),
          /*latency_sample_period=*/state.range(0), false, false,
          /*internal_benchmark_mode=*/true);
  for (auto _ : state) {
    worker->BM_WorkloadLoop();
  }
  const size_t requests_processed = kTraceSize * state.iterations();
  state.SetItemsProcessed(requests_processed);
  state.counters["PerRequestLatency"] =
      benchmark::Counter(requests_processed, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);
  std::filesystem::remove(trace_file);
}

BENCHMARK_TEMPLATE(BM_WorkerLoopOverhead, WorkloadType::kLoadA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

BENCHMARK_TEMPLATE(BM_WorkerLoopOverhead, WorkloadType::kRunA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

BENCHMARK_TEMPLATE(BM_WorkerLoopOverhead, WorkloadType::kRunE)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

}  // namespace
