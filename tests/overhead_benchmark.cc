#include <filesystem>
#include <memory>

#include "benchmark/benchmark.h"
#include "workloads/create_workload.h"
#include "ycsbr/benchmark.h"
#include "ycsbr/impl/executor.h"
#include "ycsbr/impl/flag.h"
#include "ycsbr/impl/worker.h"
#include "ycsbr/request.h"
#include "ycsbr/run_options.h"
#include "ycsbr/session.h"
#include "ycsbr/trace.h"
#include "ycsbr/workload/trace.h"

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

class SimpleWorkload {
 public:
  SimpleWorkload(size_t num_requests) : num_requests_(num_requests) {}
  class Producer {
   public:
    Producer(size_t num_requests) : num_requests_(num_requests), index_(0) {}
    bool HasNext() const { return index_ < num_requests_; }
    Request Next() {
      ++index_;
      return Request();
    }

   private:
    size_t num_requests_;
    size_t index_;
  };

  std::vector<Producer> GetProducers(const size_t num_producers) const {
    std::vector<Producer> producers;
    for (size_t i = 0; i < num_producers; ++i) {
      producers.emplace_back(num_requests_);
    }
    return producers;
  }

 private:
  size_t num_requests_;
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

template <WorkloadType Type>
void BM_SimpleInterfaceOverhead(benchmark::State& state) {
  const std::filesystem::path trace_file = CreateWorkloadFile<Type>();
  Trace::Options options;
  options.value_size = 16;
  const Trace trace = Trace::LoadFromFile(trace_file.string(), options);

  NoOpInterface db;
  BenchmarkOptions boptions;
  boptions.latency_sample_period = state.range(0);
  for (auto _ : state) {
    const auto result =
        ReplayTrace<NoOpInterface>(db, trace, nullptr, boptions);
    state.SetIterationTime(
        result.RunTime<std::chrono::duration<double>>().count());
  }

  const size_t requests_processed = kTraceSize * state.iterations();
  state.SetItemsProcessed(requests_processed);
  state.counters["PerRequestLatency"] =
      benchmark::Counter(requests_processed, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);
  std::filesystem::remove(trace_file);
}

template <WorkloadType Type>
void BM_ExecutorLoopOverhead(benchmark::State& state) {
  const std::filesystem::path trace_file = CreateWorkloadFile<Type>();
  Trace::Options options;
  options.value_size = 16;
  const Trace trace = Trace::LoadFromFile(trace_file.string(), options);
  const TraceWorkload workload = TraceWorkload(&trace);
  const std::vector<TraceWorkload::Producer> producers =
      workload.GetProducers(1);

  RunOptions roptions;
  roptions.latency_sample_period = state.range(0);
  NoOpInterface db;
  impl::Flag can_start;
  impl::Executor<NoOpInterface, TraceWorkload::Producer> executor(
      &db, producers.at(0), &can_start, roptions);
  for (auto _ : state) {
    executor.BM_WorkloadLoop();
  }
  const size_t requests_processed = kTraceSize * state.iterations();
  state.SetItemsProcessed(requests_processed);
  state.counters["PerRequestLatency"] =
      benchmark::Counter(requests_processed, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);
  std::filesystem::remove(trace_file);
}

template <WorkloadType Type>
void BM_SessionTraceReplayOverhead(benchmark::State& state) {
  const std::filesystem::path trace_file = CreateWorkloadFile<Type>();
  Trace::Options options;
  options.value_size = 16;
  const Trace trace = Trace::LoadFromFile(trace_file.string(), options);

  Session<NoOpInterface> session(1);
  RunOptions roptions;
  roptions.latency_sample_period = state.range(0);
  for (auto _ : state) {
    const auto result = session.ReplayTrace(trace, roptions);
    state.SetIterationTime(
        result.RunTime<std::chrono::duration<double>>().count());
  }

  const size_t requests_processed = kTraceSize * state.iterations();
  state.SetItemsProcessed(requests_processed);
  state.counters["PerRequestLatency"] =
      benchmark::Counter(requests_processed, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);
  std::filesystem::remove(trace_file);
}

void BM_SessionWorkloadOverhead(benchmark::State& state) {
  const size_t latency_sample_period = state.range(0);
  const size_t workload_length = state.range(1);

  Session<NoOpInterface> session(1);
  RunOptions roptions;
  roptions.latency_sample_period = latency_sample_period;
  for (auto _ : state) {
    const auto result = session.RunWorkload<SimpleWorkload>(
        SimpleWorkload(workload_length), roptions);
    state.SetIterationTime(
        result.RunTime<std::chrono::duration<double>>().count());
  }

  const size_t requests_processed = 10000 * state.iterations();
  state.SetItemsProcessed(requests_processed);
  state.counters["PerRequestLatency"] =
      benchmark::Counter(requests_processed, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);
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

BENCHMARK_TEMPLATE(BM_SimpleInterfaceOverhead, WorkloadType::kRunA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseManualTime();

BENCHMARK_TEMPLATE(BM_ExecutorLoopOverhead, WorkloadType::kLoadA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

BENCHMARK_TEMPLATE(BM_ExecutorLoopOverhead, WorkloadType::kRunA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

BENCHMARK_TEMPLATE(BM_ExecutorLoopOverhead, WorkloadType::kRunE)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseRealTime();

BENCHMARK_TEMPLATE(BM_SessionTraceReplayOverhead, WorkloadType::kRunA)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(30)
    ->UseManualTime();

BENCHMARK(BM_SessionWorkloadOverhead)
    ->Args({500, 1000})  // (latency_sample_period, workload_length)
    ->Args({1000, 10000})
    ->UseManualTime();

}  // namespace
