#include <cmath>
#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "../generator/sampling.h"
#include "../generator/zipfian.h"
#include "benchmark/benchmark.h"
#include "db_interface.h"
#include "ycsbr/gen/util.h"
#include "ycsbr/gen/workload.h"
#include "ycsbr/session.h"

namespace {

using namespace ycsbr;
using namespace ycsbr::gen;

void BM_FNVHash64(benchmark::State& state) {
  std::vector<uint64_t> values;
  values.reserve(state.range(0));
  for (uint64_t i = 0; i < state.range(0); ++i) {
    values.push_back(i);
  }

  uint64_t hash = 0;
  for (auto _ : state) {
    for (uint64_t val : values) {
      benchmark::DoNotOptimize(hash = FNVHash64(val));
    }
  }

  const size_t num_hashes = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_hashes);
  state.counters["PerHashLatency"] = benchmark::Counter(
      num_hashes, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_MersenneTwister(benchmark::State& state) {
  std::vector<uint64_t> values;
  values.reserve(state.range(0));

  std::mt19937_64 rng(42);
  for (auto _ : state) {
    values.clear();
    for (uint64_t i = 0; i < state.range(0); ++i) {
      values.push_back(rng());
    }
  }

  const size_t num_values = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_values);
  state.counters["PerNumLatency"] = benchmark::Counter(
      num_values, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_UniformDist(benchmark::State& state) {
  std::vector<double> values;
  values.reserve(state.range(0));

  std::mt19937_64 rng(42);
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  for (auto _ : state) {
    values.clear();
    for (uint64_t i = 0; i < state.range(0); ++i) {
      values.push_back(dist(rng));
    }
  }

  const size_t num_values = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_values);
  state.counters["PerNumLatency"] = benchmark::Counter(
      num_values, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_MathPow(benchmark::State& state) {
  constexpr double exponent = 0.8;
  std::vector<double> values;
  values.reserve(state.range(0));
  for (uint64_t i = 0; i < state.range(0); ++i) {
    values.push_back(i);
  }

  for (auto _ : state) {
    for (size_t i = 0; i < values.size(); ++i) {
      values[i] = std::pow(values[i], exponent);
    }
  }

  const size_t num_values = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_values);
  state.counters["PerNumLatency"] = benchmark::Counter(
      num_values, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void BM_ZipfianGen(benchmark::State& state) {
  const size_t item_count = state.range(0);
  Zipfian zipf(item_count, 0.99, /*seed=*/42);
  for (auto _ : state) {
    benchmark::DoNotOptimize(zipf());
  }
}

void BM_FloydSample(benchmark::State& state) {
  const size_t sample_size = state.range(0);
  const size_t range_size = state.range(1);
  std::mt19937 rng(42);
  std::vector<uint64_t> samples(sample_size, 0);
  for (auto _ : state) {
    FloydSample<uint64_t, std::mt19937>(sample_size, 0, range_size - 1,
                                        &samples, 0, rng);
  }
  const size_t num_samples_taken = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_samples_taken);
  state.counters["PerSampleLatency"] =
      benchmark::Counter(num_samples_taken, benchmark::Counter::kIsRate |
                                                benchmark::Counter::kInvert);
}

void BM_FisherYatesSample(benchmark::State& state) {
  const size_t sample_size = state.range(0);
  const size_t range_size = state.range(1);
  std::mt19937 rng(42);
  std::vector<uint64_t> samples(sample_size, 0);
  for (auto _ : state) {
    FisherYatesSample<uint64_t, std::mt19937>(sample_size, 0, range_size - 1,
                                              &samples, 0, rng);
  }
  const size_t num_samples_taken = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_samples_taken);
  state.counters["PerSampleLatency"] =
      benchmark::Counter(num_samples_taken, benchmark::Counter::kIsRate |
                                                benchmark::Counter::kInvert);
}

void BM_SelectionSample(benchmark::State& state) {
  const size_t sample_size = state.range(0);
  const size_t range_size = state.range(1);
  std::mt19937 rng(42);
  std::vector<uint64_t> samples(sample_size, 0);
  for (auto _ : state) {
    SelectionSample<uint64_t, std::mt19937>(sample_size, 0, range_size - 1,
                                            &samples, 0, rng);
  }
  const size_t num_samples_taken = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_samples_taken);
  state.counters["PerSampleLatency"] =
      benchmark::Counter(num_samples_taken, benchmark::Counter::kIsRate |
                                                benchmark::Counter::kInvert);
}

void BM_AutoSample(benchmark::State& state) {
  const size_t sample_size = state.range(0);
  const size_t range_size = state.range(1);
  std::mt19937 rng(42);
  std::vector<uint64_t> samples(sample_size, 0);
  for (auto _ : state) {
    SampleWithoutReplacement<uint64_t, std::mt19937>(
        sample_size, 0, range_size - 1, &samples, 0, rng);
  }
  const size_t num_samples_taken = state.range(0) * state.iterations();
  state.SetItemsProcessed(num_samples_taken);
  state.counters["PerSampleLatency"] =
      benchmark::Counter(num_samples_taken, benchmark::Counter::kIsRate |
                                                benchmark::Counter::kInvert);
}

void BM_PhasedWorkloadOverheadUniform(benchmark::State& state) {
  constexpr size_t num_requests = 1000000;
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 20000000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  read:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";

  std::unique_ptr<PhasedWorkload> workload =
      PhasedWorkload::LoadFromString(config);

  ycsbr::Session<NoOpInterface> session(1);
  ycsbr::RunOptions options;
  options.latency_sample_period = 1000;
  session.Initialize();
  for (auto _ : state) {
    auto result = session.RunWorkload(*workload, options);
    state.SetIterationTime(
        result.RunTime<std::chrono::duration<double>>().count());
  }
  session.Terminate();
  const size_t num_requests_executed = num_requests * state.iterations();
  state.SetItemsProcessed(num_requests_executed);
  state.counters["PerRequestLatency"] = benchmark::Counter(
      num_requests_executed,
      benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_FNVHash64)->Arg(10000);
BENCHMARK(BM_MersenneTwister)->Arg(10000);
BENCHMARK(BM_UniformDist)->Arg(10000);
BENCHMARK(BM_MathPow)->Arg(10000);
BENCHMARK(BM_ZipfianGen)->Arg(10000000);
BENCHMARK(BM_PhasedWorkloadOverheadUniform)->UseManualTime();

// Generally, Floyd sampling is faster than Fisher-Yates based sampling. These
// sampling techniques outperform selection sampling when the sample size is
// much smaller than the range. Let k be the sample size, and N be the range
// size. Selection sampling is an O(N) algorithm whereas Floyd sampling is
// amortized O(k). Empirically, seems like when k/N < 0.02, Floyd sampling is a
// faster choice.

BENCHMARK(BM_FloydSample)
    ->Args({100, 500000000})
    ->Args({1000, 500000000})
    ->Args({10000, 500000000})
    ->Args({100000, 500000000})
    ->Args({1000000, 500000000})
    ->Args({10000000, 500000000})
    ->Args({100000000, 500000000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_FisherYatesSample)
    ->Args({100, 500000000})
    ->Args({1000, 500000000})
    ->Args({10000, 500000000})
    ->Args({100000, 500000000})
    ->Args({1000000, 500000000})
    ->Args({10000000, 500000000})
    ->Args({100000000, 500000000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_SelectionSample)
    ->Args({100, 500000000})
    ->Args({1000, 500000000})
    ->Args({10000, 500000000})
    ->Args({100000, 500000000})
    ->Args({1000000, 500000000})
    ->Args({10000000, 500000000})
    ->Args({100000000, 500000000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_AutoSample)
    ->Args({100, 500000000})
    ->Args({1000, 500000000})
    ->Args({10000, 500000000})
    ->Args({100000, 500000000})
    ->Args({1000000, 500000000})
    ->Args({10000000, 500000000})
    ->Args({100000000, 500000000})
    ->Args({20000000, std::numeric_limits<int64_t>::max()})
    ->Args({100000000, std::numeric_limits<int64_t>::max()})
    ->Unit(benchmark::kMillisecond);

}  // namespace
