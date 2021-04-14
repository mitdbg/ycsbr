#include <limits>
#include <vector>
#include <random>

#include "benchmark/benchmark.h"
#include "ycsbr/impl/util.h"

namespace {

using namespace ycsbr::impl;

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

BENCHMARK(BM_FNVHash64)->Arg(10000);
BENCHMARK(BM_MersenneTwister)->Arg(10000);
BENCHMARK(BM_UniformDist)->Arg(10000);

}  // namespace
