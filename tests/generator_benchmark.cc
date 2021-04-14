#include <cmath>
#include <limits>
#include <vector>
#include <random>

#include "benchmark/benchmark.h"
#include "ycsbr/gen/util.h"
#include "ycsbr/gen/zipfian.h"

namespace {

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

BENCHMARK(BM_FNVHash64)->Arg(10000);
BENCHMARK(BM_MersenneTwister)->Arg(10000);
BENCHMARK(BM_UniformDist)->Arg(10000);
BENCHMARK(BM_MathPow)->Arg(10000);
BENCHMARK(BM_ZipfianGen)->Arg(10000000);

}  // namespace
