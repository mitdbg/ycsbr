#include <string>

#include "gtest/gtest.h"
#include "ycsbr/gen/workload.h"
#include "ycsbr/session.h"

using namespace ycsbr;
using namespace ycsbr::gen;

namespace {

void ParseAndPrepare(const std::string& raw_config) {
  auto workload = PhasedWorkload::LoadFromString(raw_config);
  auto producers = workload->GetProducers(1);
  for (auto& producer : producers) {
    producer.Prepare();
  }
}

TEST(GeneratorConfigTest, InvalidProportionPct) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  read:\n"
      "    proportion_pct: 55\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";
  // Proportions exceed 100%.
  ASSERT_THROW(ParseAndPrepare(config), std::invalid_argument);
}

TEST(GeneratorConfigTest, InvalidProportionPctSmall) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  read:\n"
      "    proportion_pct: 5\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";
  // Proportions do not add up to 100%.
  ASSERT_THROW(ParseAndPrepare(config), std::invalid_argument);
}

TEST(GeneratorConfigTest, LoadRangeTooSmall) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 10000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 1000\n"
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
  // The load range is too small.
  ASSERT_THROW(ParseAndPrepare(config), std::invalid_argument);
}

TEST(GeneratorConfigTest, InvalidLoadRange) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 10000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100000\n"
      "    range_max: 1000\n"
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
  // The load range min/max are swapped.
  ASSERT_THROW(ParseAndPrepare(config), std::invalid_argument);
}

TEST(GeneratorConfigTest, MissingScanLength) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 10000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  scan:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";
  ASSERT_ANY_THROW(ParseAndPrepare(config));
}

TEST(GeneratorConfigTest, InvalidScanLength) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 10000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  scan:\n"
      "    max_length: 0\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: uniform\n";
  ASSERT_THROW(ParseAndPrepare(config), std::invalid_argument);
}

TEST(GeneratorConfigTest, InvalidInserts) {
  const std::string missing_range_config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 10000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: uniform\n";
  ASSERT_ANY_THROW(ParseAndPrepare(missing_range_config));

  const std::string too_small_range_config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 10000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: uniform\n"
      "      range_min: 100\n"
      "      range_max: 1000\n";
  ASSERT_THROW(ParseAndPrepare(too_small_range_config), std::invalid_argument);

  const std::string invalid_distribution =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 10000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  insert:\n"
      "    proportion_pct: 100\n"
      "    distribution:\n"
      "      type: zipfian\n";
  ASSERT_THROW(ParseAndPrepare(invalid_distribution), std::invalid_argument);
}

TEST(GeneratorConfigTest, ValidDists) {
  const std::string config =
      "record_size_bytes: 16\n"
      "load:\n"
      "  num_records: 1000\n"
      "  distribution:\n"
      "    type: uniform\n"
      "    range_min: 100\n"
      "    range_max: 100000000\n"
      "run:\n"
      "- num_requests: 1000000\n"
      "  read:\n"
      "    proportion_pct: 20\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  update:\n"
      "    proportion_pct: 50\n"
      "    distribution:\n"
      "      type: zipfian\n"
      "      theta: 0.95\n"
      "  scan:\n"
      "    max_length: 100\n"
      "    proportion_pct: 20\n"
      "    distribution:\n"
      "      type: uniform\n"
      "  insert:\n"
      "    proportion_pct: 10\n"
      "    distribution:\n"
      "      type: hotspot\n"
      "      range_min: 10\n"
      "      range_max: 2000000\n"
      "      hot_proportion_pct: 90\n"
      "      hot_range_min: 10\n"
      "      hot_range_max: 500000\n";
  ASSERT_NO_THROW(ParseAndPrepare(config));
}

}  // namespace
