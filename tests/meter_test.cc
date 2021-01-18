#include <chrono>
#include <stdexcept>

#include "gtest/gtest.h"
#include "ycsbr/ycsbr.h"

namespace {

using namespace ycsbr;

class MeterTest : public testing::Test {
 protected:
  MeterTest() {
    m_with_entries.Record(std::chrono::nanoseconds(10), 10);
    m_with_entries.Record(std::chrono::nanoseconds(1), 10);
    m_with_entries.Record(std::chrono::nanoseconds(2), 10);
    m_with_entries.Record(std::chrono::nanoseconds(2), 10);
    m_with_entries.Record(std::chrono::nanoseconds(3), 10);
    m_with_entries.Record(std::chrono::nanoseconds(9), 10);
    m_with_entries.Record(std::chrono::nanoseconds(8), 10);
  }

  Meter m_empty, m_with_entries;
};

TEST_F(MeterTest, LatencyMinMaxMean) {
  FrozenMeter with_entries(m_with_entries.Freeze());
  ASSERT_EQ(with_entries.LatencyMin<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(1));
  ASSERT_EQ(with_entries.LatencyMax<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(10));
  ASSERT_EQ(with_entries.LatencyMean<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(5));

  FrozenMeter empty(m_empty.Freeze());
  ASSERT_EQ(empty.LatencyMin<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(0));
  ASSERT_EQ(empty.LatencyMax<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(0));
  ASSERT_EQ(empty.LatencyMean<std::chrono::nanoseconds>(),
            std::chrono::nanoseconds(0));
}

TEST_F(MeterTest, LatencyPercentile) {
  FrozenMeter with_entries(m_with_entries.Freeze());
  ASSERT_EQ(with_entries.LatencyPercentile<std::chrono::nanoseconds>(0.5),
            std::chrono::nanoseconds(3));
  ASSERT_EQ(with_entries.LatencyPercentile<std::chrono::nanoseconds>(0.99),
            std::chrono::nanoseconds(10));
  ASSERT_EQ(with_entries.LatencyPercentile<std::chrono::nanoseconds>(1.0),
            std::chrono::nanoseconds(10));
  ASSERT_EQ(with_entries.LatencyPercentile<std::chrono::nanoseconds>(0.0),
            std::chrono::nanoseconds(1));
  ASSERT_THROW(with_entries.LatencyPercentile<std::chrono::nanoseconds>(1.1),
               std::invalid_argument);
  ASSERT_THROW(with_entries.LatencyPercentile<std::chrono::nanoseconds>(-0.1),
               std::invalid_argument);
}

TEST_F(MeterTest, OperationsBytes) {
  FrozenMeter with_entries(m_with_entries.Freeze());
  ASSERT_EQ(with_entries.NumOperations(), 7);
  ASSERT_EQ(with_entries.TotalBytes(), 70);
}

}  // namespace
