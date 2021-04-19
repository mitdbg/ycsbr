#include "ycsbr/gen/keyrange.h"

#include "gtest/gtest.h"

namespace {

using namespace ycsbr::gen;

TEST(KeyRangeTest, Size) {
  KeyRange range1(1, 100), range2(100, 100);
  ASSERT_EQ(range1.size(), 100);
  ASSERT_EQ(range2.size(), 1);
}

TEST(KeyRangeTest, Contains) {
  KeyRange range1(0, 100), range2(100, 200);
  ASSERT_TRUE(range1.Contains(range1));
  ASSERT_FALSE(range1.Contains(range2));
  ASSERT_FALSE(range2.Contains(range1));

  KeyRange small(1, 1);
  ASSERT_TRUE(range1.Contains(small));
  ASSERT_FALSE(small.Contains(range1));

  KeyRange left(0, 10), right(90, 100);
  ASSERT_TRUE(range1.Contains(left));
  ASSERT_TRUE(range1.Contains(right));
  ASSERT_FALSE(range2.Contains(right));
}

TEST(KeyRangeTest, SubtractContained) {
  KeyRange overall1(0, 100), inner_left(0, 10);
  auto result = overall1.SubtractContained(inner_left);
  ASSERT_FALSE(result.first.has_value());
  ASSERT_TRUE(result.second.has_value());
  ASSERT_EQ(result.second->min(), 11);
  ASSERT_EQ(result.second->max(), 100);

  KeyRange inner_right(90, 100);
  result = overall1.SubtractContained(inner_right);
  ASSERT_TRUE(result.first.has_value());
  ASSERT_FALSE(result.second.has_value());
  ASSERT_EQ(result.first->min(), 0);
  ASSERT_EQ(result.first->max(), 89);

  // Nothing left.
  result = overall1.SubtractContained(overall1);
  ASSERT_FALSE(result.first.has_value());
  ASSERT_FALSE(result.second.has_value());

  // Check behavior at limits.
  KeyRange entire(0, std::numeric_limits<ycsbr::Request::Key>::max());
  result = entire.SubtractContained(entire);
  ASSERT_FALSE(result.first.has_value());
  ASSERT_FALSE(result.second.has_value());

  KeyRange middle(100, 200);
  result = entire.SubtractContained(middle);
  ASSERT_TRUE(result.first.has_value());
  ASSERT_TRUE(result.second.has_value());
  ASSERT_EQ(result.first->min(), 0);
  ASSERT_EQ(result.first->max(), 99);
  ASSERT_EQ(result.second->min(), 201);
  ASSERT_EQ(result.second->max(),
            std::numeric_limits<ycsbr::Request::Key>::max());

  // Check behavior at right limit.
  KeyRange right_max(101, std::numeric_limits<ycsbr::Request::Key>::max());
  result = entire.SubtractContained(right_max);
  ASSERT_TRUE(result.first.has_value());
  ASSERT_FALSE(result.second.has_value());
  ASSERT_EQ(result.first->min(), 0);
  ASSERT_EQ(result.first->max(), 100);
}

}  // namespace
