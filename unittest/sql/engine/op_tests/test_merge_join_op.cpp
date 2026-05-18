/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "ob_op_test_types.h"
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_join.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief Test fixture for Merge Join operator tests.
 */
class MergeJoinOpTest : public OpTestKit
{
protected:
  void SetUp() override
  {
    OpTestKit::SetUp();
  }

  void TearDown() override
  {
    OpTestKit::TearDown();
  }

  // Helper to check if result contains expected row (unordered)
  bool contains_row(const OpTestResult &result, const TestRow &expected)
  {
    std::vector<std::string> expected_str;
    expected_str.reserve(expected.size());
    for (const auto &val : expected) {
      expected_str.push_back(val.to_string());
    }
    for (int64_t i = 0; i < result.row_count(); ++i) {
      if (result.get_row(i) == expected_str) {
        return true;
      }
    }
    return false;
  }

  // Helper to check if result has a row matching a predicate
  bool row_matches(const OpTestResult &result, int col_idx, std::function<bool(const std::string &)> pred)
  {
    for (int64_t i = 0; i < result.row_count(); ++i) {
      const auto &row = result.get_row(i);
      if (col_idx < static_cast<int64_t>(row.size()) && pred(row[col_idx])) {
        return true;
      }
    }
    return false;
  }

  // Helper to verify that a specific column has NULL for a matching value in another column
  bool verify_null_column_for_value(const OpTestResult &result, int match_col, const std::string &match_val,
                                     int null_col)
  {
    for (int64_t i = 0; i < result.row_count(); ++i) {
      const auto &row = result.get_row(i);
      if (match_col < static_cast<int64_t>(row.size()) &&
          row[match_col] == match_val &&
          null_col < static_cast<int64_t>(row.size())) {
        // Check if the null_col is NULL (represented as "NULL" string)
        return row[null_col] == "NULL";
      }
    }
    return false;
  }
};

// ===== T1: Basic Inner Join =====

TEST_F(MergeJoinOpTest, BasicInnerJoin)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {3, 300}, {5, 500}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  // Result order may vary, check unordered
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 1, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({3, 30, 3, 300})));
}

// ===== T2: Left Outer Join =====

TEST_F(MergeJoinOpTest, LeftOuterJoin)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {3, 300}}, "c ASC")
      .join_type(LEFT_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  // Check for matched rows
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 1, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({3, 30, 3, 300})));
  EXPECT_TRUE(contains_row(result, TestRow({2, 20, TestValue::null(), TestValue::null()})));
  // Check for unmatched left row (right side NULL)
  bool found_unmatched = false;
  for (int64_t i = 0; i < result.row_count(); ++i) {
    const auto &row = result.get_row(i);
    if (row[0] == "2") {
      EXPECT_EQ(row[2], "NULL");  // t2.c is NULL
      EXPECT_EQ(row[3], "NULL");  // t2.d is NULL
      found_unmatched = true;
    }
  }
  EXPECT_TRUE(found_unmatched);
}

// ===== T3: Right Outer Join =====

TEST_F(MergeJoinOpTest, RightOuterJoin)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {2, 200}, {3, 300}}, "c ASC")
      .join_type(RIGHT_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  // Check for matched rows
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 1, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({3, 30, 3, 300})));
  EXPECT_TRUE(contains_row(result, TestRow({TestValue::null(), TestValue::null(), 2, 200})));
  // Check for unmatched right row (left side NULL)
  bool found_unmatched = false;
  for (int64_t i = 0; i < result.row_count(); ++i) {
    const auto &row = result.get_row(i);
    if (row[2] == "2") {
      EXPECT_EQ(row[0], "NULL");  // t1.a is NULL
      EXPECT_EQ(row[1], "NULL");  // t1.b is NULL
      found_unmatched = true;
    }
  }
  EXPECT_TRUE(found_unmatched);
}

// ===== T4: Full Outer Join =====

TEST_F(MergeJoinOpTest, FullOuterJoin)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}}, "a ASC")
      .with_right_sorted_data({{2, 200}, {3, 300}}, "c ASC")
      .join_type(FULL_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  // Check for unmatched left row (a=1)
  bool found_left_unmatched = false;
  for (int64_t i = 0; i < result.row_count(); ++i) {
    const auto &row = result.get_row(i);
    if (row[0] == "1") {
      EXPECT_EQ(row[2], "NULL");
      found_left_unmatched = true;
    }
  }
  EXPECT_TRUE(found_left_unmatched);

  // Check for matched row (a=2, c=2)
  EXPECT_TRUE(contains_row(result, TestRow({2, 20, 2, 200})));
  EXPECT_TRUE(contains_row(result, TestRow({TestValue::null(), TestValue::null(), 3, 300})));
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, TestValue::null(), TestValue::null()})));

  // Check for unmatched right row (c=3)
  bool found_right_unmatched = false;
  for (int64_t i = 0; i < result.row_count(); ++i) {
    const auto &row = result.get_row(i);
    if (row[2] == "3") {
      EXPECT_EQ(row[0], "NULL");
      found_right_unmatched = true;
    }
  }
  EXPECT_TRUE(found_right_unmatched);
}

// ===== T5: Multi-Key Join =====

TEST_F(MergeJoinOpTest, MultiKeyJoin)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int, v1 int")
      .right_table("t2", "c int, d int, v2 int")
      .select("t1.a, t1.b, t2.v2")
      .with_left_sorted_data({{1, 1, 10}, {1, 2, 20}, {2, 1, 30}}, "a ASC, b ASC")
      .with_right_sorted_data({{1, 2, 200}, {2, 1, 300}, {2, 2, 400}}, "c ASC, d ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c AND t1.b = t2.d")
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(contains_row(result, TestRow({1, 2, 200})));
  EXPECT_TRUE(contains_row(result, TestRow({2, 1, 300})));
}

// ===== T6: Empty Result =====

TEST_F(MergeJoinOpTest, EmptyResult)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int")
      .right_table("t2", "c int")
      .select("t1.a, t2.c")
      .with_left_sorted_data({{1}, {2}, {3}}, "a ASC")
      .with_right_sorted_data({{4}, {5}, {6}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// ===== T7: NULL Handling =====

TEST_F(MergeJoinOpTest, NullHandling)
{
  // When NULL values appear in the join key column, the merge join uses
  // row_null_last_cmp_ which always treats NULL as last, regardless of sort order.
  // This test verifies that matching rows are produced correctly even with NULLs
  // in non-key columns.
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {2, TestValue::null()}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {2, TestValue::null()}, {3, 300}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  // All three rows match since join keys (a, c) have no NULLs
  // NULLs in non-key columns are just data
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 1, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({2, TestValue::null(), 2, TestValue::null()})));
  EXPECT_TRUE(contains_row(result, TestRow({3, 30, 3, 300})));
}

// ===== T8: Duplicate Keys (Cartesian Product within groups) =====

TEST_F(MergeJoinOpTest, DuplicateKeys)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.d")
      .with_left_sorted_data({{1, 10}, {1, 20}, {2, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {1, 200}, {2, 300}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  // a=1: 2x2=4 rows, a=2: 1x1=1 row, total 5 rows
  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({1, 10, 200})));
  EXPECT_TRUE(contains_row(result, TestRow({1, 20, 100})));
  EXPECT_TRUE(contains_row(result, TestRow({1, 20, 200})));
  EXPECT_TRUE(contains_row(result, TestRow({2, 30, 300})));
}

// ===== T9: DESC Sort Direction =====

TEST_F(MergeJoinOpTest, DescSortDirection)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t2.d")
      .with_left_sorted_data({{3, 30}, {2, 20}, {1, 10}}, "a DESC")
      .with_right_sorted_data({{3, 300}, {1, 100}}, "c DESC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{3, 300}, {1, 100}}));
}

// ===== T10: Rescan Test =====

TEST_F(MergeJoinOpTest, Rescan)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {2, 200}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .with_rescan_times(3)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 100}, {2, 200}}));
}

// ===== T11: Large Data Inner Join =====

TEST_F(MergeJoinOpTest, LargeDataInnerJoin)
{
  // Generate 10000 rows for each table, all matching
  const int64_t row_count = 10000;

  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data_generator(row_count,
          gen::sequential(0),    // a: 0, 1, 2, ..., 9999 (already sorted)
          gen::random_int(1, 10000, 1))
      .with_right_data_generator(row_count,
          gen::sequential(0),    // c: 0, 1, 2, ..., 9999 (already sorted)
          gen::random_int(1, 10000, 2))
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_EQ(row_count, result.row_count());
  EXPECT_TRUE(result.verify_ordered(row_count, gen::sequential(0),
                        gen::random_int(1, 10000, 1),
                        gen::sequential(0),
                        gen::random_int(1, 10000, 2)));
}

// ===== T12: Large Data Partial Match =====

TEST_F(MergeJoinOpTest, LargeDataPartialMatch)
{
  // Left table: even numbers 0, 2, 4, ..., 19998
  // Right table: multiples of 3: 0, 3, 6, ..., 29997
  // Common: multiples of 6: 0, 6, 12, ..., 19998 -> 3334 rows
  const int64_t left_count = 10000;
  const int64_t right_count = 10000;

  auto gen0 = gen::random_int(1, 100, 1);
  auto gen1 = gen::random_int(1, 100, 2);

  auto wrap_gen0 = [&](int64_t i) -> TestValue {
    if (i >= 1) {
      gen0((i - 1) * 3 + 1);
      gen0((i - 1) * 3 + 2);
      return gen0(i * 3);
    } else {
      return gen0(i);
    }
  };
  auto wrap_gen1 = [&](int64_t i) -> TestValue {
    if (i >= 1) {
      gen1((i - 1) * 2 + 1);
      return gen1(i * 2);
    } else {
      return gen1(i);
    }
  };

  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.d")
      .with_left_data_generator(left_count,
          [](int64_t i) -> TestValue { return static_cast<int>(i * 2); },  // even
          gen::random_int(1, 100, 1))
      .with_right_data_generator(right_count,
          [](int64_t i) -> TestValue { return static_cast<int>(i * 3); },  // multiples of 3
          gen::random_int(1, 100, 2))
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  // LCM(2,3) = 6, so matches are 0, 6, 12, ..., 19998
  // 19998 / 6 + 1 = 3334 rows
  EXPECT_EQ(3334, result.row_count());
  EXPECT_TRUE(result.verify_ordered(3334,
    [](int64_t i) -> TestValue { return static_cast<int>(i * 6); },
    wrap_gen0,
    wrap_gen1));
}

// ===== T13: Batch Size Boundary =====

TEST_F(MergeJoinOpTest, BatchSizeBoundary)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {2, 200}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .with_batch_size(7)  // Prime number batch size
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered(
    {{1, 100}, {2, 200}}
  ));
}

// ===== T14: Dual Format Check =====

TEST_F(MergeJoinOpTest, DualFormatCheck)
{
  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {3, 300}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .enable_dual_format_check()
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered(
    {{1, 10, 1, 100}, {3, 30, 3, 300}}
  ));
}

// ===== T15: NULL Safe Equal Join (<=> operator) =====

TEST_F(MergeJoinOpTest, NullSafeEqualJoin)
{
  // Note: NULL-safe equal (<=>) joins NULL = NULL as true
  // This test may require special handling in the framework
  // For now, test with regular equals which treats NULL != NULL

  OpTestResult result = MergeJoinTestSpec()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_sorted_data({{1, 10}, {TestValue::null(), 20}}, "a ASC")
      .with_right_sorted_data({{1, 100}, {TestValue::null(), 200}}, "c ASC")
      .join_type(INNER_JOIN)
      .on("t1.a <=> t2.c")
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered(
    {{1, 10, 1, 100}, {TestValue::null(), 20, TestValue::null(), 200}}
  ));
}

// ===== Main =====

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // run FullOuterJoin only
  // testing::GTEST_FLAG(filter) = "MergeJoinOpTest.DualFormatCheck";
  return RUN_ALL_TESTS();
}