/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_distinct.h"

namespace oceanbase
{
namespace sql
{

class MergeDistinctOpTest : public OpTestKit
{
};

// ============================================================================
// TC1: AllColumnsDistinct_Basic - all select columns are distinct keys
// ============================================================================
TEST_F(MergeDistinctOpTest, AllColumnsDistinct_Basic)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({{1, 10}, {1, 10}, {2, 20}, {2, 20}, {3, 30}}, "a, b")
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC2: WithAdditionalExprs - distinct on "a", b and c are additional
// ============================================================================
TEST_F(MergeDistinctOpTest, WithAdditionalExprs)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, c")
      .distinct_by("a")
      .with_sorted_data({{1, 100, 1000}, {1, 100, 1000}, {2, 200, 2000}, {2, 200, 2000}, {3, 300, 3000}}, "a")
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 100, 1000}, {2, 200, 2000}, {3, 300, 3000}}));
}

// ============================================================================
// TC3: SmallBatch - batch_size=3, cross-batch deduplication
// ============================================================================
TEST_F(MergeDistinctOpTest, SmallBatch)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(3)
      .with_sorted_data({{1, 10}, {1, 10}, {2, 20}, {2, 20}, {3, 30}, {3, 30}}, "a, b")
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC4: EmptyInput - no input rows
// ============================================================================
TEST_F(MergeDistinctOpTest, EmptyInput)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({}, "a, b")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// ============================================================================
// TC5: AllUnique - all rows are unique, no deduplication
// ============================================================================
TEST_F(MergeDistinctOpTest, AllUnique)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({{1, 10}, {2, 20}, {3, 30}, {4, 40}}, "a, b")
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}, {4, 40}}));
}

// ============================================================================
// TC6: MixedTypes_WithNull - NULL values, NULL==NULL treated as same key
// ============================================================================
TEST_F(MergeDistinctOpTest, MixedTypes_WithNull)
{
  // Sorted input with NULLs; NULL==NULL treated as same distinct key.
  // Sort order (NULL LAST): non-null ints first, then NULLs.
  std::vector<TestRow> data = {
    {1, "x"}, {1, "x"},
    {2, NULL_VAL}, {2, NULL_VAL},
    {NULL_VAL, "y"}, {NULL_VAL, "y"},
    {NULL_VAL, NULL_VAL}, {NULL_VAL, NULL_VAL},
  };

  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b varchar(16)")
      .select("a, b")
      .with_sorted_data(std::move(data), "a, b")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(4, result.row_count());
  std::vector<TestRow> expected = {
    {1, "x"},
    {2, NULL_VAL},
    {NULL_VAL, "y"},
    {NULL_VAL, NULL_VAL},
  };
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC7: RescanStable - multiple rescan, memory consistency check
// ============================================================================
TEST_F(MergeDistinctOpTest, RescanStable)
{
  // 5_000 sorted rows, a in [1..100] each repeated 50 times
  std::vector<TestRow> data;
  for (int i = 1; i <= 100; ++i) {
    for (int r = 0; r < 50; ++r) {
      data.push_back({i, i * 10});
    }
  }

  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_rescan_times(3)
      .with_rescan_memory_check(true)
      .with_sorted_data(std::move(data), "a, b")
      .run(engine_);

  EXPECT_EQ(3, result.get_rescan_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
  std::vector<TestRow> expected;
  for (int i = 1; i <= 100; ++i) {
    expected.push_back({i, i * 10});
  }
  EXPECT_EQ(100, result.row_count());
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC8: DualFormat_Unordered - compare 1.0 and 2.0 results
// ============================================================================
TEST_F(MergeDistinctOpTest, DualFormat_Unordered)
{
  OpTestResult result = MergeDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({{1, 10}, {1, 10}, {2, 20}, {2, 20}, {3, 30}}, "a, b")
      .enable_dual_format_unordered_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_distinct_op.log*");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
