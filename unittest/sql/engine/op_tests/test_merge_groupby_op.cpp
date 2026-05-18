/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_groupby.h"

namespace oceanbase
{
namespace sql
{

class MergeGroupByOpTest : public OpTestKit
{
};

// ============================================================================
// TC1: SingleCol_CountStar - single column GROUP BY + COUNT(*)
// ============================================================================
TEST_F(MergeGroupByOpTest, SingleCol_CountStar)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_sorted_data({{1}, {1}, {2}, {2}, {3}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 2}, {2, 2}, {3, 1}}));
}

// ============================================================================
// TC2: SingleCol_Sum - single column + SUM
// ============================================================================
TEST_F(MergeGroupByOpTest, SingleCol_Sum)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, SUM(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 20}, {2, 30}, {2, 40}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 30}, {2, 70}}));
}

// ============================================================================
// TC3: SingleCol_MultiAggr - multiple aggregates
// ============================================================================
TEST_F(MergeGroupByOpTest, SingleCol_MultiAggr)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 3, 60, 10, 30}, {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC4: MultiCol_GroupBy - multi-column GROUP BY
// ============================================================================
TEST_F(MergeGroupByOpTest, MultiCol_GroupBy)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, SUM(c)")
      .group_by("a, b")
      .with_sorted_data({{1, 1, 10}, {1, 2, 20}, {2, 1, 30}}, "a ASC, b ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 1, 10}, {1, 2, 20}, {2, 1, 30}}));
}

// ============================================================================
// TC5: EmptyInput - empty table
// ============================================================================
TEST_F(MergeGroupByOpTest, EmptyInput)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
  EXPECT_TRUE(result.verify_ordered({}));
}

// ============================================================================
// TC6: SingleGroup - all rows same group
// ============================================================================
TEST_F(MergeGroupByOpTest, SingleGroup)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 20}, {1, 30}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 3, 60}}));
}

// ============================================================================
// TC7: AllDistinctGroups - every row is its own group
// ============================================================================
TEST_F(MergeGroupByOpTest, AllDistinctGroups)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {2, 20}, {3, 30}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 1, 10}, {2, 1, 20}, {3, 1, 30}}));
}

// ============================================================================
// TC8: LargeData - 10000 rows, 100 groups
// ============================================================================
TEST_F(MergeGroupByOpTest, LargeData)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i / 100, i});
  }

  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_sorted_data(std::move(data), "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(100, result.row_count());

  // Build expected rows: group g (g=0..99) has count=100,
  // b values are [g*100, g*100+99], sum = g*100*100 + (0+1+...+99) = 10000*g + 4950
  std::vector<TestRow> expected;
  for (int g = 0; g < 100; ++g) {
    int64_t sum_b = static_cast<int64_t>(g) * 100 * 100 +
                    static_cast<int64_t>(99) * 100 / 2;  // g*10000 + 4950
    expected.push_back({g, 100, sum_b});
  }
  EXPECT_TRUE(result.verify_ordered(expected));
}

// ============================================================================
// TC9: SmallBatchSize - small batch_size=3, same data as TC3
// ============================================================================
TEST_F(MergeGroupByOpTest, SmallBatchSize)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}}, "a ASC")
      .with_batch_size(3)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 3, 60, 10, 30}, {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC10: Rollup_SingleCol - GROUP BY a WITH ROLLUP + COUNT(*)
// Note: do NOT use enable_dual_format_check() for ROLLUP
// ============================================================================
TEST_F(MergeGroupByOpTest, Rollup_SingleCol)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_rollup()
      .with_sorted_data({{1}, {1}, {2}, {2}, {3}}, "a ASC")
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 2}, {2, 2}, {3, 1}, {NULL_VAL, 5}}));
}

// ============================================================================
// TC11: Rollup_MultiCol - GROUP BY a, b WITH ROLLUP + SUM(c)
// ============================================================================
TEST_F(MergeGroupByOpTest, Rollup_MultiCol)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, SUM(c)")
      .group_by("a, b")
      .with_rollup()
      .with_sorted_data({{1, 1, 10}, {1, 2, 20}, {2, 1, 30}}, "a ASC, b ASC")
      .run(engine_);

  EXPECT_EQ(6, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {1, 1, 10},
      {"1", "2", "20"},
      {1, NULL_VAL, 30},
      {2, 1, 30},
      {2, NULL_VAL, 30},
      {NULL_VAL, NULL_VAL, 60}
  }));
}

// ============================================================================
// TC12: DualFormatCheck - enable_dual_format_check on TC3-like data
// Checks that 1.0 and 2.0 produce identical results
// ============================================================================
TEST_F(MergeGroupByOpTest, DualFormatCheck)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 3, 60, 10, 30}, {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC13: CountDistinct - COUNT(DISTINCT b)
// ============================================================================
TEST_F(MergeGroupByOpTest, CountDistinct)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(DISTINCT b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 10}, {1, 20}, {2, 5}, {2, 5}, {2, 15}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  // a=1: distinct b values are {10, 20} -> count=2
  // a=2: distinct b values are {5, 15} -> count=2
  EXPECT_TRUE(result.verify_ordered({{1, 2}, {2, 2}}));
}

// ============================================================================
// TC14: SumDistinct - SUM(DISTINCT b)
// ============================================================================
TEST_F(MergeGroupByOpTest, SumDistinct)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, SUM(DISTINCT b)")
      .group_by("a")
      .with_sorted_data({{1, 10}, {1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 5}, {2, 15}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  // a=1: distinct b values are {10, 20, 30} -> sum=60
  // a=2: distinct b values are {5, 15} -> sum=20
  EXPECT_TRUE(result.verify_ordered({{1, 60}, {2, 20}}));
}

// ============================================================================
// TC15: MinVarchar - MIN(varchar_col)
// ============================================================================
TEST_F(MergeGroupByOpTest, MinVarchar)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "a int, b varchar(32)")
      .select("a, MIN(b)")
      .group_by("a")
      .with_sorted_data({{1, "banana"}, {1, "apple"}, {1, "cherry"}, {2, "zebra"}, {2, "ant"}}, "a ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  // a=1: min of {"banana", "apple", "cherry"} = "apple"
  // a=2: min of {"zebra", "ant"} = "ant"
  EXPECT_TRUE(result.verify_ordered({{1, "apple"}, {2, "ant"}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_groupby_op.log*");
  // OB_LOGGER.set_file_name("test_merge_groupby_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
