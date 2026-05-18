/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_hash_groupby.h"

namespace oceanbase
{
namespace sql
{

class HashGroupByOpTest : public OpTestKit
{
};

// ============================================================================
// TC1: SingleCol_CountStar - unordered output
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_CountStar)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_data({{2}, {1}, {2}, {1}, {3}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 2}, {2, 2}, {3, 1}}));
}

// ============================================================================
// TC2: SingleCol_Sum
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_Sum)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 30}, {1, 20}, {2, 40}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 30}, {2, 70}}));
}

// ============================================================================
// TC3: SingleCol_MultiAggr
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_MultiAggr)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC4: MultiCol_GroupBy
// ============================================================================
TEST_F(HashGroupByOpTest, MultiCol_GroupBy)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, SUM(c)")
      .group_by("a, b")
      .with_data({{1, 2, 20}, {2, 1, 30}, {1, 1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 1, 10},
                                       {1, 2, 20},
                                       {2, 1, 30}}));
}

// ============================================================================
// TC5: EmptyInput
// ============================================================================
TEST_F(HashGroupByOpTest, EmptyInput)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
  EXPECT_TRUE(result.verify_unordered({}));
}

// ============================================================================
// TC6: SingleGroup
// ============================================================================
TEST_F(HashGroupByOpTest, SingleGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60}}));
}

// ============================================================================
// TC7: AllDistinctGroups
// ============================================================================
TEST_F(HashGroupByOpTest, AllDistinctGroups)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 1, 10},
                                       {2, 1, 20},
                                       {3, 1, 30}}));
}

// ============================================================================
// TC8: LargeData - 10000 rows, 100 groups
// ============================================================================
TEST_F(HashGroupByOpTest, LargeData)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100, i});
  }

  // Build expected: for group g, b values are g, g+100, g+200, ..., g+9900
  // sum = 100*g + (0+100+200+...+9900) = 100*g + 100*(0+1+...+99) = 100*(g + 4950)
  std::vector<TestRow> expected;
  for (int g = 0; g < 100; ++g) {
    int64_t count = 100;
    int64_t sum = 100LL * (g + 4950);
    expected.push_back({g, count, sum});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC9: SmallBatchSize - batch_size=3, same data as TC3
// ============================================================================
TEST_F(HashGroupByOpTest, SmallBatchSize)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_batch_size(3)
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC10: DualFormatCheck - same data as TC3, explicit dual format check
// ============================================================================
TEST_F(HashGroupByOpTest, DualFormatCheck)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC11: Avg - single column GROUP BY + AVG(b)
// Group a=1: b values 10,20,30 -> avg=20
// Group a=2: b values 40,50    -> avg=45
// ============================================================================
TEST_F(HashGroupByOpTest, Avg)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, AVG(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 40}, {1, 20}, {2, 50}, {1, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 20}, {2, 45}}));
}

// ============================================================================
// TC12: Dump - large data with dump enabled
// Same 10000 rows / 100 groups as TC8
// ============================================================================
TEST_F(HashGroupByOpTest, Dump)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100, i});
  }

  std::vector<TestRow> expected;
  for (int g = 0; g < 100; ++g) {
    int64_t count = 100;
    int64_t sum = 100LL * (g + 4950);
    expected.push_back({g, count, sum});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_sql_operator_dump(true)
      .with_hash_area_size(64 * 1024)
      .with_data(std::move(data))
      .enable_dual_format_unordered_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC13: LimitPushdown - limit pushed into hash gby (2.0 only)
// 1000 rows, 20 groups; LIMIT 5 -> at most 5 groups returned
// ============================================================================
// DISABLED: limit_expr_ not available on this branch
TEST_F(HashGroupByOpTest, DISABLED_LimitPushdown)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 1000; ++i) {
    data.push_back({i % 20, i});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_limit(5)
      .with_rich_format(true)
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_LE(result.row_count(), 5);
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_hash_groupby_op.log*");
  // OB_LOGGER.set_file_name("test_hash_groupby_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
