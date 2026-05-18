/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_hash_distinct.h"

namespace oceanbase
{
namespace sql
{

class HashDistinctOpTest : public OpTestKit
{
};

// ============================================================================
// TC1: AllColumnsDistinct_Basic - all select columns are distinct keys
// ============================================================================
TEST_F(HashDistinctOpTest, AllColumnsDistinct_Basic)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC2: WithAdditionalExprs - distinct on "a", b and c are additional
// ============================================================================
TEST_F(HashDistinctOpTest, WithAdditionalExprs)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, c")
      .distinct_by("a")
      .with_data({{1, 100, 1000}, {1, 100, 1000}, {2, 200, 2000}, {2, 200, 2000}, {3, 300, 3000}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 100, 1000}, {2, 200, 2000}, {3, 300, 3000}}));
}

// ============================================================================
// TC3: BlockMode_True - explicit block mode (default)
// ============================================================================
TEST_F(HashDistinctOpTest, BlockMode_True)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .block_mode(true)
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC4: NonBlockMode - non-block mode
// ============================================================================
TEST_F(HashDistinctOpTest, NonBlockMode)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .block_mode(false)
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC5: NoBypass - bypass disabled (default)
// ============================================================================
TEST_F(HashDistinctOpTest, NoBypass)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .enable_bypass(false)
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC6: BypassEnabled_LowNDV - low number of distinct values, bypass should work
// ============================================================================
TEST_F(HashDistinctOpTest, BypassEnabled_LowNDV)
{
  // Generate 10_000 rows with a in [0..4] uniformly distributed
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 5});  // a in [0..4]
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int")
      .select("a")
      .enable_bypass(true)  // auto: non-block + push_down
      .with_data(std::move(data))
      .run(engine_);

  // Should have 5 distinct values: 0, 1, 2, 3, 4
  EXPECT_TRUE(result.verify_unordered({{0}, {1}, {2}, {3}, {4}}));
}

// ============================================================================
// TC7: SpillToDisk - large data with spill enabled
// ============================================================================
TEST_F(HashDistinctOpTest, SpillToDisk)
{
  // Generate 10_000 unique a values, each repeated 5 times = 50_000 rows
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    for (int r = 0; r < 5; ++r) {
      data.push_back({i, std::string("val_" + std::to_string(i))});
    }
  }

  // Build expected: 10_000 unique rows
  std::vector<TestRow> expected;
  for (int i = 0; i < 10000; ++i) {
    expected.push_back({i, std::string("val_" + std::to_string(i))});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_sql_operator_dump(true)
      .with_hash_area_size(64 * 1024)  // 64KB to force spill
      .with_data(std::move(data))
      .enable_dual_format_unordered_check().run(engine_);

  EXPECT_EQ(10000, result.row_count());
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC8: DualFormat_Unordered - compare 1.0 and 2.0 results
// ============================================================================
TEST_F(HashDistinctOpTest, DualFormat_Unordered)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_unordered_check().run(engine_);

  // Dual format check already verified in run()
  EXPECT_EQ(3, result.row_count());
}

// ============================================================================
// TC9: GroupDistinct_Vec - Vec-only, multi-group distinct
// ============================================================================
// DISABLED: group_distinct_exprs_/group_sort_collations_ not available on this branch
TEST_F(HashDistinctOpTest, DISABLED_GroupDistinct_Vec)
{
  // Input: group 0 distinct on a, group 1 distinct on b
  // gid=0: a values 1, 2, 1 (distinct: 1, 2)
  // gid=1: b values 10, 20, 10 (distinct: 10, 20)
  // '-' filled with 0
  std::vector<TestRow> data = {
    {0, 1, 0},   // group 0: a=1
    {0, 2, 0},   // group 0: a=2
    {0, 1, 0},   // group 0: a=1 (duplicate)
    {1, 0, 10},  // group 1: b=10
    {1, 0, 20},  // group 1: b=20
    {1, 0, 10}   // group 1: b=10 (duplicate)
  };

  // Expected: 4 rows, each group outputs distinct values
  // group 0: (gid=0, a=1, b=NULL), (gid=0, a=2, b=NULL)
  // group 1: (gid=1, a=NULL, b=10), (gid=1, a=NULL, b=20)
  // Note: NULL is represented as ObObj null in verification

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "gid int, a int, b int")
      .select("gid, a, b")
      .with_rich_format(true)  // Vec-only
      .with_group_distinct({{"a"}, {"b"}}, "gid")
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_EQ(4, result.row_count());

  // group 0: distinct on a (b nulled); group 1: distinct on b (a nulled)
  std::vector<TestRow> expected = {
    {0, 1, NULL_VAL},
    {0, 2, NULL_VAL},
    {1, NULL_VAL, 10},
    {1, NULL_VAL, 20},
  };
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC10: SmallBatch - batch_size=3, cross-batch deduplication
// ============================================================================
TEST_F(HashDistinctOpTest, SmallBatch)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(3)
      .with_data({{1, 10}, {2, 20}, {1, 10}, {3, 30}, {2, 20}, {1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ============================================================================
// TC11: EmptyInput - no input rows
// ============================================================================
TEST_F(HashDistinctOpTest, EmptyInput)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// ============================================================================
// TC12: AllUnique - all rows are unique, no deduplication
// ============================================================================
TEST_F(HashDistinctOpTest, AllUnique)
{
  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}, {4, 40}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 20}, {3, 30}, {4, 40}}));
}

// ============================================================================
// TC13: MixedTypes_WithNull - NULL values, NULL==NULL treated as same key
// ============================================================================
TEST_F(HashDistinctOpTest, MixedTypes_WithNull)
{
  // NULL==NULL treated as same distinct key
  std::vector<TestRow> data = {
    {1, "x"}, {1, "x"},
    {NULL_VAL, "y"}, {NULL_VAL, "y"},
    {2, NULL_VAL}, {2, NULL_VAL},
    {NULL_VAL, NULL_VAL}, {NULL_VAL, NULL_VAL},
  };

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b varchar(16)")
      .select("a, b")
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(4, result.row_count());
  std::vector<TestRow> expected = {
    {1, "x"},
    {NULL_VAL, "y"},
    {2, NULL_VAL},
    {NULL_VAL, NULL_VAL},
  };
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC14a: Bypass_DefaultCutRatio - default cut_ratio=3
// ============================================================================
TEST_F(HashDistinctOpTest, Bypass_DefaultCutRatio)
{
  // 10_000 rows, a in [0..99] each with deterministic b = a*10
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100, (i % 100) * 10});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .distinct_by("a")
      .enable_bypass(true)
      .with_data(std::move(data))
      .run(engine_);

  std::vector<TestRow> expected;
  for (int i = 0; i < 100; ++i) {
    expected.push_back({i, i * 10});
  }
  EXPECT_EQ(100, result.row_count());
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC14b: Bypass_HighCutRatio - cut_ratio=20, threshold 95%
// ============================================================================
TEST_F(HashDistinctOpTest, Bypass_HighCutRatio)
{
  // 10_000 rows, a in [0..4] (5 distinct, 99.95% distinct rate > 95% threshold)
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 5});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int")
      .select("a")
      .enable_bypass(true)
      .with_session_vars({{"_groupby_nopushdown_cut_ratio", "20"}})
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{0}, {1}, {2}, {3}, {4}}));
}

// ============================================================================
// TC14c: Bypass_LowCutRatio_Clamped - cut_ratio=1, clamped to 3
// ============================================================================
TEST_F(HashDistinctOpTest, Bypass_LowCutRatio_Clamped)
{
  // 10_000 rows, a in [0..99]; cut_ratio=1 clamped to 3 internally
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int")
      .select("a")
      .enable_bypass(true)
      .with_session_vars({{"_groupby_nopushdown_cut_ratio", "1"}})
      .with_data(std::move(data))
      .run(engine_);

  std::vector<TestRow> expected;
  for (int i = 0; i < 100; ++i) {
    expected.push_back({i});
  }
  EXPECT_EQ(100, result.row_count());
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC15: BypassRescan_StableAcrossRuns - multiple rescan with bypass
// ============================================================================
TEST_F(HashDistinctOpTest, BypassRescan_StableAcrossRuns)
{
  // 5_000 rows, a in [0..9] (10 distinct)
  std::vector<TestRow> data;
  for (int i = 0; i < 5000; ++i) {
    data.push_back({i % 10});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int")
      .select("a")
      .enable_bypass(true)
      .with_rescan_times(3)
      .with_rescan_memory_check(true)
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_EQ(3, result.get_rescan_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
  std::vector<TestRow> expected;
  for (int i = 0; i < 10; ++i) {
    expected.push_back({i});
  }
  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC16: NonBlockRescan_StableAcrossRuns - non-block without bypass, rescan
// ============================================================================
TEST_F(HashDistinctOpTest, NonBlockRescan_StableAcrossRuns)
{
  // 5_000 rows, a in [1..100], b = a*10 (100 distinct pairs)
  std::vector<TestRow> data;
  for (int i = 0; i < 5000; ++i) {
    data.push_back({(i % 100) + 1, ((i % 100) + 1) * 10});
  }

  OpTestResult result = HashDistinctTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .block_mode(false)
      .with_rescan_times(3)
      .with_rescan_memory_check(true)
      .with_data(std::move(data))
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

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_hash_distinct_op.log*");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}