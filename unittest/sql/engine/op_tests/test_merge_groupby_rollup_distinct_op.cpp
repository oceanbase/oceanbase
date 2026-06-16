/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Regression: agg_reuse_cell.cpp dropped the `need_reuse_ |= has_extra_info(...)`
 * line, so SUM(DISTINCT) / AVG(DISTINCT) aggregates (which have no MIN/MAX/
 * GROUP_CONCAT/APPROX_COUNT_DISTINCT sibling to flip need_reuse_) stayed on
 * the simple_reuse_ path.  ObMergeGroupByVecOp::reuse_group() then MEMSET-ed
 * the agg_row, wiping the extra_idx_offset_ slot that holds the per-group
 * DISTINCT hash-set pointer — subsequent groups produced wrong (often NULL)
 * SUM(DISTINCT) values.
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_groupby.h"

namespace oceanbase
{
namespace sql
{

class MergeGroupByRollupDistinctTest : public OpTestKit
{
};

// Trigger requirements:
//   - aggregate has_distinct_ (so has_extra_info() returns true), and
//   - the aggregate is NOT one of MIN/MAX/GROUP_CONCAT/APPROX_COUNT_DISTINCT
//     (those set need_reuse_ unconditionally via DO_INIT_CELL), and
//   - MGB.reuse_group() is invoked, which happens when a batch is fully
//     drained while more groups remain — easiest to force with WITH ROLLUP
//     plus a small batch size so the output queue is drained in batches.
//
// Construction: 12 base groups (k=1..12), each with 3 rows.  Within a group
// `v` ranges over {10, 20, 30} — all distinct, so SUM(DISTINCT v)=60 per
// base group.  Cross-group `v` overlaps maximally so a leaked DISTINCT set
// would silently swallow values and SUM<60 (or NULL when the extra index is
// corrupted).
//
// Expected (rich-format / 2.0 vec MGB):
//   12 base rows  : (k, 60)    k=1..12
//   1  rollup row : (NULL, 60)        — overall DISTINCT v is still {10,20,30}
//   Total: 13 rows.
TEST_F(MergeGroupByRollupDistinctTest, MinimalReuseAggCellBug)
{
  std::vector<TestRow> data;
  data.reserve(36);
  for (int64_t k = 1; k <= 12; ++k) {
    data.push_back({k, static_cast<int64_t>(10)});
    data.push_back({k, static_cast<int64_t>(20)});
    data.push_back({k, static_cast<int64_t>(30)});
  }

  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "k bigint, v bigint")
      .select("k, SUM(DISTINCT v)")
      .group_by("k")
      .with_rollup()
      .with_sorted_data(std::move(data), "k ASC")
      .with_rich_format(true)
      .with_batch_size(4)
      .run(engine_);

  // 12 base groups + 1 rollup total
  EXPECT_EQ(13, result.row_count());

  // Build expected: (1, 60) ... (12, 60), (NULL, 60)
  std::vector<TestRow> expected;
  expected.reserve(13);
  for (int64_t k = 1; k <= 12; ++k) {
    expected.push_back({k, static_cast<int64_t>(60)});
  }
  expected.push_back({NULL_VAL, static_cast<int64_t>(60)});

  EXPECT_TRUE(result.verify_ordered(expected))
      << "SUM(DISTINCT v) must equal 60 for every group — leaked distinct-set "
         "across reuse_group() would lower the per-group sum.";
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_groupby_rollup_distinct_op.log*");
  // OB_LOGGER.set_file_name("test_merge_groupby_rollup_distinct_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
