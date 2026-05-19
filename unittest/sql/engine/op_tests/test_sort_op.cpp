/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

class SortOpTest : public OpTestKit
{
};

TEST_F(SortOpTest, PrefixSortSkipFirstBatchRow)
{
  OpTestResult result = sort_test()
      .table("t", "a int, b int")
      .select("a, b")
      .order_by("a ASC, b ASC")
      .prefix_pos(1)
      // Input is already ordered by prefix column a, while b is intentionally unordered
      // inside each prefix. Prefix sort should sort b only among rows with the same a.
      .with_data({
          {0, 99},
          {1, 30}, {1, 10}, {1, 20},
          {2, 40}, {2, 15}, {2, 25},
          {3, 35}, {3, 5}, {3, 45}
      })
      .with_batch_size(4)
      .with_input_skips([](int64_t /*batch_idx*/, int64_t batch_size, ObBitVector *skip) {
        if (batch_size > 0) {
          skip->set(0);
        }
      })
      .enable_dual_format_check()
      .run(engine_);

  EXPECT_EQ(7, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"1", "10"}, {"1", "20"}, {"1", "30"},
      {"2", "15"}, {"2", "25"},
      {"3", "35"}, {"3", "45"}
  }));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sort_op.log*");
  common::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
