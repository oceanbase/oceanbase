/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_expand.h"

namespace oceanbase
{
namespace sql
{

class ExpandOpTest : public OpTestKit
{
};

// ============================================================================
// TC1: SingleCol_Rollup - ROLLUP(a), 3 input rows -> 6 output rows
// ============================================================================
TEST_F(ExpandOpTest, SingleCol_Rollup)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int")
      .select("a")
      .group_by_rollup("a")
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_EQ(6, result.row_count());
  // grouping_id=0: a active; grouping_id=1: a NULLed
  EXPECT_TRUE(result.verify_unordered({
      {1, 0}, {2, 0}, {3, 0},
      {NULL_VAL, 1}, {NULL_VAL, 1}, {NULL_VAL, 1}
  }));
}

// ============================================================================
// TC2: TwoCol_Rollup - ROLLUP(a, b), 3 input rows -> 9 output rows
// ============================================================================
TEST_F(ExpandOpTest, TwoCol_Rollup)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .group_by_rollup("a, b")
      .with_data({{1, 1}, {1, 2}, {2, 1}})
      .run(engine_);

  EXPECT_EQ(9, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      // grouping_id=0: a,b active
      {1, 1, 0},
      {1, 2, 0},
      {2, 1, 0},
      // grouping_id=1: b NULLed
      {1, NULL_VAL, 1},
      {1, NULL_VAL, 1},
      {2, NULL_VAL, 1},
      // grouping_id=2: a,b NULLed
      {NULL_VAL, NULL_VAL, 2},
      {NULL_VAL, NULL_VAL, 2},
      {NULL_VAL, NULL_VAL, 2}
  }));
}

// ============================================================================
// TC3: EmptyInput - 0 rows -> 0 output
// ============================================================================
TEST_F(ExpandOpTest, EmptyInput)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int")
      .select("a")
      .group_by_rollup("a")
      .with_data({})
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
  EXPECT_TRUE(result.verify_unordered({}));
}

// ============================================================================
// TC4: SingleRow - 1 input row + ROLLUP(a, b) -> 3 output rows
// ============================================================================
TEST_F(ExpandOpTest, SingleRow)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .group_by_rollup("a, b")
      .with_data({{1, 2}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      {1, 2, 0},
      {1, NULL_VAL, 1},
      {NULL_VAL, NULL_VAL, 2}
  }));
}

// ============================================================================
// TC5: SmallBatchSize - batch_size=2, same data as TC1
// ============================================================================
TEST_F(ExpandOpTest, SmallBatchSize)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int")
      .select("a")
      .group_by_rollup("a")
      .with_batch_size(2)
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_EQ(6, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      {1, 0},
      {2, 0},
      {3, 0},
      {NULL_VAL, 1},
      {NULL_VAL, 1},
      {NULL_VAL, 1}
  }));
}

// ============================================================================
// TC6: GroupingSets - explicit GROUPING SETS((a, b), (a), ())
// ============================================================================
TEST_F(ExpandOpTest, GroupingSets)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .group_by_grouping_sets("(a, b), (a), ()")
      .with_data({{1, 1}, {2, 2}})
      .run(engine_);

  EXPECT_EQ(6, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      // set 0: a,b active
      {1, 1, 0}, {2, 2, 0},
      // set 1: only a active, b NULLed
      {1, NULL_VAL, 1}, {2, NULL_VAL, 1},
      // set 2: a,b NULLed
      {NULL_VAL, NULL_VAL, 2}, {NULL_VAL, NULL_VAL, 2}
  }));
}

// ============================================================================
// TC7: GroupingSets_Asymmetric - GROUPING SETS((a), (b))
// ============================================================================
TEST_F(ExpandOpTest, GroupingSets_Asymmetric)
{
  OpTestResult result = ExpandTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .group_by_grouping_sets("(a), (b)")
      .with_data({{1, 10}, {2, 20}})
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      // set 0: a active, b NULLed
      {1, NULL_VAL, 0}, {2, NULL_VAL, 0},
      // set 1: b active, a NULLed
      {NULL_VAL, 10, 1}, {NULL_VAL, 20, 1}
  }));
}

// ============================================================================
// TC8: LargeData - 1000 rows, ROLLUP(a) -> 2000 output rows
// ============================================================================
TEST_F(ExpandOpTest, LargeData)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 1000; ++i) {
    data.push_back({i / 100});
  }

  // Build expected: each a_val produces 2 rows
  std::vector<TestRow> expected;
  for (int i = 0; i < 1000; ++i) {
    int a_val = i / 100;
    // grouping_id=0: a active
    expected.push_back({std::to_string(a_val), "0"});
    // grouping_id=1: a NULLed
    expected.push_back({"NULL", "1"});
  }

  OpTestResult result = ExpandTestSpec()
      .table("t", "a int")
      .select("a")
      .group_by_rollup("a")
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_EQ(2000, result.row_count());
  EXPECT_TRUE(result.verify_unordered(expected));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_expand_op.log*");
  // OB_LOGGER.set_file_name("test_expand_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
