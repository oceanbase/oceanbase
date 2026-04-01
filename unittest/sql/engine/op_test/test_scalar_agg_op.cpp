/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_test/ob_op_test_kit.h"
#include "unittest/sql/engine/op_test/ob_op_test_scalar_aggregate.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

class ScalarAggOpTest : public OpTestKit
{
};

// TC1: COUNT(*)
TEST_F(ScalarAggOpTest, CountStar)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 5
  EXPECT_TRUE(result.verify_ordered({{"5"}}));
}

// TC2: SUM
TEST_F(ScalarAggOpTest, Sum)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(a)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // SUM(a) = 1+2+3+4+5 = 15
  EXPECT_TRUE(result.verify_ordered({{"15"}}));
}

// TC3: AVG
// TEST_F(ScalarAggOpTest, Avg)
// {
//   OpTestResult result = ScalarAggTestSpec()
//       .table("t", "a int")
//       .select("AVG(a)")
//       .with_data({{1}, {2}, {3}, {4}, {5}})
//       .run(engine_);

//   EXPECT_EQ(1, result.row_count());
//   // AVG(a) = 15/5 = 3
//   EXPECT_TRUE(result.verify_ordered({{"3"}}));
// }

// TC4: MIN/MAX
TEST_F(ScalarAggOpTest, MinMax)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MIN(a), MAX(a)")
      .with_data({{3}, {1}, {4}, {1}, {5}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // MIN(a) = 1, MAX(a) = 5
  EXPECT_TRUE(result.verify_ordered({{"1", "5"}}));
}

// TC5: Empty table (scalar agg returns 1 row)
TEST_F(ScalarAggOpTest, EmptyTable)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), SUM(a)")
      .with_data({})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 0, SUM(a) = NULL
  EXPECT_TRUE(result.verify_ordered({{"0", "NULL"}}));
}

// TC6: Multiple aggregates
TEST_F(ScalarAggOpTest, MultipleAggregates)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int, b int")
      .select("COUNT(*), SUM(a), MAX(b)")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 3, SUM(a) = 6, AVG(b) = 20
  EXPECT_TRUE(result.verify_ordered({{"3", "6", "30"}}));
}

// TC7: Aggregation with NULL values
TEST_F(ScalarAggOpTest, WithNullValues)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), COUNT(a), SUM(a)")
      .with_data({{1}, {TestValue::null()}, {3}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 3 (all rows), COUNT(a) = 2 (non-null), SUM(a) = 4
  EXPECT_TRUE(result.verify_ordered({{"3", "2", "4"}}));
}

// TC8: Large dataset
TEST_F(ScalarAggOpTest, LargeDataSet)
{
  std::vector<TestRow> data;
  for (int i = 1; i <= 100; ++i) {
    data.push_back({i});
  }

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), SUM(a)")
      .with_batch_size(10)
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 100, SUM(a) = 5050
  EXPECT_TRUE(result.verify_ordered({{"100", "5050"}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}