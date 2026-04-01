/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_test/ob_op_test_kit.h"
#include "unittest/sql/engine/op_test/ob_op_test_limit.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

class LimitOpTest : public OpTestKit
{
};

// TC1: 基本 LIMIT
TEST_F(LimitOpTest, BasicLimit)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}})
      .limit(2)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}}));
}

// TC2: LIMIT + OFFSET
TEST_F(LimitOpTest, LimitWithOffset)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .limit(2)
      .offset(1)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"2"}, {"3"}}));
}

// TC3: LIMIT 0
TEST_F(LimitOpTest, LimitZero)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({{1}, {2}, {3}})
      .limit(0)
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// TC4: LIMIT 超过行数
TEST_F(LimitOpTest, LimitExceedsRowCount)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({{1}, {2}, {3}})
      .limit(10)
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// TC5: OFFSET 超过行数
TEST_F(LimitOpTest, OffsetExceedsRowCount)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({{1}, {2}, {3}})
      .limit(2)
      .offset(10)
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// TC6: ORDER BY + LIMIT (using pre-sorted data)
TEST_F(LimitOpTest, OrderByWithLimit)
{
  // Data pre-sorted by b DESC: (4,50), (5,40), (2,30), (3,20), (1,10)
  OpTestResult result = LimitTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({{4, 50}, {5, 40}, {2, 30}, {3, 20}, {1, 10}}, "b DESC")
      .limit(3)
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  // LIMIT 3 from sorted: (4,50), (5,40), (2,30)
  EXPECT_TRUE(result.verify_ordered({{"4", "50"}, {"5", "40"}, {"2", "30"}}));
}

// TC7: 多批次数据 + LIMIT
TEST_F(LimitOpTest, MultiBatchWithLimit)
{
  // Create 10 rows of data
  std::vector<TestRow> data;
  for (int i = 1; i <= 10; ++i) {
    data.push_back({i, i * 10});
  }

  OpTestResult result = LimitTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(3)  // Force multiple batches
      .with_data(std::move(data))
      .limit(4)
      .run(engine_);

  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", "40"}}));
}

// TC8: 空表 + LIMIT
TEST_F(LimitOpTest, EmptyTableWithLimit)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({})
      .limit(2)
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// TC9: 单行数据 + LIMIT
TEST_F(LimitOpTest, SingleRowWithLimit)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data({{42}})
      .limit(5)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"42"}}));
}

// TC10: 大数据集测试
TEST_F(LimitOpTest, LargeDataSet)
{
  // Create 100 rows of data
  std::vector<TestRow> data;
  for (int i = 1; i <= 100; ++i) {
    data.push_back({i});
  }

  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_data(std::move(data))
      .limit(10)
      .offset(20)
      .run(engine_);

  EXPECT_EQ(10, result.row_count());
  // Rows 21-30 (after offset 20)
  EXPECT_TRUE(result.verify_ordered({{"21"}, {"22"}, {"23"}, {"24"}, {"25"},
                                     {"26"}, {"27"}, {"28"}, {"29"}, {"30"}}));
}

// TC11: LIMIT with expression in SELECT
TEST_F(LimitOpTest, LimitWithExpression)
{
  OpTestResult result = LimitTestSpec()
      .table("t", "a int, b int")
      .select("a + b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .limit(2)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"11"}, {"22"}}));
}

// TC12: LIMIT + OFFSET + ORDER BY (using pre-sorted data)
TEST_F(LimitOpTest, LimitOffsetOrderBy)
{
  // Data pre-sorted by a DESC: 5, 4, 3, 2, 1
  OpTestResult result = LimitTestSpec()
      .table("t", "a int")
      .with_sorted_data({{5}, {4}, {3}, {2}, {1}}, "a DESC")
      .limit(2)
      .offset(1)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  // OFFSET 1: skip 5
  // LIMIT 2: 4, 3
  EXPECT_TRUE(result.verify_ordered({{"4"}, {"3"}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}