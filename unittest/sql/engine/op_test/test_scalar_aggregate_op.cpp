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

class ScalarAggregateOpTest : public OpTestKit
{
protected:
  // Helper to generate large test data
  std::vector<TestRow> generate_data(int64_t count, int64_t start = 1)
  {
    std::vector<TestRow> data;
    for (int64_t i = 0; i < count; ++i) {
      data.push_back({start + i});
    }
    return data;
  }

  // Helper to generate data with duplicates for DISTINCT tests
  std::vector<TestRow> generate_data_with_duplicates(int64_t unique_count, int64_t dup_per_value = 3)
  {
    std::vector<TestRow> data;
    for (int64_t i = 1; i <= unique_count; ++i) {
      for (int64_t j = 0; j < dup_per_value; ++j) {
        data.push_back({i});
      }
    }
    return data;
  }
};

// ===== Basic Aggregation Tests =====

// TC1: COUNT(*)
TEST_F(ScalarAggregateOpTest, CountStar)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"5"}}));
}

// TC2: SUM(int)
TEST_F(ScalarAggregateOpTest, SumInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(a)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"15"}}));
}

// TC3: AVG(int)
TEST_F(ScalarAggregateOpTest, AvgInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("AVG(a)")
      .with_data({{10}, {20}, {30}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // AVG(10, 20, 30) = 20
  EXPECT_TRUE(result.verify_ordered({{"20"}}));
}

// TC4: MIN(int_col)
TEST_F(ScalarAggregateOpTest, MinInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MIN(a)")
      .with_data({{5}, {3}, {8}, {1}, {9}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// TC5: MAX(string_col) - 字符串列最大值（字典序）
TEST_F(ScalarAggregateOpTest, MaxString)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "name varchar(32)")
      .select("MAX(name)")
      .with_data({TestRow{std::string("apple")},
                  TestRow{std::string("banana")},
                  TestRow{std::string("cherry")},
                  TestRow{std::string("date")}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // Max string by dictionary order: "date"
  EXPECT_TRUE(result.verify_ordered({{"date"}}));
}

// TC6: 多聚合函数组合
TEST_F(ScalarAggregateOpTest, MultipleAggregates)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int, b int")
      .select("COUNT(*), SUM(a), AVG(b), MIN(a), MAX(b)")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT=3, SUM(a)=6, AVG(b)=20, MIN(a)=1, MAX(b)=30
  EXPECT_TRUE(result.verify_ordered({{"3", "6", "20", "1", "30"}}));
}

// ===== DISTINCT Aggregation Tests =====

// TC7: COUNT(DISTINCT int)
TEST_F(ScalarAggregateOpTest, CountDistinctInt)
{
  // Data: 1,1,1,2,2,2,3,3,3 (3 unique values, each repeated 3 times)
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(DISTINCT a)")
      .with_data(generate_data_with_duplicates(3, 3))
      .enable_hash_base_distinct(true)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"3"}}));
}

// TC8: SUM(DISTINCT int)
TEST_F(ScalarAggregateOpTest, SumDistinctInt)
{
  // Data: 10, 10, 20, 20, 30, 30
  // DISTINCT: 10, 20, 30 -> SUM = 60
  std::vector<TestRow> data = {{10}, {10}, {20}, {20}, {30}, {30}};

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(DISTINCT a)")
      .with_data(std::move(data))
      .enable_hash_base_distinct(true)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"60"}}));
}

// TC9: SUM(DISTINCT decimal)
TEST_F(ScalarAggregateOpTest, SumDistinctDecimal)
{
  // Using DECIMAL type for prices
  // DISTINCT prices: 10.5, 20.5, 30.5 -> SUM = 61.5
  std::vector<TestRow> data = {{10.5}, {10.5}, {20.5}, {20.5}, {30.5}};

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "price decimal(10,2)")
      .select("SUM(DISTINCT price)")
      .with_data(std::move(data))
      .enable_hash_base_distinct(true)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
}

// TC10: COUNT(DISTINCT str_col)
TEST_F(ScalarAggregateOpTest, CountDistinctString)
{
  std::vector<TestRow> data = {
    TestRow{std::string("apple")},
    TestRow{std::string("apple")},
    TestRow{std::string("banana")},
    TestRow{std::string("banana")},
    TestRow{std::string("cherry")}
  };

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "name varchar(32)")
      .select("COUNT(DISTINCT name)")
      .with_data(std::move(data))
      .enable_hash_base_distinct(true)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"3"}}));
}

// ===== Empty Set Behavior Tests =====

// TC11: 空表 + COUNT(*)
TEST_F(ScalarAggregateOpTest, EmptyTableCount)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*)")
      .with_data({})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"0"}}));
}

// TC12: 空表 + SUM
TEST_F(ScalarAggregateOpTest, EmptyTableSum)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(a)")
      .with_data({})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // SUM on empty set returns NULL (represented as "NULL" string)
  EXPECT_EQ("NULL", result.get_row(0)[0]);
}

// TC13: 空表 + 多聚合函数
TEST_F(ScalarAggregateOpTest, EmptyTableMultipleAggregates)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a)")
      .with_data({})
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 0, others are NULL
  const auto &row = result.get_row(0);
  EXPECT_EQ("0", row[0]);         // COUNT
  EXPECT_EQ("NULL", row[1]);      // SUM
  EXPECT_EQ("NULL", row[2]);      // AVG
  EXPECT_EQ("NULL", row[3]);      // MIN
  EXPECT_EQ("NULL", row[4]);      // MAX
}

// ===== Large Data Set Tests =====

// TC14: 大数据集 - COUNT (1000+ rows)
TEST_F(ScalarAggregateOpTest, LargeDataSetCount)
{
  const int64_t row_count = 1000;

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*)")
      .with_data(generate_data(row_count))
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1000"}}));
}

// TC15: 大数据集 - SUM
TEST_F(ScalarAggregateOpTest, LargeDataSetSum)
{
  const int64_t row_count = 1000;
  // Sum of 1 to 1000 = 1000 * 1001 / 2 = 500500

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(a)")
      .with_data(generate_data(row_count))
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"500500"}}));
}

// TC16: 大数据集 - MIN/MAX
TEST_F(ScalarAggregateOpTest, LargeDataSetMinMax)
{
  const int64_t row_count = 1000;

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MIN(a), MAX(a)")
      .with_data(generate_data(row_count))
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "1000"}}));
}

// TC17: 大数据集 + DISTINCT
TEST_F(ScalarAggregateOpTest, LargeDataSetDistinct)
{
  // 100 unique values, each duplicated 10 times = 1000 rows
  std::vector<TestRow> data;
  for (int i = 1; i <= 100; ++i) {
    for (int j = 0; j < 10; ++j) {
      data.push_back({i});
    }
  }

  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(DISTINCT a)")
      .with_data(std::move(data))
      .enable_hash_base_distinct(true)
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"100"}}));
}

// TC18: 多批次数据处理
TEST_F(ScalarAggregateOpTest, MultiBatchProcessing)
{
  // 100 rows with batch size 10
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), SUM(a)")
      .with_batch_size(10)
      .with_data(generate_data(100))
      .run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT=100, SUM=5050
  EXPECT_TRUE(result.verify_ordered({{"100", "5050"}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}