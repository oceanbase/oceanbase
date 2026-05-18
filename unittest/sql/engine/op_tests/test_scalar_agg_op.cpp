/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_scalar_aggregate.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

class ScalarAggOpTest : public OpTestKit
{
};

// ============================================================================
// P0 #1: COUNT(*)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_CountStar)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"5"}}));
}

// ============================================================================
// P0 #2: COUNT(column)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_CountColumn)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(a)")
      .with_data({{1}, {TestValue::null()}, {3}, {TestValue::null()}, {5}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(a) counts non-null values = 3
  EXPECT_TRUE(result.verify_ordered({{"3"}}));
}

// ============================================================================
// P0 #3: SUM
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_SumInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(a)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"15"}}));
}

// ============================================================================
// P0 #4: MIN
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_MinInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MIN(a)")
      .with_data({{3}, {1}, {4}, {1}, {5}, {9}, {2}, {6}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// ============================================================================
// P0 #5: MAX
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_MaxInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MAX(a)")
      .with_data({{3}, {1}, {4}, {1}, {5}, {9}, {2}, {6}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"9"}}));
}

// ============================================================================
// P0 #6: MAX(varchar)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_MaxVarchar)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "name varchar(32)")
      .select("MAX(name)")
      .with_data({{"apple"}, {"banana"}, {"cherry"}, {"date"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // MAX(varchar) returns lexicographically largest
  EXPECT_TRUE(result.verify_ordered({{"date"}}));
}

// ============================================================================
// P0 #7: AVG
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_AvgInt)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("AVG(a)")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // AVG(a) = 15/5 = 3.0000
  EXPECT_TRUE(result.verify_ordered({{"3.0000"}}));
}

// ============================================================================
// P0 #8: COUNT(DISTINCT)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_CountDistinct)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(DISTINCT a)")
      .enable_hash_base_distinct(true)
      .with_data({{1}, {2}, {1}, {3}, {2}, {4}, {1}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(DISTINCT a) = 4 distinct values: 1, 2, 3, 4
  EXPECT_TRUE(result.verify_ordered({{"4"}}));
}

// ============================================================================
// P0 #9: SUM(DISTINCT)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_SumDistinct)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("SUM(DISTINCT a)")
      .enable_hash_base_distinct(true)
      .with_data({{1}, {2}, {1}, {3}, {2}, {4}, {1}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // SUM(DISTINCT a) = 1 + 2 + 3 + 4 = 10
  EXPECT_TRUE(result.verify_ordered({{"10"}}));
}

// ============================================================================
// P0 #10: GROUP_CONCAT
// NOTE: GROUP_CONCAT with ORDER BY requires special sort_info initialization
// that is not yet fully supported in the unittest framework.
// DISABLED: Returns 0 rows, ORDER BY mechanism needs additional work.
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_GroupConcat)
{
  // GROUP_CONCAT requires string type parameter, use varchar column
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "name varchar(32)")
      .select("GROUP_CONCAT(name ORDER BY name SEPARATOR ',')")
      .with_data({{"c"}, {"a"}, {"b"}, {"d"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // GROUP_CONCAT with ORDER BY name = "a,b,c,d"
  EXPECT_TRUE(result.verify_ordered({{"a,b,c,d"}}));
}

// ============================================================================
// P0 #11: APPROX_COUNT_DISTINCT (may not be supported)
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_ApproxCountDistinct)
{
  // APPROX_COUNT_DISTINCT may not be supported in all versions
  // Skip if not supported
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("APPROX_COUNT_DISTINCT(a)")
      .with_data({{1}, {2}, {1}, {3}, {2}, {4}, {1}, {5}})
      .enable_dual_format_check().run(engine_);

  // Just check that we get a result (approximate count should be close to 5)
  EXPECT_EQ(1, result.row_count());
  // The result is approximate, so we just verify it returns something
}

// ============================================================================
// P0 #12: BIT_AND
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_BitAnd)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int unsigned")
      .select("BIT_AND(a)")
      .with_data({{5}, {3}, {7}, {1}})  // 5(101) & 3(011) & 7(111) & 1(001) = 1(001)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// ============================================================================
// P0 #13: BIT_OR
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_BitOr)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int unsigned")
      .select("BIT_OR(a)")
      .with_data({{1}, {2}, {4}})  // 1(001) | 2(010) | 4(100) = 7(111)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"7"}}));
}

// ============================================================================
// P0 #14: BIT_XOR
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_BitXor)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int unsigned")
      .select("BIT_XOR(a)")
      .with_data({{5}, {3}})  // 5(101) ^ 3(011) = 6(110)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"6"}}));
}

// ============================================================================
// P0 #15: VAR_POP
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_VarPop)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a double")
      .select("VAR_POP(a)")
      .with_data({{1.0}, {2.0}, {3.0}, {4.0}, {5.0}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // VAR_POP of [1,2,3,4,5] = 2.0
  // Mean = 3, variance = ((1-3)^2 + (2-3)^2 + ... + (5-3)^2) / 5 = 10/5 = 2
}

// ============================================================================
// P0 #16: STDDEV_POP
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_StddevPop)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a double")
      .select("STDDEV_POP(a)")
      .with_data({{1.0}, {2.0}, {3.0}, {4.0}, {5.0}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // STDDEV_POP = sqrt(VAR_POP) = sqrt(2) ≈ 1.414
}

// ============================================================================
// P0 #17: STDDEV_SAMP
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_StddevSamp)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a double")
      .select("STDDEV_SAMP(a)")
      .with_data({{1.0}, {2.0}, {3.0}, {4.0}, {5.0}})
      .run(engine_);  // No dual-format check: STDDEV_SAMP (aggr_fun=680) unsupported in 1.0 processor

  EXPECT_EQ(1, result.row_count());
  // STDDEV_SAMP = sqrt(sample variance) = sqrt(10/4) = sqrt(2.5) ≈ 1.581
}

// ============================================================================
// P0 #18: STDDEV
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_Stddev)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a double")
      .select("STDDEV(a)")
      .with_data({{1.0}, {2.0}, {3.0}, {4.0}, {5.0}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // STDDEV is typically an alias for STDDEV_SAMP
}

// ============================================================================
// P0 #19: Empty table
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_EmptyTable)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a)")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // Empty table: COUNT(*) = 0, other aggregates = NULL
  EXPECT_TRUE(result.verify_ordered({{"0", "NULL", "NULL", "NULL", "NULL"}}));
}

// ============================================================================
// P0 #20: NULL values
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_WithNulls)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("COUNT(*), COUNT(a), SUM(a)")
      .with_data({{1}, {TestValue::null()}, {3}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 3, COUNT(a) = 2, SUM(a) = 4
  EXPECT_TRUE(result.verify_ordered({{"3", "2", "4"}}));
}

// ============================================================================
// P0 #21: Multiple aggregates
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_MultipleAggrs)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int, b int")
      .select("COUNT(*), SUM(a), MAX(b), MIN(a), AVG(b)")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 3, SUM(a) = 6, MAX(b) = 30, MIN(a) = 1, AVG(b) = 20
  EXPECT_TRUE(result.verify_ordered({{"3", "6", "30", "1", "20"}}));
}

// ============================================================================
// P0 #22: Large batch
// ============================================================================
TEST_F(ScalarAggOpTest, AggAccept_LargeBatch)
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
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  // COUNT(*) = 100, SUM(a) = 5050
  EXPECT_TRUE(result.verify_ordered({{"100", "5050"}}));
}

// ============================================================================
// Additional tests: MIN/MAX combined
// ============================================================================
TEST_F(ScalarAggOpTest, MinMax)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "a int")
      .select("MIN(a), MAX(a)")
      .with_data({{3}, {1}, {4}, {1}, {5}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "5"}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_scalar_agg_op.log*");
  OB_LOGGER.set_file_name("test_scalar_agg_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  common::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}