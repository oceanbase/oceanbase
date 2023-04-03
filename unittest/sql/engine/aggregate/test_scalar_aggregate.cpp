/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/ob_sql_init.h"
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "sql/engine/aggregate/ob_aggregate_test_utils.h"
#include "sql/engine/test_engine_util.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::blocksstable;
static ObSimpleMemLimitGetter getter;

class TestScalarAggregateTest: public TestDataFilePrepare
{
public:
  TestScalarAggregateTest();
  virtual ~TestScalarAggregateTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestScalarAggregateTest(const TestScalarAggregateTest &other);
  TestScalarAggregateTest& operator=(const TestScalarAggregateTest &other);
private:
  // data members
};
TestScalarAggregateTest::TestScalarAggregateTest() : TestDataFilePrepare(&getter,
                                                                         "TestDisk_scalar_groupby", 2<<20, 5000)
{
}

TestScalarAggregateTest::~TestScalarAggregateTest()
{
}

void TestScalarAggregateTest::SetUp()
{
  TestDataFilePrepare::SetUp();
  int ret = ObTmpFileManager::get_instance().init();
	ASSERT_EQ(OB_SUCCESS, ret);
}

void TestScalarAggregateTest::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

class TestScalarAggregate : public ObScalarAggregate
{
public:
  TestScalarAggregate() :ObScalarAggregate(alloc_) {}
  ~TestScalarAggregate() {}
};

TEST_F(TestScalarAggregateTest, test_utf8mb4_bin)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;
  int64_t col_count = 3;
  bool is_distinct = false;
  bool is_number = false;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;

  TestAggregateFactory::init(ctx, scalar_aggr_op, col_count, is_distinct, is_number, cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(4));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"), COL(4));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, scalar_aggr_op,
                col_count, scalar_aggr_op.get_column_count(), cs_type);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_utf8mb4_genaral_ci)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;
  int64_t col_count = 3;
  bool is_distinct = false;
  bool is_number = false;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, scalar_aggr_op, col_count, is_distinct, is_number, cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(4));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("t"), COL("r"), COL(3));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, scalar_aggr_op,
                col_count, scalar_aggr_op.get_column_count(), cs_type);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_utf8mb4_bin_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;
  int64_t col_count = 3;
  bool is_distinct = true;
  bool is_number = false;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;

  TestAggregateFactory::init(ctx, scalar_aggr_op, col_count, is_distinct, is_number, cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(4));
  ADD_ROW(fake_table, COL(5), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(6), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(7), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(8), COL("ß"), COL(4));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"), COL(4));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, scalar_aggr_op,
                col_count, scalar_aggr_op.get_column_count(), cs_type);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_utf8mb4_genaral_ci_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;
  int64_t col_count = 3;
  bool is_distinct = true;
  bool is_number = false;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, scalar_aggr_op, col_count, is_distinct, is_number, cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(4));
  ADD_ROW(fake_table, COL(5), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(6), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(7), COL("t"), COL(3));
  ADD_ROW(fake_table, COL(8), COL("ß"), COL(4));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(3), COL("t"), COL("r"), COL(3));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, scalar_aggr_op,
                col_count, scalar_aggr_op.get_column_count(), cs_type);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_aggr_without_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3, false);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(2), COL(2));
  ADD_ROW(fake_table, COL(5), COL(2), COL(3));
  ADD_ROW(fake_table, COL(4), COL(3), COL(4));
  ADD_ROW(fake_table, COL(3), COL(1), COL(3));
  ADD_ROW(fake_table, COL(6), COL(2), COL(1));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(4), COL(4));

  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(8), COL(4), COL(1), COL(18.0), COL(2.25));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT(ctx, result_table, scalar_aggr_op, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_aggr_with_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3, true);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(1), COL(1));
  ADD_ROW(fake_table, COL(3), COL(2), COL(1));
  ADD_ROW(fake_table, COL(4), COL(2), COL(1));
  ADD_ROW(fake_table, COL(5), COL(1), COL(2));
  ADD_ROW(fake_table, COL(6), COL(1), COL(2));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(5), COL(2));

  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(4), COL(5), COL(1), COL(11.0), COL(2.75), COL(4));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT(ctx, result_table, scalar_aggr_op, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_aggr_empty_set)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3, true);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(result_table, COL(null), COL(null), COL(null), COL(0), COL(null), COL(null), COL(0), COL(null), COL(null), COL(0));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT(ctx, result_table, scalar_aggr_op, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_aggr_bug_6131507)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestScalarAggregate scalar_aggr_op;

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3);
  //fake table: c, b, a(primary key)
  ADD_ROW(fake_table, COL(null), COL(4), COL(1));
  ADD_ROW(fake_table, COL(null), COL(3), COL(3));
  ADD_ROW(fake_table, COL(null), COL(3), COL(4));

  ADD_ROW(result_table, COL(null), COL(4), COL(1), COL(null),
          COL(3.33333333333333333333333333333333333333333333),
          COL(2.66666666666666666666666666666666666666666667));

  TestAggregateFactory::open_operator(ctx, scalar_aggr_op);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, scalar_aggr_op, 0, 3, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, scalar_aggr_op);
}

TEST_F(TestScalarAggregateTest, test_invalid_argument)
{
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObExecContext ctx1;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx1));
  ObFakeTable fake_table;
  TestScalarAggregate scalar_aggr_op;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_NOT_INIT, scalar_aggr_op.open(ctx));
  scalar_aggr_op.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, scalar_aggr_op.get_next_row(ctx, row));
  scalar_aggr_op.reset();
  scalar_aggr_op.set_id(0);
  scalar_aggr_op.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
  ASSERT_EQ(OB_NOT_INIT, scalar_aggr_op.open(ctx));
  scalar_aggr_op.reset();
  scalar_aggr_op.set_id(4);
  scalar_aggr_op.set_column_count(3);
  fake_table.set_id(1);
  fake_table.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, scalar_aggr_op.set_child(0, fake_table));
  ASSERT_EQ(OB_SUCCESS, ctx1.init_phy_op(3));
  ASSERT_EQ(OB_ERR_UNEXPECTED, scalar_aggr_op.open(ctx1));
}

TEST_F(TestScalarAggregateTest, test_invalid_argument1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  const ObNewRow *row = NULL;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  TestScalarAggregate scalar_aggr_op;
  ObArenaAllocator alloc;
  ObAggregateExpression col_expr(alloc);

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3, false);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table, COL(1), COL("123"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("456"), COL(1));

  scalar_aggr_op.set_mem_size_limit(1);
  ASSERT_EQ(OB_SUCCESS, scalar_aggr_op.open(ctx));
  ASSERT_EQ(OB_EXCEED_MEM_LIMIT, scalar_aggr_op.get_next_row(ctx, row));
  scalar_aggr_op.reset();
  //ASSERT_EQ(OB_NOT_INIT, scalar_aggr_op.add_aggr_column(&col_expr));
}

TEST_F(TestScalarAggregateTest, test_serialize_and_deserialize)
{
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObPhysicalPlan physical_plan;
  TestScalarAggregate scalar_aggr_op;
  TestScalarAggregate deserialize_op;
  char bin_buf[1024] = {'\0'};
  int64_t buf_len = 1024;
  int64_t data_len = 0;
  int64_t pos = 0;

  TestAggregateFactory::init(ctx, scalar_aggr_op, 3, false);
  ASSERT_EQ(OB_SUCCESS, scalar_aggr_op.serialize(bin_buf, buf_len, pos));
  ASSERT_EQ(pos, scalar_aggr_op.get_serialize_size());
  deserialize_op.set_phy_plan(&physical_plan);
  data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_op.deserialize(bin_buf, data_len, pos));
  ASSERT_EQ(0, strcmp(to_cstring(scalar_aggr_op), to_cstring(deserialize_op)));
}

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
