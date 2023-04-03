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
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/aggregate/ob_aggregate_test_utils.h"
#include "lib/stat/ob_session_stat.h"
#include "storage/memtable/ob_row_compactor.h"
#include "sql/engine/test_engine_util.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::blocksstable;

static ObSimpleMemLimitGetter getter;

class ObHashGroupbyTest: public TestDataFilePrepare
{
public:
  ObHashGroupbyTest();
  virtual ~ObHashGroupbyTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObHashGroupbyTest(const ObHashGroupbyTest &other);
  ObHashGroupbyTest& operator=(const ObHashGroupbyTest &other);
private:
  // data members
};
ObHashGroupbyTest::ObHashGroupbyTest() : TestDataFilePrepare(&getter,
                                                             "TestDisk_groupby", 2<<20, 5000)
{
}

ObHashGroupbyTest::~ObHashGroupbyTest()
{
}

void ObHashGroupbyTest::SetUp()
{
  TestDataFilePrepare::SetUp();
  int ret = ObTmpFileManager::get_instance().init();
	ASSERT_EQ(OB_SUCCESS, ret);
}

void ObHashGroupbyTest::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}
class TestHashGroupBy : public ObHashGroupBy
{
public:
  TestHashGroupBy() : ObHashGroupBy(alloc_)
  {
  }
  virtual ~TestHashGroupBy() {}
};

TEST_F(ObHashGroupbyTest, test_utf8mb4_bin_agg)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  int64_t col_count = 3;
  bool is_distinct = false;
  bool is_number = false;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  ObCollationType group_cs_type = CS_TYPE_INVALID;
  TestAggregateFactory::init(ctx, hash_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_groupby,
                col_count, hash_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_utf8mb4_genaral_ci_agg)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  int64_t col_count = 3;
  bool is_distinct = false;
  bool is_number = false;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, hash_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("t"), COL("r"));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_groupby,
                col_count, hash_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_utf8mb4_general_ci_group)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  int64_t col_count = 3;
  bool is_distinct = false;
  bool is_number = true;
  ObCollationType agg_cs_type = CS_TYPE_INVALID;
  ObCollationType group_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, hash_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(2), COL("ß"));
  ADD_ROW(fake_table, COL(2), COL(4), COL("s"));
  ADD_ROW(fake_table, COL(3), COL(6), COL("ß"));
  ADD_ROW(fake_table, COL(4), COL(8), COL("s"));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL(8), COL(2), COL(20.0), COL(5.0));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_groupby,
                col_count, hash_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_utf8mb4_bin_agg_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  int64_t col_count = 3;
  bool is_distinct = true;
  bool is_number = false;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, hash_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"), COL(4));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_groupby,
                col_count, hash_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_utf8mb4_general_ci_agg_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  int64_t col_count = 3;
  bool is_distinct = true;
  bool is_number = false;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, hash_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(3), COL("t"), COL("r"), COL(3));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_groupby,
                col_count, hash_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_groupby_without_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  TestAggregateFactory::init(ctx, hash_groupby, 3, false);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(2), COL(2));
  ADD_ROW(fake_table, COL(5), COL(2), COL(3));
  ADD_ROW(fake_table, COL(4), COL(3), COL(4));
  ADD_ROW(fake_table, COL(3), COL(1), COL(3));
  ADD_ROW(fake_table, COL(6), COL(2), COL(1));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(4), COL(4));

  ADD_ROW(result_table, COL(4), COL(3), COL(4), COL(2), COL(4), COL(3), COL(7.0), COL(3.5));
  ADD_ROW(result_table, COL(2), COL(2), COL(2), COL(2), COL(3), COL(2), COL(5.0), COL(2.5));
  ADD_ROW(result_table, COL(5), COL(2), COL(3), COL(2), COL(2), COL(1), COL(3.0), COL(1.5));
  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(2), COL(2), COL(1), COL(3.0), COL(1.5));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT(ctx, result_table, hash_groupby, agg_cs_type);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_groupby_varchar)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;

  TestAggregateFactory::init(ctx, hash_groupby, 3, false);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("1"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("2"), COL(2));
  ADD_ROW(fake_table, COL(5), COL("2"), COL(3));
  ADD_ROW(fake_table, COL(4), COL("3"), COL(4));
  ADD_ROW(fake_table, COL(3), COL("1"), COL(3));
  ADD_ROW(fake_table, COL(6), COL("2"), COL(1));
  ADD_ROW(fake_table, COL(7), COL("3"), COL(2));
  ADD_ROW(fake_table, COL(8), COL("4"), COL(4));

  ADD_ROW(result_table, COL(4), COL("3"), COL(4), COL(2), COL("4"), COL("3"), COL_T(double, 7.0), COL_T(double, 3.5));
  ADD_ROW(result_table, COL(2), COL("2"), COL(2), COL(2), COL("3"), COL("2"), COL_T(double, 5.0), COL_T(double, 2.5));
  ADD_ROW(result_table, COL(5), COL("2"), COL(3), COL(2), COL("2"), COL("1"), COL_T(double, 3.0), COL_T(double, 1.5));
  ADD_ROW(result_table, COL(1), COL("1"), COL(1), COL(2), COL("2"), COL("1"), COL_T(double, 3.0), COL_T(double, 1.5));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT(ctx, result_table, hash_groupby, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_groupby_with_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestHashGroupBy hash_groupby;

  TestAggregateFactory::init(ctx, hash_groupby, 3, true);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(1), COL(1));
  ADD_ROW(fake_table, COL(3), COL(2), COL(1));
  ADD_ROW(fake_table, COL(4), COL(2), COL(1));
  ADD_ROW(fake_table, COL(5), COL(1), COL(2));
  ADD_ROW(fake_table, COL(6), COL(1), COL(2));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(5), COL(2));

  ADD_ROW(result_table, COL(5), COL(1), COL(2), COL(3), COL(5), COL(1), COL(3), COL(9.0), COL(3.0));
  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(2), COL(2), COL(1), COL(2), COL(3.0), COL(1.5));

  TestAggregateFactory::open_operator(ctx, hash_groupby);
  EXCEPT_RESULT(ctx, result_table, hash_groupby, CS_TYPE_UTF8MB4_BIN);
  TestAggregateFactory::close_operator(ctx, hash_groupby);
}

TEST_F(ObHashGroupbyTest, test_invalid_argument)
{
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObExecContext ctx1;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx1));
  ObArenaAllocator alloc;
  ObFakeTable fake_table(alloc);
  TestHashGroupBy hash_groupby;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_NOT_INIT, hash_groupby.open(ctx));
  hash_groupby.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, hash_groupby.get_next_row(ctx, row));
  hash_groupby.reset();
  hash_groupby.set_id(0);
  hash_groupby.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
  ASSERT_EQ(OB_NOT_INIT, hash_groupby.open(ctx));
  hash_groupby.reset();
  hash_groupby.set_id(0);
  hash_groupby.set_column_count(3);
  fake_table.set_id(1);
  fake_table.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, hash_groupby.set_child(0, fake_table));
  ASSERT_EQ(OB_SUCCESS, ctx1.init_phy_op(3));
  ASSERT_EQ(OB_ERR_UNEXPECTED, hash_groupby.open(ctx1));
}

TEST_F(ObHashGroupbyTest, test_invalid_argument1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  TestHashGroupBy hash_groupby;
  ObArenaAllocator alloc;
  ObAggregateExpression col_expr(alloc);

  TestAggregateFactory::init(ctx, hash_groupby, 3, false);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table, COL(1), COL(2), COL(1));
  ADD_ROW(fake_table, COL(2), COL(2), COL(1));

}

TEST_F(ObHashGroupbyTest, test_serialize_and_deserialize)
{
  ObExecContext ctx;
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  ObPhysicalPlan physical_plan;
  TestHashGroupBy hash_groupby;
  TestHashGroupBy deserialize_op;
  char bin_buf[1024] = {'\0'};
  int64_t buf_len = 1024;
  int64_t data_len = 0;
  int64_t pos = 0;

  TestAggregateFactory::init(ctx, hash_groupby, 3, false);
  ASSERT_EQ(OB_SUCCESS, hash_groupby.serialize(bin_buf, buf_len, pos));
  ASSERT_EQ(pos, hash_groupby.get_serialize_size());
  deserialize_op.set_phy_plan(&physical_plan);
  data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_op.deserialize(bin_buf, data_len, pos));
  ASSERT_EQ(0, strcmp(to_cstring(hash_groupby), to_cstring(deserialize_op)));
}

void  __attribute__((constructor(101))) init_SessionDIBuffer()
{
  oceanbase::common::ObDITls<ObSessionDIBuffer>::get_instance();
}

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
