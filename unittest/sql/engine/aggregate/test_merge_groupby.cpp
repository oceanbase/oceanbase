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
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/aggregate/ob_aggregate_test_utils.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::blocksstable;
static ObSimpleMemLimitGetter getter;

class ObMergeGroupbyTest: public TestDataFilePrepare
{
public:
  ObMergeGroupbyTest();
  virtual ~ObMergeGroupbyTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObMergeGroupbyTest(const ObMergeGroupbyTest &other);
  ObMergeGroupbyTest& operator=(const ObMergeGroupbyTest &other);
private:
  // data members
};

ObMergeGroupbyTest::ObMergeGroupbyTest() : TestDataFilePrepare(&getter,
                                                               "TestDisk_mergegroupby", 2<<20, 5000)
{
}

ObMergeGroupbyTest::~ObMergeGroupbyTest()
{
}

void ObMergeGroupbyTest::SetUp()
{
  TestDataFilePrepare::SetUp();
  int ret = ObTmpFileManager::get_instance().init();
	ASSERT_EQ(OB_SUCCESS, ret);
}

void ObMergeGroupbyTest::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}
class TestMergeGroupBy : public ObMergeGroupBy
{
public:
  TestMergeGroupBy() : ObMergeGroupBy(alloc_) {}
  ~TestMergeGroupBy() {}
};

TEST_F(ObMergeGroupbyTest, test_utf8mb4_bin_agg)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  bool is_distinct = false;
  bool is_number = false;
  int64_t col_count = 3;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, merge_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(fake_table, COL(5), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(6), COL("r"), COL(2));
  ADD_ROW(fake_table, COL(7), COL("ß"), COL(2));
  ADD_ROW(fake_table, COL(8), COL("t"), COL(2));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby,
                col_count, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_utf8mb4_genaral_ci_agg)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  bool is_distinct = false;
  bool is_number = false;
  int64_t col_count = 3;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, merge_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(fake_table, COL(5), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(6), COL("r"), COL(2));
  ADD_ROW(fake_table, COL(7), COL("ß"), COL(2));
  ADD_ROW(fake_table, COL(8), COL("t"), COL(2));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("t"), COL("r"));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("t"), COL("r"));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby,
                col_count, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_utf8mb4_genaral_ci_group)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  bool is_distinct = false;
  bool is_number = true;
  int64_t col_count = 3;
  ObCollationType agg_cs_type = CS_TYPE_INVALID;
  ObCollationType group_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, merge_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(2), COL("ß"));
  ADD_ROW(fake_table, COL(2), COL(4), COL("s"));
  ADD_ROW(fake_table, COL(3), COL(6), COL("ß"));
  ADD_ROW(fake_table, COL(4), COL(8), COL("s"));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL(8), COL(2), COL(20.0), COL(5.0));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby,
                col_count, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_gutf8mb4_bin_agg_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  bool is_distinct = true;
  bool is_number = false;
  int64_t col_count = 3;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, merge_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(fake_table, COL(5), COL("r"), COL(2));
  ADD_ROW(fake_table, COL(6), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(7), COL("t"), COL(2));
  ADD_ROW(fake_table, COL(8), COL("ß"), COL(2));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"), COL(4));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(4), COL("ß"), COL("r"), COL(4));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby,
                col_count, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_utf8mb4_genaral_ci_agg_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  bool is_distinct = true;
  bool is_number = false;
  int64_t col_count = 3;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObCollationType group_cs_type = CS_TYPE_INVALID;

  TestAggregateFactory::init(ctx, merge_groupby, col_count, is_distinct, is_number, agg_cs_type, group_cs_type);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL("r"), COL(1));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(1));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(1));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(1));
  ADD_ROW(fake_table, COL(1), COL("r"), COL(2));
  ADD_ROW(fake_table, COL(2), COL("s"), COL(2));
  ADD_ROW(fake_table, COL(3), COL("t"), COL(2));
  ADD_ROW(fake_table, COL(4), COL("ß"), COL(2));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(3), COL("t"), COL("r"), COL(3));
  ADD_ROW(result_table, COL(0), COL(0), COL(0), COL(3), COL("t"), COL("r"), COL(3));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby,
                col_count, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}


TEST_F(ObMergeGroupbyTest, test_groupby_without_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(2), COL(1));
  ADD_ROW(fake_table, COL(3), COL(2), COL(1));
  ADD_ROW(fake_table, COL(4), COL(3), COL(1));
  ADD_ROW(fake_table, COL(5), COL(1), COL(2));
  ADD_ROW(fake_table, COL(6), COL(2), COL(2));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(4), COL(2));

  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(4), COL(3), COL(1), COL(8.0), COL(2.0));
  ADD_ROW(result_table, COL(5), COL(1), COL(2), COL(4), COL(4), COL(1), COL(10.0), COL(2.5));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby, 0, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_groupby_without_distinct1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(3), COL(1));
  ADD_ROW(fake_table, COL(2), COL(null), COL(2));
  ADD_ROW(fake_table, COL(3), COL(null), COL(2));
  ADD_ROW(fake_table, COL(4), COL(2), COL(3));
  ADD_ROW(fake_table, COL(5), COL(null), COL(3));

  ADD_ROW(result_table, COL(1), COL(3), COL(1), COL(1), COL(3), COL(3), COL(3.0), COL(3.0));
  ADD_ROW(result_table, COL(2), COL(null), COL(2), COL(0), COL(null), COL(null), COL(null), COL(null));
  ADD_ROW(result_table, COL(4), COL(2), COL(3), COL(1), COL(2), COL(2), COL(2.0), COL(2.0));
  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_groupby, 0, merge_groupby.get_column_count(), agg_cs_type);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

/*
TEST_F(ObMergeGroupbyTest, test_groupby_1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);
  ADD_ROW(fake_table, COL(1), COL(2), COL(1));
  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL(1.0), COL(2), COL(2), COL(2.0), COL(2.0));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT(ctx, result_table, merge_groupby);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_groupby_2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT(ctx, result_table, merge_groupby);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_groupby_with_distinct)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  ObFakeTable &result_table = TestAggregateFactory::get_result_table();
  TestMergeGroupBy merge_groupby;

  TestAggregateFactory::init(ctx, merge_groupby, 3, true);
  //fake table: index_col(primary key), aggr_col, groupby_col
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(2), COL(1), COL(1));
  ADD_ROW(fake_table, COL(3), COL(2), COL(1));
  ADD_ROW(fake_table, COL(4), COL(2), COL(1));
  ADD_ROW(fake_table, COL(5), COL(1), COL(2));
  ADD_ROW(fake_table, COL(6), COL(1), COL(2));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(5), COL(2));

  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(2.0), COL(2), COL(1), COL(3.0), COL(1.5));
  ADD_ROW(result_table, COL(5), COL(1), COL(2), COL(3.0), COL(5), COL(1), COL(9.0), COL(3.0));

  TestAggregateFactory::open_operator(ctx, merge_groupby);
  EXCEPT_RESULT(ctx, result_table, merge_groupby);
  TestAggregateFactory::close_operator(ctx, merge_groupby);
}

TEST_F(ObMergeGroupbyTest, test_serialize_and_deserialize)
{
  ObExecContext ctx;
  ObPhysicalPlan physical_plan;
  TestMergeGroupBy merge_groupby;
  TestMergeGroupBy deserialize_op;
  char bin_buf[1024] = {'\0'};
  int64_t buf_len = 1024;
  int64_t data_len = 0;
  int64_t pos = 0;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);
  ASSERT_EQ(OB_SUCCESS, merge_groupby.serialize(bin_buf, buf_len, pos));
  ASSERT_EQ(pos, merge_groupby.get_serialize_size());
  deserialize_op.set_phy_plan(&physical_plan);
  data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_op.deserialize(bin_buf, data_len, pos));
  ASSERT_EQ(0, strcmp(to_cstring(merge_groupby), to_cstring(deserialize_op)));
}

TEST_F(ObMergeGroupbyTest, test_invalid_argument)
{
  ObExecContext ctx;
  ObExecContext ctx1;
  ObFakeTable fake_table;
  TestMergeGroupBy merge_groupby;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_groupby.open(ctx));
  merge_groupby.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, merge_groupby.get_next_row(ctx, row));
  merge_groupby.reset();
  merge_groupby.set_id(0);
  merge_groupby.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, ctx.init(3));
  ASSERT_EQ(OB_NOT_INIT, merge_groupby.open(ctx));
  merge_groupby.reset();
  merge_groupby.set_id(0);
  merge_groupby.set_column_count(3);
  fake_table.set_id(1);
  fake_table.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, merge_groupby.set_child(0, fake_table));
  ASSERT_EQ(OB_SUCCESS, ctx1.init(3));
  ASSERT_EQ(OB_COLUMN_GROUP_NOT_FOUND, merge_groupby.open(ctx1));
}

TEST_F(ObMergeGroupbyTest, test_invalid_argument1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow *row = NULL;
  ObFakeTable &fake_table = TestAggregateFactory::get_fake_table();
  TestMergeGroupBy merge_groupby;
  ObColumnExpression col_expr;

  TestAggregateFactory::init(ctx, merge_groupby, 3, false);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table, COL(1), COL(2), COL(1));
  ADD_ROW(fake_table, COL(2), COL(2), COL(1));

  merge_groupby.set_mem_size_limit(1);
  TestAggregateFactory::open_operator(ctx, merge_groupby);
  ASSERT_EQ(OB_EXCEED_MEM_LIMIT, merge_groupby.get_next_row(ctx, row));
  merge_groupby.reset();
  ASSERT_EQ(OB_NOT_INIT, merge_groupby.add_aggr_column(&col_expr));
}
*/

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
