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
#include "sql/ob_sql_init.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/aggregate/ob_merge_distinct.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;
class TestMergeDistinct : public ObMergeDistinct
{
public:
      TestMergeDistinct() :ObMergeDistinct(alloc_) {}
            ~TestMergeDistinct() {}
};

class TestMergeDistinctTest: public ::testing::Test
{
public:
  TestMergeDistinctTest();
  virtual ~TestMergeDistinctTest();
  virtual void SetUp();
  virtual void TearDown();

  void init(ObExecContext &ctx, TestMergeDistinct &merge_distinct, int64_t col_count)
  {
    ASSERT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance()
      .add_tenant_config(ctx.get_my_session()->get_effective_tenant_id()));

    merge_distinct.reset();
    merge_distinct.reuse();
    fake_table_.reset();
    fake_table_.reuse();
    result_table_.reset();
    result_table_.reuse();
    physical_plan_.reset();
    fake_table_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    int32_t projector[3] = {0,1,2};
    result_table_.set_projector(projector, col_count);
    fake_table_.set_projector(projector, col_count);
    merge_distinct.set_projector(projector, col_count);
    merge_distinct.set_column_count(col_count);

    fake_table_.set_id(0);
    result_table_.set_id(1);
    merge_distinct.set_id(2);

    fake_table_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    merge_distinct.set_phy_plan(&physical_plan_);

    merge_distinct.set_child(0, fake_table_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ctx.get_physical_plan_ctx()->set_phy_plan(&physical_plan_);
  }

  void open_operator(ObExecContext &ctx, TestMergeDistinct &merge_distinct)
  {
    ASSERT_EQ(OB_SUCCESS, merge_distinct.open(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.open(ctx));
  }

  void close_operator(ObExecContext &ctx, TestMergeDistinct &merge_distinct)
  {
    ASSERT_EQ(OB_SUCCESS, merge_distinct.close(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.close(ctx));
  }

  ObFakeTable &get_fake_table() { return fake_table_; }
  ObFakeTable &get_result_table() { return result_table_; }
  ObPhysicalPlan &get_physical_plan() { return physical_plan_; }
protected:
  ObFakeTable fake_table_;
  ObFakeTable result_table_;
  ObPhysicalPlan physical_plan_;
private:
  // disallow copy
  TestMergeDistinctTest(const TestMergeDistinctTest &other);
  TestMergeDistinctTest& operator=(const TestMergeDistinctTest &other);
private:
  // data members
};

TestMergeDistinctTest::TestMergeDistinctTest()
{
}

TestMergeDistinctTest::~TestMergeDistinctTest()
{
}

void TestMergeDistinctTest::SetUp()
{
}

void TestMergeDistinctTest::TearDown()
{
}

TEST_F(TestMergeDistinctTest, test_utf8mb4_bin)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestMergeDistinct merge_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);

  init(ctx, merge_distinct, 3);
  //fake table: distinct column(c1)
  ADD_ROW(fake_table, COL("r"), COL(1), COL(1));
  ADD_ROW(fake_table, COL("r"), COL(1), COL(2));
  ADD_ROW(fake_table, COL("s"), COL(2), COL(3));
  ADD_ROW(fake_table, COL("s"), COL(1), COL(3));
  ADD_ROW(fake_table, COL("t"), COL(2), COL(1));
  ADD_ROW(fake_table, COL("t"), COL(3), COL(2));
  ADD_ROW(fake_table, COL("ß"), COL(3), COL(4));

  ADD_ROW(result_table, COL("r"), COL(0), COL(0));
  ADD_ROW(result_table, COL("s"), COL(1), COL(1));
  ADD_ROW(result_table, COL("t"), COL(2), COL(2));
  ADD_ROW(result_table, COL("ß"), COL(3), COL(3));

  merge_distinct.add_distinct_column(0, cs_type);
  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_distinct, 0, 0, cs_type);
  close_operator(ctx, merge_distinct);
}

TEST_F(TestMergeDistinctTest, test_utf8mb4_general_ci)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestMergeDistinct merge_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);

  init(ctx, merge_distinct, 3);
  //fake table: distinct column(c1)
  ADD_ROW(fake_table, COL("r"), COL("r"), COL(1));
  ADD_ROW(fake_table, COL("r"), COL("r"), COL(2));
  ADD_ROW(fake_table, COL("s"), COL("s"), COL(3));
  ADD_ROW(fake_table, COL("ß"), COL("s"), COL(4));
  ADD_ROW(fake_table, COL("s"), COL("ß"), COL(3));
  ADD_ROW(fake_table, COL("t"), COL("t"), COL(1));
  ADD_ROW(fake_table, COL("t"), COL("t"), COL(2));

  ADD_ROW(result_table, COL("r"), COL("r"), COL(0));
  ADD_ROW(result_table, COL("ß"), COL("ß"), COL(0));
  ADD_ROW(result_table, COL("t"), COL("t"), COL(0));

  merge_distinct.add_distinct_column(0, cs_type);
  merge_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, merge_distinct, 0, 1, cs_type);
  close_operator(ctx, merge_distinct);
}

/*
TEST_F(TestMergeDistinctTest, test_merge_distinct_1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestMergeDistinct merge_distinct;

  init(ctx, merge_distinct, 3);
  //fake table: distinct column(c1)
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(1), COL(1), COL(2));
  ADD_ROW(fake_table, COL(5), COL(2), COL(3));
  ADD_ROW(fake_table, COL(5), COL(3), COL(4));
  ADD_ROW(fake_table, COL(6), COL(1), COL(3));
  ADD_ROW(fake_table, COL(6), COL(2), COL(1));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(8), COL(4), COL(4));

  ADD_ROW(result_table, COL(1), COL(1), COL(1));
  ADD_ROW(result_table, COL(5), COL(2), COL(3));
  ADD_ROW(result_table, COL(6), COL(1), COL(3));
  ADD_ROW(result_table, COL(7), COL(3), COL(2));
  ADD_ROW(result_table, COL(8), COL(4), COL(4));

  merge_distinct.add_distinct_column(0);
  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT(ctx, result_table, merge_distinct);
  close_operator(ctx, merge_distinct);
}

TEST_F(TestMergeDistinctTest, test_merge_distinct_2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestMergeDistinct merge_distinct;

  init(ctx, merge_distinct, 3);
  //fake table: distinct column(c1, c2)
  ADD_ROW(fake_table, COL(1), COL(1), COL(1));
  ADD_ROW(fake_table, COL(1), COL(1), COL(2));
  ADD_ROW(fake_table, COL(5), COL(2), COL(3));
  ADD_ROW(fake_table, COL(5), COL(3), COL(4));
  ADD_ROW(fake_table, COL(6), COL(1), COL(3));
  ADD_ROW(fake_table, COL(6), COL(1), COL(1));
  ADD_ROW(fake_table, COL(7), COL(3), COL(2));
  ADD_ROW(fake_table, COL(7), COL(3), COL(4));

  ADD_ROW(result_table, COL(1), COL(1), COL(1));
  ADD_ROW(result_table, COL(5), COL(2), COL(3));
  ADD_ROW(result_table, COL(5), COL(3), COL(4));
  ADD_ROW(result_table, COL(6), COL(1), COL(3));
  ADD_ROW(result_table, COL(7), COL(3), COL(2));

  merge_distinct.add_distinct_column(0);
  merge_distinct.add_distinct_column(1);
  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT(ctx, result_table, merge_distinct);
  close_operator(ctx, merge_distinct);
}

TEST_F(TestMergeDistinctTest, test_merge_distinct_with_calc)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestMergeDistinct merge_distinct;

  init(ctx, merge_distinct, 4);
  merge_distinct.add_distinct_column(0);
  merge_distinct.add_distinct_column(1);
  // calculate c3 + 1
  ObColumnExpression *calc_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan_, calc_expr));
  ASSERT_FALSE(NULL == calc_expr);
  calc_expr->set_result_index(3);
  ObPostExprItem calc_item;
  calc_item.set_int(1);
  calc_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  //column c3
  calc_item.set_column(2);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  calc_item.set_op("+", 2);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  ASSERT_EQ(OB_SUCCESS, merge_distinct.add_compute(calc_expr));
  //fake table: distinct column(c1, c2)
  ADD_ROW(fake_table, COL(1), COL(1), COL(1), COL(null));
  ADD_ROW(fake_table, COL(1), COL(1), COL(2), COL(null));
  ADD_ROW(fake_table, COL(5), COL(2), COL(3), COL(null));
  ADD_ROW(fake_table, COL(5), COL(3), COL(4), COL(null));

  ADD_ROW(result_table, COL(1), COL(1), COL(1), COL(2));
  ADD_ROW(result_table, COL(5), COL(2), COL(3), COL(4));
  ADD_ROW(result_table, COL(5), COL(3), COL(4), COL(5));

  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT(ctx, result_table, merge_distinct);
  close_operator(ctx, merge_distinct);
}

TEST_F(TestMergeDistinctTest, test_aggr_empty_set)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  TestMergeDistinct merge_distinct;

  init(ctx, merge_distinct, 3);
  open_operator(ctx, merge_distinct);
  EXCEPT_RESULT(ctx, get_result_table(), merge_distinct);
  close_operator(ctx, merge_distinct);
}

TEST_F(TestMergeDistinctTest, test_serialize_and_deserialize)
{
  ObExecContext ctx;
  ObPhysicalPlan physical_plan;
  TestMergeDistinct merge_distinct;
  TestMergeDistinct deserialize_op;
  char bin_buf[1024] = {'\0'};
  int64_t buf_len = 1024;
  int64_t data_len = 0;
  int64_t pos = 0;

  init(ctx, merge_distinct, 3);
  merge_distinct.add_distinct_column(0);
  merge_distinct.add_distinct_column(1);
  ASSERT_EQ(OB_SUCCESS, merge_distinct.serialize(bin_buf, buf_len, pos));
  ASSERT_EQ(pos, merge_distinct.get_serialize_size());
  deserialize_op.set_phy_plan(&physical_plan);
  data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_op.deserialize(bin_buf, data_len, pos));
  ASSERT_EQ(0, strcmp(to_cstring(merge_distinct), to_cstring(deserialize_op)));
}

TEST_F(TestMergeDistinctTest, test_invalid_argument)
{
  ObExecContext ctx;
  ObExecContext ctx1;
  ObFakeTable fake_table;
  TestMergeDistinct merge_distinct;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_NOT_INIT, merge_distinct.open(ctx));
  merge_distinct.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, merge_distinct.get_next_row(ctx, row));
  merge_distinct.reset();
  merge_distinct.set_id(0);
  merge_distinct.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, ctx.init(3));
  ASSERT_EQ(OB_NOT_INIT, merge_distinct.open(ctx));
  merge_distinct.reset();
  merge_distinct.set_id(4);
  merge_distinct.set_column_count(3);
  fake_table.set_id(1);
  fake_table.set_column_count(3);
  ASSERT_EQ(OB_SUCCESS, merge_distinct.set_child(0, fake_table));
  ASSERT_EQ(OB_SUCCESS, ctx1.init(3));
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_distinct.open(ctx1));
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
