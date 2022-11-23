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

#include "sql/engine/join/ob_nested_loop_join.h"
#include "lib/utility/ob_test_util.h"
#include "ob_join_fake_table.h"
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/ob_sql_init.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

class ObNestedLoopJoinTest: public ::testing::Test
{
public:
  ObNestedLoopJoinTest();
  virtual ~ObNestedLoopJoinTest();
protected:
  void join_test(int64_t case_id, ObJoinType join_type);
  void serialize_test();
  void join_exception_test(int expect_ret);
  void serialize_exception_test(int expect_ret);
  // disallow copy
  ObNestedLoopJoinTest(const ObNestedLoopJoinTest &other);
  ObNestedLoopJoinTest& operator=(const ObNestedLoopJoinTest &other);
private:
  // data members
};

class ObNestedLoopJoinPlan
{
public:
  static ObNestedLoopJoin &get_instance()
  {
    return nested_loop_join_;
  }
  static ObJoinFakeTable &get_out_data()
  {
    return out_data_;
  }
  static ObPhysicalPlan *get_phy_plan()
  {
    return &phy_plan_;
  }
  static int init(int64_t case_id, ObJoinType join_type)
  {
    int ret = OB_SUCCESS;
    nested_loop_join_.set_phy_plan(&phy_plan_);
    set_id();
    set_column_count(2);
    right_op_.set_type(PHY_TABLE_SCAN);
    if      (OB_FAIL(nested_loop_join_.set_child(0, left_op_))) {}
    else if (OB_FAIL(nested_loop_join_.set_child(1, right_op_))) {}
    else if (OB_FAIL(nested_loop_join_.set_join_type(join_type))) {}
    else if (OB_FAIL(init_scan_index())) {}
    else if (OB_FAIL(init_equal_conds())) {}
    else if (OB_FAIL(init_other_conds())) {}
    else if (OB_FAIL(left_op_.prepare_data(case_id, TT_LEFT_TABLE, join_type))) {}
    else if (OB_FAIL(right_op_.prepare_data(case_id, TT_RIGHT_TABLE, join_type))) {}
    else if (OB_FAIL(out_data_.prepare_data(case_id, TT_OUT_TABLE, join_type))) {}
    return ret;
  }
  static void reuse()
  {
    phy_plan_.reset();
    equal_expr_[0].reset();
    equal_expr_[1].reset();
    other_expr_[0].reset();
    other_expr_[1].reset();
    nested_loop_join_.reuse();
    left_op_.reuse();
    right_op_.reuse();
    out_data_.reuse();
    allocator_.reuse();
  }
private:
  static void set_id()
  {
    nested_loop_join_.set_id(0);
    left_op_.set_id(1);
    right_op_.set_id(2);
    out_data_.set_id(3);
  }
  static void set_column_count(int64_t input_column_count)
  {
    nested_loop_join_.set_column_count(2 * input_column_count);
    left_op_.set_column_count(input_column_count);
    right_op_.set_column_count(input_column_count);
    out_data_.set_column_count(2 * input_column_count);
  }
  static int init_scan_index()
  {
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    if (OB_FAIL(nested_loop_join_.add_left_scan_index(idx))) {}
    return ret;
  }
  static int init_equal_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a = t2.b => (t1.a, t2.b, OP_EQ)
    ObPostExprItem item_col;
    ObPostExprItem item_op;
    int64_t idx = 1;
    item_col.set_column(idx);
    item_op.set_op("=", 2);
    if      (OB_FAIL(equal_expr_[idx].add_expr_item(item_col))) {}
    else if (OB_FAIL(equal_expr_[idx].add_expr_item(item_col))) {}
    else if (OB_FAIL(equal_expr_[idx].add_expr_item(item_op))) {}
    else if (OB_FAIL(nested_loop_join_.add_equijoin_condition(&equal_expr_[idx]))) {}
    return ret;
  }
  static int init_other_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a + t2.b > 60 => (t1.a, t2.b, OP_ADD, 60, OP_GT)
    int64_t cond_count = 1;
    ObPostExprItem item_col;
    ObPostExprItem item_op_add;
    ObPostExprItem item_int;
    ObPostExprItem item_op_gt;
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_count; ++i) {
      item_col.set_column(i);
      item_op_add.set_op("+", 2);
      item_int.set_int(60);
      item_int.set_item_type(T_INT);
      item_op_gt.set_op(">", 2);
      if      (OB_FAIL(other_expr_[i].add_expr_item(item_col))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_col))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_op_add))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_int))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_op_gt))) {}
      else if (OB_FAIL(nested_loop_join_.add_other_join_condition(&other_expr_[i]))) {}
    }
    return ret;
  }
private:
  ObNestedLoopJoinPlan();
private:
  static ObPhysicalPlan phy_plan_;
  static ObSqlExpression equal_expr_[2];
  static ObSqlExpression other_expr_[2];
  static ObSqlExpression param_expr_[2];
  static ObNestedLoopJoin nested_loop_join_;
  static ObJoinFakeTable left_op_;
  static ObJoinFakeTable right_op_;
  static ObJoinFakeTable out_data_;
  static ObArenaAllocator allocator_;
};

ObPhysicalPlan ObNestedLoopJoinPlan::phy_plan_;
ObSqlExpression ObNestedLoopJoinPlan::equal_expr_[2];
ObSqlExpression ObNestedLoopJoinPlan::other_expr_[2];
ObSqlExpression ObNestedLoopJoinPlan::param_expr_[2];
ObNestedLoopJoin ObNestedLoopJoinPlan::nested_loop_join_(ObNestedLoopJoinPlan::phy_plan_.get_allocator());
ObJoinFakeTable ObNestedLoopJoinPlan::left_op_;
ObJoinFakeTable ObNestedLoopJoinPlan::right_op_;
ObJoinFakeTable ObNestedLoopJoinPlan::out_data_;
ObArenaAllocator ObNestedLoopJoinPlan::allocator_(ObModIds::OB_PAGE_ARENA);

ObNestedLoopJoinTest::ObNestedLoopJoinTest()
{
}

ObNestedLoopJoinTest::~ObNestedLoopJoinTest()
{
}

void ObNestedLoopJoinTest::join_test(int64_t case_id, ObJoinType join_type)
{
  ASSERT_EQ(OB_SUCCESS, ObNestedLoopJoinPlan::init(case_id, join_type));
  BEGIN_THREAD_CODE(join_test, 8) {
    ObExecContext exec_ctx;
    ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
    ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());

    ObNestedLoopJoin &nested_loop_join = ObNestedLoopJoinPlan::get_instance();
    ASSERT_EQ(OB_SUCCESS, nested_loop_join.open(exec_ctx));
    ObJoinFakeTable &out_data = ObNestedLoopJoinPlan::get_out_data();
    ASSERT_EQ(OB_SUCCESS, out_data.open(exec_ctx));

    int join_ret = OB_SUCCESS;
    int out_ret = OB_SUCCESS;
    const ObNewRow *join_row = NULL;
    const ObNewRow *out_row = NULL;
    while (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
      join_ret = nested_loop_join.get_next_row(exec_ctx, join_row);
      out_ret = out_data.get_next_row(exec_ctx, out_row);
      usleep(10 * 1000);
      ASSERT_EQ(join_ret, out_ret);
      if (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
        ObObj *join_cells = join_row->cells_;
        int64_t join_cell0 = join_cells[0].is_null() ? 0 : join_cells[0].get_int();
        int64_t join_cell1 = join_cells[1].is_null() ? 0 : join_cells[1].get_int();
        int64_t join_cell2 = join_cells[2].is_null() ? 0 : join_cells[2].get_int();
        int64_t join_cell3 = join_cells[3].is_null() ? 0 : join_cells[3].get_int();
        ObObj *out_cells = out_row->cells_;
        int64_t out_cell0 = out_cells[0].is_null() ? 0 : out_cells[0].get_int();
        int64_t out_cell1 = out_cells[1].is_null() ? 0 : out_cells[1].get_int();
        int64_t out_cell2 = out_cells[2].is_null() ? 0 : out_cells[2].get_int();
        int64_t out_cell3 = out_cells[3].is_null() ? 0 : out_cells[3].get_int();
        ASSERT_EQ(join_cell0, out_cell0);
        ASSERT_EQ(join_cell1, out_cell1);
        ASSERT_EQ(join_cell2, out_cell2);
        ASSERT_EQ(join_cell3, out_cell3);
      }
    } // while
    ASSERT_EQ(OB_ITER_END, join_ret);
    ASSERT_EQ(OB_ITER_END, out_ret);
    ASSERT_EQ(OB_ITER_END, nested_loop_join.get_next_row(exec_ctx, join_row));
    ASSERT_EQ(OB_ITER_END, out_data.get_next_row(exec_ctx, out_row));
    ASSERT_EQ(OB_SUCCESS, nested_loop_join.close(exec_ctx));
    ASSERT_EQ(OB_SUCCESS, out_data.close(exec_ctx));
  } END_THREAD_CODE(join_test);
  ObNestedLoopJoinPlan::reuse();
}

void ObNestedLoopJoinTest::serialize_test()
{
  ObNestedLoopJoin &nested_loop_join_1 = ObNestedLoopJoinPlan::get_instance();
  ObNestedLoopJoin nested_loop_join_2;
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObNestedLoopJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, nested_loop_join_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, nested_loop_join_1.get_serialize_size());
  int64_t data_len = pos;

  nested_loop_join_2.set_phy_plan(const_cast<ObPhysicalPlan *>(nested_loop_join_1.get_phy_plan()));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, nested_loop_join_2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  const char *str_1 = to_cstring(nested_loop_join_1);
  const char *str_2 = to_cstring(nested_loop_join_2);
  ASSERT_EQ(0, strcmp(str_1, str_2));

  ObNestedLoopJoinPlan::reuse();
}

void ObNestedLoopJoinTest::join_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObNestedLoopJoin &nested_loop_join = ObNestedLoopJoinPlan::get_instance();

  if (OB_FAIL(ObNestedLoopJoinPlan::init(0, LEFT_OUTER_JOIN))) {}
  else if (OB_FAIL(nested_loop_join.open(exec_ctx))) {}
  else {
    while (OB_SUCC(ret)) {
      ret = nested_loop_join.get_next_row(exec_ctx, row);
    }
    if (OB_ITER_END == ret) {
      ret = nested_loop_join.close(exec_ctx);
    }
  }
  ObNestedLoopJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void ObNestedLoopJoinTest::serialize_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoin &nested_loop_join = ObNestedLoopJoinPlan::get_instance();
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObNestedLoopJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  if (OB_FAIL(nested_loop_join.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos))) {}
  else {
    int64_t data_len = pos;
    pos = 0;
    ObNestedLoopJoinPlan::reuse();
    nested_loop_join.set_phy_plan(ObNestedLoopJoinPlan::get_phy_plan());
    if (OB_FAIL(nested_loop_join.deserialize(buf, data_len, pos))) {}
  }
  ObNestedLoopJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

TEST_F(ObNestedLoopJoinTest, join_case_0)
{
  join_test(0, INNER_JOIN);
  join_test(0, LEFT_OUTER_JOIN);
}

TEST_F(ObNestedLoopJoinTest, join_case_1)
{
  join_test(1, INNER_JOIN);
  join_test(1, LEFT_OUTER_JOIN);
}

TEST_F(ObNestedLoopJoinTest, join_case_2)
{
  join_test(2, INNER_JOIN);
  join_test(2, LEFT_OUTER_JOIN);
}

TEST_F(ObNestedLoopJoinTest, join_case_3)
{
  join_test(3, INNER_JOIN);
  join_test(3, LEFT_OUTER_JOIN);
}

TEST_F(ObNestedLoopJoinTest, join_case_4)
{
  join_test(4, INNER_JOIN);
  join_test(4, LEFT_OUTER_JOIN);
}

TEST_F(ObNestedLoopJoinTest, join_case_5)
{
  join_test(5, INNER_JOIN);
  join_test(5, LEFT_OUTER_JOIN);
}

//TEST_F(ObNestedLoopJoinTest, serialize_case_0)
//{
//  serialize_test();
//}

#define JOIN_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    join_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

//TEST_F(ObNestedLoopJoinTest, join_exception_0)
//{
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "open", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "open", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "open", "t5", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "close", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "inner_get_next_row", "t1", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "inner_get_next_row", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "inner_get_next_row", "t5", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "inner_create_operator_ctx", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_left_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_left_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_left_func_going", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_right_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_right_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_right_func_going", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_right_func_going", "t5", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "read_right_func_end", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "calc_right_query_range", "t1", -1, OB_NOT_IMPLEMENT);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "calc_right_query_range", "t3", 1, OB_NOT_INIT);
//  JOIN_EXCEPTION_TEST("ob_nested_loop_join.cpp", "calc_right_query_range", "t5", 1, OB_NOT_INIT);
//}
//
//TEST_F(ObNestedLoopJoinTest, join_exception_1)
//{
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "open", "t1", UNKNOWN_JOIN, OB_NOT_INIT);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "open", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_equijoin_condition", "t1", 1, OB_NOT_INIT);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_equijoin_condition", "t3", 1, OB_ALLOCATE_MEMORY_FAILED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_equijoin_condition", "t5", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_other_join_condition", "t1", 1, OB_NOT_INIT);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_other_join_condition", "t3", 1, OB_ALLOCATE_MEMORY_FAILED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "add_other_join_condition", "t5", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "join_rows", "t1", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "left_join_rows", "t1", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "calc_equal_conds", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "calc_other_conds", "t1", OB_ERROR, OB_ERROR);
//}

#define SERIALIZE_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    serialize_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

//TEST_F(ObNestedLoopJoinTest, serialize_exception)
//{
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "serialize", "t1", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "serialize", "t3", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "serialize", "t5", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "serialize", "t7", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "serialize", "t9", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t1", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t3", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t5", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t7", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t9", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t11", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t13", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t15", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_join.cpp", "deserialize", "t17", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_nested_loop_join.cpp", "serialize", "t1", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_nested_loop_join.cpp", "serialize", "t3", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_nested_loop_join.cpp", "deserialize", "t1", OB_ERROR, OB_ERROR);
//  SERIALIZE_EXCEPTION_TEST("ob_nested_loop_join.cpp", "deserialize", "t3", OB_ERROR, OB_ERROR);
//}

int main(int argc, char **argv)
{
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
