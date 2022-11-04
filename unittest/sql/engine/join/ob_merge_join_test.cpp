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

#include "lib/utility/ob_test_util.h"
#include <gtest/gtest.h>
#define private  public
#define protected  public
#include "ob_join_fake_table.h"
#include "sql/engine/join/ob_merge_join.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"
#include "sql/engine/test_engine_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;
class MockSqlExpression : public ObSqlExpression
{
public:
  MockSqlExpression(): ObSqlExpression(alloc_)
  {
    set_item_count(10);
  }
  ~MockSqlExpression() {}
};

class ObMergeJoinTest: public ::testing::Test
{
public:
  ObMergeJoinTest();
  virtual ~ObMergeJoinTest();
protected:
  void join_test(int64_t case_id, ObJoinType join_type);
  void serialize_test();
  void join_exception_test(int expect_ret);
  void serialize_exception_test(int expect_ret);
  // disallow copy
  ObMergeJoinTest(const ObMergeJoinTest &other);
  ObMergeJoinTest& operator=(const ObMergeJoinTest &other);
private:
  // data members
};

class ObMergeJoinPlan
{
public:
  static ObMergeJoin &get_instance()
  {
    return merge_join_;
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
    if (OB_FAIL(left_op_.init(MERGE_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "left op init failed", K(ret));
    } else if (OB_FAIL(right_op_.init(MERGE_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "right op init failed", K(ret));
    } else if (OB_FAIL(out_data_.init(MERGE_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "out data init failed", K(ret));
    } else {
      merge_join_.set_phy_plan(&phy_plan_);
      left_op_.set_phy_plan(&phy_plan_);
      right_op_.set_phy_plan(&phy_plan_);
      out_data_.set_phy_plan(&phy_plan_);
      set_id();
      set_column_count(2);
      projector_[0] = 0;
      projector_[1] = 1;
      projector_size_ = 2;
      left_op_.set_projector(projector_, projector_size_);
      right_op_.set_projector(projector_, projector_size_);
      ObArray<ObOrderDirection> array;
      ObOrderDirection d1 = oceanbase::sql::NULLS_FIRST_ASC;
      ObOrderDirection d2 = oceanbase::sql::NULLS_FIRST_ASC;
      array.push_back(d1);
      array.push_back(d2);
      if      (OB_FAIL(merge_join_.set_child(0, left_op_))) {}
      else if (OB_FAIL(merge_join_.set_child(1, right_op_))) {}
      else if (OB_FAIL(merge_join_.set_join_type(join_type))) {}
      else if (OB_FAIL(merge_join_.set_merge_directions(array))) {}
      else if (OB_FAIL(merge_join_.set_join_type(join_type))) {}
      else if (OB_FAIL(init_equal_conds())) {}
      else if (OB_FAIL(init_other_conds())) {}
      else if (OB_FAIL(left_op_.prepare_data(case_id, TT_LEFT_TABLE, join_type))) {}
      else if (OB_FAIL(right_op_.prepare_data(case_id, TT_RIGHT_TABLE, join_type))) {}
      else if (OB_FAIL(out_data_.prepare_data(case_id, TT_OUT_TABLE, join_type))) {}
    }
    return ret;
  }
  static void reuse()
  {
    phy_plan_.reset();
    equal_expr_[0].reset();
    equal_expr_[1].reset();
    other_expr_[0].reset();
    other_expr_[1].reset();
    equal_expr_[0].set_item_count(10);
    equal_expr_[1].set_item_count(10);
    other_expr_[0].set_item_count(10);
    other_expr_[1].set_item_count(10);
    merge_join_.reuse();
    left_op_.reuse();
    right_op_.reuse();
    out_data_.reuse();
    allocator_.reuse();
  }
private:
  static void set_id()
  {
    merge_join_.set_id(0);
    left_op_.set_id(1);
    right_op_.set_id(2);
    out_data_.set_id(3);
  }
  static void set_column_count(int64_t input_column_count)
  {
    merge_join_.set_column_count(2 * input_column_count);
    left_op_.set_column_count(input_column_count);
    right_op_.set_column_count(input_column_count);
    out_data_.set_column_count(2 * input_column_count);
  }
  static int init_equal_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a = t2.b => (t1.a, t2.b, OP_EQ)
    int64_t cond_count = 2;
    ObPostExprItem item_col1;
    ObPostExprItem item_col2;
    ObPostExprItem item_op;
    ObExprResType res_type;
    res_type.set_calc_type(ObIntType);
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_count; ++i) {
      item_col1.set_column(i);
      item_col2.set_column(i + cond_count);
      item_op.set_op(phy_plan_.get_allocator(), "=", 2);
      item_op.get_expr_operator()->set_result_type(res_type);
      if      (OB_FAIL(equal_expr_[i].add_expr_item(item_col1))) {}
      else if (OB_FAIL(equal_expr_[i].add_expr_item(item_col2))) {}
      else if (OB_FAIL(equal_expr_[i].add_expr_item(item_op))) {}
      else if (OB_FAIL(merge_join_.add_equijoin_condition(&equal_expr_[i]))) {}
    }
    return ret;
  }
  static int init_other_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a + t2.b > 60 => (t1.a, t2.b, OP_ADD, 60, OP_GT)
    int64_t cond_count = 1;
    ObPostExprItem item_col1;
    ObPostExprItem item_col2;
    ObPostExprItem item_op_add;
    ObPostExprItem item_int;
    ObPostExprItem item_op_gt;
    ObExprResType res_type_add;
    ObExprResType res_type_gt;
    res_type_add.set_calc_type(ObIntType);
    res_type_gt.set_calc_type(ObIntType);
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_count; ++i) {
      item_col1.set_column(i);
      item_col2.set_column(i + cond_count);
      item_op_add.set_op(phy_plan_.get_allocator(), "+", 2);
      item_op_add.get_expr_operator()->set_result_type(res_type_add);
      item_int.set_int(60);
      item_int.set_item_type(T_INT);
      item_op_gt.set_op(phy_plan_.get_allocator(), ">", 2);
      item_op_gt.get_expr_operator()->set_result_type(res_type_gt);
      if      (OB_FAIL(other_expr_[i].add_expr_item(item_col1))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_col2))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_op_add))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_int))) {}
      else if (OB_FAIL(other_expr_[i].add_expr_item(item_op_gt))) {}
      else if (OB_FAIL(merge_join_.add_other_join_condition(&other_expr_[i]))) {}
    }
    return ret;
  }
private:
  ObMergeJoinPlan();
private:
  static ObPhysicalPlan phy_plan_;
  static MockSqlExpression equal_expr_[2];
  static MockSqlExpression other_expr_[2];
  static ObMergeJoin merge_join_;
  static ObJoinFakeTable left_op_;
  static ObJoinFakeTable right_op_;
  static ObJoinFakeTable out_data_;
  static ObArenaAllocator allocator_;
  static int32_t projector_[2];
  static int64_t projector_size_;
};

ObPhysicalPlan ObMergeJoinPlan::phy_plan_;
MockSqlExpression ObMergeJoinPlan::equal_expr_[2];
MockSqlExpression ObMergeJoinPlan::other_expr_[2];
ObMergeJoin ObMergeJoinPlan::merge_join_(ObMergeJoinPlan::phy_plan_.get_allocator());
ObJoinFakeTable ObMergeJoinPlan::left_op_;
ObJoinFakeTable ObMergeJoinPlan::right_op_;
ObJoinFakeTable ObMergeJoinPlan::out_data_;
ObArenaAllocator ObMergeJoinPlan::allocator_(ObModIds::OB_PAGE_ARENA);
int32_t ObMergeJoinPlan::projector_[2];
int64_t ObMergeJoinPlan::projector_size_;

ObMergeJoinTest::ObMergeJoinTest()
{
}

ObMergeJoinTest::~ObMergeJoinTest()
{
}

void ObMergeJoinTest::join_test(int64_t case_id, ObJoinType join_type)
{
  ASSERT_EQ(OB_SUCCESS, ObMergeJoinPlan::init(case_id, join_type));
  //  BEGIN_THREAD_CODE(join_test, 8) {
  ObExecContext exec_ctx;
  ObString tenant_name("test");
  uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_ctx));
  auto my_session = exec_ctx.get_my_session();
  ASSERT_TRUE(NULL != my_session);
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, my_session->load_default_sys_variable(false, true));
  ASSERT_EQ(OB_SUCCESS, my_session->init_tenant(tenant_name, tenant_id));

  ObMergeJoin &merge_join = ObMergeJoinPlan::get_instance();
  ASSERT_EQ(OB_SUCCESS, merge_join.open(exec_ctx));
  ObJoinFakeTable &out_data = ObMergeJoinPlan::get_out_data();
  ASSERT_EQ(OB_SUCCESS, out_data.open(exec_ctx));

  int join_ret = OB_SUCCESS;
  int out_ret = OB_SUCCESS;
  const ObNewRow *join_row = NULL;
  const ObNewRow *out_row = NULL;
  while (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
    join_ret = merge_join.get_next_row(exec_ctx, join_row);
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
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(exec_ctx, join_row));
  ASSERT_EQ(OB_ITER_END, out_data.get_next_row(exec_ctx, out_row));
  ASSERT_EQ(OB_SUCCESS, merge_join.close(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, out_data.close(exec_ctx));
  //  } END_THREAD_CODE(join_test);
  ObMergeJoinPlan::reuse();
}

void ObMergeJoinTest::serialize_test()
{
  ObMergeJoin &merge_join_1 = ObMergeJoinPlan::get_instance();
  ObArenaAllocator alloc;
  ObMergeJoin merge_join_2(alloc);
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObMergeJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, merge_join_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, merge_join_1.get_serialize_size());
  int64_t data_len = pos;

  merge_join_2.set_phy_plan(const_cast<ObPhysicalPlan *>(merge_join_1.get_phy_plan()));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, merge_join_2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  const char *str_1 = to_cstring(merge_join_1);
  const char *str_2 = to_cstring(merge_join_2);
  ASSERT_EQ(0, strcmp(str_1, str_2));

  ObMergeJoinPlan::reuse();
}

void ObMergeJoinTest::join_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObMergeJoin &merge_join = ObMergeJoinPlan::get_instance();

  if (OB_FAIL(ObMergeJoinPlan::init(0, FULL_OUTER_JOIN))) {}
  else if (OB_FAIL(merge_join.open(exec_ctx))) {}
  else {
    while (OB_SUCC(ret)) {
      ret = merge_join.get_next_row(exec_ctx, row);
    }
    if (OB_ITER_END == ret) {
      ret = merge_join.close(exec_ctx);
    }
  }
  ObMergeJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void ObMergeJoinTest::serialize_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObMergeJoin &merge_join = ObMergeJoinPlan::get_instance();
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObMergeJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  if (OB_FAIL(merge_join.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos))) {}
  else {
    int64_t data_len = pos;
    pos = 0;
    ObMergeJoinPlan::reuse();
    merge_join.set_phy_plan(ObMergeJoinPlan::get_phy_plan());
    if (OB_FAIL(merge_join.deserialize(buf, data_len, pos))) {}
  }
  ObMergeJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

TEST_F(ObMergeJoinTest, join_case_0)
{
  join_test(0, INNER_JOIN);
  join_test(0, LEFT_OUTER_JOIN);
  join_test(0, RIGHT_OUTER_JOIN);
  join_test(0, FULL_OUTER_JOIN);
}

TEST_F(ObMergeJoinTest, join_case_1)
{
  join_test(1, INNER_JOIN);
  join_test(1, LEFT_OUTER_JOIN);
  join_test(1, RIGHT_OUTER_JOIN);
  join_test(1, FULL_OUTER_JOIN);
}

TEST_F(ObMergeJoinTest, join_case_2)
{
  join_test(2, INNER_JOIN);
  join_test(2, LEFT_OUTER_JOIN);
  join_test(2, RIGHT_OUTER_JOIN);
  join_test(2, FULL_OUTER_JOIN);
}

TEST_F(ObMergeJoinTest, join_case_3)
{
  join_test(3, INNER_JOIN);
  join_test(3, LEFT_OUTER_JOIN);
  join_test(3, RIGHT_OUTER_JOIN);
  join_test(3, FULL_OUTER_JOIN);
}

TEST_F(ObMergeJoinTest, join_case_4)
{
  join_test(4, INNER_JOIN);
  join_test(4, LEFT_OUTER_JOIN);
  join_test(4, RIGHT_OUTER_JOIN);
  join_test(4, FULL_OUTER_JOIN);
}

TEST_F(ObMergeJoinTest, join_case_5)
{
  join_test(5, INNER_JOIN);
  join_test(5, LEFT_OUTER_JOIN);
  join_test(5, RIGHT_OUTER_JOIN);
  join_test(5, FULL_OUTER_JOIN);
}

//TEST_F(ObMergeJoinTest, serialize_case_0)
//{
//  serialize_test();
//}

#define JOIN_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    join_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

//TEST_F(ObMergeJoinTest, join_exception_0)
//{
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "open", "t1", -1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "open", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "open", "t5", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "open", "t7", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "open", "t9", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "close", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "inner_get_next_row", "t1", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "inner_get_next_row", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "inner_get_next_row", "t5", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "inner_get_next_row", "t7", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "join_begin_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "left_join_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "left_join_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "right_join_cache_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "right_join_cache_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "right_join_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "right_join_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "read_cache_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "read_cache_func_going", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "read_cache_func_end", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "full_cache_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "full_cache_operate", "t3", OB_ITER_END, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "full_cache_func_equal", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "empty_cache_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "empty_cache_operate", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "empty_cache_func_equal", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "empty_cache_func_diff", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "empty_cache_func_diff", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "fill_cache_operate", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "fill_cache_func_equal", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "fill_cache_func_diff_end", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "trans_to_read_cache", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "trans_to_read_cache", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "trans_to_fill_cache", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "trans_to_fill_cache", "t3", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_merge_join.cpp", "trans_to_fill_cache", "t5", OB_ERROR, OB_ERROR);
//}
//
//TEST_F(ObMergeJoinTest, join_exception_1)
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
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "right_join_rows", "t1", 1, OB_ERR_UNEXPECTED);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "calc_equal_conds", "t1", OB_ERROR, OB_ERROR);
//  JOIN_EXCEPTION_TEST("ob_join.cpp", "calc_other_conds", "t1", OB_ERROR, OB_ERROR);
//}

#define SERIALIZE_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    serialize_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

//TEST_F(ObMergeJoinTest, serialize_exception)
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
//}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  oceanbase::lib::set_memory_limit(20L << 30);
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
