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
#define private  public
#define protected  public
#include "sql/engine/join/ob_block_based_nested_loop_join.h"
#include "lib/utility/ob_test_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "ob_join_fake_table.h"
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
  MockSqlExpression(): ObSqlExpression(alloc_) { set_item_count(10);}
  ~MockSqlExpression() {}
};
class ObBLKNestedLoopJoinTest : public ::testing::Test
{
public:
  ObBLKNestedLoopJoinTest();
  virtual ~ObBLKNestedLoopJoinTest();
protected:
  void join_test(int64_t case_id, ObJoinType join_type);
  void serialize_test();
  void join_exception_test(int expect_ret);
  void serialize_exception_test(int expect_ret);
  ObBLKNestedLoopJoinTest(const ObBLKNestedLoopJoinTest &other);
  ObBLKNestedLoopJoinTest& operator=(const ObBLKNestedLoopJoinTest &other);
private:
  // data members
};

class ObBLKNestedLoopJoinPlan
{
public:
  static ObBLKNestedLoopJoin &get_instance()
  {
    return blk_nested_loop_join_;
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
    if (OB_FAIL(left_op_.init(BNL_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "left op init failed", K(ret));
    } else if (OB_FAIL(right_op_.init(BNL_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "right op init failed", K(ret));
    } else if (OB_FAIL(out_data_.init(BNL_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "out data init failed", K(ret));
    } else {
      blk_nested_loop_join_.set_phy_plan(&phy_plan_);
      left_op_.set_phy_plan(&phy_plan_);
      right_op_.set_phy_plan(&phy_plan_);
      out_data_.set_phy_plan(&phy_plan_);
      set_id();
      set_column_count(2);
      if (OB_FAIL(blk_nested_loop_join_.set_child(0, left_op_))) {
        SQL_ENG_LOG(WARN, "set child failed", K(ret));
      } else if (OB_FAIL(blk_nested_loop_join_.set_child(1, right_op_))) {
        SQL_ENG_LOG(WARN, "set child failed", K(ret));
      } else if (OB_FAIL(blk_nested_loop_join_.set_join_type(join_type))) {} else if (OB_FAIL(init_equal_conds())) {
        SQL_ENG_LOG(WARN, "set join type failed", K(ret));
      } else if (OB_FAIL(init_other_conds())) {
        SQL_ENG_LOG(WARN, "init other conds", K(ret));
      } else if (OB_FAIL(left_op_.prepare_data(case_id, TT_LEFT_TABLE, join_type))) {
        SQL_ENG_LOG(WARN, "left op prepare data failed", K(ret));
      } else if (OB_FAIL(right_op_.prepare_data(case_id, TT_RIGHT_TABLE, join_type))) {
        SQL_ENG_LOG(WARN, "right op prepare data failed", K(ret));
      } else if (OB_FAIL(out_data_.prepare_data(case_id, TT_OUT_TABLE, join_type))) {
        SQL_ENG_LOG(WARN, "out data prepare data failed", K(ret));
      }
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
    blk_nested_loop_join_.reuse();
    left_op_.reuse();
    right_op_.reuse();
    out_data_.reuse();
    allocator_.reuse();
  }
private:
  static void set_id()
  {
    blk_nested_loop_join_.set_id(0);
    left_op_.set_id(1);
    right_op_.set_id(2);
    out_data_.set_id(3);
  }
  static void set_column_count(int64_t input_column_count)
  {
    blk_nested_loop_join_.set_column_count(2 * input_column_count);
    left_op_.set_column_count(input_column_count);
    left_projector_[0] = 0;
    left_projector_[1] = 1;
    left_op_.set_projector(left_projector_, 2);
    right_op_.set_column_count(input_column_count);
    right_projector_[0] = 0;
    right_projector_[1] = 1;
    right_op_.set_projector(right_projector_, 2);
    out_data_.set_column_count(2 * input_column_count);
    out_projector_[0] = 0;
    out_projector_[1] = 1;
    out_projector_[2] = 2;
    out_projector_[3] = 3;
    out_data_.set_projector(out_projector_, 4);
  }
  static int init_equal_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a = t2.b => (t1.a, t2.b, OP_EQ)
    ObPostExprItem item_col_left;
    ObPostExprItem item_col_right;
    ObPostExprItem item_op;
    ObExprResType cmp_type;
    cmp_type.set_type(ObIntType);
    cmp_type.set_calc_type(ObIntType);

    int64_t idx = 1;
    int64_t column_count = 2;
    for (idx = 0; OB_SUCC(ret) && idx < column_count; ++idx) {
      item_col_left.set_column(idx);
      item_col_right.set_column(idx + column_count);
      item_op.set_op(phy_plan_.get_allocator(), "=", 2);
      item_op.get_expr_operator()->set_result_type(cmp_type);
      if (OB_FAIL(equal_expr_[idx].add_expr_item(item_col_left))) {
        SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
      } else if (OB_FAIL(equal_expr_[idx].add_expr_item(item_col_right))) {
        SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
      } else if (OB_FAIL(equal_expr_[idx].add_expr_item(item_op))) {
        SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
      } else if (OB_FAIL(blk_nested_loop_join_.add_other_join_condition(&equal_expr_[idx]))) {
        SQL_ENG_LOG(WARN, "add equal join condition failed", K(ret));
      }
    }
    return ret;
  }
  static int init_other_conds()
  {
    int ret = OB_SUCCESS;
    // like t1.a + t2.b > 60 => (t1.a, t2.b, OP_ADD, 60, OP_GT)
    int64_t column_count = 2;
 //   int64_t cond_count = 1;
    ObPostExprItem item_col_left;
    ObPostExprItem item_col_right;
    ObPostExprItem item_op_add;
    ObPostExprItem item_int;
    ObPostExprItem item_op_gt;
    ObExprResType add_type;
    add_type.set_type(ObIntType);
    add_type.set_calc_type(ObIntType);
    ObExprResType cmp_type;
    cmp_type.set_type(ObIntType);
    cmp_type.set_calc_type(ObIntType);

    item_col_left.set_column(0);
    item_col_right.set_column(0 + column_count);
    item_op_add.set_op(phy_plan_.get_allocator(), "+", 2);
    item_op_add.get_expr_operator()->set_result_type(add_type);
    item_int.set_int(60);
    item_int.set_item_type(T_INT);
    item_op_gt.set_op(phy_plan_.get_allocator(), ">", 2);
    item_op_gt.get_expr_operator()->set_result_type(add_type);
    if (OB_FAIL(other_expr_[0].add_expr_item(item_col_left))) {
      SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
    } else if (OB_FAIL(other_expr_[0].add_expr_item(item_col_right))) {
      SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
    } else if (OB_FAIL(other_expr_[0].add_expr_item(item_op_add))) {
      SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
    } else if (OB_FAIL(other_expr_[0].add_expr_item(item_int))) {
      SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
    } else if (OB_FAIL(other_expr_[0].add_expr_item(item_op_gt))) {
      SQL_ENG_LOG(WARN, "add expr item failed", K(ret));
    } else if (OB_FAIL(blk_nested_loop_join_.add_other_join_condition(&other_expr_[0]))) {
      SQL_ENG_LOG(WARN, "add equal join condition failed", K(ret));
    }

    return ret;
  }
private:
  ObBLKNestedLoopJoinPlan();
public:
  static ObPhysicalPlan phy_plan_;
  static MockSqlExpression equal_expr_[2];
  static MockSqlExpression other_expr_[2];
  static ObBLKNestedLoopJoin blk_nested_loop_join_;
  static ObJoinFakeTable left_op_;
  static ObJoinFakeTable right_op_;
  static ObJoinFakeTable out_data_;
  static ObArenaAllocator allocator_;
  static int32 left_projector_[10];
  static int32 right_projector_[10];
  static int32 out_projector_[10];
};

ObPhysicalPlan ObBLKNestedLoopJoinPlan::phy_plan_;
MockSqlExpression ObBLKNestedLoopJoinPlan::equal_expr_[2];
MockSqlExpression ObBLKNestedLoopJoinPlan::other_expr_[2];
ObBLKNestedLoopJoin ObBLKNestedLoopJoinPlan::blk_nested_loop_join_(ObBLKNestedLoopJoinPlan::phy_plan_.get_allocator());
ObJoinFakeTable ObBLKNestedLoopJoinPlan::left_op_;
ObJoinFakeTable ObBLKNestedLoopJoinPlan::right_op_;
ObJoinFakeTable ObBLKNestedLoopJoinPlan::out_data_;
ObArenaAllocator ObBLKNestedLoopJoinPlan::allocator_(ObModIds::OB_PAGE_ARENA);
int32 ObBLKNestedLoopJoinPlan::left_projector_[10];
int32 ObBLKNestedLoopJoinPlan::right_projector_[10];
int32 ObBLKNestedLoopJoinPlan::out_projector_[10];

ObBLKNestedLoopJoinTest::ObBLKNestedLoopJoinTest()
{
}

ObBLKNestedLoopJoinTest::~ObBLKNestedLoopJoinTest()
{
}

void ObBLKNestedLoopJoinTest::join_test(int64_t case_id, ObJoinType join_type)
{
  ASSERT_EQ(OB_SUCCESS, ObBLKNestedLoopJoinPlan::init(case_id, join_type));
  BEGIN_THREAD_CODE(join_test, 8) {
    ObExecContext exec_ctx;
    ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
    ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(exec_ctx));
    auto my_session = exec_ctx.get_my_session();
    ASSERT_FALSE(NULL == my_session );
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    my_session->load_default_sys_variable(false, false);

    ObBLKNestedLoopJoin &blk_nested_loop_join = ObBLKNestedLoopJoinPlan::get_instance();
    ASSERT_EQ(OB_SUCCESS, blk_nested_loop_join.open(exec_ctx));
    ObJoinFakeTable &out_data = ObBLKNestedLoopJoinPlan::get_out_data();
    ASSERT_EQ(OB_SUCCESS, out_data.open(exec_ctx));

    int join_ret = OB_SUCCESS;
    int out_ret = OB_SUCCESS;
    const ObNewRow *join_row = NULL;
    const ObNewRow *out_row = NULL;
    while (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
      join_ret = blk_nested_loop_join.get_next_row(exec_ctx, join_row);
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
    ASSERT_EQ(OB_ITER_END, blk_nested_loop_join.get_next_row(exec_ctx, join_row));
    ASSERT_EQ(OB_ITER_END, out_data.get_next_row(exec_ctx, out_row));
    ASSERT_EQ(OB_SUCCESS, blk_nested_loop_join.close(exec_ctx));
    ASSERT_EQ(OB_SUCCESS, out_data.close(exec_ctx));
  } END_THREAD_CODE(join_test);
  ObBLKNestedLoopJoinPlan::reuse();
}

void ObBLKNestedLoopJoinTest::serialize_test()
{
  ObBLKNestedLoopJoin &blk_nested_loop_join_1 = ObBLKNestedLoopJoinPlan::get_instance();
  ObBLKNestedLoopJoin blk_nested_loop_join_2(ObBLKNestedLoopJoinPlan::phy_plan_.get_allocator());
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObBLKNestedLoopJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blk_nested_loop_join_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, blk_nested_loop_join_1.get_serialize_size());
  int64_t data_len = pos;

  blk_nested_loop_join_2.set_phy_plan(const_cast<ObPhysicalPlan *>(blk_nested_loop_join_1.get_phy_plan()));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, blk_nested_loop_join_2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  const char *str_1 = to_cstring(blk_nested_loop_join_1);
  const char *str_2 = to_cstring(blk_nested_loop_join_2);
  ASSERT_EQ(0, strcmp(str_1, str_2));

  ObBLKNestedLoopJoinPlan::reuse();
}

void ObBLKNestedLoopJoinTest::join_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObBLKNestedLoopJoin &blk_nested_loop_join = ObBLKNestedLoopJoinPlan::get_instance();

  if (OB_FAIL(ObBLKNestedLoopJoinPlan::init(0, LEFT_OUTER_JOIN))) {
    SQL_ENG_LOG(WARN, "init bnl join plan failed", K(ret));
  } else if (OB_FAIL(blk_nested_loop_join.open(exec_ctx))) {
    SQL_ENG_LOG(WARN, "open bnl join plan failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ret = blk_nested_loop_join.get_next_row(exec_ctx, row);
    }
    if (OB_ITER_END == ret) {
      ret = blk_nested_loop_join.close(exec_ctx);
    }
  }
  ObBLKNestedLoopJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void ObBLKNestedLoopJoinTest::serialize_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObBLKNestedLoopJoin &blk_nested_loop_join = ObBLKNestedLoopJoinPlan::get_instance();
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObBLKNestedLoopJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  if (OB_FAIL(blk_nested_loop_join.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos))) {
    SQL_ENG_LOG(WARN, "serialize bnl join plan failed", K(ret));
  } else {
    int64_t data_len = pos;
    pos = 0;
    ObBLKNestedLoopJoinPlan::reuse();
    blk_nested_loop_join.set_phy_plan(ObBLKNestedLoopJoinPlan::get_phy_plan());
    if (OB_FAIL(blk_nested_loop_join.deserialize(buf, data_len, pos))) {
      SQL_ENG_LOG(WARN, "deserialize bnl join plan failed", K(ret));
    }
  }
  ObBLKNestedLoopJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_0)
{
  join_test(0, INNER_JOIN);
  join_test(0, LEFT_OUTER_JOIN);
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_1)
{
  join_test(1, INNER_JOIN);
  join_test(1, LEFT_OUTER_JOIN);
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_2)
{
  join_test(2, INNER_JOIN);
  join_test(2, LEFT_OUTER_JOIN);
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_3)
{
  join_test(3, INNER_JOIN);
  join_test(3, LEFT_OUTER_JOIN);
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_4)
{
  join_test(4, INNER_JOIN);
  join_test(4, LEFT_OUTER_JOIN);
}

TEST_F(ObBLKNestedLoopJoinTest, join_case_5)
{
  join_test(5, INNER_JOIN);
  join_test(5, LEFT_OUTER_JOIN);
}

int main(int argc, char **argv)
{
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
