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
#include "lib/utility/ob_test_util.h"
#include "sql/engine/join/ob_hash_join.h"
#include "ob_join_fake_table.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"
#include <vector>
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

class ObHashJoinTest: public ::testing::Test
{
public:
  ObHashJoinTest();
  virtual ~ObHashJoinTest();
protected:
  void join_test(int64_t case_id, ObJoinType join_type);
  void serialize_test();
  void join_exception_test(int expect_ret);
  void serialize_exception_test(int expect_ret);
  void memlimit_exception_test(int expect_ret, int64_t mem_size);
  // disallow copy
  ObHashJoinTest(const ObHashJoinTest &other);
  ObHashJoinTest& operator=(const ObHashJoinTest &other);
private:
  // data members
};

class ObHashJoinPlan
{
public:
  static ObHashJoin &get_instance()
  {
    return hash_join_;
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
    if (OB_FAIL(left_op_.init(HASH_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "left op init failed", K(ret));
    } else if (OB_FAIL(right_op_.init(HASH_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "right op init failed", K(ret));
    } else if (OB_FAIL(out_data_.init(HASH_JOIN_TEST))) {
      SQL_ENG_LOG(WARN, "out data init failed", K(ret));
    } else {
      hash_join_.set_phy_plan(&phy_plan_);
      left_op_.set_phy_plan(&phy_plan_);
      right_op_.set_phy_plan(&phy_plan_);
      left_op_.set_rows(10000); // default
      out_data_.set_phy_plan(&phy_plan_);
      set_id();
      set_column_count(2);
      projector_[0] = 0;
      projector_[1] = 1;
      projector_size_ = 2;
      left_op_.set_projector(projector_, projector_size_);
      right_op_.set_projector(projector_, projector_size_);
      if      (OB_FAIL(hash_join_.set_child(0, left_op_))) {}
      else if (OB_FAIL(hash_join_.set_child(1, right_op_))) {}
      else if (OB_FAIL(hash_join_.set_join_type(join_type))) {}
      else if (OB_FAIL(hash_join_.set_join_type(join_type))) {}
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
    hash_join_.reuse();
    left_op_.reuse();
    right_op_.reuse();
    out_data_.reuse();
    allocator_.reuse();
  }
private:
  static void set_id()
  {
    hash_join_.set_id(0);
    left_op_.set_id(1);
    right_op_.set_id(2);
    out_data_.set_id(3);
  }
  static void set_column_count(int64_t input_column_count)
  {
    hash_join_.set_column_count(2 * input_column_count);
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
      else if (OB_FAIL(hash_join_.add_equijoin_condition(&equal_expr_[i]))) {}
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
      else if (OB_FAIL(hash_join_.add_other_join_condition(&other_expr_[i]))) {}
    }
    return ret;
  }
private:
  ObHashJoinPlan();
private:
  static ObPhysicalPlan phy_plan_;
  static MockSqlExpression equal_expr_[2];
  static MockSqlExpression other_expr_[2];
  static ObHashJoin hash_join_;
  static ObJoinFakeTable left_op_;
  static ObJoinFakeTable right_op_;
  static ObJoinFakeTable out_data_;
  static ObArenaAllocator allocator_;
  static int32_t projector_[2];
  static int64_t projector_size_;
};

ObPhysicalPlan ObHashJoinPlan::phy_plan_;
MockSqlExpression ObHashJoinPlan::equal_expr_[2];
MockSqlExpression ObHashJoinPlan::other_expr_[2];
ObHashJoin ObHashJoinPlan::hash_join_(ObHashJoinPlan::phy_plan_.get_allocator());
ObJoinFakeTable ObHashJoinPlan::left_op_;
ObJoinFakeTable ObHashJoinPlan::right_op_;
ObJoinFakeTable ObHashJoinPlan::out_data_;
ObArenaAllocator ObHashJoinPlan::allocator_(ObModIds::OB_PAGE_ARENA);
int32_t ObHashJoinPlan::projector_[2];
int64_t ObHashJoinPlan::projector_size_;

ObHashJoinTest::ObHashJoinTest()
{
}

ObHashJoinTest::~ObHashJoinTest()
{
}

void ObHashJoinTest::join_test(int64_t case_id, ObJoinType join_type)
{
  ASSERT_EQ(OB_SUCCESS, ObHashJoinPlan::init(case_id, join_type));
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

  ObHashJoin &hash_join = ObHashJoinPlan::get_instance();
  ASSERT_EQ(OB_SUCCESS, hash_join.open(exec_ctx));
  ObJoinFakeTable &out_data = ObHashJoinPlan::get_out_data();
  ASSERT_EQ(OB_SUCCESS, out_data.open(exec_ctx));

  int join_ret = OB_SUCCESS;
  int out_ret = OB_SUCCESS;
  const ObNewRow *join_row = NULL;
  const ObNewRow *out_row = NULL;
  std::vector<std::vector<int64_t>> join_res;
  std::vector<std::vector<int64_t>> out_res;
  while (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
    join_ret = hash_join.get_next_row(exec_ctx, join_row);
    out_ret = out_data.get_next_row(exec_ctx, out_row);
    usleep(10 * 1000);

    ASSERT_EQ(join_ret, out_ret);
    if (OB_SUCCESS == join_ret && OB_SUCCESS == out_ret) {
      ObObj *join_cells = join_row->cells_;
      std::vector<int64_t> res;
      res.resize(4);
      for (int i = 0;i < 4; i++) {
        res.at(i) = join_cells[i].is_null() ? 0 : join_cells[i].get_int();
      }
      join_res.push_back(res);
      ObObj *out_cells = out_row->cells_;
      for (int i = 0;i < 4; i++) {
        res.at(i) = out_cells[i].is_null() ? 0 : out_cells[i].get_int();
      }
      out_res.push_back(res);
    }
  } // while
  auto cmp_func = [](const std::vector<int64_t> &l, std::vector<int64_t> r) {
    for (int i = 0; i < l.size(); i++) {
      if (l.at(i) != r.at(i)) {
        return l.at(i) < r.at(i);
      }
    }
    return false;
  };

  std::sort(join_res.begin(), join_res.end(), cmp_func);
  std::sort(out_res.begin(), out_res.end(), cmp_func);

  for (int i = 0; i < join_res.size(); i++) {
    ASSERT_FALSE(cmp_func(join_res.at(i), out_res.at(i)));
    ASSERT_FALSE(cmp_func(out_res.at(i), join_res.at(i)));
  }

  ASSERT_EQ(OB_ITER_END, join_ret);
  ASSERT_EQ(OB_ITER_END, out_ret);
  ASSERT_EQ(OB_ITER_END, hash_join.get_next_row(exec_ctx, join_row));
  ASSERT_EQ(OB_ITER_END, out_data.get_next_row(exec_ctx, out_row));
  ASSERT_EQ(OB_SUCCESS, hash_join.close(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, out_data.close(exec_ctx));
  ObHashJoinPlan::reuse();
}

void ObHashJoinTest::serialize_test()
{
  ObHashJoin &hash_join_1 = ObHashJoinPlan::get_instance();
  ObArenaAllocator alloc;
  ObHashJoin hash_join_2(alloc);
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObHashJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, hash_join_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, hash_join_1.get_serialize_size());
  int64_t data_len = pos;

  hash_join_2.set_phy_plan(const_cast<ObPhysicalPlan *>(hash_join_1.get_phy_plan()));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, hash_join_2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  const char *str_1 = to_cstring(hash_join_1);
  const char *str_2 = to_cstring(hash_join_2);
  ASSERT_EQ(0, strcmp(str_1, str_2));

  ObHashJoinPlan::reuse();
}

void ObHashJoinTest::join_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  const ObNewRow *row = NULL;

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(4));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObHashJoin &hash_join = ObHashJoinPlan::get_instance();

  if (OB_FAIL(ObHashJoinPlan::init(0, FULL_OUTER_JOIN))) {}
  else if (OB_FAIL(hash_join.open(exec_ctx))) {}
  else {
    while (OB_SUCC(ret)) {
      ret = hash_join.get_next_row(exec_ctx, row);
    }
    if (OB_ITER_END == ret) {
      ret = hash_join.close(exec_ctx);
    }
  }
  ObHashJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void ObHashJoinTest::serialize_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObHashJoin &hash_join = ObHashJoinPlan::get_instance();
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObHashJoinPlan::init(0, INNER_JOIN));

  int64_t pos = 0;
  if (OB_FAIL(hash_join.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos))) {}
  else {
    int64_t data_len = pos;
    pos = 0;
    ObHashJoinPlan::reuse();
    hash_join.set_phy_plan(ObHashJoinPlan::get_phy_plan());
    if (OB_FAIL(hash_join.deserialize(buf, data_len, pos))) {}
  }
  ObHashJoinPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void ObHashJoinTest::memlimit_exception_test(int expect_ret, int64_t memsize)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, ObHashJoinPlan::init(0, INNER_JOIN));
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
  ASSERT_EQ(OB_SUCCESS, my_session->load_default_sys_variable(false, true));  ASSERT_EQ(OB_SUCCESS, my_session->init_tenant(tenant_name, tenant_id));
  ObHashJoin &hash_join = ObHashJoinPlan::get_instance();
  oceanbase::lib::set_wa_limit(10, memsize);
  ret =  hash_join.open(exec_ctx);
  ObHashJoinPlan::reuse();
  // need remote condition
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

TEST_F(ObHashJoinTest, join_case_0)
{
  join_test(0, INNER_JOIN);
  join_test(0, LEFT_OUTER_JOIN);
  join_test(0, RIGHT_OUTER_JOIN);
  join_test(0, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, join_case_1)
{
  join_test(1, INNER_JOIN);
  join_test(1, LEFT_OUTER_JOIN);
  join_test(1, RIGHT_OUTER_JOIN);
  join_test(1, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, join_case_2)
{
  join_test(2, INNER_JOIN);
  join_test(2, LEFT_OUTER_JOIN);
  join_test(2, RIGHT_OUTER_JOIN);
  join_test(2, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, join_case_3)
{
  join_test(3, INNER_JOIN);
  join_test(3, LEFT_OUTER_JOIN);
  join_test(3, RIGHT_OUTER_JOIN);
  join_test(3, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, join_case_4)
{
  join_test(4, INNER_JOIN);
  join_test(4, LEFT_OUTER_JOIN);
  join_test(4, RIGHT_OUTER_JOIN);
  join_test(4, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, join_case_5)
{
  join_test(5, INNER_JOIN);
  join_test(5, LEFT_OUTER_JOIN);
  join_test(5, RIGHT_OUTER_JOIN);
  join_test(5, FULL_OUTER_JOIN);
}

TEST_F(ObHashJoinTest, memlimit_test)
{
  memlimit_exception_test(OB_ALLOCATE_MEMORY_FAILED, 100*1024*1024);
  memlimit_exception_test(OB_SUCCESS, 100*1024*1024);
}

#define JOIN_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    join_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

#define SERIALIZE_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/join/"file, func, key, err); \
    serialize_exception_test(expect_ret); \
    TP_SET_ERROR("engine/join/"file, func, key, NULL); \
  } while (0)

int main(int argc, char **argv)
{
  init_sql_factories();
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
