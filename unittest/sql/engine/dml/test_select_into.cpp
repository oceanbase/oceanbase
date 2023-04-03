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
#include <fstream>
#define private  public
#define protected  public
#include "lib/utility/ob_test_util.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "sql/test_sql_utils.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/basic/ob_select_into.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/test_engine_util.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObSelectIntoTest: public ::testing::Test
{
  static const uint64_t TABLE_ID = 1099511677877;
public:
  ObSelectIntoTest();
  virtual ~ObSelectIntoTest();
protected:
  void test_into_file(int64_t row_count, const char *name);
  void init_op_for_file(ObSelectInto &op_into);
  int add_rows(ObFakeTable &table, const int64_t row_num);
  int init_op_for_variables(ObSelectInto *op_into);
  int create_expr_values(int64_t num, ObExprValues *&expr_values);
  int create_sql_expr(ObObj &obj, ObSqlExpression *&expr);
  // disallow copy
  ObSelectIntoTest(const ObSelectIntoTest &other);
  ObSelectIntoTest& operator=(const ObSelectIntoTest &other);
private:
  ObPhysicalPlan physical_plan_;
  ObString vars_str_[10];
  int64_t vars_count_;
  ObObj obj_val_[10];
};

ObSelectIntoTest::ObSelectIntoTest()
{

}

ObSelectIntoTest::~ObSelectIntoTest()
{
}


int ObSelectIntoTest::create_sql_expr(ObObj &obj, ObSqlExpression *&expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem expr_item;

  if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_sql_expr(&physical_plan_, expr))) {
    SQL_ENG_LOG(WARN, "make expression failed", K(ret));
  } else if (OB_SUCCESS != (ret = expr_item.assign(obj))) {
    SQL_ENG_LOG(WARN, "assign expr item failed", K(ret));
  } else if (OB_FAIL(expr->set_item_count(1))) {
    SQL_ENG_LOG(WARN, "fail to set item count", K(ret));
  } else if (OB_SUCCESS != (ret = expr->add_expr_item(expr_item))) {
    SQL_ENG_LOG(WARN, "add expression item failed", K(ret));
  }
  return ret;
}

void ObSelectIntoTest::init_op_for_file(ObSelectInto &op_into)
{
  UNUSED(op_into);
}

int ObSelectIntoTest::add_rows(ObFakeTable &table, const int64_t row_num)
{
  int ret = OB_SUCCESS;
  ObNewRow *row = NULL;
  int64_t column_count = 5;
  ObObj *objs = NULL;
  if (OB_ISNULL(row = static_cast<ObNewRow *>(physical_plan_.get_allocator().alloc(sizeof(ObNewRow))))) {
    SQL_ENG_LOG(WARN, "str is null", K(ret));
  } else if (OB_ISNULL(objs = static_cast<ObObj *>(physical_plan_.get_allocator().alloc(sizeof(ObObj) * 5)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0 ; i < column_count ; i++) {
      new(objs + i)ObObj();
      (objs + i)->set_int(row_num * column_count + i);
    }
    row->cells_ = objs;
    row->count_ = column_count;
    ret = table.add_row(*row);
  }
  return ret;
}

int ObSelectIntoTest::init_op_for_variables(ObSelectInto *op_into)
{
  int ret = OB_SUCCESS;
  op_into->set_column_count(5);
  op_into->set_id(1);
  op_into->set_into_type(T_INTO_VARIABLES);
  vars_count_ = 5;
  common::ObSEArray<ObString, 16> user_vars;
  for (int i = 0 ; i < vars_count_; i++) {
    char *str = NULL;
    if (OB_ISNULL(str = static_cast<char *>(physical_plan_.get_allocator().alloc(2)))) {
      SQL_ENG_LOG(WARN, "str is null", K(ret));
    } else {
      str[0] = static_cast<char>('a' + i);
      vars_str_[i].assign(str, 1);
      if (OB_FAIL(user_vars.push_back(vars_str_[i]))) {
        SQL_ENG_LOG(WARN, "push back varstr failed", K(ret));
      }
    }
  }
  op_into->set_user_vars(user_vars);
  return ret;
}

int ObSelectIntoTest::create_expr_values(int64_t num, ObExprValues *&expr_values)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(physical_plan_.alloc_operator_by_type(PHY_EXPR_VALUES, expr_values))) {
  } else {
    expr_values->set_column_count(5);
    ObObj obj;
    if (OB_FAIL(expr_values->init_value_count(num))) {
      SQL_ENG_LOG(WARN, "init expr values count failed", K(ret));
    }
    for (int64_t i = 0  ; i < num && OB_SUCC(ret);i++) {
      ObSqlExpression *expr = NULL;
      obj.set_int(i);
      obj_val_[i] = obj;
      if (OB_FAIL(create_sql_expr(obj, expr))) {
      } else if (OB_FAIL(expr_values->add_value(expr))) {
      } else {}
    }
  }
  return ret;
}

void ObSelectIntoTest::test_into_file(int64_t row_count, const char *name)
{
  //int ret = OB_SUCCESS;
  ObSelectInto op_into(physical_plan_.get_allocator());
  ObFakeTable fake_op;
  ObExecContext exec_ctx;
  ObPhysicalPlan physical_plan;

  // init op_into
  op_into.set_column_count(5);
  op_into.set_id(1);
  op_into.set_into_type(T_INTO_OUTFILE);
  ObObj file_name;
  file_name.set_varchar(name);
  op_into.set_outfile_name(file_name);
  ObObj field_str;
  field_str.set_varchar(",");
  op_into.set_filed_str(field_str);
  ObObj line_str;
  line_str.set_varchar("\n");
  op_into.set_line_str(line_str);
  char enclosed_str = '|';
  bool is_optional = false;
  op_into.set_closed_cht(enclosed_str);
  op_into.set_is_optional(is_optional);
  op_into.set_phy_plan(&physical_plan);

  fake_op.set_id(0);
  fake_op.set_phy_plan(&physical_plan);
  fake_op.set_parent(&op_into);
  fake_op.set_column_count(5);
  for (int64_t i = 0 ;i < row_count ; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_rows(fake_op, i));
  }

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(2));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.get_my_session()->set_time_zone(ObString("+8:00"), true, true));
  my_session = exec_ctx.get_my_session();
  ASSERT_TRUE(NULL != my_session);
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, my_session->load_default_sys_variable(false, false));

  OK(op_into.set_child(0, fake_op)); // set child
  OK(op_into.open(exec_ctx));
  OK(op_into.close(exec_ctx));
}

TEST_F(ObSelectIntoTest, test_into_file)
{
  const char* tmp_file = "test_select_into_file.tmp";
  const char* result_file = "test_select_into_file.result";
  std::remove(tmp_file);

  test_into_file(100, tmp_file);

  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  bool is_equal = std::equal(it_result, std::istream_iterator<std::string>(), it_expected);
  if (is_equal) {
    std::remove(tmp_file);
  }
  ASSERT_EQ(true, is_equal);

}

TEST_F(ObSelectIntoTest, test_into_variables)
{
  ObExprValues *expr_values = NULL;
  ASSERT_EQ(OB_SUCCESS, create_expr_values(5, expr_values));
  ASSERT_TRUE(NULL != expr_values);
  ObExecContext exec_ctx;
  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(2));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_ctx));
  session = exec_ctx.get_my_session();
  ASSERT_TRUE(NULL != session);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.get_my_session()->set_time_zone(ObString("+8:00"), true, true));
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session->load_default_sys_variable(false, false));
  ObSelectInto *op_into = NULL;
  ASSERT_EQ(OB_SUCCESS, physical_plan_.alloc_operator_by_type(PHY_SELECT_INTO, op_into));
  ASSERT_TRUE(NULL != op_into);
  ASSERT_EQ(OB_SUCCESS, init_op_for_variables(op_into));
  ASSERT_EQ(OB_SUCCESS, op_into->set_child(0, *expr_values));
  expr_values->set_parent(op_into);
  ASSERT_EQ(OB_SUCCESS, op_into->open(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, op_into->close(exec_ctx));
  ObObj val;
  for (int64_t i = 0 ; i < vars_count_ ; ++i) {
    ASSERT_EQ(OB_SUCCESS, session->get_user_variable_value(vars_str_[i], val));
    ASSERT_EQ(val, obj_val_[i]);
  }
}

int main(int argc, char **argv)
{
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
