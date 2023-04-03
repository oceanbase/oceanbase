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
#include "lib/utility/ob_test_util.h"
#include "sql/resolver/expr/ob_raw_expr_print_visitor.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestRawExprPrintVisitor: public ::testing::Test
{
public:
  TestRawExprPrintVisitor();
  virtual ~TestRawExprPrintVisitor();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRawExprPrintVisitor);
protected:
  // function members
protected:
  // data members
};

TestRawExprPrintVisitor::TestRawExprPrintVisitor()
{
}

TestRawExprPrintVisitor::~TestRawExprPrintVisitor()
{
}

void TestRawExprPrintVisitor::SetUp()
{
}

void TestRawExprPrintVisitor::TearDown()
{
}

TEST_F(TestRawExprPrintVisitor, const_test)
{
  {
    ObObj obj;
    obj.set_int(123);
    ObConstRawExpr expr(obj, T_INT);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
  {
    ObString str = ObString::make_string("abcd");
    ObObj obj;
    obj.set_varchar(str);
    ObConstRawExpr expr(obj, T_VARCHAR);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
  {
    ObObj obj;
    obj.set_null();
    ObConstRawExpr expr(obj, T_NULL);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
  {
    ObObj obj;
    obj.set_int(3);
    ObConstRawExpr expr(obj, T_QUESTIONMARK);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
  {
    number::ObNumber nmb;
    ObArenaAllocator allocator(ObModIds::TEST);
    nmb.from(9000000000L, allocator);
    ObObj obj;
    obj.set_number(nmb);
    ObConstRawExpr expr(obj, T_NUMBER);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
  {
    ObString var = ObString::make_string("sql_mode");
    ObObj obj;
    obj.set_varchar(var);
    ObConstRawExpr expr(obj, T_SYSTEM_VARIABLE);
    _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  }
}

TEST_F(TestRawExprPrintVisitor, unary_ref_test)
{
  ObQueryRefRawExpr expr(1, T_REF_QUERY);
  _OB_LOG(INFO, "unary=%s", S(ObRawExprPrintVisitor(expr)));
}

TEST_F(TestRawExprPrintVisitor, binary_ref_test)
{
  ObColumnRefRawExpr expr(3, 7, T_REF_COLUMN);
  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
}

TEST_F(TestRawExprPrintVisitor, multi_op_test)
{
  ObObj obj;
  obj.set_int(123);
  ObConstRawExpr const_expr1(obj, T_INT);
  ObConstRawExpr const_expr2(obj, T_INT);
  ObConstRawExpr const_expr3(obj, T_INT);
  ObConstRawExpr const_expr4(obj, T_INT);
  ObOpRawExpr expr;
  expr.set_expr_type(T_OP_ROW);
  OK(expr.add_param_expr(&const_expr1));
  OK(expr.add_param_expr(&const_expr2));
  OK(expr.add_param_expr(&const_expr3));
  OK(expr.add_param_expr(&const_expr4));

  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
}

TEST_F(TestRawExprPrintVisitor, case_op_test)
{
  ObObj obj;
  obj.set_int(123);
  ObConstRawExpr const_expr1(obj, T_INT);
  ObConstRawExpr const_expr2(obj, T_INT);
  ObConstRawExpr const_expr3(obj, T_INT);
  ObConstRawExpr const_expr4(obj, T_INT);

  ObCaseOpRawExpr expr;
  expr.set_arg_param_expr(&const_expr1);
  OK(expr.add_when_param_expr(&const_expr2));
  OK(expr.add_then_param_expr(&const_expr3));
  expr.set_default_param_expr(&const_expr4);
  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
}

TEST_F(TestRawExprPrintVisitor, agg_op_test)
{
  ObObj obj;
  obj.set_int(123);
  ObConstRawExpr const_expr1(obj, T_INT);
  ObSEArray<ObRawExpr *, 1, ModulePageAllocator, true> real_param_exprs1;
  OK(real_param_exprs1.push_back(&const_expr1));
  ObAggFunRawExpr expr(real_param_exprs1, true, T_FUN_MAX);
  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
  ObSEArray<ObRawExpr *, 1, ModulePageAllocator, true> real_param_exprs2;
  ObAggFunRawExpr expr2(real_param_exprs2, false, T_FUN_COUNT);
  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr2)));
}

TEST_F(TestRawExprPrintVisitor, sys_fun_test)
{
  ObObj obj;
  obj.set_int(123);
  ObConstRawExpr const_expr1(obj, T_INT);
  ObConstRawExpr const_expr2(obj, T_INT);
  ObSysFunRawExpr expr;
  expr.set_func_name(ObString::make_string("myfunc"));
  OK(expr.add_param_expr(&const_expr1));
  OK(expr.add_param_expr(&const_expr2));
  _OB_LOG(INFO, "%s", S(ObRawExprPrintVisitor(expr)));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
