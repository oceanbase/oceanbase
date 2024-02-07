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
#include "sql/parser/ob_parser.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_print_visitor.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/ob_sql_init.h"
#include "../test_sql_utils.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/resolver/expr/ob_raw_expr_deduce_type.h"
#define private public
#include "sql/resolver/ob_stmt_resolver.h"
#include <fstream>
#include <iterator>

using namespace oceanbase::common;
using namespace oceanbase::sql;

#define BUF_LEN 102400
namespace test
{
class TestRawExpr: public TestSqlUtils, public ::testing::Test
{
public:
  TestRawExpr();
  virtual ~TestRawExpr();
  virtual void SetUp();
  virtual void TearDown();
  ObArenaAllocator allocator;
  ObRawExprFactory expr_factory_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRawExpr);
protected:
  // function members
  int get_raw_expr(const char *expr, ObRawExpr *&raw_expr, const char *&json_expr);
  int get_result_tree(const char *sql,ParseResult &parse_result, ObIAllocator &allocator);
  void resolver(const char *expr, const char *&json_expr);
protected:
  // data members

};

TestRawExpr::TestRawExpr() : allocator(ObModIds::TEST), expr_factory_(allocator)
{
}

TestRawExpr::~TestRawExpr()
{
}

void TestRawExpr::SetUp()
{
}

void TestRawExpr::TearDown()
{
}
int TestRawExpr::get_result_tree(const char *sql,ParseResult &parse_result, ObIAllocator &allocator)
{
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator, mode);
  ObString query = ObString::make_string(sql);
  int ret = OB_SUCCESS;
  ret  = parser.parse(query, parse_result);
  return ret;
}
int TestRawExpr::get_raw_expr(const char *expr, ObRawExpr *&raw_expr, const char *&json_expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> expr_store;
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;

  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory_, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ObString ob_str(expr);
  ret = ObRawExprUtils::make_raw_expr_from_str(ob_str, ctx, raw_expr, columns, sys_vars, &sub_query_info, aggr_exprs ,win_exprs, udf_info, op_exprs);
  UNUSED(json_expr);
  return ret;
}

void TestRawExpr::resolver(const char *expr, const char *&json_expr)
{
  const char *canon_expr = NULL;
  ObRawExpr *raw_expr = NULL;
  if (OB_SUCCESS == get_raw_expr(expr, raw_expr, canon_expr)) {
    char buf[BUF_LEN];
    int64_t pos = 0;
    raw_expr->get_name_internal(buf, BUF_LEN, pos, EXPLAIN_EXTENDED);
    _OB_LOG(INFO, "===========================================\n");
    _OB_LOG(INFO, "%s\n", buf);
    json_expr = CSJ(raw_expr);
  } else {
    json_expr = "ERR----TEST";
  }
}

TEST_F(TestRawExpr, basic_test)
{
  const char *canon_expr = NULL;

  ObRawExpr *praw_expr = NULL;
  get_raw_expr("1", praw_expr, canon_expr);
  ObRawExprPrintVisitor pvistor(*praw_expr);
  ObRawExpr *raw_expr1 = NULL;
  get_raw_expr("1", raw_expr1, canon_expr);
  ObConstRawExpr *const_expr1 = dynamic_cast<ObConstRawExpr *>(raw_expr1);
  ObRawExpr *raw_expr2 = NULL;
  get_raw_expr("1.5", raw_expr2, canon_expr);
  ObConstRawExpr *const_expr2 = dynamic_cast<ObConstRawExpr *>(raw_expr2);

  ASSERT_FALSE(const_expr2->same_as(*const_expr1));
  (*const_expr1).assign (*const_expr2);
  ASSERT_TRUE(const_expr2->same_as(*const_expr1));
  char buf[BUF_LEN];
  char str_buf[BUF_LEN];
  pvistor.to_string(buf, BUF_LEN);
  OK(const_expr1->preorder_accept(pvistor));
  OK(const_expr1->postorder_accept(pvistor));
  OK(const_expr1->postorder_replace(pvistor));
  _OB_LOG(INFO, "\nconst_expr------\n%s ", buf);
  int64_t pos = 0;
  const_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_FORMAT_JSON);
  raw_expr2->reset();
  const_expr2->reset();

  get_raw_expr("c1 in (select t1 from table1)", raw_expr1, canon_expr);
  ObOpRawExpr *op_expr1 = dynamic_cast<ObOpRawExpr *>(raw_expr1);
  get_raw_expr("c2 not in (select t2 from table1)", raw_expr2, canon_expr);
  ObOpRawExpr *op_expr2 = dynamic_cast<ObOpRawExpr *>(raw_expr2);

  ASSERT_FALSE(ObRawExprUtils::is_same_raw_expr(op_expr1, op_expr2));
  ObArray<ObRawExpr*> expr_store;
  OK(ObRawExprUtils::extract_column_exprs(op_expr1, expr_store));
  OK(op_expr1->preorder_accept(pvistor));
  OK(op_expr1->postorder_accept(pvistor));
  _OB_LOG(INFO, "\nop_expr------\n%s ", buf);

  ObColumnRefRawExpr *bin_expr1 = dynamic_cast<ObColumnRefRawExpr *>(op_expr1->get_param_expr(0));
  ObColumnRefRawExpr *bin_expr2 = dynamic_cast<ObColumnRefRawExpr *>(op_expr2->get_param_expr(0));

  bin_expr1->set_ref_id(1,1);
  bin_expr2->set_ref_id(2,2);
  bin_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);
  bin_expr2->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);

  ASSERT_FALSE(bin_expr1->same_as(*bin_expr2));
  (*bin_expr1).assign(*bin_expr2);
  ASSERT_TRUE(bin_expr1->same_as(*bin_expr2));
  ObRawExpr *copy_expr = NULL;;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::copy_expr(expr_factory_, bin_expr1, copy_expr, true, true, true));
  ASSERT_TRUE(bin_expr1->same_as(*copy_expr));

  OK(bin_expr1->preorder_accept(pvistor));
  OK(bin_expr1->postorder_accept(pvistor));
  OK(bin_expr1->postorder_replace(pvistor));
  _OB_LOG(INFO, "\nbin_expr------\n%s", buf);

  ObQueryRefRawExpr *sub_expr1 = dynamic_cast<ObQueryRefRawExpr *>(op_expr1->get_param_expr(1));
  ObQueryRefRawExpr *sub_expr2 = dynamic_cast<ObQueryRefRawExpr *>(op_expr2->get_param_expr(1));
  OK(ObRawExprUtils::copy_expr(expr_factory_, sub_expr1, copy_expr, false, true, true));
  ASSERT_TRUE(sub_expr1->same_as(*sub_expr1));
  ObRawExpr *texpr1 = NULL;
  ObRawExpr *texpr2 = NULL;
  ASSERT_FALSE(ObRawExprUtils::is_same_raw_expr(texpr1, sub_expr2));
  ASSERT_TRUE(ObRawExprUtils::is_same_raw_expr(texpr1, texpr2));
  sub_expr1->set_ref_id(1);
  sub_expr2->set_ref_id(2);
  ASSERT_FALSE(ObRawExprUtils::is_same_raw_expr(sub_expr1, sub_expr2));
  ASSERT_TRUE(ObRawExprUtils::is_same_raw_expr(sub_expr1, sub_expr1));
  ASSERT_FALSE(sub_expr1->same_as(*sub_expr2));
  (*sub_expr1).assign(*sub_expr2);
  ASSERT_TRUE(sub_expr1->same_as(*sub_expr2));
  sub_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);

  OK(sub_expr1->preorder_accept(pvistor));
  OK(sub_expr1->postorder_accept(pvistor));
  OK(sub_expr1->postorder_replace(pvistor));
  OK(sub_expr2->preorder_accept(pvistor));
  OK(sub_expr2->postorder_accept(pvistor));
  OK(sub_expr2->postorder_replace(pvistor));
  _OB_LOG(INFO, "\nsub_expr------\n%s", buf);

  ObOpRawExpr op_expr(bin_expr1, sub_expr1, T_OP_IN);
  op_expr.set_param_expr(bin_expr1);
  op_expr.set_param_exprs(bin_expr1, sub_expr1);
  op_expr.set_param_exprs(bin_expr1, sub_expr1, NULL);
  op_expr.clear_child();
  OK(op_expr.set_param_expr(bin_expr1));
  op_expr.clear_child();
  OK(op_expr.set_param_exprs(bin_expr1, sub_expr1));
  op_expr.clear_child();
  OK(op_expr.set_param_exprs(bin_expr1, sub_expr1, NULL));
  op_expr.reset();
  ObRawExprPrintVisitor op_vistor(*op_expr1);
  char op_buf[1024];
  op_vistor.to_string(op_buf, sizeof(op_buf));
  OK(op_expr1->postorder_replace(op_vistor));
  ASSERT_FALSE(op_expr1->same_as(*op_expr2));
  (*op_expr1).assign(*op_expr2);
  ASSERT_TRUE(op_expr1->same_as(*op_expr2));

  sub_expr1->reset();
  bin_expr1->reset();

  get_raw_expr("case name  when 'sam' then 'yong'  when 'lee' then 'handsome'  else 'good' end", raw_expr1, canon_expr);
  get_raw_expr("case when 1>0 then 'true' else 'false' end", raw_expr2, canon_expr);
  ObCaseOpRawExpr *case_expr1 = dynamic_cast<ObCaseOpRawExpr *>(raw_expr1);
  ObCaseOpRawExpr *case_expr2 = dynamic_cast<ObCaseOpRawExpr *>(raw_expr2);
  OK(ObRawExprUtils::copy_expr(expr_factory_, raw_expr1, copy_expr, false, true, true));

  OK(case_expr1->preorder_accept(pvistor));
  OK(case_expr1->postorder_accept(pvistor));
  case_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);
  ASSERT_FALSE(case_expr1->same_as(*case_expr2));
  (*case_expr1).assign(*case_expr2);
  ASSERT_TRUE(case_expr1->same_as(*case_expr2));
  ObRawExprPrintVisitor case_vistor(*case_expr1);
  char case_buf[1024];
  case_vistor.to_string(case_buf, sizeof(case_buf));
  OK(case_expr1->postorder_replace(case_vistor));
  case_expr1->reset();
  case_expr2->reset();
  _OB_LOG(INFO, "\ncase_expr------\n%s", buf);

  get_raw_expr("count(*)", raw_expr1, canon_expr);
  get_raw_expr("max(c1)", raw_expr2, canon_expr);
  ObAggFunRawExpr *agg_expr1 = dynamic_cast<ObAggFunRawExpr *>(raw_expr1);
  ObAggFunRawExpr *agg_expr2 = dynamic_cast<ObAggFunRawExpr *>(raw_expr2);

  _OB_LOG(INFO, "\nstr_buf------\n%s",str_buf);

  ObRawExprPrintVisitor agg_vistor(*agg_expr1);
  char agg_buf[1024];
  agg_vistor.to_string(agg_buf, sizeof(agg_buf));
  OK(agg_expr1->preorder_accept(pvistor));
  OK(agg_expr1->postorder_accept(pvistor));
  OK(agg_expr1->postorder_replace(agg_vistor));
  _OB_LOG(INFO, "\nagg_buf--post_replace----\n%s",agg_buf);
  ASSERT_FALSE(agg_expr1->same_as(*agg_expr2));
  (*agg_expr1).assign(*agg_expr2);
  ASSERT_TRUE(agg_expr1->same_as(*agg_expr2));
  agg_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);
  agg_expr1->reset();

  get_raw_expr("charset(\"test\")", raw_expr1, canon_expr);
  get_raw_expr("now()", raw_expr2, canon_expr);
  ObSysFunRawExpr *sys_expr1 = dynamic_cast<ObSysFunRawExpr *>(raw_expr1);
  ObSysFunRawExpr *sys_expr2 = dynamic_cast<ObSysFunRawExpr *>(raw_expr2);
  sys_expr1->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);
  sys_expr2->get_name_internal(str_buf, BUF_LEN, pos, EXPLAIN_EXTENDED);

  ObRawExprPrintVisitor sys_vistor(*sys_expr1);
  char sys_buf[1024];
  sys_vistor.to_string(sys_buf, sizeof(sys_buf));
  OK(sys_expr1->preorder_accept(pvistor));
  OK(sys_expr1->postorder_accept(pvistor));
  OK(sys_expr1->postorder_replace(sys_vistor));
  ASSERT_FALSE(sys_expr1->same_as(*sys_expr2));
  (*sys_expr1).assign(*sys_expr2);
  ASSERT_TRUE(sys_expr1->same_as(*sys_expr2));
  sys_expr1->reset();
  sys_expr2->reset();
  _OB_LOG(INFO, "\n---------\n%s", buf);
  _OB_LOG(INFO, "\n-----get_name_intrenal------\n%s", str_buf);
}

TEST_F(TestRawExpr, special_test)
{
  static const char* test_file = "./expr/test_raw_op_expr.test";
  static const char* tmp_file = "./expr/test_raw_op_expr.tmp";
  static const char* result_file = "./expr/test_raw_op_expr.result";

  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* json_expr = NULL;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t case_id = 0;
  while (std::getline(if_tests, line)) {
    of_result << '[' << case_id++ << "] " << line << std::endl;
    resolver(line.c_str(), json_expr);
    of_result << json_expr << std::endl;
  }
  of_result.close();
  // verify results
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  is_equal_content(tmp_file, result_file);
}
}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  init_sql_factories();
  test::parse_cmd_line_param(argc, argv, test::clp);
  return RUN_ALL_TESTS();
}
