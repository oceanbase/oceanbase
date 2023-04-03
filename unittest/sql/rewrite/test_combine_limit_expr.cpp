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

#define USING_LOG_PREFIX SQL_REWRITE

#include <gtest/gtest.h>
#include "lib/json/ob_json.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_subquery_unnested_rewrite.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace test
{
static const int64_t MAX_OP_NUM = 32;

#define MAKE_RAW_EXPR_FROM_STR(str, expr)                                       \
  ({                                                                            \
    ObArray<ObAggFunRawExpr*> aggr_exprs;                                       \
    ObArray<ObWinFunRawExpr*> win_exprs;                                        \
    ObArray<ObUDFInfo> udf_info;                                               \
	  ret = ObRawExprUtils::make_raw_expr_from_str(str,                           \
                                                 strlen(str),                   \
                                                 allocator,                     \
                                                 ctx,                           \
                                                 expr,                          \
                                                 columns,                       \
                                                 sys_vars,                      \
                                                 &sub_query_info,               \
                                                 aggr_exprs,                    \
                                                 win_exprs,                     \
                                                 udf_info);                     \
    ret;                                                                        \
  })

// calc ObRawExpr with an empty row
#define CALC_RAW_EXPR(raw_expr, row, result)                                                      \
	({                                                                                              \
    RowDesc row_desc;                                                        \
    ObSqlExpression sql_expr;                                                                     \
    sql_expr.set_str_buf(&allocator);                                                             \
    ObExprGeneratorImpl expr_generator(row_desc);                                                 \
    expr_generator.generate(raw_expr, sql_expr);                                                  \
    ObExecContext exec_ctx;                                                                       \
    if (OB_FAIL(exec_ctx.init_phy_op(MAX_OP_NUM))) {                                              \
      LOG_WARN("fail to init exec_ctx", "MAX_OP_NUM", static_cast<int64_t>(MAX_OP_NUM), K(ret));  \
    } else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {                            \
      LOG_WARN("fail to create_physical_plan_ctx", K(ret));                                       \
    } else {                                                                                      \
      ObExprCtx expr_ctx;                                                                         \
      expr_ctx.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();                                                          \
      expr_ctx.calc_buf_ = &allocator;                                                            \
      if (OB_FAIL(sql_expr.calc(expr_ctx, row, result))) {                                        \
        LOG_WARN("fail to calc expr", K(row), K(ret));                                            \
      }                                                                                           \
    }                                                                                             \
    ret;                                                                                          \
  })

class TestCombineLimitExpr: public ::testing::Test
{
public:
  TestCombineLimitExpr() {}
  virtual ~TestCombineLimitExpr() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy and assign
  TestCombineLimitExpr(const TestCombineLimitExpr &other);
  TestCombineLimitExpr& operator=(const TestCombineLimitExpr &ohter);
};

TEST_F(TestCombineLimitExpr, basic_test)
{
  int ret = OB_SUCCESS;
  ObExprOperatorGFactory::get_instance()->init();
  ObSubqueryNestedRewrite rewriter;
  // mock params
  // stmts
  ObArenaAllocator allocator(ObModIds::TEST);
  void *select_ptr = allocator.alloc(sizeof(ObSelectStmt));
  void *child_ptr = allocator.alloc(sizeof(ObSelectStmt));
  ObSelectStmt *select_stmt = new(select_ptr) ObSelectStmt;
  ObSelectStmt *child_stmt = new(child_ptr) ObSelectStmt;
  // offset/limit exprs
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObExprResolveContext ctx(allocator);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);

  // for calc
  ObNewRow row;
  ObObj result;

  ObRawExpr *inner_offset_expr = NULL;      // a
  ObRawExpr *inner_limit_expr = NULL;       // b
  ObRawExpr *outer_offset_expr = NULL;      // c
  ObRawExpr *outer_limit_expr = NULL;       // d

  const char* inner_offset = "1";
  const char* inner_limit = "10";
  const char* outer_offset = "2";
  const char* outer_limit = "6";

  // result
  ObRawExpr *result_offset = NULL;
  ObRawExpr *result_limit = NULL;

  // (inner limit a,b) outer limit c,d
  // b or c may be NULL
  /***************************************************************************/
  // b = NULL, c = NULL
  // result_offset = a
  // result_limit = d
  inner_offset_expr = NULL;      // a
  inner_limit_expr = NULL;       // b
  outer_offset_expr = NULL;      // c
  outer_limit_expr = NULL;       // d
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // a, d
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(inner_offset_expr == result_offset);
  EXPECT_TRUE(outer_limit_expr == result_limit);
  /***************************************************************************/
  // b = NULL, c != NULL
  // a = NULL, result_offset = c
  // a != NULL, result_offset = a + c
  // result_limit = d
  inner_offset_expr = NULL;      // a
  inner_limit_expr = NULL;       // b
  outer_offset_expr = NULL;      // c
  outer_limit_expr = NULL;       // d

  // b = NULL
  inner_limit_expr = NULL;
  // c != NULL
  MAKE_RAW_EXPR_FROM_STR(outer_offset, outer_offset_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // a = NULL
  inner_offset_expr = NULL;
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // c, d
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(outer_offset_expr == result_offset);
  EXPECT_TRUE(outer_limit_expr == result_limit);
  //---------------------------------------------------------------------------
  // a != NULL
  MAKE_RAW_EXPR_FROM_STR(inner_offset, inner_offset_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // a+c, d
  EXPECT_TRUE(OB_SUCC(ret));
  CALC_RAW_EXPR(*result_offset, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(3 == result.get_int());
  EXPECT_TRUE(outer_limit_expr == result_limit);
  /***************************************************************************/
  // b != NULL, c = NULL
  // result_offset = a
  // d = NULL, result_limit = b
  // d != NULL, result_limist = min(b, d)
  inner_offset_expr = NULL;      // a
  inner_limit_expr = NULL;       // b
  outer_offset_expr = NULL;      // c
  outer_limit_expr = NULL;       // d

  // b != NULL
  MAKE_RAW_EXPR_FROM_STR(inner_limit, inner_limit_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // c = NULL
  outer_offset_expr = NULL;
  // d = NULL
  outer_limit_expr = NULL;
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // a, b
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(inner_offset_expr == result_offset);
  EXPECT_TRUE(inner_limit_expr == result_limit);
  //---------------------------------------------------------------------------
  // d != NULL
  MAKE_RAW_EXPR_FROM_STR(outer_limit, outer_limit_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // a, min(b, d)
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(inner_offset_expr == result_offset);
  CALC_RAW_EXPR(*result_limit, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(6 == result.get_int());
  /***************************************************************************/
  // b != NULL, c != NULL
  // if b >= c
  //   a = NULL, result_offset = c
  //   a != NULL, result_offset = a+c
  //   d = NULL, result_limit = b-c
  //   d != NULL, result_limit = min(b-c, d)
  // else
  //   result_offset = 0
  //   result_limit = 0
  inner_offset_expr = NULL;      // a
  inner_limit_expr = NULL;       // b
  outer_offset_expr = NULL;      // c
  outer_limit_expr = NULL;       // d
  // b != NULL
  MAKE_RAW_EXPR_FROM_STR(inner_limit, inner_limit_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // c != NULL
  MAKE_RAW_EXPR_FROM_STR(outer_offset, outer_offset_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // a = NULL, d = NULL
  inner_offset_expr = NULL;
  outer_limit_expr = NULL;
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // c, b-c
  EXPECT_TRUE(OB_SUCC(ret));
  // LOG_INFO("raw expr", K(*result_offset));
  // LOG_INFO("raw expr", K(*result_limit));
  // LOG_INFO("result_offset", K(*result_offset));
  CALC_RAW_EXPR(*result_offset, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(2 == result.get_int());
  CALC_RAW_EXPR(*result_limit, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(8 == result.get_int());
  //---------------------------------------------------------------------------
  // a != NULL
  MAKE_RAW_EXPR_FROM_STR(inner_offset, inner_offset_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // b != NULL
  MAKE_RAW_EXPR_FROM_STR(inner_limit, inner_limit_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // c != NULL
  MAKE_RAW_EXPR_FROM_STR(outer_offset, outer_offset_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // d != NULL
  MAKE_RAW_EXPR_FROM_STR(outer_limit, outer_limit_expr);
  EXPECT_TRUE(OB_SUCC(ret));
  // test
  child_stmt->set_limit_offset(inner_limit_expr, inner_offset_expr);
  select_stmt->set_limit_offset(outer_limit_expr, outer_offset_expr);
  ret = rewriter.combine_limit_expr(&allocator,
                                    select_stmt,
                                    child_stmt,
                                    result_offset,
                                    result_limit);
  // a + c, min(b-c, d)
  EXPECT_TRUE(OB_SUCC(ret));
  CALC_RAW_EXPR(*result_offset, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(3 == result.get_int());
  CALC_RAW_EXPR(*result_limit, row, result);
  EXPECT_TRUE(OB_SUCC(ret));
  EXPECT_TRUE(6 == result.get_int());
}

}  // end test

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
