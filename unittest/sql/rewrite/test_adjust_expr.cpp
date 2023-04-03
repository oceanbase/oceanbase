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

#include <fstream>
#include <iterator>
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/oblog/ob_log.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker2.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/rewrite/ob_subquery_unnested_rewrite.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "../test_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace test
{

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

class TestAdjustExpr: public ::testing::Test, public TestSqlUtils
{
public:
  static const int64_t MAX_OP_NUM = 32;

  TestAdjustExpr();
  virtual ~TestAdjustExpr();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestAdjustExpr);
};

TestAdjustExpr::TestAdjustExpr()
{
}

TestAdjustExpr::~TestAdjustExpr()
{
}

void TestAdjustExpr::SetUp()
{
}

void TestAdjustExpr::TearDown()
{
}

TEST_F(TestAdjustExpr, basic_test)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  schema::ObSchemaManager *schema_mgr = new schema::ObSchemaManager();
  ASSERT_TRUE(schema_mgr);
  OK(schema_mgr->init());
  // create schema
  const char *t1 = "CREATE TABLE t1(c1 INT PRIMARY KEY, c2 INT)";
  do_create_table(t1);

  // temp test
  const char *query_string = "select c1+1, c1+1 from t1";
  ObStmt *query_stmt = NULL;
  do_resolve(query_string, query_stmt);
  ObSelectStmt *query_select = static_cast<ObSelectStmt *>(query_stmt);
  ObOpRawExpr *first_expr = static_cast<ObOpRawExpr * >(query_select->get_select_item(0).expr_);
  ObOpRawExpr *second_expr = static_cast<ObOpRawExpr * >(query_select->get_select_item(1).expr_);
  ObRawExpr *test1 = first_expr;
  ObRawExpr *test2 = second_expr;
  LOG_INFO("=========================================");
  LOG_INFO("rawexpr", K(test1));
  LOG_INFO("rawexpr", K(test2));
  LOG_INFO("rawexpr", K(*test1));
  LOG_INFO("rawexpr", K(*test2));
  LOG_INFO("=========================================");
  EXPECT_TRUE(test1 != test2);

  // run tests
  int ret = OB_SUCCESS;
  ObStmt *stmt = NULL;
  const char *query = "select s.c1 from (select c1 from t1) s";
  do_resolve(allocator, schema_mgr, query, stmt);
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
  uint64_t table_id = select_stmt->get_from_item(0).table_id_;
  ObStmt *child_query = select_stmt->get_table_item_by_id(table_id)->ref_query_;
  ObSelectStmt *child_stmt = static_cast<ObSelectStmt *>(child_query);
  ObSubqueryNestedRewrite rewriter;
  // adjust main stmt's select item
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
    ObRawExpr *&expr = select_stmt->get_select_item(i).expr_;
    if (OB_SUCCESS != (ret = rewriter.adjust_query_expr(&allocator,
                                                        expr,
                                                        table_id,
                                                        child_stmt))) {
      LOG_WARN("fail to adjust expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // push down main stmt's select item
    child_stmt->clear_select_item();
    for (int64_t i = 0; i < select_stmt->get_select_item_size(); ++i) {
      child_stmt->add_select_item(select_stmt->get_select_item(i));
    }
  }
  LOG_INFO("child_stmt", K(*child_stmt));

  // for calc
  ObNewRow row;
  ObObj input;
  input.set_int(10);
  row.cells_ = &input;
  row.count_ = 1;
  ObObj result;
  CALC_RAW_EXPR(*child_stmt->get_select_item(0).expr_, row, result);

  // EXPECT_TRUE(OB_SUCC(ret));
  // EXPECT_TRUE(11 == result.get_int());

  // destroy
  if (NULL != schema_mgr) {
    delete schema_mgr;
  }
}

}  // end test

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
