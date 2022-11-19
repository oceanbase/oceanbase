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

#include "sql/test_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_canonicalizer_impl.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/ob_sql_context.h"
#include "lib/json/ob_json_print_utils.h"
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestRawExprCanonicalizer: public ::testing::Test
{
public:
  TestRawExprCanonicalizer();
  virtual ~TestRawExprCanonicalizer();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRawExprCanonicalizer);
protected:
  // function members
  void canon(const char* expr, const char *&canon_expr);
protected:
  // data members
};

TestRawExprCanonicalizer::TestRawExprCanonicalizer()
{
}

TestRawExprCanonicalizer::~TestRawExprCanonicalizer()
{
}

void TestRawExprCanonicalizer::SetUp()
{
}

void TestRawExprCanonicalizer::TearDown()
{
}

void TestRawExprCanonicalizer::canon(const char* expr, const char *&canon_expr)
{
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  const char *expr_str = expr;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObTimeZoneInfo tz_info;
  ObStmt stmt;
  ObQueryCtx query_ctx;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  stmt.query_ctx_ = &query_ctx;
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ctx.stmt_ = &stmt;
  ObSQLSessionInfo session;
  ctx.session_info_ = &session;
  ObRawExpr *raw_expr = NULL;
  OK(ObRawExprUtils::make_raw_expr_from_str(expr_str, strlen(expr_str), ctx, raw_expr, columns,
                                            sys_vars, &sub_query_info, aggr_exprs ,win_exprs, udf_info));
  _OB_LOG(DEBUG, "================================================================");
  _OB_LOG(DEBUG, "%s", expr);
  _OB_LOG(DEBUG, "%s", CSJ(raw_expr));
  ObRawExprCanonicalizerImpl canon(ctx);
  OK(canon.canonicalize(raw_expr));
  canon_expr = CSJ(raw_expr);
  _OB_LOG(DEBUG, "canon_expr=%s", canon_expr);
}

TEST_F(TestRawExprCanonicalizer, basic_test)
{
  std::ifstream if_tests("./expr/test_raw_expr_canonicalizer.test");
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* canon_expr = NULL;
  std::ofstream of_result("./expr/test_raw_expr_canonicalizer.tmp");
  ASSERT_TRUE(of_result.is_open());
  while (std::getline(if_tests, line)) {
    of_result << line << std::endl;
    canon(line.c_str(), canon_expr);
    of_result << canon_expr << std::endl;
  }
  of_result.close();

  std::ifstream if_result("./expr/test_raw_expr_canonicalizer.tmp");
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected("./expr/test_raw_expr_canonicalizer.result");
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove("./test_raw_expr_canonicalizer.tmp");
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
