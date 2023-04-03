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

#define USING_LOG_PREFIX SQL_OPTIMIZER

#include <gtest/gtest.h>
#include "lib/json/ob_json.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test
{

#define MAKE_RAW_EXPR_FROM_STR(str, expr)                                       \
  ({                                                                            \
	  ret = ObRawExprUtils::make_raw_expr_from_str(str,                           \
                                                 strlen(str),                   \
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

class TestRawExprToStr: public ::testing::Test
{
public:
  TestRawExprToStr() {}
  virtual ~TestRawExprToStr() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy and assign
  TestRawExprToStr(const TestRawExprToStr &other);
  TestRawExprToStr& operator=(const TestRawExprToStr &ohter);
};

TEST_F(TestRawExprToStr, basic)
{
  int ret = OB_SUCCESS;
  // mock params
  // stmts
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ctx.is_extract_param_type_ = false;
  ObSQLSessionInfo session;
  ctx.session_info_ = &session;

  const int64_t buf_len = 1024;
  int64_t pos = 0;
  char buf[buf_len];

  ObRawExpr *expr = NULL;
  // const char* inner_offset = "1+c1 > ? and 'abc' || c2 = 'def'";
  const char* expr1 = "1+c1 > ? and SUM(1) OR 2 >= 1";
  const char* expr2 = "CASE WHEN 10>=2 THEN 1+2 ELSE 0 END";
  const char* expr3 = "CASE WHEN 10>=2 THEN 10-2 ELSE SUM(10-2) END";
  const char* expr4 = "1/2";

  MAKE_RAW_EXPR_FROM_STR(expr1, expr);
  EXPECT_TRUE(OB_SUCC(ret));
  pos = 0;
  ret = expr->get_name(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos), buf);

  pos = 0;
  MAKE_RAW_EXPR_FROM_STR(expr2, expr);
  EXPECT_TRUE(OB_SUCC(ret));
  ret = expr->get_name(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos), buf);

  pos = 0;
  MAKE_RAW_EXPR_FROM_STR(expr3, expr);
  EXPECT_TRUE(OB_SUCC(ret));
  ret = expr->get_name(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos), buf);

  pos = 0;
  MAKE_RAW_EXPR_FROM_STR(expr4, expr);
  EXPECT_TRUE(OB_SUCC(ret));
  ret = expr->get_name(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos), buf);
}

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
