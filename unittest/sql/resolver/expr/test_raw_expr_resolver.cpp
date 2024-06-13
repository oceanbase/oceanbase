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
#include "sql/test_sql_utils.h"
#include "lib/utility/ob_test_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_print_visitor.h"
#include "sql/ob_sql_init.h"
#include "lib/json/ob_json_print_utils.h"
#include "share/ob_cluster_version.h"
#include <fstream>
#define private public
#include "observer/ob_server.h"
#undef private
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::lib;
using namespace oceanbase;

class TestRawExprResolver: public ::testing::Test
{
public:
  TestRawExprResolver();
  virtual ~TestRawExprResolver();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRawExprResolver);
protected:
  // function members
  void resolve(const char* expr, const char *&json_expr);
protected:
  // data members
};

TestRawExprResolver::TestRawExprResolver()
{
}

TestRawExprResolver::~TestRawExprResolver()
{
}

void TestRawExprResolver::SetUp()
{
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_VERSION_4_1_0_0);
}

void TestRawExprResolver::TearDown()
{
}

void TestRawExprResolver::resolve(const char* expr, const char *&json_expr)
{
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  const char *expr_str = expr;
  ObIAllocator &allocator = CURRENT_CONTEXT->get_arena_allocator();
  ObRawExprFactory expr_factory(allocator);
  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ctx.is_extract_param_type_ = false;
  ObSQLSessionInfo session;
  ctx.session_info_ = &session;

  EXPECT_TRUE(OB_SUCCESS == oceanbase::ObPreProcessSysVars::init_sys_var());
  EXPECT_TRUE(OB_SUCCESS == session.test_init(0, 0, 0, NULL));
  EXPECT_TRUE(OB_SUCCESS == session.load_default_sys_variable(false, true));

  ObRawExpr *raw_expr = NULL;
  OBSERVER.init_version();
  OK(ObRawExprUtils::make_raw_expr_from_str(expr_str, strlen(expr_str), ctx, raw_expr, columns,
                                            sys_vars, &sub_query_info, aggr_exprs , win_exprs, udf_info));
  _OB_LOG(DEBUG, "================================================================");
  _OB_LOG(DEBUG, "%s", expr);
  _OB_LOG(DEBUG, "%s", CSJ(raw_expr));
  OK(raw_expr->extract_info());
  //OK(raw_expr->deduce_type());
  json_expr = CSJ(raw_expr);
}

TEST_F(TestRawExprResolver, all)
{
  set_compat_mode(Worker::CompatMode::MYSQL);
  static const char* test_file = "./expr/test_raw_expr_resolver.test";
  static const char* tmp_file = "./expr/test_raw_expr_resolver.tmp";
  static const char* result_file = "./expr/test_raw_expr_resolver.result";

  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* json_expr = NULL;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t case_id = 0;
  while (std::getline(if_tests, line)) {
    of_result << '[' << case_id++ << "] " << line << std::endl;
    resolve(line.c_str(), json_expr);
    of_result << json_expr << std::endl;
  }
  of_result.close();
  // verify results
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

int main(int argc, char **argv)
{
  int ret = 0;
  ::testing::InitGoogleTest(&argc,argv);
  system("rm -rf test_optimizer.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_raw_expr_resolver.log", true);
  ContextParam param;
  param.set_mem_attr(1001, "RawExprResolver", ObCtxIds::WORK_AREA)
    .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
  init_sql_factories();
  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
