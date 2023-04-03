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
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_sql_init.h"
#include "lib/json/ob_json_print_utils.h"
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestExprGenerator: public ::testing::Test
{
public:
  TestExprGenerator();
  virtual ~TestExprGenerator();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestExprGenerator);
protected:
  // function members
  void generate_expr(const char *expr_str,
                     const char* &post_expr);
protected:
  // data members
  ObPhysicalPlan phy_plan_;
};

TestExprGenerator::TestExprGenerator()
{
}

TestExprGenerator::~TestExprGenerator()
{
}

void TestExprGenerator::SetUp()
{
}

void TestExprGenerator::TearDown()
{
}

void TestExprGenerator::generate_expr(const char *expr_str,
                                      const char* &post_expr)
{
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ObRawExpr *expr = NULL;
  OK(ObRawExprUtils::make_raw_expr_from_str(expr_str, strlen(expr_str), ctx, expr, columns,
                                            sys_vars, &sub_query_info, aggr_exprs, win_exprs, udf_info));
  RowDesc row_desc;
  OK(row_desc.init());
  for (int64_t i = 0; i < columns.count(); ++i) {
    OK(row_desc.add_column(columns[i].ref_expr_));
  }
  ObSqlExpression *sql_expr = NULL;
  ObColumnExpression col_expr(allocator);
  ObAggregateExpression aggr_expr(allocator);
  if (expr->is_aggr_expr()) {
    sql_expr = &aggr_expr;
  } else {
    sql_expr = &col_expr;
  }
  ObExprGeneratorImpl expr_generator(0, 0, NULL, row_desc);
  OK(expr_generator.generate(*expr, *sql_expr));
  phy_plan_.set_regexp_op_count(expr_generator.get_cur_regexp_op_count());
  post_expr = CSJ(sql_expr);
}

//由于实现enum和set后对cg进行了更改，依赖expr调用deduce_type。
//而该测试用无法成功调用deduce type，因为无法获得column 的type，因此暂时注销掉该case
TEST_F(TestExprGenerator, DISABLED_basic_test)
{
  const char* test_file = "test_expr_generator.test";
  const char* result_file = "test_expr_generator.result";
  const char* tmp_file = "test_expr_generator.tmp";
  const char* post_expr = NULL;
  // run tests
  std::string line;
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  while (std::getline(if_tests, line)) {
    of_result << line << std::endl;
    generate_expr(line.c_str(), post_expr);
    of_result << post_expr << std::endl;
  }
  of_result.close();
  // verify results
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove("./test_resolver.tmp");
}

int main(int argc, char **argv)
{
  system("rm -rf test_expr_generator.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_expr_generator.log", true);

  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
