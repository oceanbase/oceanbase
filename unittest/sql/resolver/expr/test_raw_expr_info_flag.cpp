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
#include <fstream>
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_test_util.h"
#define private public
#define protected public
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"
#undef protected
#undef private

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace test
{

void verify_results(const char* result_file, const char* tmp_file) {
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

TEST(TestRawExprFlag, expr_info_flag)
{
  static const char* tmp_file = "./expr/test_raw_expr_info_flag.tmp";
  static const char* result_file = "./expr/test_raw_expr_info_flag.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  of_result << "| flag | value |" << std::endl;
  of_result << "| --- | --- |" << std::endl;
#define DEF_EXPR_INFO_FLAG(flag, args...) of_result << "| " << #flag << " | " << flag << " |" << std::endl;
#include "sql/resolver/expr/ob_expr_info_flag.h"
#undef DEF_EXPR_INFO_FLAG

  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST(TestRawExprFlag, inherit_info_flag)
{
  static const char* tmp_file = "./expr/test_raw_expr_info_flag_inherit.tmp";
  static const char* result_file = "./expr/test_raw_expr_info_flag_inherit.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  ObConstRawExpr child_expr;
  ObConstRawExpr parent_expr;
  const ObExprInfo &child_info = child_expr.get_expr_info();
  const ObExprInfo &parent_info = parent_expr.get_expr_info();

  of_result << "========== inherit is flag ==========" << std::endl;
  of_result << std::endl;
  of_result << "| is_flag | cnt_flag |" << std::endl;
  of_result << "| --- | --- |" << std::endl;

  for (int32_t i = 0; i <= IS_INFO_MASK_END; ++i) {
    ObExprInfoFlag child_flag = static_cast<ObExprInfoFlag>(i);
    ObExprInfoFlag parent_flag = static_cast<ObExprInfoFlag>(i + CNT_INFO_MASK_BEGIN);
    child_expr.reset_flag();
    parent_expr.reset_flag();
    OK (child_expr.add_flag(i));
    OK (parent_expr.add_child_flags(child_info));
    EXPECT_EQ(1, parent_info.num_members());
    EXPECT_TRUE(parent_info.has_member(i + CNT_INFO_MASK_BEGIN));
    of_result << "| " << get_expr_info_flag_str(child_flag)
              << " | " << get_expr_info_flag_str(parent_flag) << " |" << std::endl;
  }

  of_result << std::endl;
  of_result << "========== inherit cnt flag ==========" << std::endl;
  of_result << std::endl;
  of_result << "| cnt_flag |" << std::endl;
  of_result << "| --- |" << std::endl;

  for (int32_t i = INHERIT_MASK_BEGIN; i <= INHERIT_MASK_END; ++i) {
    ObExprInfoFlag flag = static_cast<ObExprInfoFlag>(i);
    child_expr.reset_flag();
    parent_expr.reset_flag();
    OK (child_expr.add_flag(i));
    OK (parent_expr.add_child_flags(child_info));
    EXPECT_EQ(1, parent_info.num_members());
    EXPECT_TRUE(parent_info.has_member(i));
    EXPECT_EQ(parent_info, child_info);
    of_result << "| " << get_expr_info_flag_str(flag) << " |" << std::endl;
  }

  of_result.close();
  verify_results(result_file, tmp_file);
}

}

/*
cp ./expr/test_raw_expr_info_flag.tmp ../../../../unittest/sql/resolver/expr/test_raw_expr_info_flag.result
cp ./expr/test_raw_expr_info_flag_inherit.tmp ../../../../unittest/sql/resolver/expr/test_raw_expr_info_flag_inherit.result
*/

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
