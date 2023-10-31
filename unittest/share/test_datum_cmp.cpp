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

#define private public
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/expr/ob_expr_cmp_func.h"
#include "sql/test_sql_utils.h"

#include <fstream>
#include <iterator>
#include <string>
#include <algorithm>

#undef private

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace sql;
static bool is_equal_content(const char *tmp_file, const char *result_file)
{
  std::ifstream if_tmp(tmp_file);
  std::ifstream if_result(result_file);

  EXPECT_TRUE(if_tmp.is_open());
  EXPECT_TRUE(if_result.is_open());

  std::istream_iterator<std::string> if_tmp_iter(if_tmp);
  std::istream_iterator<std::string> if_result_iter(if_result);

  return std::equal(if_tmp_iter, std::istream_iterator<std::string>(), if_result_iter);
}

class ObTestDatumCmp: public ::testing::Test
{
public:
  ObTestDatumCmp() {}
  ~ObTestDatumCmp() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObTestDatumCmp);
};

TEST(ObTestDatumCmp, defined_nullsafe_func_by_type)
{
  const char* defined_func_file = "./test_defined_func_by_type.result";
  const char* tmp_file = "./test_defined_func_by_type.tmp";
  std::ofstream of_result(tmp_file);

  for (int i = 0; i < ObMaxType; i++) {
    of_result << "/**************** " << inner_obj_type_str(static_cast<ObObjType>(i))
              << " ****************/" << "\n\n";
    for (int j = 0; j < ObMaxType; j++) {
      of_result << "<"
                << inner_obj_type_str(static_cast<ObObjType>(i))
                << ", "
                << inner_obj_type_str(static_cast<ObObjType>(j))
                << ">"
                << " : ";
      if (NULL != ObDatumFuncs::get_nullsafe_cmp_func(static_cast<ObObjType>(i),
                                                      static_cast<ObObjType>(j),
                                                      NULL_FIRST,
                                                      CS_TYPE_COLLATION_FREE,
                                                      SCALE_UNKNOWN_YET,
                                                      false, false)) {
        of_result << "defined\n";
      } else {
        of_result << "not defined\n";
      }
    } // for end
    of_result << "\n";
  } // for end
  of_result.flush();
  EXPECT_TRUE(is_equal_content(tmp_file, defined_func_file));
}

TEST(ObTestDatumCmp, defined_expr_func_by_type)
{
  const char* defined_func_file = "./test_defined_expr_func_by_type.result";
  const char* tmp_file = "./test_defined_expr_func.tmp";
  std::ofstream of_result(tmp_file);

  for (int i = 0; i < ObMaxType; i++) {
    of_result << "/**************** " << inner_obj_type_str(static_cast<ObObjType>(i))
              << " ****************/" << "\n\n";
    for (int j = 0; j < ObMaxType; j++) {
      of_result << "<"
                << inner_obj_type_str(static_cast<ObObjType>(i))
                << ", "
                << inner_obj_type_str(static_cast<ObObjType>(j))
                << "> : ";
      if (NULL != ObExprCmpFuncsHelper::get_datum_expr_cmp_func(static_cast<ObObjType>(i),
                                                        static_cast<ObObjType>(j),
                                                        SCALE_UNKNOWN_YET,
                                                        SCALE_UNKNOWN_YET,
                                                        PRECISION_UNKNOWN_YET,
                                                        PRECISION_UNKNOWN_YET,
                                                        false,
                                                        CS_TYPE_COLLATION_FREE,
                                                        false)) {
        of_result << "defined\n";
      } else {
        of_result << "not defined\n";
      }
    } // for end
    of_result << "\n";
  } // for end
  of_result.flush();
  EXPECT_TRUE(is_equal_content(tmp_file, defined_func_file));
}
} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_datum_cmp.log", true);
  return RUN_ALL_TESTS();
}
