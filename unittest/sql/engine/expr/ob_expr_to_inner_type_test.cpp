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
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/object/ob_obj_cast.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include <iostream>
#include <stdio.h>
#include "rpc/obmysql/ob_mysql_util.h"
#include "ob_expr_test_utils.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace std;

#define T_SET(is_null, is_to_str, param_num, res_val, ...)                                              \
  EXPECT_RESULTN_TO_INNER(to_set_inner, &buf, calc_result2, is_null, is_to_str, param_num, set_inner, res_val, __VA_ARGS__)

#define T_ENUM(is_null, is_to_str, param_num, res_val, ...)                                              \
  EXPECT_RESULTN_TO_INNER(to_enum_inner, &buf, calc_result2, is_null, is_to_str, param_num, enum_inner, res_val, __VA_ARGS__)

TEST(ObExprToSetInnerType, DISABLED_basic_test)
{

  ObArenaAllocator buf;
  ObExprSetToInnerType to_set_inner(buf);
  T_SET(false, false, 3, "abc", varchar, "abc", int, 0, set, 1);

  T_SET(false, false, 4, "bcd", varchar, "abc", varchar, "bcd", int, 0, set, 2);
  T_SET(false, false, 4, "abc,bcd", varchar, "abc", varchar, "bcd", int, 0, set, 3);
  T_SET(false, false, 4, "", varchar, "abc", varchar, "bcd", int, 0, set, 0);

  T_SET(false, false, 12, "", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 0);
  T_SET(false, false, 12, "1", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 1);
  T_SET(false, false, 12, "10", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 512);
  T_SET(false, false, 12, "1,10", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 513);
  T_SET(false, false, 12, "1,2", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 0x03);
  T_SET(false, false, 12, "1,2,6,8,9,10", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 0x03A3);
  T_SET(false, false, 12, "1,2,3,4,5,6,7,8,9,10", varchar, "1", varchar, "2", varchar, "3",varchar, "4",varchar, "5",varchar, "6",varchar, "7", varchar, "8", varchar, "9", varchar, "10",
    int, 0, set, 1023);
}

TEST(ObExprToSetInnerType, DISABLED_test_empty_null)
{
  ObArenaAllocator buf;
  ObExprSetToInnerType to_set_inner(buf);
  T_SET(false, false, 3, "", varchar, "", int, 0, set, 0);

  T_SET(false, false, 7, "", varchar, "1", varchar, "2", varchar, "", varchar, "3", varchar, "4", int, 0, set, 0);
  T_SET(false, false, 7, "1", varchar, "1", varchar, "2", varchar, "", varchar, "3", varchar, "4", int, 0, set, 1);
  T_SET(false, false, 7, "1,2,,3,4", varchar, "1", varchar, "2", varchar, "", varchar, "3", varchar, "4", int, 0, set, 31);
  T_SET(true, false, 7, "1,2,,3,4", varchar, "1", varchar, "2", varchar, "", varchar, "3", varchar, "4", int, 0, set, 31);
}

TEST(ObExprEnumToInnerType, DISABLED_basic_test)
{
  ObArenaAllocator buf;
  ObExprEnumToInnerType to_enum_inner(buf);

  T_ENUM(false, false, 3, "", varchar, "", int, 0, enum, 0);
  T_ENUM(false, false, 3, "", varchar, "", int, 0, enum, 1);

  T_ENUM(false, false, 5, "", varchar, "1", varchar, "2", varchar, "3", int, 0, enum, 0);
  T_ENUM(false, false, 5, "1", varchar, "1", varchar, "2", varchar, "3", int, 0, enum, 1);
  T_ENUM(false, false, 5, "2", varchar, "1", varchar, "2", varchar, "3", int, 0, enum, 2);
  T_ENUM(false, false, 5, "3", varchar, "1", varchar, "2", varchar, "3", int, 0, enum, 3);
  T_ENUM(true, false, 5, "3", varchar, "1", varchar, "2", varchar, "3", int, 0, enum, 3);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
