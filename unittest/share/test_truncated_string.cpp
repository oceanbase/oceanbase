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

#define USING_LOG_PREFIX LIB

#include <gtest/gtest.h>

#include "share/ob_define.h"
#include "share/ob_truncated_string.h"

namespace oceanbase
{
namespace common
{
TEST(TestTruncatedString, common)
{
  // construct functions
  ObString str("123456789");
  ObTruncatedString printer_less(str, 5);
  ObTruncatedString printer_more(str, 10);
  ObTruncatedString printer_empty(str, 0);
  ObTruncatedString printer_error(str, -1);
  ASSERT_EQ(0, strcmp(to_cstring(printer_less), "12345"));
  ASSERT_EQ(0, strcmp(to_cstring(printer_more), "123456789"));
  ASSERT_EQ(0, strcmp(to_cstring(printer_empty), ""));
  ASSERT_EQ(0, strcmp(to_cstring(printer_error), ""));
}

}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
