/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  ObCStringHelper helper;
  ASSERT_EQ(0, strcmp(helper.convert(printer_less), "12345"));
  ASSERT_EQ(0, strcmp(helper.convert(printer_more), "123456789"));
  ASSERT_EQ(0, strcmp(helper.convert(printer_empty), ""));
  ASSERT_EQ(0, strcmp(helper.convert(printer_error), ""));
}

}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
