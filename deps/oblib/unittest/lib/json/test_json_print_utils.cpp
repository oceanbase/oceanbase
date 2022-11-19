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
#include "lib/ob_define.h"
#include "lib/json/ob_json_print_utils.h"

namespace oceanbase
{
namespace json
{
using namespace common;


void check_converted_json(const char *json, const char *std_json)
{
  ObStdJsonConvertor convertor;
  const int64_t buf_len = strlen(json) * 2 + 1;
  char buf[buf_len];

  ASSERT_EQ(OB_SUCCESS, convertor.init(json, buf, buf_len));
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, convertor.convert(len));
  ASSERT_LT(len, buf_len);
  buf[len] = 0;
  ASSERT_STREQ(buf, std_json);
}

TEST(TestStdJsonConvertor, all)
{
  const char *json = "";
  check_converted_json(json, json);
  ASSERT_FALSE(HasFailure());

  json = "{}";
  check_converted_json(json, json);
  ASSERT_FALSE(HasFailure());

  json = "      [             ]    ";
  check_converted_json(json, json);
  ASSERT_FALSE(HasFailure());

  json = " { abc : 123,   \"abc2\"   : \"xxxxxxxxx   x\" ,  xyz: [] }  ";
  const char * std_json = " { \"abc\" : 123,   \"abc2\"   : \"xxxxxxxxx   x\" ,  \"xyz\": [] }  ";
  check_converted_json(json, std_json);
  ASSERT_FALSE(HasFailure());

  check_converted_json(std_json, std_json);
  ASSERT_FALSE(HasFailure());

  // with escaped string and json not finished
  json = "{a:123, b:[false, true,\"abc\\\", abc:456\\\\\", ]:c:\"\",d : \"abc";
  std_json = "{\"a\":123, \"b\":[false, true,\"abc\\\", abc:456\\\\\", ]:\"c\":\"\",\"d\" : \"abc";

  check_converted_json(json, std_json);
  ASSERT_FALSE(HasFailure());

  check_converted_json(std_json, std_json);
  ASSERT_FALSE(HasFailure());

  // buf not enough
  ObStdJsonConvertor convertor;
  const int64_t buf_len = 10;
  char buf[buf_len];
  ASSERT_EQ(OB_SUCCESS, convertor.init(json, buf, buf_len));
  int64_t len = 0;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, convertor.convert(len));
  ASSERT_TRUE(strncmp(std_json, buf, buf_len) == 0);
}
} // end namespace json
} // end namespace oceanase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
