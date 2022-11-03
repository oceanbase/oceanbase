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
#include "ob_log_adapt_string.h"    // ObLogAdaptString

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

class TestLogAdaptString : public ::testing::Test
{
public:
  TestLogAdaptString() {}
  ~TestLogAdaptString() {}
};

void test_append_str(ObLogAdaptString &str, std::string &std_str, const char *cstr)
{
  const char *ret_cstr = NULL;
  ASSERT_EQ(OB_SUCCESS, str.append(cstr));
  std_str.append(cstr);

  ASSERT_EQ(OB_SUCCESS, str.cstr(ret_cstr));
  ASSERT_STREQ(std_str.c_str(), ret_cstr);
}

TEST_F(TestLogAdaptString, smoke_test)
{
  ObLogAdaptString str(ObModIds::OB_LOG_TEMP_MEMORY);
  std::string std_str;
  const char *cstr = "";

  test_append_str(str, std_str, "");
  test_append_str(str, std_str, "I am me ");
  test_append_str(str, std_str, "中华人民共和国 ");

  EXPECT_EQ(OB_SUCCESS, str.append_int64(100));
  std_str.append("100");

  ASSERT_EQ(OB_SUCCESS, str.cstr(cstr));
  ASSERT_STREQ(std_str.c_str(), cstr);

  OBLOG_LOG(INFO, "cstr", K(cstr), K(str));
}

TEST_F(TestLogAdaptString, argument_test)
{
  ObLogAdaptString str(ObModIds::OB_LOG_TEMP_MEMORY);
  std::string std_str;

  EXPECT_EQ(OB_INVALID_ARGUMENT, str.append(NULL));

  EXPECT_EQ(OB_SUCCESS, str.append(""));
  std_str.append("");
  EXPECT_EQ(OB_SUCCESS, str.append_int64(-1));
  std_str.append("-1");

  EXPECT_EQ(OB_SUCCESS, str.append_int64(INT64_MAX));
  char int64_max[100];
  sprintf(int64_max, "%ld", INT64_MAX);
  std_str.append(int64_max);


  const char *cstr = "";
  ASSERT_EQ(OB_SUCCESS, str.cstr(cstr));
  ASSERT_STREQ(std_str.c_str(), cstr);

  OBLOG_LOG(INFO, "cstr", K(cstr), K(std_str.c_str()));
}

TEST_F(TestLogAdaptString, all_sort_of_string)
{
  ObLogAdaptString str(ObModIds::OB_LOG_TEMP_MEMORY);
  std::string std_str;
  const char *cstr = "";
  char buf[1 * _M_ + 1];

  (void)memset(buf, 'a', sizeof(buf));

  // Empty strings are also equal
  EXPECT_EQ(OB_SUCCESS, str.cstr(cstr));
  EXPECT_STREQ(std_str.c_str(), cstr);

  for (int i = 0; i < 3; i++) {
    // less than 8K
    test_append_str(str, std_str, "");
    test_append_str(str, std_str, "11111111111111");
    test_append_str(str, std_str, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

    // equals to 8K
    buf[8 * _K_] = '\0';
    test_append_str(str, std_str, buf);
    test_append_str(str, std_str, buf);
    test_append_str(str, std_str, buf);
    buf[8 * _K_] = 'a';

    // greater than 8K
    buf[16 * _K_] = '\0';
    test_append_str(str, std_str, buf);
    buf[16 * _K_] = 'a';
    buf[32 * _K_] = '\0';
    test_append_str(str, std_str, buf);
    buf[32 * _K_] = 'a';
    buf[1 * _M_] = '\0';
    test_append_str(str, std_str, buf);
    buf[1 * _M_] = 'a';
  }
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ob_log_adapt_string.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
