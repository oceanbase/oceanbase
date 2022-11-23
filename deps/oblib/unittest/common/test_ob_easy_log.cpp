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
#include "lib/oblog/ob_easy_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/profile/ob_profile_log.h"
#include "test_profile_utils.h"
namespace oceanbase
{
namespace common
{
class TestEasyLog : public ::testing::Test
  {
  public:
    TestEasyLog() {}
    virtual ~TestEasyLog() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(TestEasyLog);
  };

TEST(TestEasyLog, ob_easy_log_format)
{
  int level = 5;
  char file_name[129] = "test_ob_easy_log.cpp";
  int line = 520;
  char *function = NULL; //no used
  const int64_t str_buf_length = 4096;
  char str_buf[str_buf_length];
  int64_t int_value = 5;
  int64_t str_length = 3;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
  str_length = -3;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
  str_length = 151;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
  str_length = 4094;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
  str_length = 4095;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
  str_length = 4096;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  ob_easy_log_format(level, file_name, line, function, 0, FMT_STR, str_buf, int_value);
}
} //end common
} //end oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
