/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/oblog/ob_easy_log.h"
#include "deps/oblib/src/lib/net/ob_addr.h"
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
