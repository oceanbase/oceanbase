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
class TestProfileLog : public ::testing::Test
  {
  public:
    TestProfileLog() {}
    virtual ~TestProfileLog() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(TestProfileLog);
  };

TEST(TestProfileLog, printlog)
{
  ObProfileLogger *profile_logger = ObProfileLogger::getInstance();
  if (OB_ISNULL(profile_logger)) {
    abort();
  }
  profile_logger->setLogDir("./");
  profile_logger->setFileName("test_ob_profile_log.log");
  profile_logger->setLogLevel("INFO");
  EXPECT_EQ(OB_SUCCESS, profile_logger->init());

  int line = 520;
  ObProfileLogger::LogLevel loglevel = ObProfileLogger::INFO;
  char file_name[] = "test_ob_profile_log.cpp";
  char function_name[] = "test_profile_log";
  pthread_t tid = pthread_self();
  int64_t int_value = 5;
  const int64_t str_buf_length = 1024;
  char str_buf[str_buf_length];
  int64_t str_length = 3;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

  str_length = -3;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

  str_length = 254;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

  str_length = 255;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

  str_length = 256;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

  str_length = 516;
  EXPECT_EQ(OB_SUCCESS, TestProfileUtils::build_string(str_buf, str_buf_length, str_length));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, FMT_STR, str_buf, int_value));
  EXPECT_EQ(OB_SUCCESS, profile_logger->printlog(
          loglevel, file_name, line, function_name, tid, int_value, FMT_STR, str_buf, int_value));

}
} //end common
} //end oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
