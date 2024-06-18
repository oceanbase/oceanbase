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
#include <sys/statvfs.h>
#include "lib/oblog/ob_log.h"
#include "lib/profile/ob_trace_id.h"
#include "common/ob_clock_generator.h"

using namespace std;
namespace oceanbase
{
namespace common
{

TEST(ObDbaLog, basic_test)
{
  char buf[100];
  int64_t len = 100;
  int64_t pos = 0;
  ObPLogFDType data_enum = FD_ALERT_FILE;
  uint8_t data_uint8 = 2;
  uint16_t data_uint16 = 33;
  int32_t data_int32 = -444;
  int64_t data_int64 = 5555;
  char data_char = '@';
  float data_float = 3.1415926;
  double data_double = 233.233;
  bool data_bool = false;
  char data_str[10] = {'h','e','l','l','o',' ',0};
  const char *data_const_str = "world! ";
  const char *data_null = nullptr;
  ObClockGenerator *data_obj_ptr = (ObClockGenerator*)0x7fc1d30996d0; // do not access this addr
  const ObClockGenerator *data_obj_null = NULL;
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_enum));
  ASSERT_STREQ(buf, "5");
  ASSERT_EQ(pos, 1);
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_uint8));
  ASSERT_EQ(pos, 2);
  ASSERT_STREQ(buf, "52");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_uint16));
  ASSERT_EQ(pos, 4);
  ASSERT_STREQ(buf, "5233");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_int32));
  ASSERT_EQ(pos, 8);
  ASSERT_STREQ(buf, "5233-444");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_int64));
  ASSERT_EQ(pos, 12);
  ASSERT_STREQ(buf, "5233-4445555");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_char));
  ASSERT_EQ(pos, 13);
  ASSERT_STREQ(buf, "5233-4445555@");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_float));
  ASSERT_EQ(pos, 28);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+00");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_double));
  ASSERT_EQ(pos, 52);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_bool));
  ASSERT_EQ(pos, 57);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02false");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_str));
  ASSERT_EQ(pos, 63);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02falsehello ");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_const_str));
  ASSERT_EQ(pos, 70);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02falsehello world! ");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_null));
  ASSERT_EQ(pos, 74);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02falsehello world! NULL");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_obj_ptr));
  ASSERT_EQ(pos, 88);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02falsehello world! NULL0x7fc1d30996d0");
  ASSERT_EQ(OB_SUCCESS, logdata_print_value(buf, len, pos, data_obj_null));
  ASSERT_EQ(pos, 92);
  ASSERT_STREQ(buf, "5233-4445555@3.141592503e+002.332330000000000041e+02falsehello world! NULL0x7fc1d30996d0NULL");
}

TEST(ObDbaLog, test_1)
{
  int ret = OB_SUCCESS;
  ObClockGenerator::get_instance().init();

  // 0. alert log level test
  ASSERT_EQ(OB_SUCCESS, ObLogger::get_logger().parse_check_alert("WARN", 5));
  ASSERT_EQ(OB_SUCCESS, ObLogger::get_logger().parse_check_alert("ERROR", 5));
  ASSERT_EQ(OB_SUCCESS, ObLogger::get_logger().parse_check_alert("INFO", 5));

  // 1. base test
  LOG_DBA_INFO_("TEST_EVENT", "print dba log only");
  LOG_DBA_INFO_V2("TEST_EVENT", "print both rd and dba log");
  LOG_DBA_INFO_V2("TEST_EVENT", "print both rd and dba log");
  LOG_DBA_WARN_("TEST_EVENT", ret, "print dba log only");
  LOG_DBA_WARN_V2("TEST_EVENT", ret, "print both rd and dba log");
  LOG_DBA_ERROR_V2("TEST_EVENT", ret, "print both rd and dba log");
  LOG_DBA_ERROR_("TEST_EVENT", ret, "print dba log only");

  // 2. test REACH_TIME_INTERVAL
  const char * const str1 = "it's right to print this log";
  const char * const str2 = "it's wrong to print this log";
  const char *log_str = str1;
  for (int i = 0; i < 20; i++) {
    LOG_DBA_WARN_("TEST_EVENT", ret, log_str);
    log_str = str2;
  }
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y0-1111111111111111-0-0"));
  log_str = str1;
  for (int i = 0; i < 20; i++) {
    LOG_DBA_INFO_("TEST_EVENT", log_str);
    log_str = str2;
  }

  // 3. test need_to_print_dba
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y0-0000000000000000-0-0"));
  ASSERT_EQ(true, OB_LOG_NEED_TO_PRINT_DBA(DBA_WARN, false));
  ASSERT_EQ(true, OB_LOG_NEED_TO_PRINT_DBA(DBA_WARN, false));
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(true, OB_LOG_NEED_TO_PRINT_DBA(DBA_WARN, false));
  }
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y0-2222222222222222-0-0"));
  ASSERT_EQ(true, OB_LOG_NEED_TO_PRINT_DBA(DBA_WARN, false));
  ASSERT_EQ(false, OB_LOG_NEED_TO_PRINT_DBA(DBA_WARN, false));
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y0-3333333333333333-0-0"));
  ASSERT_EQ(true, OB_LOG_NEED_TO_PRINT_DBA(DBA_ERROR, false));
  ASSERT_EQ(false, OB_LOG_NEED_TO_PRINT_DBA(DBA_ERROR, false));
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(false, OB_LOG_NEED_TO_PRINT_DBA(DBA_ERROR, false));
  }
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y0-4444444444444444-0-0"));
  LOG_DBA_WARN_V2("TEST_EVENT", ret, "print both rd and dba log");
  LOG_DBA_WARN_V2("TEST_EVENT", ret, "print rd log only");
  LOG_DBA_WARN_V2("TEST_EVENT", ret, "print rd log only");
  LOG_DBA_ERROR_V2("TEST_EVENT", ret, "print both rd and dba log");
  LOG_DBA_ERROR_V2("TEST_EVENT", ret, "print rd log only");
  ObCurTraceId::get_trace_id()->reset();
}

TEST(ObDbaLog, test_2)
{
  int ret = OB_SUCCESS;

  // 1. test DBA_STEP_INC_INFO, DBA_STEP_INFO
  LOG_DBA_INFO_V2("TEST_EVENT", DBA_STEP_INC_INFO(bootstrap), "print both rd and dba log");
  LOG_DBA_INFO_V2("TEST_EVENT", DBA_STEP_INC_INFO(bootstrap), "print both rd and dba log");
  LOG_DBA_INFO_("TEST_EVENT", DBA_STEP_INC_INFO(bootstrap), "print dba log only");
  ASSERT_EQ(3, bootstrap_thread_local_step_get());
  LOG_DBA_ERROR_V2("TEST_EVENT", OB_SUCCESS, DBA_STEP_INFO(bootstrap), "print both rd and dba log");
  LOG_DBA_ERROR_("TEST_EVENT", OB_SUCCESS, DBA_STEP_INFO(bootstrap), "print dba log only");
  ASSERT_EQ(3, bootstrap_thread_local_step_get());

  // 2. test need_to_print_dba and DBA_STEP_INC_INFO
  for (int i = 0; i < LIST_EVENT_COUNT(bootstrap) + 1; i++) {
    LOG_DBA_INFO_V2("TEST_EVENT", DBA_STEP_INC_INFO(bootstrap), "print both rd and dba log");
    LOG_DBA_INFO_V2("TEST_EVENT", DBA_STEP_INC_INFO(bootstrap), "print both rd and dba log");
  }
  ASSERT_EQ(5, bootstrap_thread_local_step_get());
}

TEST(ObDbaLog, test_3)
{
  int ret = OB_SUCCESS;

  // 1. test statvfs
  int64_t unused_disk_space = 0;
  struct statvfs file_system;
  ASSERT_EQ(0, statvfs("./", &file_system));
  unused_disk_space = file_system.f_bsize * file_system.f_bfree;
  LOG_DBA_INFO_V2("TEST_EVENT", "free disk size: ", unused_disk_space);
}

}
}

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_dba.log", true);
  logger.set_log_level(OB_LOG_LEVEL_WARN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
