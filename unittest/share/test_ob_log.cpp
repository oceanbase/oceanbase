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
#include <unistd.h>
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/utility/ob_print_utils.h"
using namespace oceanbase;
using namespace common;

class ObLoggerTest: public ::testing::Test
{
public:

private:
  ObLoggerTest(const ObLoggerTest &other);
  ObLoggerTest& operator=(const ObLoggerTest &other);
private:

};
class ObTestLog
{
public:
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    BUF_PRINTF("TestLog");
    OB_LOG(WARN, "print log");
    return pos;
  }
};


TEST(ObLoggerTest, logger_test)
{
  OB_LOGGER.set_log_level("INFO", "WARN");
  const char *file_name = "ob_log_logger_test.log";
  OB_LOGGER.set_file_name(file_name, true, true);
  int32_t valid_length = 0;
  int ret = 0;
  const char *buf_set_0 = "INFO";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_0, static_cast<int32_t>(strlen(buf_set_0)), valid_length);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen(buf_set_0)), valid_length);

  //COMMMON is not a sub module of SQL. The parse of "SQL.COMMON:INFO" would be error.
  //parse_set function would return valid length of "ALL.*:DEBUG,"
  const char *buf_set_1 =
      "ALL.                             *:                                                                  DEBUG, SQL.COMMON:INFO";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_1, static_cast<int32_t>(strlen(buf_set_1)), valid_length);
  EXPECT_EQ(OB_LOG_MODULE_UNKNOWN, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen("ALL.                             *:                                                                  DEBUG,")), valid_length);

  // (1)using ',' as delimiter between different mods
  const char *buf_set_2 = "ALL.*:INFO, SQL.*:DEBUG, SQL.OPT:TRACE, STORAGE.*:ERROR";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_2, static_cast<int32_t>(strlen(buf_set_2)), valid_length);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen(buf_set_2)), valid_length);

  // (2)using ';' as delimiter between different mods
  const char *buf_set_3 = "ALL.*:INFO; SQL.*:DEBUG; SQL.OPT:TRACE; STORAGE.*:ERROR";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_3, static_cast<int32_t>(strlen(buf_set_3)), valid_length);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen(buf_set_3)), valid_length);

  // (3) mix-using ';' and ',' as delimiter between different mods, is invalid
  const char *buf_set_4 = "ALL.*:INFO; SQL.*:DEBUG; SQL.OPT:TRACE, ELECT.*:WARN, STORAGE.*:ERROR";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_4, static_cast<int32_t>(strlen(buf_set_4)), valid_length);
  EXPECT_EQ(OB_LOG_LEVEL_INVALID, ret);
  EXPECT_EQ(0, valid_length);

  ObObj val;
  val.set_int(100);
  sql::ObConstRawExpr const_expr;
  const_expr.set_value(val);
  sql::ObConstRawExpr *expr_ = &const_expr;
  OB_LOG(WARN, "Const expr", "expr", PNAME(expr_));
  OB_LOG(WARN, "Const expr", KPNAME(expr_));
  OB_LOG(WARN, "Const expr", KPNAME(const_expr));
  OB_LOG(WARN, "Const expr", KPNAME_(expr));

  char data[100];
  int64_t pos = 0;
  logdata_printf(data, 0, pos, "hello");
  EXPECT_TRUE(pos >= 0);

  const char *buf_set_proxy = "PROXY.*        :DEBUG;"
                              "PROXY.EVENT    :DEBUG;"
                              "PROXY.NET      :DEBUG;"
                              "PROXY.SOCK     :DEBUG;"
                              "PROXY.TXN      :DEBUG;"
                              "PROXY.TUNNEL   :DEBUG;"
                              "PROXY.SM       :DEBUG;"
                              "PROXY.CS       :DEBUG;"
                              "PROXY.SS       :DEBUG;"
                              "PROXY.PVC      :DEBUG;"
                              "PROXY.TRANSFORM:DEBUG;"
                              "PROXY.API      :DEBUG";
  ret = OB_LOGGER.parse_set_with_valid_ret(buf_set_proxy, static_cast<int32_t>(strlen(buf_set_proxy)), valid_length);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen(buf_set_proxy)), valid_length);
  ret = OB_LOGGER.set_mod_log_levels(buf_set_proxy);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(static_cast<int32_t>(strlen(buf_set_proxy)), valid_length);

  OB_LOG(INFO, "OB_LOG INFO");
  COMMON_LOG(INFO, "COMMON_LOG INFO");
  EXPECT_TRUE(OB_LOG_MOD_NEED_TO_PRINT(COMMON, INFO));

  OB_LOGGER.set_check(0);

  SQL_LOG(DEBUG, "SQL_LOG DEBUG");
  EXPECT_TRUE(OB_LOG_MOD_NEED_TO_PRINT(SQL, DEBUG));

  SQL_OPT_LOG(TRACE, "SQL_OPT_LOG TRACE");
  EXPECT_TRUE(OB_LOG_SUBMOD_NEED_TO_PRINT(SQL, OPT, TRACE));

  COMMON_LOG(TRACE, "!! COMMON_LOG TRACE, should not be printed!");
  EXPECT_FALSE(OB_LOG_MOD_NEED_TO_PRINT(COMMON, TRACE));

  SQL_OPT_LOG(DEBUG, "!! SQL_OPT_LOG DEBUG, should not be printed!");
  EXPECT_FALSE(OB_LOG_SUBMOD_NEED_TO_PRINT(SQL, OPT, DEBUG));

  STORAGE_LOG(INFO, "!! STORAGE_LOG INFO, should not be printed!");
  EXPECT_FALSE(OB_LOG_MOD_NEED_TO_PRINT(STORAGE, INFO));

  OB_LOG(ERROR, "ERROR message");
  _OB_NUM_LEVEL_LOG(OB_LOG_LEVEL_INFO ,"%s %d", "hello world", 10);
}

enum EnumType
{
  EnumA,
  EnumB,
};

enum
{
  EnumNoTypeA,
  EnumNoTypeB,
};
TEST(ObLoggerTest, key_value_log)
{
  OB_LOGGER.set_max_file_size(256 * 1024L * 1024L); //256M
  const int32_t MAX_LOG_SIZE = 64 * 1024;//64KB

  //test for MAX_LOG_SIZE
  char buffer[MAX_LOG_SIZE];
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  memset(buffer, 'h', static_cast<size_t>(MAX_LOG_SIZE));
  ObString ob_str;
  ob_str.assign_ptr(buffer, MAX_LOG_SIZE);
  SQL_OPT_LOG(INFO, "test for MAX_LOG_SIZE", "ObString with MAX_LOG_SIZE length", ob_str);
  OB_LOG(INFO, "hexprint", KPHEX(buffer, MAX_LOG_SIZE));
  char *buff_ = buffer;
  OB_LOG(INFO, "hexprint", KPHEX(buff_, MAX_LOG_SIZE));
  const char *msg = "";
  FORWARD_USER_ERROR(-1, msg);
  errno = 1;
  OB_LOG(WARN, "Error", KERRMSG);
  OB_LOG(WARN, "Error", "err", ERRMSG);
  errno = 20000;
  OB_LOG(WARN, "Error", "err", ERRMSG);
  OB_LOG(WARN, "Error", KERRMSG);
  OB_LOG(WARN, "Error", "err", ERRNOMSG(2));
  OB_LOG(WARN, "Error", KERRNOMSG(2));
  OB_LOG(WARN, "Error", K(errno));

  errno = 0;
  //type defined in OB
  ob_str.assign_ptr("hello world", static_cast<ObString::obstr_size_t>(strlen("hello world")));
  ObString empty;
  _OB_LOG(WARN, " %s, %s, %s, %s, %s", to_cstring(ob_str), to_cstring(ob_str), to_cstring(ob_str), to_cstring(ob_str),
                                                to_cstring(ob_str));
  _OB_LOG(WARN, "empty%s", to_cstring(empty));
  ObBitSet<32> bit_set;
  bit_set.add_member(12);
  ObSqlString sql_str;
  sql_str.append_fmt("%s", "sql_stirng");

  OB_LOG(INFO, "hexprint", "hex", PHEX(ob_str.ptr(), ob_str.length()), KPHEX(ob_str.ptr(), ob_str.length()));

  const ObString *p_ob_str = NULL;
  OB_LOG(INFO, "NULL pointer", K(p_ob_str));
  OB_LOG(INFO, "PC print", "pointer", PC(p_ob_str));
  p_ob_str = &ob_str;
  OB_LOG(INFO, "PC print", "pointer", PC(p_ob_str));
  OB_LOG(INFO, "KPC print", KPC(p_ob_str));
  p_ob_str = NULL;
  OB_LOG(INFO, "KPC print", KPC(p_ob_str));
  const ObString *p_ob_str_ = &ob_str;
  ObString *p_ob_str2 = &ob_str;
  OB_LOG(INFO, "KPC_ print", KPC_(p_ob_str));
  OB_LOG(INFO, "KPC print", KPC(p_ob_str2));
  p_ob_str_ = NULL;
  OB_LOG(INFO, "KPC_ print", KPC_(p_ob_str));
  const ObString * const p_const_ob_str = &ob_str;
  class ObTestLog test_log;
  OB_LOG(INFO, "nest log", K(test_log));

  //built-in data types
  //int8_t int8_max = INT8_MAX;
  int8_t int8_min = INT8_MIN;
  int16_t int16_max = INT16_MAX;
  int16_t int16_min = INT16_MIN;
  int32_t int32_max = INT32_MAX;
  int32_t int32_min = INT32_MIN;
  int64_t int64_max = INT64_MAX;
  int64_t int64_min = INT64_MIN;
  uint8_t uint8_type = UINT8_MAX;
  uint16_t uint16_type = UINT16_MAX;
  uint32_t uint32_type = UINT32_MAX;
  uint64_t uint64_type = UINT64_MAX;
  double double_type = 3.1415926535897932384626;
  float float_type = static_cast<float>(3.1415926);
  bool bool_type = FALSE;
  const char *p_string = "hello world";

  COMMON_LOG(INFO, "test for no key/value pair");

  EnumType enum_var = EnumA;
  OB_LOG(INFO, "test for enum type and K macro", K(EnumA), K(EnumB), K(enum_var));

  OB_LOG(INFO,
         "test for enum without type define",
         "EnumNoTypeA",
         static_cast<int64_t>(EnumNoTypeA),
         "EnumNoTypeB",
         static_cast<int64_t>(EnumNoTypeB));
  OB_LOG(INFO, "test for pointers", K(p_ob_str), K(p_const_ob_str));

  OB_LOG(ERROR, "test for 1 key/value pair", K(bit_set));

  COMMON_LOG(INFO,
             "test for 3 key/value pairs",
             K(EnumA),
             K(sql_str),
             K(bit_set));

  STORAGE_LOG(ERROR,
              "test for 4 key/value pairs",
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));

  SQL_LOG(INFO,
          "test for 5 key/value pairs",
          K(ob_str),
          K(float_type),
          K(EnumA),
          K(sql_str),
          K(bit_set));

  SQL_OPT_LOG(TRACE,
              "test for 6 key/value pairs",
              K(p_string),
              K(ob_str),
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));

  COMMON_LOG(ERROR,
             "test for 7 key/value pairs",
             K(bool_type),
             K(p_string),
             K(ob_str),
             K(float_type),
             K(EnumA),
             K(sql_str),
             K(bit_set));

  SQL_ENG_LOG(DEBUG,
              "test for 9 key/value pairs",
              K(double_type),
              K(float_type),
              K(bool_type),
              K(p_string),
              K(ob_str),
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));

  SQL_LOG(INFO,
          "test for 10 key/value pairs",
          K(uint64_type),
          K(double_type),
          K(float_type),
          K(bool_type),
          K(p_string),
          K(ob_str),
          K(float_type),
          K(EnumA),
          K(sql_str),
          K(bit_set));

  COMMON_LOG(ERROR,
             "test for 11 key/value pairs",
             K(uint32_type),
             K(uint64_type),
             K(double_type),
             K(float_type),
             K(bool_type),
             K(p_string),
             K(ob_str),
             K(float_type),
             K(EnumA),
             K(sql_str),
             K(bit_set));

  STORAGE_LOG(ERROR,
              "test for 12 key/value pairs",
              K(uint16_type),
              K(uint32_type),
              K(uint64_type),
              K(double_type),
              K(float_type),
              K(bool_type),
              K(p_string),
              K(ob_str),
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));

  RS_LOG(INFO,
         "test for 13 key/value pairs",
         K(uint8_type),
         K(uint16_type),
         K(uint32_type),
         K(uint64_type),
         K(double_type),
         K(float_type),
         K(bool_type),
         K(p_string),
         K(ob_str),
         K(float_type),
         K(EnumA),
         K(sql_str),
         K(bit_set));

  COMMON_LOG(ERROR,
             "test for 15 key/value pairs",
             K(int64_max),
             K(int64_min),
             K(uint8_type),
             K(uint16_type),
             K(uint32_type),
             K(uint64_type),
             K(double_type),
             K(float_type),
             K(bool_type),
             K(p_string),
             K(ob_str),
             K(float_type),
             K(EnumA),
             K(sql_str),
             K(bit_set));

  SQL_LOG(INFO,
          "test for 16 key/value pairs",
          K(int32_min),
          K(int64_max),
          K(int64_min),
          K(uint8_type),
          K(uint16_type),
          K(uint32_type),
          K(uint64_type),
          K(double_type),
          K(float_type),
          K(bool_type),
          K(p_string),
          K(ob_str),
          K(float_type),
          K(EnumA),
          K(sql_str),
          K(bit_set));

  SQL_OPT_LOG(TRACE,
              "test for 17 key/value pairs",
              K(int32_max),
              K(int32_min),
              K(int64_max),
              K(int64_min),
              K(uint8_type),
              K(uint16_type),
              K(uint32_type),
              K(uint64_type),
              K(double_type),
              K(float_type),
              K(bool_type),
              K(p_string),
              K(ob_str),
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));

  SQL_LOG(ERROR,
          "test for 19 key/value pairs",
          K(int16_max),
          K(int16_min),
          K(int32_max),
          K(int32_min),
          K(int64_max),
          K(int64_min),
          K(uint8_type),
          K(uint16_type),
          K(uint32_type),
          K(uint64_type),
          K(double_type),
          K(float_type),
          K(bool_type),
          K(p_string),
          K(ob_str),
          K(float_type),
          K(EnumA),
          K(sql_str),
          K(bit_set));

  SQL_OPT_LOG(INFO,
              "test for 20 key/value pairs",
              K(int8_min),
              K(int16_max),
              K(int16_min),
              K(int32_max),
              K(int32_min),
              K(int64_max),
              K(int64_min),
              K(uint8_type),
              K(uint16_type),
              K(uint32_type),
              K(uint64_type),
              K(double_type),
              K(float_type),
              K(bool_type),
              K(p_string),
              K(ob_str),
              K(float_type),
              K(EnumA),
              K(sql_str),
              K(bit_set));
}

TEST(ObLoggerTest, warning_buffer)
{
  ObWarningBuffer wb;
  ob_setup_tsi_warning_buffer(&wb);
  ob_get_tsi_warning_buffer()->set_warn_log_on(true);
  EXPECT_TRUE(ob_get_tsi_warning_buffer()->is_warn_log_on());

  char warning_buffer[1024];
  const uint32_t BUFFER_SIZE = ob_get_tsi_warning_buffer()->get_buffer_size();

  for (uint32_t warning_index = 0; warning_index < (BUFFER_SIZE + 5); ++warning_index) {
    snprintf(warning_buffer, 1024, "ObWarningBuffer %d", warning_index);
    LOG_USER_WARN(OB_ERR_ILLEGAL_ID, warning_buffer);
    EXPECT_EQ(warning_index+1, ob_get_tsi_warning_buffer()->get_total_warning_count());

    if (warning_index < BUFFER_SIZE) {
      const ObWarningBuffer::WarningItem *item = ob_get_tsi_warning_buffer()->get_warning_item(warning_index);
      if (NULL != item) {
        EXPECT_EQ(0, static_cast<int32_t>(strcmp(warning_buffer, item->msg_)));
      }
    }
    else {
      EXPECT_EQ(BUFFER_SIZE, ob_get_tsi_warning_buffer()->get_readable_warning_count());
    }
  }
  ob_get_tsi_warning_buffer()->reset();
  const char *err_msg = "USR_ERROR message";
  LOG_USER_ERROR( OB_ERR_ILLEGAL_ID, err_msg);
  EXPECT_EQ(0, strcmp(err_msg, ob_get_tsi_warning_buffer()->get_err_msg()));
  ob_get_tsi_warning_buffer()->reset_err();
}

TEST(ObLoggerTest, check_file)
{
  const char *filename = "ob_log_check_file.log";
  ObPLogFileStruct log_struct;
  log_struct.fd_ = 0;
  log_struct.wf_fd_ = 0;
  memcpy(log_struct.filename_, filename, strlen(filename));
  log_struct.filename_[strlen(filename)] = '\0';
  OB_LOGGER.check_file(log_struct, true);
  log_struct.fd_ = 0;
  log_struct.wf_fd_ = 0;
  OB_LOGGER.check_file(log_struct, true);
  EXPECT_EQ(0 , log_struct.fd_);
  EXPECT_EQ(0, log_struct.wf_fd_);
}

TEST(ObLoggerTest, set_file_name)
{
  OB_LOGGER.set_log_level("INFO", "WARN");
  OB_LOGGER.set_max_file_size(8);//set only 8 bytes, to triger rotate_log
  OB_LOGGER.set_max_file_index(2);
  OB_LOGGER.set_check(false);
  const char *file_name = "ob_log_set_file_name.log";
  OB_LOGGER.set_file_name(file_name, true, true);
  OB_LOG(INFO, "before set rs log file, ObLoggerTest info test");
  OB_LOG(WARN, "before set rs log file, ObLoggerTest warn test");
  RS_LOG(INFO, "before set rs log file, ObLoggerTest info RS_LOG test");
  RS_LOG(WARN, "before set rs log file, ObLoggerTest warn RS_LOG test");
  SQL_LOG(INFO, "before set rs log file, ObLoggerTest info SQL_LOG test");
  SQL_LOG(WARN, "before set rs log file, ObLoggerTest warn SQL_LOG test");
  OB_LOG(INFO, "before set rs log file, infostring format should be considered as string %s, %d", "should not cause core", "test");
  OB_LOG(WARN, "before set rs log file, infostring format should be considered as string %s, %d", "should not cause core", "test");
  const char *rs_file_name = "ob_log_set_file_name_rs.log";
  OB_LOGGER.set_file_name(file_name, true, true, rs_file_name);
  OB_LOG(INFO, "after set rs log file, ObLoggerTest info test");
  OB_LOG(WARN, "after set rs log file, ObLoggerTest warn test");
  RS_LOG(INFO, "after set rs log file, ObLoggerTest info RS_LOG test");
  RS_LOG(WARN, "after set rs log file, ObLoggerTest warn RS_LOG test");
  SQL_LOG(INFO, "after set rs log file, ObLoggerTest info SQL_LOG test");
  SQL_LOG(WARN, "after set rs log file, ObLoggerTest warn SQL_LOG test");
  OB_LOG(INFO, "after set rs log file, infostring format should be considered as string %s, %d", "should not cause core", "test");
  OB_LOG(WARN, "after set rs log file, infostring format should be considered as string %s, %d", "should not cause core", "test");
  OB_LOGGER.set_file_name(file_name, true, true, rs_file_name);
}

TEST(ObLoggerTest, backtrace)
{
  OB_LOGGER.set_log_level("INFO", "WARN");
  OB_LOGGER.set_max_file_size(0);
  OB_LOGGER.set_file_name("test_ob_log_backtrace.log", true, true, "test_ob_log_backtrace_rs.log");
  OB_LOG(INFO, "test info", "key", "value");
  OB_LOG(ERROR, "test error", "key", "value");
  _OB_LOG(ERROR, "test error, %s", "hello world");
  RS_LOG(ERROR, "test rs error");
}

TEST(ObLoggerTest, redirect_stdout_stderr)
{
  OB_LOGGER.set_file_name("ob_log_redirect_stdout_stderrt.log", false, true);
  OB_LOG(INFO, "redirect_stdout_stderr test");
  OB_LOGGER.set_file_name("ob_log_redirect_stdout_stderr_new.log", true, true);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
