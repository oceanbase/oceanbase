/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/allocator/ob_concurrent_fifo_allocator.h"     //ObConcurrentFIFOAllocator
#include "lib/allocator/page_arena.h"
#include "ob_log_miner_utils.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

TEST(test_ob_log_miner_utils, ObLogMinerDeepCopyCstring)
{
  const char *archive_dest = "archive_dest";
  char *val = nullptr;
  ObArenaAllocator alloc;
  EXPECT_EQ(OB_SUCCESS, deep_copy_cstring(alloc, archive_dest, val));
  EXPECT_STREQ(archive_dest, val);
  EXPECT_EQ(OB_ERR_UNEXPECTED, deep_copy_cstring(alloc, archive_dest, val));
  val = nullptr;
  EXPECT_EQ(OB_SUCCESS, deep_copy_cstring(alloc, nullptr, val));
  EXPECT_EQ(nullptr, val);
  EXPECT_EQ(OB_SUCCESS, deep_copy_cstring(alloc, "", val));
  EXPECT_STREQ("", val);
}
TEST(test_ob_log_miner_utils, ObLogMinerUtilParseLineString)
{
  const char *key_str = "PROGRESS";
  const char *buf = "PROGRESS=abcde\n";
  int64_t pos = 0;
  ObString value;
  ObArenaAllocator alloc;
  char *cstr = nullptr;
  EXPECT_EQ(OB_SUCCESS, parse_line(key_str, buf, strlen(buf), pos, value));
  EXPECT_EQ(value, ObString("abcde"));
  EXPECT_EQ(pos, strlen(buf));
  pos = 0;
  key_str = "CUR_FILE_ID";
  EXPECT_EQ(OB_INVALID_DATA, parse_line(key_str, buf, strlen(buf), pos, value));
  buf = "CUR_FILE_ID=001\n";
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, parse_line(key_str, buf, strlen(buf), pos, value));
  EXPECT_EQ(ObString("001"), value);
  EXPECT_EQ(pos, strlen(buf));
  pos = 0;
  key_str = "kkkk";
  buf = "kkkk=llll\n";
  EXPECT_EQ(OB_SUCCESS, parse_line(key_str, buf, strlen(buf), pos, alloc, cstr));
  EXPECT_STREQ("llll", cstr);
  const char *charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwlyz_-.";
  const int64_t charset_len = strlen(charset);
  char data_buf[100];
  char key[30];
  char val[30];
  for (int i = 0; i < 10000; i++) {
    int64_t kv_len = abs(rand()) % (sizeof(key)-1) + 1;
    pos = 0;
    for (int j = 0; j < kv_len; j++) {
      int k = rand() % charset_len;
      int v = rand() % charset_len;
      key[j] = charset[k];
      val[j] = charset[v];
    }
    key[kv_len] = '\0';
    val[kv_len] = '\0';
    snprintf(data_buf, 100, "%s=%s\n", key, val);
    EXPECT_EQ(OB_SUCCESS, parse_line(key, data_buf, sizeof(data_buf), pos, value));
    EXPECT_EQ(ObString(val), value);
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, parse_line(key, data_buf, sizeof(data_buf), pos, alloc, cstr));
    EXPECT_STREQ(val, cstr);
  }
}

TEST(test_ob_log_miner_utils, ObLogMinerUtilsStrToLL)
{
  int64_t num = 0;
  EXPECT_EQ(OB_SUCCESS, logminer_str2ll("111222", num));
  EXPECT_EQ(111222, num);
  EXPECT_EQ(OB_INVALID_ARGUMENT, logminer_str2ll("9223372036854775808", num));
  EXPECT_EQ(OB_INVALID_ARGUMENT, logminer_str2ll("-9223372036854775809", num));
  EXPECT_EQ(OB_INVALID_ARGUMENT, logminer_str2ll(nullptr, num));
  EXPECT_EQ(OB_SUCCESS, logminer_str2ll("12345aaa", num));
  EXPECT_EQ(12345, num);
  const char *timestr = "1688473439\n";
  char *end_ptr = nullptr;
  EXPECT_EQ(OB_SUCCESS, logminer_str2ll(timestr, end_ptr, num));
  EXPECT_EQ(1688473439, num);
  EXPECT_EQ(end_ptr, timestr + strlen(timestr)-1);
  timestr = "aaaaa";
  EXPECT_EQ(OB_INVALID_ARGUMENT, logminer_str2ll(timestr, end_ptr, num));
  EXPECT_EQ(end_ptr, timestr);
}

TEST(test_ob_log_miner_utils, ObLogMinerUtilsParseLine)
{
  const char *key_str = "PROGRESS";
  const char *buf = "PROGRESS=1234567\n";
  int64_t pos = 0;
  int64_t data = 0;
  EXPECT_EQ(OB_SUCCESS, parse_line(key_str, buf, strlen(buf), pos, data));
  EXPECT_EQ(data, 1234567);
  EXPECT_EQ(pos, strlen(buf));
  pos = 0;
  key_str = "CUR_FILE_ID";
  EXPECT_EQ(OB_INVALID_DATA, parse_line(key_str, buf, strlen(buf), pos, data));
  buf = "CUR_FILE_ID=001\n";
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, parse_line(key_str, buf, strlen(buf), pos, data));
  EXPECT_EQ(1, data);
  EXPECT_EQ(pos, strlen(buf));
  buf = "CUR_FILE_ID:1\n";
  pos = 0;
  EXPECT_EQ(OB_INVALID_DATA, parse_line(key_str, buf, strlen(buf), pos, data));
  buf = "CUR_FILE_ID=1";
  pos = 0;
  EXPECT_EQ(OB_SIZE_OVERFLOW, parse_line(key_str, buf, strlen(buf), pos, data));
  const char *charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwlyz_-.";
  const int64_t charset_len = strlen(charset);
  char data_buf[100];
  char key[30];
  for (int i = 0; i < 10000; i++) {
    int64_t key_len = abs(rand()) % (sizeof(key)-1) + 1;
    pos = 0;
    for (int j = 0; j < key_len; j++) {
      int k = rand() % charset_len;
      key[j] = charset[k];
    }
    key[key_len] = '\0';
    int64_t val = rand();
    snprintf(data_buf, 100, "%s=%ld\n", key, val);
    EXPECT_EQ(OB_SUCCESS, parse_line(key, data_buf, sizeof(data_buf), pos, data));
    EXPECT_EQ(val, data);
  }
}

TEST(test_ob_log_miner_utils, ParseDigit)
{

  int64_t pos = 0;
  int64_t data = 0;
  const char *buf = "12345667";
  EXPECT_EQ(OB_SUCCESS, parse_int(buf, strlen(buf), pos, data));
  EXPECT_EQ(data, 12345667);
  EXPECT_EQ(pos, strlen(buf));
  pos = 0; data = 0;
  const char buf0[] = {'1','2','3','4','5'};
  EXPECT_EQ(OB_SUCCESS, parse_int(buf0, sizeof(buf0), pos, data));
  EXPECT_EQ(data, 12345);
  EXPECT_EQ(pos, sizeof(buf0));
  pos = 0; data = 0;
  const char *buf2 = "  +1234567aaa";
  EXPECT_EQ(OB_SUCCESS, parse_int(buf2, strlen(buf2), pos, data));
  EXPECT_EQ(data, 1234567);
  EXPECT_EQ(pos, 10);
  pos = 0; data = 0;
  const char *buf3 = "  -1234567aaa";
  EXPECT_EQ(OB_SUCCESS, parse_int(buf3, strlen(buf3), pos, data));
  EXPECT_EQ(data, -1234567);
  EXPECT_EQ(pos, 10);
  pos = 0; data = 0;
  const char *buf4 = "9223372036854775807";
  EXPECT_EQ(OB_SUCCESS, parse_int(buf4, strlen(buf4), pos, data));
  EXPECT_EQ(data, 9223372036854775807LL);
  EXPECT_EQ(pos, strlen(buf4));
  pos = 0; data = 0;
  const char *buf5 = "-9223372036854775808";
  EXPECT_EQ(OB_SUCCESS, parse_int(buf5, strlen(buf5), pos, data));
  EXPECT_EQ(data, -9223372036854775807LL - 1);
  EXPECT_EQ(pos, strlen(buf5));
  pos = 0; data = 0;
  const char *buf6 = "9223372036854775808";
  EXPECT_EQ(OB_SIZE_OVERFLOW, parse_int(buf6, strlen(buf6), pos, data));
  pos = 0; data = 0;
  const char *buf7 = "-9223372036854775809";
  EXPECT_EQ(OB_SIZE_OVERFLOW, parse_int(buf7, strlen(buf7), pos, data));
  pos = 0; data = 0;
  const char *buf_invalid = "+-111";
  EXPECT_EQ(OB_INVALID_DATA, parse_int(buf_invalid, strlen(buf_invalid), pos, data));
  pos = 0; data = 0;
  buf_invalid = "  +  111";
  EXPECT_EQ(OB_INVALID_DATA, parse_int(buf_invalid, strlen(buf_invalid), pos, data));
  pos = 0; data = 0;
  buf_invalid = "  a111";
  EXPECT_EQ(OB_INVALID_DATA, parse_int(buf_invalid, strlen(buf_invalid), pos, data));
  pos = 0; data = 0;
  char buf_tmp[100];
  for (int i = 0; i < 10000; i++) {
    pos = 0;
    int64_t data1 = rand();
    EXPECT_EQ(OB_SUCCESS, databuff_printf(buf_tmp, 100, pos, "CUR_FILE_ID="));
    int64_t tmp_pos = pos;
    EXPECT_EQ(OB_SUCCESS, databuff_printf(buf_tmp, 100, pos, "%ld\n", data1));
    EXPECT_EQ(OB_SUCCESS, parse_int(buf_tmp, 100, tmp_pos, data));
    EXPECT_EQ(data, data1);
  }
}

TEST(test_ob_log_miner_utils, ExpectToken)
{
  const char *buf = "MIN_COMMIT_TS=1";
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "MIN_COMMIT_TS"));
  EXPECT_EQ(pos, 13);
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "="));
  EXPECT_EQ(pos, 14);
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "1"));
  EXPECT_EQ(pos, 15);
  pos = 0;

  buf = "abcdefghxxxx";
  EXPECT_EQ(OB_SIZE_OVERFLOW, expect_token(buf, strlen(buf), pos, "abcdefghxxxxx"));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "abcdefgh"));
  EXPECT_EQ(pos, 8);
  EXPECT_EQ(OB_SIZE_OVERFLOW, expect_token(buf, strlen(buf), pos, "xxxxx"));
  pos = 8;
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "xxxx"));
  pos = 0;

  buf = "CUR_FILE_ID=1\n";
  EXPECT_EQ(OB_INVALID_DATA, expect_token(buf, strlen(buf), pos, "MAX_FILE_ID"));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "CUR_FILE_ID"));
  EXPECT_EQ(pos, 11);
  EXPECT_EQ(OB_INVALID_DATA, expect_token(buf, strlen(buf), pos, "1"));
  pos = 11;
  EXPECT_EQ(OB_SUCCESS, expect_token(buf, strlen(buf), pos, "=1\n"));
  EXPECT_EQ(pos, 14);
  pos = 0;
}

TEST(test_ob_log_miner_utils, ConvertUintToBit)
{
  uint64_t val = 0;
  ObArenaAllocator tmp_alloc;
  ObStringBuffer bit_str(&tmp_alloc);
  EXPECT_EQ(OB_SUCCESS, uint_to_bit(val, bit_str));
  EXPECT_STREQ("0", bit_str.ptr());
  bit_str.reset();

  val = 1;
  EXPECT_EQ(OB_SUCCESS, uint_to_bit(val, bit_str));
  EXPECT_STREQ("1", bit_str.ptr());
  bit_str.reset();

  val = 6;
  EXPECT_EQ(OB_SUCCESS, uint_to_bit(val, bit_str));
  EXPECT_STREQ("110", bit_str.ptr());
  bit_str.reset();

  val = 1836032;
  EXPECT_EQ(OB_SUCCESS, uint_to_bit(val, bit_str));
  EXPECT_STREQ("111000000010000000000", bit_str.ptr());
  bit_str.reset();

  val = 183848324234;
  EXPECT_EQ(OB_SUCCESS, uint_to_bit(val, bit_str));
  EXPECT_STREQ("10101011001110001101101100110010001010", bit_str.ptr());
  bit_str.reset();
}

TEST(test_ob_log_miner_utils, ObLogMinerUtilsWriteRecord)
{
  ObConcurrentFIFOAllocator alloc;
  ObStringBuffer str_buf(&alloc);
  KeyArray key_arr;
  EXPECT_EQ(OB_SUCCESS, alloc.init(1 << 20, 1 << 20, 1 << 13));
  EXPECT_EQ(key_arr.push_back("aaa"), OB_SUCCESS);
  EXPECT_EQ(key_arr.push_back("bbb"), OB_SUCCESS);
  EXPECT_EQ(key_arr.push_back("ccc"), OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, write_keys(key_arr, str_buf));
  EXPECT_STREQ("\"aaa/bbb/ccc\"", str_buf.ptr());
  str_buf.reset();
  EXPECT_EQ(OB_SUCCESS, write_signed_number(-12345, str_buf));
  EXPECT_STREQ("-12345", str_buf.ptr());
  str_buf.reset();
  EXPECT_EQ(OB_SUCCESS, write_unsigned_number(1111, str_buf));
  EXPECT_STREQ("1111", str_buf.ptr());
  str_buf.reset();
  EXPECT_EQ(OB_SUCCESS, write_string_no_escape("aaaaabbbb'", str_buf));
  EXPECT_STREQ("\"aaaaabbbb'\"", str_buf.ptr());
  str_buf.reset();
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_utils.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_utils.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
