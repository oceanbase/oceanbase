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
#include "share/ob_define.h"
#include "deps/oblib/src/lib/container/ob_array_iterator.h"
#include "logservice/libobcdc/src/ob_log_utils.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{

/*
 * TEST1.
 * Test split.
 */
TEST(utils, split)
{
  int err = OB_SUCCESS;
  char str[] = "tt1.database1";
  const char *delimiter = ".";
  const char *res[16];
  int64_t res_cnt = 0;

  err = split(str, delimiter, 2, res, res_cnt);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(2, res_cnt);
  EXPECT_STREQ("tt1", res[0]);
  EXPECT_STREQ("database1", res[1]);

  char str1[] = "tt2.database2.test";
  err = split(str1, delimiter, 3, res, res_cnt);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(3, res_cnt);
  EXPECT_STREQ("tt2", res[0]);
  EXPECT_STREQ("database2", res[1]);
  EXPECT_STREQ("test", res[2]);
}

/*
 * TEST2.
 * Test split. Boundary tests
 */
TEST(utils, split_boundary)
{
  int err = OB_SUCCESS;
  char str[] = "tt1.database1";
  const char *delimiter = ".";
  const char *res[16];
  int64_t res_cnt = 0;

  err = split(NULL, delimiter, 2, res, res_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  EXPECT_EQ(0, res_cnt);

  char str1[] = "";
  err = split(str1, delimiter, 2, res, res_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  EXPECT_EQ(0, res_cnt);

  err = split(str, NULL, 2, res, res_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  EXPECT_EQ(0, res_cnt);

  const char *delimiter1 = "";
  err = split(str, delimiter1, 2, res, res_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  EXPECT_EQ(0, res_cnt);

  // Test for incoming length errors
  err = split(str, delimiter, 1, res, res_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  EXPECT_EQ(1, res_cnt);
}

TEST(utils, split_int64_all)
{
  char delimiter = '|';
  ObString str;
  const char *ptr = NULL;
  ObSEArray<int64_t, 8> ret_array;

  // Store a single number
  ptr = "100";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(1, ret_array.count());
  EXPECT_EQ(100, ret_array.at(0));

  // Store multi numbers
  ptr = "100|2000|30000|400000";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(4, ret_array.count());
  EXPECT_EQ(100, ret_array.at(0));
  EXPECT_EQ(2000, ret_array.at(1));
  EXPECT_EQ(30000, ret_array.at(2));
  EXPECT_EQ(400000, ret_array.at(3));

  // Store multiple numbers with a separator at the end
  ptr = "100|2000|30000|400000|";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(4, ret_array.count());
  EXPECT_EQ(100, ret_array.at(0));
  EXPECT_EQ(2000, ret_array.at(1));
  EXPECT_EQ(30000, ret_array.at(2));
  EXPECT_EQ(400000, ret_array.at(3));

  // no number
  ptr = "";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(0, ret_array.count());

  // obly seperator
  ptr = "|";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(0, ret_array.count());

  // There are no numbers, only invalid content
  ptr = ",";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_INVALID_DATA, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(0, ret_array.count());

  // Numerical limit values
  char max_int[100];
  snprintf(max_int, sizeof(max_int), "%ld", INT64_MAX);
  str.assign_ptr(max_int, (ObString::obstr_size_t)strlen(max_int));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(1, ret_array.count());
  EXPECT_EQ(INT64_MAX, ret_array.at(0));

  // Exceeding numerical limits
  std::string over_size_int(100, '9');
  str.assign_ptr(over_size_int.c_str(), (ObString::obstr_size_t)strlen(over_size_int.c_str()));
  ret_array.reuse();
  EXPECT_EQ(OB_INVALID_DATA, split_int64(str, delimiter, ret_array));

  // Use other delimite characters
  // Only the first one can be parsed
  ptr = "100,200,300";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_INVALID_DATA, split_int64(str, delimiter, ret_array));

  // constains other char
  ptr = "100a|200b|300c";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_INVALID_DATA, split_int64(str, delimiter, ret_array));

  // delimite at first pos
  ptr = "|100|200|";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(2, ret_array.count());
  EXPECT_EQ(100, ret_array.at(0));
  EXPECT_EQ(200, ret_array.at(1));

  // The separator appears several times in succession
  ptr = "300||400|||500|";
  str.assign_ptr(ptr, (ObString::obstr_size_t)strlen(ptr));
  ret_array.reuse();
  EXPECT_EQ(OB_SUCCESS, split_int64(str, delimiter, ret_array));
  EXPECT_EQ(3, ret_array.count());
  EXPECT_EQ(300, ret_array.at(0));
  EXPECT_EQ(400, ret_array.at(1));
  EXPECT_EQ(500, ret_array.at(2));
}

TEST(utils, kv_pair)
{
  int ret = OB_SUCCESS;
  char kv_str[] = "test:999";
  const char *delimiter1 = ":";
  const char *delimiter2 = "%";

  ObLogKVCollection::KVPair kvpair;
  ret = kvpair.init(delimiter1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = kvpair.deserialize(kv_str);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("test", kvpair.get_key());
  EXPECT_STREQ("999", kvpair.get_value());

  kvpair.reset();
  char key[] = "kjdngasdey";
  char value[] = "vaksahgasfashjlue";
  ret = kvpair.init(delimiter2);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = kvpair.set_key_and_value(key, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("kjdngasdey", kvpair.get_key());
  EXPECT_STREQ("vaksahgasfashjlue", kvpair.get_value());
  int64_t pos = 0;
  int64_t len = kvpair.length();
  EXPECT_EQ(strlen(key) + strlen(value) + strlen(delimiter2), len);
  char buf[len+1];
  ret = kvpair.serialize(buf, len+1, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("kjdngasdey%vaksahgasfashjlue", buf);
}

TEST(utils, kv_collection)
{
  int ret = OB_SUCCESS;
  char kv_str[] = "data:2346234;test:5asdfgasf; time:21354213";
  int64_t origin_len = strlen(kv_str);
  const char *pair_delimiter = "; ";
  const char *kv_delimiter = ":";
  ObLogKVCollection kv_c;
  ret = kv_c.init(kv_delimiter, pair_delimiter);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = kv_c.deserialize(kv_str);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, kv_c.size());
  int64_t len = kv_c.length();
  EXPECT_EQ(origin_len, len-1);
  bool contain = false;
  ret = kv_c.contains_key("data", contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, contain);
  ret = kv_c.contains_key("versin", contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(false, contain);
  const char *value_time = NULL;
  ret = kv_c.get_value_of_key("time", value_time);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("21354213", value_time);
  kv_c.reset();

  // test append
  kv_c.init(kv_delimiter, pair_delimiter);
  ObLogKVCollection::KVPair kvpair;
  char key[] = "jakds";
  char value[] = "dsagads";
  ret = kvpair.init(kv_delimiter);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = kvpair.set_key_and_value(key, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, kvpair.is_valid());
  ret = kv_c.append_kv_pair(kvpair);
  EXPECT_EQ(OB_SUCCESS, ret);

  kvpair.reset();
  char key1[] = "time";
  char value1[] = "1237851204";
  ret = kvpair.init(kv_delimiter);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = kvpair.set_key_and_value(key1, value1);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, kvpair.is_valid());
  ret = kv_c.append_kv_pair(kvpair);
  EXPECT_EQ(OB_SUCCESS, ret);
  kvpair.reset();
  ret = kv_c.contains_key("time1", contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(false, contain);
  origin_len = strlen(key) + strlen(value) + strlen(key1) + strlen(value1)
    + 2 * strlen(kv_delimiter) + strlen(pair_delimiter);
  len = kv_c.length();
  EXPECT_EQ(origin_len, len);
  char buf[len+1];
  int64_t pos = 0;
  ret = kv_c.serialize(buf, len+1, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("jakds:dsagads; time:1237851204", buf);
}

TEST(utils, cstring_to_num)
{
  char numstr1[] = "123412";
  char numstr2[] = "-683251";
  char numstr3[] = "0";
  char numstr4[] = "a123";
  char numstr5[] = " 123";
  char numstr6[] = "";
  int64_t val = 0;
  int ret = c_str_to_int(numstr1, val);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(123412, val);
  ret = c_str_to_int(numstr2, val);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(-683251, val);
  ret = c_str_to_int(numstr3, val);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, val);
  ret = c_str_to_int(numstr4, val);
  EXPECT_EQ(OB_INVALID_DATA, ret);
  ret = c_str_to_int(numstr5, val);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(123, val);
  ret = c_str_to_int(numstr6, val);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(utils, unique_arr_uint64)
{
  auto fn = [](uint64_t &a, uint64_t &b) { return a < b; };
  ObArray<uint64_t> arr;
  EXPECT_EQ(OB_SUCCESS, arr.push_back(0));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(2));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(1));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(3));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(1));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(2));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(3));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(2));
  EXPECT_EQ(OB_SUCCESS, sort_and_unique_array(arr, fn));
  EXPECT_EQ(4, arr.count());
  EXPECT_EQ(0, arr.at(0));
  EXPECT_EQ(1, arr.at(1));
  EXPECT_EQ(2, arr.at(2));
  EXPECT_EQ(3, arr.at(3));
}

TEST(utils, unique_arr_lsn)
{
  auto fn = [](palf::LSN &a, palf::LSN &b) { return a < b; };
  ObSEArray<palf::LSN, 8> arr;
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(0)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(2)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(1)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(3)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(1)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(2)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(3)));
  EXPECT_EQ(OB_SUCCESS, arr.push_back(palf::LSN(2)));
  EXPECT_EQ(OB_SUCCESS, sort_and_unique_array(arr, fn));
  EXPECT_EQ(4, arr.count());
  EXPECT_EQ(palf::LSN(0), arr.at(0));
  EXPECT_EQ(palf::LSN(1), arr.at(1));
  EXPECT_EQ(palf::LSN(2), arr.at(2));
  EXPECT_EQ(palf::LSN(3), arr.at(3));
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_log_utils.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_log_utils.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
//  testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
