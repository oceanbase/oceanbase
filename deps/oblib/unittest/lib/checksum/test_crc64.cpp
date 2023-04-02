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

#include <pthread.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#include "lib/checksum/ob_crc64.h"

using namespace oceanbase::common;
using namespace std;

char *rand_str(char *str, const int64_t len)
{
  int64_t i;
  for(i = 0; i< len; ++i)
      str[i] =(char)('A' + rand() % 26);
  str[++i] = '\0';
  return str;
}

int64_t get_current_time_us() {
   timeval tv;
   gettimeofday(&tv, 0);
   return (int64_t)tv.tv_sec * 1000000 + (int64_t)tv.tv_usec;
}

TEST(TestCrc64, common)
{
  const int64_t STR_LEN = 10240;
  char *tmp_str = new char[STR_LEN];
  char *str = NULL;
  uint64_t intermediate_hash = 0;
  for (int64_t i = 1; i < (STR_LEN - 1); ++i) {
    str = rand_str(tmp_str, i);
    uint64_t manually_hash = crc64_sse42_manually(intermediate_hash, str, i);
    uint64_t fast_manually_hash = fast_crc64_sse42_manually(intermediate_hash, str, i);
    uint64_t sse42_hash = crc64_sse42(intermediate_hash, str, i);
    uint64_t isal_hash = ob_crc64_isal(intermediate_hash, str, i);
    ASSERT_EQ(manually_hash, fast_manually_hash);
    ASSERT_EQ(manually_hash, sse42_hash);
    ASSERT_EQ(manually_hash, isal_hash);
    //cout << "st = "<< tmp_str << endl;
    //cout << "crc64c = "<< manually_hash << endl;
  }
}

TEST(TestCrc64, test_speed)
{
  const int64_t STR_LEN = 2 << 20;
  const int64_t COUNT = 100;
  char *tmp_str = new char[STR_LEN];

  for (int64_t i = 2 << 8; i <= STR_LEN; i *= 2) {

    int64_t start = get_current_time_us();
    for (int64_t j = 0; j < COUNT; ++j) {
      crc64_sse42_manually(0, tmp_str, i);
    }
    int64_t end = get_current_time_us();
    cout << "     crc64_sse42_manually, execut_count = "<< COUNT << ", cost_us = " << end - start << " len = " << i << endl;

    start = get_current_time_us();
    for (int64_t j = 0; j < COUNT; ++j) {
      fast_crc64_sse42_manually(0, tmp_str, i);
    }
    end = get_current_time_us();
    cout << "fast_crc64_sse42_manually, execut_count = "<< COUNT << ", cost_us = " << end - start << " len = " << i << endl;

    start = get_current_time_us();
    for (int64_t j = 0; j < COUNT; ++j) {
      crc64_sse42(0, tmp_str, i);
    }
    end = get_current_time_us();
    cout << "          ob_crc64(sse42), execut_count = "<< COUNT << ", cost_us = " << end - start << " len = " << i << endl;

    start = get_current_time_us();
    for (int64_t j = 0; j < COUNT; ++j) {
      ob_crc64_isal(0, tmp_str, i);
    }
    end = get_current_time_us();
    cout << "          ob_crc64(ob_crc64_isal), execut_count = " << COUNT << ", cost_us = " << end - start << " len = " << i
         << endl;

    cout << endl;
    cout << endl;
  }
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
