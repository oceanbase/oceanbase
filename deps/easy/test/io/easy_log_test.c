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

#include "io/easy_log.h"
#include "io/easy_io.h"
#include <string.h>
#include <easy_test.h>

static int test_easy_log_cnt = 0;
static void test_easy_log_print(const char* message)
{
  test_easy_log_cnt++;
}
TEST(easy_log, print)
{
  int i;
  easy_log_level = (easy_log_level_t)10;
  easy_fatal_log("log\n\n\n");

  easy_log_set_print(test_easy_log_print);

  for (i = EASY_LOG_OFF; i <= EASY_LOG_ALL; i++) {
    test_easy_log_cnt = 0;
    easy_log_level = (easy_log_level_t)i;
    easy_fatal_log("a:%d\n", i);
    easy_error_log("a:%d\n", i);
    easy_warn_log("a:%d\n", i);
    easy_info_log("a:%d\n", i);
    easy_debug_log("a:%d\n", i);
    easy_trace_log("a:%d\n", i);
    int d = 0;
    if (i < EASY_LOG_USER_ERROR) {
      d = 2;
    } else if (i < EASY_LOG_ALL) {
      d = 1;
    }
    EXPECT_EQ(test_easy_log_cnt, (i + d));
  }

  // other branch
  easy_fatal_log("");
  easy_io_t* eio = (easy_io_t*)easy_malloc(sizeof(easy_io_t));
  memset(eio, 0, sizeof(easy_io_t));
  easy_baseth_self = ((easy_baseth_t*)eio);
  easy_fatal_log("");
  easy_free(eio);
}
void easy_log_format_test(
    int level, const char* file, int line, const char* function, uint64_t location_hash_val, const char* fmt, ...)
{
  va_list args;
  va_start(args, fmt);
  fprintf(stderr, fmt, args);
  va_end(args);
}
TEST(easy_log, format)
{
  easy_log_set_format(easy_log_format_test);
  test_easy_log_cnt = 0;
  easy_error_log("test_easy_log_cnt: %d\n", test_easy_log_cnt);
  easy_log_set_format(easy_log_format_default);
}

void easy_log_format_vsnprintf(char* buf, int size, const char* fmt, ...)
{
  va_list args;
  va_start(args, fmt);
  easy_vsnprintf(buf, size, fmt, args);
  va_end(args);
}

// test "%.*s", 0, NULL
TEST(easy_log, vsnprintf)
{
  char buf[1024];
  char* dst;

  dst = "easy_vsnprintf:";
  easy_log_format_vsnprintf(buf, 1024, "easy_vsnprintf:%.*s", 0, NULL);
  EXPECT_TRUE(strlen(dst) == strlen(buf) && memcmp(buf, dst, strlen(buf)) == 0);

  dst = "easy_vsnprintf: taob";
  easy_log_format_vsnprintf(buf, 1024, "easy_vsnprintf: %.*s", 4, "taobao");
  EXPECT_TRUE(strlen(dst) == strlen(buf) && memcmp(buf, dst, strlen(buf)) == 0);
  dst = "easy_vsnprintf: tao";
  EXPECT_TRUE(strlen(dst) != strlen(buf));
}
