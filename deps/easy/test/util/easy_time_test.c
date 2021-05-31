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

#include "util/easy_time.h"
#include <easy_test.h>

TEST(easy_time, localtime)
{
  time_t t;
  struct tm tm1, tm2;
  int cnt, cnt1;

  t = time(NULL);
  localtime_r(&t, &tm1);
  easy_localtime(&t, &tm2);

  if (memcmp(&tm1, &tm2, 32))
    EXPECT_TRUE(0);

  t -= 86400 * 365 * 15;
  cnt = cnt1 = 0;

  for (; t > 0 && t < 0x7fff0000; t += 40000) {
    localtime_r(&t, &tm1);
    easy_localtime(&t, &tm2);
    cnt++;

    if (memcmp(&tm1, &tm2, 32) == 0)
      cnt1++;
  }

  EXPECT_EQ(cnt1, cnt);

  // < 0
  t = -86400;
  t *= (365 * 1000);
  timezone = 3600 * 23;

  for (; t < 0; t += (86400 * 100)) {
    easy_localtime(&t, &tm2);
  }
}

TEST(easy_time, now)
{
  int64_t s = easy_time_now();
  usleep(1);
  int64_t e = easy_time_now();
  int t = time(NULL);
  EXPECT_TRUE(e > s);
  s /= 1000000;
  e /= 1000000;
  EXPECT_TRUE(s == t || e == t);
}
