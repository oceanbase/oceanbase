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

#include "lib/time/tbtimeutil.h"
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace oceanbase {
namespace obsys {

/*
 * get current time
 */
int64_t CTimeUtil::getTime()
{
  struct timeval t;
  (void)gettimeofday(&t, NULL);
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

int64_t CTimeUtil::getMonotonicTime()
{
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_nsec / 1000));
}

/**
 * format int into 20080101101010
 */
char* CTimeUtil::timeToStr(time_t t, char* dest)
{
  struct tm r;
  memset(&r, 0, sizeof(r));
  if (localtime_r((const time_t*)&t, &r) == NULL) {
    fprintf(stderr, "TIME: %m (%d)\n", errno);
    dest[0] = '\0';
    return dest;
  }
  sprintf(dest, "%04d%02d%02d%02d%02d%02d", r.tm_year + 1900, r.tm_mon + 1, r.tm_mday, r.tm_hour, r.tm_min, r.tm_sec);
  return dest;
}

/**
 * format string to time(local)
 */
/*
int CTimeUtil::strToTime(char *str)
{
  if (str == NULL || strlen(str) != 14) {
    return 0;
  }
  char *p = str;
  while((*p)) {
    if ((*p) < '0' || (*p) > '9') return 0;
    p ++;
  }

  struct tm t;
  t.tm_year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0') - 1900;
  t.tm_mon = (str[4] - '0') * 10 + (str[5] - '0') - 1;
  t.tm_mday = (str[6] - '0') * 10 + (str[7] - '0');
  t.tm_hour = (str[8] - '0') * 10 + (str[9] - '0');
  t.tm_min = (str[10] - '0') * 10 + (str[11] - '0');
  t.tm_sec = (str[12] - '0') * 10 + (str[13] - '0');
  t.tm_isdst = 0;
  int t1 = mktime(&t);
  return t1;
}
*/
}  // namespace obsys
}  // namespace oceanbase
