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

#define USING_LOG_PREFIX SERVER
#include <gtest/gtest.h>
#include "observer/ob_server.h"
#include "observer/dbms_scheduler/ob_dbms_sched_time_utils.h"
#include "observer/dbms_scheduler/parser/dbms_sched_calendar_parser.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::dbms_scheduler;
using namespace observer;
using namespace test;

class ObDBMSSchedCalendarTest : public ::testing::Test
{
public:
  ObDBMSSchedCalendarTest() {}
  virtual ~ObDBMSSchedCalendarTest() {}
  virtual void SetUp() { init(); }
  virtual void TearDown() {}
  virtual void init() {  }

public:
  // data members

};

TEST_F(ObDBMSSchedCalendarTest, test_resolve)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("calendar_test");
  ObStmtNodeTree *parse_tree = NULL;
  ObDBMSSchedCalendarParser parser(allocator, ObCharsets4Parser());
  ASSERT_EQ(OB_SUCCESS, parser.parse("FREQ=YEARLY; BYYEARDAY=255; BYMONTH=3,4,5; BYMONTHDAY=1,5,10; BYHOUR=20,21,22; BYMINUTE=5,10,15", parse_tree));
  CK (OB_NOT_NULL(parse_tree));
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_minutely)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  if (1 < current_tm.tm_min && 58 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; BYSECOND=0,59");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_sec, 59);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);


    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; BYSECOND=0,1");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
    next_time = next_date/1000000LL;
    next_tm = *localtime(&next_time);
    //应该在下一分钟的第 1 秒执行
    ASSERT_EQ(next_tm.tm_sec, 0);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min + 1);

    //当前秒等于设定秒 如 xx:10  bysecond = 10 应该在 xx + 1::10 执行
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; BYSECOND=%d", current_tm.tm_sec);
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
    next_time = next_date/1000000LL;
    next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min + 1);

    //测试interval
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; INTERVAL=2; BYSECOND=%d", current_tm.tm_sec);
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
    next_time = next_date/1000000LL;
    next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min + 2);

    //不指定BYSECOND, start date = 2025-07-17 09:57:38.050354
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; INTERVAL=1");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
    next_time = next_date/1000000LL;
    next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_sec, 38);
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_hourly)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  if (1 < current_tm.tm_hour && 22 > current_tm.tm_hour
   && 1 < current_tm.tm_min && 59 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    //当前秒小于设定秒 如 当前 10:10  byminute=10 bysecond = 59
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; BYMINUTE=%d; BYSECOND=59", current_tm.tm_min);
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    }

    //当前秒小于设定秒,当前分小于设定分 如 当前 10:10  byminute=59 bysecond=59
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; BYMINUTE=59; BYSECOND=59");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      //下次执行在 59 分 59 秒
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, 59);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    }

    //当前秒位于设定秒中间,当前分小于设定分 如 当前 10:10  byminute=59 bysecond=1,59
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; BYMINUTE=59; BYSECOND=1,59");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      //下次执行在 59 分 1 秒
      ASSERT_EQ(next_tm.tm_sec, 1);
      ASSERT_EQ(next_tm.tm_min, 59);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    }

    //当前秒位于设定秒中间,当前分大于设定分 如 当前 10:10  byminute=0,1 bysecond=1,59
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; BYMINUTE=0,1; BYSECOND=1,59");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      //下次执行在下一小时 0 分 1 秒
      ASSERT_EQ(next_tm.tm_sec, 1);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour + 1);
    }

    //测试interval
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; INTERVAL=2; BYMINUTE=0,1; BYSECOND=1,59");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 1);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour + 2);
    }

    //不指定BYMINUTE, start date = 2025-07-17 09:57:38.050354
    {
      snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; BYSECOND=1,59");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_min, 57);
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_daily)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  char days[7][4] = {"SUN","MON","TUE","WED","THU","FRI","SAT"};
  if (1 < current_tm.tm_mday && 27 > current_tm.tm_mday
   && 1 < current_tm.tm_hour && 23 > current_tm.tm_hour
   && 1 < current_tm.tm_min && 59 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    {
      snprintf(calendar_expr_buf, 256, "FREQ=DAILY; BYHOUR=%d; BYMINUTE=%d; BYSECOND=59", current_tm.tm_hour, current_tm.tm_min);
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    }

    //进位
    {
      snprintf(calendar_expr_buf, 256, "FREQ=DAILY; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday + 1);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=DAILY; INTERVAL=2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday + 2);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=DAILY; BYMINUTE=0,1; BYSECOND=0,1");
      // 不指定byhour, start date = 2025-07-17 09:57:38.050354
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_hour, 9);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}


TEST_F(ObDBMSSchedCalendarTest, test_freq_weekly)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  char days[7][4] = {"SUN","MON","TUE","WED","THU","FRI","SAT"};
  if (1 < current_tm.tm_yday && 350 > current_tm.tm_yday
   && 1 < current_tm.tm_mday && 24 > current_tm.tm_mday
   && 1 < current_tm.tm_wday && 6 > current_tm.tm_wday
   && 1 < current_tm.tm_hour && 23 > current_tm.tm_hour
   && 1 < current_tm.tm_min && 59 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    {
      snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY; BYDAY=SUN,MON; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_wday, 0);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday+(7-current_tm.tm_wday));
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY; INTERVAL=2; BYDAY=MON,TUE; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_wday, 1);
      ASSERT_EQ(next_tm.tm_yday, current_tm.tm_yday+(15-current_tm.tm_wday));
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      // 不指定byday, start date = 2025-07-17 09:57:38.050354 THR
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_wday, 4);
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}


TEST_F(ObDBMSSchedCalendarTest, test_freq_monthly)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  if (1 < current_tm.tm_mon && 10 > current_tm.tm_mon
   && 1 < current_tm.tm_mday && 28 > current_tm.tm_mday
   && 1 < current_tm.tm_hour && 23 > current_tm.tm_hour
   && 1 < current_tm.tm_min && 59 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    {
      snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; BYMONTHDAY=%d; BYHOUR=%d; BYMINUTE=%d; BYSECOND=59", current_tm.tm_mday, current_tm.tm_hour, current_tm.tm_min);
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
      ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; BYMONTHDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, 1);
      //ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon + 1);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; INTERVAL=2; BYMONTHDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, 1);
      ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon + 2);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      // 不指定bymonthday, start date = 2025-07-17 09:57:38.050354 THR
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_mday, 17);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_yearly)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time); // 将时间戳转换为本地时间的 struct tm
  char calendar_expr_buf[256];
  if (1 < current_tm.tm_mon && 11 > current_tm.tm_mon
   && 1 < current_tm.tm_mday && 28 > current_tm.tm_mday
   && 1 < current_tm.tm_hour && 23 > current_tm.tm_hour
   && 1 < current_tm.tm_min && 59 > current_tm.tm_min
   && 1 < current_tm.tm_sec && 59 > current_tm.tm_sec) {
    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; BYMONTH=%d; BYMONTHDAY=%d; BYHOUR=%d; BYMINUTE=%d; BYSECOND=59", current_tm.tm_mon + 1, current_tm.tm_mday, current_tm.tm_hour, current_tm.tm_min);
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
      ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
      ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; BYYEARDAY=%d; BYHOUR=%d; BYMINUTE=%d; BYSECOND=59", current_tm.tm_yday + 1, current_tm.tm_hour, current_tm.tm_min);
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 59);
      ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
      ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
      ASSERT_EQ(next_tm.tm_yday, current_tm.tm_yday);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; BYMONTH=1,2; BYMONTHDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, 1);
      ASSERT_EQ(next_tm.tm_mon, 0);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 1);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; BYYEARDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_yday, 0);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 1);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=2; BYMONTH=1,2; BYMONTHDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_mday, 1);
      ASSERT_EQ(next_tm.tm_mon, 0);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 2);
    }
    
    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=2; BYYEARDAY=1,2; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 0, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_sec, 0);
      ASSERT_EQ(next_tm.tm_min, 0);
      ASSERT_EQ(next_tm.tm_hour, 0);
      ASSERT_EQ(next_tm.tm_yday, 0);
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 2);
    }

    {
      snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; BYHOUR=0,1; BYMINUTE=0,1; BYSECOND=0,1");
      // 不指定bymonth,bymonthday,byyearday, start date = 2025-07-17 09:57:38.050354 THR, 预期按照bymonth当前月份和日期
      ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, 1752717458050354, next_date));
      time_t next_time = next_date/1000000LL;
      struct tm next_tm = *localtime(&next_time);
      ASSERT_EQ(next_tm.tm_mday, 17);
      ASSERT_EQ(next_tm.tm_mon, 6);
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_only)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time);
  char calendar_expr_buf[256];
  
  // 测试只有 FREQ=YEARLY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=YEARLY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一年相同的时间执行
    ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 1);
    ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  // 测试只有 FREQ=MONTHLY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一月相同的时间执行
    if (current_tm.tm_mon == 11) {
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 1);
      ASSERT_EQ(next_tm.tm_mon, 0);
    } else {
      ASSERT_EQ(next_tm.tm_year, current_tm.tm_year);
      //ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon + 1);
    }
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  // 测试只有 FREQ=WEEKLY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一周相同的时间执行
    ASSERT_EQ(next_tm.tm_wday, current_tm.tm_wday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是下一周
    int days_diff = (int)(difftime(next_time, current_time) / (24 * 3600));
    ASSERT_EQ(days_diff, 7);
    ASSERT_LE(days_diff, 7);
  }

  // 测试只有 FREQ=DAILY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=DAILY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在明天相同的时间执行
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是明天
    int days_diff = (int)(difftime(next_time, current_time) / (24 * 3600));
    ASSERT_EQ(days_diff, 1);
  }

  // 测试只有 FREQ=HOURLY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=HOURLY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一小时相同的时间执行
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是下一小时
    int hours_diff = (int)(difftime(next_time, current_time) / 3600);
    ASSERT_EQ(hours_diff, 1);
  }

  // 测试只有 FREQ=MINUTELY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一分钟相同的时间执行
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是下一分钟
    int minutes_diff = (int)(difftime(next_time, current_time) / 60);
    ASSERT_EQ(minutes_diff, 1);
  }

  // 测试只有 FREQ=SECONDLY 的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=SECONDLY");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一秒执行
    int seconds_diff = (int)(difftime(next_time, current_time));
    ASSERT_EQ(seconds_diff, 1);
  }

  // 测试带 INTERVAL 的 FREQ=YEARLY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=2");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在两年后相同的时间执行
    ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 2);
    ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  // 测试带 INTERVAL 的 FREQ=MONTHLY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; INTERVAL=3");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在三个月后相同的时间执行
    int expected_month = current_tm.tm_mon + 3;
    int expected_year = current_tm.tm_year;
    if (expected_month >= 12) {
      expected_month -= 12;
      expected_year += 1;
    }
    ASSERT_EQ(next_tm.tm_year, expected_year);
    ASSERT_EQ(next_tm.tm_mon, expected_month);
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  // 测试带 INTERVAL 的 FREQ=WEEKLY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY; INTERVAL=2");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在两周后相同的时间执行
    ASSERT_EQ(next_tm.tm_wday, current_tm.tm_wday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是两周后
    int days_diff = (int)(difftime(next_time, current_time) / (24 * 3600));
    ASSERT_EQ(days_diff, 14);
  }

  // 测试带 INTERVAL 的 FREQ=DAILY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=DAILY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在五天后相同的时间执行
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是五天后
    int days_diff = (int)(difftime(next_time, current_time) / (24 * 3600));
    ASSERT_EQ(days_diff, 5);
  }

  // 测试带 INTERVAL 的 FREQ=HOURLY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; INTERVAL=2");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在两小时后相同的时间执行
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是两小时后
    int hours_diff = (int)(difftime(next_time, current_time) / 3600);
    ASSERT_EQ(hours_diff, 2);
  }

  // 测试带 INTERVAL 的 FREQ=MINUTELY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; INTERVAL=10");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在十分钟后相同的时间执行
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
    // 验证是十分钟后
    int minutes_diff = (int)(difftime(next_time, current_time) / 60);
    ASSERT_EQ(minutes_diff, 10);
  }

  // 测试带 INTERVAL 的 FREQ=SECONDLY
  {
    snprintf(calendar_expr_buf, 256, "FREQ=SECONDLY; INTERVAL=30");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在三十秒后执行
    int seconds_diff = (int)(difftime(next_time, current_time));
    ASSERT_EQ(seconds_diff, 30);
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObDBMSSchedCalendarTest, test_freq_only_edge_cases)
{
  ObDBMSSchedTimeUtil util;
  int ret = OB_SUCCESS;
  int64_t next_date;
  time_t current_time = time(NULL);
  struct tm current_tm = *localtime(&current_time);
  char calendar_expr_buf[256];


  // 测试当前时间处于进位临界点的情况，如59分，23时
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_sec = 59;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=MINUTELY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    int min_diff = (int)(difftime(next_time, tmp_time) / (60));
    ASSERT_EQ(min_diff, 5);
  }
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_min = 59;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=HOURLY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    int hour_diff = (int)(difftime(next_time, tmp_time) / (3600));
    ASSERT_EQ(hour_diff, 5);
  }
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_hour = 23;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=DAILY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    int days_diff = (int)(difftime(next_time, tmp_time) / (24 * 3600));
    ASSERT_EQ(days_diff, 5);
  }
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_hour = 23;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=WEEKLY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    int days_diff = (int)(difftime(next_time, tmp_time) / (7 * 24 * 3600));
    ASSERT_EQ(days_diff, 5);
  }
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_mon = 6;
    tmp_tm.tm_mday = 31;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=MONTHLY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_mon, tmp_tm.tm_mon + 5);
  }
  {
    time_t tmp_time = time(NULL);
    struct tm tmp_tm = *localtime(&tmp_time);
    tmp_tm.tm_mon = 11;
    tmp_time = mktime(&tmp_tm);
    snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=5");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, tmp_time * 1000000LL, next_date, tmp_time * 1000000LL));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    ASSERT_EQ(next_tm.tm_year, tmp_tm.tm_year + 5);
  }

  // 测试 INTERVAL=1 的情况（默认值）
  {
    snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=1");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在下一年相同的时间执行
    ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 1);
    ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  // 测试大 INTERVAL 值的情况
  {
    snprintf(calendar_expr_buf, 256, "FREQ=YEARLY; INTERVAL=100");
    ASSERT_EQ(OB_SUCCESS, util.calc_repeat_interval_next_date(calendar_expr_buf, current_time * 1000000LL, next_date));
    time_t next_time = next_date/1000000LL;
    struct tm next_tm = *localtime(&next_time);
    // 应该在100年后相同的时间执行
    ASSERT_EQ(next_tm.tm_year, current_tm.tm_year + 100);
    ASSERT_EQ(next_tm.tm_mon, current_tm.tm_mon);
    ASSERT_EQ(next_tm.tm_mday, current_tm.tm_mday);
    ASSERT_EQ(next_tm.tm_hour, current_tm.tm_hour);
    ASSERT_EQ(next_tm.tm_min, current_tm.tm_min);
    ASSERT_EQ(next_tm.tm_sec, current_tm.tm_sec);
  }

  ASSERT_EQ(OB_SUCCESS, ret);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_dbms_sched_calendar.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

