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
#include <stdint.h>
#include "lib/ob_define.h"
    #include "lib/timezone/ob_timezone_util.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_time_format.h"
#include "lib/timezone/ob_timezone_info.h"

using namespace oceanbase;
using namespace oceanbase::common;

class ObTimeConvertTest : public ::testing::Test
{
public:
  ObTimeConvertTest();
  virtual ~ObTimeConvertTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTimeConvertTest(const ObTimeConvertTest &other);
  ObTimeConvertTest& operator=(const ObTimeConvertTest &other);
};



static bool ob_time_eq(const ObTime& ans, int64_t year, int64_t month, int64_t day,
                       int64_t hour, int64_t minute, int64_t second, int64_t usecond)
{
  bool ret = (ans.parts_[DT_YEAR] == year || -1 == year)
              && (ans.parts_[DT_MON] == month || -1 == month)
              && (ans.parts_[DT_MDAY] == day || -1 == day)
              && (ans.parts_[DT_HOUR] == hour || -1 == hour)
              && (ans.parts_[DT_MIN] == minute || -1 == minute)
              && (ans.parts_[DT_SEC] == second || -1 == second)
              && (ans.parts_[DT_USEC] == usecond || -1 == usecond);
  if (!ret) {
    printf("%04u-%02u-%02u %02u:%02u:%02u.%06u\n",
           ans.parts_[DT_YEAR], ans.parts_[DT_MON], ans.parts_[DT_MDAY],
           ans.parts_[DT_HOUR], ans.parts_[DT_MIN], ans.parts_[DT_SEC], ans.parts_[DT_USEC]);
  }
  return ret;
}

static bool ob_interval_eq(const ObInterval& ans, int64_t year, int64_t month, int64_t day,
                           int64_t hour, int64_t minute, int64_t second, int64_t usecond)
{
  bool ret = (ans.parts_[DT_YEAR] == year || -1 == year)
              && (ans.parts_[DT_MON] == month || -1 == month)
              && (ans.parts_[DT_MDAY] == day || -1 == day)
              && (ans.parts_[DT_HOUR] == hour || -1 == hour)
              && (ans.parts_[DT_MIN] == minute || -1 == minute)
              && (ans.parts_[DT_SEC] == second || -1 == second)
              && (ans.parts_[DT_USEC] == usecond || -1 == usecond);
  if (!ret) {
    printf("%04u-%02u-%02u %02u:%02u:%02u.%06u\n",
           ans.parts_[DT_YEAR], ans.parts_[DT_MON], ans.parts_[DT_MIN],
           ans.parts_[DT_HOUR], ans.parts_[DT_MIN], ans.parts_[DT_SEC], ans.parts_[DT_USEC]);
  }
  return ret;
}

int64_t interval_value(uint32_t day, uint32_t hour, uint32_t minute, uint32_t second, uint32_t usecond)
{
  int64_t value = day;
  value *= HOURS_PER_DAY;
  value += hour;
  value *= MINS_PER_HOUR;
  value += minute;
  value *= SECS_PER_MIN;
  value += second;
  value *= USECS_PER_SEC;
  value += usecond;
  return value;
}

TEST(ObTimeConvertTest, str_to_datetime)
{
//  ObTimezoneUtils zone;
  ObTimeZoneInfo tz_info;
  char buf[50] = {0};
  ObString str;
  int64_t value;

//  zone.init("/usr/share/zoneinfo/America/Chicago");
//  strcpy(buf, "1970-01-01 00:26:30");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 23190 * USECS_PER_SEC);
//  strcpy(buf, "2015-4-15 4:22:7");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1429089727 * USECS_PER_SEC);
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/Cordoba");
//  strcpy(buf, "2015-4-15 6.33.40.0123");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1429090420 * USECS_PER_SEC + 12300);
//  strcpy(buf, "1971.3.28 12.46.30.123");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 39023190 * USECS_PER_SEC + 123000);
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Asia/Tehran");
//  strcpy(buf, "2015/4/15 14.5.41.04560");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1429090541 * USECS_PER_SEC + 45600);
//  strcpy(buf, "1976/12/1 1.41.32.45600");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 218239892 * USECS_PER_SEC + 456000);
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Singapore");
//  strcpy(buf, "2015/4/15 17:37:26");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1429090646 * USECS_PER_SEC);
//  strcpy(buf, "2005/6/29 13:45:38");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1120023938 * USECS_PER_SEC);
//
//  // dst test
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/New_York");
//  strcpy(buf, "2015/4/20 8:4:49");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1429531489 * USECS_PER_SEC);
//  strcpy(buf, "2015/1/15 12:00:00");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, 1421341200 * USECS_PER_SEC);
//
//  // usec < 0
//  zone.parse_timezone_file("/usr/share/zoneinfo/UTC");
//  strcpy(buf, "1969/12/31 23:59:59");
//  str.assign(buf, static_cast<int32_t>(strlen(buf)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, zone.get_tz_ptr(), value));
//  EXPECT_EQ(value, -1 * USECS_PER_SEC);

  // timezone with offset only.
  ObTimeConvertCtx cvrt_ctx(&tz_info, true);
  strcpy(buf, "+8:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  strcpy(buf, "1000-1-1 0:0:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value / USECS_PER_SEC, -30610252800);
  strcpy(buf, "-08:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  strcpy(buf, "2015-7-3 11:12:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value / static_cast<int64_t>(USECS_PER_SEC), 1435950720);

  // NULL timezone
  cvrt_ctx.is_timestamp_ = false;
  strcpy(buf, "1000-1-1 0:0:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value / USECS_PER_SEC, -30610224000);
  strcpy(buf, "2015-7-3 11:12:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value / static_cast<int64_t>(USECS_PER_SEC), 1435921920);
  strcpy(buf, "9999-12-31 23:59:59");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value / static_cast<int64_t>(USECS_PER_SEC), 253402300799);
}



TEST(ObTimeConvertTest, str_to_otimestamp)
{
//  ObTimezoneUtils zone;
  ObTimeZoneInfo tz_info;
  char buf[50] = {0};
  ObString str;
  int64_t value = 0;
  ObOTimestampData ot_data;
  int16_t scale = 0;

  ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);

  // timezone with offset only.
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  strcpy(buf, "+00:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);

#define CHECK_OTIMESTAMP_VALUE(err_time_str, err_no) \
  strcpy(buf, err_time_str); \
  str.assign(buf, static_cast<int32_t>(strlen(buf))); \
  EXPECT_EQ(err_no, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));

  CHECK_OTIMESTAMP_VALUE("-1001-7-3 11:12:00.1234567", OB_ERR_NON_NUMERIC_CHARACTER_VALUE);
  CHECK_OTIMESTAMP_VALUE("10001-7-3 11:12:00.1234567", OB_INVALID_DATE_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-0-3 11:12:00.1234567", OB_ERR_DAY_OF_MONTH_RANGE);
  CHECK_OTIMESTAMP_VALUE("1001-13-3 11:12:00.1234567", OB_INVALID_DATE_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-12-3 11:12:00.1234567", OB_SUCCESS);
  CHECK_OTIMESTAMP_VALUE("1001-1-3 11:12:00.1234567", OB_SUCCESS);
  CHECK_OTIMESTAMP_VALUE("1001-7-0 11:12:00.1234567", OB_ERR_DAY_OF_MONTH_RANGE);
  CHECK_OTIMESTAMP_VALUE("1001-7-32 11:12:00.1234567", OB_ERR_DAY_OF_MONTH_RANGE);
  CHECK_OTIMESTAMP_VALUE("2011-2-29 11:12:00.1234567", OB_INVALID_DATE_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 25:12:00.1234567", OB_ERR_INVALID_HOUR24_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 24:12:00.1234567", OB_ERR_INVALID_HOUR24_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 11:60:00.1234567", OB_ERR_INVALID_MINUTES_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 11:12:60.1234567", OB_ERR_INVALID_SECONDS_VALUE);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 11:12:10.-1234567", OB_ERR_NON_NUMERIC_CHARACTER_VALUE);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 0);
  CHECK_OTIMESTAMP_VALUE("1001-7-3 11:12:10.+1234567", OB_ERR_NON_NUMERIC_CHARACTER_VALUE);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 0);
  CHECK_OTIMESTAMP_VALUE("0000-0-0 00:00:00", OB_ERR_INVALID_YEAR_VALUE);
  CHECK_OTIMESTAMP_VALUE("0000-1-1 00:00:00.000000000", OB_ERR_INVALID_YEAR_VALUE);
  CHECK_OTIMESTAMP_VALUE("9999-12-31 23:59:59.999999999", OB_SUCCESS);

  strcpy(buf, "+8:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  strcpy(buf, "1000-1-1 0:0:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, -30610252800 * USECS_PER_SEC);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 0);
  EXPECT_EQ(ot_data.time_ctx_.offset_min_, 480);
  EXPECT_EQ(scale, 9);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampLTZType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, -30610252800 * USECS_PER_SEC);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 0);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.offset_min_, 0);
  EXPECT_EQ(scale, 9);

  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampNanoType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, -30610224000 * USECS_PER_SEC);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 0);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.offset_min_, 0);
  EXPECT_EQ(scale, 9);


  strcpy(buf, "-8:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  strcpy(buf, "2015-7-3 11:12:00.1234567");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, 1435950720123456);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.get_offset_min(), -480);
  EXPECT_EQ(scale, 7);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampLTZType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, 1435950720123456);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.get_offset_min(), 0);
  EXPECT_EQ(scale, 7);

  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampNanoType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, 1435921920123456);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.offset_min_, 0);
  EXPECT_EQ(scale, 7);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  strcpy(buf, "2015-7-3 11:12:00.1234567 -07:00");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  EXPECT_EQ(ot_data.time_us_, 1435947120123456);
  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
  EXPECT_EQ(ot_data.time_ctx_.get_offset_min(), -420);
  EXPECT_EQ(scale, 7);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  EXPECT_EQ(OB_INVALID_DATE_FORMAT_END, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampLTZType, ot_data, scale));
//  EXPECT_EQ(ot_data.time_us_, 1435947120123456);
//  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
//  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
//  EXPECT_EQ(ot_data.time_ctx_.get_offset_min(), 0);
//  EXPECT_EQ(scale, 7);

  EXPECT_EQ(OB_INVALID_DATE_FORMAT_END, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampNanoType, ot_data, scale));
//  EXPECT_EQ(ot_data.time_us_, 1435921920123456);
//  EXPECT_EQ(ot_data.time_ctx_.tail_nsec_, 700);
//  EXPECT_EQ(ot_data.time_ctx_.store_tz_id_, 0);
//  EXPECT_EQ(ot_data.time_ctx_.offset_min_, 0);
//  EXPECT_EQ(scale, 7);


  // NULL timezone
  cvrt_ctx.is_timestamp_ = false;
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
  strcpy(buf, "1000-1-1 0:0:0");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value, -30610224000 * USECS_PER_SEC);
  strcpy(buf, "9999-12-31 23:59:59");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, nullptr, 0));
  EXPECT_EQ(value, 253402300799 * static_cast<int64_t>(USECS_PER_SEC));
}

#define STR_TO_DATE_SUCC(str, day) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0)); \
    EXPECT_EQ(value, 0); \
  } while (0)

TEST(ObTimeConvertTest, str_to_date)
{
  char buf[50] = {0};
  ObString str;
  int32_t value;


  strcpy(buf, "1970-1-1");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0));
  EXPECT_EQ(value, 0);
  strcpy(buf, "2015-4-15");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0));
  EXPECT_EQ(value, 16540);
  strcpy(buf, "1969-12-31");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0));
  EXPECT_EQ(value, -1);
  strcpy(buf, "1000-1-1");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0));
  EXPECT_EQ(value, -354285);
  strcpy(buf, "9999-12-31");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_date(str, value, 0));
  EXPECT_EQ(value, 2932896);
}

#define STR_TO_INTERVAL_SUCC(str, unit, interval) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_interval(buf, unit, value)); \
    EXPECT_EQ(value, interval); \
  } while (0)

#define STR_TO_INTERVAL_FAIL(str, unit) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    EXPECT_NE(OB_SUCCESS, ObTimeConverter::str_to_interval(buf, unit, value)); \
  } while (0)

TEST(ObTimeConvertTest, str_to_interval)
{
  ObString buf;
  int64_t value;
  // normal case.
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_MICROSECOND, interval_value(0, 0, 0, 0, 1));
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_SECOND,      interval_value(0, 0, 0, 1, 0));
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_MINUTE,      interval_value(0, 0, 1, 0, 0));
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_HOUR,        interval_value(0, 1, 0, 0, 0));
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_DAY,         interval_value(1, 0, 0, 0, 0));
  STR_TO_INTERVAL_SUCC("1", DATE_UNIT_WEEK,        interval_value(7, 0, 0, 0, 0));
  STR_TO_INTERVAL_FAIL("1", DATE_UNIT_MONTH);
  STR_TO_INTERVAL_FAIL("1", DATE_UNIT_QUARTER);
  STR_TO_INTERVAL_FAIL("1", DATE_UNIT_YEAR);
  STR_TO_INTERVAL_SUCC("1:2", DATE_UNIT_SECOND_MICROSECOND, interval_value(0, 0, 0, 1, 200000));
  STR_TO_INTERVAL_SUCC("1-2+3", DATE_UNIT_MINUTE_MICROSECOND, interval_value(0, 0, 1, 2, 300000));
  STR_TO_INTERVAL_SUCC("1_2", DATE_UNIT_MINUTE_SECOND, interval_value(0, 0, 1, 2, 0));
  STR_TO_INTERVAL_SUCC("1=2(3)4", DATE_UNIT_HOUR_MICROSECOND, interval_value(0, 1, 2, 3, 400000));
  STR_TO_INTERVAL_SUCC("1&2*3", DATE_UNIT_HOUR_SECOND, interval_value(0, 1, 2, 3, 0));
  STR_TO_INTERVAL_SUCC("1^2", DATE_UNIT_HOUR_MINUTE, interval_value(0, 1, 2, 0, 0));
  STR_TO_INTERVAL_SUCC("1@2#3$4%5", DATE_UNIT_DAY_MICROSECOND, interval_value(1, 2, 3, 4, 500000));
  STR_TO_INTERVAL_SUCC("1|2~3!4", DATE_UNIT_DAY_SECOND, interval_value(1, 2, 3, 4, 0));
  STR_TO_INTERVAL_SUCC("1`2;3", DATE_UNIT_DAY_MINUTE, interval_value(1, 2, 3, 0, 0));
  STR_TO_INTERVAL_SUCC("1?2", DATE_UNIT_DAY_HOUR, interval_value(1, 2, 0, 0, 0));
  STR_TO_INTERVAL_FAIL("1/2", DATE_UNIT_YEAR_MONTH);
  // mutli-delimiters or letters or spaces.
  STR_TO_INTERVAL_SUCC("1he ,.2<llo>3{} wo4[rld]5", DATE_UNIT_DAY_MICROSECOND, interval_value(1, 2, 3, 4, 500000));
}

TEST(ObTimeConvertTest, str_to_time)
{
  char buf[50] = {0};
  ObString str;
  int64_t value;

  strcpy(buf, "12:5:9.01234");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_time(str, value));
  EXPECT_EQ(value, 43509 * static_cast<int64_t>(USECS_PER_SEC) + 12340);
  strcpy(buf, "123:0:59");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_time(str, value));
  EXPECT_EQ(value, 442859 * static_cast<int64_t>(USECS_PER_SEC));
  strcpy(buf, "-12:5:9.01234");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_time(str, value));
  EXPECT_EQ(value, -43509 * static_cast<int64_t>(USECS_PER_SEC) - 12340);
  strcpy(buf, "-123:0:59");
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_time(str, value));
  EXPECT_EQ(value, -442859 * static_cast<int64_t>(USECS_PER_SEC));
}

TEST(ObTimeConvertTest, datetime_to_str)
{
//  ObTimezoneUtils zone;
  ObTimeZoneInfo tz_info;
  char buf[50] = {0};
  ObString str;
  int64_t value;
  str.assign_buffer(buf, 50);
  int64_t pos = 0;

//  zone.init("/usr/share/zoneinfo/America/Chicago");
//  value = 23190 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "1970-01-01 00:26:30"));
//  value = 1429089727 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-04-15 04:22:07"));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/Cordoba");
//  value = 1429090420 * USECS_PER_SEC + 12300;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-04-15 06:33:40.012300"));
//  value = 39023190 * USECS_PER_SEC + 123000;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "1971-03-28 12:46:30.123000"));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Asia/Tehran");
//  value = 1429090541 * USECS_PER_SEC + 45600;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-04-15 14:05:41.045600"));
//  value = 218239892 * USECS_PER_SEC + 456000;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "1976-12-01 01:41:32.456000"));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Singapore");
//  value = 1429090646 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-04-15 17:37:26"));
//  value = 1120023938 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2005-06-29 13:45:38"));
//
//  // dst test
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/New_York");
//  value = 1429531489 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-04-20 08:04:49"));
//  value = 1421341200 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "2015-01-15 12:00:00"));
//
//  // usec < 0
//  zone.parse_timezone_file("/usr/share/zoneinfo/UTC");
//  value = -1 * USECS_PER_SEC;
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, zone.get_tz_ptr(), str));
//  EXPECT_TRUE(0 == strcmp(str.ptr(), "1969-12-31 23:59:59"));

  // timezone with offset only.
  strcpy(buf, "+8:00");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  value = -30610252800 * USECS_PER_SEC;
  const ObDataTypeCastParams dtc_params(&tz_info);
  const ObString nls_format;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, &tz_info, nls_format, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1000-01-01 00:00:00"));
  pos = 0;
  strcpy(buf, "-8:00");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  value = 1435950720 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, &tz_info, nls_format, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-07-03 11:12:00"));
  pos = 0;

  const ObDataTypeCastParams dtc_params2(NULL);
  // NULL timezone
  value = -30610224000 * USECS_PER_SEC;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, NULL, nls_format, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1000-01-01 00:00:00"));
  pos = 0;
  value = 253402300799 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_str(value, NULL, nls_format, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "9999-12-31 23:59:59"));
  pos = 0;
}



TEST(ObTimeConvertTest, otimestamp_to_str)
{
//  ObTimezoneUtils zone;
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  char buf[50] = {0};
  ObString str;
  ObOTimestampData ot_data;
  str.assign_buffer(buf, 50);
  int64_t pos = 0;

  const int64_t encode_buf_len = 256;
  char encode_buf[256] = {};
  ObOTimestampData otimestamp_out;
  int8_t scale = 9;

  // timezone with offset only.
  strcpy(buf, "+8:00");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);

  ot_data.reset();
  ot_data.time_us_ = -30610252800 * USECS_PER_SEC;
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  const ObDataTypeCastParams dtc_params(&tz_info);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampLTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1000-01-01 00:00:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampLTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampLTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampNanoType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:00:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampNanoType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampNanoType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:00:00. +00:00 "));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(540);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1000-01-01 01:00:00. +09:00 "));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);
  pos = 0;
  memset(buf, 0, sizeof(buf));

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  strcpy(buf, "-8:00");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  ot_data.reset();
  ot_data.time_us_ = 1435950720 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampLTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-07-03 11:12:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampLTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampLTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampNanoType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-07-03 19:12:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampNanoType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampNanoType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-07-03 19:12:00. +00:00 "));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(-420);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-07-03 12:12:00. -07:00 "));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);
  pos = 0;
  memset(buf, 0, sizeof(buf));

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  strcpy(buf, "+00:00");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  ot_data.reset();
  ot_data.time_ctx_.set_offset_min(0);
  ot_data.time_us_ = 253402300799 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampLTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "9999-12-31 23:59:59."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampLTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampLTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "9999-12-31 23:59:59. +00:00 "));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampNanoType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "9999-12-31 23:59:59."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampNanoType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampNanoType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  pos = 0;
  memset(buf, 0, sizeof(buf));
  strcpy(buf, "-00:10");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  ot_data.reset();
  ot_data.time_us_ = -30610252800 * USECS_PER_SEC;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampLTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 15:50:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampLTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampLTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);

  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(-10);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 15:50:00. -00:10 "));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);


  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(0);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampNanoType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:00:00."));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampNanoType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampNanoType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);


  pos = 0;
  memset(buf, 0, sizeof(buf));
  strcpy(buf, "+00:10");
  str.set_length(static_cast<int32_t>(strlen(buf)));
  tz_info.set_timezone(str);
  ot_data.reset();
  ot_data.time_us_ = -30610252800 * USECS_PER_SEC;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampLTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:10:00."));
  pos = 0;
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampLTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampLTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);


  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(10);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampTZType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:10:00. +00:10 "));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampTZType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampTZType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);


  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  pos = 0;
  memset(buf, 0, sizeof(buf));
  ot_data.time_ctx_.set_offset_min(0);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, 0, ObTimestampNanoType, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "0999-12-31 16:00:00."));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  otimestamp_out.reset();
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::encode_otimestamp(ObTimestampNanoType, encode_buf, encode_buf_len, pos, &tz_info, ot_data, scale));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::decode_otimestamp(ObTimestampNanoType, encode_buf, pos, cvrt_ctx, otimestamp_out, scale));
  EXPECT_TRUE(ot_data.time_us_ == otimestamp_out.time_us_);
  EXPECT_TRUE(ot_data.time_ctx_.desc_ == otimestamp_out.time_ctx_.desc_);
}

TEST(ObTimeConvertTest, date_to_str)
{
  char buf[50] = {0};
  ObString str;
  int32_t value;
  str.assign_buffer(buf, 50);
  int64_t pos = 0;

  value = 0;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_str(value, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1970-01-01"));
  pos = 0;
  value = 16540;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_str(value, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "2015-04-15"));
  pos = 0;
  value = -1;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_str(value, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(str.ptr(), "1969-12-31"));
  pos = 0;
  value = -354285;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_str(value, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "1000-01-01"));
  pos = 0;
  value = 2932896;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_str(value, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "9999-12-31"));
  pos = 0;
}

TEST(ObTimeConvertTest, time_to_str)
{
  char buf[50] = {0};
  ObString str;
  int64_t value;
  str.assign_buffer(buf, 50);
  int64_t pos = 0;

  value = 43509 * static_cast<int64_t>(USECS_PER_SEC) + 12340;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_str(value, 6, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "12:05:09.012340"));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  value = 442859 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_str(value, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "123:00:59"));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  value = -43509 * static_cast<int64_t>(USECS_PER_SEC) - 12340;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_str(value, 6, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "-12:05:09.012340"));
  pos = 0;
  memset(buf, 0, sizeof(buf));
  value = -442859 * static_cast<int64_t>(USECS_PER_SEC);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_str(value, 0, buf, sizeof(buf), pos));
  EXPECT_TRUE(0 == strcmp(buf, "-123:00:59"));
  pos = 0;
}

void int_to_week_succ(int64_t int64, int64_t mode, int32_t res_week, int64_t line)
{
  int ret = OB_SUCCESS;
  int32_t value = 0;
  if (OB_FAIL(ObTimeConverter::int_to_week(int64, mode, value))) {
    EXPECT_EQ(0, line);
  } else if (res_week != value) {
    EXPECT_EQ(0, line);
  }
}

TEST(ObTimeConvertTest, int_to_week)
{
  int_to_week_succ(10000101, 0, 0, __LINE__);
  int_to_week_succ(10000101, 1, 1, __LINE__);
  int_to_week_succ(10000101, 2, 52, __LINE__);
  int_to_week_succ(10000101, 3, 1, __LINE__);
  int_to_week_succ(10000101, 4, 1, __LINE__);
  int_to_week_succ(10000101, 5, 0, __LINE__);
  int_to_week_succ(10000101, 6, 1, __LINE__);
  int_to_week_succ(10000101, 7, 52, __LINE__);
  int_to_week_succ(10000103, 0, 0, __LINE__);
  int_to_week_succ(10000103, 1, 1, __LINE__);
  int_to_week_succ(10000103, 2, 52, __LINE__);
  int_to_week_succ(10000103, 3, 1, __LINE__);
  int_to_week_succ(10000103, 4, 1, __LINE__);
  int_to_week_succ(10000103, 5, 0, __LINE__);
  int_to_week_succ(10000103, 6, 1, __LINE__);
  int_to_week_succ(10000103, 7, 52, __LINE__);
  int_to_week_succ(10000105, 0, 1, __LINE__);
  int_to_week_succ(10000105, 1, 1, __LINE__);
  int_to_week_succ(10000105, 2, 1, __LINE__);
  int_to_week_succ(10000105, 3, 1, __LINE__);
  int_to_week_succ(10000105, 4, 2, __LINE__);
  int_to_week_succ(10000105, 5, 0, __LINE__);
  int_to_week_succ(10000105, 6, 2, __LINE__);
  int_to_week_succ(10000105, 7, 52, __LINE__);
  int_to_week_succ(10000107, 0, 1, __LINE__);
  int_to_week_succ(10000107, 1, 2, __LINE__);
  int_to_week_succ(10000107, 2, 1, __LINE__);
  int_to_week_succ(10000107, 3, 2, __LINE__);
  int_to_week_succ(10000107, 4, 2, __LINE__);
  int_to_week_succ(10000107, 5, 1, __LINE__);
  int_to_week_succ(10000107, 6, 2, __LINE__);
  int_to_week_succ(10000107, 7, 1, __LINE__);
  int_to_week_succ(10001231, 0, 52, __LINE__);
  int_to_week_succ(10001231, 1, 53, __LINE__);
  int_to_week_succ(10001231, 2, 52, __LINE__);
  int_to_week_succ(10001231, 3, 1, __LINE__);
  int_to_week_succ(10001231, 4, 53, __LINE__);
  int_to_week_succ(10001231, 5, 52, __LINE__);
  int_to_week_succ(10001231, 6, 53, __LINE__);
  int_to_week_succ(10001231, 7, 52, __LINE__);
  int_to_week_succ(15000101, 0, 0, __LINE__);
  int_to_week_succ(15000101, 1, 1, __LINE__);
  int_to_week_succ(15000101, 2, 53, __LINE__);
  int_to_week_succ(15000101, 3, 1, __LINE__);
  int_to_week_succ(15000101, 4, 1, __LINE__);
  int_to_week_succ(15000101, 5, 1, __LINE__);
  int_to_week_succ(15000101, 6, 1, __LINE__);
  int_to_week_succ(15000101, 7, 1, __LINE__);
  int_to_week_succ(15000103, 0, 0, __LINE__);
  int_to_week_succ(15000103, 1, 1, __LINE__);
  int_to_week_succ(15000103, 2, 53, __LINE__);
  int_to_week_succ(15000103, 3, 1, __LINE__);
  int_to_week_succ(15000103, 4, 1, __LINE__);
  int_to_week_succ(15000103, 5, 1, __LINE__);
  int_to_week_succ(15000103, 6, 1, __LINE__);
  int_to_week_succ(15000103, 7, 1, __LINE__);
  int_to_week_succ(15000105, 0, 0, __LINE__);
  int_to_week_succ(15000105, 1, 1, __LINE__);
  int_to_week_succ(15000105, 2, 53, __LINE__);
  int_to_week_succ(15000105, 3, 1, __LINE__);
  int_to_week_succ(15000105, 4, 1, __LINE__);
  int_to_week_succ(15000105, 5, 1, __LINE__);
  int_to_week_succ(15000105, 6, 1, __LINE__);
  int_to_week_succ(15000105, 7, 1, __LINE__);
  int_to_week_succ(15000107, 0, 1, __LINE__);
  int_to_week_succ(15000107, 1, 1, __LINE__);
  int_to_week_succ(15000107, 2, 1, __LINE__);
  int_to_week_succ(15000107, 3, 1, __LINE__);
  int_to_week_succ(15000107, 4, 2, __LINE__);
  int_to_week_succ(15000107, 5, 1, __LINE__);
  int_to_week_succ(15000107, 6, 2, __LINE__);
  int_to_week_succ(15000107, 7, 1, __LINE__);
  int_to_week_succ(15001231, 0, 52, __LINE__);
  int_to_week_succ(15001231, 1, 53, __LINE__);
  int_to_week_succ(15001231, 2, 52, __LINE__);
  int_to_week_succ(15001231, 3, 1, __LINE__);
  int_to_week_succ(15001231, 4, 53, __LINE__);
  int_to_week_succ(15001231, 5, 53, __LINE__);
  int_to_week_succ(15001231, 6, 1, __LINE__);
  int_to_week_succ(15001231, 7, 53, __LINE__);
  int_to_week_succ(20000101, 0, 0, __LINE__);
  int_to_week_succ(20000101, 1, 0, __LINE__);
  int_to_week_succ(20000101, 2, 52, __LINE__);
  int_to_week_succ(20000101, 3, 52, __LINE__);
  int_to_week_succ(20000101, 4, 0, __LINE__);
  int_to_week_succ(20000101, 5, 0, __LINE__);
  int_to_week_succ(20000101, 6, 52, __LINE__);
  int_to_week_succ(20000101, 7, 52, __LINE__);
  int_to_week_succ(20000103, 0, 1, __LINE__);
  int_to_week_succ(20000103, 1, 1, __LINE__);
  int_to_week_succ(20000103, 2, 1, __LINE__);
  int_to_week_succ(20000103, 3, 1, __LINE__);
  int_to_week_succ(20000103, 4, 1, __LINE__);
  int_to_week_succ(20000103, 5, 1, __LINE__);
  int_to_week_succ(20000103, 6, 1, __LINE__);
  int_to_week_succ(20000103, 7, 1, __LINE__);
  int_to_week_succ(20000105, 0, 1, __LINE__);
  int_to_week_succ(20000105, 1, 1, __LINE__);
  int_to_week_succ(20000105, 2, 1, __LINE__);
  int_to_week_succ(20000105, 3, 1, __LINE__);
  int_to_week_succ(20000105, 4, 1, __LINE__);
  int_to_week_succ(20000105, 5, 1, __LINE__);
  int_to_week_succ(20000105, 6, 1, __LINE__);
  int_to_week_succ(20000105, 7, 1, __LINE__);
  int_to_week_succ(20000107, 0, 1, __LINE__);
  int_to_week_succ(20000107, 1, 1, __LINE__);
  int_to_week_succ(20000107, 2, 1, __LINE__);
  int_to_week_succ(20000107, 3, 1, __LINE__);
  int_to_week_succ(20000107, 4, 1, __LINE__);
  int_to_week_succ(20000107, 5, 1, __LINE__);
  int_to_week_succ(20000107, 6, 1, __LINE__);
  int_to_week_succ(20000107, 7, 1, __LINE__);
  int_to_week_succ(20001231, 0, 53, __LINE__);
  int_to_week_succ(20001231, 1, 52, __LINE__);
  int_to_week_succ(20001231, 2, 53, __LINE__);
  int_to_week_succ(20001231, 3, 52, __LINE__);
  int_to_week_succ(20001231, 4, 53, __LINE__);
  int_to_week_succ(20001231, 5, 52, __LINE__);
  int_to_week_succ(20001231, 6, 1, __LINE__);
  int_to_week_succ(20001231, 7, 52, __LINE__);
}

#define DATE_ADJUST_SUCC(is_add, dt_str, it_str, unit, res_str, res_dt) \
  do { \
    base_buf.assign_ptr(dt_str, static_cast<int32_t>(strlen(dt_str))); \
    interval_buf.assign_ptr(it_str, static_cast<int32_t>(strlen(it_str))); \
    EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_adjust(base_buf, interval_buf, unit, value, is_add)); \
    EXPECT_EQ(res_dt, value); \
  } while (0)

/*
#define DATE_ADJUST_SUCC(is_add, dt_str, it_str, unit, res_str, res_dt) \
    date_adjust_succ(is_add, dt_str, it_str, unit, res_str, res_dt, __LINE__)

void date_adjust_succ(bool is_add, const char *dt_str, const char *it_str, ObDateUnitType unit,
                      const char *res_str, int64_t res_dt, int line_no)
{
  int ret = OB_SUCCESS;
  ObString base_buf;
  ObString interval_buf;
  int64_t value;
  base_buf.assign_ptr(dt_str, static_cast<int32_t>(strlen(dt_str)));
  interval_buf.assign_ptr(it_str, static_cast<int32_t>(strlen(it_str)));
  if (OB_FAIL(ObTimeConverter::date_adjust(is_add, base_buf, interval_buf, unit, value)) || res_dt != value) {
    printf("line: %d, ret: %d, res_str: %s, res_dt: %ld, value: %ld\n", line_no, ret, res_str, res_dt, value);
    EXPECT_TRUE(false);
  }
}
*/

#define DATE_ADJUST_FAIL(is_add, dt_str, it_str, unit) \
  do { \
    base_buf.assign_ptr(dt_str, static_cast<int32_t>(strlen(dt_str))); \
    interval_buf.assign_ptr(it_str, static_cast<int32_t>(strlen(it_str))); \
    EXPECT_NE(OB_SUCCESS, ObTimeConverter::date_adjust(base_buf, interval_buf, unit, value, is_add)); \
  } while (0)

TEST(ObTimeConvertTest, date_adjust)
{
  ObString base_buf;
  ObString interval_buf;
  int64_t value;
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999999", -2177539200000001);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_SECOND, "1900-12-30 23:59:59.000000", -2177539201000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_MINUTE, "1900-12-30 23:59:00.000000", -2177539260000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_HOUR, "1900-12-30 23:00:00.000000", -2177542800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_DAY, "1900-12-30 00:00:00.000000", -2177625600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_WEEK, "1900-12-24 00:00:00.000000", -2178144000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_MONTH, "1900-11-30 00:00:00.000000", -2180217600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_QUARTER, "1900-09-30 00:00:00.000000", -2185488000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_YEAR, "1899-12-31 00:00:00.000000", -2209075200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_SECOND_MICROSECOND, "1900-12-30 23:59:59.900000", -2177539200100000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:59:59.900000", -2177539200100000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_MINUTE_SECOND, "1900-12-30 23:59:59.000000", -2177539201000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:59:59.900000", -2177539200100000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_HOUR_SECOND, "1900-12-30 23:59:59.000000", -2177539201000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_HOUR_MINUTE, "1900-12-30 23:59:00.000000", -2177539260000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:59:59.900000", -2177539200100000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_DAY_SECOND, "1900-12-30 23:59:59.000000", -2177539201000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_DAY_MINUTE, "1900-12-30 23:59:00.000000", -2177539260000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_DAY_HOUR, "1900-12-30 23:00:00.000000", -2177542800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-1", DATE_UNIT_YEAR_MONTH, "1900-11-30 00:00:00.000000", -2180217600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000200", -2177539199999800);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_SECOND, "1900-12-31 00:03:20.000000", -2177539000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_MINUTE, "1900-12-31 03:20:00.000000", -2177527200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_HOUR, "1901-01-08 08:00:00.000000", -2176819200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_DAY, "1901-07-19 00:00:00.000000", -2160259200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_WEEK, "1904-10-31 00:00:00.000000", -2056579200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_MONTH, "1917-08-31 00:00:00.000000", -1651622400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_QUARTER, "1950-12-31 00:00:00.000000", -599702400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_YEAR, "2100-12-31 00:00:00.000000", 4133894400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_SECOND_MICROSECOND, "1900-12-31 00:00:00.200000", -2177539199800000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:00:00.200000", -2177539199800000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_MINUTE_SECOND, "1900-12-31 00:03:20.000000", -2177539000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:00:00.200000", -2177539199800000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_HOUR_SECOND, "1900-12-31 00:03:20.000000", -2177539000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_HOUR_MINUTE, "1900-12-31 03:20:00.000000", -2177527200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:00:00.200000", -2177539199800000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_DAY_SECOND, "1900-12-31 00:03:20.000000", -2177539000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_DAY_MINUTE, "1900-12-31 03:20:00.000000", -2177527200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_DAY_HOUR, "1901-01-08 08:00:00.000000", -2176819200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "200", DATE_UNIT_YEAR_MONTH, "1917-08-31 00:00:00.000000", -1651622400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999997", -2177539200000003);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_SECOND, "1900-12-30 23:59:57.000000", -2177539203000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_MINUTE, "1900-12-30 23:57:00.000000", -2177539380000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_HOUR, "1900-12-30 21:00:00.000000", -2177550000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_DAY, "1900-12-28 00:00:00.000000", -2177798400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_WEEK, "1900-12-10 00:00:00.000000", -2179353600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_MONTH, "1900-09-30 00:00:00.000000", -2185488000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_QUARTER, "1900-03-31 00:00:00.000000", -2201299200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_YEAR, "1897-12-31 00:00:00.000000", -2272147200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "1900-12-30 23:59:56.700000", -2177539203300000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:59:56.700000", -2177539203300000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_MINUTE_SECOND, "1900-12-30 23:56:30.000000", -2177539410000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:59:56.700000", -2177539203300000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_HOUR_SECOND, "1900-12-30 23:56:30.000000", -2177539410000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_HOUR_MINUTE, "1900-12-30 20:30:00.000000", -2177551800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:59:56.700000", -2177539203300000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_DAY_SECOND, "1900-12-30 23:56:30.000000", -2177539410000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_DAY_MINUTE, "1900-12-30 20:30:00.000000", -2177551800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_DAY_HOUR, "1900-12-26 18:00:00.000000", -2177906400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-3:30", DATE_UNIT_YEAR_MONTH, "1895-06-30 00:00:00.000000", -2351203200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000030", -2177539199999970);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_SECOND, "1900-12-31 00:00:30.000000", -2177539170000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_MINUTE, "1900-12-31 00:30:00.000000", -2177537400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_HOUR, "1901-01-01 06:00:00.000000", -2177431200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_DAY, "1901-01-30 00:00:00.000000", -2174947200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_WEEK, "1901-07-29 00:00:00.000000", -2159395200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_MONTH, "1903-06-30 00:00:00.000000", -2098828800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_QUARTER, "1908-06-30 00:00:00.000000", -1940976000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_YEAR, "1930-12-31 00:00:00.000000", -1230854400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_SECOND_MICROSECOND, "1900-12-31 00:00:30.300000", -2177539169700000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:00:30.300000", -2177539169700000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_MINUTE_SECOND, "1900-12-31 00:30:03.000000", -2177537397000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:00:30.300000", -2177539169700000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_HOUR_SECOND, "1900-12-31 00:30:03.000000", -2177537397000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_HOUR_MINUTE, "1901-01-01 06:03:00.000000", -2177431020000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:00:30.300000", -2177539169700000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_DAY_SECOND, "1900-12-31 00:30:03.000000", -2177537397000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_DAY_MINUTE, "1901-01-01 06:03:00.000000", -2177431020000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_DAY_HOUR, "1901-01-30 03:00:00.000000", -2174936400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "30!3", DATE_UNIT_YEAR_MONTH, "1931-03-31 00:00:00.000000", -1223078400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999996", -2177539200000004);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_SECOND, "1900-12-30 23:59:55.600000", -2177539204400000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE, "1900-12-30 23:56:00.000000", -2177539440000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR, "1900-12-30 20:00:00.000000", -2177553600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_DAY, "1900-12-27 00:00:00.000000", -2177884800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_WEEK, "1900-12-03 00:00:00.000000", -2179958400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_MONTH, "1900-08-31 00:00:00.000000", -2188080000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_QUARTER, "1899-12-31 00:00:00.000000", -2209075200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_YEAR, "1896-12-31 00:00:00.000000", -2303683200000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:55:19.600000", -2177539480400000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:55:19.600000", -2177539480400000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_SECOND, "1900-12-30 19:13:20.000000", -2177556400000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:55:19.600000", -2177539480400000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_SECOND, "1900-12-30 19:13:20.000000", -2177556400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_MINUTE, "1900-12-25 01:20:00.000000", -2178052800000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000400", -2177539199999600);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_SECOND, "1900-12-31 00:06:40.000000", -2177538800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE, "1900-12-31 06:40:00.000000", -2177515200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_HOUR, "1901-01-16 16:00:00.000000", -2176099200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_DAY, "1902-02-04 00:00:00.000000", -2142979200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_WEEK, "1908-08-31 00:00:00.000000", -1935619200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_MONTH, "1934-04-30 00:00:00.000000", -1125792000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_QUARTER, "2000-12-31 00:00:00.000000", 978220800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_YEAR, "2300-12-31 00:00:00.000000", 10445241600000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 06:40:40.400000", -2177515159600000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 06:40:40.400000", -2177515159600000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_SECOND, "1901-01-16 16:40:04.000000", -2176096796000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 06:40:40.400000", -2177515159600000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_DAY_SECOND, "1901-01-16 16:40:04.000000", -2176096796000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "400@40#4", DATE_UNIT_DAY_MINUTE, "1902-02-05 16:04:00.000000", -2142834960000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999995", -2177539200000005);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_SECOND, "1900-12-30 23:59:55.000000", -2177539205000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE, "1900-12-30 23:55:00.000000", -2177539500000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR, "1900-12-30 19:00:00.000000", -2177557200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY, "1900-12-26 00:00:00.000000", -2177971200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_WEEK, "1900-11-26 00:00:00.000000", -2180563200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MONTH, "1900-07-31 00:00:00.000000", -2190758400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_QUARTER, "1899-09-30 00:00:00.000000", -2217024000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_YEAR, "1895-12-31 00:00:00.000000", -2335305600000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 18:01:39.500000", -2177560700500000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 18:01:39.500000", -2177560700500000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "1900-12-23 12:16:40.000000", -2178186200000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.005000", -2177539199995000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_SECOND, "1900-12-31 01:23:20.000000", -2177534200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE, "1901-01-03 11:20:00.000000", -2177239200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR, "1901-07-27 08:00:00.000000", -2159539200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY, "1914-09-09 00:00:00.000000", -1745539200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_WEEK, "1996-10-28 00:00:00.000000", 846460800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_MONTH, "2317-08-31 00:00:00.000000", 10971158400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_QUARTER, "3150-12-31 00:00:00.000000", 37268640000000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_YEAR, "6900-12-31 00:00:00.000000", 155607264000000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "1901-07-27 16:20:50.500000", -2159509149500000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "1901-07-27 16:20:50.500000", -2159509149500000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "1914-09-29 20:50:05.000000", -1743736195000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000006", -2177539199999994);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_SECOND, "1900-12-31 00:00:06.000000", -2177539194000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "1900-12-31 00:06:00.000000", -2177538840000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR, "1900-12-31 06:00:00.000000", -2177517600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY, "1901-01-06 00:00:00.000000", -2177020800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_WEEK, "1901-02-11 00:00:00.000000", -2173910400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MONTH, "1901-06-30 00:00:00.000000", -2161900800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "1902-06-30 00:00:00.000000", -2130364800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_YEAR, "1906-12-31 00:00:00.000000", -1988236800000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "1901-01-08 23:40:00.600000", -2176762799400000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000001", -2177539199999999);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_SECOND, "1900-12-31 00:00:01.200000", -2177539198800000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE, "1900-12-31 00:01:00.000000", -2177539140000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR, "1900-12-31 01:00:00.000000", -2177535600000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY, "1901-01-01 00:00:00.000000", -2177452800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_WEEK, "1901-01-07 00:00:00.000000", -2176934400000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_MONTH, "1901-01-31 00:00:00.000000", -2174860800000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_QUARTER, "1901-03-31 00:00:00.000000", -2169763200000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_YEAR, "1901-12-31 00:00:00.000000", -2146003200000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:01:05.456789", -2177539134543211);
  DATE_ADJUST_FAIL(true, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:01:05.456789", -2177539134543211);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "1901-02-09 01:15:09.000000", -2174078691000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:01:05.456789", -2177539134543211);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_SECOND, "1901-02-09 01:15:09.000000", -2174078691000000);
  DATE_ADJUST_SUCC(true, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "1907-07-29 15:09:00.000000", -1970038260000000);
  DATE_ADJUST_FAIL(true, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1900-12-31", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999999", -1);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_SECOND, "1969-12-31 23:59:59.000000", -1000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_MINUTE, "1969-12-31 23:59:00.000000", -60000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_HOUR, "1969-12-31 23:00:00.000000", -3600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_DAY, "1969-12-31 00:00:00.000000", -86400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_WEEK, "1969-12-25 00:00:00.000000", -604800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_MONTH, "1969-12-01 00:00:00.000000", -2678400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_QUARTER, "1969-10-01 00:00:00.000000", -7948800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_YEAR, "1969-01-01 00:00:00.000000", -31536000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_SECOND_MICROSECOND, "1969-12-31 23:59:59.900000", -100000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:59:59.900000", -100000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_MINUTE_SECOND, "1969-12-31 23:59:59.000000", -1000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:59:59.900000", -100000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_HOUR_SECOND, "1969-12-31 23:59:59.000000", -1000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_HOUR_MINUTE, "1969-12-31 23:59:00.000000", -60000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:59:59.900000", -100000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_DAY_SECOND, "1969-12-31 23:59:59.000000", -1000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_DAY_MINUTE, "1969-12-31 23:59:00.000000", -60000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_DAY_HOUR, "1969-12-31 23:00:00.000000", -3600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-1", DATE_UNIT_YEAR_MONTH, "1969-12-01 00:00:00.000000", -2678400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000200", 200);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_SECOND, "1970-01-01 00:03:20.000000", 200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_MINUTE, "1970-01-01 03:20:00.000000", 12000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_HOUR, "1970-01-09 08:00:00.000000", 720000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_DAY, "1970-07-20 00:00:00.000000", 17280000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_WEEK, "1973-11-01 00:00:00.000000", 120960000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_MONTH, "1986-09-01 00:00:00.000000", 525916800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_QUARTER, "2020-01-01 00:00:00.000000", 1577836800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_YEAR, "2170-01-01 00:00:00.000000", 6311433600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_SECOND_MICROSECOND, "1970-01-01 00:00:00.200000", 200000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:00:00.200000", 200000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_MINUTE_SECOND, "1970-01-01 00:03:20.000000", 200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:00:00.200000", 200000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_HOUR_SECOND, "1970-01-01 00:03:20.000000", 200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_HOUR_MINUTE, "1970-01-01 03:20:00.000000", 12000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:00:00.200000", 200000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_DAY_SECOND, "1970-01-01 00:03:20.000000", 200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_DAY_MINUTE, "1970-01-01 03:20:00.000000", 12000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_DAY_HOUR, "1970-01-09 08:00:00.000000", 720000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "200", DATE_UNIT_YEAR_MONTH, "1986-09-01 00:00:00.000000", 525916800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999997", -3);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_SECOND, "1969-12-31 23:59:57.000000", -3000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_MINUTE, "1969-12-31 23:57:00.000000", -180000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_HOUR, "1969-12-31 21:00:00.000000", -10800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_DAY, "1969-12-29 00:00:00.000000", -259200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_WEEK, "1969-12-11 00:00:00.000000", -1814400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_MONTH, "1969-10-01 00:00:00.000000", -7948800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_QUARTER, "1969-04-01 00:00:00.000000", -23760000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_YEAR, "1967-01-01 00:00:00.000000", -94694400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "1969-12-31 23:59:56.700000", -3300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:59:56.700000", -3300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_MINUTE_SECOND, "1969-12-31 23:56:30.000000", -210000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:59:56.700000", -3300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_HOUR_SECOND, "1969-12-31 23:56:30.000000", -210000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_HOUR_MINUTE, "1969-12-31 20:30:00.000000", -12600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:59:56.700000", -3300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_DAY_SECOND, "1969-12-31 23:56:30.000000", -210000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_DAY_MINUTE, "1969-12-31 20:30:00.000000", -12600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_DAY_HOUR, "1969-12-27 18:00:00.000000", -367200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-3:30", DATE_UNIT_YEAR_MONTH, "1964-07-01 00:00:00.000000", -173664000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000030", 30);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_SECOND, "1970-01-01 00:00:30.000000", 30000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_MINUTE, "1970-01-01 00:30:00.000000", 1800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_HOUR, "1970-01-02 06:00:00.000000", 108000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_DAY, "1970-01-31 00:00:00.000000", 2592000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_WEEK, "1970-07-30 00:00:00.000000", 18144000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_MONTH, "1972-07-01 00:00:00.000000", 78796800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_QUARTER, "1977-07-01 00:00:00.000000", 236563200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_YEAR, "2000-01-01 00:00:00.000000", 946684800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_SECOND_MICROSECOND, "1970-01-01 00:00:30.300000", 30300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:00:30.300000", 30300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_MINUTE_SECOND, "1970-01-01 00:30:03.000000", 1803000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:00:30.300000", 30300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_HOUR_SECOND, "1970-01-01 00:30:03.000000", 1803000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_HOUR_MINUTE, "1970-01-02 06:03:00.000000", 108180000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:00:30.300000", 30300000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_DAY_SECOND, "1970-01-01 00:30:03.000000", 1803000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_DAY_MINUTE, "1970-01-02 06:03:00.000000", 108180000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_DAY_HOUR, "1970-01-31 03:00:00.000000", 2602800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "30!3", DATE_UNIT_YEAR_MONTH, "2000-04-01 00:00:00.000000", 954547200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999996", -4);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_SECOND, "1969-12-31 23:59:55.600000", -4400000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE, "1969-12-31 23:56:00.000000", -240000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR, "1969-12-31 20:00:00.000000", -14400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_DAY, "1969-12-28 00:00:00.000000", -345600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_WEEK, "1969-12-04 00:00:00.000000", -2419200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_MONTH, "1969-09-01 00:00:00.000000", -10540800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_QUARTER, "1969-01-01 00:00:00.000000", -31536000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_YEAR, "1966-01-01 00:00:00.000000", -126230400000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:55:19.600000", -280400000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:55:19.600000", -280400000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_SECOND, "1969-12-31 19:13:20.000000", -17200000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:55:19.600000", -280400000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_SECOND, "1969-12-31 19:13:20.000000", -17200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_MINUTE, "1969-12-26 01:20:00.000000", -513600000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000400", 400);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_SECOND, "1970-01-01 00:06:40.000000", 400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE, "1970-01-01 06:40:00.000000", 24000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_HOUR, "1970-01-17 16:00:00.000000", 1440000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_DAY, "1971-02-05 00:00:00.000000", 34560000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_WEEK, "1977-09-01 00:00:00.000000", 241920000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_MONTH, "2003-05-01 00:00:00.000000", 1051747200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_QUARTER, "2070-01-01 00:00:00.000000", 3155760000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_YEAR, "2370-01-01 00:00:00.000000", 12622780800000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 06:40:40.400000", 24040400000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 06:40:40.400000", 24040400000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_SECOND, "1970-01-17 16:40:04.000000", 1442404000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 06:40:40.400000", 24040400000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_DAY_SECOND, "1970-01-17 16:40:04.000000", 1442404000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "400@40#4", DATE_UNIT_DAY_MINUTE, "1971-02-06 16:04:00.000000", 34704240000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999995", -5);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_SECOND, "1969-12-31 23:59:55.000000", -5000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE, "1969-12-31 23:55:00.000000", -300000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR, "1969-12-31 19:00:00.000000", -18000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY, "1969-12-27 00:00:00.000000", -432000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_WEEK, "1969-11-27 00:00:00.000000", -3024000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MONTH, "1969-08-01 00:00:00.000000", -13219200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_QUARTER, "1968-10-01 00:00:00.000000", -39484800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_YEAR, "1965-01-01 00:00:00.000000", -157766400000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 18:01:39.500000", -21500500000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 18:01:39.500000", -21500500000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "1969-12-24 12:16:40.000000", -647000000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.005000", 5000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_SECOND, "1970-01-01 01:23:20.000000", 5000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE, "1970-01-04 11:20:00.000000", 300000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR, "1970-07-28 08:00:00.000000", 18000000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY, "1983-09-10 00:00:00.000000", 432000000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_WEEK, "2065-10-29 00:00:00.000000", 3024000000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_MONTH, "2386-09-01 00:00:00.000000", 13148697600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_QUARTER, "3220-01-01 00:00:00.000000", 39446179200000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_YEAR, "6970-01-01 00:00:00.000000", 157784803200000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "1970-07-28 16:20:50.500000", 18030050500000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "1970-07-28 16:20:50.500000", 18030050500000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "1983-09-30 20:50:05.000000", 433803005000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000006", 6);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_SECOND, "1970-01-01 00:00:06.000000", 6000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "1970-01-01 00:06:00.000000", 360000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR, "1970-01-01 06:00:00.000000", 21600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY, "1970-01-07 00:00:00.000000", 518400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_WEEK, "1970-02-12 00:00:00.000000", 3628800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MONTH, "1970-07-01 00:00:00.000000", 15638400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "1971-07-01 00:00:00.000000", 47174400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_YEAR, "1976-01-01 00:00:00.000000", 189302400000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "1970-01-09 23:40:00.600000", 776400600000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000001", 1);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_SECOND, "1970-01-01 00:00:01.200000", 1200000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE, "1970-01-01 00:01:00.000000", 60000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR, "1970-01-01 01:00:00.000000", 3600000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY, "1970-01-02 00:00:00.000000", 86400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_WEEK, "1970-01-08 00:00:00.000000", 604800000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_MONTH, "1970-02-01 00:00:00.000000", 2678400000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_QUARTER, "1970-04-01 00:00:00.000000", 7776000000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_YEAR, "1971-01-01 00:00:00.000000", 31536000000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:01:05.456789", 65456789);
  DATE_ADJUST_FAIL(true, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:01:05.456789", 65456789);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "1970-02-10 01:15:09.000000", 3460509000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:01:05.456789", 65456789);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_SECOND, "1970-02-10 01:15:09.000000", 3460509000000);
  DATE_ADJUST_SUCC(true, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "1976-07-29 15:09:00.000000", 207500940000000);
  DATE_ADJUST_FAIL(true, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "1970-1-1", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999999", 1330473599999999);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_SECOND, "2012-02-28 23:59:59.000000", 1330473599000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_MINUTE, "2012-02-28 23:59:00.000000", 1330473540000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_HOUR, "2012-02-28 23:00:00.000000", 1330470000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_DAY, "2012-02-28 00:00:00.000000", 1330387200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_WEEK, "2012-02-22 00:00:00.000000", 1329868800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_MONTH, "2012-01-29 00:00:00.000000", 1327795200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_QUARTER, "2011-11-29 00:00:00.000000", 1322524800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_YEAR, "2011-02-28 00:00:00.000000", 1298851200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_SECOND_MICROSECOND, "2012-02-28 23:59:59.900000", 1330473599900000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:59:59.900000", 1330473599900000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_MINUTE_SECOND, "2012-02-28 23:59:59.000000", 1330473599000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:59:59.900000", 1330473599900000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_HOUR_SECOND, "2012-02-28 23:59:59.000000", 1330473599000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_HOUR_MINUTE, "2012-02-28 23:59:00.000000", 1330473540000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:59:59.900000", 1330473599900000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_DAY_SECOND, "2012-02-28 23:59:59.000000", 1330473599000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_DAY_MINUTE, "2012-02-28 23:59:00.000000", 1330473540000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_DAY_HOUR, "2012-02-28 23:00:00.000000", 1330470000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-1", DATE_UNIT_YEAR_MONTH, "2012-01-29 00:00:00.000000", 1327795200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000200", 1330473600000200);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_SECOND, "2012-02-29 00:03:20.000000", 1330473800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_MINUTE, "2012-02-29 03:20:00.000000", 1330485600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_HOUR, "2012-03-08 08:00:00.000000", 1331193600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_DAY, "2012-09-16 00:00:00.000000", 1347753600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_WEEK, "2015-12-30 00:00:00.000000", 1451433600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_MONTH, "2028-10-29 00:00:00.000000", 1856390400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_QUARTER, "2062-02-28 00:00:00.000000", 2908310400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_YEAR, "2212-02-29 00:00:00.000000", 7641820800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_SECOND_MICROSECOND, "2012-02-29 00:00:00.200000", 1330473600200000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:00:00.200000", 1330473600200000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_MINUTE_SECOND, "2012-02-29 00:03:20.000000", 1330473800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:00:00.200000", 1330473600200000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_HOUR_SECOND, "2012-02-29 00:03:20.000000", 1330473800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_HOUR_MINUTE, "2012-02-29 03:20:00.000000", 1330485600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:00:00.200000", 1330473600200000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_DAY_SECOND, "2012-02-29 00:03:20.000000", 1330473800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_DAY_MINUTE, "2012-02-29 03:20:00.000000", 1330485600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_DAY_HOUR, "2012-03-08 08:00:00.000000", 1331193600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "200", DATE_UNIT_YEAR_MONTH, "2028-10-29 00:00:00.000000", 1856390400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999997", 1330473599999997);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_SECOND, "2012-02-28 23:59:57.000000", 1330473597000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_MINUTE, "2012-02-28 23:57:00.000000", 1330473420000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_HOUR, "2012-02-28 21:00:00.000000", 1330462800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_DAY, "2012-02-26 00:00:00.000000", 1330214400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_WEEK, "2012-02-08 00:00:00.000000", 1328659200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_MONTH, "2011-11-29 00:00:00.000000", 1322524800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_QUARTER, "2011-05-29 00:00:00.000000", 1306627200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_YEAR, "2009-02-28 00:00:00.000000", 1235779200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "2012-02-28 23:59:56.700000", 1330473596700000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:59:56.700000", 1330473596700000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_MINUTE_SECOND, "2012-02-28 23:56:30.000000", 1330473390000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:59:56.700000", 1330473596700000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_HOUR_SECOND, "2012-02-28 23:56:30.000000", 1330473390000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_HOUR_MINUTE, "2012-02-28 20:30:00.000000", 1330461000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:59:56.700000", 1330473596700000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_DAY_SECOND, "2012-02-28 23:56:30.000000", 1330473390000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_DAY_MINUTE, "2012-02-28 20:30:00.000000", 1330461000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_DAY_HOUR, "2012-02-24 18:00:00.000000", 1330106400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-3:30", DATE_UNIT_YEAR_MONTH, "2006-08-29 00:00:00.000000", 1156809600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000030", 1330473600000030);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_SECOND, "2012-02-29 00:00:30.000000", 1330473630000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_MINUTE, "2012-02-29 00:30:00.000000", 1330475400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_HOUR, "2012-03-01 06:00:00.000000", 1330581600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_DAY, "2012-03-30 00:00:00.000000", 1333065600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_WEEK, "2012-09-26 00:00:00.000000", 1348617600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_MONTH, "2014-08-29 00:00:00.000000", 1409270400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_QUARTER, "2019-08-29 00:00:00.000000", 1567036800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_YEAR, "2042-02-28 00:00:00.000000", 2277158400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_SECOND_MICROSECOND, "2012-02-29 00:00:30.300000", 1330473630300000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:00:30.300000", 1330473630300000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_MINUTE_SECOND, "2012-02-29 00:30:03.000000", 1330475403000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:00:30.300000", 1330473630300000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_HOUR_SECOND, "2012-02-29 00:30:03.000000", 1330475403000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_HOUR_MINUTE, "2012-03-01 06:03:00.000000", 1330581780000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:00:30.300000", 1330473630300000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_DAY_SECOND, "2012-02-29 00:30:03.000000", 1330475403000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_DAY_MINUTE, "2012-03-01 06:03:00.000000", 1330581780000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_DAY_HOUR, "2012-03-30 03:00:00.000000", 1333076400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "30!3", DATE_UNIT_YEAR_MONTH, "2042-05-29 00:00:00.000000", 2284934400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999996", 1330473599999996);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_SECOND, "2012-02-28 23:59:55.600000", 1330473595600000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE, "2012-02-28 23:56:00.000000", 1330473360000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR, "2012-02-28 20:00:00.000000", 1330459200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_DAY, "2012-02-25 00:00:00.000000", 1330128000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_WEEK, "2012-02-01 00:00:00.000000", 1328054400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_MONTH, "2011-10-29 00:00:00.000000", 1319846400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_QUARTER, "2011-02-28 00:00:00.000000", 1298851200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_YEAR, "2008-02-29 00:00:00.000000", 1204243200000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:55:19.600000", 1330473319600000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:55:19.600000", 1330473319600000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_SECOND, "2012-02-28 19:13:20.000000", 1330456400000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:55:19.600000", 1330473319600000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_SECOND, "2012-02-28 19:13:20.000000", 1330456400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_MINUTE, "2012-02-23 01:20:00.000000", 1329960000000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000400", 1330473600000400);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_SECOND, "2012-02-29 00:06:40.000000", 1330474000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE, "2012-02-29 06:40:00.000000", 1330497600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_HOUR, "2012-03-16 16:00:00.000000", 1331913600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_DAY, "2013-04-04 00:00:00.000000", 1365033600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_WEEK, "2019-10-30 00:00:00.000000", 1572393600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_MONTH, "2045-06-29 00:00:00.000000", 2382307200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_QUARTER, "2112-02-29 00:00:00.000000", 4486147200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_YEAR, "2412-02-29 00:00:00.000000", 13953254400000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 06:40:40.400000", 1330497640400000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 06:40:40.400000", 1330497640400000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_SECOND, "2012-03-16 16:40:04.000000", 1331916004000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 06:40:40.400000", 1330497640400000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_DAY_SECOND, "2012-03-16 16:40:04.000000", 1331916004000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "400@40#4", DATE_UNIT_DAY_MINUTE, "2013-04-05 16:04:00.000000", 1365177840000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999995", 1330473599999995);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_SECOND, "2012-02-28 23:59:55.000000", 1330473595000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE, "2012-02-28 23:55:00.000000", 1330473300000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR, "2012-02-28 19:00:00.000000", 1330455600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY, "2012-02-24 00:00:00.000000", 1330041600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_WEEK, "2012-01-25 00:00:00.000000", 1327449600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MONTH, "2011-09-29 00:00:00.000000", 1317254400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_QUARTER, "2010-11-29 00:00:00.000000", 1290988800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_YEAR, "2007-02-28 00:00:00.000000", 1172620800000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 18:01:39.500000", 1330452099500000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 18:01:39.500000", 1330452099500000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "2012-02-21 12:16:40.000000", 1329826600000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.005000", 1330473600005000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_SECOND, "2012-02-29 01:23:20.000000", 1330478600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE, "2012-03-03 11:20:00.000000", 1330773600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR, "2012-09-24 08:00:00.000000", 1348473600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY, "2025-11-07 00:00:00.000000", 1762473600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_WEEK, "2107-12-28 00:00:00.000000", 4354473600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_MONTH, "2428-10-29 00:00:00.000000", 14479171200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_QUARTER, "3262-02-28 00:00:00.000000", 40776652800000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_YEAR, "7012-02-29 00:00:00.000000", 159115190400000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "2012-09-24 16:20:50.500000", 1348503650500000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "2012-09-24 16:20:50.500000", 1348503650500000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "2025-11-27 20:50:05.000000", 1764276605000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000006", 1330473600000006);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_SECOND, "2012-02-29 00:00:06.000000", 1330473606000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "2012-02-29 00:06:00.000000", 1330473960000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR, "2012-02-29 06:00:00.000000", 1330495200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY, "2012-03-06 00:00:00.000000", 1330992000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_WEEK, "2012-04-11 00:00:00.000000", 1334102400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MONTH, "2012-08-29 00:00:00.000000", 1346198400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "2013-08-29 00:00:00.000000", 1377734400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_YEAR, "2018-02-28 00:00:00.000000", 1519776000000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "2012-03-08 23:40:00.600000", 1331250000600000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000001", 1330473600000001);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_SECOND, "2012-02-29 00:00:01.200000", 1330473601200000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE, "2012-02-29 00:01:00.000000", 1330473660000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR, "2012-02-29 01:00:00.000000", 1330477200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY, "2012-03-01 00:00:00.000000", 1330560000000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_WEEK, "2012-03-07 00:00:00.000000", 1331078400000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_MONTH, "2012-03-29 00:00:00.000000", 1332979200000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_QUARTER, "2012-05-29 00:00:00.000000", 1338249600000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_YEAR, "2013-02-28 00:00:00.000000", 1362009600000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:01:05.456789", 1330473665456789);
  DATE_ADJUST_FAIL(true, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:01:05.456789", 1330473665456789);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "2012-04-09 01:15:09.000000", 1333934109000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:01:05.456789", 1330473665456789);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_SECOND, "2012-04-09 01:15:09.000000", 1333934109000000);
  DATE_ADJUST_SUCC(true, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "2018-09-26 15:09:00.000000", 1537974540000000);
  DATE_ADJUST_FAIL(true, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(true, "2012-2-29", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000001", -2177539199999999);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_SECOND, "1900-12-31 00:00:01.000000", -2177539199000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_MINUTE, "1900-12-31 00:01:00.000000", -2177539140000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_HOUR, "1900-12-31 01:00:00.000000", -2177535600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_DAY, "1901-01-01 00:00:00.000000", -2177452800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_WEEK, "1901-01-07 00:00:00.000000", -2176934400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_MONTH, "1901-01-31 00:00:00.000000", -2174860800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_QUARTER, "1901-03-31 00:00:00.000000", -2169763200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_YEAR, "1901-12-31 00:00:00.000000", -2146003200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_SECOND_MICROSECOND, "1900-12-31 00:00:00.100000", -2177539199900000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:00:00.100000", -2177539199900000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_MINUTE_SECOND, "1900-12-31 00:00:01.000000", -2177539199000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:00:00.100000", -2177539199900000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_HOUR_SECOND, "1900-12-31 00:00:01.000000", -2177539199000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_HOUR_MINUTE, "1900-12-31 00:01:00.000000", -2177539140000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:00:00.100000", -2177539199900000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_DAY_SECOND, "1900-12-31 00:00:01.000000", -2177539199000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_DAY_MINUTE, "1900-12-31 00:01:00.000000", -2177539140000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_DAY_HOUR, "1900-12-31 01:00:00.000000", -2177535600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-1", DATE_UNIT_YEAR_MONTH, "1901-01-31 00:00:00.000000", -2174860800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999800", -2177539200000200);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_SECOND, "1900-12-30 23:56:40.000000", -2177539400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_MINUTE, "1900-12-30 20:40:00.000000", -2177551200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_HOUR, "1900-12-22 16:00:00.000000", -2178259200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_DAY, "1900-06-14 00:00:00.000000", -2194819200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_WEEK, "1897-03-01 00:00:00.000000", -2298499200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_MONTH, "1884-04-30 00:00:00.000000", -2703542400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_QUARTER, "1850-12-31 00:00:00.000000", -3755376000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_YEAR, "1700-12-31 00:00:00.000000", -8488886400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_SECOND_MICROSECOND, "1900-12-30 23:59:59.800000", -2177539200200000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:59:59.800000", -2177539200200000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_MINUTE_SECOND, "1900-12-30 23:56:40.000000", -2177539400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:59:59.800000", -2177539200200000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_HOUR_SECOND, "1900-12-30 23:56:40.000000", -2177539400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_HOUR_MINUTE, "1900-12-30 20:40:00.000000", -2177551200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:59:59.800000", -2177539200200000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_DAY_SECOND, "1900-12-30 23:56:40.000000", -2177539400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_DAY_MINUTE, "1900-12-30 20:40:00.000000", -2177551200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_DAY_HOUR, "1900-12-22 16:00:00.000000", -2178259200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "200", DATE_UNIT_YEAR_MONTH, "1884-04-30 00:00:00.000000", -2703542400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000003", -2177539199999997);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_SECOND, "1900-12-31 00:00:03.000000", -2177539197000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_MINUTE, "1900-12-31 00:03:00.000000", -2177539020000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_HOUR, "1900-12-31 03:00:00.000000", -2177528400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_DAY, "1901-01-03 00:00:00.000000", -2177280000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_WEEK, "1901-01-21 00:00:00.000000", -2175724800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_MONTH, "1901-03-31 00:00:00.000000", -2169763200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_QUARTER, "1901-09-30 00:00:00.000000", -2153952000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_YEAR, "1903-12-31 00:00:00.000000", -2082931200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "1900-12-31 00:00:03.300000", -2177539196700000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:00:03.300000", -2177539196700000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_MINUTE_SECOND, "1900-12-31 00:03:30.000000", -2177538990000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:00:03.300000", -2177539196700000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_HOUR_SECOND, "1900-12-31 00:03:30.000000", -2177538990000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_HOUR_MINUTE, "1900-12-31 03:30:00.000000", -2177526600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:00:03.300000", -2177539196700000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_DAY_SECOND, "1900-12-31 00:03:30.000000", -2177538990000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_DAY_MINUTE, "1900-12-31 03:30:00.000000", -2177526600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_DAY_HOUR, "1901-01-04 06:00:00.000000", -2177172000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-3:30", DATE_UNIT_YEAR_MONTH, "1906-06-30 00:00:00.000000", -2004134400000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999970", -2177539200000030);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_SECOND, "1900-12-30 23:59:30.000000", -2177539230000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_MINUTE, "1900-12-30 23:30:00.000000", -2177541000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_HOUR, "1900-12-29 18:00:00.000000", -2177647200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_DAY, "1900-12-01 00:00:00.000000", -2180131200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_WEEK, "1900-06-04 00:00:00.000000", -2195683200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_MONTH, "1898-06-30 00:00:00.000000", -2256508800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_QUARTER, "1893-06-30 00:00:00.000000", -2414275200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_YEAR, "1870-12-31 00:00:00.000000", -3124224000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_SECOND_MICROSECOND, "1900-12-30 23:59:29.700000", -2177539230300000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:59:29.700000", -2177539230300000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_MINUTE_SECOND, "1900-12-30 23:29:57.000000", -2177541003000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:59:29.700000", -2177539230300000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_HOUR_SECOND, "1900-12-30 23:29:57.000000", -2177541003000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_HOUR_MINUTE, "1900-12-29 17:57:00.000000", -2177647380000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:59:29.700000", -2177539230300000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_DAY_SECOND, "1900-12-30 23:29:57.000000", -2177541003000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_DAY_MINUTE, "1900-12-29 17:57:00.000000", -2177647380000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_DAY_HOUR, "1900-11-30 21:00:00.000000", -2180142000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "30!3", DATE_UNIT_YEAR_MONTH, "1870-09-30 00:00:00.000000", -3132172800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000004", -2177539199999996);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_SECOND, "1900-12-31 00:00:04.400000", -2177539195600000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE, "1900-12-31 00:04:00.000000", -2177538960000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR, "1900-12-31 04:00:00.000000", -2177524800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_DAY, "1901-01-04 00:00:00.000000", -2177193600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_WEEK, "1901-01-28 00:00:00.000000", -2175120000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_MONTH, "1901-04-30 00:00:00.000000", -2167171200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_QUARTER, "1901-12-31 00:00:00.000000", -2146003200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_YEAR, "1904-12-31 00:00:00.000000", -2051308800000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-31 00:04:40.400000", -2177538919600000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 00:04:40.400000", -2177538919600000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_SECOND, "1900-12-31 04:46:40.000000", -2177522000000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 00:04:40.400000", -2177538919600000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_SECOND, "1900-12-31 04:46:40.000000", -2177522000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_MINUTE, "1901-01-05 22:40:00.000000", -2177025600000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999600", -2177539200000400);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_SECOND, "1900-12-30 23:53:20.000000", -2177539600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE, "1900-12-30 17:20:00.000000", -2177563200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_HOUR, "1900-12-14 08:00:00.000000", -2178979200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_DAY, "1899-11-26 00:00:00.000000", -2212099200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_WEEK, "1893-05-01 00:00:00.000000", -2419459200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_MONTH, "1867-08-31 00:00:00.000000", -3229459200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_QUARTER, "1800-12-31 00:00:00.000000", -5333212800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_YEAR, "1500-12-31 00:00:00.000000", -14800320000000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 17:19:19.600000", -2177563240400000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 17:19:19.600000", -2177563240400000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_SECOND, "1900-12-14 07:19:56.000000", -2178981604000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 17:19:19.600000", -2177563240400000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_DAY_SECOND, "1900-12-14 07:19:56.000000", -2178981604000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "400@40#4", DATE_UNIT_DAY_MINUTE, "1899-11-24 07:56:00.000000", -2212243440000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "1900-12-31 00:00:00.000005", -2177539199999995);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_SECOND, "1900-12-31 00:00:05.000000", -2177539195000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE, "1900-12-31 00:05:00.000000", -2177538900000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR, "1900-12-31 05:00:00.000000", -2177521200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY, "1901-01-05 00:00:00.000000", -2177107200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_WEEK, "1901-02-04 00:00:00.000000", -2174515200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MONTH, "1901-05-31 00:00:00.000000", -2164492800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_QUARTER, "1902-03-31 00:00:00.000000", -2138227200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_YEAR, "1905-12-31 00:00:00.000000", -2019772800000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "1900-12-31 05:58:20.500000", -2177517699500000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "1900-12-31 05:58:20.500000", -2177517699500000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "1901-01-07 11:43:20.000000", -2176892200000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.995000", -2177539200005000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_SECOND, "1900-12-30 22:36:40.000000", -2177544200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE, "1900-12-27 12:40:00.000000", -2177839200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR, "1900-06-05 16:00:00.000000", -2195539200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY, "1887-04-23 00:00:00.000000", -2609539200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_WEEK, "1805-03-04 00:00:00.000000", -5201539200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_MONTH, "1484-04-30 00:00:00.000000", -15326323200000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_QUARTER, "0650-12-31 00:00:00.000000", -41623718400000000);
//  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_YEAR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "1900-06-05 07:39:09.500000", -2195569250500000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "1900-06-05 07:39:09.500000", -2195569250500000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "1887-04-02 03:09:55.000000", -2611342205000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999994", -2177539200000006);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_SECOND, "1900-12-30 23:59:54.000000", -2177539206000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "1900-12-30 23:54:00.000000", -2177539560000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR, "1900-12-30 18:00:00.000000", -2177560800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY, "1900-12-25 00:00:00.000000", -2178057600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_WEEK, "1900-11-19 00:00:00.000000", -2181168000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MONTH, "1900-06-30 00:00:00.000000", -2193436800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "1899-06-30 00:00:00.000000", -2224972800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_YEAR, "1894-12-31 00:00:00.000000", -2366841600000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "1900-12-22 00:19:59.400000", -2178315600600000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_MICROSECOND, "1900-12-30 23:59:59.999999", -2177539200000001);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_SECOND, "1900-12-30 23:59:58.800000", -2177539201200000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE, "1900-12-30 23:59:00.000000", -2177539260000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR, "1900-12-30 23:00:00.000000", -2177542800000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY, "1900-12-30 00:00:00.000000", -2177625600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_WEEK, "1900-12-24 00:00:00.000000", -2178144000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_MONTH, "1900-11-30 00:00:00.000000", -2180217600000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_QUARTER, "1900-09-30 00:00:00.000000", -2185488000000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_YEAR, "1899-12-31 00:00:00.000000", -2209075200000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "1900-12-30 23:58:54.543211", -2177539265456789);
  DATE_ADJUST_FAIL(false, "1900-12-31", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "1900-12-30 23:58:54.543211", -2177539265456789);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "1900-11-20 22:44:51.000000", -2180999709000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "1900-12-30 23:58:54.543211", -2177539265456789);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_SECOND, "1900-11-20 22:44:51.000000", -2180999709000000);
  DATE_ADJUST_SUCC(false, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "1894-06-03 08:51:00.000000", -2385040140000000);
  DATE_ADJUST_FAIL(false, "1900-12-31", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1900-12-31", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000001", 1);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_SECOND, "1970-01-01 00:00:01.000000", 1000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_MINUTE, "1970-01-01 00:01:00.000000", 60000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_HOUR, "1970-01-01 01:00:00.000000", 3600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_DAY, "1970-01-02 00:00:00.000000", 86400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_WEEK, "1970-01-08 00:00:00.000000", 604800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_MONTH, "1970-02-01 00:00:00.000000", 2678400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_QUARTER, "1970-04-01 00:00:00.000000", 7776000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_YEAR, "1971-01-01 00:00:00.000000", 31536000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_SECOND_MICROSECOND, "1970-01-01 00:00:00.100000", 100000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:00:00.100000", 100000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_MINUTE_SECOND, "1970-01-01 00:00:01.000000", 1000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:00:00.100000", 100000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_HOUR_SECOND, "1970-01-01 00:00:01.000000", 1000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_HOUR_MINUTE, "1970-01-01 00:01:00.000000", 60000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:00:00.100000", 100000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_DAY_SECOND, "1970-01-01 00:00:01.000000", 1000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_DAY_MINUTE, "1970-01-01 00:01:00.000000", 60000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_DAY_HOUR, "1970-01-01 01:00:00.000000", 3600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-1", DATE_UNIT_YEAR_MONTH, "1970-02-01 00:00:00.000000", 2678400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999800", -200);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_SECOND, "1969-12-31 23:56:40.000000", -200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_MINUTE, "1969-12-31 20:40:00.000000", -12000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_HOUR, "1969-12-23 16:00:00.000000", -720000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_DAY, "1969-06-15 00:00:00.000000", -17280000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_WEEK, "1966-03-03 00:00:00.000000", -120960000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_MONTH, "1953-05-01 00:00:00.000000", -526089600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_QUARTER, "1920-01-01 00:00:00.000000", -1577923200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_YEAR, "1770-01-01 00:00:00.000000", -6311347200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_SECOND_MICROSECOND, "1969-12-31 23:59:59.800000", -200000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:59:59.800000", -200000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_MINUTE_SECOND, "1969-12-31 23:56:40.000000", -200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:59:59.800000", -200000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_HOUR_SECOND, "1969-12-31 23:56:40.000000", -200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_HOUR_MINUTE, "1969-12-31 20:40:00.000000", -12000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:59:59.800000", -200000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_DAY_SECOND, "1969-12-31 23:56:40.000000", -200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_DAY_MINUTE, "1969-12-31 20:40:00.000000", -12000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_DAY_HOUR, "1969-12-23 16:00:00.000000", -720000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "200", DATE_UNIT_YEAR_MONTH, "1953-05-01 00:00:00.000000", -526089600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000003", 3);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_SECOND, "1970-01-01 00:00:03.000000", 3000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_MINUTE, "1970-01-01 00:03:00.000000", 180000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_HOUR, "1970-01-01 03:00:00.000000", 10800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_DAY, "1970-01-04 00:00:00.000000", 259200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_WEEK, "1970-01-22 00:00:00.000000", 1814400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_MONTH, "1970-04-01 00:00:00.000000", 7776000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_QUARTER, "1970-10-01 00:00:00.000000", 23587200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_YEAR, "1973-01-01 00:00:00.000000", 94694400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "1970-01-01 00:00:03.300000", 3300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:00:03.300000", 3300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_MINUTE_SECOND, "1970-01-01 00:03:30.000000", 210000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:00:03.300000", 3300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_HOUR_SECOND, "1970-01-01 00:03:30.000000", 210000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_HOUR_MINUTE, "1970-01-01 03:30:00.000000", 12600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:00:03.300000", 3300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_DAY_SECOND, "1970-01-01 00:03:30.000000", 210000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_DAY_MINUTE, "1970-01-01 03:30:00.000000", 12600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_DAY_HOUR, "1970-01-05 06:00:00.000000", 367200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-3:30", DATE_UNIT_YEAR_MONTH, "1975-07-01 00:00:00.000000", 173404800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999970", -30);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_SECOND, "1969-12-31 23:59:30.000000", -30000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_MINUTE, "1969-12-31 23:30:00.000000", -1800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_HOUR, "1969-12-30 18:00:00.000000", -108000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_DAY, "1969-12-02 00:00:00.000000", -2592000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_WEEK, "1969-06-05 00:00:00.000000", -18144000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_MONTH, "1967-07-01 00:00:00.000000", -79056000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_QUARTER, "1962-07-01 00:00:00.000000", -236822400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_YEAR, "1940-01-01 00:00:00.000000", -946771200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_SECOND_MICROSECOND, "1969-12-31 23:59:29.700000", -30300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:59:29.700000", -30300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_MINUTE_SECOND, "1969-12-31 23:29:57.000000", -1803000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:59:29.700000", -30300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_HOUR_SECOND, "1969-12-31 23:29:57.000000", -1803000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_HOUR_MINUTE, "1969-12-30 17:57:00.000000", -108180000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:59:29.700000", -30300000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_DAY_SECOND, "1969-12-31 23:29:57.000000", -1803000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_DAY_MINUTE, "1969-12-30 17:57:00.000000", -108180000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_DAY_HOUR, "1969-12-01 21:00:00.000000", -2602800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "30!3", DATE_UNIT_YEAR_MONTH, "1939-10-01 00:00:00.000000", -954720000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000004", 4);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_SECOND, "1970-01-01 00:00:04.400000", 4400000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE, "1970-01-01 00:04:00.000000", 240000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR, "1970-01-01 04:00:00.000000", 14400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_DAY, "1970-01-05 00:00:00.000000", 345600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_WEEK, "1970-01-29 00:00:00.000000", 2419200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_MONTH, "1970-05-01 00:00:00.000000", 10368000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_QUARTER, "1971-01-01 00:00:00.000000", 31536000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_YEAR, "1974-01-01 00:00:00.000000", 126230400000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "1970-01-01 00:04:40.400000", 280400000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 00:04:40.400000", 280400000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_SECOND, "1970-01-01 04:46:40.000000", 17200000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 00:04:40.400000", 280400000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_SECOND, "1970-01-01 04:46:40.000000", 17200000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_MINUTE, "1970-01-06 22:40:00.000000", 513600000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999600", -400);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_SECOND, "1969-12-31 23:53:20.000000", -400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE, "1969-12-31 17:20:00.000000", -24000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_HOUR, "1969-12-15 08:00:00.000000", -1440000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_DAY, "1968-11-27 00:00:00.000000", -34560000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_WEEK, "1962-05-03 00:00:00.000000", -241920000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_MONTH, "1936-09-01 00:00:00.000000", -1051920000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_QUARTER, "1870-01-01 00:00:00.000000", -3155673600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_YEAR, "1570-01-01 00:00:00.000000", -12622780800000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 17:19:19.600000", -24040400000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 17:19:19.600000", -24040400000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_SECOND, "1969-12-15 07:19:56.000000", -1442404000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 17:19:19.600000", -24040400000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_DAY_SECOND, "1969-12-15 07:19:56.000000", -1442404000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "400@40#4", DATE_UNIT_DAY_MINUTE, "1968-11-25 07:56:00.000000", -34704240000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "1970-01-01 00:00:00.000005", 5);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_SECOND, "1970-01-01 00:00:05.000000", 5000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE, "1970-01-01 00:05:00.000000", 300000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR, "1970-01-01 05:00:00.000000", 18000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY, "1970-01-06 00:00:00.000000", 432000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_WEEK, "1970-02-05 00:00:00.000000", 3024000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MONTH, "1970-06-01 00:00:00.000000", 13046400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_QUARTER, "1971-04-01 00:00:00.000000", 39312000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_YEAR, "1975-01-01 00:00:00.000000", 157766400000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "1970-01-01 05:58:20.500000", 21500500000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "1970-01-01 05:58:20.500000", 21500500000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "1970-01-08 11:43:20.000000", 647000000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.995000", -5000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_SECOND, "1969-12-31 22:36:40.000000", -5000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE, "1969-12-28 12:40:00.000000", -300000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR, "1969-06-06 16:00:00.000000", -18000000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY, "1956-04-24 00:00:00.000000", -432000000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_WEEK, "1874-03-05 00:00:00.000000", -3024000000000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_MONTH, "1553-05-01 00:00:00.000000", -13148870400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_QUARTER, "0720-01-01 00:00:00.000000", -39446265600000000);
//  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_YEAR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "1969-06-06 07:39:09.500000", -18030050500000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "1969-06-06 07:39:09.500000", -18030050500000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "1956-04-03 03:09:55.000000", -433803005000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999994", -6);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_SECOND, "1969-12-31 23:59:54.000000", -6000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "1969-12-31 23:54:00.000000", -360000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR, "1969-12-31 18:00:00.000000", -21600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY, "1969-12-26 00:00:00.000000", -518400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_WEEK, "1969-11-20 00:00:00.000000", -3628800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MONTH, "1969-07-01 00:00:00.000000", -15897600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "1968-07-01 00:00:00.000000", -47433600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_YEAR, "1964-01-01 00:00:00.000000", -189388800000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "1969-12-23 00:19:59.400000", -776400600000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_MICROSECOND, "1969-12-31 23:59:59.999999", -1);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_SECOND, "1969-12-31 23:59:58.800000", -1200000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE, "1969-12-31 23:59:00.000000", -60000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR, "1969-12-31 23:00:00.000000", -3600000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY, "1969-12-31 00:00:00.000000", -86400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_WEEK, "1969-12-25 00:00:00.000000", -604800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_MONTH, "1969-12-01 00:00:00.000000", -2678400000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_QUARTER, "1969-10-01 00:00:00.000000", -7948800000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_YEAR, "1969-01-01 00:00:00.000000", -31536000000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "1969-12-31 23:58:54.543211", -65456789);
  DATE_ADJUST_FAIL(false, "1970-1-1", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "1969-12-31 23:58:54.543211", -65456789);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "1969-11-21 22:44:51.000000", -3460509000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "1969-12-31 23:58:54.543211", -65456789);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_SECOND, "1969-11-21 22:44:51.000000", -3460509000000);
  DATE_ADJUST_SUCC(false, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "1963-06-05 08:51:00.000000", -207500940000000);
  DATE_ADJUST_FAIL(false, "1970-1-1", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "1970-1-1", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000001", 1330473600000001);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_SECOND, "2012-02-29 00:00:01.000000", 1330473601000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_MINUTE, "2012-02-29 00:01:00.000000", 1330473660000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_HOUR, "2012-02-29 01:00:00.000000", 1330477200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_DAY, "2012-03-01 00:00:00.000000", 1330560000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_WEEK, "2012-03-07 00:00:00.000000", 1331078400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_MONTH, "2012-03-29 00:00:00.000000", 1332979200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_QUARTER, "2012-05-29 00:00:00.000000", 1338249600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_YEAR, "2013-02-28 00:00:00.000000", 1362009600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_SECOND_MICROSECOND, "2012-02-29 00:00:00.100000", 1330473600100000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:00:00.100000", 1330473600100000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_MINUTE_SECOND, "2012-02-29 00:00:01.000000", 1330473601000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:00:00.100000", 1330473600100000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_HOUR_SECOND, "2012-02-29 00:00:01.000000", 1330473601000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_HOUR_MINUTE, "2012-02-29 00:01:00.000000", 1330473660000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:00:00.100000", 1330473600100000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_DAY_SECOND, "2012-02-29 00:00:01.000000", 1330473601000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_DAY_MINUTE, "2012-02-29 00:01:00.000000", 1330473660000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_DAY_HOUR, "2012-02-29 01:00:00.000000", 1330477200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-1", DATE_UNIT_YEAR_MONTH, "2012-03-29 00:00:00.000000", 1332979200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999800", 1330473599999800);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_SECOND, "2012-02-28 23:56:40.000000", 1330473400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_MINUTE, "2012-02-28 20:40:00.000000", 1330461600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_HOUR, "2012-02-20 16:00:00.000000", 1329753600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_DAY, "2011-08-13 00:00:00.000000", 1313193600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_WEEK, "2008-04-30 00:00:00.000000", 1209513600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_MONTH, "1995-06-29 00:00:00.000000", 804384000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_QUARTER, "1962-02-28 00:00:00.000000", -247449600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_YEAR, "1812-02-29 00:00:00.000000", -4980960000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_SECOND_MICROSECOND, "2012-02-28 23:59:59.800000", 1330473599800000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:59:59.800000", 1330473599800000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_MINUTE_SECOND, "2012-02-28 23:56:40.000000", 1330473400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:59:59.800000", 1330473599800000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_HOUR_SECOND, "2012-02-28 23:56:40.000000", 1330473400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_HOUR_MINUTE, "2012-02-28 20:40:00.000000", 1330461600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:59:59.800000", 1330473599800000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_DAY_SECOND, "2012-02-28 23:56:40.000000", 1330473400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_DAY_MINUTE, "2012-02-28 20:40:00.000000", 1330461600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_DAY_HOUR, "2012-02-20 16:00:00.000000", 1329753600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "200", DATE_UNIT_YEAR_MONTH, "1995-06-29 00:00:00.000000", 804384000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000003", 1330473600000003);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_SECOND, "2012-02-29 00:00:03.000000", 1330473603000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_MINUTE, "2012-02-29 00:03:00.000000", 1330473780000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_HOUR, "2012-02-29 03:00:00.000000", 1330484400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_DAY, "2012-03-03 00:00:00.000000", 1330732800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_WEEK, "2012-03-21 00:00:00.000000", 1332288000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_MONTH, "2012-05-29 00:00:00.000000", 1338249600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_QUARTER, "2012-11-29 00:00:00.000000", 1354147200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_YEAR, "2015-02-28 00:00:00.000000", 1425081600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_SECOND_MICROSECOND, "2012-02-29 00:00:03.300000", 1330473603300000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:00:03.300000", 1330473603300000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_MINUTE_SECOND, "2012-02-29 00:03:30.000000", 1330473810000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:00:03.300000", 1330473603300000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_HOUR_SECOND, "2012-02-29 00:03:30.000000", 1330473810000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_HOUR_MINUTE, "2012-02-29 03:30:00.000000", 1330486200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:00:03.300000", 1330473603300000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_DAY_SECOND, "2012-02-29 00:03:30.000000", 1330473810000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_DAY_MINUTE, "2012-02-29 03:30:00.000000", 1330486200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_DAY_HOUR, "2012-03-04 06:00:00.000000", 1330840800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-3:30", DATE_UNIT_YEAR_MONTH, "2017-08-29 00:00:00.000000", 1503964800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999970", 1330473599999970);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_SECOND, "2012-02-28 23:59:30.000000", 1330473570000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_MINUTE, "2012-02-28 23:30:00.000000", 1330471800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_HOUR, "2012-02-27 18:00:00.000000", 1330365600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_DAY, "2012-01-30 00:00:00.000000", 1327881600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_WEEK, "2011-08-03 00:00:00.000000", 1312329600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_MONTH, "2009-08-29 00:00:00.000000", 1251504000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_QUARTER, "2004-08-29 00:00:00.000000", 1093737600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_YEAR, "1982-02-28 00:00:00.000000", 383702400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_SECOND_MICROSECOND, "2012-02-28 23:59:29.700000", 1330473569700000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:59:29.700000", 1330473569700000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_MINUTE_SECOND, "2012-02-28 23:29:57.000000", 1330471797000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:59:29.700000", 1330473569700000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_HOUR_SECOND, "2012-02-28 23:29:57.000000", 1330471797000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_HOUR_MINUTE, "2012-02-27 17:57:00.000000", 1330365420000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:59:29.700000", 1330473569700000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_DAY_SECOND, "2012-02-28 23:29:57.000000", 1330471797000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_DAY_MINUTE, "2012-02-27 17:57:00.000000", 1330365420000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_DAY_HOUR, "2012-01-29 21:00:00.000000", 1327870800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "30!3", DATE_UNIT_YEAR_MONTH, "1981-11-29 00:00:00.000000", 375840000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000004", 1330473600000004);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_SECOND, "2012-02-29 00:00:04.400000", 1330473604400000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE, "2012-02-29 00:04:00.000000", 1330473840000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR, "2012-02-29 04:00:00.000000", 1330488000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_DAY, "2012-03-04 00:00:00.000000", 1330819200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_WEEK, "2012-03-28 00:00:00.000000", 1332892800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_MONTH, "2012-06-29 00:00:00.000000", 1340928000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_QUARTER, "2013-02-28 00:00:00.000000", 1362009600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_YEAR, "2016-02-29 00:00:00.000000", 1456704000000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-4.40.400", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-29 00:04:40.400000", 1330473880400000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-4.40.400", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 00:04:40.400000", 1330473880400000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_SECOND, "2012-02-29 04:46:40.000000", 1330490800000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-4.40.400", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 00:04:40.400000", 1330473880400000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_SECOND, "2012-02-29 04:46:40.000000", 1330490800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_MINUTE, "2012-03-05 22:40:00.000000", 1330987200000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-4.40.400", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-4.40.400", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999600", 1330473599999600);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_SECOND, "2012-02-28 23:53:20.000000", 1330473200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE, "2012-02-28 17:20:00.000000", 1330449600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_HOUR, "2012-02-12 08:00:00.000000", 1329033600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_DAY, "2011-01-25 00:00:00.000000", 1295913600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_WEEK, "2004-06-30 00:00:00.000000", 1088553600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_MONTH, "1978-10-29 00:00:00.000000", 278467200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_QUARTER, "1912-02-29 00:00:00.000000", -1825286400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_YEAR, "1612-02-29 00:00:00.000000", -11292307200000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "400@40#4", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 17:19:19.600000", 1330449559600000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "400@40#4", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 17:19:19.600000", 1330449559600000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_SECOND, "2012-02-12 07:19:56.000000", 1329031196000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "400@40#4", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 17:19:19.600000", 1330449559600000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_DAY_SECOND, "2012-02-12 07:19:56.000000", 1329031196000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "400@40#4", DATE_UNIT_DAY_MINUTE, "2011-01-23 07:56:00.000000", 1295769360000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "400@40#4", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "400@40#4", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MICROSECOND, "2012-02-29 00:00:00.000005", 1330473600000005);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_SECOND, "2012-02-29 00:00:05.000000", 1330473605000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE, "2012-02-29 00:05:00.000000", 1330473900000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR, "2012-02-29 05:00:00.000000", 1330491600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY, "2012-03-05 00:00:00.000000", 1330905600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_WEEK, "2012-04-04 00:00:00.000000", 1333497600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MONTH, "2012-07-29 00:00:00.000000", 1343520000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_QUARTER, "2013-05-29 00:00:00.000000", 1369785600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_YEAR, "2017-02-28 00:00:00.000000", 1488240000000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_MICROSECOND, "2012-02-29 05:58:20.500000", 1330495100500000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_MICROSECOND, "2012-02-29 05:58:20.500000", 1330495100500000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_SECOND, "2012-03-07 11:43:20.000000", 1331120600000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "-5+50=500*5000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.995000", 1330473599995000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_SECOND, "2012-02-28 22:36:40.000000", 1330468600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE, "2012-02-25 12:40:00.000000", 1330173600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR, "2011-08-04 16:00:00.000000", 1312473600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY, "1998-06-22 00:00:00.000000", 898473600000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_WEEK, "1916-05-03 00:00:00.000000", -1693526400000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_MONTH, "1595-06-29 00:00:00.000000", -11818396800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_QUARTER, "0762-02-28 00:00:00.000000", -38115792000000000);
//  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_YEAR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_MICROSECOND, "2011-08-04 07:39:09.500000", 1312443549500000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_MICROSECOND, "2011-08-04 07:39:09.500000", 1312443549500000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_SECOND, "1998-06-01 03:09:55.000000", 896670595000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "5000+500=50*5", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999994", 1330473599999994);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_SECOND, "2012-02-28 23:59:54.000000", 1330473594000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE, "2012-02-28 23:54:00.000000", 1330473240000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR, "2012-02-28 18:00:00.000000", 1330452000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY, "2012-02-23 00:00:00.000000", 1329955200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_WEEK, "2012-01-18 00:00:00.000000", 1326844800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MONTH, "2011-08-29 00:00:00.000000", 1314576000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_QUARTER, "2010-08-29 00:00:00.000000", 1283040000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_YEAR, "2006-02-28 00:00:00.000000", 1141084800000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_MICROSECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_SECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_MICROSECOND, "2012-02-20 00:19:59.400000", 1329697199400000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_SECOND);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_MINUTE);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "6&60^600%6000$60000", DATE_UNIT_YEAR_MONTH);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_MICROSECOND, "2012-02-28 23:59:59.999999", 1330473599999999);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_SECOND, "2012-02-28 23:59:58.800000", 1330473598800000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE, "2012-02-28 23:59:00.000000", 1330473540000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR, "2012-02-28 23:00:00.000000", 1330470000000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY, "2012-02-28 00:00:00.000000", 1330387200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_WEEK, "2012-02-22 00:00:00.000000", 1329868800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_MONTH, "2012-01-29 00:00:00.000000", 1327795200000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_QUARTER, "2011-11-29 00:00:00.000000", 1322524800000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_YEAR, "2011-02-28 00:00:00.000000", 1298851200000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "1.2.3456789", DATE_UNIT_SECOND_MICROSECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE_MICROSECOND, "2012-02-28 23:58:54.543211", 1330473534543211);
  DATE_ADJUST_FAIL(false, "2012-2-29", "1.2.3456789", DATE_UNIT_MINUTE_SECOND);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_MICROSECOND, "2012-02-28 23:58:54.543211", 1330473534543211);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_SECOND, "2012-01-19 22:44:51.000000", 1327013091000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "1.2.3456789", DATE_UNIT_HOUR_MINUTE);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_MICROSECOND, "2012-02-28 23:58:54.543211", 1330473534543211);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_SECOND, "2012-01-19 22:44:51.000000", 1327013091000000);
  DATE_ADJUST_SUCC(false, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_MINUTE, "2005-08-02 08:51:00.000000", 1122972660000000);
  DATE_ADJUST_FAIL(false, "2012-2-29", "1.2.3456789", DATE_UNIT_DAY_HOUR);
  DATE_ADJUST_FAIL(false, "2012-2-29", "1.2.3456789", DATE_UNIT_YEAR_MONTH);
}

#define INT_TO_OB_TIME_SUCC(int_val, mode, year, month, day, hour, minute, second, usecond) \
  do { \
    memset(&ob_time, 0, sizeof(ob_time)); \
    ob_time.mode_ = mode; \
    if (DT_TYPE_DATE & mode) { \
      EXPECT_EQ(OB_SUCCESS, ObTimeConverter::int_to_ob_time_with_date(int_val, ob_time, 0)); \
    } else { \
      EXPECT_EQ(OB_SUCCESS, ObTimeConverter::int_to_ob_time_without_date(int_val, ob_time)); \
    } \
    EXPECT_TRUE(ob_time_eq(ob_time, year, month, day, hour, minute, second, usecond)); \
  } while (0)

#define INT_TO_OB_TIME_FAIL(int_val, mode) \
  do { \
    memset(&ob_time, 0, sizeof(ob_time)); \
    ob_time.mode_ = mode; \
    if (DT_TYPE_DATE & mode) { \
      EXPECT_NE(OB_SUCCESS, ObTimeConverter::int_to_ob_time_with_date(int_val, ob_time, 0)); \
    } else { \
      EXPECT_NE(OB_SUCCESS, ObTimeConverter::int_to_ob_time_without_date(int_val, ob_time)); \
    } \
  } while (0)

TEST(ObTimeConvertTest, int_to_ob_time)
{
  ObTime ob_time;
  // datetime.
  INT_TO_OB_TIME_FAIL(23, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_SUCC(213, DT_TYPE_DATETIME, 2000, 2, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_FAIL(2113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_SUCC(21113, DT_TYPE_DATETIME, 2002, 11, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(211113, DT_TYPE_DATETIME, 2021, 11, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(2111113, DT_TYPE_DATETIME, 211, 11, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(21111113, DT_TYPE_DATETIME, 2111, 11, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(211111113, DT_TYPE_DATETIME, 2000, 2, 11, 11, 11, 13, 0);
  INT_TO_OB_TIME_FAIL(2111111113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_SUCC(21111111113, DT_TYPE_DATETIME, 2002, 11, 11, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(211111111113, DT_TYPE_DATETIME, 2021, 11, 11, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(2111111111113, DT_TYPE_DATETIME, 211, 11, 11, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(21111111111113, DT_TYPE_DATETIME, 2111, 11, 11, 11, 11, 13, 0);
  INT_TO_OB_TIME_FAIL(211111111111113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(2111111111111113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(21111111111111113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(211111111111111113, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(9223372036854775807, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(10, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_SUCC(101, DT_TYPE_DATETIME, 2000, 1, 1, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(1011, DT_TYPE_DATETIME, 2000, 10, 11, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(10111, DT_TYPE_DATETIME, 2001, 1, 11, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(101112, DT_TYPE_DATETIME, 2010, 11, 12, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(1011121, DT_TYPE_DATETIME, 101, 11, 21, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(10111213, DT_TYPE_DATETIME, 1011, 12, 13, 0, 0, 0, 0);
  INT_TO_OB_TIME_SUCC(101112131, DT_TYPE_DATETIME, 2000, 1, 1, 11, 21, 31, 0);
  INT_TO_OB_TIME_SUCC(1011121314, DT_TYPE_DATETIME, 2000, 10, 11, 12, 13, 14, 0);
  INT_TO_OB_TIME_SUCC(10111213141, DT_TYPE_DATETIME, 2001, 1, 11, 21, 31, 41, 0);
  INT_TO_OB_TIME_SUCC(101112131415, DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  INT_TO_OB_TIME_FAIL(1011121314151, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_SUCC(10111213141516, DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  INT_TO_OB_TIME_FAIL(101112131415161, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(1011121314151617, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(10111213141516171, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(101112131415161718, DT_TYPE_DATETIME);
  INT_TO_OB_TIME_FAIL(9223372036854775807, DT_TYPE_DATETIME);
  // date.
  INT_TO_OB_TIME_FAIL(23, DT_TYPE_DATE);
  INT_TO_OB_TIME_SUCC(213, DT_TYPE_DATE, 2000, 2, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_FAIL(2113, DT_TYPE_DATE);
  INT_TO_OB_TIME_SUCC(21113, DT_TYPE_DATE, 2002, 11, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(211113, DT_TYPE_DATE, 2021, 11, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(2111113, DT_TYPE_DATE, 211, 11, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(21111113, DT_TYPE_DATE, 2111, 11, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(211111113, DT_TYPE_DATE, 2000, 2, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_FAIL(2111111113, DT_TYPE_DATE);
  INT_TO_OB_TIME_SUCC(21111111113, DT_TYPE_DATE, 2002, 11, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(211111111113, DT_TYPE_DATE, 2021, 11, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(2111111111113, DT_TYPE_DATE, 211, 11, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(21111111111113, DT_TYPE_DATE, 2111, 11, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_FAIL(211111111111113, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(2111111111111113, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(21111111111111113, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(211111111111111113, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(9223372036854775807, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(10, DT_TYPE_DATE);
  INT_TO_OB_TIME_SUCC(101, DT_TYPE_DATE, 2000, 1, 1, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(1011, DT_TYPE_DATE, 2000, 10, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10111, DT_TYPE_DATE, 2001, 1, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(101112, DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(1011121, DT_TYPE_DATE, 101, 11, 21, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10111213, DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(101112131, DT_TYPE_DATE, 2000, 1, 1, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(1011121314, DT_TYPE_DATE, 2000, 10, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10111213141, DT_TYPE_DATE, 2001, 1, 11, -1, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(101112131415, DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  INT_TO_OB_TIME_FAIL(1011121314151, DT_TYPE_DATE);
  INT_TO_OB_TIME_SUCC(10111213141516, DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  INT_TO_OB_TIME_FAIL(101112131415161, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(1011121314151617, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(10111213141516171, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(101112131415161718, DT_TYPE_DATE);
  INT_TO_OB_TIME_FAIL(9223372036854775807, DT_TYPE_DATE);
  // time.
  INT_TO_OB_TIME_SUCC(23, DT_TYPE_TIME, -1, -1, -1, 0, 0, 23, 0);
  INT_TO_OB_TIME_SUCC(213, DT_TYPE_TIME, -1, -1, -1, 0, 2, 13, 0);
  INT_TO_OB_TIME_SUCC(2113, DT_TYPE_TIME, -1, -1, -1, 0, 21, 13, 0);
  INT_TO_OB_TIME_SUCC(21113, DT_TYPE_TIME, -1, -1, -1, 2, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(211113, DT_TYPE_TIME, -1, -1, -1, 21, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(2111113, DT_TYPE_TIME, -1, -1, -1, 211, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(21111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(211111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(2111111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(21111111113, DT_TYPE_TIME, -1, -1, -1, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(211111111113, DT_TYPE_TIME, -1, -1, -1, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(2111111111113, DT_TYPE_TIME, -1, -1, -1, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(21111111111113, DT_TYPE_TIME, -1, -1, -1, 11, 11, 13, 0);
  INT_TO_OB_TIME_SUCC(211111111111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(2111111111111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(21111111111111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(211111111111111113, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(9223372036854775807, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10, DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 0);
  INT_TO_OB_TIME_SUCC(101, DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 0);
  INT_TO_OB_TIME_SUCC(1011, DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 0);
  INT_TO_OB_TIME_SUCC(10111, DT_TYPE_TIME, -1, -1, -1, 1, 1, 11, 0);
  INT_TO_OB_TIME_SUCC(101112, DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 0);
  INT_TO_OB_TIME_SUCC(1011121, DT_TYPE_TIME, -1, -1, -1, 101, 11, 21, 0);
  INT_TO_OB_TIME_SUCC(10111213, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(101112131, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(1011121314, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10111213141, DT_TYPE_TIME, -1, -1, -1, 21, 31, 41, 0);
  INT_TO_OB_TIME_SUCC(101112131415, DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  INT_TO_OB_TIME_FAIL(1011121314151, DT_TYPE_TIME);
  INT_TO_OB_TIME_SUCC(10111213141516, DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  INT_TO_OB_TIME_SUCC(101112131415161, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(1011121314151617, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(10111213141516171, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(101112131415161718, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  INT_TO_OB_TIME_SUCC(9223372036854775807, DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
}

#define STR_TO_OB_TIME_SUCC(str, mode, year, month, day, hour, minute, second, usecond) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    memset(&ob_time, 0, sizeof(ob_time)); \
    ob_time.mode_ = mode; \
    if (DT_TYPE_TIME != mode) { \
      EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_with_date(buf, ob_time, NULL, false, 0)); \
    } else { \
      EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_without_date(buf, ob_time)); \
    } \
    EXPECT_TRUE(ob_time_eq(ob_time, year, month, day, hour, minute, second, usecond)); \
  } while (0)

#define STR_TO_OB_TIME_FAIL(str, mode) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    memset(&ob_time, 0, sizeof(ob_time)); \
    ob_time.mode_ = mode; \
    if (DT_TYPE_TIME != mode) { \
      EXPECT_NE(OB_SUCCESS, ObTimeConverter::str_to_ob_time_with_date(buf, ob_time, NULL, false, 0)); \
    } else { \
      EXPECT_NE(OB_SUCCESS, ObTimeConverter::str_to_ob_time_without_date(buf, ob_time)); \
    } \
  } while (0)

TEST(ObTimeConvertTest, str_to_ob_time)
{
  ObString buf;
  ObTime ob_time;
  // datetime.
  STR_TO_OB_TIME_SUCC("2015-05-30 11:12:13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015.05.30 11.12.13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015/05/30 11/12/13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015+05=30 11*12&13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015^05%30 11$12#13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015@05!30 11~12`13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("0000-05-30 11:12:13.1415", DT_TYPE_DATETIME, 0, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("00002015-00000005-00000030 11:12:13.1415", DT_TYPE_DATETIME, 2015, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("00000015-00000005-00000030 11:12:13.1415", DT_TYPE_DATETIME, 15, 5, 30, 11, 12, 13, 141500);
  STR_TO_OB_TIME_FAIL("00000015.00000005.00000030 11:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10000-05-30 11:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10000.05.30 11:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2015-13-30 11:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2015-05-32 11:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 24:12:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:60:13.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:12:60.1415", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("2015-12-31 23:59:59.9999999", DT_TYPE_DATETIME, 2016, 1, 1, 00, 00, 00, 000000);
  STR_TO_OB_TIME_FAIL("10", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("101", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("10111", DT_TYPE_DATETIME, 2010, 11, 1, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("101112", DT_TYPE_DATETIME, 2010, 11, 12, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("1011121", DT_TYPE_DATETIME, 2010, 11, 12, 1, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("10111213", DT_TYPE_DATETIME, 1011, 12, 13, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("101112131", DT_TYPE_DATETIME, 2010, 11, 12, 13, 1, 0, 0);
  STR_TO_OB_TIME_SUCC("1011121314", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 0, 0);
  STR_TO_OB_TIME_SUCC("10111213141", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 1, 0);
  STR_TO_OB_TIME_SUCC("101112131415", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("1011121314151", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("10111213141516", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516171", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_FAIL("10", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10.1", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10.11", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011.1", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011.12", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("101112.1", DT_TYPE_DATETIME, 2010, 11, 12, 1, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("101112.13", DT_TYPE_DATETIME, 2010, 11, 12, 13, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("10111213.1", DT_TYPE_DATETIME, 1011, 12, 13, 1, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("10111213.14", DT_TYPE_DATETIME, 1011, 12, 13, 14, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("1011121314.1", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 1, 0);
  STR_TO_OB_TIME_SUCC("1011121314.15", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.1", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 100000);
  STR_TO_OB_TIME_SUCC("101112131415.16", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 160000);
  STR_TO_OB_TIME_SUCC("101112131415.16.17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 160000);
  STR_TO_OB_TIME_SUCC("101112131415.16-17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 160000);
  STR_TO_OB_TIME_SUCC("101112131415..16", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415..16.17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415..16-17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16.17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16-17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_FAIL("101112131415-16", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("101112131415-16.17", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("101112131415-16-17", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("101112131415.161111.17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 161111);
  STR_TO_OB_TIME_SUCC("101112131415.1611112.17", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 161111);
  STR_TO_OB_TIME_SUCC("10111213141516.1", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 100000);
  STR_TO_OB_TIME_SUCC("10111213141516.17", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 170000);
  STR_TO_OB_TIME_SUCC("1011121314151617.1", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617.18", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161.718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516.1718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 171800);
  STR_TO_OB_TIME_SUCC("1011121314151.61718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 161718);
  STR_TO_OB_TIME_SUCC("10111213141.5161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 1, 516172);
  STR_TO_OB_TIME_SUCC("1011121314.15161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131.415161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 1, 41, 0);
  STR_TO_OB_TIME_SUCC("10111213.1415161718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121.31415161718", DT_TYPE_DATETIME, 2010, 11, 12, 1, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("101112.131415161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("10111.2131415161718", DT_TYPE_DATETIME, 2010, 11, 1, 21, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("1011.12131415161718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101.112131415161718", DT_TYPE_DATETIME, 2010, 1, 11, 21, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("10.1112131415161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("10.11121314151617.18", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101.112131415161.718", DT_TYPE_DATETIME, 2010, 1, 11, 21, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("1011.1213141516.1718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 171800);
  STR_TO_OB_TIME_SUCC("10111.21314151.61718", DT_TYPE_DATETIME, 2010, 11, 1, 21, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("101112.131415.161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 161718);
  STR_TO_OB_TIME_SUCC("1011121.3141.5161718", DT_TYPE_DATETIME, 2010, 11, 12, 1, 31, 41, 516172);
  STR_TO_OB_TIME_SUCC("10111213.14.15161718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415.1.61718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 100000);
  STR_TO_OB_TIME_SUCC("101112131415.16.1718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 160000);
  STR_TO_OB_TIME_SUCC("101112131415.161.718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 161000);
  STR_TO_OB_TIME_SUCC("10111213141516.1.718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 100000);
  STR_TO_OB_TIME_SUCC("10111213141516.17.18", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 170000);
  STR_TO_OB_TIME_SUCC("10111213141516.171.8", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 171000);
  STR_TO_OB_TIME_SUCC("10..1112131415161718", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101..112131415161718", DT_TYPE_DATETIME, 2010, 1, 11, 21, 31, 41, 0);
  STR_TO_OB_TIME_SUCC("1011..12131415161718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516..1718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161..718", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617..18", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("2012.010112345", DT_TYPE_DATETIME, 2012, 1, 1, 12, 34, 5, 0);
  STR_TO_OB_TIME_FAIL("2012+010112345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("2012.01=0112345", DT_TYPE_DATETIME, 2012, 1, 1, 12, 34, 5, 0);
  STR_TO_OB_TIME_FAIL("2012*01=0112345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("2012.01=011*2345", DT_TYPE_DATETIME, 2012, 1, 1, 1, 23, 45, 0);
  STR_TO_OB_TIME_FAIL("2012!01=011*2345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("2012.*01^011&2345", DT_TYPE_DATETIME, 2012, 1, 1, 1, 23, 45, 0);
  STR_TO_OB_TIME_FAIL("2012*.01^011&2345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("2012**01^011&2345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("201201.0112345", DT_TYPE_DATETIME, 2020, 12, 1, 1, 12, 34, 0);
  STR_TO_OB_TIME_FAIL("201201%0112345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("201201.01#12345", DT_TYPE_DATETIME, 2020, 12, 1, 1, 12, 34, 0);
  STR_TO_OB_TIME_FAIL("201201%01#12345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("201201.01@12!345", DT_TYPE_DATETIME, 2020, 12, 1, 1, 12, 34, 0);
  STR_TO_OB_TIME_FAIL("201201%01@12!345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("2012.01%01@12!34:5", DT_TYPE_DATETIME, 2012, 1, 1, 12, 34, 5, 0);
  STR_TO_OB_TIME_SUCC("2012;01%01@12!34:5", DT_TYPE_DATETIME, 2012, 1, 1, 12, 34, 5, 0);
  STR_TO_OB_TIME_SUCC("201-2-1 1-12-34", DT_TYPE_DATETIME, 201, 2, 1, 1, 12, 34, 0);
  STR_TO_OB_TIME_FAIL("201-0201 1-12-34", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("201. 0201 1-12-34", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("201.2 1 1-12 34", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("123:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12345:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("123456:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234567:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12345678:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("123456789:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234567890:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011131314151617", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011123214151617", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("101.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1011.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("10111.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("101112.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("1011121.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 1, 55, 55, 0);
  STR_TO_OB_TIME_FAIL("10111213.55.55", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("101112131.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 13, 1, 55, 550000);
  STR_TO_OB_TIME_SUCC("1011121314.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 55, 550000);
  STR_TO_OB_TIME_SUCC("10111213141.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 1, 550000);
  STR_TO_OB_TIME_SUCC("101112131415.22.22", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 220000);
  STR_TO_OB_TIME_SUCC("101112131415.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 550000);
  STR_TO_OB_TIME_SUCC("1011121314151.55.55", DT_TYPE_DATETIME, 2010, 11, 12, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("10111213141516.55.55", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 550000);
  STR_TO_OB_TIME_SUCC("101112131415161.55.55", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617.55.55", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516171.55.55", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161718.55.55", DT_TYPE_DATETIME, 1011, 12, 13, 14, 15, 16, 0);
  STR_TO_OB_TIME_FAIL("1", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("123", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12345", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("123456", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1123456", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12+3456", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:3456", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234=56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("1234:56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12!34@56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34@56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12!34:56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34::56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:=56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34+:56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34: 56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34 :56", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56..789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56:.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56.:789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56 .789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:56. 789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("12:34:61.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12:34:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("200 : 59 : 59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("200 :59 :59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("200: 59: 59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11: 12:34:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11. 12:34:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 :12:34:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 .12:34:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12:3456.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12+3456.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 1234:56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 1234=56.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 1234.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12:34.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12.789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12 .789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("11 12. 789", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_SUCC("1000-01-01 00:00:00", DT_TYPE_DATETIME, 1000, 1, 1, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("1969-12-31 23:59:59", DT_TYPE_DATETIME, 1969, 12, 31, 23, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("1970-01-01 00:00:00", DT_TYPE_DATETIME, 1970, 1, 1, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("9999-12-31 23:59:59", DT_TYPE_DATETIME, 9999, 12, 31, 23, 59, 59, 0);
  STR_TO_OB_TIME_FAIL("838:59:59", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("838:59:59.000001", DT_TYPE_DATETIME);
  STR_TO_OB_TIME_FAIL("840:00:00", DT_TYPE_DATETIME);
  // date.
  STR_TO_OB_TIME_SUCC("2015-05-30 11:12:13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2015.05.30 11.12.13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2015/05/30 11/12/13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2015+05=30 11*12&13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2015^05%30 11$12#13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2015@05!30 11~12`13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("0000-05-30 11:12:13.1415", DT_TYPE_DATE, 0, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("00002015-00000005-00000030 11:12:13.1415", DT_TYPE_DATE, 2015, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("00000015-00000005-00000030 11:12:13.1415", DT_TYPE_DATE, 15, 5, 30, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("00000015.00000005.00000030 11:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10000-05-30 11:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10000.05.30 11:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2015-13-30 11:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2015-05-32 11:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2015-05-30 24:12:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:60:13.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:12:60.1415", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("2015-12-31 23:59:59.9999999", DT_TYPE_DATE, 2016, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("10", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("101", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("10111", DT_TYPE_DATE, 2010, 11, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151617", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516171", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("10", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10.1", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10.11", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011.1", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011.12", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("101112.1", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112.13", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.1", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.14", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.1", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.15", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.1", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.16", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.16.17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.16-17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415..16", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415..16.17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415..16-17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.-.16", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.-.16.17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.-.16-17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("101112131415-16", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("101112131415-16.17", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("101112131415-16-17", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("101112131415.161111.17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.1611112.17", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.1", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.17", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151617.1", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151617.18", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161.718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.1718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151.61718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141.5161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.15161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131.415161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.1415161718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121.31415161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112.131415161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111.2131415161718", DT_TYPE_DATE, 2010, 11, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011.12131415161718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101.112131415161718", DT_TYPE_DATE, 2010, 1, 11, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10.1112131415161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10.11121314151617.18", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101.112131415161.718", DT_TYPE_DATE, 2010, 1, 11, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011.1213141516.1718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111.21314151.61718", DT_TYPE_DATE, 2010, 11, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112.131415.161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121.3141.5161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.14.15161718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.1.61718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.16.1718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.161.718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.1.718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.17.18", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.171.8", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10..1112131415161718", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101..112131415161718", DT_TYPE_DATE, 2010, 1, 11, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011..12131415161718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516..1718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161..718", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151617..18", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2012.010112345", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("2012+010112345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("2012.01=0112345", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("2012*01=0112345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("2012.01=011*2345", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("2012!01=011*2345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("2012.*01^011&2345", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("2012*.01^011&2345", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("2012**01^011&2345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("201201.0112345", DT_TYPE_DATE, 2020, 12, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("201201%0112345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("201201.01#12345", DT_TYPE_DATE, 2020, 12, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("201201%01#12345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("201201.01@12!345", DT_TYPE_DATE, 2020, 12, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("201201%01@12!345", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("2012.01%01@12!34:5", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("2012;01%01@12!34:5", DT_TYPE_DATE, 2012, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("201-2-1 1-12-34", DT_TYPE_DATE, 201, 2, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("201-0201 1-12-34", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("201. 0201 1-12-34", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("201.2 1 1-12 34", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("123:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12345:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("123456:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234567:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12345678:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("123456789:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234567890:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011131314151617", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011123214151617", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("101.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1011.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("10111.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("101112.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("1011121.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("10111213.55.55", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("101112131.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.22.22", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151.55.55", DT_TYPE_DATE, 2010, 11, 12, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516.55.55", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161.55.55", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314151617.55.55", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213141516171.55.55", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415161718.55.55", DT_TYPE_DATE, 1011, 12, 13, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("1", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("123", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12345", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("123456", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1123456", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12+3456", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:3456", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234=56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("1234:56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12!34@56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34@56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12!34:56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34::56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:=56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34+:56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34: 56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34 :56", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56..789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56:.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56.:789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56 .789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:56. 789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("12:34:61.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12:34:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("200 : 59 : 59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("200 :59 :59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("200: 59: 59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11: 12:34:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11. 12:34:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 :12:34:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 .12:34:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12:3456.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12+3456.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 1234:56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 1234=56.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 1234.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12:34.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12.789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12 .789", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("11 12. 789", DT_TYPE_DATE);
  STR_TO_OB_TIME_SUCC("1000-01-01 00:00:00", DT_TYPE_DATE, 1000, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1969-12-31 23:59:59", DT_TYPE_DATE, 1969, 12, 31, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1970-01-01 00:00:00", DT_TYPE_DATE, 1970, 1, 1, -1, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("9999-12-31 23:59:59", DT_TYPE_DATE, 9999, 12, 31, -1, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("838:59:59", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("838:59:59.000001", DT_TYPE_DATE);
  STR_TO_OB_TIME_FAIL("840:00:00", DT_TYPE_DATE);
  // time.
  STR_TO_OB_TIME_SUCC("2015-05-30 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015.05.30 11.12.13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015/05/30 11/12/13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015+05=30 11*12&13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015^05%30 11$12#13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("2015@05!30 11~12`13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("0000-05-30 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("00002015-00000005-00000030 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("00000015-00000005-00000030 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 11, 12, 13, 141500);
  STR_TO_OB_TIME_SUCC("00000015.00000005.00000030 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 0, 0, 15, 0);
  STR_TO_OB_TIME_FAIL("10000-05-30 11:12:13.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("10000.05.30 11:12:13.1415", DT_TYPE_TIME, -1, -1, -1, 1, 0, 0, 50000);
  STR_TO_OB_TIME_FAIL("2015-13-30 11:12:13.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("2015-05-32 11:12:13.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 24:12:13.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:60:13.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("2015-05-30 11:12:60.1415", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("2015-12-31 23:59:59.9999999", DT_TYPE_TIME, -1, -1, -1, 0, 0, 0, 000000);
  STR_TO_OB_TIME_SUCC("10", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 0);
  STR_TO_OB_TIME_SUCC("101", DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 0);
  STR_TO_OB_TIME_SUCC("1011", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 0);
  STR_TO_OB_TIME_SUCC("10111", DT_TYPE_TIME, -1, -1, -1, 1, 1, 11, 0);
  STR_TO_OB_TIME_SUCC("101112", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 0);
  STR_TO_OB_TIME_SUCC("1011121", DT_TYPE_TIME, -1, -1, -1, 101, 11, 21, 0);
  STR_TO_OB_TIME_SUCC("10111213", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("10111213141", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("101112131415", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("1011121314151", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("10111213141516", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516171", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161718", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 0);
  STR_TO_OB_TIME_SUCC("10.1", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 100000);
  STR_TO_OB_TIME_SUCC("10.11", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 110000);
  STR_TO_OB_TIME_SUCC("1011.1", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 100000);
  STR_TO_OB_TIME_SUCC("1011.12", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 120000);
  STR_TO_OB_TIME_SUCC("101112.1", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 100000);
  STR_TO_OB_TIME_SUCC("101112.13", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 130000);
  STR_TO_OB_TIME_SUCC("10111213.1", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.14", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.1", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.15", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131415.1", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 100000);
  STR_TO_OB_TIME_SUCC("101112131415.16", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 160000);
  STR_TO_OB_TIME_FAIL("101112131415.16.17", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.16-17", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("101112131415..16", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415..16.17", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415..16-17", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16.17", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.-.16-17", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_FAIL("101112131415-16", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415-16.17", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415-16-17", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.161111.17", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("101112131415.1611112.17", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 161111);
  STR_TO_OB_TIME_SUCC("10111213141516.1", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 100000);
  STR_TO_OB_TIME_SUCC("10111213141516.17", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 170000);
  STR_TO_OB_TIME_SUCC("1011121314151617.1", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617.18", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161.718", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516.1718", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 171800);
  STR_TO_OB_TIME_SUCC("1011121314151.61718", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_SUCC("101112131415.161718", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 161718);
  STR_TO_OB_TIME_SUCC("10111213141.5161718", DT_TYPE_TIME, -1, -1, -1, 13, 14, 1, 516172);
  STR_TO_OB_TIME_SUCC("1011121314.15161718", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131.415161718", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("10111213.1415161718", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121.31415161718", DT_TYPE_TIME, -1, -1, -1, 101, 11, 21, 314152);
  STR_TO_OB_TIME_SUCC("101112.131415161718", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 131415);
  STR_TO_OB_TIME_SUCC("10111.2131415161718", DT_TYPE_TIME, -1, -1, -1, 1, 1, 11, 213142);
  STR_TO_OB_TIME_SUCC("1011.12131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 121314);
  STR_TO_OB_TIME_SUCC("101.112131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 112131);
  STR_TO_OB_TIME_SUCC("10.1112131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 111213);
  STR_TO_OB_TIME_SUCC("10.11121314151617.18", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 111213);
  STR_TO_OB_TIME_SUCC("101.112131415161.718", DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 112131);
  STR_TO_OB_TIME_SUCC("1011.1213141516.1718", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 121314);
  STR_TO_OB_TIME_SUCC("10111.21314151.61718", DT_TYPE_TIME, -1, -1, -1, 1, 1, 11, 213142);
  STR_TO_OB_TIME_SUCC("101112.131415.161718", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 131415);
  STR_TO_OB_TIME_SUCC("1011121.3141.5161718", DT_TYPE_TIME, -1, -1, -1, 101, 11, 21, 314100);
  STR_TO_OB_TIME_SUCC("10111213.14.15161718", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("101112131415.1.61718", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.16.1718", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.161.718", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("10111213141516.1.718", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("10111213141516.17.18", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("10111213141516.171.8", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("10..1112131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 0);
  STR_TO_OB_TIME_SUCC("101..112131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 0);
  STR_TO_OB_TIME_SUCC("1011..12131415161718", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 0);
  STR_TO_OB_TIME_SUCC("10111213141516..1718", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161..718", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617..18", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("2012.010112345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 10112);
  // STR_TO_OB_TIME_SUCC("2012+010112345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("2012.01=0112345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 10000);
  STR_TO_OB_TIME_SUCC("2012*01=0112345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("2012.01=011*2345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 10000);
  STR_TO_OB_TIME_SUCC("2012!01=011*2345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("2012.*01^011&2345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("2012*.01^011&2345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("2012**01^011&2345", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("201201.0112345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 11235);
  //STR_TO_OB_TIME_SUCC("201201%0112345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 0);
  STR_TO_OB_TIME_SUCC("201201.01#12345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 10000);
  STR_TO_OB_TIME_SUCC("201201%01#12345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 0);
  STR_TO_OB_TIME_SUCC("201201.01@12!345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 10000);
  STR_TO_OB_TIME_SUCC("201201%01@12!345", DT_TYPE_TIME, -1, -1, -1, 20, 12, 1, 0);
  STR_TO_OB_TIME_SUCC("2012.01%01@12!34:5", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 10000);
  STR_TO_OB_TIME_SUCC("2012;01%01@12!34:5", DT_TYPE_TIME, -1, -1, -1, 0, 20, 12, 0);
  STR_TO_OB_TIME_SUCC("201-2-1 1-12-34", DT_TYPE_TIME, -1, -1, -1, 1, 12, 34, 0);
  STR_TO_OB_TIME_SUCC("201-0201 1-12-34", DT_TYPE_TIME, -1, -1, -1, 0, 2, 1, 0);
  STR_TO_OB_TIME_SUCC("201. 0201 1-12-34", DT_TYPE_TIME, -1, -1, -1, 0, 2, 1, 0);
  STR_TO_OB_TIME_SUCC("201.2 1 1-12 34", DT_TYPE_TIME, -1, -1, -1, 0, 2, 1, 200000);
  STR_TO_OB_TIME_SUCC("12:59:59", DT_TYPE_TIME, -1, -1, -1, 12, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("123:59:59", DT_TYPE_TIME, -1, -1, -1, 123, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("1234:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("12345:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("123456:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1234567:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("12345678:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("123456789:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1234567890:59:59", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("1011131314151617", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("1011123214151617", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("10.55.55", DT_TYPE_TIME, -1, -1, -1, 0, 0, 10, 550000);
  STR_TO_OB_TIME_SUCC("101.55.55", DT_TYPE_TIME, -1, -1, -1, 0, 1, 1, 550000);
  STR_TO_OB_TIME_SUCC("1011.55.55", DT_TYPE_TIME, -1, -1, -1, 0, 10, 11, 550000);
  STR_TO_OB_TIME_SUCC("10111.55.55", DT_TYPE_TIME, -1, -1, -1, 1, 1, 11, 550000);
  STR_TO_OB_TIME_SUCC("101112.55.55", DT_TYPE_TIME, -1, -1, -1, 10, 11, 12, 550000);
  STR_TO_OB_TIME_SUCC("1011121.55.55", DT_TYPE_TIME, -1, -1, -1, 101, 11, 21, 550000);
  STR_TO_OB_TIME_SUCC("10111213.55.55", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("101112131.55.55", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("1011121314.55.55", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_FAIL("10111213141.55.55", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.22.22", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("101112131415.55.55", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("1011121314151.55.55", DT_TYPE_TIME, -1, -1, -1, 13, 14, 15, 0);
  STR_TO_OB_TIME_FAIL("10111213141516.55.55", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("101112131415161.55.55", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1011121314151617.55.55", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("10111213141516171.55.55", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("101112131415161718.55.55", DT_TYPE_TIME, -1, -1, -1, 14, 15, 16, 0);
  STR_TO_OB_TIME_SUCC("1", DT_TYPE_TIME, -1, -1, -1, 0, 0, 1, 0);
  STR_TO_OB_TIME_SUCC("12", DT_TYPE_TIME, -1, -1, -1, 0, 0, 12, 0);
  STR_TO_OB_TIME_SUCC("123", DT_TYPE_TIME, -1, -1, -1, 0, 1, 23, 0);
  STR_TO_OB_TIME_SUCC("1234", DT_TYPE_TIME, -1, -1, -1, 0, 12, 34, 0);
  STR_TO_OB_TIME_SUCC("12345", DT_TYPE_TIME, -1, -1, -1, 1, 23, 45, 0);
  STR_TO_OB_TIME_SUCC("123456", DT_TYPE_TIME, -1, -1, -1, 12, 34, 56, 0);
  STR_TO_OB_TIME_SUCC("1123456", DT_TYPE_TIME, -1, -1, -1, 112, 34, 56, 0);
  STR_TO_OB_TIME_SUCC("12+3456", DT_TYPE_TIME, -1, -1, -1, 0, 0, 12, 0);
  STR_TO_OB_TIME_FAIL("12:3456", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("1234=56", DT_TYPE_TIME, -1, -1, -1, 0, 12, 34, 0);
  STR_TO_OB_TIME_SUCC("1234:56", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("12!34@56", DT_TYPE_TIME, -1, -1, -1, 0, 0, 12, 0);
  STR_TO_OB_TIME_SUCC("12:34@56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12!34:56", DT_TYPE_TIME, -1, -1, -1, 0, 0, 12, 0);
  STR_TO_OB_TIME_SUCC("12:34:56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 56, 0);
  STR_TO_OB_TIME_SUCC("12:34::56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12:34:=56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12:34+:56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12:34: 56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12:34 :56", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 0);
  STR_TO_OB_TIME_SUCC("12:34:56..789", DT_TYPE_TIME, -1, -1, -1, 12, 34, 56, 0);
  STR_TO_OB_TIME_SUCC("12:34:56:.789", DT_TYPE_TIME, -1, -1, -1, 12, 34, 56, 0);
  STR_TO_OB_TIME_SUCC("12:34:56.:789", DT_TYPE_TIME, -1, -1, -1, 12, 34, 56, 0);
  STR_TO_OB_TIME_FAIL("12:34:56 .789", DT_TYPE_TIME);
  STR_TO_OB_TIME_FAIL("12:34:56. 789", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("12:34.789", DT_TYPE_TIME, -1, -1, -1, 12, 34, 0, 789000);
  STR_TO_OB_TIME_FAIL("12:34:61.789", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("11 12:34:56.789", DT_TYPE_TIME, -1, -1, -1, 276, 34, 56, 789000);
  STR_TO_OB_TIME_SUCC("200 : 59 : 59", DT_TYPE_TIME, -1, -1, -1, 0, 2, 0, 0);
  STR_TO_OB_TIME_SUCC("200 :59 :59", DT_TYPE_TIME, -1, -1, -1, 200, 59, 0, 0);
  STR_TO_OB_TIME_SUCC("200: 59: 59", DT_TYPE_TIME, -1, -1, -1, 0, 2, 0, 0);
  STR_TO_OB_TIME_SUCC("11: 12:34:56.789", DT_TYPE_TIME, -1, -1, -1, 0, 0, 11, 0);
  STR_TO_OB_TIME_SUCC("11. 12:34:56.789", DT_TYPE_TIME, -1, -1, -1, 0, 0, 11, 0);
  STR_TO_OB_TIME_SUCC("11 :12:34:56.789", DT_TYPE_TIME, -1, -1, -1, 11, 12, 34, 0);
//  STR_TO_OB_TIME_SUCC("11 .12:34:56.789", DT_TYPE_TIME, -1, -1, -1, 0, 0, 11, 120000);
  STR_TO_OB_TIME_FAIL("11 12:3456.789", DT_TYPE_TIME);
  STR_TO_OB_TIME_SUCC("11 12+3456.789", DT_TYPE_TIME, -1, -1, -1, 276, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("11 1234:56.789", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("11 1234=56.789", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("11 1234.789", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
  STR_TO_OB_TIME_SUCC("11 12:34.789", DT_TYPE_TIME, -1, -1, -1, 276, 34, 0, 789000);
  STR_TO_OB_TIME_SUCC("11 12.789", DT_TYPE_TIME, -1, -1, -1, 276, 0, 0, 789000);
  STR_TO_OB_TIME_SUCC("11 12 .789", DT_TYPE_TIME, -1, -1, -1, 276, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("11 12. 789", DT_TYPE_TIME, -1, -1, -1, 276, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("1000-01-01 00:00:00", DT_TYPE_TIME, -1, -1, -1, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("1969-12-31 23:59:59", DT_TYPE_TIME, -1, -1, -1, 23, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("1970-01-01 00:00:00", DT_TYPE_TIME, -1, -1, -1, 0, 0, 0, 0);
  STR_TO_OB_TIME_SUCC("9999-12-31 23:59:59", DT_TYPE_TIME, -1, -1, -1, 23, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("838:59:59", DT_TYPE_TIME, -1, -1, -1, 838, 59, 59, 0);
  STR_TO_OB_TIME_SUCC("838:59:59.000001", DT_TYPE_TIME, -1, -1, -1, 838, 59, 59, 1);
  STR_TO_OB_TIME_SUCC("840:00:00", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);
}

//void STR_TO_OB_INTERVAL_SUCC(const char *str, ObDateUnitType unit,
//                             uint32_t year, uint32_t month, uint32_t day,
//                             uint32_t hour, uint32_t minute, uint32_t second, uint32_t usecond)
//{
//  ObString buf;
//  ObInterval ob_interval;
//  buf.assign_ptr(str, static_cast<int32_t>(strlen(str)));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_interval(buf, unit, ob_interval));
//  EXPECT_TRUE(ob_interval_eq(ob_interval, year, month, day, hour, minute, second, usecond));
//}

#define STR_TO_OB_INTERVAL_SUCC(str, unit, year, month, day, hour, minute, second, usecond) \
  do { \
    buf.assign_ptr(str, static_cast<int32_t>(strlen(str))); \
    memset(&ob_interval, 0, sizeof(ob_interval)); \
    EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_interval(buf, unit, ob_interval)); \
    EXPECT_TRUE(ob_interval_eq(ob_interval, year, month, day, hour, minute, second, usecond)); \
  } while (0)

TEST(ObTimeConvertTest, str_to_ob_interval)
{
  ObString buf;
  ObInterval ob_interval;
  // normal case.
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_MICROSECOND, 0, 0, 0, 0, 0, 0, 1);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_SECOND,      0, 0, 0, 0, 0, 1, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_MINUTE,      0, 0, 0, 0, 1, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_HOUR,        0, 0, 0, 1, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_DAY,         0, 0, 1, 0, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_WEEK,        0, 0, 7, 0, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_MONTH,       0, 1, 0, 0, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_QUARTER,     0, 3, 0, 0, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1", DATE_UNIT_YEAR,        1, 0, 0, 0, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1:2", DATE_UNIT_SECOND_MICROSECOND, 0, 0, 0, 0, 0, 1, 200000);
  STR_TO_OB_INTERVAL_SUCC("1-2+3", DATE_UNIT_MINUTE_MICROSECOND, 0, 0, 0, 0, 1, 2, 300000);
  STR_TO_OB_INTERVAL_SUCC("1_2", DATE_UNIT_MINUTE_SECOND, 0, 0, 0, 0, 1, 2, 0);
  STR_TO_OB_INTERVAL_SUCC("1=2(3)4", DATE_UNIT_HOUR_MICROSECOND, 0, 0, 0, 1, 2, 3, 400000);
  STR_TO_OB_INTERVAL_SUCC("1&2*3", DATE_UNIT_HOUR_SECOND, 0, 0, 0, 1, 2, 3, 0);
  STR_TO_OB_INTERVAL_SUCC("1^2", DATE_UNIT_HOUR_MINUTE, 0, 0, 0, 1, 2, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1@2#3$4%5", DATE_UNIT_DAY_MICROSECOND, 0, 0, 1, 2, 3, 4, 500000);
  STR_TO_OB_INTERVAL_SUCC("1|2~3!4", DATE_UNIT_DAY_SECOND, 0, 0, 1, 2, 3, 4, 0);
  STR_TO_OB_INTERVAL_SUCC("1`2;3", DATE_UNIT_DAY_MINUTE, 0, 0, 1, 2, 3, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1?2", DATE_UNIT_DAY_HOUR, 0, 0, 1, 2, 0, 0, 0);
  STR_TO_OB_INTERVAL_SUCC("1/2", DATE_UNIT_YEAR_MONTH, 1, 2, 0, 0, 0, 0, 0);
  // mutli-delimiters or letters or spaces.
  STR_TO_OB_INTERVAL_SUCC("1he ,.2<llo>3{} wo4[rld]5", DATE_UNIT_DAY_MICROSECOND, 0, 0, 1, 2, 3, 4, 500000);
}

//TEST(ObTimeConvertTest, datetime_to_ob_time)
//{
//  ObTimezoneUtils zone;
//  ObTime ob_time;
//
//  zone.init("/usr/share/zoneinfo/America/Chicago");
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 15, 4, 22, 7, 0));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(23190 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 1970, 1, 1, 0, 26, 30, 0));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/Cordoba");
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429090420 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 15, 6, 33, 40, 0));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(39023190 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 1971, 3, 28, 12, 46, 30, 0));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Asia/Tehran");
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429090541 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 15, 14, 5, 41, 0));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(218239892 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 1976, 12, 1, 1, 41, 32, 0));
//
//  zone.parse_timezone_file("/usr/share/zoneinfo/Singapore");
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429090646 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 15, 17, 37, 26, 0));
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1120023938 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2005, 6, 29, 13, 45, 38, 0));
//
//  // test dst time New York
//  zone.parse_timezone_file("/usr/share/zoneinfo/America/New_York");
//  //in dst
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429531489 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 20, 8, 4, 49, 0));
//  // not in dst
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1421341200 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 1, 15, 12, 0, 0, 0));
//
//  // usec < 0
//  zone.parse_timezone_file("/usr/share/zoneinfo/UTC");
//  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(-1 * USECS_PER_SEC, zone.get_tz_ptr(), ob_time));
//  EXPECT_TRUE(ob_time_eq(ob_time, 1969, 12, 31, 23, 59, 59, 0));
//}

TEST(ObTimeConvertTest, date_to_ob_time)
{
  ObTime ob_time;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(0, ob_time));
  EXPECT_TRUE(ob_time_eq(ob_time, 1970, 1, 1, 0, 0, 0, 0));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time));
  EXPECT_TRUE(ob_time_eq(ob_time, 2015, 4, 15, 0, 0, 0, 0));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(-1, ob_time));
  EXPECT_TRUE(ob_time_eq(ob_time, 1969, 12, 31, 0, 0, 0, 0));
}

TEST(ObTimeConvertTest, time_to_ob_time)
{
  ObTime ob_time;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time));
  EXPECT_TRUE(ob_time_eq(ob_time, 0, 0, 0, 12, 5, 9, 1234));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(442859 * static_cast<int64_t>(USECS_PER_SEC), ob_time));
  EXPECT_TRUE(ob_time_eq(ob_time, 0, 0, 0, 123, 0, 59, 0));
  // TODO: -123:34:45
}

bool varify_interval_encode_decode(const ObIntervalYMValue ori_value, const ObScale ori_scale)
{
  static const int64_t buf_len = 256;
  static char buf[buf_len];
  memset(buf, 0, sizeof(buf));
  int64_t pos = 0;

  ObIntervalYMValue res_value;
  ObScale res_scale;

  bool ret_b = (OB_SUCCESS == ObTimeConverter::encode_interval_ym(buf, buf_len, pos, ori_value, ori_scale))
              && (OB_SUCCESS == ObTimeConverter::decode_interval_ym(buf, pos, res_value, res_scale))
              && (ori_value == res_value)
              && (ori_scale == res_scale);
  if (!ret_b) {
    printf("Diff: %ld %d, %ld %d\n", res_value.get_nmonth(), res_scale, ori_value.get_nmonth(), ori_scale);
  }
  return ret_b;

}

bool varify_interval_encode_decode(const ObIntervalDSValue ori_value, const ObScale ori_scale)
{
  static const int64_t buf_len = 256;
  static char buf[buf_len];
  memset(buf, 0, sizeof(buf));
  int64_t pos = 0;

  ObIntervalDSValue res_value;
  ObScale res_scale;

  bool ret_b = (OB_SUCCESS == ObTimeConverter::encode_interval_ds(buf, buf_len, pos, ori_value, ori_scale))
              && (OB_SUCCESS == ObTimeConverter::decode_interval_ds(buf, pos, res_value, res_scale))
              && (ori_value == res_value)
              && (ori_scale == res_scale);
  if (!ret_b) {
    printf("Diff: %ld %d %d, %ld %d %d\n", res_value.get_nsecond(), res_value.get_fs(), res_scale,
           ori_value.get_nsecond(), ori_value.get_fs(), ori_scale);
  }
  return ret_b;
}

TEST(ObTimeConvertTest, interval)
{

  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 0, 0), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 0, 11), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 9, 2), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(1)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 99, 3), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(2)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 999, 4), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(3)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 9999, 5), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(4)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 99999, 6), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(5)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 999999, 7), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(6)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 9999999, 8), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(7)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 99999999, 9), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(8)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 999999999, 10), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(9)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(false, 999999999, 11), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(9)));


  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 0, 11), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 9, 2), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(1)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 99, 3), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(2)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 999, 4), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(3)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 9999, 5), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(4)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 99999, 6), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(5)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 999999, 7), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(6)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 9999999, 8), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(7)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 99999999, 9), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(8)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 999999999, 10), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(9)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalYMValue(true, 999999999, 11), ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(9)));

  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 0, 0, 0, 0, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(0, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 0, 23, 59, 59, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(0, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 9, 1, 1, 1, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(1, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 99, 2, 2, 2, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(2, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 999, 3, 3, 3, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 9999, 4, 4, 4, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(4, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 99999, 5, 5, 5, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(5, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 999999, 6, 6, 6, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(6, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 9999999, 7, 7, 7, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(7, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 99999999, 8, 8, 8, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(8, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 999999999, 9, 9, 9, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(9, 0)));

  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 0, 0, 0, 0, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(0, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 0, 23, 59, 59, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(0, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 9, 1, 1, 1, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(1, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 99, 2, 2, 2, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(2, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 999, 3, 3, 3, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 9999, 4, 4, 4, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(4, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 99999, 5, 5, 5, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(5, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 999999, 6, 6, 6, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(6, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 9999999, 7, 7, 7, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(7, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 99999999, 8, 8, 8, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(8, 0)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(true, 999999999, 9, 9, 9, 0), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(9, 0)));

  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 9), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 1)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 99), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 2)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 3)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 9999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 4)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 99999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 5)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 999999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 6)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 9999999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 7)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 99999999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 8)));
  EXPECT_TRUE(varify_interval_encode_decode(ObIntervalDSValue(false, 123, 23, 59, 59, 999999999), ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(3, 9)));

}

TEST(ObTimeConvertTest, scn_to_str)
{
  // timezone with offset only.
  ObString tz_str;
  ObTimeZoneInfo tz_info;
  char tz_buf[50] = {0};
  tz_str.assign_buffer(tz_buf, 50);
  strcpy(tz_buf, "+8:00");
  tz_str.set_length(static_cast<int32_t>(strlen(tz_buf)));
  tz_info.set_timezone(tz_str);

  const int64_t BUF_LEN = 100;
  char buf[BUF_LEN] = {0};
  int64_t pos = 0;
  uint64_t scn_val = 9223372036854775808UL;

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTimeConverter::scn_to_str(scn_val, NULL, buf, BUF_LEN, pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTimeConverter::scn_to_str(scn_val, &tz_info, NULL, BUF_LEN, pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTimeConverter::scn_to_str(scn_val, &tz_info, buf, 0, pos));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::scn_to_str(scn_val, &tz_info, buf, BUF_LEN, pos));
  OB_LOG(INFO, "YYY +8:00", K(scn_val), K(tz_buf), K(buf));

  pos= 0;
  scn_val = 1687780338123456789;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::scn_to_str(scn_val, &tz_info, buf, BUF_LEN, pos));
  OB_LOG(INFO, "YYY +8:00", K(scn_val), K(tz_buf), K(buf));
  ASSERT_TRUE(0 == strcmp(buf, "2023-06-26 19:52:18.123456789"));


  strcpy(tz_buf, "-08:00");
  tz_str.assign(tz_buf, static_cast<int32_t>(strlen(tz_buf)));
  tz_info.set_timezone(tz_str);

  pos= 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::scn_to_str(scn_val, &tz_info, buf, BUF_LEN, pos));
  OB_LOG(INFO, "YYY -08:00", K(scn_val), K(tz_buf), K(buf));
  ASSERT_TRUE(0 == strcmp(buf, "2023-06-26 03:52:18.123456789"));

}

int main(int argc, char **argv)
{
  system("rm -f test_time_convert.log");
  OB_LOGGER.set_file_name("test_time_convert.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
