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

#define USING_LOG_PREFIX LIB_TIME
#include "share/ob_time_utility2.h"
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"

#define IS_MINUS(str, is_minus) \
  if (OB_SUCC(ret)) \
  { \
    const char *ptr = str.ptr(); \
    const char *end_ptr = str.ptr() + str.length(); \
    while ((ptr < end_ptr) && (*ptr < '0' || *ptr > '9')) \
    { \
      if ('-' == *ptr) \
      { \
        is_minus = true; \
      } \
      ptr++; \
    } \
  }

namespace oceanbase
{
using namespace common;
namespace share
{
const char *ObTimeUtility2::mday_name_[31] =
{
  "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
  "11th", "12th", "13th", "14th", "15th", "16th", "17th", "18th", "19th", "20th",
  "21st", "22nd", "23rd", "24th", "25th", "26th", "27th", "28th", "29th", "30th",
  "31st"
};

const char *ObTimeUtility2::weekday_name_[7] =
{
  "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
};

const char *ObTimeUtility2::weekday_abbr_name_[7] =
{
  "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
};

const char *ObTimeUtility2::month_abbr_name_[12] =
{
  "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};

const char *ObTimeUtility2::month_name_[12] =
{
  "January", "February", "March", "April", "May", "June", "July", "August", "September",
  "October", "November", "December"
};

const char *ObTimeUtility2::STD_TS_FORMAT_WITH_USEC = "%Y-%m-%d %H:%i:%s.%f";

const char *ObTimeUtility2::STD_TS_FORMAT_WITHOUT_USEC = "%Y-%m-%d %H:%i:%s";

int ObTimeUtility2::make_second(struct tm &t, time_t &second)
{
  int ret = OB_SUCCESS;
  time_t s = 0;

  t.tm_isdst = -1; //let system to determine whether DST is effect
  if (-1 == (s = mktime(&t))) {
    struct tm tm_cmp;
    time_t t_cmp = -1;

    if ((NULL != localtime_r(&t_cmp, &tm_cmp))
        && (t.tm_sec == tm_cmp.tm_sec && t.tm_min == tm_cmp.tm_min
            && t.tm_hour == tm_cmp.tm_hour && t.tm_mday == tm_cmp.tm_mday
            && t.tm_mon == tm_cmp.tm_mon && t.tm_year == tm_cmp.tm_year)) {
      //1970-01-01 07:59:59 in +08:00 timezone
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERR_SYS;
      LOG_ERROR("make time failed", K(errno), KERRMSG,
                K(t.tm_sec), K(t.tm_min), K(t.tm_hour), K(t.tm_mday), K(t.tm_mon), K(t.tm_year));
    }
  }
  if (OB_SUCC(ret)) {
    second = s;
  }
  return ret;
}

int ObTimeUtility2::timestamp_to_usec(struct tm &base_tm, int64_t base_usec, int64_t &result_usec)
{
  int ret = OB_SUCCESS;
  int64_t second = 0;
  if (OB_FAIL(make_second(base_tm, second))) {
    LOG_WARN("call make second failed", K(ret));
  } else {
    result_usec = second * 1000L * 1000L + base_usec;
  }
  return ret;
}


time_t ObTimeUtility2::extract_second(int64_t usec)
{
  return usec >= 0 ? static_cast<time_t>(usec / 1000000L) : static_cast<time_t>(usec / 1000000L - 1);
}

int ObTimeUtility2::usec_format_to_str(int64_t usec, const ObString &format, char *buf,
                                      int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  struct tm t;
  int64_t second = 0;
  int64_t incre_usec = 0;

  second = usec / (1000L * 1000L);
  incre_usec = usec % (1000L * 1000L);
  if (incre_usec < 0) {
    incre_usec += 1000L * 1000L;
    second -= 1;
  }
  memset(&t, 0, sizeof(struct tm));
  t.tm_isdst = -1;
  if (NULL == localtime_r(&second, &t)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("convert second to struct tm failed", K(second));
  } else if (OB_FAIL(timestamp_format_to_str(t, incre_usec, format, buf, buf_len, pos))) {
    LOG_WARN("format date failed", K(ret));
  }
  return ret;
}

int ObTimeUtility2::usec_to_str(int64_t usec, char *buf, int64_t buf_len, int64_t &pos)
{
  const char *format = NULL;
  if (usec % 1000000 != 0) {
    format = STD_TS_FORMAT_WITH_USEC;
  } else {
    format = STD_TS_FORMAT_WITHOUT_USEC;
  }
  return usec_format_to_str(usec, ObString(format), buf, buf_len, pos);
}

int ObTimeUtility2::timestamp_format_to_str(const struct tm &t, int64_t usec, const ObString &format,
                                           char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(format.ptr())
      || OB_UNLIKELY(format.length() <= 0)
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!is_valid_date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday))
      || OB_UNLIKELY(!is_valid_time(t.tm_hour, t.tm_min, t.tm_sec, static_cast<int>(usec)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(format), K(buf), K(buf_len), K(t.tm_year), K(t.tm_mon),
             K(t.tm_mday), K(t.tm_hour), K(t.tm_min), K(t.tm_sec), K(usec));
  } else if (pos >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    //size overflow isn't an error, so don't print warnings
  } else {
    const char *format_ptr = format.ptr();
    const char *end_ptr = format.ptr() + format.length();
    while (format_ptr < end_ptr && OB_SUCC(ret)) {
      if ('%' == *format_ptr) {
        ++format_ptr;
        if (format_ptr >= end_ptr) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("format string is invalid", K(format));
          break;
        }
        switch (*format_ptr) {
          case 'a': { //Abbreviated weekday name (Sun..Sat)
            ret = databuff_printf(buf, buf_len, pos, "%s", weekday_abbr_name_[t.tm_wday]);
            break;
          }
          case 'b': { //Abbreviated month name (Jan..Dec)
            ret = databuff_printf(buf, buf_len, pos, "%s", month_abbr_name_[t.tm_mon]);
            break;
          }
          case 'c': { //Month, numeric (0..12)
            ret = databuff_printf(buf, buf_len, pos, "%d", t.tm_mon + 1);
            break;
          }
          case 'D': { //Day of the month with English suffix (0th, 1st, 2nd, 3rd, ��)
            ret = databuff_printf(buf, buf_len, pos, "%s", mday_name_[t.tm_mday - 1]);
            break;
          }
          case 'd': { //Day of the month, numeric (00..31)
            ret = databuff_printf(buf, buf_len, pos, "%02d", t.tm_mday);
            break;
          }
          case 'e': { //Day of the month, numeric (0..31)
            ret = databuff_printf(buf, buf_len, pos, "%d", t.tm_mday);
            break;
          }
          case 'f': { //Microseconds (000000..999999)
            ret = databuff_printf(buf, buf_len, pos, "%06ld", usec);
            break;
          }
          case 'H': { //Hour (00..23)
            ret = databuff_printf(buf, buf_len, pos, "%02d", t.tm_hour);
            break;
          }
          case 'h': //Hour (01..12)
          case 'I': { //Hour (01..12)
            int hour = 0;
            if (0 == t.tm_hour) {
              hour = 12; //AM 12 clock
            } else if (t.tm_hour > 12) {
              hour = t.tm_hour - 12;
            } else {
              hour = t.tm_hour;
            }
            ret = databuff_printf(buf, buf_len, pos, "%02d", hour);
            break;
          }
          case 'i': { //Minutes, numeric (00..59)
            ret = databuff_printf(buf, buf_len, pos, "%02d", t.tm_min);
            break;
          }
          case 'j': { //Day of year (001..366)
            ret = databuff_printf(buf, buf_len, pos, "%03d", t.tm_yday + 1);
            break;
          }
          case 'k': { //Hour (0..23)
            ret = databuff_printf(buf, buf_len, pos, "%d", t.tm_hour);
            break;
          }
          case 'l': { //Hour (1..12)
            int hour = 0;
            if (t.tm_hour == 0) {
              hour = 12; //AM 12 clock
            } else if (t.tm_hour > 12) {
              hour = t.tm_hour - 12; //PM
            } else {
              hour = t.tm_hour;
            }
            ret = databuff_printf(buf, buf_len, pos, "%d", hour);
            break;
          }
          case 'M': { //Month name (January..December)
            ret = databuff_printf(buf, buf_len, pos, "%s", month_name_[t.tm_mon]);
            break;
          }
          case 'm': { //Month, numeric (00..12)
            ret = databuff_printf(buf, buf_len, pos, "%02d", t.tm_mon + 1);
            break;
          }
          case 'p': { //AM or PM
            const char *ptr = t.tm_hour >= 12 ? "PM" : "AM";
            if (t.tm_hour >= 0 && t.tm_hour < 12) {
              ptr = "AM"; //AM 12 clock - 11 clock
            } else {
              ptr = "PM";
            }
            ret = databuff_printf(buf, buf_len, pos, "%s", ptr);
            break;
          }
          case 'r': { //Time, 12-hour (hh:mm:ss followed by AM or PM)
            int hour = t.tm_hour % 12;
            const char *ptr = t.tm_hour >= 12 ? "PM" : "AM";
            ret = databuff_printf(buf, buf_len, pos, "%02d:%02d:%02d %s", hour, t.tm_min, t.tm_sec, ptr);
            break;
          }
          case 'S': //Seconds (00..60)
          case 's': { //Seconds (00..60) (1 leap second)
            ret = databuff_printf(buf, buf_len, pos, "%02d", t.tm_sec);
            break;
          }
          case 'T': { //Time, 24-hour (hh:mm:ss)
            ret = databuff_printf(buf, buf_len, pos, "%02d:%02d:%02d", t.tm_hour, t.tm_min, t.tm_sec);
            break;
          }
          case 'U': { //Week (00..53), where Sunday is the first day of the week
            struct tm tmp = t;
            uint8_t flag_mask = START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY;
            ret = databuff_printf(buf, buf_len, pos, "%02d", get_weeks_of_year(tmp, flag_mask));
            break;
          }
          case 'u': { //Week (00..53), where Monday is the first day of the week
            struct tm tmp = t;
            uint8_t flag_mask = 0;
            ret = databuff_printf(buf, buf_len, pos, "%02d", get_weeks_of_year(tmp, flag_mask));
            break;
          }
          case 'V': { //Week (01..53), where Sunday is the first day of the week; used with %X
            struct tm tmp = t;
            uint8_t flag_mask = START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK;
            ret = databuff_printf(buf, buf_len, pos, "%02d", get_weeks_of_year(tmp, flag_mask));
            break;
          }
          case 'v': { //Week (01..53), where Monday is the first day of the week; used with %x
            struct tm tmp = t;
            uint8_t flag_mask = INCLUDE_CRITICAL_WEEK;
            ret = databuff_printf(buf, buf_len, pos, "%02d", get_weeks_of_year(tmp, flag_mask));
            break;
          }
          case 'W': { //Weekday name (Sunday..Saturday)
            ret = databuff_printf(buf, buf_len, pos, "%s", weekday_name_[t.tm_wday]);
            break;
          }
          case 'w': { //Day of the week (0=Sunday..6=Saturday)
            ret = databuff_printf(buf, buf_len, pos, "%d", t.tm_wday);
            break;
          }
          case 'X': { //Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
            struct tm tmp = t;
            uint8_t flag_mask = START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK;
            get_weeks_of_year(tmp, flag_mask);
            ret = databuff_printf(buf, buf_len, pos, "%04d", tmp.tm_year + 1900);
            break;
          }
          case 'x': { //Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
            struct tm tmp = t;
            uint8_t flag_mask = INCLUDE_CRITICAL_WEEK;
            get_weeks_of_year(tmp, flag_mask);
            ret = databuff_printf(buf, buf_len, pos, "%04d", tmp.tm_year + 1900);
            break;
          }
          case 'Y': { //Year, numeric, four digits
            ret = databuff_printf(buf, buf_len, pos, "%04d", t.tm_year + 1900);
            break;
          }
          case 'y': { //Year, numeric (two digits)
            int year = (t.tm_year + 1900) % 100;
            ret = databuff_printf(buf, buf_len, pos, "%02d", year);
            break;
          }
          case '%': { //A literal "%" character
            if (pos >= buf_len) {
              ret = OB_SIZE_OVERFLOW;
              break;
            }
            buf[pos++] = '%';
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          format_ptr++;
        }
      } else if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        break;
      } else {
        buf[pos++] = *(format_ptr++);
      }
    }
  }
  return ret;
}

int ObTimeUtility2::get_start_weekday(int year_day, int week_day, bool week_first_weekday)
{
  int start_weekday = 0;

  start_weekday = (year_day - week_day + 1) % 7;
  if (start_weekday <= 0) {
    start_weekday += 7;
  }

  //if use ISO 8601:1988 protocol, need to calculate the critical week belong to which year
  if (!week_first_weekday) {
    if (start_weekday >= 5 && start_weekday <= 7) {
      start_weekday = start_weekday - 7;
    }
  }
  return start_weekday;
}

bool ObTimeUtility2::check_in_next_critical_week(int yday, int wday, int year, uint8_t flag_mask)
{
  bool bret = false;
  bool week_first_weekday = flag_mask & WEEK_FIRST_WEEKDAY;
  bool include_critical_week = flag_mask & INCLUDE_CRITICAL_WEEK;
  int max_day = is_leap_year(year) ? 366 : 365;

  if (include_critical_week && !week_first_weekday && yday > max_day - 3) {
    int week_first_day = yday - wday + 1;
    if (max_day - week_first_day + 1 <= 3) {
      bret = true;
    }
  }
  return bret;
}

int ObTimeUtility2::get_weeks_of_year(struct tm &t, uint8_t flag_mask)
{
  bool start_with_sunday = false;
  bool week_first_weekday = false;
  bool include_critical_week = false;
  int start_day = 0;
  int today = 0;
  int wday = 0;
  int week_count = 0;

  start_with_sunday = flag_mask & START_WITH_SUNDAY;
  week_first_weekday = flag_mask & WEEK_FIRST_WEEKDAY;
  include_critical_week = flag_mask & INCLUDE_CRITICAL_WEEK;

  today = t.tm_yday + 1;
  //calculate the specified date in which day of the week
  if (start_with_sunday) {
    wday = t.tm_wday + 1;
  } else {
    if (0 == t.tm_wday) {
      //t.tm_wday means sunday, if the week doesn't start with sunday, sunday is the last day
      wday = 7;
    } else {
      wday = t.tm_wday;
    }
  }
  if (check_in_next_critical_week(today, wday, t.tm_year + 1900, flag_mask)) {
    t.tm_year += 1;
    week_count = 1;
  } else {
    start_day = get_start_weekday(today, wday, week_first_weekday);
    if (today < start_day && include_critical_week) {
      t.tm_year -= 1;
      today += is_leap_year(t.tm_year + 1900) ? 366 : 365;
      start_day = get_start_weekday(today, wday, week_first_weekday);
      week_count = (today - start_day) / 7 + 1;
    } else if (today < start_day && !include_critical_week) {
      week_count = 0;
    } else {
      week_count = (today - start_day) / 7 + 1;
    }
  }
  return week_count;
}

int ObTimeUtility2::extract_usec(const ObString &str, int64_t &pos, int64_t &usec, DecimalDigts num_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)
      || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= str.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str), K(pos));
  } else {
    const char *cur_ptr = str.ptr() + pos;
    const char *end_ptr = str.ptr() + str.length();
    int scanned = 0;
    int64_t result = 0;
    int64_t cur_value = 0;

    //skip non-numeric character
    while (cur_ptr < end_ptr && (*cur_ptr > '9' || *cur_ptr < '0')) {
      ++cur_ptr;
    }
    if (cur_ptr >= end_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current ptr is invalid", K(cur_ptr), K(end_ptr));
    } else {
      while (cur_ptr < end_ptr && scanned < 6 && *cur_ptr <= '9' && *cur_ptr >= '0') {
        cur_value = *cur_ptr - '0';
        result = result * 10L + cur_value;
        ++scanned;
        ++cur_ptr;
      }
      if (num_flag == DIGTS_SENSITIVE && 6 == scanned && cur_ptr < end_ptr) {
         
ret = OB_INVALID_ARGUMENT;
         LOG_USER_ERROR(OB_INVALID_ARGUMENT, "extract usec from string");
      } else if (scanned < 6) {
        for (int i = scanned; i < 6; i ++) {
          result *= 10L;
        }
      }
      pos = cur_ptr - str.ptr();
      usec = result;
    }
  }
  return ret;
}

int ObTimeUtility2::extract_date(const ObString &str, int n, int64_t &pos, int64_t &value)
{
  return extract_int(str, n, pos, value);
}

int ObTimeUtility2::str_to_time(const ObString &date, int64_t &usec, DecimalDigts num_flag)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t sec = 0;
  int64_t tmp_usec = 0;
  if (OB_FAIL(extract_int(date, 13, pos, sec))) {
    LOG_WARN("extract second failed", K(ret));
  } else if (OB_ISNULL(date.ptr()) || OB_UNLIKELY(date.length() <= 0) || OB_UNLIKELY(pos >= date.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("date string is invalid", K(date), K(pos));
  } else {
    usec = sec * 1000000L;
    const char *ptr = date.ptr();
    //search for '.'
    while (pos < date.length() && ptr[pos] != '.') {
      ++pos;
    }
    if (pos < date.length()) {
      ++pos; //skip '.'
      if (OB_FAIL(extract_usec(date, pos, tmp_usec, num_flag))) {
        LOG_WARN("extract usec part failed", K(ret));
      } else {
        usec += tmp_usec;
      }
    }
  }
  return ret;
}

int ObTimeUtility2::str_to_timestamp(const ObString &date, struct tm &t, int64_t &usec)
{
  int64_t dates[7] = {0};
  int matched = 0;
  int64_t pos = 0;
  bool is_minus = false;
  int ret = OB_SUCCESS;

  usec = 0;
  //matching relaxed date format and strict date format
  //such as 'ww 2013 ***11%%%^^^09>>>08:45&&03?678'
  //and '2013-11-09 08:45:03.678'
  //match year value
  if (OB_ISNULL(date.ptr()) || OB_UNLIKELY(date.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date string is invalid", K(date));
  }
  IS_MINUS(date, is_minus);
  for (int i = 0; OB_SUCC(ret) && i < 7; i++) {
    if (OB_UNLIKELY(6 == matched)) {
      //to match usec
      if (OB_FAIL(ObTimeUtility2::extract_usec(date, pos, dates[i], DIGTS_INSENSITIVE))) {
        LOG_WARN("extract usec with digts insensitive failed", K(ret));
      }
    } else {
      if (OB_FAIL(ObTimeUtility2::extract_date(date, 0, pos, dates[i]))) {
        LOG_WARN("extract date failed", K(ret));
      }
    }
    ++matched;
  }

  if (OB_SUCC(ret)) {
    if (is_minus) {
      dates[0] = -dates[0];
    }
    if (OB_UNLIKELY(matched < 3)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("year, month, day is necessary in date format", K(date));
    }
    //check the date value whether valid
    else if (!is_valid_date(static_cast<int>(dates[0]), static_cast<int>(dates[1]),
                            static_cast<int>(dates[2]))
             || !is_valid_time(static_cast<int>(dates[3]), static_cast<int>(dates[4]),
                               static_cast<int>(dates[5]),
                               static_cast<int>(dates[6]))) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("the date format is invalid");
    } else {
      //not use fuzzy year rule making by MySQL
      /*
      //if year value is fuzzy, we use this rule:
      //[00, 69] treat as [2000, 2069]
      //[70, 99] treat as [1970, 1999]
      if (dates[0] >= 0 && dates[0] <= 69)
      {
        dates[0] += 2000;
      }
      else if (dates[0] >= 70 && dates[0] <= 99)
      {
        dates[0] += 1900;
      }*/
      t.tm_sec = static_cast<int>(dates[5]);
      t.tm_min = static_cast<int>(dates[4]);
      t.tm_hour = static_cast<int>(dates[3]);
      t.tm_mday = static_cast<int>(dates[2]);
      t.tm_mon = static_cast<int>(dates[1]) - 1;
      t.tm_year = static_cast<int>(dates[0]) - 1900;
      usec = dates[6];
    }
  }
  return ret;
}

int ObTimeUtility2::str_to_usec(const ObString &date, int64_t &usec)
{
  int ret = OB_SUCCESS;
  struct tm t;
  int64_t sec = 0;
  int64_t tmp_usec = 0;

  memset(&t, 0, sizeof(struct tm));
  t.tm_isdst = -1;
  if (OB_FAIL(str_to_timestamp(date, t, tmp_usec))) {
    LOG_WARN("parse string to date failed", K(ret));
  } else if (OB_FAIL(make_second(t, sec))) {
    LOG_WARN("parse time to usec failed", K(ret));
  } else {
    usec = sec * 1000L * 1000L + tmp_usec;
  }
  return ret;
}

//TODO:: not support Julian day now. @yanhua
bool ObTimeUtility2::is_valid_year(int year)
{
  //(0, 9999]
  bool ret = true;
  if (year <= 0 || year > 9999) {
    ret = false;
    //let user catch the error
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "year", year, 1, 9999);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_month(int month)
{
  bool ret = true;
  if (month < 1 || month > 12) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "month", month, 1, 12);
  }
  return ret;
}

bool ObTimeUtility2::is_leap_year(int year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

bool ObTimeUtility2::is_valid_mday(int year, int month, int mday)
{
  bool ret = true;

  switch (month) {
    case 1:
    case 3:
    case 5:
    case 7:
    case 8:
    case 10:
    case 12: {
      if (mday > 31 || mday < 1) {
        ret = false;
        LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "mday", mday, 1, 31);
      }
      break;
    }
    case 4:
    case 6:
    case 9:
    case 11: {
      if (mday > 30 || mday < 1) {
        ret = false;
        LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "mday", mday, 1, 30);
      }
      break;
    }
    case 2: {
      if (is_leap_year(year)) {
        if (mday > 29 || mday < 1) {
          ret = false;
          LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "mday", mday, 1, 29);
        }
      } else {
        if (mday > 28 || mday < 1) {
          ret = false;
          LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "mday", mday, 1, 28);
        }
      }
      break;
    }
    default: {
      ret = false;
      LOG_WARN("month must between 1 and 12", K(month));
      break;
    }
  }
  return ret;
}

bool ObTimeUtility2::is_valid_hour(int hour)
{
  bool ret = true;
  if (hour > 23 || hour < 0) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "hour", hour, 0, 23);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_hour_offset(int hour)
{
  bool ret = true;
  if (hour > 14 || hour < -12) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "hour", hour, -12, 14);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_minute(int minute)
{
  bool ret = true;
  if (minute > 59 || minute < 0) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "minute", minute, 0, 59);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_second(int second)
{
  bool ret = true;
  if (second > 59 || second < 0) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "second", second, 0, 59);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_usec(int usec)
{
  bool ret = true;
  if (usec > 999999 || usec < 0) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "usec", usec, 0, 999999);
  }
  return ret;
}

bool ObTimeUtility2::is_valid_nsec(int nsec)
{
  bool ret = true;
  if (nsec > 999999999 || nsec < 0) {
    ret = false;
    LOG_USER_ERROR(OB_INVALID_DATE_FORMAT, "nsec", nsec, 0, 999999999);
  }
  return ret;
}
bool ObTimeUtility2::is_valid_date(int year, int month, int mday)
{
  return is_valid_year(year) && is_valid_month(month) && is_valid_mday(year, month, mday);
}

bool ObTimeUtility2::is_valid_time(int hour, int minute, int second, int usec)
{
  return is_valid_hour(hour) && is_valid_minute(minute) && is_valid_second(second) &&
         is_valid_usec(usec);
}

bool ObTimeUtility2::is_valid_oracle_time(int hour, int minute, int second, int nsec)
{
  return (is_valid_hour(hour)
          && is_valid_minute(minute)
          && is_valid_second(second)
          && is_valid_nsec(nsec));
}

bool ObTimeUtility2::is_valid_time_offset(int offset_hour, int offset_minute)
{
  return (is_valid_hour_offset(offset_hour)
          && is_valid_minute(offset_minute));
}


}  // namespace share
}  // namespace oceanbase
