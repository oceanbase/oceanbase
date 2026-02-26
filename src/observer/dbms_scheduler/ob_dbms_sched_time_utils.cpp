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

#include "ob_dbms_sched_time_utils.h"
#include "ob_dbms_sched_job_utils.h"
#include "parser/dbms_sched_calendar_parser.h"
#include "pl/ob_pl_resolver.h"
#include "lib/utility/ob_print_utils.h"
#include <time.h>

namespace oceanbase
{
using namespace common;
using namespace pl;

namespace dbms_scheduler
{


int get_days_in_month(int year, int month) {
  int days_in_month;
  if (month < 1 || month > 12) {
    days_in_month = 0;
  } else {
    switch (month) {
      case 1: case 3: case 5: case 7: case 8: case 10: case 12: // 31天的月份
        days_in_month = 31;
        break;
      case 4: case 6: case 9: case 11: // 30天的月份
        days_in_month = 30;
        break;
      case 2: // 2月
        // 判断是否闰年
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
          days_in_month = 29; // 闰年
        } else {
          days_in_month = 28; // 非闰年
        }
        break;
      default:
        days_in_month = 0;
    }
  }
  return days_in_month;
}

// 判断日期有效性的函数
bool is_valid_date(int year, int month, int day) {
  bool is_valid = true;
  // 检查月份范围
  if (month < 1 || month > 12) {
    is_valid = false;
  } else {
    // 检查每个月的天数
    int days_in_month = get_days_in_month(year, month);
    // 检查日期是否在合法范围内
    if (day < 1 || day > days_in_month) {
      is_valid = false;
    }
  }
  return is_valid;
}

//bymonthday值转换到bitset，1~31对应正数日期，32~62对应负数日期-1~-31
int value_to_month_day_bitset(int value)
{
  return value > 0 ? value : 31 - value;
}

//monthday转换到负数日期对应的bitset，如7月30号，对应的负数日期是-2，bitset是33
int month_day_to_bitset(int year, int month, int day)
{
  return 31 - (day - (get_days_in_month(year, month) + 1));
}

int ObDBMSSchedTimeUtil::check_is_valid_repeat_interval(const int64_t tenant_id, const ObString &repeat_interval, bool is_limit_interval_num)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  reset();
  OZ (resolve_repeat_interval(repeat_interval));
  if (is_limit_interval_num) {
    if (0 >= interval_num_) { ret = OB_NOT_SUPPORTED; }
    else if (8000 <= interval_num_) { ret = OB_INVALID_ARGUMENT_NUM; }
  }
  OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
  if (OB_SUCC(ret) && has_by_clause_ && !DATA_VERSION_SUPPORT_CALENDAR(data_version)) {
    ret = OB_NOT_SUPPORTED;
    ObString err_info("repeat_interval contains byclause is not support when upgrade");
    LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
  }
  int64_t tmp_next_time = 0;
  int64_t tmp_start_time = 0;
  const int64_t now = ObTimeUtility::current_time();
  OZ (calc_expr(tmp_start_time, tmp_next_time, now, now));
  return ret;
}

int ObDBMSSchedTimeUtil::calc_repeat_interval_next_date(const ObString &repeat_interval, int64_t start_date, int64_t &next_date, int64_t base_date, int64_t return_date_after, bool is_first_time)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  reset();
  is_first_time_ = is_first_time;
  OZ (resolve_repeat_interval(repeat_interval));
  base_date = (base_date == 0 ? now : base_date);
  return_date_after = (return_date_after == 0 ? base_date : return_date_after);
  OZ (calc_expr(start_date, next_date, base_date, return_date_after));
  return ret;
}

void ObDBMSSchedTimeUtil::reset()
{
  freq_type_ = UNKNOWN_FREQ;
  interval_num_ = 0;
  has_by_clause_ = false;
  is_first_time_ = false;
  interval_type_bitset_.reset();
  month_bitset_.reset();
  weekno_bitset_.reset();
  month_day_bitset_.reset();
  year_day_bitset_.reset();
  day_bitset_.reset();
  hour_bitset_.reset();
  minute_bitset_.reset();
  second_bitset_.reset();
}

int ObDBMSSchedTimeUtil::resolve_repeat_interval(const ObString &repeat_interval)
{
  int ret = OB_SUCCESS;
  ObStmtNodeTree *parse_tree = NULL;
  ParseNode *calendar_node = NULL;
  ParseNode *freq_node = NULL;
  ParseNode *interval_node = NULL;
  ParseNode *interval_by_list_node = NULL;
  ParseNode *interval_by_value_node = NULL;
  CALENDAR_INTERVAL_TYPE interval_by_type = INTERVAL;
  ObArenaAllocator allocator("sched_calendar");
  ObDBMSSchedCalendarParser parser(allocator, ObCharsets4Parser());

/*
  frequency_clause
  frequency_clause ; interval_clause
  frequency_clause ; interval_clause ; by_clause_list
  frequency_clause ; by_clause_list
*/

  OZ (parser.parse(repeat_interval, parse_tree));
  CK (OB_NOT_NULL(parse_tree));
  OV (T_SCHED_CALENDAR == parse_tree->type_, OB_NOT_SUPPORTED);
  OV (1 == parse_tree->num_child_ || 2 == parse_tree->num_child_ || 3 == parse_tree->num_child_, OB_NOT_SUPPORTED, K(parse_tree->num_child_));
  OX (freq_node = parse_tree->children_[0];)
  OV (OB_NOT_NULL(freq_node) && T_SCHED_CALENDAR_FREQ == freq_node->type_, OB_NOT_SUPPORTED)
  OX (freq_type_ = (CALENDAR_FREQ_TYPE)freq_node->value_;)
  OX (interval_num_ = 1;)
  OX (
    if (parse_tree->num_child_ >= 2) {
      OX (interval_node = parse_tree->children_[1];)
      OV (OB_NOT_NULL(interval_node) && (T_SCHED_CALENDAR_INTERVAL == interval_node->type_ || (T_SCHED_CALENDAR_BY_LIST == interval_node->type_ && 2 == parse_tree->num_child_)), OB_NOT_SUPPORTED)
    }
  )
  OX (
    if (OB_NOT_NULL(interval_node) && T_SCHED_CALENDAR_INTERVAL == interval_node->type_) {
      OX (interval_type_bitset_.add_member(INTERVAL);)
      OX (interval_num_ = interval_node->value_;)
      if (3 == parse_tree->num_child_) { // 如果有3个参数，第3个必须是bylist
        OX (interval_node = parse_tree->children_[2];)
        OV (OB_NOT_NULL(interval_node) && (T_SCHED_CALENDAR_BY_LIST == interval_node->type_), OB_NOT_SUPPORTED)
      }
    }
  )
  OX (
    if (OB_NOT_NULL(interval_node) && T_SCHED_CALENDAR_BY_LIST == interval_node->type_) {
      int bymonth_day_max = 0;
      for (int64_t i = 0; i < interval_node->num_child_ && OB_SUCC(ret); i++) {
        OX (interval_by_list_node = interval_node->children_[i]);
        OV (OB_NOT_NULL(interval_by_list_node) && !interval_type_bitset_.has_member(interval_by_list_node->value_), OB_NOT_SUPPORTED);
        OZ (interval_type_bitset_.add_member(interval_by_list_node->value_));
        OX (interval_by_type = (CALENDAR_INTERVAL_TYPE) interval_by_list_node->value_);
        OX (
          for (int64_t j = 0; j < interval_by_list_node->num_child_ && OB_SUCC(ret); j++) {
            OX (interval_by_value_node = interval_by_list_node->children_[j]);
            switch (interval_by_type) {
              case BYMONTH: {
                OV (freq_type_ == YEARLY, OB_NOT_SUPPORTED);
                OV (1 <= interval_by_value_node->value_ && MAX_MONTH >= interval_by_value_node->value_, OB_NOT_SUPPORTED); //1-12
                OX ( //找出一个集合里的最多可设天数, 例如 bymonth = 2,3 bymonth_day 只可设 1-30
                  if (bymonth_day_max < get_days_in_month(2000, interval_by_value_node->value_)) {
                    bymonth_day_max = get_days_in_month(2000, interval_by_value_node->value_);
                  }
                )
                OV (OB_NOT_NULL(interval_by_value_node) && !month_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (month_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYWEEKNO: {
                ret = OB_NOT_SUPPORTED;
                break;
              }
              case BYYEARDAY: {
                OV (freq_type_ == YEARLY, OB_NOT_SUPPORTED);
                OV (1 <= interval_by_value_node->value_ && MAX_YEAR_DAY >= interval_by_value_node->value_, OB_NOT_SUPPORTED); //1-366
                OV (OB_NOT_NULL(interval_by_value_node) && !year_day_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (year_day_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYDATE: {
                ret = OB_NOT_SUPPORTED;
                break;
              }
              case BYMONTHDAY: {
                int bitset_value = interval_by_value_node->value_;
                OV (freq_type_ == YEARLY || freq_type_ == MONTHLY, OB_NOT_SUPPORTED);
                OV (-MAX_MONTH_DAY <= interval_by_value_node->value_ && MAX_MONTH_DAY >= interval_by_value_node->value_ && interval_by_value_node->value_ != 0, OB_NOT_SUPPORTED); //-31~31，非0
                OX (bitset_value = value_to_month_day_bitset(interval_by_value_node->value_));
                OV (OB_NOT_NULL(interval_by_value_node) && !month_day_bitset_.has_member(bitset_value), OB_NOT_SUPPORTED);
                OZ (month_day_bitset_.add_member(bitset_value));
                break;
              }
              case BYDAY: {
                OV (freq_type_ == WEEKLY, OB_NOT_SUPPORTED);
                OV (1 <= interval_by_value_node->value_ && MAX_DAY >= interval_by_value_node->value_, OB_NOT_SUPPORTED); //1-7
                OV (OB_NOT_NULL(interval_by_value_node) && !day_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (day_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYTIME: {
                ret = OB_NOT_SUPPORTED;
                break;
              }
              case BYHOUR: {
                OV (freq_type_ != HOURLY && freq_type_ != MINUTELY && freq_type_ != SECONDLY, OB_NOT_SUPPORTED);
                OV (0 <= interval_by_value_node->value_ && MAX_HOUR > interval_by_value_node->value_, OB_NOT_SUPPORTED); //0-23
                OV (OB_NOT_NULL(interval_by_value_node) && !hour_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (hour_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYMINUTE: {
                OV (freq_type_ != MINUTELY && freq_type_ != SECONDLY, OB_NOT_SUPPORTED);
                OV (0 <= interval_by_value_node->value_ && MAX_MINUTE_OR_SECOND > interval_by_value_node->value_, OB_NOT_SUPPORTED); //0-59
                OV (OB_NOT_NULL(interval_by_value_node) && !minute_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (minute_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYSECOND: {
                OV (freq_type_ != SECONDLY, OB_NOT_SUPPORTED);
                OV (0 <= interval_by_value_node->value_ && MAX_MINUTE_OR_SECOND > interval_by_value_node->value_, OB_NOT_SUPPORTED); //0-59
                OV (OB_NOT_NULL(interval_by_value_node) && !second_bitset_.has_member(interval_by_value_node->value_), OB_NOT_SUPPORTED);
                OZ (second_bitset_.add_member(interval_by_value_node->value_));
                break;
              }
              case BYSETPOS: {
                ret = OB_NOT_SUPPORTED;
                break;
              }
              default:{
                break;
              }
            }
          }
        )
      }

      OX ( //检查, 必须要输入一个能找到的时间, 例如指定 BYMONTH = 2, BYMONTH_DAY= 31 永远找不到这一天, 计算时会死循环
        if (interval_type_bitset_.has_member(BYMONTH) && interval_type_bitset_.has_member(BYMONTHDAY)) {
          //比较 29/30/31 就行
          for (int day = 29; day <= MAX_MONTH_DAY && OB_SUCC(ret); day++) {
            if (month_day_bitset_.has_member(day) && day > bymonth_day_max) {
              ret = OB_NOT_SUPPORTED;
            }
          }
        }
      )

      OX (has_by_clause_ = true;)
    }
  )
  return ret;
}

int ObDBMSSchedTimeUtil::calc_expr(int64_t start_date, int64_t &next_date, int64_t base_date, int64_t return_date_after)
{
  int ret = OB_SUCCESS;
  if (UNKNOWN_FREQ == freq_type_ || 0 >= interval_num_) {
    next_date = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
    ret = OB_NOT_SUPPORTED;
  } else if (is_first_time_ && !has_by_clause_) {
    next_date = start_date;
  } else {
    time_t start_time = (time_t)(start_date / 1000000LL);
    time_t base_time = (time_t)(base_date / 1000000LL);
    if (start_time > base_time) {
      is_first_time_ = true;
    }
    base_time = std::max(base_time, start_time);
    time_t return_time_after = (time_t)(return_date_after / 1000000LL);
    base_time_ = *localtime(&base_time);
    next_time_ = *localtime(&base_time);
    start_time_ = *localtime(&start_time);
    return_time_after_ = *localtime(&return_time_after);
    switch (freq_type_) {
      case YEARLY: {
        if (interval_type_bitset_.has_member(BYYEARDAY) &&
          (interval_type_bitset_.has_member(BYMONTH) || interval_type_bitset_.has_member(BYMONTHDAY))) {
          next_date = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
          ret = OB_NOT_SUPPORTED;
        } else {
          ret = hand_yearly();
        }
        break;
      }
      case MONTHLY: {
        ret = hand_monthly();
        break;
      }
      case WEEKLY: {
        ret = hand_weekly();
        break;
      }
      case DAILY: {
        ret = hand_daily();
        break;
      }
      case HOURLY: {
        ret = hand_hourly();
        break;
      }
      case MINUTELY: {
        ret = hand_minutely();
        break;
      }
      case SECONDLY: {
        ret = hand_secondly();
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }

    OX (
      time_t seconds_since_epoch = mktime(&next_time_);
      if (return_time_after > seconds_since_epoch) {
        next_date = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
      } else {
        next_date = static_cast<int64_t>(seconds_since_epoch) * 1000000LL;
        next_date = std::max(next_date, start_date);
      }
    )
  }
  return ret;
}

//只在确认秒时判断最终执行的时间是否大于 base_time
bool ObDBMSSchedTimeUtil::find_second()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_second = next_time_.tm_sec; // 当前秒 (0-59)
  for (int i = current_second; i < MAX_MINUTE_OR_SECOND && !found; i++) {
    // 检查当前秒是否在 second_bitset_ 中, 未指定则用start_time
    if ((!interval_type_bitset_.has_member(BYSECOND) && start_time_.tm_sec == i) || second_bitset_.has_member(i)) {
      next_time_.tm_sec = i;
      find_time = mktime(&next_time_);
      if (find_time > mktime(&return_time_after_)) {
        found = true;
      } else if (is_first_time_ && (find_time == mktime(&return_time_after_))) {
        found = true;
      }
    }
  }
  // 如果没有符合条件的秒，归零并按分钟进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}

bool ObDBMSSchedTimeUtil::find_minute()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_minute = next_time_.tm_min; // 当前分钟 (0-59)
  for (int i = current_minute; i < MAX_MINUTE_OR_SECOND && !found; i++) {
    // 检查当前分钟是否在 minute_bitset_ 中, 未指定则用start_time
    if ((!interval_type_bitset_.has_member(BYMINUTE) && start_time_.tm_min == i) || minute_bitset_.has_member(i)) {
      next_time_.tm_min = i;
      if (i > current_minute) {
        next_time_.tm_sec = 0;
      }
      found = true;
    }
  }

  // 如果没有符合条件的分钟，归零并按小时进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}


bool ObDBMSSchedTimeUtil::find_hour()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_hour = next_time_.tm_hour; // 当前小时 (0-23)
  for (int i = current_hour; i < MAX_HOUR && !found; i++) {
    // 检查当前小时是否在 hour_bitset_ 中, 未指定则用start_time
    if ((!interval_type_bitset_.has_member(BYHOUR) && start_time_.tm_hour == i) || hour_bitset_.has_member(i)) {
      next_time_.tm_hour = i;
      if (i > current_hour) {
        next_time_.tm_sec = 0;
        next_time_.tm_min = 0;
      }
      found = true;
    }
  }
  // 如果没有符合条件的小时，归零并按天进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour = 0;
    next_time_.tm_mday += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}


bool ObDBMSSchedTimeUtil::find_day()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int cur_wday = (next_time_.tm_wday == 0 ? MAX_DAY : next_time_.tm_wday); //星期几 (1=Monday, ..., 7=Sunday)
  int start_wday = (start_time_.tm_wday == 0 ? MAX_DAY : start_time_.tm_wday);
  for (int i = cur_wday; i <= MAX_DAY && !found; i++) {
    // 检查该日期是否在 day_bitset_ 中
    if ((!interval_type_bitset_.has_member(BYDAY) && start_wday == i) || day_bitset_.has_member(i)) {
        next_time_.tm_mday += i - cur_wday;
        if (i > cur_wday) {
          next_time_.tm_sec = 0;
          next_time_.tm_min = 0;
          next_time_.tm_hour = 0;
        }
        found = true;
    }
  }
  // 如果没有符合条件的日期，归零并按周进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour = 0;
    next_time_.tm_mday += MAX_DAY + 1 - cur_wday;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}

bool ObDBMSSchedTimeUtil::find_month_day()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_year = next_time_.tm_year + 1900;
  int current_month = next_time_.tm_mon + 1; // 当前月 (1-12)
  int current_month_day = next_time_.tm_mday; // 当前日期 (1-31)
  for (int i = current_month_day; i <= MAX_MONTH_DAY && !found; i++) {
    // 检查当前日期是否在 month_day_bitset_ 中, 未指定则用start_time, 并且合法
    if (((!interval_type_bitset_.has_member(BYMONTHDAY) && start_time_.tm_mday == i) || month_day_bitset_.has_member(i) || month_day_bitset_.has_member(month_day_to_bitset(current_year, current_month, i)))
      && is_valid_date(current_year, current_month, i)) {
      next_time_.tm_mday = i;
      if (i > current_month_day) {
        next_time_.tm_sec = 0;
        next_time_.tm_min = 0;
        next_time_.tm_hour = 0;
      }
      found = true;
    }
  }

  // 如果没有符合条件的日期，归零并按月进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour = 0;
    next_time_.tm_mday = 1;
    next_time_.tm_mon += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}

bool ObDBMSSchedTimeUtil::find_year_day()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_year = next_time_.tm_year + 1900;
  int current_year_max_day = 365;
  if ((current_year % 4 == 0 && current_year % 100 != 0) || (current_year % 400 == 0)) {
    current_year_max_day = 366;
  }
  // 计算当前日期是一年中的第几天 (tm_yday 从 0 开始，范围为 0-365)
  int current_year_day = next_time_.tm_yday + 1; // 转换为 1-366
  for (int i = current_year_day; i <= current_year_max_day && !found; i++) {
    // 检查当前日期是否在 year_day_bitset_ 中
    if (year_day_bitset_.has_member(i)) {
      if (i > current_year_day) {
        next_time_.tm_sec = 0;
        next_time_.tm_min = 0;
        next_time_.tm_hour = 0;
        next_time_.tm_mday += i - current_year_day;
      }
      found = true;
    }
  }
  // 如果没有符合条件的日期，按年进位，并重置时间
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour = 0;
    next_time_.tm_mday = 1;
    next_time_.tm_mon = 0;
    next_time_.tm_year += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}

bool ObDBMSSchedTimeUtil::find_weekno()
{
  //暂不支持
  return false;
}

bool ObDBMSSchedTimeUtil::find_month()
{
  bool found = false;
  time_t find_time = mktime(&next_time_);
  int current_month = next_time_.tm_mon + 1; //转换为 1-12
  for (int i = current_month; i <= MAX_MONTH && !found; i++) {
    // 检查当前月份是否在 month_bitset_ 中, 未指定则用start_time
    if ((!interval_type_bitset_.has_member(BYMONTH) && start_time_.tm_mon == i - 1) || month_bitset_.has_member(i)) {
      next_time_.tm_mon = i - 1;
      if (i > current_month) {
        next_time_.tm_sec = 0;
        next_time_.tm_min = 0;
        next_time_.tm_hour = 0;
        next_time_.tm_mday = 1;
      }
      found = true;
    }
  }
  // 如果没有符合条件的月份，归零并按年进位
  if (!found) {
    next_time_.tm_sec = 0;
    next_time_.tm_min = 0;
    next_time_.tm_hour = 0;
    next_time_.tm_mday = 1;
    next_time_.tm_mon = 0;
    next_time_.tm_year += 1;
  }
  // 修正溢出并规范化时间
  find_time = mktime(&next_time_);
  return found;
}

int ObDBMSSchedTimeUtil::hand_yearly()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  if (interval_type_bitset_.has_member(BYYEARDAY)) {
    while (!found && find_time < (time_t)(ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE / 1000000LL)) {
      found = find_year_day();
      if (found) {
        found = find_hour();
      }
      if (found) {
        found = find_minute();
      }
      if (found) {
        found = find_second();
      }
      // 修正进位
      if (!found && next_time_.tm_year != old_next_time.tm_year) {
        next_time_.tm_year -= 1;
        find_time = mktime(&next_time_);
        next_time_.tm_year += interval_num_;
        find_time = mktime(&next_time_);
        old_next_time = *localtime(&find_time);
      }
    }
  } else {
    while (!found && find_time < (time_t)(ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE / 1000000LL)) { // 未指定byyearday则按照bymonth
      found = find_month();
      if (found) {
        found = find_month_day();
      }
      if (found) {
        found = find_hour();
      }
      if (found) {
        found = find_minute();
      }
      if (found) {
        found = find_second();
      }
      // 修正进位
      if (!found && next_time_.tm_year != old_next_time.tm_year) {
        next_time_.tm_year -= 1;
        find_time = mktime(&next_time_);
        next_time_.tm_year += interval_num_;
        find_time = mktime(&next_time_);
        old_next_time = *localtime(&find_time);
      }
    }
  }
  if (find_time >= (time_t)(ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE / 1000000LL)) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedTimeUtil::hand_monthly()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  while (!found) {
    found = find_month_day();
    if (found) {
      found = find_hour();
    }
    if (found) {
      found = find_minute();
    }
    if (found) {
      found = find_second();
    }
    // 修正进位
    if (!found && next_time_.tm_mon != old_next_time.tm_mon) {
      next_time_.tm_mon -= 1;
      find_time = mktime(&next_time_);
      next_time_.tm_mon += interval_num_;
      find_time = mktime(&next_time_);
      old_next_time = *localtime(&find_time);
    }
  }
  return ret;
}

int ObDBMSSchedTimeUtil::hand_weekly()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  while (!found) {
    found = find_day();
    if (found) {
      found = find_hour();
    }
    if (found) {
      found = find_minute();
    }
    if (found) {
      found = find_second();
    }
    // 修正进位
    if (!found && next_time_.tm_mday != old_next_time.tm_mday && next_time_.tm_wday == 1) {
      int old_wday = (old_next_time.tm_wday == 0 ? MAX_DAY : old_next_time.tm_wday);
      next_time_.tm_mday -= MAX_DAY + 1 - old_wday;
      find_time = mktime(&next_time_);
      next_time_.tm_mday += MAX_DAY + 1 - old_wday + (interval_num_ - 1) * MAX_DAY;
      find_time = mktime(&next_time_);
      old_next_time = *localtime(&find_time);
    }
  }
  return ret;
}

int ObDBMSSchedTimeUtil::hand_daily()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  while (!found) {
    found = find_hour();
    if (found) {
      found = find_minute();
    }
    if (found) {
      found = find_second();
    }
    // 修正进位
    if (!found && next_time_.tm_mday != old_next_time.tm_mday) {
      next_time_.tm_mday -= 1;
      find_time = mktime(&next_time_);
      next_time_.tm_mday = old_next_time.tm_mday + interval_num_;
      find_time = mktime(&next_time_);
      old_next_time = *localtime(&find_time);
    }
  }
  return ret;
}

int ObDBMSSchedTimeUtil::hand_hourly()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  while (!found) {
    found = find_minute();
    if (found) {
      found = find_second();
    }
    // 修正进位
    if (!found && next_time_.tm_hour != old_next_time.tm_hour) {
      next_time_.tm_hour -= 1;
      find_time = mktime(&next_time_);
      next_time_.tm_hour = old_next_time.tm_hour + interval_num_;
      find_time = mktime(&next_time_);
      old_next_time = *localtime(&find_time);
    }
  }
  return ret;
}

int ObDBMSSchedTimeUtil::hand_minutely()
{
  int ret = OB_SUCCESS;
  bool found = false;
  time_t find_time = mktime(&next_time_);
  struct tm old_next_time = *localtime(&find_time);
  while (!found) {
    found = find_second();
    // 修正进位
    if (!found && next_time_.tm_min != old_next_time.tm_min) {
      next_time_.tm_min -= 1;
      find_time = mktime(&next_time_);
      next_time_.tm_min = old_next_time.tm_min + interval_num_;
      find_time = mktime(&next_time_);
      old_next_time = *localtime(&find_time);
    }
  }
  return ret;
}


int ObDBMSSchedTimeUtil::hand_secondly()
{
  int ret = OB_SUCCESS;
  time_t find_time = mktime(&next_time_);
  while (find_time <= mktime(&return_time_after_)) {
    next_time_.tm_sec += interval_num_;
    find_time = mktime(&next_time_);
  }
  return ret;
}

}
}