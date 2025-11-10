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

#ifndef SRC_OBSERVER_DBMS_SCHED_CALENDAR_H_
#define SRC_OBSERVER_DBMS_SCHED_CALENDAR_H_

#include "lib/allocator/ob_mod_define.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace dbms_scheduler
{
enum CALENDAR_FREQ_TYPE
{
  UNKNOWN_FREQ = 0,
  YEARLY,
  MONTHLY,
  WEEKLY,
  DAILY,
  HOURLY,
  MINUTELY,
  SECONDLY
};

enum CALENDAR_INTERVAL_TYPE
{
  INTERVAL = 0,
  BYMONTH,
  BYWEEKNO,
  BYYEARDAY,
  BYDATE,
  BYMONTHDAY,
  BYDAY,
  BYTIME,
  BYHOUR,
  BYMINUTE,
  BYSECOND,
  BYSETPOS
};

class ObDBMSSchedJobInfo;
class ObDBMSSchedTimeUtil
{
public:

  ObDBMSSchedTimeUtil() {}
  virtual ~ObDBMSSchedTimeUtil() {}
  int check_is_valid_repeat_interval(const int64_t tenant_id, const ObString &repeat_interval, bool is_limit_interval_num);
  int calc_repeat_interval_next_date(const ObString &repeat_interval, int64_t start_date, int64_t &next_date, int64_t base_date = 0, int64_t return_date_after = 0, bool is_first_time = false);
  void reset();

private:
  static const int MAX_BY_TYPE = 12;
  static const int MAX_MONTH = 12; //每年第 xx 月
  static const int MAX_WEEKNO = 53; //每年第 xx 周
  static const int MAX_MONTH_DAY = 31; //每月第 xx 天
  static const int MAX_YEAR_DAY = 366; //每年第 xx 天
  static const int MAX_DAY = 7; //每周第 xx 天
  static const int MAX_HOUR = 24; //每天第 xx 小时
  static const int MAX_MINUTE_OR_SECOND = 60; //每小时第 xx 分 or 每分第 xx 秒
  CALENDAR_FREQ_TYPE freq_type_ = UNKNOWN_FREQ;
  int64_t interval_num_ = 0;
  bool has_by_clause_ = false;
  bool is_first_time_ = false;
  common::ObBitSet<MAX_BY_TYPE> interval_type_bitset_;
  common::ObBitSet<MAX_MONTH + 1> month_bitset_;
  common::ObBitSet<MAX_WEEKNO + 1> weekno_bitset_;
  common::ObBitSet<2 * MAX_MONTH_DAY + 1> month_day_bitset_;
  common::ObBitSet<MAX_YEAR_DAY + 1> year_day_bitset_;
  common::ObBitSet<MAX_DAY + 1> day_bitset_;
  common::ObBitSet<MAX_HOUR> hour_bitset_;
  common::ObBitSet<MAX_MINUTE_OR_SECOND> minute_bitset_;
  common::ObBitSet<MAX_MINUTE_OR_SECOND> second_bitset_;
  struct tm start_time_;
  struct tm next_time_;
  struct tm base_time_;
  struct tm return_time_after_;
  //解析 repeat_interval
  int resolve_repeat_interval(const ObString &repeat_interval);
  //计算 calendar 表达式不需要开始时间
  int calc_expr(int64_t start_date, int64_t &next_date, int64_t base_date, int64_t return_date_after);

  bool find_second();
  bool find_minute();
  bool find_hour();
  bool find_day();
  bool find_month_day();
  bool find_year_day();
  bool find_weekno();
  bool find_month();


  int hand_yearly();
  int hand_monthly();
  int hand_weekly();
  int hand_daily();
  int hand_hourly();
  int hand_minutely();
  int hand_secondly();
};

}
}
#endif /* SRC_OBSERVER_DBMS_SCHED_CALENDAR_H_ */