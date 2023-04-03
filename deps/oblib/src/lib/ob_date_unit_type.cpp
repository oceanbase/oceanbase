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

#include <sys/types.h>
#include "lib/ob_date_unit_type.h"
#include "lib/utility/ob_macro_utils.h"

const char *ob_date_unit_type_str(enum ObDateUnitType type)
{
  static const char *date_unit_type_name[DATE_UNIT_MAX + 1] =
  {
    "microsecond",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "second_microsecond",
    "minute_microsecond",
    "minute_second",
    "hour_microsecond",
    "hour_second",
    "hour_minute",
    "day_microsecond",
    "day_second",
    "day_minute",
    "day_hour",
    "year_month",
    "timezone_hour",
    "timezone_minute",
    "timezone_region",
    "timezone_abbr",
    "unknown",
  };
  static_assert(DATE_UNIT_MAX + 1 == ARRAYSIZEOF(date_unit_type_name),
              "size of array not match enum size");
  return date_unit_type_name[type];
}

const char *ob_date_unit_type_str_upper(enum ObDateUnitType type)
{
  static const char *date_unit_type_name[DATE_UNIT_MAX + 1] =
  {
    "MICROSECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
    "SECOND_MICROSECOND",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "HOUR_MICROSECOND",
    "HOUR_SECOND",
    "HOUR_MINUTE",
    "DAY_MICROSECOND",
    "DAY_SECOND",
    "DAY_MINUTE",
    "DAY_HOUR",
    "YEAR_MONTH",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TIMEZONE_REGION",
    "TIMEZONE_ABBR",
    "UNKNOWN",
  };
  static_assert(DATE_UNIT_MAX + 1 == ARRAYSIZEOF(date_unit_type_name),
              "size of array not match enum size");
  return date_unit_type_name[type];
}
const char* ob_date_unit_type_num_str(enum ObDateUnitType type)
{
  static const char *date_unit_type_num[DATE_UNIT_MAX + 1] =
  {
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
  };
  static_assert(DATE_UNIT_MAX + 1 == ARRAYSIZEOF(date_unit_type_num),
              "size of array not match enum size");
  return date_unit_type_num[type];
}

const char *ob_get_format_unit_type_str(enum ObGetFormatUnitType type)
{
  static const char *get_format_unit_type_name[GET_FORMAT_MAX + 1] =
  {
    "date",
    "time",
    "datetime",
    "unknown",
  };
  static_assert(GET_FORMAT_MAX + 1 == ARRAYSIZEOF(get_format_unit_type_name),
              "size of array not match enum size");
  return get_format_unit_type_name[type];
}