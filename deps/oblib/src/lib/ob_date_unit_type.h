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

#ifndef _OCEANBASE_COMMON_DATE_UNIT_TYPE_H_
#define _OCEANBASE_COMMON_DATE_UNIT_TYPE_H_

#ifdef __cplusplus
extern "C" {
#endif
enum ObDateUnitType
{
  /* the type of date unit */
  DATE_UNIT_MICROSECOND = 0,
  DATE_UNIT_SECOND,
  DATE_UNIT_MINUTE,
  DATE_UNIT_HOUR,
  DATE_UNIT_DAY,
  DATE_UNIT_WEEK,
  DATE_UNIT_MONTH,
  DATE_UNIT_QUARTER,
  DATE_UNIT_YEAR,
  DATE_UNIT_SECOND_MICROSECOND,
  DATE_UNIT_MINUTE_MICROSECOND,
  DATE_UNIT_MINUTE_SECOND,
  DATE_UNIT_HOUR_MICROSECOND,
  DATE_UNIT_HOUR_SECOND,
  DATE_UNIT_HOUR_MINUTE,
  DATE_UNIT_DAY_MICROSECOND,
  DATE_UNIT_DAY_SECOND,
  DATE_UNIT_DAY_MINUTE,
  DATE_UNIT_DAY_HOUR,
  DATE_UNIT_YEAR_MONTH,
  DATE_UNIT_TIMEZONE_HOUR,
  DATE_UNIT_TIMEZONE_MINUTE,
  DATE_UNIT_TIMEZONE_REGION,
  DATE_UNIT_TIMEZONE_ABBR,
  DATE_UNIT_MAX
};

enum ObGetFormatUnitType
{
  GET_FORMAT_DATE = 0,
  GET_FORMAT_TIME = 1,
  GET_FORMAT_DATETIME = 2,
  GET_FORMAT_MAX = 3,
};

const char* ob_date_unit_type_str(enum ObDateUnitType type);
const char* ob_date_unit_type_str_upper(enum ObDateUnitType type);
const char* ob_date_unit_type_num_str(enum ObDateUnitType type);
const char* ob_get_format_unit_type_str(enum ObGetFormatUnitType type);

#ifdef __cplusplus
}

static_assert(DATE_UNIT_DAY > DATE_UNIT_HOUR
              && DATE_UNIT_HOUR > DATE_UNIT_MINUTE
              && DATE_UNIT_MINUTE > DATE_UNIT_SECOND, "Please keep the sequence of interval day to second types");
static_assert(DATE_UNIT_YEAR > DATE_UNIT_MONTH, "Please keep the sequence of interval year to month types");

#endif
#endif //_OCEANBASE_COMMON_DATE_UNIT_TYPE_H_
