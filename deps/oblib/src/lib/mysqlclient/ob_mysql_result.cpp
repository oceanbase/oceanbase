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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
ObMySQLResult::ObMySQLResult()
{
}

ObMySQLResult::~ObMySQLResult()
{

}

int ObMySQLResult::varchar2datetime(const ObString &varchar, int64_t &datetime) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(varchar.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty value", K(varchar), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ret = ObTimeConverter::str_to_datetime(varchar, cvrt_ctx, datetime, NULL);
  }
  return ret;
}

int ObMySQLResult::get_single_int(const int64_t row_idx, const int64_t col_idx, int64_t &int_val)
{
  int ret = OB_SUCCESS;
  if (row_idx < 0 || col_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(row_idx), K(col_idx));
  } else {
    int64_t i = 0;
    for (; i < row_idx && OB_SUCC(ret); ++i) {
      ret = this->next();
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        LOG_WARN("too few result rows", K(row_idx), K(i));
      }
    } else {
      // get the single row
      if (OB_FAIL(this->next())) {
        LOG_WARN("too few result rows", K(ret), K(row_idx));
      } else {
        ret = get_int(col_idx, int_val);
      }
    }
  }
  return ret;
}

int ObMySQLResult::print_info() const
{
  return OB_SUCCESS;
}

void ObMySQLResult::format_precision_scale_length(int16_t &precision, int16_t &scale, int32_t &length, 
                                                  oceanbase::common::ObObjType ob_type, oceanbase::common::ObCollationType cs_type,
                                                  DblinkDriverProto link_type, bool old_max_length) const
{
  int16_t tmp_precision = precision;
  int16_t tmp_scale = scale;
  int32_t tmp_length = length;
  // format precision from others to oceanbase
  if (ob_is_nstring(ob_type)) {
    precision = LS_CHAR; // precision is LS_CHAR means national character set (unicode)
  } else if (ObNumberFloatType == ob_type) {
    precision = tmp_precision; //bit precision, not decimal precision
  } else if (tmp_precision < OB_MIN_NUMBER_PRECISION || tmp_precision > OB_MAX_NUMBER_PRECISION) {
    precision = -1; // for other data type, need format it to -1 if precison out of range
  } else {
    precision = tmp_precision; // for a valid precison([OB_MIN_NUMBER_PRECISION, OB_MAX_NUMBER_PRECISION]), just set it
  }

  // format scale from others to oceanbase
  if (DBLINK_DRV_OCI == link_type && (ObFloatType == ob_type || ObDoubleType == ob_type)) {
    scale = OB_MIN_NUMBER_SCALE - 1; // binary_float and binar_double scale from oci is 0, need set to -85
  } else if (DBLINK_DRV_OCI == link_type && ObDateTimeType == ob_type) {
    scale = 0;
  } else if (tmp_scale < OB_MIN_NUMBER_SCALE || tmp_scale > OB_MAX_NUMBER_SCALE) {
    scale = OB_MIN_NUMBER_SCALE - 1; // format it to -85 if scale out of range
  } else {
    scale = tmp_scale; //  for a valid scale, just set it
  }

  // format length from others to oceanbase
  if (ob_is_accuracy_length_valid_tc(ob_type)) {
    int32_t max_length = 0;
    if (old_max_length) {
      max_length = OB_MAX_LONGTEXT_LENGTH_OLD;
    } else {
      max_length = OB_MAX_LONGTEXT_LENGTH;
    }
    if (tmp_length < 1 || tmp_length > max_length) {
      length = max_length;
    } else {
      length = tmp_length;
    }
  }
}

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
