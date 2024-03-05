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

int ObMySQLResult::format_precision_scale_length(int16_t &precision, int16_t &scale, int32_t &length,
                                                  oceanbase::common::ObObjType ob_type, oceanbase::common::ObCollationType meta_cs_type,DblinkDriverProto link_type, bool old_max_length) const
{
  int ret = OB_SUCCESS;
  int16_t tmp_precision = precision;
  int16_t tmp_scale = scale;
  int32_t tmp_length = length;
  int64_t mbminlen = 0;
  LOG_DEBUG("dblink pull meta", K(precision), K(scale), K(length), K(ret), K(ob_type));
  // format precision from others to oceanbase
  if (!lib::is_oracle_mode()) {
    switch (ob_type) {
      case ObUNumberType:{
        if (0 != scale) {
          precision = length - 1;// remove length of decimal point
        } else {
          precision = length;
        }
        length = -1;
        break;
      }
      case ObNumberType: {
        if (0 != scale) {
          precision = length - 2;// remove length of decimal point and sign(-/+)
        } else {
          precision = length - 1;// remove length of decimal point
        }
        length = -1;
        break;
      }
      case ObUFloatType:
      case ObUDoubleType:
      case ObDoubleType:
      case ObFloatType: {// for mysql double type and float type
        precision = length;
        length = -1;
        if (scale > precision) {
          scale = -1;
          precision = -1;
        }
        break;
      }
      case ObCharType:
      case ObVarcharType: {
        if (OB_FAIL(common::ObCharset::get_mbminlen_by_coll(meta_cs_type, mbminlen))) {
          LOG_WARN("fail to get mbminlen", K(meta_cs_type), K(ret));
        } else {
          length /= mbminlen;
          precision = -1;
          scale = -1;
          if (ObCharType == ob_type && length > 256) {
            length = 256;
          }
        }
        break;
      }
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObBitType: {
        precision = length;
        length = -1;
        scale = -1;
        break;
      }
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        precision = -1;
        scale = -1;
        length = 0;
        break;
      }
      default:
       break;
    }
  } else {
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
    if (DBLINK_DRV_OCI == link_type && ObDateTimeType == ob_type) {
      scale = 0;
    } else if (tmp_scale < OB_MIN_NUMBER_SCALE || tmp_scale > OB_MAX_NUMBER_SCALE ||
              (-1 == precision && ObNumberType == ob_type)) {
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
    if (ObDoubleType == ob_type || ObUDoubleType == ob_type) {
      precision = -1;
      scale = -85;
      length = 22;
    }
    if (ObFloatType == ob_type ||  ObUFloatType == ob_type) {
      precision = -1;
      scale = -85;
      length = 12;
    }
    if (ObIntervalYMType == ob_type || ObIntervalDSType == ob_type) {
      precision = -1;
      length = -1;
      if (DBLINK_DRV_OCI == link_type) {
        scale = (ObIntervalYMType == ob_type)  ? tmp_precision : (tmp_precision * 10 + tmp_scale);
      } else {
        // do nothing, keep the value of scale unchanged
      }
    }
  }
  LOG_DEBUG("dblink pull meta after format", K(precision), K(scale), K(length), K(ret));
  return ret;
}

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
