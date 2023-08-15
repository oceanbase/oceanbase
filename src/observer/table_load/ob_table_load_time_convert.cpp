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

#include "observer/table_load/ob_table_load_time_convert.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace sql;

ObTableLoadTimeConverter::ObTableLoadTimeConverter()
  : is_inited_(false)
{
}

ObTableLoadTimeConverter::~ObTableLoadTimeConverter()
{
}

int ObTableLoadTimeConverter::init(const ObString &format)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    ObTime ob_time;
    ob_time.mode_ |= DT_TYPE_DATETIME;
    if (ob_is_otimestamp_type(ObDateTimeType)) {
      ob_time.mode_ |= DT_TYPE_ORACLE;
    }
    if (ob_is_timestamp_tz(ObDateTimeType)) {
      ob_time.mode_ |= DT_TYPE_TIMEZONE;
    }
    if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(format, dfm_elems_))) {
      LOG_WARN("fail to parse oracle datetime format string", KR(ret), K(format));
    } else if (OB_FAIL(ObDFMUtil::check_semantic(dfm_elems_, elem_flags_, ob_time.mode_))) {
      LOG_WARN("check semantic of format string failed", KR(ret), K(format));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTimeConverter::str_to_datetime_oracle(const ObString &str,
                                                     const ObTimeConvertCtx &cvrt_ctx,
                                                     ObDateTime &value) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTimeConverter not init", KR(ret));
  } else {
    ObTime ob_time;
    ObDateTime result_value = 0;
    ObScale scale = 0; // not used
    if (OB_FAIL(str_to_ob_time(str, cvrt_ctx, ObDateTimeType, ob_time, scale))) {
      LOG_WARN("failed to convert str to ob_time", K(str), K(cvrt_ctx.oracle_nls_format_));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_value))) {
      LOG_WARN("convert ob_time to datetime failed", K(ret), K(ob_time));
    } else {
      value = result_value;
    }
  }
  return ret;
}

int ObTableLoadTimeConverter::str_to_ob_time(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                                             const ObObjType target_type, ObTime &ob_time,
                                             ObScale &scale) const
{
  int ret = OB_SUCCESS;
  const ObString &format = cvrt_ctx.oracle_nls_format_;

  if (OB_UNLIKELY(str.empty() || format.empty() ||
                  (!ob_is_otimestamp_type(target_type) && !ob_is_datetime_tc(target_type)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(str), K(format), K(ob_time.mode_));
  } else {
    ob_time.mode_ |= DT_TYPE_DATETIME;
    if (ob_is_otimestamp_type(target_type)) {
      ob_time.mode_ |= DT_TYPE_ORACLE;
    }
    if (ob_is_timestamp_tz(target_type)) {
      ob_time.mode_ |= DT_TYPE_TIMEZONE;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTimeConverter::str_to_ob_time_by_dfm_elems(str, dfm_elems_, elem_flags_, cvrt_ctx,
                                                             target_type, ob_time, scale))) {
      LOG_WARN("fail to str to ob time by dfm elements", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase