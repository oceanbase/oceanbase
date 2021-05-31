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

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLResult::ObMySQLResult()
{}

ObMySQLResult::~ObMySQLResult()
{}

int ObMySQLResult::varchar2datetime(const ObString& varchar, int64_t& datetime) const
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

int ObMySQLResult::get_single_int(const int64_t row_idx, const int64_t col_idx, int64_t& int_val)
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

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
