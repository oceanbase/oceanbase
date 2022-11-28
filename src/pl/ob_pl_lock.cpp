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

#define USING_LOG_PREFIX PL

#include "lib/string/ob_string.h"
#include "pl/ob_pl_lock.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share;
using namespace number;
namespace pl
{
  int PlPackageLock::sleep(sql::ObExecContext &ctx,
            sql::ParamStore &params,
            common::ObObj &result)
  {
    UNUSED(result);
    int ret = OB_SUCCESS;
    const int64_t rate_from_s_to_us = 1000000;
    char local_buff[ObNumber::MAX_BYTE_LEN];
      ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    ObNumber rate_to_us;
    int64_t total_sleep_time_of_us = 0;
    int64_t once_sleep_time = 1000000;  // 一次睡眠1s
    CK (1 == params.count());
    CK (params.at(0).is_number());
    ObNumber sleep_time = params.at(0).get_number();
    if (sleep_time.is_negative()) {
      LOG_WARN("sleep_time is negative");
      ret = OB_ERR_INPUT_TIME_TYPE;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(rate_to_us.from(rate_from_s_to_us, local_alloc))) {
      LOG_WARN("cast int64_t to number fail", K(ret));
    } else if (OB_FAIL(sleep_time.trunc(2))) { // 精度到小数点后两位，单位为s
      LOG_WARN("trunc number fail", K(ret));
    } else if (OB_FAIL(sleep_time.mul_v3(rate_to_us, sleep_time, local_alloc, true, false))) {
      LOG_WARN("mul rate_to_us with sleep_time fail", K(ret));
    } else if (!sleep_time.is_valid_int64(total_sleep_time_of_us)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast sleep_time to int_64 fail", K(ret));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      while (total_sleep_time_of_us > 0 && OB_SUCC(ctx.check_status())) {
        if (total_sleep_time_of_us >= once_sleep_time) {
          // 剩余睡眠时间大于1s
          ob_usleep(once_sleep_time);
          total_sleep_time_of_us = total_sleep_time_of_us - once_sleep_time;
        } else {
          // 剩余睡眠时间小于1s
          ob_usleep(total_sleep_time_of_us);
          total_sleep_time_of_us = 0;
        }
      }
    }

    return ret;
  }

} // namespace pl
} // oceanbase
