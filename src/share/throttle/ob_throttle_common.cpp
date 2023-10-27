/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "share/throttle/ob_throttle_common.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
namespace share {

int64_t ObThrottleStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(total_throttle_time_us),
       K(total_skip_throttle_time_us),
       K(last_log_timestamp),
       K(last_throttle_status));
  for(int i =0; i < MAX; i++) {
    common::databuff_printf(buf, buf_len, pos, ", %d=%ld", i, detail_skip_time_us[i]);
  }
  J_OBJ_END();
  return pos;
}

bool ObThrottleStat::need_log(const bool is_throttle_now)
{
  bool is_need_log = false;
  const int64_t cur_ts = ObClockGenerator::getClock();
  if (cur_ts - last_log_timestamp > MAX_LOG_INTERVAL) {
    is_need_log = true;
  }
  if (is_throttle_now == !last_throttle_status) {
    is_need_log = true;
    last_throttle_status = is_throttle_now;
  }
  if (is_need_log) {
    last_log_timestamp = cur_ts;
  }
  return is_need_log;
}

} // end namespace share
} // end namespace oceanbase
