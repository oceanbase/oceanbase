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

#ifndef OCEABASE_SHARE_THROTTLE_COMMON_
#define OCEABASE_SHARE_THROTTLE_COMMON_
#include "lib/literals/ob_literals.h"
#include "lib/ob_define.h"
namespace oceanbase {
namespace share {

class ObThrottleStat final
{
  const int64_t MAX_LOG_INTERVAL = 10_s;
public:
  ObThrottleStat() :
    total_throttle_time_us(0),
    total_skip_throttle_time_us(0),
    last_log_timestamp(0),
    last_throttle_status(false)
  {
    memset(detail_skip_time_us, 0, sizeof(detail_skip_time_us));
  }
  ~ObThrottleStat()
  {}
  void reset()
  {
    total_throttle_time_us = 0;
    total_skip_throttle_time_us = 0;
    last_log_timestamp = 0;
    last_throttle_status = 0;
    memset(detail_skip_time_us, 0, sizeof(detail_skip_time_us));
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool need_log(const bool is_throttle_now);
  void update(const int64_t total_expected_wait_us,
              const int64_t from_user_skip_us,
              const int64_t user_timeout_skip_us,
              const int64_t frozen_memtable_skip_us,
              const int64_t replay_frozen_skip_us)
  {
    total_throttle_time_us += total_expected_wait_us;
    total_skip_throttle_time_us += (from_user_skip_us +
                                    user_timeout_skip_us +
                                    frozen_memtable_skip_us +
                                    replay_frozen_skip_us);
    detail_skip_time_us[FROM_USER] += from_user_skip_us;
    detail_skip_time_us[USER_TIMEOUT] += user_timeout_skip_us;
    detail_skip_time_us[FROZEN_MEMTABLE] += frozen_memtable_skip_us;
    detail_skip_time_us[REPLAY_FROZEN] += replay_frozen_skip_us;
  }
public:
  enum
  {
    FROM_USER = 0,       // the use note that the process should not sleep because of throttle
    USER_TIMEOUT = 1,    // throttle meets timeout such as statement timeout.
    FROZEN_MEMTABLE = 2, // frozen memtable's write should not sleep.
    REPLAY_FROZEN = 3,   // the replay process should not sleep if it is freezing.
    MAX
  };
  int64_t total_throttle_time_us;      // the total time we need sleep because of throttle(us).
  int64_t total_skip_throttle_time_us; // the total time we have skip sleep.
  int64_t detail_skip_time_us[MAX];       // the skip sleep time of every mode.

  int64_t last_log_timestamp; // the last time we have logged, for log print.
  bool last_throttle_status;    // the last throttle status.
};

OB_INLINE ObThrottleStat &get_throttle_stat()
{
  RLOCAL_INLINE(ObThrottleStat, throttle_stat_);
  return throttle_stat_;
}

// record the alloc size of current thread
OB_INLINE int64_t &get_thread_alloc_stat()
{
  RLOCAL_INLINE(int64_t, allock_stat);
  return allock_stat;
}

} // end namespace share
} // end namespace oceanbase
#endif
