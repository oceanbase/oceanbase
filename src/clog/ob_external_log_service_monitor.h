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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_LOG_SERVICE_MONITOR_
#define OCEANBASE_CLOG_OB_EXTERNAL_LOG_SERVICE_MONITOR_

#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "ob_log_direct_reader.h"
#include "ob_log_reader_interface.h"

namespace oceanbase {
namespace logservice {

class ObExtLogServiceMonitor {
public:
  inline static void locate_count()
  {
    ATOMIC_INC(&locate_count_);
  }
  inline static void open_count()
  {
    ATOMIC_INC(&open_count_);
  }
  inline static void fetch_count()
  {
    ATOMIC_INC(&fetch_count_);
  }
  inline static void heartbeat_count()
  {
    ATOMIC_INC(&heartbeat_count_);
  }

  inline static void locate_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&locate_time_, time);
  }
  inline static void open_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&open_time_, time);
  }
  inline static void fetch_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&fetch_time_, time);
  }
  inline static void l2s_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&l2s_time_, time);
  }
  inline static void svr_queue_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&svr_queue_time_, time);
  }
  inline static void heartbeat_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&heartbeat_time_, time);
  }

  inline static void enable_feedback_count()
  {
    ATOMIC_INC(&enable_feedback_count_);
  }
  inline static void fetch_size(const int64_t size)
  {
    (void)ATOMIC_AAF(&fetch_size_, size);
  }
  inline static void read_disk_count(const int64_t count)
  {
    (void)ATOMIC_AAF(&read_disk_count_, count);
  }
  inline static void fetch_log_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&fetch_log_count_, c);
  }
  inline static void get_cursor_batch_time(const int64_t t)
  {
    (void)ATOMIC_AAF(&get_cursor_batch_time_, t);
  }
  inline static void batch_read_log_time(const int64_t t)
  {
    (void)ATOMIC_AAF(&read_log_time_, t);
  }
  inline static void total_fetch_pkey_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&total_fetch_pkey_count_, c);
  }
  inline static void reach_upper_ts_pkey_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&reach_upper_ts_pkey_count_, c);
  }
  inline static void reach_max_log_pkey_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&reach_max_log_pkey_count_, c);
  }
  inline static void need_fetch_pkey_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&need_fetch_pkey_count_, c);
  }
  inline static void scan_round_count(const int64_t c)
  {
    (void)ATOMIC_AAF(&scan_round_count_, c);
  }
  inline static void round_rate(const int64_t rate)
  {
    (void)ATOMIC_AAF(&round_rate_, rate);
  }
  inline static void feedback_count()
  {
    ATOMIC_INC(&feedback_count_);
  }
  inline static void feedback_pkey_count(const int64_t pkey_count)
  {
    (void)ATOMIC_AAF(&feedback_pkey_count_, pkey_count);
  }
  inline static void feedback_time(const int64_t time)
  {
    (void)ATOMIC_AAF(&feedback_time_, time);
  }

  static void reset()
  {
    ATOMIC_STORE(&locate_count_, 0);
    ATOMIC_STORE(&open_count_, 0);
    ATOMIC_STORE(&fetch_count_, 0);
    ATOMIC_STORE(&heartbeat_count_, 0);

    ATOMIC_STORE(&locate_time_, 0);
    ATOMIC_STORE(&open_time_, 0);
    ATOMIC_STORE(&fetch_time_, 0);
    ATOMIC_STORE(&l2s_time_, 0);
    ATOMIC_STORE(&svr_queue_time_, 0);
    ATOMIC_STORE(&heartbeat_time_, 0);

    ATOMIC_STORE(&enable_feedback_count_, 0);
    ATOMIC_STORE(&fetch_size_, 0);
    ATOMIC_STORE(&read_disk_count_, 0);
    ATOMIC_STORE(&fetch_log_count_, 0);
    ATOMIC_STORE(&get_cursor_batch_time_, 0);
    ATOMIC_STORE(&read_log_time_, 0);
    ATOMIC_STORE(&total_fetch_pkey_count_, 0);
    ATOMIC_STORE(&reach_upper_ts_pkey_count_, 0);
    ATOMIC_STORE(&reach_max_log_pkey_count_, 0);
    ATOMIC_STORE(&need_fetch_pkey_count_, 0);
    ATOMIC_STORE(&scan_round_count_, 0);
    ATOMIC_STORE(&round_rate_, 0);

    ATOMIC_STORE(&feedback_count_, 0);
    ATOMIC_STORE(&feedback_pkey_count_, 0);
    ATOMIC_STORE(&feedback_time_, 0);
  }

  static void report()
  {
    int64_t round_rate = calc_rate_int(ATOMIC_LOAD(&round_rate_), ATOMIC_LOAD(&fetch_count_));
    int64_t get_cursor_batch_time_rate =
        calc_rate_int(10000 * ATOMIC_LOAD(&get_cursor_batch_time_), ATOMIC_LOAD(&fetch_time_));
    int64_t batch_read_log_time_rate = calc_rate_int(10000 * ATOMIC_LOAD(&read_log_time_), ATOMIC_LOAD(&fetch_time_));
    _EXTLOG_LOG(INFO,
        "ObExtLogServiceMonitor Report: "
        "locate_count=%ld, open_count=%ld, heartbeat_count=%ld, "
        "locate_time=%ld, open_time=%ld, heartbeat_time=%ld, "
        "fetch_count=%ld, enable_feedback_count=%ld, "
        "fetch_size=%ld, read_disk_count=%ld, fetch_log_count=%ld, "
        "l2s_time=%ld, svr_queue_time=%ld, fetch_time=%ld, get_cursor_batch_time=%ld, "
        "read_log_and_fill_resp_time=%ld, "
        "get_cursor_batch_time_rate=%ld%%%%, batch_read_log_time_rate=%ld%%%%, "
        "total_fetch_pkey_count=%ld, reach_upper_ts_pkey_count=%ld, "
        "reach_max_log_pkey_count=%ld, need_fetch_pkey_count=%ld, "
        "scan_round_count=%ld, round_rate=%ld, "
        "feedback_count=%ld, feedback_pkey_count=%ld, feedback_time=%ld",
        ATOMIC_LOAD(&locate_count_),
        ATOMIC_LOAD(&open_count_),
        ATOMIC_LOAD(&heartbeat_count_),
        ATOMIC_LOAD(&locate_time_),
        ATOMIC_LOAD(&open_time_),
        ATOMIC_LOAD(&heartbeat_time_),
        ATOMIC_LOAD(&fetch_count_),
        ATOMIC_LOAD(&enable_feedback_count_),
        ATOMIC_LOAD(&fetch_size_),
        ATOMIC_LOAD(&read_disk_count_),
        ATOMIC_LOAD(&fetch_log_count_),
        ATOMIC_LOAD(&l2s_time_),
        ATOMIC_LOAD(&svr_queue_time_),
        ATOMIC_LOAD(&fetch_time_),
        ATOMIC_LOAD(&get_cursor_batch_time_),
        ATOMIC_LOAD(&read_log_time_),
        get_cursor_batch_time_rate,
        batch_read_log_time_rate,
        ATOMIC_LOAD(&total_fetch_pkey_count_),
        ATOMIC_LOAD(&reach_upper_ts_pkey_count_),
        ATOMIC_LOAD(&reach_max_log_pkey_count_),
        ATOMIC_LOAD(&need_fetch_pkey_count_),
        ATOMIC_LOAD(&scan_round_count_),
        round_rate,
        ATOMIC_LOAD(&feedback_count_),
        ATOMIC_LOAD(&feedback_pkey_count_),
        ATOMIC_LOAD(&feedback_time_));

    reset();
  }
  inline static int64_t calc_rate_int(int64_t divisor, int64_t dividend)
  {
    return dividend == 0 ? 0 : divisor / dividend;
  }

private:
  // request count
  static int64_t locate_count_;
  static int64_t open_count_;
  static int64_t fetch_count_;
  static int64_t heartbeat_count_;

  // rpc time
  static int64_t locate_time_;
  static int64_t open_time_;
  static int64_t fetch_time_;
  static int64_t l2s_time_;
  static int64_t svr_queue_time_;
  static int64_t heartbeat_time_;

  // fetch log efficiency
  static int64_t enable_feedback_count_;
  static int64_t fetch_size_;       // bytes
  static int64_t read_disk_count_;  // times of disk reads when reading clog
  static int64_t fetch_log_count_;
  static int64_t get_cursor_batch_time_;
  static int64_t read_log_time_;
  static int64_t total_fetch_pkey_count_;
  static int64_t reach_upper_ts_pkey_count_;
  static int64_t reach_max_log_pkey_count_;
  static int64_t need_fetch_pkey_count_;
  static int64_t scan_round_count_;
  static int64_t round_rate_;

  // feedback
  static int64_t feedback_count_;
  static int64_t feedback_pkey_count_;
  static int64_t feedback_time_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
