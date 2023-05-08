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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_MONITOR_H
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_MONITOR_H

#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace cdc
{
class ObCdcServiceMonitor
{
public:
  inline static void locate_count() { ATOMIC_INC(&locate_count_); }
  inline static void fetch_count() { ATOMIC_INC(&fetch_count_); }

  inline static void locate_time(const int64_t time) { (void)ATOMIC_AAF(&locate_time_, time); }
  inline static void fetch_time(const int64_t time) { (void)ATOMIC_AAF(&fetch_time_, time); }
  inline static void l2s_time(const int64_t time) { (void)ATOMIC_AAF(&l2s_time_, time); }
  inline static void svr_queue_time(const int64_t time) { (void)ATOMIC_AAF(&svr_queue_time_, time); }

  inline static void fetch_size(const int64_t size) { (void)ATOMIC_AAF(&fetch_size_, size); }
  inline static void fetch_log_count(const int64_t c) { (void)ATOMIC_AAF(&fetch_log_count_, c); }
  inline static void reach_upper_ts_pkey_count(const int64_t c) { (void)ATOMIC_AAF(&reach_upper_ts_pkey_count_, c); }
  inline static void reach_max_log_pkey_count(const int64_t c) { (void)ATOMIC_AAF(&reach_max_log_pkey_count_, c); }
  inline static void need_fetch_pkey_count(const int64_t c) { (void)ATOMIC_AAF(&need_fetch_pkey_count_, c); }
  inline static void scan_round_count(const int64_t c) { (void)ATOMIC_AAF(&scan_round_count_, c); }
  inline static void round_rate(const int64_t rate) { (void)ATOMIC_AAF(&round_rate_, rate); }

  static void reset()
  {
    ATOMIC_STORE(&locate_count_, 0);
    ATOMIC_STORE(&fetch_count_, 0);

    ATOMIC_STORE(&locate_time_, 0);
    ATOMIC_STORE(&fetch_time_, 0);
    ATOMIC_STORE(&l2s_time_, 0);
    ATOMIC_STORE(&svr_queue_time_, 0);

    ATOMIC_STORE(&fetch_size_, 0);
    ATOMIC_STORE(&fetch_log_count_, 0);
    ATOMIC_STORE(&reach_upper_ts_pkey_count_, 0);
    ATOMIC_STORE(&reach_max_log_pkey_count_, 0);
    ATOMIC_STORE(&need_fetch_pkey_count_, 0);
    ATOMIC_STORE(&scan_round_count_, 0);
    ATOMIC_STORE(&round_rate_, 0);
  }

  static void report()
  {
    int64_t round_rate = calc_rate_int(ATOMIC_LOAD(&round_rate_), ATOMIC_LOAD(&fetch_count_));

    _EXTLOG_LOG(INFO, "ObCdcServiceMonitor Report: "
                "locate_count=%ld, locate_time=%ld, "
                "fetch_count=%ld, fetch_size=%ld, fetch_log_count=%ld, "
                "l2s_time=%ld, svr_queue_time=%ld, fetch_time=%ld, "
                "reach_upper_ts_pkey_count=%ld, "
                "reach_max_log_pkey_count=%ld, need_fetch_pkey_count=%ld, "
                "scan_round_count=%ld, round_rate=%ld",
                ATOMIC_LOAD(&locate_count_), ATOMIC_LOAD(&locate_time_),
                ATOMIC_LOAD(&fetch_count_), ATOMIC_LOAD(&fetch_size_), ATOMIC_LOAD(&fetch_log_count_),
                ATOMIC_LOAD(&l2s_time_), ATOMIC_LOAD(&svr_queue_time_), ATOMIC_LOAD(&fetch_time_),
                ATOMIC_LOAD(&reach_upper_ts_pkey_count_), ATOMIC_LOAD(&reach_max_log_pkey_count_), ATOMIC_LOAD(&need_fetch_pkey_count_),
                ATOMIC_LOAD(&scan_round_count_), round_rate);

    reset();
  }
  inline static int64_t calc_rate_int(int64_t divisor, int64_t dividend)
  {
    return dividend == 0 ? 0 : divisor / dividend;
  }
private:
  // request count
  static int64_t locate_count_;
  static int64_t fetch_count_;

  // rpc time
  static int64_t locate_time_;
  static int64_t fetch_time_;
  static int64_t l2s_time_;
  static int64_t svr_queue_time_;

  // fetch log efficiency
  static int64_t fetch_size_; // bytes
  static int64_t fetch_log_count_;
  static int64_t reach_upper_ts_pkey_count_;
  static int64_t reach_max_log_pkey_count_;
  static int64_t need_fetch_pkey_count_;
  static int64_t scan_round_count_;
  static int64_t round_rate_;
};

class ObCdcFetchLogTimeStats {
public:
  ObCdcFetchLogTimeStats() {
    reset();
  }

  void reset() {
    fetch_total_time_ = 0;
    fetch_log_time_ = 0;
    fetch_palf_time_ = 0;
    fetch_archive_time_ = 0;
    fetch_log_post_process_time_ = 0;
    prefill_resp_time_ = 0;
  }

  void inc_fetch_total_time(int64_t time_cost) {
    fetch_total_time_ += time_cost;
  }

  void inc_fetch_log_time(int64_t time_cost) {
    fetch_log_time_ += time_cost;
  }

  void inc_fetch_palf_time(int64_t time_cost) {
    fetch_palf_time_ += time_cost;
  }

  void inc_fetch_archive_time(int64_t time_cost) {
    fetch_archive_time_ += time_cost;
  }

  void inc_fetch_log_post_process_time(int64_t time_cost) {
    fetch_log_post_process_time_ += time_cost;
  }

  void inc_prefill_resp_time(int64_t time_cost) {
    prefill_resp_time_ += time_cost;
  }
  //  total_time (fetch_log)
  //   ├-- fetch_log_time (ls_fetch_log)
  //   |    ├-- fetch_palf_time (fetch_log_in_palf)
  //   |    ├-- fetch_archive_time (fetch_log_in_archive)
  //   |    ├-- fetch_log_post_process_time
  //   |    |    ├-- prefill_resp_time (prefill_resp_with_log_group_entry)
  //   |    |    └-- other_post_process_time (virtual)
  //   |    └-- other_fetch_log_time (virtual)
  //   └-- other_time (virtual)
  TO_STRING_KV(
    K_(fetch_total_time),
    K_(fetch_log_time),
    K_(fetch_palf_time),
    K_(fetch_archive_time),
    K_(fetch_log_post_process_time),
    K_(prefill_resp_time)
  );

private:
  int64_t fetch_total_time_;
  int64_t fetch_log_time_;
  int64_t fetch_palf_time_;
  int64_t fetch_archive_time_;
  int64_t fetch_log_post_process_time_;
  int64_t prefill_resp_time_;
};

} // namespace cdc
} // namespace oceanbase

#endif

