/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_PERF_STAT_H_
#define OCEANBASE_STORAGE_OB_PERF_STAT_H_
#include "share/ob_occam_time_guard.h"

namespace oceanbase
{
namespace common
{
class ObPerfStatItem
{
public:
  ObPerfStatItem(const char *name)
  : name_(name), cnt_(0), total_time_us_(0),
    max_time_us_(0), min_time_us_(INT64_MAX), last_abs_print_ts_(0) {}
  ~ObPerfStatItem() {}
  void reset() { cnt_ = 0; total_time_us_ = 0; max_time_us_ = 0; min_time_us_ = INT64_MAX; }
  void update(int64_t time_us) {
    cnt_++;
    total_time_us_ += time_us;
    if (time_us > max_time_us_) max_time_us_ = time_us;
    if (time_us < min_time_us_ || cnt_ == 1) min_time_us_ = time_us;
  }
  void print();
public:
  const char *name_;
  int64_t cnt_;
  int64_t total_time_us_;
  int64_t max_time_us_;
  int64_t min_time_us_;
  int64_t last_abs_print_ts_;
};

class ObPerfStatGuard
{
  const static int64_t DEFAULT_PERF_STAT_INTERVAL = 20_s; // 20 seconds
public:
  ObPerfStatGuard(ObPerfStatItem &stat) : stat_(stat), abs_start_time_ts_(common::ObTimeUtility::current_time()) {}
  ~ObPerfStatGuard()
  {
    int64_t curr_time_ts = common::ObTimeUtility::current_time();
    stat_.update(curr_time_ts - abs_start_time_ts_);
    if (curr_time_ts - stat_.last_abs_print_ts_ > DEFAULT_PERF_STAT_INTERVAL) {
      stat_.print();
    }
  }
public:
  ObPerfStatItem &stat_;
  int64_t abs_start_time_ts_;
};

constexpr static int64_t DEFAULT_TIMEGUARD_US = 100_ms;
#define ENABLE_PERF_STAT 1
#ifdef ENABLE_PERF_STAT
#define PERF_GUARD_INIT(name) ObPerfStatGuard perf_guard(name)
#define PERF_STAT_ITEM(name) thread_local ObPerfStatItem name(#name)
#define EXTERN_PERF_STAT_ITEM(name) extern thread_local ObPerfStatItem name
#define PERF_TIMEGUARD_WITH_MOD_INIT(mod) TIMEGUARD_INIT(mod, DEFAULT_TIMEGUARD_US/2)
#else
#define PERF_GUARD_INIT(name)
#define PERF_STAT_ITEM(name)
#define EXTERN_PERF_STAT_ITEM(name)
#define PERF_TIMEGUARD_WITH_MOD_INIT(mod) TIMEGUARD_INIT(mod, DEFAULT_TIMEGUARD_US)
#endif

class ObTaskPerfStat
{
public:
  ObTaskPerfStat() :
     total_cnt_(0),
     last_executed_cnt_(0),
     last_ave_execute_time_(0),
     expected_thread_cnt_(0),
     total_estimate_execute_time_(0) {}
  ~ObTaskPerfStat() {}
public:
  int64_t total_cnt_;
  int64_t last_executed_cnt_;
  int64_t last_ave_execute_time_;
  int64_t expected_thread_cnt_;
  int64_t total_estimate_execute_time_;
};

}// namespace common
}// namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_SHARED_META_SERVICE_
