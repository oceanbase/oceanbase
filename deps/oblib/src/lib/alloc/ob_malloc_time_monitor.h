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

#ifndef OCEABASE_MALLOC_TIME_MONITOR_H_
#define OCEABASE_MALLOC_TIME_MONITOR_H_
#include "lib/alloc/alloc_struct.h"
#include "lib/metrics/ob_counter.h"
#include "lib/oblog/ob_log.h"
namespace oceanbase
{
namespace lib
{
class ObMallocTimeMonitor
{
public:
  static volatile int64_t WARN_THRESHOLD;
  static constexpr const int64_t TIME_SLOT[] = {10, 100, 1000, 10000, 100000, 1000000, INT64_MAX};
  static const int64_t TIME_SLOT_NUM = ARRAYSIZEOF(TIME_SLOT);
  ObMallocTimeMonitor()
  {
    MEMSET(this, 0, sizeof(*this));
  }

  static ObMallocTimeMonitor &get_instance()
  {
    static ObMallocTimeMonitor instance;
    return instance;
  }
  void inc(int64_t cost_time)
  {
    for (int i = 0; i < TIME_SLOT_NUM; ++i) {
      if (cost_time < TIME_SLOT[i]) {
        total_cost_times_[i].inc(cost_time);
        counts_[i].inc(1);
        break;
      }
    }
  }
  void record_malloc_time(ObBasicTimeGuard& time_guard, const int64_t size, const ObMemAttr& attr)
  {
    const int64_t cost_time = time_guard.get_diff();
    inc(cost_time);
    if (OB_UNLIKELY(cost_time > WARN_THRESHOLD)) {
      const int64_t buf_len = 1024;
      char buf[buf_len] = {'\0'};
      int64_t pos = attr.to_string(buf, buf_len);
      (void)logdata_printf(buf, buf_len, pos, ", size=%ld, ", size);
      pos += time_guard.to_string(buf + pos, buf_len - pos);
      int64_t tid = GETTID();
      fprintf(stderr, "[%ld]OB_MALLOC COST TOO MUCH TIME, cost_time=%ld, %.*s\n", tid, cost_time, static_cast<int>(pos), buf);
    }
  }
  void print();
private:
  int64_t last_total_cost_times_[TIME_SLOT_NUM];
  int64_t last_counts_[TIME_SLOT_NUM];
  common::ObTCCounter total_cost_times_[TIME_SLOT_NUM];
  common::ObTCCounter counts_[TIME_SLOT_NUM];
};
} // end of namespace lib
} // end of namespace oceanbase

#endif