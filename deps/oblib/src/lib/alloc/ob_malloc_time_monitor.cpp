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
#define USING_LOG_PREFIX LIB

#include "lib/alloc/ob_malloc_time_monitor.h"
#include "lib/utility/ob_print_utils.h"
using namespace oceanbase::lib;
using namespace oceanbase::common;

volatile int64_t ObMallocTimeMonitor::WARN_THRESHOLD = 100000;
void ObMallocTimeMonitor::print()
{
  char buf[1024] = {'\0'};
  int64_t pos = 0;
  for (int i = 0; i < TIME_SLOT_NUM; ++i) {
    int64_t total_cost_time = total_cost_times_[i].value();
    int64_t count = counts_[i].value();
    int64_t delta_total_cost_time = total_cost_time - last_total_cost_times_[i];
    int64_t delta_count = count - last_counts_[i];
    int64_t avg_cost_time = (0 == delta_count ? 0 : delta_total_cost_time / delta_count);
    last_total_cost_times_[i] = total_cost_time;
    last_counts_[i] = count;
    int64_t left = (0 == i ? 0 : TIME_SLOT[i-1]);
    int64_t right = TIME_SLOT[i];
    databuff_printf(buf, sizeof(buf), pos, "[MALLOC_TIME_MONITOR] [%8ld,%20ld): delta_total_cost_time=%15ld, delta_count=%15ld, avg_cost_time=%8ld\n",
                    left, right, delta_total_cost_time, delta_count, avg_cost_time);

  }
  buf[pos] = '\0';
  _OB_LOG(INFO, "[MALLOC_TIME_MONITOR] show the distribution of ob_malloc's cost_time\n%s", buf);
}