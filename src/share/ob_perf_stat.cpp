/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log.h"
#include "share/ob_perf_stat.h"
#include "lib/time/ob_time_utility.h"
namespace oceanbase
{
namespace common
{

void ObPerfStatItem::print()
{
  if (cnt_ > 0) {
    #define KNN(name, x) #name, ::oceanbase::common::check_char_array(x)

    int64_t cnt = MAX(1, cnt_);
    last_abs_print_ts_ = common::ObTimeUtility::current_time();
    LOG_INFO("ObPerfStatItem: ", K_(name), K_(cnt), K_(total_time_us), K_(max_time_us), K_(min_time_us), KNN(avg_time_us, total_time_us_ / cnt));
    #undef KNN
  }
}

}// namespace common
}// namespace oceanbase