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

#ifndef OCEANBASE_LOG_OB_CLOG_TIMER_UTILITY_H_
#define OCEANBASE_LOG_OB_CLOG_TIMER_UTILITY_H_

#include <stdint.h>
#include "clog/ob_clog_config.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
namespace clog {
class ObLogTimerUtility {
public:
  ObLogTimerUtility() : bt1_(common::OB_INVALID_TIMESTAMP), bt2_(common::OB_INVALID_TIMESTAMP)
  {}

public:
  void start_timer()
  {
    if (ENABLE_CLOG_PERF) {
      bt1_ = common::ObTimeUtility::current_time();
    }
  }
  void finish_timer(const char* file, const int64_t line, const int64_t timeout)
  {
    if (ENABLE_CLOG_PERF) {
      if (common::OB_INVALID_TIMESTAMP == bt1_) {
        CLOG_LOG(INFO, "ObCLogTimerUtility doesn't call start_timer");
      } else {
        bt2_ = common::ObTimeUtility::current_time();
        if (bt2_ - bt1_ > timeout) {
          CLOG_LOG(WARN, "cost too much time", K(file), K(line), "time", bt2_ - bt1_);
        }
      }
      bt1_ = common::ObTimeUtility::current_time();
      bt2_ = common::OB_INVALID_TIMESTAMP;
    }
  }

private:
  int64_t bt1_;
  int64_t bt2_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTimerUtility);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_LOG_OB_CLOG_TIMER_UTILITY_H_
