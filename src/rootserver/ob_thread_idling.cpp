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

#define USING_LOG_PREFIX RS

#include "ob_thread_idling.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/thread.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

ObThreadIdling::ObThreadIdling(volatile bool &stop)
    : cond_(), stop_(stop), wakeup_cnt_(0)
{
  cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT);
}

void ObThreadIdling::wakeup()
{
  ObThreadCondGuard guard(cond_);
  wakeup_cnt_++;
  int tmp_ret = cond_.broadcast();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "condition broadcast fail", K(tmp_ret));
  }
}

int ObThreadIdling::idle()
{
  int ret = OB_SUCCESS;
  const int64_t max_time_us = INT64_MAX;
  if (OB_FAIL(idle(max_time_us))) {
    LOG_WARN("idle failed", K(ret));
  }
  return ret;
}

int ObThreadIdling::idle(const int64_t max_idle_time_us)
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  const int64_t begin_time_ms = ObTimeUtility::current_time() / 1000;
  while (!stop_ && wakeup_cnt_ == 0 && OB_SUCC(ret)) {
    const int64_t now_ms = ObTimeUtility::current_time() / 1000;
    const int64_t idle_time_ms = std::min(max_idle_time_us, get_idle_interval_us()) / 1000;
    int64_t wait_time_ms = begin_time_ms + idle_time_ms - now_ms;
    IGNORE_RETURN lib::Thread::update_loop_ts();
    if (wait_time_ms <= 0) {
      break;
    }
    wait_time_ms = std::min(1000l, wait_time_ms);
    if (OB_FAIL(cond_.wait(static_cast<int>(wait_time_ms)))) {
      ret = OB_SUCCESS;
    } else {
      break;
    }
  }
  if (stop_) {
    ret = OB_CANCELED;
  }
  wakeup_cnt_ = 0;
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
