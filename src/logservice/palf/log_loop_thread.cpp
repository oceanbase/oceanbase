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

#include "log_loop_thread.h"
#include "palf_env_impl.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogLoopThread::LogLoopThread()
    : palf_env_impl_(NULL),
      run_interval_(DEFAULT_PALF_LOG_LOOP_INTERVAL_US),
      is_inited_(false)
{
}

LogLoopThread::~LogLoopThread()
{
  destroy();
}

int LogLoopThread::init(const bool is_normal_mode, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "LogLoopThread has been inited", K(ret));
  } else if (NULL == palf_env_impl) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(palf_env_impl));
  } else {
    palf_env_impl_ = palf_env_impl;
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    if (false == is_normal_mode) {
      run_interval_ = PALF_LOG_LOOP_INTERVAL_US_UPPER_BOUND;
    }
    is_inited_ = true;
  }

  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  PALF_LOG(INFO, "LogLoopThread init finished", K(ret));
  return ret;
}

void LogLoopThread::destroy()
{
  stop();
  PALF_LOG(INFO, "runlin trace stop");
  wait();
  PALF_LOG(INFO, "runlin trace wait");
  is_inited_ = false;
  palf_env_impl_ = NULL;
}

void LogLoopThread::run1()
{
  lib::set_thread_name("LogLoop");
  log_loop_();
  PALF_LOG(INFO, "log_loop_thread will stop");
}

void LogLoopThread::log_loop_()
{
  int64_t last_switch_state_time = OB_INVALID_TIMESTAMP;
  int64_t last_check_freeze_mode_time = OB_INVALID_TIMESTAMP;
  int64_t last_sw_freeze_time = OB_INVALID_TIMESTAMP;

  while (!has_set_stop()) {
    int tmp_ret = OB_SUCCESS;
    const int64_t start_ts = ObTimeUtility::current_time();

    auto switch_state_func = [](IPalfHandleImpl *ipalf_handle_impl) {
      return ipalf_handle_impl->check_and_switch_state();
    };
    if (start_ts - last_switch_state_time >= 10 * 1000) {
      if (OB_SUCCESS != (tmp_ret = palf_env_impl_->for_each(switch_state_func))) {
        PALF_LOG_RET(WARN, tmp_ret, "for_each switch_state_func failed", K(tmp_ret));
      }
      last_switch_state_time = start_ts;
    }

    auto switch_freeze_mode_func  = [](IPalfHandleImpl *ipalf_handle_impl) {
      return ipalf_handle_impl->check_and_switch_freeze_mode();
    };
    if (start_ts - last_check_freeze_mode_time >= 1 * 1000 * 1000) {
      if (OB_SUCCESS != (tmp_ret = palf_env_impl_->for_each(switch_freeze_mode_func))) {
        PALF_LOG_RET(WARN, tmp_ret, "for_each switch_freeze_mode_func failed", K(tmp_ret));
      }
      last_check_freeze_mode_time = start_ts;
    }

    auto try_freeze_log_func = [](IPalfHandleImpl *ipalf_handle_impl) {
      return ipalf_handle_impl->period_freeze_last_log();
    };
    if (OB_SUCCESS != (tmp_ret = palf_env_impl_->for_each(try_freeze_log_func))) {
      PALF_LOG_RET(WARN, tmp_ret, "for_each try_freeze_log_func failed", K(tmp_ret));
    }

    const int64_t round_cost_time = ObTimeUtility::current_time() - start_ts;
    int32_t sleep_ts = run_interval_ - static_cast<const int32_t>(round_cost_time);
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    ob_usleep(sleep_ts);

    if (REACH_TENANT_TIME_INTERVAL(5 * 1000 * 1000)) {
      PALF_LOG(INFO, "LogLoopThread round_cost_time", K(round_cost_time));
    }
  }
}
} // namespace palf
} // namespace oceanbase
