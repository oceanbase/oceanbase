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

#include "ob_rs_thread_checker.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace rootserver
{

ObRsThreadChecker::ObRsThreadChecker()
  :inited_(false)
{}

ObRsThreadChecker::~ObRsThreadChecker()
{}

int ObRsThreadChecker::init()
{
  int ret = OB_SUCCESS;
  static const int64_t thread_checker_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rs_monitor_check : init twice", KR(ret));
  } else if (OB_FAIL(create(thread_checker_thread_cnt, "RsThreadChecker"))) {
    LOG_WARN("rs_monitor_check : fail to create thread", KR(ret), K(thread_checker_thread_cnt));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObRsThreadChecker::run3()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rs_monitor_check : not init", KR(ret));
  } else {
    LOG_INFO("rs_monitor_check : thread checker start");
  }
  if (OB_SUCC(ret)) {
    ObThreadCondGuard guard(get_cond());
    while (!stop_) {
      ObRsReentrantThread::check_thread_set_.loop_operation(ObRsReentrantThread::check_alert);
      if (!stop_) {
        get_cond().wait(CHECK_TIMEVAL_US / 1000);
      }
    }
  }
  LOG_WARN("rs_monitor_check : thread checker stop");
}

int ObRsThreadChecker::destroy() {
  int ret = OB_SUCCESS;
  if (inited_) {
    stop();
    wait();
    inited_ = false;
  }
  if (OB_FAIL(ObRsReentrantThread::destroy())) {
    LOG_WARN("rs_monitor_check : fail to destory", KR(ret));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
