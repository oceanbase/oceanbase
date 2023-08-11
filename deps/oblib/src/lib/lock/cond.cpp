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

#include <sys/time.h>
#include "lib/lock/cond.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

namespace obutil
{
Cond::Cond()
{

    int rt = pthread_condattr_init(&_attr);
    if (0 != rt) {
      _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to init cond attr, err=%d", rt);
    }
    // Set the attribute to use CLOCK_MONOTONIC clock source
    rt = pthread_condattr_setclock(&_attr, CLOCK_MONOTONIC);
    if (0 != rt) {
      _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to set MONOTONIC Clock, err=%d", rt);
    }

    rt = pthread_cond_init(&_cond, &_attr);
    if (0 != rt) {
      _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to init cond, err=%d", rt);
    }
}

Cond::~Cond()
{
  int rt = pthread_condattr_destroy(&_attr);
  if (0 != rt) {
    _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to destroy cond attr, err=%d", rt);
  }
  rt = pthread_cond_destroy(&_cond);
  if (0 != rt) {
    _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to destroy cond, err=%d", rt);
  }
}

void Cond::signal()
{
    const int rt = pthread_cond_signal(&_cond);
    if (0 != rt) {
      _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to signal condition, err=%d", rt);
    }
}

void Cond::broadcast()
{
    const int rt = pthread_cond_broadcast(&_cond);
    if (0 != rt) {
      _OB_LOG_RET(WARN, OB_ERR_SYS, "Failed to broadcast condition, err=%d", rt);
    }
}
}//end namespace obutil
