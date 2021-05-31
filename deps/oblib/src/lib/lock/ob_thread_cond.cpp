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

#include "lib/lock/ob_thread_cond.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace common {
ObThreadCond::ObThreadCond()
    : mutex_(), cond_(), event_no_(0), cond_inited_(false), mutex_inited_(false), is_inited_(false)
{}

ObThreadCond::~ObThreadCond()
{
  destroy();
}

int ObThreadCond::init(const int32_t event_no)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The thread cond has been inited, ", K(this), K(common::lbt()), K(ret));
  } else if (!mutex_inited_ && OB_UNLIKELY(0 != (tmp_ret = pthread_mutex_init(&mutex_, NULL)))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "Fail to init pthread mutex, ", K(tmp_ret), K(ret));
  } else {
    mutex_inited_ = true;
    if (!cond_inited_ && OB_UNLIKELY(0 != (tmp_ret = pthread_cond_init(&cond_, NULL)))) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "Fail to init pthread cond, ", K(tmp_ret), K(ret));
    } else {
      event_no_ = event_no;
      cond_inited_ = true;
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObThreadCond::destroy()
{
  int ret = 0;
  if (cond_inited_) {
    if (OB_UNLIKELY(0 != (ret = pthread_cond_destroy(&cond_)))) {
      COMMON_LOG(WARN, "Fail to destroy pthread cond, ", K(ret));
    } else {
      cond_inited_ = false;
    }
  }

  if (mutex_inited_) {
    if (OB_UNLIKELY(0 != (ret = pthread_mutex_destroy(&mutex_)))) {
      COMMON_LOG(WARN, "Fail to destroy pthread mutex, ", K(ret));
    } else {
      mutex_inited_ = false;
    }
  }

  is_inited_ = false;
}

int ObThreadCond::lock()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret), K(lbt()));
  } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_mutex_lock(&mutex_)))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "Fail to lock pthread mutex, ", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObThreadCond::unlock()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret), K(lbt()));
  } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_mutex_unlock(&mutex_)))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "Fail to unlock pthread mutex, ", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObThreadCond::wait(const uint64_t time_ms)
{
  return wait_us(time_ms * 1000);
}

int ObThreadCond::wait_us(const uint64_t time_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret), K(lbt()));
  } else {
    ObWaitEventGuard guard(event_no_, time_us / 1000, reinterpret_cast<int64_t>(this));
    if (0 == time_us) {
      if (OB_UNLIKELY(0 != (tmp_ret = pthread_cond_wait(&cond_, &mutex_)))) {
        ret = OB_ERR_SYS;
        COMMON_LOG(WARN, "Fail to cond wait, ", K(tmp_ret), K(ret));
      }
    } else {
      struct timeval curtime;
      struct timespec abstime;
      if (OB_UNLIKELY(0 != (tmp_ret = gettimeofday(&curtime, NULL)))) {
        ret = OB_ERR_SYS;
        COMMON_LOG(WARN, "Fail to get time, ", K(tmp_ret), K(ret));
      } else {
        uint64_t us = (static_cast<int64_t>(curtime.tv_sec) * static_cast<int64_t>(1000000) +
                       static_cast<int64_t>(curtime.tv_usec) + time_us);

        abstime.tv_sec = static_cast<int>(us / static_cast<uint64_t>(1000000));
        abstime.tv_nsec = static_cast<int>(us % static_cast<uint64_t>(1000000)) * 1000;
        if (OB_UNLIKELY(0 != (tmp_ret = pthread_cond_timedwait(&cond_, &mutex_, &abstime)))) {
          if (ETIMEDOUT != tmp_ret) {
            ret = OB_ERR_SYS;
            COMMON_LOG(WARN, "Fail to timed cond wait, ", K(time_us), K(tmp_ret), K(ret));
          } else {
            ret = OB_TIMEOUT;
          }
        }
      }
    }
  }

  return ret;
}

int ObThreadCond::signal()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_cond_signal(&cond_)))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "Fail to signal thread cond, ", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObThreadCond::broadcast()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread cond has not been inited, ", K(this), K(common::lbt()), K(ret));
  } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_cond_broadcast(&cond_)))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "Fail to broadcast thread cond, ", K(tmp_ret), K(ret));
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
