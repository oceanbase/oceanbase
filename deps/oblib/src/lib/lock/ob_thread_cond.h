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

#ifndef OB_THREAD_COND_H_
#define OB_THREAD_COND_H_

#include <pthread.h>
#include "lib/wait_event/ob_wait_event.h"
#include "lib/ob_define.h"
#include "lib/lock/ob_lock_guard.h"

namespace oceanbase
{
namespace common
{
class ObThreadCond
{
public:
  ObThreadCond()
    : mutex_(),
      cond_(),
      event_no_(0),
      cond_inited_(false),
      mutex_inited_(false),
      is_inited_(false) {}
  virtual ~ObThreadCond()
  {
    destroy();
  }
  int init(const int32_t event_no);
  void destroy();
  int lock()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret), KCSTRING(lbt()));
    } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_mutex_lock(&mutex_)))) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "Fail to lock pthread mutex, ", K(tmp_ret), K(ret));
    }
    return ret;
  }
  int unlock()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "The thread cond has not been inited, ", K(ret), KCSTRING(lbt()));
    } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_mutex_unlock(&mutex_)))) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "Fail to unlock pthread mutex, ", K(tmp_ret), K(ret));
    }
    return ret;
  }
  int wait(const uint64_t time_ms = 0)
  {
    return wait_us(time_ms * 1000);
  }
  int wait_us(const uint64_t time_us = 0);
  int signal()
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
  int broadcast()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "The thread cond has not been inited, ", K(this), KCSTRING(common::lbt()), K(ret));
    } else if (OB_UNLIKELY(0 != (tmp_ret = pthread_cond_broadcast(&cond_)))) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "Fail to broadcast thread cond, ", K(tmp_ret), K(ret));
    }
    return ret;
  }
  bool is_inited() { return is_inited_; } //for reuse, no need init again
private:
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
  int32_t event_no_;
  bool cond_inited_;
  bool mutex_inited_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObThreadCond);
};

typedef lib::ObLockGuard<ObThreadCond> ObThreadCondGuard;

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_THREAD_COND_H_ */
