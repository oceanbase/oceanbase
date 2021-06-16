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

namespace oceanbase {
namespace common {
class ObThreadCond {
public:
  ObThreadCond();
  virtual ~ObThreadCond();
  int init(const int32_t event_no);
  void destroy();
  int lock();
  int unlock();
  int wait(const uint64_t time_ms = 0);
  int wait_us(const uint64_t time_us = 0);
  int signal();
  int broadcast();

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
