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

#ifndef OB_RECURSIVE_MUTEX_H_
#define OB_RECURSIVE_MUTEX_H_

#include "lib/lock/ob_latch.h"
#include "lib/lock/ob_lock_guard.h"

namespace oceanbase
{
namespace common
{
class ObRecursiveMutex
{
public:
  explicit ObRecursiveMutex(const uint32_t latch_id);
  ~ObRecursiveMutex();
  int lock();
  int unlock();
  int trylock();
  int rdlock();
  int wr2rdlock();
private:
  ObLatch latch_;
  uint32_t latch_id_;
  uint32_t lock_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveMutex);
};

inline ObRecursiveMutex::ObRecursiveMutex(const uint32_t latch_id)
  : latch_(), latch_id_(latch_id), lock_cnt_(0)
{
}

inline ObRecursiveMutex::~ObRecursiveMutex()
{
}

inline int ObRecursiveMutex::lock()
{
  int ret = OB_SUCCESS;
  if (latch_.is_wrlocked_by()) {
    ++lock_cnt_;
  } else {
    if (OB_FAIL(latch_.wrlock(latch_id_))) {
      COMMON_LOG(WARN, "Fail to lock ObRecursiveMutex, ", K_(latch_id), K(ret));
    } else {
      ++lock_cnt_;
    }
  }
  return ret;
}

inline int ObRecursiveMutex::unlock()
{
  int ret = OB_SUCCESS;
  if (latch_.is_rdlocked()) {
    if (OB_FAIL(latch_.unlock())) {
      COMMON_LOG(WARN, "Fail to unlock the ObRecursiveMutex, ", K_(latch_id), K(ret));
    }
  } else if (0 == --lock_cnt_) {
    if (OB_FAIL(latch_.unlock())) {
      COMMON_LOG(WARN, "Fail to unlock the ObRecursiveMutex, ", K_(latch_id), K(ret));
    }
  }
  return ret;
}

inline int ObRecursiveMutex::trylock()
{
  int ret = OB_SUCCESS;
  if (latch_.is_wrlocked_by()) {
    ++lock_cnt_;
  } else {
    if (OB_FAIL(latch_.try_wrlock(latch_id_))) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        COMMON_LOG(WARN, "Fail to try lock ObRecursiveMutex, ", K_(latch_id), K(ret));
      }
    } else {
      ++lock_cnt_;
    }
  }
  return ret;
}

inline int ObRecursiveMutex::rdlock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(latch_.rdlock(latch_id_))) {
    COMMON_LOG(WARN, "Fail to rdlock ObRecursiveMutex, ", K_(latch_id), K(ret));
  }
  return ret;
}

inline int ObRecursiveMutex::wr2rdlock()
{
  int ret = OB_SUCCESS;
  if (latch_.is_wrlocked_by()) {
    if (lock_cnt_ != 1) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "lock count is not 1, can not to downgrade, ", K_(latch_id), K_(lock_cnt), K(ret));
    } else {
      --lock_cnt_;
      if (OB_FAIL(latch_.wr2rdlock())) {
        COMMON_LOG(WARN, "Fail to downgrade ObRecursiveMutex, ", K_(latch_id), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ObRecursiveMutex is not wrlocked, ", K_(latch_id), K(ret));
  }
  return ret;
}

typedef lib::ObLockGuard<ObRecursiveMutex> ObRecursiveMutexGuard;
}
}
#endif
