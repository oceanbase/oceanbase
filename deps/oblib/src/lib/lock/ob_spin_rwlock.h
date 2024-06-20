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

#ifndef  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#define  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "lib/ob_define.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/lock/ob_latch.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace common
{
class SpinRWLock
{
public:
  explicit SpinRWLock(uint32_t latch_id = ObLatchIds::DEFAULT_SPIN_RWLOCK)
      : latch_(), latch_id_(latch_id)
  {
  }
  ~SpinRWLock()
  {
  }
  int init(const lib::ObMemAttr &mem_attr)
  {
    UNUSED(mem_attr);
    return OB_SUCCESS;
  }
  void destroy() {}
public:
  void set_latch_id(const uint32_t latch_id) { latch_id_ = latch_id; }
  inline bool try_rdlock() { return OB_SUCCESS == latch_.try_rdlock(latch_id_); }
  inline int rdlock(const int64_t abs_timeout_us = INT64_MAX) { return latch_.rdlock(latch_id_, abs_timeout_us); }
  inline int wrlock(const int64_t abs_timeout_us = INT64_MAX) { return latch_.wrlock(latch_id_, abs_timeout_us); }
  inline bool try_wrlock() { return OB_SUCCESS == latch_.try_wrlock(latch_id_); }
  inline int unlock() { return latch_.unlock(); }
  inline void rdunlock() { unlock(); }
  inline void wrunlock() { unlock(); }
  inline void enable_record_stat(bool enable) { latch_.enable_record_stat(enable); }
private:
  ObLatch latch_;
  uint32_t latch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(SpinRWLock);
};

class SpinRLockGuard
{
public:
  explicit SpinRLockGuard(const SpinRWLock &lock)
      : lock_(const_cast<SpinRWLock&>(lock)), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
    }
  }
  ~SpinRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock, ", K_(ret));
      }
    }
  }
  inline int get_ret() const { return ret_; }
private:
  SpinRWLock &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(SpinRLockGuard);
};

class SpinRLockManualGuard
{
public:
  explicit SpinRLockManualGuard()
      : lock_(nullptr), ret_(OB_SUCCESS)
  {
  }
  ~SpinRLockManualGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_) && OB_NOT_NULL(lock_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_->unlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock, ", K_(ret));
      } else {
        lock_ = nullptr;
      }
    }
  }
  void lock(SpinRWLock &lock) {
    lock_ = &lock;
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_->rdlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
    }
  }
  inline int get_ret() const { return ret_; }
private:
  SpinRWLock *lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(SpinRLockManualGuard);
};

class SpinWLockGuard
{
public:
  explicit SpinWLockGuard(const SpinRWLock &lock)
      : lock_(const_cast<SpinRWLock&>(lock)), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to write lock, ", K_(ret));
    }
  }
  ~SpinWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock, ", K_(ret));
      }
    }
  }
  inline int get_ret() const { return ret_; }
private:
  SpinRWLock &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(SpinWLockGuard);
};
}
}

#endif //OCEANBASE_COMMON_SPIN_RWLOCK_H_
