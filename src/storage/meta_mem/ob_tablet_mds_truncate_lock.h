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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_TRUNCATE_LOCK_H
#define OCEANBASE_STORAGE_OB_TABLET_MDS_TRUNCATE_LOCK_H

#include "deps/oblib/src/lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace storage
{

struct ObTabletMDSTruncateLock : public common::SpinRWLock
{
  ObTabletMDSTruncateLock() : lock_times_(0), lock_owner_(0) {}
  bool try_exclusive_lock() {
    bool ret = false;
    if (ATOMIC_LOAD(&lock_owner_) == ob_gettid()) {
      ret = true;
    } else if (OB_LIKELY(ret = common::SpinRWLock::try_wrlock())) {
      ATOMIC_STORE(&lock_owner_, ob_gettid());
    }
    if (OB_LIKELY(ret)) {
      ATOMIC_AAF(&lock_times_, 1);
    }
    return ret;
  }
  void exclusive_lock() {
    if (!is_exclusive_locked_by_self()) {
      common::SpinRWLock::wrlock();
      ATOMIC_STORE(&lock_owner_, ob_gettid());
    }
    ATOMIC_AAF(&lock_times_, 1);
  }
  void exclusive_unlock() {
    uint32_t lock_times = ATOMIC_LOAD(&lock_times_);
    if (OB_UNLIKELY(ATOMIC_LOAD(&lock_owner_) != ob_gettid()) || lock_times <= 0) {
      ob_abort();
    } else if (ATOMIC_AAF(&lock_times_, -1) == 0) {
      common::SpinRWLock::wrunlock();
      ATOMIC_STORE(&lock_owner_, 0);
    }
  }
  bool is_exclusive_locked_by_self() {
    return ATOMIC_LOAD(&lock_owner_) == ob_gettid();
  }
private:
  uint32_t lock_times_;
  int64_t lock_owner_;
};

struct ObTabletMdsSharedLockGuard
{
  ObTabletMdsSharedLockGuard() : lock_(nullptr) {}
  ObTabletMdsSharedLockGuard(ObTabletMDSTruncateLock &lock) : lock_(nullptr) {
    if (lock.is_exclusive_locked_by_self()) {
      // do nothing
    } else {
      lock.rdlock();
      lock_ = &lock;
    }
  }
  ~ObTabletMdsSharedLockGuard() {
    if (lock_) {
      lock_->rdunlock();
    }
  }
  ObTabletMDSTruncateLock *lock_;
};

struct ObTabletMdsExclusiveLockGuard
{
  ObTabletMdsExclusiveLockGuard() : lock_(nullptr) {}
  ObTabletMdsExclusiveLockGuard(ObTabletMDSTruncateLock &lock) : lock_(nullptr) { set(lock); }
  void set(ObTabletMDSTruncateLock &lock) {
    if (OB_UNLIKELY(lock_)) {
      ob_abort();
    } else {
      lock.exclusive_lock();
      lock_ = &lock;
    }
  }
  ~ObTabletMdsExclusiveLockGuard() {
    if (lock_) {
      lock_->exclusive_unlock();
    }
  }
  ObTabletMDSTruncateLock *lock_;
};

}
}

#endif