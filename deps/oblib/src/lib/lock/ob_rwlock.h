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

#ifndef OB_RW_LOCK_H
#define OB_RW_LOCK_H

#include <pthread.h>
#include "stdint.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase
{
namespace obsys
{
template <class T>
class ObLockGuardBase
{
public:
  [[nodiscard]] ObLockGuardBase(const T& lock, bool block = true);
  ~ObLockGuardBase();
  bool acquired() const { return acquired_; }

private:
    const T& lock_;
    mutable bool acquired_;
};

enum LockMode
{
  NO_PRIORITY,
  WRITE_PRIORITY,
  READ_PRIORITY
};

template <class T = ObLatch>
class ObRLock
{
public:
  explicit ObRLock(T* lock, uint32_t latch_id) : rlock_(lock), latch_id_(latch_id) {}
  ~ObRLock() {}
  int lock() const;
  int trylock() const;
  int unlock() const;
  void set_latch_id(uint32_t latch_id);
private:
  mutable T* rlock_;
  uint32_t latch_id_;
};

template <class T = ObLatch>
class ObWLock
{
public:
  explicit ObWLock(T* lock, uint32_t latch_id) : wlock_(lock), latch_id_(latch_id) {}
  ~ObWLock() {}
  int lock() const;
  int trylock() const;
  int unlock() const;
  void set_latch_id(uint32_t latch_id);
private:
  mutable T* wlock_;
  uint32_t latch_id_;
};

template <LockMode lockMode = NO_PRIORITY>
class ObRWLock
{
public:
  ObRWLock(uint32_t latch_id = common::ObLatchIds::DEFAULT_RWLOCK)
  : rlock_(&rwlock_, latch_id),
    wlock_(&rwlock_, latch_id) {}
  ~ObRWLock() {}
  ObRLock<ObLatch>* rlock() const { return const_cast<ObRLock<ObLatch>*>(&rlock_); }
  ObWLock<ObLatch>* wlock() const { return const_cast<ObWLock<ObLatch>*>(&wlock_); }
  void set_latch_id(uint32_t latch_id);
private:
  ObLatch rwlock_;
  ObRLock<ObLatch> rlock_;
  ObWLock<ObLatch> wlock_;
};

template<>
class ObRWLock<WRITE_PRIORITY> {
  public:
  ObRWLock(uint32_t latch_id = common::ObLatchIds::DEFAULT_RWLOCK)
  : rlock_(&rwlock_, latch_id),
    wlock_(&rwlock_, latch_id)
  {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&rwlock_, &attr);
  }
  ~ObRWLock()
  {
    pthread_rwlock_destroy(&rwlock_);
  }
  ObRLock<pthread_rwlock_t>* rlock() const { return const_cast<ObRLock<pthread_rwlock_t>*>(&rlock_); }
  ObWLock<pthread_rwlock_t>* wlock() const { return const_cast<ObWLock<pthread_rwlock_t>*>(&wlock_); }
  void set_latch_id(uint32_t latch_id);
private:
  pthread_rwlock_t rwlock_;
  ObRLock<pthread_rwlock_t> rlock_;
  ObWLock<pthread_rwlock_t> wlock_;
};

template<LockMode lockMode = NO_PRIORITY>
class ObRLockGuard
{
public:
  [[nodiscard]] ObRLockGuard(const ObRWLock<>& rwlock, bool block = true) : guard_((*rwlock.rlock()), block) {}
  ~ObRLockGuard(){}
  bool acquired() { return guard_.acquired(); }
private:
  ObLockGuardBase<ObRLock<>> guard_;
};

template<>
class ObRLockGuard<WRITE_PRIORITY>
{
public:
  [[nodiscard]] ObRLockGuard(const ObRWLock<WRITE_PRIORITY>& rwlock, bool block = true) : guard_((*rwlock.rlock()), block) {}
  ~ObRLockGuard(){}
  bool acquired() { return guard_.acquired(); }
private:
  ObLockGuardBase<ObRLock<pthread_rwlock_t>> guard_;
};

template<LockMode lockMode = NO_PRIORITY>
class ObWLockGuard
{
public:
  [[nodiscard]] ObWLockGuard(const ObRWLock<>& rwlock, bool block = true) : guard_((*rwlock.wlock()), block) {}
  ~ObWLockGuard(){}
  bool acquired() { return guard_.acquired(); }
private:
  ObLockGuardBase<ObWLock<>> guard_;
};

template<>
class ObWLockGuard<WRITE_PRIORITY>
{
public:
  [[nodiscard]] ObWLockGuard(const ObRWLock<WRITE_PRIORITY>& rwlock, bool block = true) : guard_((*rwlock.wlock()), block) {}
  ~ObWLockGuard(){}
  bool acquired() { return guard_.acquired(); }
private:
  ObLockGuardBase<ObWLock<pthread_rwlock_t>> guard_;
};

}
}
#endif
