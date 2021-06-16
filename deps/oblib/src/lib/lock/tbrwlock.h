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

#ifndef TBSYS_RW_LOCK_H
#define TBSYS_RW_LOCK_H

#include <pthread.h>

namespace oceanbase {
namespace obsys {
enum ELockMode { NO_PRIORITY, WRITE_PRIORITY, READ_PRIORITY };

/**
 * @brief Encapsulation of read lock in linux-thread read-write lock
 */
class CRLock {
public:
  explicit CRLock(pthread_rwlock_t* lock) : _rlock(lock)
  {}
  ~CRLock()
  {}

  int lock() const;
  int tryLock() const;
  int unlock() const;

private:
  mutable pthread_rwlock_t* _rlock;
};

/**
 * @brief Encapsulation of write lock in linux-thread read-write lock
 */
class CWLock {
public:
  explicit CWLock(pthread_rwlock_t* lock) : _wlock(lock)
  {}
  ~CWLock()
  {}

  int lock() const;
  int tryLock() const;
  int unlock() const;

private:
  mutable pthread_rwlock_t* _wlock;
};

class CRWLock {
public:
  CRWLock(ELockMode lockMode = NO_PRIORITY);
  ~CRWLock();

  CRLock* rlock() const
  {
    return _rlock;
  }
  CWLock* wlock() const
  {
    return _wlock;
  }

private:
  CRLock* _rlock;
  CWLock* _wlock;
  pthread_rwlock_t _rwlock;
};

template <class T>
class CLockGuard {
public:
  CLockGuard(const T& lock, bool block = true) : _lock(lock)
  {
    _acquired = !(block ? _lock.lock() : _lock.tryLock());
  }

  ~CLockGuard()
  {
    if (_acquired)
      _lock.unlock();
  }

  bool acquired() const
  {
    return _acquired;
  }

private:
  const T& _lock;
  mutable bool _acquired;
};
/**
 * @brief Helper class for read lock in linux thread lock
 */
class CRLockGuard {
public:
  CRLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.rlock()), block)
  {}
  ~CRLockGuard()
  {}

  bool acquired()
  {
    return _guard.acquired();
  }

private:
  CLockGuard<CRLock> _guard;
};

/**
 * @brief Helper class for write lock in linux thread lock
 */
class CWLockGuard {
public:
  CWLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.wlock()), block)
  {}
  ~CWLockGuard()
  {}

  bool acquired()
  {
    return _guard.acquired();
  }

private:
  CLockGuard<CWLock> _guard;
};
}  // namespace obsys
}  // namespace oceanbase

#endif
