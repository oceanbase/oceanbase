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

#ifndef TBSYS_RMUTEX_H
#define TBSYS_RMUTEX_H
#include "lib/lock/Lock.h"
#include <pthread.h>
namespace tbutil {
class Cond;
/**
 * @brief RecMutex implements recursive mutex
 * The use of non-recursive mutexes is the same. When using recursive mutexes, you must follow some simple rules:
 * 1.Unless the calling thread holds the lock, do not call unlock for a mutex
 * 2.In order for the mutex to be acquired by other threads,
 * the number of times you call unlock must be the same as the number of times you call lock
 * (in the internal implementation of recursive mutex, there is a counter initialized to zero.
 * Every time lock is called, the counter is It will increase by one, and each time unlock is called,
 * the counter will decrease by one; when the counter returns to zero, another thread can acquire the mutex)
 */
class RecMutex {
public:
  typedef LockT<RecMutex> Lock;
  typedef TryLockT<RecMutex> TryLock;

  RecMutex();
  ~RecMutex();

  /**
   * @brief The lock function attempts to acquire the mutex.
   * If the mutex is locked by another thread, it will suspend the calling thread until the mutex becomes available.
   * If the mutex is available or has been locked by the calling thread, the call will lock the mutex and return
   * immediately
   */
  void lock() const;

  /**
   * @brief The function of tryLock is similar to lock, but if the mutex has been locked by another thread,
   * it will not block the caller, but will return false.
   * Otherwise the return value is true
   * @return
   */
  bool tryLock() const;

  /**
   * @brief The unlock function unlocks the mutex
   */
  void unlock() const;

  bool willUnlock() const;

private:
  // noncopyable
  RecMutex(const RecMutex&);
  RecMutex& operator=(const RecMutex&);

  struct LockState {
    pthread_mutex_t* mutex;
    int count;
  };

  void unlock(LockState&) const;
  void lock(LockState&) const;

  friend class Cond;

  mutable pthread_mutex_t _mutex;

  mutable int _count;
};
}  // namespace tbutil
#endif
