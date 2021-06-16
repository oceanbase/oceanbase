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

#ifndef TBSYS_MUTEX_H
#define TBSYS_MUTEX_H

#include <pthread.h>
#include "lib/lock/Lock.h"

namespace tbutil {
/**
@brief Mutex, implemented as a simple data structure
Mutex non-recursive lock, you need to pay attention to the following points when using:
1.Do not call lock a second time in the same thread
2.Unless the calling thread holds a certain mutex, do not call unlock for the mutex
*/
class Mutex {
public:
  typedef LockT<Mutex> Lock;
  typedef TryLockT<Mutex> TryLock;

  Mutex();
  ~Mutex();

  /**
   * @brief The lock function attempts to acquire the mutex.
   * If the mutex is locked, it will suspend the calling thread until the mutex becomes available.
   * Once the calling thread obtains the mutex, the call returns immediately
   */
  void lock() const;

  /**
   * @brief The tryLock function attempts to acquire the mutex.
   * If the mutex is available, the mutex will be locked, and the call will return true.
   * If other threads lock the mutex, the call returns false
   *
   * @return
   */
  bool tryLock() const;

  /**
   * @brief The unlock function unlocks the mutex
   */
  void unlock() const;

  /**
   * @brief Whether it has been locked
   *
   * @return
   */
  bool willUnlock() const;

private:
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);

  struct LockState {
    pthread_mutex_t* mutex;
  };

  void unlock(LockState&) const;
  void lock(LockState&) const;
  mutable pthread_mutex_t _mutex;

  friend class Cond;
};
}  // namespace tbutil
#endif
