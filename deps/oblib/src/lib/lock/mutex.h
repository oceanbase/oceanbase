/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef MUTEX_H
#define MUTEX_H

#include <pthread.h>
#include "lib/lock/ob_lock.h"

namespace obutil
{

/**
 * A wrapper class of pthread mutex.
 * This class is intended to be used in other low level construct only.
 * In most situations, you should use ObLatch or ObMutex.
 *
 */
class ObUtilMutex
{
public:

  typedef ObLockT<ObUtilMutex> Lock;
  typedef ObTryLockT<ObUtilMutex> TryLock;

  ObUtilMutex();
  ~ObUtilMutex();

  void lock() const;
  bool trylock() const;
  void unlock() const;
  bool will_unlock() const;

private:

  ObUtilMutex(const ObUtilMutex&);
  ObUtilMutex& operator=(const ObUtilMutex&);

  struct LockState
  {
    pthread_mutex_t* mutex;
  };

  void unlock(LockState&) const;
  void lock(LockState&) const;
  mutable pthread_mutex_t _mutex;

  friend class Cond;
};
typedef ObUtilMutex Mutex;
}//end namespace
#endif
