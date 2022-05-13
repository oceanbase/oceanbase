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
#ifndef MUTEX_H
#define MUTEX_H

#include <pthread.h>
#include "lib/lock/ob_lock.h"

namespace obutil
{
class Mutex
{
public:

  typedef ObLockT<Mutex> Lock;
  typedef ObTryLockT<Mutex> TryLock;

  Mutex();
  ~Mutex();

  void lock() const;
  bool trylock() const;
  void unlock() const;
  bool will_unlock() const;

private:

  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);

  struct LockState
  {
    pthread_mutex_t* mutex;
  };

  void unlock(LockState&) const;
  void lock(LockState&) const;
  mutable pthread_mutex_t _mutex;

  friend class Cond;
};
}//end namespace
#endif
