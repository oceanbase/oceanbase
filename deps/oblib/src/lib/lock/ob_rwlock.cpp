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

#include "lib/lock/ob_rwlock.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase;
using namespace obsys;

int ObRLock::lock() const
{
  return pthread_rwlock_rdlock(rlock_);
}

int ObRLock::trylock() const
{
  return pthread_rwlock_tryrdlock(rlock_);
}

int ObRLock::unlock() const
{
  return pthread_rwlock_unlock(rlock_);
}

int ObWLock::lock() const
{
  return pthread_rwlock_wrlock(wlock_);
}

int ObWLock::trylock() const
{
  return pthread_rwlock_trywrlock(wlock_);
}

int ObWLock::unlock() const
{
  return pthread_rwlock_unlock(wlock_);
}

ObRWLock::ObRWLock(LockMode lockMode)
  : rlock_(&rwlock_),
    wlock_(&rwlock_)
{
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  if (lockMode == READ_PRIORITY) {
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
  } else if (lockMode == WRITE_PRIORITY) {
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  }
  pthread_rwlock_init(&rwlock_, &attr);
}

ObRWLock::~ObRWLock()
{
  pthread_rwlock_destroy(&rwlock_);
}
