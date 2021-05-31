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

#include "lib/lock/tbrwlock.h"

using namespace oceanbase::obsys;

int CRLock::lock() const
{
  return pthread_rwlock_rdlock(_rlock);
}

int CRLock::tryLock() const
{
  return pthread_rwlock_tryrdlock(_rlock);
}

int CRLock::unlock() const
{
  return pthread_rwlock_unlock(_rlock);
}

int CWLock::lock() const
{
  return pthread_rwlock_wrlock(_wlock);
}

int CWLock::tryLock() const
{
  return pthread_rwlock_trywrlock(_wlock);
}

int CWLock::unlock() const
{
  return pthread_rwlock_unlock(_wlock);
}

//////////////////////////////////////////////////////////////////////////////////////

CRWLock::CRWLock(ELockMode lockMode)
{
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  if (lockMode == READ_PRIORITY) {
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
  } else if (lockMode == WRITE_PRIORITY) {
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  }
  pthread_rwlock_init(&_rwlock, &attr);
  _rlock = new CRLock(&_rwlock);
  _wlock = new CWLock(&_rwlock);
}

CRWLock::~CRWLock()
{
  pthread_rwlock_destroy(&_rwlock);
  delete _rlock;
  delete _wlock;
}
