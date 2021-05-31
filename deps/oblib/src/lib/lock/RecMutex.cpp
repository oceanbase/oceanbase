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

#include "lib/lock/RecMutex.h"
#include "lib/oblog/ob_log.h"
namespace tbutil {
RecMutex::RecMutex() : _count(0)
{
  pthread_mutexattr_t attr;
  int rt = pthread_mutexattr_init(&attr);
#ifdef _NO_EXCEPTION
  assert(0 == rt);
  if (rt != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  }
#else
  if (0 != rt) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
  rt = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
#ifdef _NO_EXCEPTION
  assert(0 == rt);
  if (rt != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  }
#else
  if (0 != rt) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
  rt = pthread_mutex_init(&_mutex, &attr);
#ifdef _NO_EXCEPTION
  assert(0 == rt);
  if (rt != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  }
#else
  if (0 != rt) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif

  rt = pthread_mutexattr_destroy(&attr);
#ifdef _NO_EXCEPTION
  assert(0 == rt);
  if (rt != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  }
#else
  if (0 != rt) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
}

RecMutex::~RecMutex()
{
  assert(_count == 0);
  const int rc = pthread_mutex_destroy(&_mutex);
  assert(rc == 0);
  if (rc != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  }
}

void RecMutex::lock() const
{
  const int rt = pthread_mutex_lock(&_mutex);
  if (rt != 0) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
  } else {
    if (++_count > 1) {
      const int rc = pthread_mutex_unlock(&_mutex);
      if (rc != 0) {
        _OB_LOG(ERROR, "%s", "ThreadSyscallException");
      }
    }
  }
}

bool RecMutex::tryLock() const
{
  const int rc = pthread_mutex_trylock(&_mutex);
  const bool result = (rc == 0);
  if (!result) {
    assert(EBUSY == rc);
  } else if (++_count > 1) {
    const int rt = pthread_mutex_unlock(&_mutex);
    if (rt != 0) {
      _OB_LOG(ERROR, "%s", "ThreadSyscallException");
    }
  }
  return result;
}

void RecMutex::unlock() const
{
  if (--_count == 0) {
    const int rc = pthread_mutex_unlock(&_mutex);
    if (rc != 0) {
      _OB_LOG(ERROR, "%s", "ThreadSyscallException");
    }
  }
}

void RecMutex::unlock(LockState& state) const
{
  state.mutex = &_mutex;
  state.count = _count;
  _count = 0;
}

void RecMutex::lock(LockState& state) const
{
  _count = state.count;
}

bool RecMutex::willUnlock() const
{
  return _count == 1;
}
}  // namespace tbutil
