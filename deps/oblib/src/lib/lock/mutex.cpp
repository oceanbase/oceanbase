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

#include "lib/lock/mutex.h"
#include "lib/oblog/ob_log.h"
using namespace oceanbase::common;
namespace obutil
{
ObUtilMutex::ObUtilMutex()
{
  const int rt = pthread_mutex_init(&_mutex, NULL);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
#else
  if ( rt != 0 ) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
}

ObUtilMutex::~ObUtilMutex()
{
  const int rt = pthread_mutex_destroy(&_mutex);
  assert(rt == 0);
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
}

bool ObUtilMutex::trylock() const
{
  const int rt = pthread_mutex_trylock(&_mutex);
#ifdef _NO_EXCEPTION
  if ( rt != 0 && rt !=EBUSY ) {
    if ( rt == EDEADLK ) {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadLockedException ");
    } else {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
    }
    return false;
  }
#else
  if(rt != 0 && rt != EBUSY) {
    if(rt == EDEADLK) {
      throw ThreadLockedException(__FILE__, __LINE__);
    } else {
      throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
  }
#endif
  return (rt == 0);
}

void ObUtilMutex::lock() const
{
  const int rt = pthread_mutex_lock(&_mutex);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    if ( rt == EDEADLK ) {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadLockedException ");
    } else {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
    }
  }
#else
  if( rt != 0 ) {
    if(rt == EDEADLK) {
      throw ThreadLockedException(__FILE__, __LINE__);
    } else {
      throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
  }
#endif
}

void ObUtilMutex::lock(LockState&) const
{
}

void ObUtilMutex::unlock() const
{
  const int rt = pthread_mutex_unlock(&_mutex);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
#else
  if ( rt != 0 ) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
}

void ObUtilMutex::unlock(LockState& state) const
{
  state.mutex = &_mutex;
}

bool ObUtilMutex::will_unlock() const
{
  return true;
}
}//end namespace obutil
