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

#ifndef COND_H
#define COND_H
#include "lib/time/Time.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
namespace obutil
{
template<class T> class ObMonitor;

/**
 * A wrapper class of pthread condition that implements a condition variable.
 * @note The condition variable itself is not thread safe and should be protected
 * by a mutex.
 * See also ObThreadCond which is suitable for most situations.
 */
class ObUtilMutex;
typedef ObUtilMutex Mutex;
class Cond
{
public:

  Cond();
  ~Cond();

  void signal();
  void broadcast();
  template <typename Lock> inline bool
  wait(const Lock& lock) const
  {
    bool ret = false;
    if (!lock.acquired()) {
      _OB_LOG(ERROR,"%s","ThreadLockedException");
      ret = false;
    } else {
      ret = wait_impl(lock._mutex);
    }
    return ret;
  }

  template <typename Lock> inline bool
  timed_wait(const Lock& lock, const ObSysTime& timeout) const
  {
    bool ret = false;
    if (!lock.acquired()) {
      _OB_LOG(ERROR,"%s","ThreadLockedException");
      ret = false;
    } else {
      ret = timed_wait_impl(lock._mutex, timeout);
    }
    return ret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(Cond);
private:

  friend class ObMonitor<Mutex>;

  template <typename M> bool wait_impl(const M&) const;
  template <typename M> bool timed_wait_impl(const M&, const ObSysTime&) const;

  mutable pthread_cond_t _cond;
  mutable pthread_condattr_t _attr;
};

template <typename M> inline bool
Cond::wait_impl(const M& mutex) const
{
  bool ret = true;
  typedef typename M::LockState LockState;

  LockState state;
  mutex.unlock(state);
  const int rc = ob_pthread_cond_wait(&_cond, state.mutex);
  mutex.lock(state);

  if ( 0 != rc ) {
    _OB_LOG(ERROR,"%s","ThreadSyscallException");
    ret = false;
  }
  return ret;
}

template <typename M> inline bool
Cond::timed_wait_impl(const M& mutex, const ObSysTime& timeout) const
{
  bool ret = true;
  if (timeout < ObSysTime::microSeconds(0)) {
    _OB_LOG(ERROR,"%s","InvalidTimeoutException");
    ret = false;
  } else {
    typedef typename M::LockState LockState;

    LockState state;
    mutex.unlock(state);

    timeval tv = ObSysTime::now(ObSysTime::Monotonic) + timeout;
    timespec ts;
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
    /*timeval tv = ObSysTime::now(ObSysTime::Realtime);
    timespec ts;
    ts.tv_sec  = tv.tv_sec + timeout/1000;
    ts.tv_nsec = tv.tv_usec * 1000 + ( timeout % 1000 ) * 1000000;*/
    const int rc = ob_pthread_cond_timedwait(&_cond, state.mutex, &ts);
    mutex.lock(state);

    if (rc != 0) {
      if ( rc != ETIMEDOUT ) {
        _OB_LOG(ERROR,"%s","ThreadSyscallException");
        ret = false;
      }
    }
  }
  return ret;
}
}// end namespace
#endif
