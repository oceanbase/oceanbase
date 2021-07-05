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

#ifndef TBSYS_COND_H
#define TBSYS_COND_H
#include "lib/time/Time.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
namespace tbutil {
template <class T>
class Monitor;
class Mutex;
class RecMutex;
/**
 * @brief Condition variable of linux-thread
 */
class Cond {
public:
  Cond();
  ~Cond();

  /**
   * @brief Signal to thread
   */
  void signal();

  /**
   * @brief Broadcast to thread
   */
  void broadcast();

  /**
   * @brief Thread blocking waiting
   *
   * @param lock
   *
   * @return
   */
  template <typename Lock>
  inline bool wait(const Lock& lock) const
  {
    if (!lock.acquired()) {
      _OB_LOG(ERROR, "%s", "ThreadLockedException");
      return false;
    }
    return waitImpl(lock._mutex);
  }

  /**
   * @brief Thread blocking waiting with timeout
   *
   * @param lock
   * @param timeout
   *
   * @return
   */
  template <typename Lock>
  inline bool timedWait(const Lock& lock, const Time& timeout) const
  {
    if (!lock.acquired()) {
      _OB_LOG(ERROR, "%s", "ThreadLockedException");
      return false;
    }
    return timedWaitImpl(lock._mutex, timeout);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(Cond);

private:
  friend class Monitor<Mutex>;
  friend class Monitor<RecMutex>;

  template <typename M>
  bool waitImpl(const M&) const;
  template <typename M>
  bool timedWaitImpl(const M&, const Time&) const;

  mutable pthread_cond_t _cond;
};

template <typename M>
inline bool Cond::waitImpl(const M& mutex) const
{
  typedef typename M::LockState LockState;

  LockState state;
  mutex.unlock(state);
  const int rc = pthread_cond_wait(&_cond, state.mutex);
  mutex.lock(state);

  if (0 != rc) {
    _OB_LOG(ERROR, "%s", "ThreadSyscallException");
    return false;
  }
  return true;
}

template <typename M>
inline bool Cond::timedWaitImpl(const M& mutex, const Time& timeout) const
{
  if (timeout < Time::microSeconds(0)) {
    _OB_LOG(ERROR, "%s", "InvalidTimeoutException");
    return false;
  }

  typedef typename M::LockState LockState;

  LockState state;
  mutex.unlock(state);

  timeval tv = Time::now(Time::Realtime) + timeout;
  timespec ts;
  ts.tv_sec = tv.tv_sec;
  ts.tv_nsec = tv.tv_usec * 1000;
  /*timeval tv = Time::now(Time::Realtime);
  timespec ts;
  ts.tv_sec  = tv.tv_sec + timeout/1000;
  ts.tv_nsec = tv.tv_usec * 1000 + ( timeout % 1000 ) * 1000000;*/
  const int rc = pthread_cond_timedwait(&_cond, state.mutex, &ts);
  mutex.lock(state);

  if (rc != 0) {
    if (rc != ETIMEDOUT) {
      _OB_LOG(ERROR, "%s", "ThreadSyscallException");
      return false;
    }
  }
  return true;
}
}  // namespace tbutil
#endif
