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

#ifndef OB_MONITOR_H
#define OB_MONITOR_H
#include "lib/lock/ob_lock.h"
#include "lib/lock/cond.h"


namespace obutil
{
/**
 * A monitor is a synchronization construct that allows threads to have both mutual exclusion and the ability to
 * wait (block) for a certain condition to become false. Monitors also have a mechanism for signaling other threads
 * that their condition has been met.
 */
template <class T>
class ObMonitor
{
public:

  typedef ObLockT<ObMonitor<T> > Lock;
  typedef ObTryLockT<ObMonitor<T> > TryLock;

  ObMonitor();
  ~ObMonitor();

  void lock() const;
  void unlock() const;
  bool trylock() const;
  bool wait() const;
  bool timed_wait(const ObSysTime&) const;
  void notify();
  void notify_all();

private:

  ObMonitor(const ObMonitor&);
  ObMonitor& operator=(const ObMonitor&);

  void notify_impl(int) const;

  mutable Cond cond_;
  T mutex_;
  mutable int nnotify_;
};

template <class T>
ObMonitor<T>::ObMonitor() :
  nnotify_(0)
{
}

template <class T>
ObMonitor<T>::~ObMonitor()
{
}

template <class T> inline void
ObMonitor<T>::lock() const
{
  mutex_.lock();
  if(mutex_.will_unlock()) {
    nnotify_ = 0;
  }
}

template <class T> inline void
ObMonitor<T>::unlock() const
{
  if(mutex_.will_unlock()) {
    notify_impl(nnotify_);
  }
  mutex_.unlock();
}

template <class T> inline bool
ObMonitor<T>::trylock() const
{
  bool result = mutex_.trylock();
  if(result && mutex_.will_unlock()) {
    nnotify_ = 0;
  }
  return result;
}

template <class T> inline bool
ObMonitor<T>::wait() const
{
  notify_impl(nnotify_);
#ifdef _NO_EXCEPTION
  const bool bRet = cond_.wait_impl(mutex_);
  nnotify_ = 0;
  return bRet;
#else
  try {
    cond_.wait_impl(mutex_);
  } catch(...) {
    nnotify_ = 0;
    throw;
  }

  nnotify_ = 0;
  return true;
#endif
}

template <class T> inline bool
ObMonitor<T>::timed_wait(const ObSysTime& timeout) const
{
  notify_impl(nnotify_);
#ifdef _NO_EXCEPTION
  const bool rc = cond_.timed_wait_impl(mutex_, timeout);
  nnotify_ = 0;
  return rc;
#else
  try {
    cond_.timed_wait_impl(mutex_, timeout);
  } catch(...) {
    nnotify_ = 0;
    throw;
  }
  nnotify_ = 0;
  return true;
#endif
}

template <class T> inline void
ObMonitor<T>::notify()
{
  if(nnotify_ != -1) {
    ++nnotify_;
  }
}

template <class T> inline void
ObMonitor<T>::notify_all()
{
  nnotify_ = -1;
}


template <class T> inline void
ObMonitor<T>::notify_impl(int nnotify) const
{
  if (nnotify == 0) {
  } else if (nnotify == -1) {
    cond_.broadcast();
  } else {
    while(nnotify > 0) {
      cond_.signal();
      --nnotify;
    }
  }
}
}//end namespace
#endif
