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

#ifndef TBSYS_MONITOR_H
#define TBSYS_MONITOR_H
#include "lib/lock/Lock.h"
#include "lib/lock/Cond.h"

// using namespace std;

namespace tbutil {
/**
 * @brief The monitor is a template class, which requires Mutex or RecMutex as template parameters.
 * The monitor is a synchronization mechanism used to protect the critical section.
 * Like a mutex, only one thread can be active in the critical section.
 *
 * The monitor allows you to suspend threads in the critical section,
 * so that another thread can enter the critical section,
 * the second thread can exit the critical section, or suspend itself in the critical section,
 * no matter what the original thread will be Wake up and continue to perform the original task
 */
template <class T>
class Monitor {
public:
  typedef LockT<Monitor<T> > Lock;
  typedef TryLockT<Monitor<T> > TryLock;

  Monitor();
  ~Monitor();

  /**
   * @brief This function attempts to lock the monitor. If the monitor is locked by another thread,
   * the calling thread will be suspended until the monitor is available. When the call returns, the monitor has been
   * locked by it
   */
  void lock() const;
  /**
   * @brief This function unlocks the monitor. If there are other threads waiting to enter the monitor
   * (that is, blocked in the lock call), one of the threads will be awakened and the monitor will be locked
   *
   * @return
   */
  void unlock() const;
  /**
   * @brief This function attempts to lock the monitor. If the monitor is available,
   * the call will lock the monitor and return true. If the monitor has been locked by another thread, the call returns
   * false
   */
  bool tryLock() const;

  /**
   * @brief This function suspends the calling thread and releases the lock on the monitor at the same time.
   * Other threads may call notify or notifyAll to wake up the thread that was suspended in the wait call.
   * When the wait call returns, the monitor is locked again, and the suspended thread will resume execution
   * @return
   */
  bool wait() const;
  /**
   * @brief This function suspends the thread that called it until the specified time has elapsed.
   * If another thread calls notify or notifyAll to wake up the suspended thread before the timeout occurs,this call
   * returns true, the monitor is locked again, and the suspended thread resumes execution. If a timeout occurs, the
   * function returns false
   * @param Time
   *
   * @return
   */
  bool timedWait(const Time&) const;
  /**
   * @brief This function wakes up a thread currently suspended in the wait call.
   * If there is no such thread when notify is called, the notification will be lost
   * (that is, if no thread can be awakened, the call to notify will not be remembered)
   */
  void notify();
  /**
   * @brief This function wakes up all threads currently suspended in the wait call.
   * Like notify, if there are no suspended threads at this time, the call to notifyAll will be lost
   */
  void notifyAll();

private:
  Monitor(const Monitor&);
  Monitor& operator=(const Monitor&);

  void notifyImpl(int) const;

  mutable Cond _cond;
  T _mutex;
  mutable int _nnotify;
};

template <class T>
Monitor<T>::Monitor() : _nnotify(0)
{}

template <class T>
Monitor<T>::~Monitor()
{}

template <class T>
inline void Monitor<T>::lock() const
{
  _mutex.lock();
  if (_mutex.willUnlock()) {
    _nnotify = 0;
  }
}

template <class T>
inline void Monitor<T>::unlock() const
{
  if (_mutex.willUnlock()) {
    notifyImpl(_nnotify);
  }
  _mutex.unlock();
}

template <class T>
inline bool Monitor<T>::tryLock() const
{
  bool result = _mutex.tryLock();
  if (result && _mutex.willUnlock()) {
    _nnotify = 0;
  }
  return result;
}

template <class T>
inline bool Monitor<T>::wait() const
{
  notifyImpl(_nnotify);
#ifdef _NO_EXCEPTION
  const bool bRet = _cond.waitImpl(_mutex);
  _nnotify = 0;
  return bRet;
#else
  try {
    _cond.waitImpl(_mutex);
  } catch (...) {
    _nnotify = 0;
    throw;
  }

  _nnotify = 0;
#endif
  return true;
}

template <class T>
inline bool Monitor<T>::timedWait(const Time& timeout) const
{
  notifyImpl(_nnotify);
#ifdef _NO_EXCEPTION
  const bool rc = _cond.timedWaitImpl(_mutex, timeout);
  _nnotify = 0;
  return rc;
#else
  try {
    _cond.timedWaitImpl(_mutex, timeout);
  } catch (...) {
    _nnotify = 0;
    throw;
  }
  _nnotify = 0;
#endif
  return true;
}

template <class T>
inline void Monitor<T>::notify()
{
  if (_nnotify != -1) {
    ++_nnotify;
  }
}

template <class T>
inline void Monitor<T>::notifyAll()
{
  _nnotify = -1;
}

template <class T>
inline void Monitor<T>::notifyImpl(int nnotify) const
{
  if (nnotify != 0) {
    if (nnotify == -1) {
      _cond.broadcast();
      return;
    } else {
      while (nnotify > 0) {
        _cond.signal();
        --nnotify;
      }
    }
  }
}
}  // namespace tbutil
#endif
