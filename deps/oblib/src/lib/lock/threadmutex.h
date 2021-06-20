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

#ifndef TBSYS_MUTEX_H_
#define TBSYS_MUTEX_H_

#include <assert.h>
#include <pthread.h>
namespace oceanbase {
namespace obsys {

/*
 * author cjxrobot
 *
 * Linux thread-lock
 */

/**
 * @brief Simple encapsulation of linux thread-lock and mutex-lock
 */
class CThreadMutex {

public:
  /*
   * Constructor
   */
  CThreadMutex()
  {
    // assert(pthread_mutex_init(&_mutex, NULL) == 0);
    const int iRet = pthread_mutex_init(&_mutex, NULL);
    (void)iRet;
    assert(iRet == 0);
  }

  /*
   * Destructor
   */
  ~CThreadMutex()
  {
    pthread_mutex_destroy(&_mutex);
  }

  /**
   * Lock
   */

  void lock()
  {
    pthread_mutex_lock(&_mutex);
  }

  /**
   * trylock
   */

  int trylock()
  {
    return pthread_mutex_trylock(&_mutex);
  }

  /**
   * Unlock
   */
  void unlock()
  {
    pthread_mutex_unlock(&_mutex);
  }

protected:
  pthread_mutex_t _mutex;
};

/**
 * @brief Thread guard
 */
class CThreadGuard {
public:
  CThreadGuard(CThreadMutex* mutex)
  {
    _mutex = NULL;
    if (mutex) {
      _mutex = mutex;
      _mutex->lock();
    }
  }
  ~CThreadGuard()
  {
    if (_mutex) {
      _mutex->unlock();
    }
  }

private:
  CThreadMutex* _mutex;
};

}  // namespace obsys
}  // namespace oceanbase

#endif /*MUTEX_H_*/
