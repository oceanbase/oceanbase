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

#ifndef THREAD_MUTEX_H_
#define THREAD_MUTEX_H_

#include <assert.h>
#include <pthread.h>
namespace oceanbase {
namespace obsys {

class ThreadMutex {

public:
  ThreadMutex() {
    //assert(pthread_mutex_init(&mutex_, NULL) == 0);
    const int iRet = pthread_mutex_init(&mutex_, NULL);
    (void) iRet;
    assert( iRet == 0 );
  }

  ~ThreadMutex() {
    pthread_mutex_destroy(&mutex_);
  }

  void lock () {
    pthread_mutex_lock(&mutex_);
  }

  int trylock () {
    return pthread_mutex_trylock(&mutex_);
  }

  void unlock() {
    pthread_mutex_unlock(&mutex_);
  }

protected:

  pthread_mutex_t mutex_;
};

class ThreadGuard
{
public:
  ThreadGuard(ThreadMutex *mutex)
  {
    mutex_ = NULL;
    if (mutex) {
      mutex_ = mutex;
      mutex_->lock();
    }
  }
  ~ThreadGuard()
  {
    if (mutex_) {
      mutex_->unlock();
    }
  }
private:
  ThreadMutex *mutex_;
};

}  // namespace obsys
}  // namespace oceanbase

#endif /*THREAD_MUTEX_H_*/
