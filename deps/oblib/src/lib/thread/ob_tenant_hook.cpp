/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEABASE_TENANT_PRELOAD_H_
#define _OCEABASE_TENANT_PRELOAD_H_

#define _GNU_SOURCE 1
#include "lib/thread/thread.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/stat/ob_diagnose_info.h"
#include <dlfcn.h>
#include <poll.h>
#include <sys/epoll.h>

#define SYS_HOOK(func_name, ...)                                             \
  ({                                                                         \
    int ret = 0;                                                             \
    if (!in_sys_hook++) {                                                    \
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT); \
      ret = real_##func_name(__VA_ARGS__);                                   \
    } else {                                                                 \
      ret = real_##func_name(__VA_ARGS__);                                   \
    }                                                                        \
    in_sys_hook--;                                                           \
    ret;                                                                     \
  })

namespace oceanbase {
namespace omt {
thread_local int in_sys_hook = 0;
}
}

using namespace oceanbase;
using namespace omt;

extern "C" {

int pthread_mutex_lock(pthread_mutex_t *__mutex)
{
  static int (*real_pthread_mutex_lock)(pthread_mutex_t * __mutex) =
      (typeof(real_pthread_mutex_lock))dlsym(RTLD_NEXT,
                                             "pthread_mutex_lock");
  int ret = 0;
  ret = SYS_HOOK(pthread_mutex_lock, __mutex);
  return ret;
}

int pthread_mutex_timedlock(pthread_mutex_t *__restrict __mutex,
                            const struct timespec *__restrict __abstime)
{
  static int (*real_pthread_mutex_timedlock)(
      pthread_mutex_t *__restrict __mutex,
      const struct timespec *__restrict __abstime) =
      (typeof(real_pthread_mutex_timedlock))dlsym(RTLD_NEXT,
                                                  "pthread_mutex_timedlock");
  int ret = 0;
  ret = SYS_HOOK(pthread_mutex_timedlock, __mutex, __abstime);
  return ret;
}

int pthread_rwlock_rdlock(pthread_rwlock_t *__rwlock)
{
  static int (*real_pthread_rwlock_rdlock)(pthread_rwlock_t * __rwlock) =
      (typeof(real_pthread_rwlock_rdlock))dlsym(RTLD_NEXT,
                                                "pthread_rwlock_rdlock");
  int ret = 0;
  ret = SYS_HOOK(pthread_rwlock_rdlock, __rwlock);
  return ret;
}

#ifdef __USE_XOPEN2K
int pthread_rwlock_timedrdlock(pthread_rwlock_t *__restrict __rwlock,
                               const struct timespec *__restrict __abstime)
{
  static int (*real_pthread_rwlock_timedrdlock)(
      pthread_rwlock_t *__restrict __rwlock,
      const struct timespec *__restrict __abstime) =
      (typeof(real_pthread_rwlock_timedrdlock))dlsym(
          RTLD_NEXT, "pthread_rwlock_timedrdlock");
  int ret = 0;
  ret = SYS_HOOK(pthread_rwlock_timedrdlock, __rwlock, __abstime);
  return ret;
}
#endif

int pthread_rwlock_wrlock(pthread_rwlock_t *__rwlock)
{
  static int (*real_pthread_rwlock_wrlock)(pthread_rwlock_t * __rwlock) =
      (typeof(real_pthread_rwlock_wrlock))dlsym(RTLD_NEXT,
                                                "pthread_rwlock_wrlock");
  int ret = 0;
  ret = SYS_HOOK(pthread_rwlock_wrlock, __rwlock);
  return ret;
}

int pthread_join(pthread_t _thread, void **__retval)
{
  static int (*real_pthread_join)(pthread_t _thread, void **__retval) =
      (typeof(real_pthread_join))dlsym(RTLD_NEXT, "pthread_join");
  int ret = 0;
  ::oceanbase::lib::Thread::JoinGuard guard(_thread);
  ret = SYS_HOOK(pthread_join, _thread, __retval);
  return ret;
}

#ifdef __USE_XOPEN2K
int pthread_rwlock_timedwrlock(pthread_rwlock_t *__restrict __rwlock,
                               const struct timespec *__restrict __abstime)
{
  static int (*real_pthread_rwlock_timedwrlock)(
      pthread_rwlock_t *__restrict __rwlock,
      const struct timespec *__restrict __abstime) =
      (typeof(real_pthread_rwlock_timedwrlock))dlsym(
          RTLD_NEXT, "pthread_rwlock_timedwrlock");
  int ret = 0;
  ret = SYS_HOOK(pthread_rwlock_timedwrlock, __rwlock, __abstime);
  return ret;
}
#endif

int ob_epoll_wait(int __epfd, struct epoll_event *__events,
		              int __maxevents, int __timeout)
{
  static int (*real_epoll_wait)(
      int __epfd, struct epoll_event *__events,
		  int __maxevents, int __timeout) = epoll_wait;
  int ret = 0;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_IO_EVENT);
  ret = SYS_HOOK(epoll_wait, __epfd, __events, __maxevents, __timeout);
  return ret;
}

int ob_poll(struct pollfd *__fds, nfds_t __nfds, int __timeout)
{
  static int (*real_poll)(
      struct pollfd *__fds, nfds_t __nfds, int __timeout) = poll;
  int ret = 0;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_IO_EVENT);
  ret = SYS_HOOK(poll, __fds, __nfds, __timeout);
  return ret;
}

int ob_pthread_cond_wait(pthread_cond_t *__restrict __cond,
                         pthread_mutex_t *__restrict __mutex)
{
  static int (*real_pthread_cond_wait)(pthread_cond_t *__restrict __cond,
      pthread_mutex_t *__restrict __mutex) = pthread_cond_wait;
  int ret = 0;
  ret = SYS_HOOK(pthread_cond_wait, __cond, __mutex);
  return ret;
}

int ob_pthread_cond_timedwait(pthread_cond_t *__restrict __cond,
                              pthread_mutex_t *__restrict __mutex,
                              const struct timespec *__restrict __abstime)
{
  static int (*real_pthread_cond_timedwait)(
      pthread_cond_t *__restrict __cond, pthread_mutex_t *__restrict __mutex,
      const struct timespec *__restrict __abstime) = pthread_cond_timedwait;
  int ret = 0;
  ret = SYS_HOOK(pthread_cond_timedwait, __cond, __mutex, __abstime);
  return ret;
}

void ob_usleep(const useconds_t v)
{
  oceanbase::common::ObSleepEventGuard wait_guard((int64_t)v);
  ::usleep(v);
}

int futex_hook(uint32_t *uaddr, int futex_op, uint32_t val, const struct timespec* timeout)
{
  static long int (*real_syscall)(long int __sysno, ...) = syscall;
  int ret = 0;
  if (futex_op == FUTEX_WAIT_PRIVATE) {
    ret = (int)SYS_HOOK(syscall, SYS_futex, uaddr, futex_op, val, timeout, nullptr, 0u);
  } else {
    ret = (int)real_syscall(SYS_futex, uaddr, futex_op, val, timeout, nullptr, 0u);
  }
  return ret;
}

void ob_set_thread_name(const char* type)
{
  ::oceanbase::lib::set_thread_name(type);
}

int64_t ob_update_loop_ts()
{
  return ::oceanbase::lib::Thread::update_loop_ts();
}

} /* extern "C" */

#endif /* _OCEABASE_TENANT_PRELOAD_H_ */
