#ifndef _OCEABASE_TENANT_PRELOAD_H_
#define _OCEABASE_TENANT_PRELOAD_H_

#ifndef PERF_MODE
#define _GNU_SOURCE 1
#include "lib/worker.h"
#include <dlfcn.h>

#define SYS_HOOK(func_name, ...)                                               \
  ({                                                                           \
    int ret = 0;                                                               \
    if (!in_sys_hook++ && OB_NOT_NULL(oceanbase::lib::Worker::self_))  {       \
      oceanbase::lib::Worker::self_->set_is_blocking(true);                    \
      ret = real_##func_name(__VA_ARGS__);                                     \
      oceanbase::lib::Worker::self_->set_is_blocking(false);                   \
    } else {                                                                   \
      ret = real_##func_name(__VA_ARGS__);                                     \
    }                                                                          \
    in_sys_hook--;                                                             \
    ret;                                                                       \
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

int futex_hook(uint32_t *uaddr, int futex_op, uint32_t val, const struct timespec* timeout)
{
  static long int (*real_syscall)(long int __sysno, ...) =
      (typeof(real_syscall))dlsym(RTLD_NEXT, "syscall");
  int ret = 0;
  if (futex_op == FUTEX_WAIT_PRIVATE) {
    ret = (int)SYS_HOOK(syscall, SYS_futex, uaddr, futex_op, val, timeout, nullptr, 0u);
  } else {
    ret = (int)real_syscall(SYS_futex, uaddr, futex_op, val, timeout, nullptr, 0u);
  }
  return ret;
}

} /* extern "C" */

#else
int ob_pthread_cond_wait(pthread_cond_t *__restrict __cond,
                         pthread_mutex_t *__restrict __mutex)
{
  return pthread_cond_wait(__cond, __mutex);
}

int ob_pthread_cond_timedwait(pthread_cond_t *__restrict __cond,
                              pthread_mutex_t *__restrict __mutex,
                              const struct timespec *__restrict __abstime)
{
  return pthread_cond_timedwait(__cond, __mutex, __abstime);
}

#endif /* PERF_MODE */
#endif /* _OCEABASE_TENANT_PRELOAD_H_ */
