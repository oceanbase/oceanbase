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

#ifndef USSL_HOOK_USSL_DEPS_
#define USSL_HOOK_USSL_DEPS_

#ifndef ATOMIC_LOAD
#define ATOMIC_LOAD(x) __atomic_load_n((x), __ATOMIC_ACQUIRE)
#endif

#ifndef ATOMIC_STORE
#define ATOMIC_STORE(x, v) __atomic_store_n((x), (v), __ATOMIC_RELEASE)
#endif

#ifndef ATOMIC_TAS
#define ATOMIC_TAS(val, newv) __sync_lock_test_and_set((val), (newv))
#endif

#ifndef ATOMIC_FAA
#define ATOMIC_FAA(val, addv) __sync_fetch_and_add((val), (addv))
#endif

#ifndef ATOMIC_AAF
#define ATOMIC_AAF(val, addv) __sync_add_and_fetch((val), (addv))
#endif

#ifndef ATOMIC_VCAS
#define ATOMIC_VCAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#endif

#ifndef ATOMIC_BCAS
#define ATOMIC_BCAS(val, cmpv, newv) __sync_bool_compare_and_swap((val), (cmpv), (newv))
#endif


#include <errno.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/epoll.h>
static pid_t ussl_gettid() { return syscall(SYS_gettid); }

#include <sys/time.h>
#include <time.h>
static int64_t ussl_get_us()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000 + tv.tv_usec;
}

#ifndef ussl_log
#include <stdio.h>
static const char *ussl_format_ts(char *buf, int64_t limit, int64_t time_us)
{
  const char *format = "%Y-%m-%d %H:%M:%S";
  buf[0] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(buf, limit, format, &time_struct);
    if (pos < limit) {
      snprintf(buf + pos, limit - pos, ".%.6ld", cur_second_time_us);
    }
  }
  return buf;
}

#include <string.h>
#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1):__FILE__)

#define ussl_log(level, errcode, format, ...)                                                                 \
  {                                                                                                           \
    char time_str[64];                                                                                        \
    (void)errcode;                                                                                            \
    ussl_format_ts(time_str, sizeof(time_str), ussl_get_us());                                                \
    fprintf(stderr, "[%s] %s %s:%d [%d] %s [ussl_log]" format "\n", time_str, #level, __FILENAME__, __LINE__, \
            ussl_gettid(), __func__, ##__VA_ARGS__);                                                          \
  }
#endif

#define ussl_log_info(...) ussl_log(INFO, oceanbase::common::OB_SUCCESS, ##__VA_ARGS__)
#define ussl_log_error(...) ussl_log(ERROR, oceanbase::common::OB_ERR_SYS, ##__VA_ARGS__)
#define ussl_log_warn(...) ussl_log(WARN, oceanbase::common::OB_SUCCESS, ##__VA_ARGS__)
#define ussl_fatal(...)                                          \
{                                                                \
  ussl_log(ERROR, oceanbase::common::OB_ERR_SYS, ##__VA_ARGS__); \
  exit(1);                                                       \
}

#include <stddef.h>

#define ussl_structof(p, T, m) (T *)((char *)p - offsetof(T, m))

#define ussl_arrlen(x) (sizeof(x) / sizeof(x[0]))

typedef struct ussl_dlink_t
{
  struct ussl_dlink_t *next;
  struct ussl_dlink_t *prev;
} ussl_dlink_t;

void ussl_dlink_init(ussl_dlink_t *n)
{
  n->prev = n;
  n->next = n;
}

void __ussl_dlink_insert(ussl_dlink_t *prev, ussl_dlink_t *next, ussl_dlink_t *n)
{
  n->prev = prev;
  n->next = next;
  prev->next = n;
  next->prev = n;
}

void __ussl_dlink_delete(ussl_dlink_t *prev, ussl_dlink_t *next)
{
  prev->next = next;
  next->prev = prev;
}

void ussl_dlink_insert(ussl_dlink_t *head, ussl_dlink_t *n) { __ussl_dlink_insert(head, head->next, n); }

void ussl_dlink_delete(ussl_dlink_t *n)
{
  if (n->next) {
    __ussl_dlink_delete(n->prev, n->next);
    n->next = NULL;
  }
}

#define ussl_dlink_for(head, p) \
  for (ussl_dlink_t *p = (head)->next, *_np = p->next; p != (head); p = _np, _np = p->next)

static int (*libc_setsockopt)(int sockfd, int level, int optname, const void *optval,
                              socklen_t optlen);
static int (*libc_listen)(int sockfd, int backlog);
static int (*libc_connect)(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
// libc_accept is used in other files, so don't make it static.
int (*libc_accept)(int sockfd, struct sockaddr *addr, socklen_t *addrlen);
static int (*libc_accept4)(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags);
static int (*libc_epoll_ctl)(int epfd, int op, int fd, struct epoll_event *event);
static int (*libc_close)(int fd);
static ssize_t (*libc_read)(int fd, void *buf, size_t count);
static ssize_t (*libc_write)(int fd, const void *buf, size_t count);
static ssize_t (*libc_writev)(int fildes, const struct iovec *iov, int iovcnt);

int make_socket_non_blocking(int fd);

#endif // USSL_HOOK_USSL_DEPS_
