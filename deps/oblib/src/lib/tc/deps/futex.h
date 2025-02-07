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
#include <linux/futex.h>
#include <unistd.h>
#include <sys/syscall.h>

static int tc_futex(int *uaddr, int op, int val, const struct timespec *timeout, int *uaddr2, int val3) {
  return (int)syscall(SYS_futex, uaddr, op, val, timeout, uaddr2, val3);
}

static int tc_futex_wake(int *p, int val) {
  return tc_futex((int *)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0);
}

static struct timespec *tc_make_timespec(struct timespec *ts, int64_t us)
{
  ts->tv_sec = us / 1000000;
  ts->tv_nsec = 1000 * (us % 1000000);
  return ts;
}

static int tc_futex_wait(int *p, int val, const int64_t timeout_us) {
  int err = 0;
  struct timespec ts;
  if (0 != tc_futex((int *)p, FUTEX_WAIT_PRIVATE, val, tc_make_timespec(&ts, timeout_us), NULL, 0)) {
    err = errno;
  }
  return err;
}
