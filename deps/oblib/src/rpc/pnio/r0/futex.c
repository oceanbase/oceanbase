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

#include <linux/futex.h>
#ifdef futex
#define rk_futex(...) futex(__VA_ARGS__)
#else
#define rk_futex(...) ((int)syscall(SYS_futex, __VA_ARGS__))
#endif

int rk_futex_wake(int *p, int val) {
  int err = 0;
  if (0 != rk_futex((uint *)p, FUTEX_WAKE_PRIVATE, val, NULL)) {
    err = errno;
  }
  return err;
}

int rk_futex_wait(int *p, int val, const struct timespec *timeout) {
  int err = 0;
  if (0 != rk_futex((uint *)p, FUTEX_WAIT_PRIVATE, val, timeout)) {
    err = errno;
  }
  return err;
}
