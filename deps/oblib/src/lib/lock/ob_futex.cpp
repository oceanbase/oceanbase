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

#include "ob_futex.h"
#include <linux/futex.h>
#include "lib/ob_abort.h"

static struct timespec make_timespec(int64_t us)
{
  timespec ts;
  ts.tv_sec = us / 1000000;
  ts.tv_nsec = 1000 * (us % 1000000);
  return ts;
}

extern "C" {
int __attribute__((weak)) futex_hook(uint32_t *uaddr, int futex_op, uint32_t val, const struct timespec* timeout)
{
  return syscall(SYS_futex, uaddr, futex_op, val, timeout);
}
}

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace lib {

int ObFutex::wait(int v, int64_t timeout)
{
  int ret = OB_SUCCESS;
  const auto ts = make_timespec(timeout);
  ATOMIC_INC(&sys_waiters_);
  int eret = futex_wait(&v_, v, &ts);
  if (OB_UNLIKELY(eret != 0)) {
    if (OB_UNLIKELY(eret == ETIMEDOUT)) {
      // only return timeout error code, others treat as success.
      ret = OB_TIMEOUT;
    }
  }
  ATOMIC_DEC(&sys_waiters_);
  return ret;
}

int ObFutex::wake(int64_t n)
{
  int cnt = 0;
  if (n >= INT32_MAX) {
    cnt = futex_wake(&v_, INT32_MAX);
  } else {
    cnt = futex_wake(&v_, static_cast<int32_t>(n));
  }
  return cnt;
}

}  // lib
}  // oceanbase
