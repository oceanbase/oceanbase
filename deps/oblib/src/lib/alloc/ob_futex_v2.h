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

#ifndef OB_FUTEX_v2_H
#define OB_FUTEX_v2_H

#ifndef ENABLE_SANITY
#else
#include <atomic>
#include "lib/ob_errno.h"
#include "lib/list/ob_dlist.h"
#include <linux/futex.h>
#include "lib/ob_abort.h"
namespace oceanbase {
namespace lib {
class ObFutexV2
{
public:
  ObFutexV2()
      : v_(),
        sys_waiters_()
  {}

  int32_t &val() { return v_; }
  uint32_t &uval() { return reinterpret_cast<uint32_t&>(v_); }

  // This function atomically verifies v_ still equals to argument v,
  // and sleeps awaiting wake call on this futex. If the timeout
  // argument is positive, it contains the maximum duration described
  // in milliseconds of the wait, which is infinite otherwise.
  int wait(int v, int64_t timeout);
  // This function wakes at most n, which is in argument list,
  // routines up waiting on the futex.
  int wake(int64_t n);

public:

private:
  int v_;
  int sys_waiters_;
} CACHE_ALIGNED;

}  // lib
}  // oceanbase

#endif /* ENABLE_SANITY */
#endif /* OB_FUTEX_v2_H */
