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

#include "lib/lock/ob_mutex.h"
#include "lib/coro/syscall/co_futex.h"

namespace oceanbase {
namespace lib {

class CoCond {
public:
  CoCond()
  {}
  ~CoCond()
  {}

  inline int lock()
  {
    return mutex_.lock();
  }

  inline int unlock()
  {
    return mutex_.unlock();
  }

  int wait()
  {
    return timed_wait(INT64_MAX);
  }

  int timed_wait(const int64_t timeout)
  {
    int ret = OB_SUCCESS;
    int seq = futex_.val();
    mutex_.unlock();
    ret = futex_.wait(seq, timeout);
    mutex_.lock();
    return ret;
  }

  int signal()
  {
    int ret = OB_SUCCESS;
    ATOMIC_INC(&futex_.val());
    futex_.wake(1);
    return ret;
  }

  int broadcast()
  {
    int ret = OB_SUCCESS;
    ATOMIC_INC(&futex_.val());
    futex_.wake(INT64_MAX);
    return ret;
  }

private:
  ObMutex mutex_;
  CoFutex futex_;

private:
  DISALLOW_COPY_AND_ASSIGN(CoCond);
};

}  // namespace lib
}  // namespace oceanbase
