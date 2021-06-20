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

#ifndef OBLIB_THREAD_POOL_H
#define OBLIB_THREAD_POOL_H

#include "lib/coro/co_user_thread.h"
#include "lib/worker.h"

namespace oceanbase {
namespace lib {

// src/lib/coro/co_user_thread.h: using CoKThread = CoXThread;
// src/lib/coro/co_user_thread.h: using CoXThread = CoKThreadTemp<CoUserThread>;
// lib/coro/co_user_thread.h      using CoUserThread = CoUserThreadTemp<CoSetSched>;
class ThreadPool : public lib::CoKThread /* CoKThreadTemp */
{
public:
  void run(int64_t idx) final
  {
    thread_idx_ = static_cast<uint64_t>(idx);
    run0();
  }

  virtual bool is_monopoly()
  {
    return true;
  }

  virtual void run0()
  {
    // Create worker for current thread.
    Worker worker;
    this_thread::set_monopoly(is_monopoly());
    run1();
  }

  virtual void run1() = 0;

protected:
  uint64_t get_thread_idx() const
  {
    return thread_idx_;
  }

private:
  static __thread uint64_t thread_idx_;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* OBLIB_THREAD_POOL_H */
