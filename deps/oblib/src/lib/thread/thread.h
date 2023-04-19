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

#ifndef CORO_THREAD_H
#define CORO_THREAD_H

#include <functional>
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase {
namespace lib {

/// \class
/// A wrapper of Linux thread that supports normal thread operations.
class Thread {
public:
  using Runnable = std::function<void()>;
  static constexpr int PATH_SIZE = 128;
  Thread();
  Thread(int64_t stack_size);
  Thread(Runnable runnable, int64_t stack_size=0);
  ~Thread();

  int start();
  int start(Runnable runnable);
  void stop();
  void wait();
  void destroy();
  void dump_pth();

  /// \brief Get current thread object.
  ///
  /// \warning It would encounter segment fault if current thread
  /// isn't created with this class.
  static Thread &current();

  bool has_set_stop() const;

  OB_INLINE static int64_t update_loop_ts(int64_t t)
  {
    int64_t ret = loop_ts_;
    loop_ts_ = t;
    ObLatch::clear_lock();
    return ret;
  }

  OB_INLINE static int64_t update_loop_ts()
  {
    return update_loop_ts(common::ObTimeUtility::fast_current_time());
  }
public:
  static thread_local int64_t loop_ts_;
  static thread_local pthread_t thread_joined_;
  static thread_local int64_t sleep_us_;
  static thread_local bool is_blocking_;
private:
  static void* __th_start(void *th);
  void destroy_stack();
  static thread_local Thread* current_thread_;

private:
  static int64_t total_thread_count_;
private:
  pthread_t pth_;
  Runnable runnable_;
#ifndef OB_USE_ASAN
  void *stack_addr_;
#endif
  int64_t stack_size_;
  bool stop_;
  int64_t join_concurrency_;
  pid_t pid_before_stop_;
  pid_t tid_before_stop_;
};

OB_INLINE bool Thread::has_set_stop() const
{
  IGNORE_RETURN update_loop_ts();
  return stop_;
}

extern int get_max_thread_num();
}  // lib
}  // oceanbase

#endif /* CORO_THREAD_H */
