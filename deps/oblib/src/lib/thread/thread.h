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
#include "lib/utility/ob_macro_utils.h"

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

  pid_t get_pid() const;
  pid_t get_tid() const;

  /// \brief Get current thread object.
  ///
  /// \warning It would encounter segment fault if current thread
  /// isn't created with this class.
  static Thread &current();

  bool has_set_stop() const;

private:
  static void* __th_start(void *th);
  void destroy_stack();
  static TLOCAL(Thread *, current_thread_);

private:
  static int64_t total_thread_count_;
private:
  pthread_t pth_;
  pid_t pid_;
  pid_t tid_;
  Runnable runnable_;
#ifndef OB_USE_ASAN
  void *stack_addr_;
#endif
  int64_t stack_size_;
  bool stop_;
  int64_t join_concurrency_;
};

OB_INLINE pid_t Thread::get_pid() const
{
  return pid_;
}

OB_INLINE pid_t Thread::get_tid() const
{
  return tid_;
}

OB_INLINE bool Thread::has_set_stop() const
{
  return stop_;
}

extern int get_max_thread_num();
}  // lib
}  // oceanbase

#endif /* CORO_THREAD_H */
