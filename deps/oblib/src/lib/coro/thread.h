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
  using Runnable = std::function<void()>;

public:
  Thread();
  Thread(int64_t stack_size);
  Thread(Runnable runnable, int64_t stack_size = 0);
  ~Thread();

  int start();
  int start(Runnable runnable);
  void stop();
  void wait();
  void destroy();

  pid_t get_pid() const;
  pid_t get_tid() const;

  /// \brief Get current thread object.
  ///
  /// \warning It would encounter segment fault if current thread
  /// isn't created with this class.
  static Thread& current();

  bool has_set_stop() const;

public:
  static __thread bool monopoly_;

private:
  static void* __th_start(void* th);
  void destroy_stack();
  static __thread Thread* current_thread_;

private:
  static int64_t total_thread_count_;

private:
  pthread_t pth_;
  pid_t pid_;
  pid_t tid_;
  Runnable runnable_;
  void* stack_addr_;
  int64_t stack_size_;
  bool stop_;
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

}  // namespace lib
}  // namespace oceanbase

#endif /* CORO_THREAD_H */
