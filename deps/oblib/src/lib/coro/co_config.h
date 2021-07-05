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

#ifndef CO_CONFIG_H
#define CO_CONFIG_H

#include <cstdint>

namespace oceanbase {
namespace lib {
namespace coro {

struct CoConfig {
  // 16KB for Runtime context size.
  static constexpr auto MAX_RTCTX_SIZE = 16ul << 10;

  ///
  /// Default stack size for all routines, it'll lead to new routines to
  /// be created use this stack size but won't affect which has been
  /// created.
  int64_t stack_size_ = 2 << 20;

  ///
  /// Max co-routine local storage size for each routine.
  ///
  /// \see stack_size_
  ///
  /// \warning Don't change this value after any routine has been
  /// created except the first routine in the main thread.
  static constexpr auto MAX_CRLS_SIZE = 128ul << 10;

  ///
  /// Routine CPU slice in CPU ticks. It's the minimal time, in CPU
  /// ticks, a CPU busy routine would make REAL checkpoint by checking
  /// whether there are routines waiting to run.
  int64_t slice_ticks_ = 4 << 20;

private:
  /// Make \c config() to be the only place where can construct \c
  /// CoConfig instance.
  friend CoConfig& config();

  /// Ctor is hidden to prevent creating instance outside except the
  /// friend function \c config().
  CoConfig() = default;
};

inline CoConfig& config()
{
  static CoConfig instance;
  return instance;
}

}  // namespace coro
}  // namespace lib
}  // namespace oceanbase

#endif /* CO_CONFIG_H */
