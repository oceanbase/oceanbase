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

#ifndef CO_VAR_CENTER_H
#define CO_VAR_CENTER_H

#include <stdint.h>
#include <functional>
#include "lib/coro/co_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/coro/co_config.h"

namespace oceanbase {
namespace lib {

// maximum size of Co-Routine Local Storage.
extern __thread char* DEFAULT_CRLS;
extern pthread_key_t default_crls_key;

#define CVC (CoVarCenter::instance())

// It defines a hook that can attach to CVC that would be called
// when CV creates and destroys.
class CoVarHook {
  friend class CoVarCenter;

protected:
  using Func = std::function<void()>;
  CoVarHook(Func init, Func deinit) : init_(init), deinit_(deinit)
  {}

  Func init_;
  Func deinit_;
};

class CoRoutine;

class CoVarCenter {
public:
  /// Apply new co-routine local variable with type of T. Every
  /// co-routine local variable should use this method to get offset of
  /// its CRLS buffer.
  ///
  /// \tparam T \c T is the type of applying variable.
  ///
  /// \return If succeed, an non-negative int64 which represents offset
  /// of co-routine local storage buffer would been return. Otherwise
  /// -1 would been return.
  template <class T>
  int64_t apply()
  {
    int64_t pos = 0;
    lock_.lock();
    int64_t aligned = (size_ - 1 + alignof(T)) & ~(alignof(T) - 1);
    if (aligned + sizeof(T) > coro::CoConfig::MAX_CRLS_SIZE) {
      pos = -1;
    }
    if (pos != -1) {
      pos = aligned;
      size_ = aligned + sizeof(T);
    }
    lock_.unlock();
    return pos;
  }

  int register_hook(CoVarHook* hook);
  void at_routine_create();
  void at_routine_exit();

  static CoVarCenter& instance()
  {
    static CoVarCenter cvc_;
    return cvc_;
  }

private:
  CoVarCenter();
  ~CoVarCenter();

private:
  CoSpinLock lock_;
  int64_t size_ = 0;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_VAR_CENTER_H */
