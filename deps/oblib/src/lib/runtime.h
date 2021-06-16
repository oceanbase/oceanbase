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

#ifndef LIB_RUNTIME_H
#define LIB_RUNTIME_H

#include "lib/ob_define.h"
#include "lib/coro/co_routine.h"
#include "lib/coro/co_sched.h"

namespace oceanbase {
namespace lib {
class RuntimeContext {
public:
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};

extern bool g_runtime_enabled;

inline RuntimeContext* get_default_runtime_context()
{
  /*
   * Used for the threads created by raw pthread APIs, which do not
   * have runtime routine context.
   */
  static __thread char default_rtctx[coro::CoConfig::MAX_RTCTX_SIZE];

  return reinterpret_cast<RuntimeContext*>(&(default_rtctx[0]));
}

inline RuntimeContext* get_runtime_context()
{
  void* buf = NULL;

  if (OB_NOT_NULL(CoSched::get_active_routine())) {
    buf = CoSched::get_active_routine()->get_rtctx();
  } else {
    buf = get_default_runtime_context();
  }
  return reinterpret_cast<RuntimeContext*>(buf);
}

}  // namespace lib
}  // namespace oceanbase

#endif /* LIB_RUNTIME_H */
