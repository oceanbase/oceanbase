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

#ifndef _OCEANBASE_SMART_CALL_
#define _OCEANBASE_SMART_CALL_

#include "common/ob_common_utility.h"
#include "lib/utility/utility.h"
#include "lib/coro/context/fcontext.hpp"
#include "lib/coro/co_protected_stack_allocator.h"

namespace oceanbase {
namespace common {
namespace context_detail = boost::context::detail;

static constexpr int64_t ALL_STACK_LIMIT = 10L << 20;
static constexpr int64_t STACK_PER_EXTEND = 2L << 20;
static constexpr int64_t STACK_RESERVED_SIZE = 128L << 10;
extern RLOCAL(int64_t, all_stack_size);

struct SContext {
  typedef int (*Func)(void*);
  Func func_;
  void* arg_;
};

inline void SStart(context_detail::transfer_t from)
{
  auto sctx = reinterpret_cast<SContext*>(from.data);
  int64_t ret = sctx->func_(sctx->arg_);
  context_detail::jump_fcontext(from.fctx, (void*)ret);
  OB_ASSERT(0);
}

inline int call_with_new_stack(SContext& sctx)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "smart_call", K(lbt()));
  void* ori_stack_addr = nullptr;
  size_t ori_stack_size = 0;
  void* stack_addr = nullptr;
  const int64_t stack_size = STACK_PER_EXTEND;
  if (OB_FAIL(get_stackattr(ori_stack_addr, ori_stack_size))) {
  } else if (FALSE_IT(all_stack_size = 0 == all_stack_size ? ori_stack_size : all_stack_size)) {
  } else if (all_stack_size + STACK_PER_EXTEND > ALL_STACK_LIMIT) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_ISNULL(stack_addr = lib::g_stack_allocer.alloc(OB_SERVER_TENANT_ID, stack_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    all_stack_size += stack_size;
    context_detail::fcontext_t fctx = make_fcontext((char*)stack_addr + stack_size, stack_size, SStart);

    // To prevent the check_stack_overflow call before the jump
    // Do not introduce other code between the two set_stackattr
    set_stackattr(stack_addr, stack_size);
    context_detail::transfer_t transfer = context_detail::jump_fcontext(fctx, &sctx);
    set_stackattr(ori_stack_addr, ori_stack_size);

    ret = reinterpret_cast<int64_t>(transfer.data);
    lib::g_stack_allocer.dealloc(stack_addr);
    all_stack_size -= stack_size;
  }
  OB_LOG(INFO, "smart_call finish");
  return ret;
}

#define SMART_CALL(func)                                                   \
  ({                                                                       \
    int ret = OB_SUCCESS;                                                  \
    bool is_overflow = false;                                              \
    if (OB_FAIL(check_stack_overflow(is_overflow, STACK_RESERVED_SIZE))) { \
    } else if (!is_overflow) {                                             \
      ret = func;                                                          \
    } else {                                                               \
      std::function<int()> f = [&]() {                                     \
        int ret = OB_SUCCESS;                                              \
        try {                                                              \
          ret = func;                                                      \
        } catch (OB_BASE_EXCEPTION & except) {                             \
          ret = except.get_errno();                                        \
        }                                                                  \
        return ret;                                                        \
      };                                                                   \
      SContext sctx;                                                       \
      sctx.func_ = [](void* arg) { return (*(decltype(f)*)(arg))(); };     \
      sctx.arg_ = &f;                                                      \
      ret = call_with_new_stack(sctx);                                     \
    }                                                                      \
    ret;                                                                   \
  })

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OCEANBASE_SMART_CALL_ */
