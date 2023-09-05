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
#include "lib/thread/protected_stack_allocator.h"

namespace oceanbase
{
namespace common
{
static constexpr int64_t ALL_STACK_LIMIT = 10L << 20;
static constexpr int64_t STACK_PER_EXTEND = (2L << 20) - ACHUNK_PRESERVE_SIZE * 2;
static constexpr int64_t STACK_RESERVED_SIZE = 128L << 10;
RLOCAL_EXTERN(int64_t, all_stack_size);

int jump_call(void * arg_, int(*func_) (void*), void* stack_addr);

inline int call_with_new_stack(void * arg_, int(*func_) (void*))
{
  int ret = OB_SUCCESS;
#if defined(__x86_64__) || defined(__aarch64__)
  void *ori_stack_addr = nullptr;
  size_t ori_stack_size = 0;
  void *stack_addr = nullptr;
  uint64_t tenant_id = GET_TENANT_ID() == 0 ? 500 : GET_TENANT_ID();
  const int64_t stack_size = STACK_PER_EXTEND;
  if (OB_FAIL(get_stackattr(ori_stack_addr, ori_stack_size))) {
  } else if (FALSE_IT(all_stack_size = 0 == all_stack_size ?
    ori_stack_size : all_stack_size)) {
  } else if (all_stack_size + stack_size > ALL_STACK_LIMIT) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_ISNULL(stack_addr = lib::g_stack_allocer.smart_call_alloc(tenant_id, stack_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    all_stack_size += stack_size;
    // To prevent the check_stack_overflow call before the jump
    // Do not introduce other code between the two set_stackattr
    set_stackattr(stack_addr, stack_size);
    ret = jump_call(arg_, func_, (char *)stack_addr + stack_size);
    set_stackattr(ori_stack_addr, ori_stack_size);
    lib::g_stack_allocer.dealloc(stack_addr);
    all_stack_size -= stack_size;
  }
#else
  ret = func_(arg_);
#endif
  return ret;
}


#ifndef OB_USE_ASAN
#define SMART_CALL(func)                                                    \
  ({                                                                        \
    int ret = OB_SUCCESS;                                                   \
    bool is_overflow = false;                                               \
    if (OB_FAIL(check_stack_overflow(is_overflow, STACK_RESERVED_SIZE))) {  \
    } else if (!is_overflow) {                                              \
      ret = func;                                                           \
    } else {                                                                \
      std::function<int()> f = [&]() {                                      \
        int ret = OB_SUCCESS;                                               \
        try {                                                               \
          in_try_stmt = true;                                               \
          ret = func;                                                       \
          in_try_stmt = false;                                              \
        } catch (OB_BASE_EXCEPTION &except) {                               \
          ret = except.get_errno();                                         \
          in_try_stmt = false;                                              \
        }                                                                   \
        return ret;                                                         \
      };                                                                    \
      int(*func_) (void*) = [](void *arg) { return (*(decltype(f)*)(arg))(); };\
      void * arg_ = &f;                                                     \
      ret = call_with_new_stack(arg_, func_);                               \
    }                                                                       \
    ret;                                                                    \
  })
#else
#define SMART_CALL(func)                                                    \
  ({                                                                        \
    int ret = OB_SUCCESS;                                                   \
    bool is_overflow = false;                                               \
    if (OB_FAIL(check_stack_overflow(is_overflow, STACK_RESERVED_SIZE))) {  \
    } else if (!is_overflow) {                                              \
      ret = func;                                                           \
    } else {                                                                \
      ret = OB_SIZE_OVERFLOW;                                               \
   }                                                                        \
    ret;                                                                    \
  })
#endif
} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEANBASE_SMART_CALL_ */
