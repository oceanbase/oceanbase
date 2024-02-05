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

#ifndef _OCEANBASE_SMART_VAR_
#define _OCEANBASE_SMART_VAR_

#include "common/ob_common_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

// SMART_VAR stack selection strategy:
// When sizeof(T) <8K ||
// (sizeof(T) <128K && stack_used <256K && stack_free> sizeof(T) + 64K)
// Allocate from the stack, and the rest are allocated from the heap
constexpr int64_t SMART_VAR_MAX_STACK_USE_SIZE = 256L << 10;
#ifndef TEST_SMART_VAR
  constexpr int64_t SMART_VAR_MAX_SINGLE_STACK_USE_SIZE = 128L << 10;
#else
  constexpr int64_t SMART_VAR_MAX_SINGLE_STACK_USE_SIZE = 2L << 20;
#endif
constexpr int64_t LOCAL_VARIABLE_SIZE_HARD_LIMIT = 2L << 20;

inline int check_from_heap(const int size, bool &from_heap)
{
  int ret = OB_SUCCESS;
  from_heap = false;
  bool is_overflow = false;
  int64_t used = 0;
  if (OB_FAIL(check_stack_overflow(is_overflow,
                                   size + get_reserved_stack_size(),
                                   &used))) {
  } else {
    if (OB_UNLIKELY(is_overflow || used > SMART_VAR_MAX_STACK_USE_SIZE)) {
      from_heap = true;
    }
  }
  return ret;
}

template<typename T>
inline int precheck(const bool direct_heap, bool &from_heap)
{
  int ret = OB_SUCCESS;
  if (direct_heap) {
    from_heap = true;
  } else {
    ret = check_from_heap(sizeof(T), from_heap);
  }
  return ret;
}

extern void *smart_alloc(const int64_t, const char *);
extern void smart_free(void *);

class SVBase
{
public:
  SVBase(const int ret, const bool from_heap)
    : ret_(ret),
      from_heap_(from_heap)
  {}
  int ret_;
  const bool from_heap_;
};

template<typename T,
         bool direct_heap>
class SV : public SVBase
{
public:
  template<typename Constructor>
  SV(const int ret, const bool from_heap, Constructor &&cons)
    : SVBase(ret, from_heap), v_(nullptr)
  {
    if (OB_SUCCESS == ret_) {
      if (from_heap_) {
        void *ptr = smart_alloc(sizeof(T), "HeapVar");
        if (OB_ISNULL(ptr)) {
          ret_ = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          v_ = cons(ptr);
        }
      } else {
        // Stack memory is delayed until the caller's context allocation
      }
    }
  }
  ~SV()
  {
    if (OB_LIKELY(v_ != nullptr)) {
      OB_ASSERT(OB_SUCCESS == ret_);
      v_->~T();
      if (OB_UNLIKELY(from_heap_)) {
        smart_free(v_);
      }
      v_ = nullptr;
    }
  }
  T &get()
  {
    return *v_;
  }
  T *v_;
};

template<typename T, int N, bool direct_heap>
class SV<T[N], direct_heap> : public SVBase
{
  typedef T Array[N];
public:
  template<typename Constructor>
  SV(const int ret, const bool from_heap, Constructor &&cons)
    : SVBase(ret, from_heap), v_(nullptr)
  {
    if (OB_SUCCESS == ret_) {
      if (from_heap_) {
        void *ptr = smart_alloc(sizeof(Array), "HeapVar");
        if (OB_ISNULL(ptr)) {
          ret_ = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          v_ = cons(ptr);
        }
      } else {
        // Stack memory is delayed until the caller's context allocation
      }
    }
  }
  ~SV()
  {
    if (OB_LIKELY(v_ != nullptr)) {
      OB_ASSERT(OB_SUCCESS == ret_);
      for (int i = 0; i < N; i++) {
        v_[i].~T();
      }
      if (OB_UNLIKELY(from_heap_)) {
        smart_free(v_);
      }
      v_ = nullptr;
    }
  }
  Array &get()
  {
    return *reinterpret_cast<Array*>(v_);
  }
  T *v_;
};

#define SELECT2(f, s, ...) s
#define EVAL_MACRO(macro, args...) macro(args)

#ifndef ENABLE_SMART_VAR_CHECK
#define __SMART_VAR(direct_heap, from_heap, s, buf, T, v, args...)                    \
  bool from_heap = false;                                                             \
  ret = OB_SUCCESS != ret ? ret : precheck<T>(direct_heap, from_heap);                \
  char buf[OB_SUCCESS == ret && !from_heap ? sizeof(T) : 0];                          \
  SV<T, direct_heap> s{ret, from_heap, [&](void *ptr) { return new (ptr) T{args}; }}; \
  if (OB_SUCCESS == s.ret_ && !from_heap) {                                           \
    s.v_ = new (buf) T{args};                                                         \
  }                                                                                   \
  auto &v = s.get();                                                                  \
  auto v##_ret = s.ret_;

#define _SMART_VAR(direct_heap, T, v, args...)                  \
  __SMART_VAR(direct_heap,                                      \
              v##_from_heap,                                    \
              v##_s,                                            \
              v##_buf,                                          \
              T, v, ##args)

#define SMART_VAR_HELPER(T, v, args...)                                         \
  STATIC_ASSERT(sizeof(T) < LOCAL_VARIABLE_SIZE_HARD_LIMIT, "large object!!!"); \
  _SMART_VAR((sizeof(T) > SMART_VAR_MAX_SINGLE_STACK_USE_SIZE), T, v, ##args)

#define HEAP_VAR_HELPER(T, v, args...)          \
  _SMART_VAR(true, T, v, ##args)

#define SMART_VAR(T, v, args...)                \
  SMART_VAR_HELPER(T, v, ##args)                \
  if (OB_SUCC(v##_ret))

#define HEAP_VAR(T, v, args...)                 \
  HEAP_VAR_HELPER(T, v, ##args)                 \
  if (OB_SUCC(v##_ret))

#define __SMART_or_HEAP_VARS_2(helper, v1, v2, tuple1, tuple2) \
  helper tuple1                                                \
  helper tuple2                                                \
  if (OB_SUCC(v1##_ret) && OB_SUCC(v2##_ret))

#define _SMART_or_HEAP_VARS_2(type, v1, v2, tuple1, tuple2)             \
  __SMART_or_HEAP_VARS_2(type##_VAR_HELPER, v1, v2, tuple1, tuple2)

#define __SMART_or_HEAP_VARS_3(helper, v1, v2, v3, tuple1, tuple2, tuple3) \
  helper tuple1                                                            \
  helper tuple2                                                            \
  helper tuple3                                                            \
  if (OB_SUCC(v1##_ret) && OB_SUCC(v2##_ret) && OB_SUCC(v3##_ret))

#define _SMART_or_HEAP_VARS_4(type, v1, v2, v3, v4, tuple1, tuple2, tuple3, tuple4) \
  __SMART_or_HEAP_VARS_4(type##_VAR_HELPER, v1, v2, v3, v4, tuple1, tuple2, tuple3, tuple4)

#define __SMART_or_HEAP_VARS_4(helper, v1, v2, v3, v4, tuple1, tuple2, tuple3, tuple4) \
  helper tuple1                                                       \
  helper tuple2                                                       \
  helper tuple3                                                       \
  helper tuple4                                                       \
  if (OB_SUCC(v1##_ret) && OB_SUCC(v2##_ret) && OB_SUCC(v3##_ret) && OB_SUCC(v4##_ret))

#define _SMART_or_HEAP_VARS_3(type, v1, v2, v3, tuple1, tuple2, tuple3) \
  __SMART_or_HEAP_VARS_3(type##_VAR_HELPER, v1, v2, v3, tuple1, tuple2, tuple3)

#define SMART_VARS_2(tuple1, tuple2)                                    \
  EVAL_MACRO(_SMART_or_HEAP_VARS_2, SMART, SELECT2 tuple1, SELECT2 tuple2, tuple1, tuple2)

#define SMART_VARS_3(tuple1, tuple2, tuple3)                            \
  EVAL_MACRO(_SMART_or_HEAP_VARS_3, SMART, SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, tuple1, tuple2, tuple3)

#define SMART_VARS_4(tuple1, tuple2, tuple3, tuple4)                                                       \
  EVAL_MACRO(_SMART_or_HEAP_VARS_4, SMART, SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, SELECT2 tuple4, \
             tuple1, tuple2, tuple3, tuple4)

#define HEAP_VARS_2(tuple1, tuple2)                                     \
  EVAL_MACRO(_SMART_or_HEAP_VARS_2, HEAP, SELECT2 tuple1, SELECT2 tuple2, tuple1, tuple2)

#define HEAP_VARS_3(tuple1, tuple2, tuple3)                             \
  EVAL_MACRO(_SMART_or_HEAP_VARS_3, HEAP, SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, tuple1, tuple2, tuple3)

#define HEAP_VARS_4(tuple1, tuple2, tuple3, tuple4)                                                       \
  EVAL_MACRO(_SMART_or_HEAP_VARS_4, HEAP, SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, SELECT2 tuple4, \
             tuple1, tuple2, tuple3, tuple4)

#else

#define SELECT1(f, ...) f
#define SELECT_ARGS(T, v, args...) args

#define SMART_VAR(T, v, args...)                \
  using t_##v = T;                              \
  for (t_##v v{args};;)                         \
    if (false)

#define HEAP_VAR(T, v, args...) SMART_VAR(T, v, args)

#define _SMART_VARS_2(T1, T2, v1, v2, tuple1, tuple2)   \
  using t_##v1 = T1;                                    \
  using t_##v2 = T2;                                    \
  for (t_##v1 v1{SELECT_ARGS tuple1};;)                 \
    for (t_##v2 v2{SELECT_ARGS tuple2};;)               \
      if (false)

#define SMART_VARS_2(tuple1, tuple2)                            \
  EVAL_MACRO(_SMART_VARS_2, SELECT1 tuple1, SELECT1 tuple2,     \
             SELECT2 tuple1, SELECT2 tuple2, tuple1, tuple2)

#define _SMART_VARS_3(T1, T2, T3, v1, v2, v3, tuple1, tuple2, tuple3)   \
  using t_##v1 = T1;                                                    \
  using t_##v2 = T2;                                                    \
  using t_##v3 = T3;                                                    \
  for (t_##v1 v1{SELECT_ARGS tuple1};;)                                 \
    for (t_##v2 v2{SELECT_ARGS tuple2};;)                               \
      for (t_##v3 v3{SELECT_ARGS tuple3};;)                             \
        if (false)

#define SMART_VARS_3(tuple1, tuple2, tuple3)                                \
  EVAL_MACRO(_SMART_VARS_3, SELECT1 tuple1, SELECT1 tuple2, SELECT1 tuple3, \
             SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, tuple1, tuple2, tuple3)

#define _SMART_VARS_4(T1, T2, T3, T4, v1, v2, v3, v4, tuple1, tuple2, tuple3, tuple4) \
  using t_##v1 = T1;                                                    \
  using t_##v2 = T2;                                                    \
  using t_##v3 = T3;                                                    \
  using t_##v4 = T4;                                                    \
  for (t_##v1 v1{SELECT_ARGS tuple1};;)                                 \
    for (t_##v2 v2{SELECT_ARGS tuple2};;)                               \
      for (t_##v3 v3{SELECT_ARGS tuple3};;)                             \
        for (t_##v4 v4{SELECT_ARGS tuple4};;)                           \
          if (false)

#define SMART_VARS_4(tuple1, tuple2, tuple3, tuple4)                                        \
  EVAL_MACRO(_SMART_VARS_4, SELECT1 tuple1, SELECT1 tuple2, SELECT1 tuple3, SELECT1 tuple4, \
             SELECT2 tuple1, SELECT2 tuple2, SELECT2 tuple3, SELECT2 tuple4, tuple1, tuple2, tuple3, tuple4)

#define HEAP_VARS_2(tuple1, tuple2) SMART_VARS_2(tuple1, tuple2)

#define HEAP_VARS_3(tuple1, tuple2, tuple3) SMART_VARS_3(tuple1, tuple2, tuple3)

#define HEAP_VARS_4(tuple1, tuple2, tuple3, tuple4) SMART_VARS_4(tuple1, tuple2, tuple3, tuple4)

#endif // ENABLE_SMART_VAR_CHECK

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEANBASE_SMART_VAR_ */
