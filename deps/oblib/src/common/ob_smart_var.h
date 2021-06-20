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

namespace oceanbase {
namespace common {

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

inline int check_from_heap(const int size, bool& from_heap)
{
  int ret = OB_SUCCESS;
  from_heap = false;
  bool is_overflow = false;
  int64_t used = 0;
  if (OB_FAIL(check_stack_overflow(is_overflow, size + get_reserved_stack_size(), &used))) {
  } else {
    if (OB_UNLIKELY(is_overflow || used > SMART_VAR_MAX_STACK_USE_SIZE)) {
      from_heap = true;
    }
  }
  return ret;
}

extern void* smart_alloc(const int64_t, const char*);
extern void smart_free(void*);

class _SBase {
public:
  _SBase(const int ret, const bool from_heap) : ret_(ret), from_heap_(from_heap)
  {}
  int ret_;
  const bool from_heap_;
};

template <typename T, bool large, bool direct_heap = false>
class _S : public _SBase {
public:
  static int precheck(bool& from_heap, bool& by_vla)
  {
    int ret = OB_SUCCESS;
    if (direct_heap) {
      from_heap = true;
    } else {
      ret = check_from_heap(sizeof(T), from_heap);
    }
    by_vla = !from_heap;
    return ret;
  }

public:
  template <typename... Args>
  _S(const int ret, const bool from_heap, Args&&... args) : _SBase(ret, from_heap), v_(nullptr)
  {
    if (OB_SUCCESS == ret_) {
      if (from_heap_) {
        void* ptr = smart_alloc(sizeof(T), "HeapVar");
        if (OB_ISNULL(ptr)) {
          ret_ = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          v_ = new (ptr) T(args...);
        }
      } else {
        // Stack memory is delayed until the caller's context allocation
      }
    }
  }
  ~_S()
  {
    if (OB_LIKELY(v_ != nullptr)) {
      OB_ASSERT(OB_SUCCESS == ret_);
      v_->~T();
      if (OB_UNLIKELY(from_heap_)) {
        smart_free(v_);
      }
    }
  }
  T& get()
  {
    return *v_;
  }
  T* v_;
};

template <typename T, int N, bool direct_heap>
class _S<T[N], true, direct_heap> : public _SBase {
  typedef T Array[N];

public:
  static int precheck(bool& from_heap, bool& by_vla)
  {
    int ret = OB_SUCCESS;
    if (direct_heap) {
      from_heap = true;
    } else {
      ret = check_from_heap(sizeof(Array), from_heap);
    }
    by_vla = !from_heap;
    return ret;
  }

public:
  _S(const int ret, const bool from_heap) : _SBase(ret, from_heap), v_(nullptr)
  {
    if (OB_SUCCESS == ret_) {
      if (from_heap_) {
        void* ptr = smart_alloc(sizeof(Array), "HeapVar");
        if (OB_ISNULL(ptr)) {
          ret_ = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          v_ = new (ptr) Array;
        }
      } else {
        // Stack memory is delayed until the caller's context allocation
      }
    }
  }
  ~_S()
  {
    if (OB_LIKELY(v_ != nullptr)) {
      OB_ASSERT(OB_SUCCESS == ret_);
      for (int i = 0; i < N; i++) {
        v_[i].~T();
      }
      if (OB_UNLIKELY(from_heap_)) {
        smart_free(v_);
      }
    }
  }
  Array& get()
  {
    return *reinterpret_cast<Array*>(v_);
  }
  T* v_;
};

template <typename T>
class _S<T, false> : public _SBase {
public:
  static int precheck(bool& from_heap, bool& by_vla)
  {
    from_heap = false;
    by_vla = false;
    return OB_SUCCESS;
  }

public:
  template <typename... Args>
  _S(const int ret, const bool from_heap, Args&&... args) : _SBase(ret, from_heap), obj_(args...), v_(&obj_)
  {}
  T& get()
  {
    return *v_;
  }
  T obj_;
  T* v_;
};

template <typename T, int N>
class _S<T[N], false> : public _SBase {
  typedef T Array[N];

public:
  static int precheck(bool& from_heap, bool& by_vla)
  {
    from_heap = false;
    by_vla = false;
    return OB_SUCCESS;
  }

public:
  _S(const int ret, const bool from_heap) : _SBase(ret, from_heap), v_(&array_[0])
  {}
  Array& get()
  {
    return *reinterpret_cast<Array*>(v_);
  }
  Array array_;
  T* v_;
};

template <typename T, bool direct_heap>
using AssitType = _S<T, (sizeof(T) > OB_MAX_LOCAL_VARIABLE_SIZE), direct_heap>;

#define ___SMART_VAR(from_heap, by_vla, s, buf, direct_heap, T, v, args...) \
  bool from_heap = false;                                                   \
  bool by_vla = false;                                                      \
  ret = AssitType<T, direct_heap>::precheck(from_heap, by_vla);             \
  char buf[OB_SUCCESS == ret && by_vla ? sizeof(T) : 0];                    \
  AssitType<T, direct_heap> s{ret, from_heap, args};                        \
  if (OB_SUCCESS == s.ret_ && by_vla) {                                     \
    s.v_ = new (buf) T(args);                                               \
  }                                                                         \
  auto& v = s.get();                                                        \
  if (OB_SUCC(s.ret_))

#define __SMART_VAR(postfix, direct_heap, T, v, args...) \
  ___SMART_VAR(_from_heap##postfix, _by_vla##postfix, _s##postfix, _buf##postfix, direct_heap, T, v, args)

#define _SMART_VAR(postfix, direct_heap, T, v, args...) __SMART_VAR(postfix, direct_heap, T, v, args)

#define SMART_VAR(T, v, args...)                                                \
  STATIC_ASSERT(sizeof(T) < LOCAL_VARIABLE_SIZE_HARD_LIMIT, "large object!!!"); \
  _SMART_VAR(__COUNTER__, (sizeof(T) > SMART_VAR_MAX_SINGLE_STACK_USE_SIZE), T, v, args)

#define HEAP_VAR(T, v, args...) _SMART_VAR(__COUNTER__, true, T, v, args)

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OCEANBASE_SMART_VAR_ */
