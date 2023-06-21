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

// ┌─────────────┐Upgrade ┌─────────────┐
// │ObUniqueGuard├───────►│ObSharedGuard│
// └─────────────┘        └──▲──────┬───┘
//                           │      │
//                    Upgrade│      │Downgrade
//                           │      │
//                         ┌─┴──────▼──┐
//                         │ObWeakGuard│
//                         └───────────┘

// ┌─────────────┐ Transfer Ownership ┌─────────────┐
// │ObUniqueGuard│◄──────────────────►│ObUniqueGuard│
// └─────────────┘                    └─────────────┘

// ┌─────────────┐ Share Ownership ┌─────────────┐
// │ObSharedGuard│◄───────────────►│ObSharedGuard│
// └─────────────┘                 └─────────────┘

// ┌───────────┐ Observe the same object ┌───────────┐
// │ObWeakGuard│◄───────────────────────►│ObWeakGuard│
// └───────────┘                         └───────────┘

/**
 * ObUniqueGuard
 *
 * ObUniqueGuard protect a raw pointer's ownership, promise there is at most one owner holding the
 * guard, if there is no owner holding, the protected raw pointer will be deleted properly.
 *
 *
 * When to use:
 *   - hope a pointer has unique ownership, and could delete(both logically and physically) it
 *     customized under RAII.
 *
 * When not to use:
 *   - you don't hope so.
 *
 * Memory usage:
 *   - sizeof(ObUniqueGuard<T>) is 24 Bytes in common, including 3 pointers.
 *   - if ObUniqueGuard is assigned by a raw pointer, a piece of heap memory for deleter will be al-
 *     located, to avoid this, using ob_make_unique() or ob_alloc_unique() to ceate a ObUniqueGuard
 *     and object with only one time alloc.
 *
 * Manual:
 *   - Interface
 *     1. construction
 *     + default construction:
 *         ObUniqueGuard()
 *     + copy construction:
 *         ObUniqueGuard(const ObUniqueGuard<T> &rhs)
 *     2. destruction
 *     + reset:
 *         void reset()
 *         idempotent interface
 *     + default construction:
 *         ~ObUniqueGuard()
 *         just simply call reset()
 *     3. check
 *     + valid:
 *         bool is_valid()
 *     4. assign
 *     + int assign(const ObUniqueGuard<T> &)
 *         success always
 *     + template <typename FUNC>
 *       int assign(T *, FUNC &&, ObIAllocator &)
 *         general assign, could assigned by a raw pointer, may return OB_INVALID_ARGUMENT or
 *         OB_ALLOCATE_MEMORY_FAILED.
 *     5. using
 *       using ObUniqueGuard<T> obj just like using T* obj.
 *
 *  - CAUTION:
 *      + MAKE SURE ObUniqueGuard is valid before using it, or will CRASH.
 *      + if ObUniqueGuard is created by ob_make_unique() or ob_alloc_unique(), the delete action
 *        behavior is just call ptr's destuction method and call allocator's free method, WILL NOT
 *        CALL destory() method even if ptr has one.
 *
 *  - Contact  for help.
 */

#ifndef OCEANBASE_LIB_GUARD_OB_UNIQUE_GUARD_H
#define OCEANBASE_LIB_GUARD_OB_UNIQUE_GUARD_H

#include "lib/function/ob_function.h"

namespace oceanbase
{
namespace unittest
{
class TestObGuard;
}
namespace common
{

template <typename T>
class ObUniqueGuard;

template <typename T>
class ObSharedGuard;

namespace guard
{

struct DefaultUniqueGuardAllocator : public ObIAllocator {
  void *alloc(const int64_t size) override {
#ifdef UNITTEST_DEBUG
    total_alive_num++;
#endif
    static lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "ObGuard");
    SET_USE_500(attr);
    return ob_malloc(size, attr);
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override {
#ifdef UNITTEST_DEBUG
    total_alive_num--;
#endif
    ob_free(ptr);
  }
#ifdef UNITTEST_DEBUG
  int total_alive_num = 0;
#endif
  static DefaultUniqueGuardAllocator &get_default_allocator() {
    static DefaultUniqueGuardAllocator default_allocator;
    return default_allocator;
  }
};

template <typename T>
struct UniqueGuardFriend {
  static inline ObUniqueGuard<T> create_unique(T *data, ObIAllocator &alloc) {
    ObUniqueGuard<T> ptr;
    ptr.data_ = data;
    ptr.allocator_ = &alloc;
    return ptr;
  }
};

}// namespace guard

#define DEFAULT_ALLOCATOR guard::DefaultUniqueGuardAllocator::get_default_allocator()

template <typename T>
class ObUniqueGuard
{
  friend class unittest::TestObGuard;
  template <typename T2>
  friend struct guard::UniqueGuardFriend;
  template <typename T3>
  friend class ObSharedGuard;
public:
  // exception-safety : no throw
  ObUniqueGuard() : data_(nullptr), allocator_(nullptr), deleter_(nullptr) {}

  // exception-safety : no throw
  ObUniqueGuard(const ObUniqueGuard<T> &rhs) :
  data_(nullptr), allocator_(nullptr), deleter_(nullptr) { swap_(rhs); }

  ~ObUniqueGuard() { reset(); }

  // exception-safety : strong
  ObUniqueGuard<T> &operator=(const ObUniqueGuard<T> &rhs) {
    (void) assign(rhs);
    return *this;
  }

  // exception-safety : no throw
  int assign(const ObUniqueGuard<T> &rhs) {
    if (&rhs != this) {
      reset();
      swap_(rhs);
    }
    return OB_SUCCESS;
  }

  // exception-safety : strong
  template <typename FUNC>
  int assign(T *data, FUNC &&deleter, ObIAllocator &alloc = DEFAULT_ALLOCATOR) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObFunction<void(T*)>* temp_ptr =
                            (ObFunction<void(T*)>*)alloc.alloc(sizeof(ObFunction<void(T*)>));
      if (OB_LIKELY(nullptr != temp_ptr)) {
        temp_ptr = new (temp_ptr)ObFunction<void(T*)>(alloc);// success always
        // may alloc memory failed if deleter is too big
        ret = temp_ptr->assign(std::forward<FUNC>(deleter));
        if (OB_SUCC(ret)) {
          reset();// it's safe to reset now
          data_ = data;
          allocator_ = &alloc;
          deleter_ = temp_ptr;
        } else {
          alloc.free(temp_ptr);
        }
      } else {// exception-safety guaranteed
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    return ret;
  }

  // exception-safety : no throw
  void reset() {
    if ((!OB_ISNULL(data_)) && (!OB_ISNULL(allocator_))) {
      if ((!OB_ISNULL(deleter_)) && OB_LIKELY(deleter_->is_valid())) {// constructed from assign()
        (*deleter_)(data_);
        allocator_->free(deleter_);
      } else {// constructed from make_unique() or alloc_unique()
        data_->~T();
        allocator_->free(data_);
      }
    } else if (!OB_ISNULL(data_)) {
      OCCAM_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "should never go here", K(*this));
    } else if (!OB_ISNULL(allocator_)) {
      OCCAM_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "should never go here", K(*this));
    } else {}// it'ok that both data_ and allocator_ are nullptr, ObUniqueGuard has been reset
    data_ = nullptr;
    deleter_ = nullptr;
    allocator_ = nullptr;
  }

  // exception-safety : no throw
  bool is_valid() const { return nullptr != data_ && nullptr != allocator_; }
  T *get_ptr() const { return data_; }
  T &operator*() const { return *data_; }
  T *operator->() const { return data_; }
  TO_STRING_KV(KP(this), KP_(data), KP_(deleter));
private:
  // exception-safety : no throw
  void swap_(const ObUniqueGuard<T> &rhs) {
    void *temp = rhs.data_;
    const_cast<ObUniqueGuard<T>&>(rhs).data_ = data_;
    data_ = (T*)temp;
    temp = rhs.allocator_;
    const_cast<ObUniqueGuard<T>&>(rhs).allocator_ = allocator_;
    allocator_ = (ObIAllocator*)temp;
    temp = rhs.deleter_;
    const_cast<ObUniqueGuard<T>&>(rhs).deleter_ = deleter_;
    deleter_ = (ObFunction<void(T*)>*)temp;
  }

private:
  T *data_;
  ObIAllocator *allocator_;
  ObFunction<void(T*)> *deleter_;
};

template<typename T,
         typename ...Args,
         typename std::enable_if<!std::is_array<T>::value, bool>::type = true>
inline int ob_alloc_unique(ObUniqueGuard<T> &guard, ObIAllocator &alloc, Args&& ...args) {
  int ret = OB_SUCCESS;
  static_assert(std::is_constructible<T, Args...>::value, "the object is not constructible");
  T *data = nullptr;
  if (OB_UNLIKELY(nullptr != (data = (T *)alloc.alloc(sizeof(T))))) {
    data = new (data)T(std::forward<Args>(args)...);
    (void) guard.assign(guard::UniqueGuardFriend<T>::create_unique(data, alloc));
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

template<typename T,
         typename ...Args,
         typename std::enable_if<!std::is_array<T>::value, bool>::type = true>
inline int ob_make_unique(ObUniqueGuard<T> &guard, Args&& ...args) {
  return ob_alloc_unique<T>(guard, DEFAULT_ALLOCATOR, std::forward<Args>(args)...);
}

}// namespace common
}// namespace oceanbase
#undef DEFAULT_ALLOCATOR
#endif
