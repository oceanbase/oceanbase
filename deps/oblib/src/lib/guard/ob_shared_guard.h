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
 * ObSharedGuard
 *
 * ObSharedGuard protect a raw pointer's ownership, promise if there is at least one owner holding
 * the guard, pointer will not be deleted, and if there is no holder, pointer will be deleted
 * properly.
 *
 *
 * When to use:
 *   - hope a pointer has shared ownership, and could delete(both logically and physically) it
 *     customized under RAII.
 *
 * When not to use:
 *   - you don't hope so.
 *
 * Memory usage:
 *   - sizeof(ObSharedGuard<T>) is 16 Bytes in common, including 2 pointers.
 *   - if ObSharedGuard is assigned by a raw pointer, a piece of heap memory for control block will
 *     be allocated, to avoid this, using ob_make_shared() or ob_alloc_shared() to ceate a
 *     ObSharedGuard and object with only one time alloc.
 *
 * Manual:
 *   - Interface
 *     1. construction
 *     + default construction:
 *         ObSharedGuard()
 *     + copy construction:
 *         ObSharedGuard(const ObSharedGuard<T> &rhs)
 *     2. destruction
 *     + reset:
 *         void reset()
 *         idempotent interface
 *     + default construction:
 *         ~ObSharedGuard()
 *         just simply call reset()
 *     3. check
 *     + valid:
 *         bool is_valid()
 *     4. assign
 *     + int assign(const ObSharedGuard<T> &)
 *         success always
 *     + template <typename FUNC>
 *       int assign(T *, FUNC &&, ObIAllocator &)
 *         general assign, could assigned by a raw pointer, may return OB_INVALID_ARGUMENT or
 *         OB_ALLOCATE_MEMORY_FAILED.
 *     5. using
 *       using ObSharedGuard<T> obj just like using T* obj.
 *
 *  - CAUTION:
 *      + MAKE SURE ObSharedGuard is valid before using it, or will CRASH.
 *      + if the first ObSharedGuard is created by ob_make_shared() or ob_alloc_shared(), the delete
 *        action will be acted after the last ObSharedGuard destructed, and it's behavior is just
 *        call ptr's destruction method and call allocator's free method, WILL NOT CALL destory()
 *        method even if ptr has one.
 *      + if ObSharedGuard is created by ob_make_shared() or ob_alloc_shared(), ptr's destruct
 *        method will be called immediately, but it's memory will be delay-freed after the last
 *        associated ObWeakGuard destructed.
 *
 *  - Contact  for help.
 */

#ifndef OCEANBASE_LIB_GUARD_OB_SHARED_GUARD_H
#define OCEANBASE_LIB_GUARD_OB_SHARED_GUARD_H

#include <type_traits>
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/function/ob_function.h"
#include "ob_unique_guard.h"

namespace oceanbase
{
namespace unittest
{
class TestObGuard;
}
namespace common
{

template <typename T>
class ObWeakGuard;

template <typename T>
class ObSharedGuard;

namespace guard
{

struct DefaultSharedGuardAllocator : public ObIAllocator {
  void *alloc(const int64_t size) override {
#ifdef UNITTEST_DEBUG
    total_alive_num++;
#endif
    static lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "ObGuard");
    SET_USE_500(attr);
    return ob_malloc(size, attr);
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
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
  static DefaultSharedGuardAllocator &get_default_allocator() {
    static DefaultSharedGuardAllocator default_allocator;
    return default_allocator;
  }
};

template <typename T>
struct ControlBlock {
  ControlBlock(ObIAllocator &allocator, void *alloc_area_pointer) :
  weak_count_(1),
  shared_count_(1),
  allocator_(allocator),
  alloc_area_pointer_(alloc_area_pointer) {}
  template <typename FUNC>
  ControlBlock(FUNC &&deleter, ObIAllocator &allocator, void *alloc_area_pointer) :
  weak_count_(1),
  shared_count_(1),
  deleter_(std::forward<FUNC>(deleter), allocator),
  allocator_(allocator),
  alloc_area_pointer_(alloc_area_pointer) {}
  void inc_weak_and_shared() {
    ATOMIC_INC(&weak_count_);
    ATOMIC_INC(&shared_count_);
  }
  int64_t dec_shared() {
    return ATOMIC_AAF(&shared_count_, -1);
  }
  void inc_weak() {
    ATOMIC_INC(&weak_count_);
  }
  int64_t dec_weak() {
    return ATOMIC_AAF(&weak_count_, -1);
  }
  int64_t lock() {
    int64_t old_count_;
    do {
      old_count_ = ATOMIC_LOAD(&shared_count_);
    } while(old_count_ > 0 && !ATOMIC_CAS(&shared_count_, old_count_, old_count_ + 1));
    if (OB_LIKELY(old_count_ != 0)) {
      inc_weak();
    }
    return old_count_;
  }
  // data field
  int64_t weak_count_;
  int64_t shared_count_;
  ObFunction<void(T*)> deleter_;
  ObIAllocator &allocator_;
  void *alloc_area_pointer_;
};

template <typename T>
struct BlockPtr {
  // default construction
  BlockPtr() : control_ptr_(nullptr), data_ptr_(nullptr) {}
  // copy construction
  BlockPtr(const BlockPtr &block_ptr) :
  control_ptr_(block_ptr.control_ptr_), data_ptr_(block_ptr.data_ptr_) {}
  // normal construction
  BlockPtr(ControlBlock<T> *control_block_ptr, T *data_block_ptr) :
  control_ptr_(control_block_ptr), data_ptr_(data_block_ptr) {}
  bool is_valid() const { return OB_NOT_NULL(control_ptr_) && OB_NOT_NULL(data_ptr_); }
  void reset() { control_ptr_ = nullptr; data_ptr_ = nullptr; }
  // data field
  ControlBlock<T> *control_ptr_;
  T *data_ptr_;
};

template <typename T>
struct CombinedBLock {
  CombinedBLock() = delete;
  // data field
  ControlBlock<T> control_block_;
  T data_block_;
};

template <typename T>
struct ObSharedGuardFriend {
  // this construction used by ob_make_shared, to alllocating a piece of contiguous memory
  // to store both control block and data block.
  static inline ObSharedGuard<T> create_shared_from_combined_block(const CombinedBLock<T> &block) {
    ObSharedGuard<T> ptr;
    ptr.block_ptr_.control_ptr_ = &(const_cast<CombinedBLock<T> &>(block).control_block_);
    ptr.block_ptr_.data_ptr_ = &(const_cast<CombinedBLock<T> &>(block).data_block_);
    return ptr;
  }
  // this construction used by ObWeakGuard, after inc shared count success,
  // so no need add count here, just construct a obj contains control and data block ptr
  static inline ObSharedGuard<T> create_shared_from_block_ptr(const BlockPtr<T> &block_ptr) {
    ObSharedGuard<T> ptr;
    ptr.block_ptr_ = block_ptr;
    return ptr;
  }
};
}// namespace guard

#define DEFAULT_ALLOCATOR guard::DefaultSharedGuardAllocator::get_default_allocator()

template <typename T>
class ObSharedGuard {
  friend class unittest::TestObGuard;
  template <typename T2>
  friend struct guard::ObSharedGuardFriend;
  template <typename T3>
  friend class ObWeakGuard;
public:
  ObSharedGuard() = default;

  ObSharedGuard(const ObSharedGuard<T> &rhs) :
  block_ptr_(rhs.block_ptr_) {
    if (OB_LIKELY(block_ptr_.is_valid())) {
      block_ptr_.control_ptr_->inc_weak_and_shared();
    }
  }

  ~ObSharedGuard() { reset(); }

  ObSharedGuard &operator=(const ObSharedGuard<T> &rhs) {
    (void)assign(rhs);// success always
    return *this;
  }

  int assign(const ObSharedGuard<T> &rhs) {
    if (&rhs != this) {
      reset();
      block_ptr_ = rhs.block_ptr_;
      if (OB_LIKELY(block_ptr_.is_valid())) {
        block_ptr_.control_ptr_->inc_weak_and_shared();
      }
    }
    return OB_SUCCESS;
  }

  int assign(const ObUniqueGuard<T> &unique_guard, ObIAllocator &allocator = DEFAULT_ALLOCATOR) {
    #define UNI_PTR const_cast<ObUniqueGuard<T> &>(unique_guard)
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!unique_guard.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      guard::ControlBlock<T> *temp_ptr = nullptr;
      if (OB_NOT_NULL(temp_ptr =
        (guard::ControlBlock<T> *)allocator.alloc(sizeof(guard::ControlBlock<T>)))) {
        ObIAllocator *uniq_alloc_ptr = UNI_PTR.allocator_;
        // unique_ptr is created by make_unique() or alloc_unique()
        if (OB_ISNULL(UNI_PTR.deleter_)) {
          temp_ptr = new (temp_ptr)guard::ControlBlock<T>([uniq_alloc_ptr](T *ptr) {
                                                            ptr->~T();// logical delete
                                                            uniq_alloc_ptr->free(ptr);// physical delete
                                                          },
                                                          allocator,
                                                          temp_ptr);
        } else { // unique_ptr is created by raw pointer
          temp_ptr = new (temp_ptr)guard::ControlBlock<T>(std::move(*UNI_PTR.deleter_),
                                                          allocator,
                                                          temp_ptr);
        }
        if (OB_LIKELY(temp_ptr->deleter_.is_valid())) {
          reset();
          block_ptr_.data_ptr_ = UNI_PTR.get_ptr();
          block_ptr_.control_ptr_ = temp_ptr;
          // make sure unique_guard not hold pointer anymore.
          // unique_guard's deleter is allocated from heap, need free.
          // (but may be NULL if unique_guard is created by make_unique() or alloc_unique())
          if (OB_NOT_NULL(UNI_PTR.deleter_)) {
            UNI_PTR.deleter_->~ObFunction();// logical delete
            UNI_PTR.allocator_->free(UNI_PTR.deleter_);// physical delete
          }
          UNI_PTR.deleter_ = nullptr;
          UNI_PTR.allocator_ = nullptr;
          UNI_PTR.data_ = nullptr;
        } else {// if ObFunction move assign failed, unique_guard.deleter_ is not changed
          allocator.free(temp_ptr);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    return ret;
    #undef UNI_PTR
  }

  template <typename FUNC>
  int assign(T *data, FUNC &&deleter, ObIAllocator &allocator = DEFAULT_ALLOCATOR) {
    int ret = OB_SUCCESS;
    if (!OB_ISNULL(data)) {
      guard::ControlBlock<T> *temp_ptr = nullptr;
      if (OB_UNLIKELY(nullptr != (temp_ptr =
        (guard::ControlBlock<T> *)allocator.alloc(sizeof(guard::ControlBlock<T>))))) {
        temp_ptr = new (temp_ptr)guard::ControlBlock<T>(std::forward<FUNC>(deleter),
                                                        allocator,
                                                        temp_ptr);
        // may alloc memory failed if deleter is too big
        if (OB_UNLIKELY(!temp_ptr->deleter_.is_valid())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          allocator.free(temp_ptr);
        } else {// it's safe to reset now
          reset();
          block_ptr_.data_ptr_ = data;
          block_ptr_.control_ptr_ = temp_ptr;
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    return ret;
  }

  bool is_valid() const {
    return block_ptr_.is_valid();
  }

  void reset() {
    if (OB_LIKELY(is_valid())) {
      if (OB_UNLIKELY(block_ptr_.control_ptr_->dec_shared() == 0)) {
        if (block_ptr_.control_ptr_->deleter_.is_valid()) {
          // logical and physical delete raw pointer(free memory is user's duty)
          (block_ptr_.control_ptr_->deleter_)(block_ptr_.data_ptr_);
        } else {
          block_ptr_.data_ptr_->~T();// logical delete raw pointer
        }
      }

      if (OB_UNLIKELY(block_ptr_.control_ptr_->dec_weak() == 0)) {
        // physical delete control block
        // (maybe with data block either if SharedGuard is created through make_shared() or alloc_s-
        // hared())
        block_ptr_.control_ptr_->~ControlBlock();// deleter will be logical deleted
        block_ptr_.control_ptr_->allocator_.free(block_ptr_.control_ptr_->alloc_area_pointer_);
      }
    }
    block_ptr_.reset();
  }

  T &operator*() const { return *block_ptr_.data_ptr_; }
  T *operator->() const { return block_ptr_.data_ptr_; }
  T *get_ptr() const { return block_ptr_.data_ptr_; }
  int64_t get_weak_count() const { return ATOMIC_LOAD(&(block_ptr_.control_ptr_->weak_count_)); }
  int64_t get_shared_count() const { return ATOMIC_LOAD(&(block_ptr_.control_ptr_->shared_count_)); }
  TO_STRING_KV(KP(this), KP_(block_ptr_.control_ptr), KP_(block_ptr_.data_ptr));
private:
  guard::BlockPtr<T> block_ptr_;
};

template<typename T,
         typename ...Args,
         typename std::enable_if<!std::is_array<T>::value, bool>::type = true>
inline int ob_alloc_shared(ObSharedGuard<T> &guard, ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  guard::CombinedBLock<T> *temp_ptr = nullptr;
  if (OB_NOT_NULL(temp_ptr =
    (guard::CombinedBLock<T> *)alloc.alloc(sizeof(guard::CombinedBLock<T>)))) {
    guard.reset();
    new(&(temp_ptr->control_block_)) guard::ControlBlock<T>(alloc, (void *)temp_ptr);
    guard = guard::ObSharedGuardFriend<T>::create_shared_from_combined_block(*temp_ptr);
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

template<typename T,
         typename ...Args,
         typename std::enable_if<!std::is_array<T>::value, bool>::type = true>
inline int ob_make_shared(ObSharedGuard<T> &guard, Args&& ...args) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_alloc_shared<T>(guard, DEFAULT_ALLOCATOR))) {
    OCCAM_LOG(WARN, "allock memory failed", K(ret), K(lbt()));
  } else {
    new(guard.get_ptr()) T(std::forward<Args>(args)...);
  }
  return ret;
}
#undef DEFAULT_ALLOCATOR

}// namespace common
}// namespace oceanbase
#endif
