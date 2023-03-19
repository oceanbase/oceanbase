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
 * ObWeakGuard
 *
 * ObWeakGuard doesn't hold any ownership, a valid ObWeakGuard just observe a valid ObSharedGuard,
 * ObWeakGuard just "observe" a object, ObWeakGuard could feel if the observed object exists, and
 * if so, ObWeakGuard could get the ownership of this object.
 * 
 * When to use:
 *   - you hope observe a shared object, but not own it.
 *
 * When not to use:
 *   - you don't hope so.
 *
 * Memory usage:
 *   - sizeof(ObWeakGuard<T>) is 16 Bytes in common, including 2 pointers.
 *   - ObWeakGuard promise WON'T alloc extra memory.
 *
 * Manual:
 *   - Interface
 *     1. construction
 *     + default construction:
 *         ObWeakGuard()
 *     + copy construction:
 *         ObWeakGuard(const ObWeakGuard<T> &rhs)
 *     + downgrade from ObSharedGuard construction:
 *         ObWeakGuard(const ObSharedGuard<T> &rhs)
 *     2. destruction
 *     + reset:
 *         void reset()
 *         idempotent interface
 *     + destruction:
 *         ~ObWeakGuard()
 *         just simply call reset()
 *     3. check
 *     + valid:
 *         bool is_valid() const
 *         check if ObWeakGuard is dangling
 *     + lockable
 *         bool upgradeable() const
 *         check if the observed object still exist
 *     4. assign
 *     + int assign(const ObWeakGuard<T> &rhs)
 *         success always
 *     + int assign(const ObSharedGuard<T> &rhs)
 *         success always
 *     5. ObSharedGuard<T> upgrade() const
 *       call upgrade() to get a ObSharedGuard<T> guard and hold the protected object's ownership,
 *       if the protected object dosen't exist, the returned ObSharedGuard will be invalid.
 *
 *  - CAUTION:
 *      + MAKE SURE ObWeakGuard is valid before using it, or will CRASH.
 *      + if the ObSharedGuard observed by ObWeakGuard is created by ob_make_shared() or 
 *        ob_alloc_shared(), the protected object will be logical deleted(call it's destruction) 
 *        when the last ObSharedGuard destructed, but the protected object will be physical 
 *        deleted(free it's memory) when the last ObWeakGuard destructed.
 *
 *  - Contact  for help.
 */

#ifndef OCEANBASE_LIB_GUARD_OB_WEAK_GUARD_H
#define OCEANBASE_LIB_GUARD_OB_WEAK_GUARD_H

#include "ob_shared_guard.h"

namespace oceanbase
{
namespace unittest
{
class TestObGuard;
}
namespace common
{

template <typename T>
class ObWeakGuard {
  friend class unittest::TestObGuard;
public:
  ObWeakGuard() = default;

  ObWeakGuard(const ObWeakGuard<T> &rhs) : ObWeakGuard(rhs.block_ptr_) {}

  ObWeakGuard(const ObSharedGuard<T> &rhs) : ObWeakGuard(rhs.block_ptr_) {}

  ~ObWeakGuard() { reset(); }

  ObWeakGuard<T> &operator=(const ObWeakGuard<T> &rhs) { (void) assign(rhs); return *this; }

  int assign(const ObWeakGuard<T> &rhs) { return assign_(rhs.block_ptr_); }

  int assign(const ObSharedGuard<T> &rhs) { return assign_(rhs.block_ptr_); }

  int64_t get_weak_count() const { return ATOMIC_LOAD(&(block_ptr_.control_ptr_->weak_count_)); }
  
  int64_t get_shared_count() const { return ATOMIC_LOAD(&(block_ptr_.control_ptr_->shared_count_)); }

  bool is_valid() const { return block_ptr_.is_valid(); }

  void reset() {
    if (OB_LIKELY(block_ptr_.is_valid())) {
      if (OB_UNLIKELY(block_ptr_.control_ptr_->dec_weak() == 0)) {
        block_ptr_.control_ptr_->~ControlBlock();// deleter will be logical deleted
        block_ptr_.control_ptr_->allocator_.free(block_ptr_.control_ptr_->alloc_area_pointer_);
      }
    }
    block_ptr_.reset();
  }

  bool upgradeable() const {
    return block_ptr_.is_valid() && 
           ATOMIC_LOAD(&(block_ptr_.control_ptr_->shared_count_)) != 0;
  }

  ObSharedGuard<T> upgrade() const {
    if (OB_LIKELY(block_ptr_.is_valid() && (block_ptr_.control_ptr_->lock() != 0))) {
      return guard::ObSharedGuardFriend<T>::create_shared_from_block_ptr(block_ptr_);
    } else {
      return ObSharedGuard<T>();
    }
  }

  TO_STRING_KV(KP(this), KP_(block_ptr_.control_ptr), KP_(block_ptr_.data_ptr));

private:
  explicit ObWeakGuard(const guard::BlockPtr<T> &block_ptr) : block_ptr_(block_ptr) {
    if (OB_LIKELY(block_ptr_.is_valid())) {
      block_ptr_.control_ptr_->inc_weak();
    } else {
      block_ptr_.reset();
    }
  }

  int assign_(const guard::BlockPtr<T> &block_ptr) {
    reset();
    if (OB_LIKELY(block_ptr.is_valid())) {
      block_ptr.control_ptr_->inc_weak();
      block_ptr_.control_ptr_ = block_ptr.control_ptr_;
      block_ptr_.data_ptr_ = block_ptr.data_ptr_;
    }
    return OB_SUCCESS;
  }

private:
  guard::BlockPtr<T> block_ptr_;
};

}// namespace oceanbase
}// namespace common
#endif
