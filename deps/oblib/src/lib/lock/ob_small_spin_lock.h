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

#ifndef OCEANBASE_LIB_LOCK_OB_SMALL_LOCK_
#define OCEANBASE_LIB_LOCK_OB_SMALL_LOCK_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{

// Small spin lock.
// A small spin lock that uses only 1 bit.
//
// Usage I: To build a lock from nothing.
//   typedef ObSmallSpinLock<uint32_t, 0> SmallLock;
//   SmallLock lock;
//   lock.lock();
//   lock.set_data(0x02);
//   uint32_t val = lock.get_data();
//   lock.unlock();
//
// Recommended:
// ObByteLock is a specialization of small spin lock that uses only a byte.
//   class ObByteLock;
//   class ObByteLockGuard;
//
// Usage II: Use 1 bit in an integer as a lock.
//   typedef ObSmallSpinLock<uint32_t, 0> FlagLock;
//   uint32_t flag = 0x00;
//   FlagLock &lock = FlagLock::AsLock(flag);
//   lock.init(0x02); // Init before use.
//   lock.lock();
//   lock.set_data(0x04); // Use this setter & getter to access remaining bits.
//   uint32_t val = lock.get_data();
//   lock.unlock();
//
// Usage III: Use the last bit of a pointer as the lock.
// It works since a pointer is in fact an even integer.
//    typedef ObPtrSpinLock<char> PtrLock;
//    PtrLock ptr_lock;
//    ptr_lock.init();
//    ptr_lock.lock();
//    ptr_lock.set_ptr("CharAddr");
//    const char *addr = ptr_lock.get_ptr();
//    ptr_lock.unlock();
//

template <typename LockType> class ObSmallSpinLockGuard;

template <typename IntType,
          int64_t LockBit = 0,
          int64_t MaxSpin = 500,
          int64_t USleep = 100>
class ObSmallSpinLock
{
public:
  typedef ObSmallSpinLock<IntType, LockBit, MaxSpin, USleep> MyType;
  typedef ObSmallSpinLockGuard<MyType> Guard;
  typedef ObSmallSpinLockGuard<MyType> guard; // Adapt to old code.
public:
  // Constructor.
  // Usage I: To build a lock from nothing.
  // Non-lock bits are init to default value.
  ObSmallSpinLock() : lock_() { init(); }
  // No virtual so no virtual function table.
  ~ObSmallSpinLock() { }

  // Converter.
// Usage II: Use 1 bit in an integer as a lock.
  // Remember to init() it before use.
  static MyType& AsLock(IntType &val) { return reinterpret_cast<MyType&>(val); }
public:
  // Init.
  // Init lock bit. Other bits of IntType unchanged.
  void init()
  {
    lock_ = static_cast<IntType>(lock_ & (~LOCK_MASK));
  }

  // Init with value.
  // Init lock bit. Other bits equal to initial value.
  void init(const IntType &val)
  {
    init();
    set_data(val);
  }

  // Try lock.
  // Test lock for only once. Return true on success.
  bool try_lock()
  {
    bool ret = false;
    IntType lock_val = ATOMIC_LOAD(&lock_);
    if (0 == (lock_val & LOCK_MASK)) {
      ret = ATOMIC_BCAS(&lock_, lock_val, (lock_val | LOCK_MASK));
    }
    return ret;
  }

  // Lock.
  // Keep spinning till success.
  void lock()
  {
    int64_t cnt = 0;
    while (false == try_lock()) {
      // Sleep when it exceeds spin limit.
      if (MaxSpin <= (cnt++)) {
        cnt = 0;
        ::usleep(USleep);
      }
      PAUSE();
    }
  }

  // Unlock.
  void unlock()
  {
    IntType lock_val = ATOMIC_LOAD(&lock_);
    int ret = ATOMIC_SET(&lock_, (lock_val & (~LOCK_MASK)));
    UNUSED(ret);
  }

  // Test lock.
  bool is_locked() const
  {
    IntType lock_val = ATOMIC_LOAD(&lock_);
    return (0 != (lock_val & LOCK_MASK));
  }

  // Get non-lock bits of this lock. Lock bit is returned as 0.
  IntType get_data() const
  {
    return static_cast<IntType>(lock_ & (~LOCK_MASK));
  }

  // Set non-lock bits of this lock.
  void set_data(const IntType &val)
  {
    lock_ = static_cast<IntType>((val & (~LOCK_MASK)) | (lock_ & (LOCK_MASK)));
  }

private:
  static const IntType LOCK_MASK = static_cast<IntType>(1) << LockBit;
  IntType lock_;
};

// Lock guard.
template<typename LockType>
class ObSmallSpinLockGuard
{
public:
  explicit ObSmallSpinLockGuard(LockType &lock) : lock_(&lock) { lock_->lock(); }
  ~ObSmallSpinLockGuard() { lock_->unlock(); }
private:
  LockType *lock_;
  DISALLOW_COPY_AND_ASSIGN(ObSmallSpinLockGuard);
};

// Byte lock.
typedef ObSmallSpinLock<uint8_t, 0> ObByteLock; // 1 byte lock.
typedef ObSmallSpinLockGuard<ObByteLock> ObByteLockGuard;

// Ptr spin lock.
// A wrapper of small spin lock that uses the last bit of a pointer as lock.
template <typename T>
struct ObPtrSpinLock
{
  // Use last bit in T*.
  typedef uint64_t ValType;
  ObSmallSpinLock<ValType, 0> lock_;

  // Init.
  // Init to NULL ptr.
  void init()
  {
    lock_.init();
    lock_.set_data(0); // 0 as NULL.
  }

  bool try_lock()
  {
    return lock_.try_lock();
  }

  void lock()
  {
    lock_.lock();
  }

  void unlock()
  {
    lock_.unlock();
  }

  // Get ptr.
  T* get_ptr()
  {
    ValType val = lock_.get_data();
    return reinterpret_cast<T*>(val);
  }

  // Set ptr.
  // Ptr can't be an 'odd' pointer.
  void set_ptr(const T *ptr)
  {
    ValType val = reinterpret_cast<ValType>(ptr);
    lock_.set_data(val);
  }
};

// Lock guard.
template <typename LockType>
class ObPtrSpinLockGuard
{
public:
  explicit ObPtrSpinLockGuard(LockType &lock) : lock_(&lock) { lock_->lock(); }
  ~ObPtrSpinLockGuard() { lock_->unlock(); }
private:
  LockType *lock_;
  DISALLOW_COPY_AND_ASSIGN(ObPtrSpinLockGuard);
};

}
}

#endif // OCEANBASE_LIB_LOCK_OB_SMALL_LOCK_
