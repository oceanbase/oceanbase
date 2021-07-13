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

#ifndef TBSYS_LOCK_H
#define TBSYS_LOCK_H
#include <assert.h>
namespace tbutil {
/**
 * @brief LockT is a simple template, composed of constructor and destructor
 * Constructor calls lock for its parameters, destructor calls unlock
 * By instantiating a local variable of type Lock, the deadlock problem can be completely solved
 */
template <typename T>
class LockT {
public:
  explicit LockT(const T& mutex) : _mutex(mutex)
  {
    _mutex.lock();
    _acquired = true;
  }

  ~LockT()
  {
    if (_acquired) {
      _mutex.unlock();
    }
  }

  void acquire() const
  {
    if (_acquired) {
#ifdef _NO_EXCEPTION
      assert(!"ThreadLockedException");
#else
      throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    _mutex.lock();
    _acquired = true;
  }

  bool tryAcquire() const
  {
    if (_acquired) {
#ifdef _NO_EXCEPTION
      assert(!"ThreadLockedException");
#else
      throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    _acquired = _mutex.tryLock();
    return _acquired;
  }

  void release() const
  {
    if (!_acquired) {
#ifdef _NO_EXCEPTION
      assert(!"ThreadLockedException");
#else
      throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    _mutex.unlock();
    _acquired = false;
  }

  bool acquired() const
  {
    return _acquired;
  }

protected:
  LockT(const T& mutex, bool) : _mutex(mutex)
  {
    _acquired = _mutex.tryLock();
  }

private:
  LockT(const LockT&);
  LockT& operator=(const LockT&);

  const T& _mutex;
  mutable bool _acquired;

  friend class Cond;
};

/**
 * @brief TryLockT is a simple template, composed of constructor and destructor
 * Constructor calls lock for its parameters, destructor calls unlock
 * By instantiating a local variable of type TryLock, the deadlock problem can be completely solved
 */
template <typename T>
class TryLockT : public LockT<T> {
public:
  TryLockT(const T& mutex) : LockT<T>(mutex, true)
  {}
};
}  // namespace tbutil

#endif
