/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_LOCK_H
#define OB_LOCK_H
#include <assert.h>
namespace obutil
{
template <typename T>
class ObLockT
{
public:

  explicit ObLockT(const T& mutex) :
    mutex_(mutex)
  {
    mutex_.lock();
    acquired_ = true;
  }

  ~ObLockT()
  {
    if (acquired_)
    {
      mutex_.unlock();
    }
  }

  void acquire() const
  {
    if (acquired_)
    {
#ifdef _NO_EXCEPTION
       assert(!"ThreadLockedException");
#else
       throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    mutex_.lock();
    acquired_ = true;
  }


  bool try_acquire() const
  {
    if (acquired_)
    {
#ifdef _NO_EXCEPTION
      assert(!"ThreadLockedException");
#else
      throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    acquired_ = mutex_.trylock();
    return acquired_;
  }

  void release() const
  {
    if (!acquired_)
    {
#ifdef _NO_EXCEPTION
      assert(!"ThreadLockedException");
#else
      throw ThreadLockedException(__FILE__, __LINE__);
#endif
    }
    mutex_.unlock();
    acquired_ = false;
  }

  bool acquired() const
  {
    return acquired_;
  }

protected:

  ObLockT(const T& mutex, bool) :
    mutex_(mutex)
  {
    acquired_ = mutex_.trylock();
  }

private:

  ObLockT(const ObLockT&);
  ObLockT& operator=(const ObLockT&);

  const T& mutex_;
  mutable bool acquired_;

  friend class Cond;
};

template <typename T>
class ObTryLockT : public ObLockT<T>
{
public:

  ObTryLockT(const T& mutex) :
    ObLockT<T>(mutex, true)
  {}
};
}

#endif
