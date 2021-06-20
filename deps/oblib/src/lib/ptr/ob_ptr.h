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

#ifndef OB_LIB_PTR_H_
#define OB_LIB_PTR_H_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase {
namespace common {

class ObRefCount {
public:
  ObRefCount() : ref_count_(0)
  {}
  virtual ~ObRefCount()
  {}

  virtual void free() = 0;

  inline void inc_ref()
  {
    ATOMIC_FAA(&ref_count_, 1);
  }

  inline void dec_ref()
  {
    if (1 == ATOMIC_FAA(&ref_count_, -1)) {
      free();
    }
  }

  volatile int64_t ref_count_;
};

class ObForceVFPTToTop {
public:
  ObForceVFPTToTop()
  {}
  virtual ~ObForceVFPTToTop()
  {}
};

// class ObRefCountObj
// prototypical class for reference counting
class ObRefCountObj : public ObForceVFPTToTop {
public:
  ObRefCountObj() : refcount_(0)
  {}
  virtual ~ObRefCountObj()
  {}

  ObRefCountObj(const ObRefCountObj& s) : refcount_(0)
  {
    UNUSED(s);
  }

  ObRefCountObj& operator=(const ObRefCountObj& s)
  {
    UNUSED(s);
    return (*this);
  }

  int64_t refcount_inc();
  int64_t refcount_dec();
  int64_t refcount() const;

  virtual void free()
  {
    delete this;
  }

  volatile int64_t refcount_;
};

// Increment the reference count, returning the new count.
inline int64_t ObRefCountObj::refcount_inc()
{
  return ATOMIC_FAA(&refcount_, 1) + 1;
}

// Decrement the reference count, returning the new count.
inline int64_t ObRefCountObj::refcount_dec()
{
  return ATOMIC_FAA(&refcount_, -1) - 1;
}

inline int64_t ObRefCountObj::refcount() const
{
  return refcount_;
}

// class ObPtr
template <class T>
class ObPtr {
public:
  explicit ObPtr(T* p = NULL);
  ObPtr(const ObPtr<T>&);
  ~ObPtr();

  void release();
  ObPtr<T>& operator=(const ObPtr<T>&);
  ObPtr<T>& operator=(T*);

  operator T*() const
  {
    return (ptr_);
  }
  T* operator->() const
  {
    return (ptr_);
  }
  T& operator*() const
  {
    return (*ptr_);
  }
  bool operator==(const T* p)
  {
    return (ptr_ == p);
  }
  bool operator==(const ObPtr<T>& p)
  {
    return (ptr_ == p.ptr_);
  }
  bool operator!=(const T* p)
  {
    return (ptr_ != p);
  }
  bool operator!=(const ObPtr<T>& p)
  {
    return (ptr_ != p.ptr_);
  }
  ObRefCountObj* get_ptr()
  {
    return reinterpret_cast<ObRefCountObj*>(ptr_);
  }

  T* ptr_;
};

template <typename T>
ObPtr<T> make_ptr(T* p)
{
  return ObPtr<T>(p);
}

template <class T>
inline ObPtr<T>::ObPtr(T* ptr /* = NULL */) : ptr_(ptr)
{
  if (NULL != ptr_) {
    get_ptr()->refcount_inc();
  }
}

template <class T>
inline ObPtr<T>::ObPtr(const ObPtr<T>& src) : ptr_(src.ptr_)
{
  if (NULL != ptr_) {
    get_ptr()->refcount_inc();
  }
}

template <class T>
inline ObPtr<T>::~ObPtr()
{
  if ((NULL != ptr_) && 0 == get_ptr()->refcount_dec()) {
    get_ptr()->free();
  }
  return;
}

template <class T>
inline ObPtr<T>& ObPtr<T>::operator=(T* p)
{
  T* temp_ptr = ptr_;

  if (ptr_ != p) {
    ptr_ = p;

    if (NULL != ptr_) {
      get_ptr()->refcount_inc();
    }

    if ((NULL != temp_ptr) && 0 == (reinterpret_cast<ObRefCountObj*>(temp_ptr))->refcount_dec()) {
      (reinterpret_cast<ObRefCountObj*>(temp_ptr))->free();
    }
  }

  return (*this);
}

template <class T>
inline void ObPtr<T>::release()
{
  if (NULL != ptr_) {
    if (0 == (reinterpret_cast<ObRefCountObj*>(ptr_))->refcount_dec()) {
      (reinterpret_cast<ObRefCountObj*>(ptr_))->free();
    }
    ptr_ = NULL;
  }
}

template <class T>
inline ObPtr<T>& ObPtr<T>::operator=(const ObPtr<T>& src)
{
  return (operator=(src.ptr_));
}

}  // end of namespace common
}  // end of namespace oceanbase

#endif  // OB_LIB_PTR_H_
