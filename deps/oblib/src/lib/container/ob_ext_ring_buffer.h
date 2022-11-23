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

#ifndef OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_H_
#define OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_H_

#include "lib/container/ob_ext_ring_buffer_impl.h"

namespace oceanbase
{
namespace common
{

// ---- ObExtendibleRingBuffer<T> ----
//
// ObExtendibleRingBuffer<T> is a ring buffer that has infinite capacity.
// You think of it as an infinite size array that stores T*, on which you
// are allowed to set a T* at any place between [begin_sn, +...), or pop
// out the T* at begin_sn on a user specified condition, or get any T* in
// range [begin_sn, end_sn).
//
// begin_sn set by user during initialization points to the first T*
// in this array, end_sn points to the element next to the last T* in this
// array. After user sets a T* at [sn], the uninitialized T*s between
// [begin_sn, sn) are initialized to default value of T*, which is NULL.
//
// Interface:
//   set(serial_no, T*) : err
//   get(serial_no, T*&) : err
//   pop(condition, T*&, popped) : err
//
// The main purpose of this container is to store objects with certain order
// who arrives randomly and leaves in its order. It uses the concept of
// ring buffer but breaks the limit in its size.
//
// All ringbuffers share one hazard pointer, so donnot access another ringbuffer
// in functors passed into ringbuffer.


template <typename T, typename RefT = erb::DummyRef<T>>
class ObExtendibleRingBuffer : protected erb::ObExtendibleRingBufferBase<T*, erb::PtrSlot<T>, RefT >
{
public:
  ObExtendibleRingBuffer();
  virtual ~ObExtendibleRingBuffer();
public:
  int init(const int64_t begin_sn,
      const int64_t seg_size = erb::ObExtendibleRingBufferBase<T*, erb::PtrSlot<T> >::DEFAULT_SEG_SIZE);
  int destroy();
  bool is_inited() const;
public:
  // Get T* at sn.
  // sn should be in [begin_sn, end_sn)
  //
  // err:
  //   OB_SUCCESS: done
  //   OB_ERR_OUT_OF_LOWER_BOUND: sn < begin_sn
  //   OB_ERR_OUT_OF_UPPER_BOUND: end_sn <= sn
  //   else: includes out of memory, all fatal.
  int get(const int64_t sn, T *&ptr) const;

  // Set or replace T* at sn.
  // sn should be >= begin_sn.
  // ptr can be NULL.
  //
  // err:
  //   OB_SUCCESS: done
  //   OB_ERROR_OUT_OF_RANGE: sn < begin_sn
  //   else: includes out of memory, all fatal.
  int set(const int64_t sn, T* const &ptr);

  // Conditional set or replace T* at sn.
  // sn should be >= begin_sn.
  // ptr can be NULL.
  // T* is replaced on true return value.
  // CAUTION: T* might be NULL.
  //   struct CondOnTPtr
  //   {
  //     bool operator()(T *oldptr, T *newptr);
  //   }
  //
  // err:
  //   OB_SUCCESS with set == true: done
  //   OB_SUCCESS with set == false: cond returns false
  //   OB_ERROR_OUT_OF_RANGE: sn < begin_sn
  //   else: includes out of memory, all fatal.
  template <typename Func> int set(const int64_t sn, T* const &ptr, Func &cond, bool &set);

  // Pop T* at begin_sn.
  // The cond is a functor that decides to pop this T* or not.
  // T* is popped on true return value.
  //   struct CondOnTPtr
  //   {
  //     bool operator()(T *ptr);
  //   }
  //
  // err:
  //   OB_SUCCESS with popped == true: T* popped, begin_sn += 1
  //   OB_SUCCESS with popped == false: cond returns false on T*
  //   OB_ENTRY_NOT_EXIST: no element, begin_sn == end_sn
  //   else: includes out of memory, all fatal.
  template <typename Func> int pop(Func &cond, T *&ptr, bool &popped, bool use_lock = false);

  // Get begin_sn or end_sn.
  // Empty when begin_sn == end_sn.
  // Size = end_sn - begin_sn.
  int64_t begin_sn() const;
  int64_t end_sn() const;
private:
  erb::RingBufferAlloc alloc_;
private:
  typedef ObExtendibleRingBuffer<T, RefT> MyType;
  typedef erb::ObExtendibleRingBufferBase<T*, erb::PtrSlot<T>, RefT > BaseType;
  DISALLOW_COPY_AND_ASSIGN(ObExtendibleRingBuffer);
};



// Definitions.
template <typename T, typename RefT>
ObExtendibleRingBuffer<T, RefT>::ObExtendibleRingBuffer() :
  BaseType(),
  alloc_()
{ }

template <typename T, typename RefT>
ObExtendibleRingBuffer<T, RefT>::~ObExtendibleRingBuffer()
{ }

template <typename T, typename RefT>
int ObExtendibleRingBuffer<T, RefT>::init(const int64_t begin_sn, const int64_t seg_size)
{
  int ret = OB_SUCCESS;
  if (begin_sn < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "err begin sn", K(ret), K(begin_sn));
  } else if (OB_FAIL(BaseType::init(begin_sn, &alloc_, seg_size))) {
    CLOG_LOG(WARN, "err init", K(ret), K(begin_sn));
  }
  return ret;
}

template <typename T, typename RefT>
bool ObExtendibleRingBuffer<T, RefT>::is_inited() const
{
  return BaseType::is_inited();
}

template <typename T, typename RefT>
int ObExtendibleRingBuffer<T, RefT>::destroy()
{
  return BaseType::destroy();
}

template <typename T, typename RefT>
int ObExtendibleRingBuffer<T, RefT>::get(const int64_t sn, T*& ptr) const
{
  return BaseType::get(sn, ptr);
}

template <typename T, typename RefT>
int ObExtendibleRingBuffer<T, RefT>::set(const int64_t sn, T* const &ptr)
{
  return BaseType::set(sn, ptr);
}

template <typename T, typename RefT>
template <typename Func>
int ObExtendibleRingBuffer<T, RefT>::set(const int64_t sn, T* const& ptr, Func& cond, bool &set)
{
  return BaseType::set(sn, ptr, cond, set);
}

template <typename T, typename RefT>
template <typename Func>
int ObExtendibleRingBuffer<T, RefT>::pop(Func& cond, T*& ptr, bool& popped, bool use_lock)
{
  return BaseType::pop(cond, ptr, popped, use_lock);
}

template <typename T, typename RefT>
int64_t ObExtendibleRingBuffer<T, RefT>::begin_sn() const
{
  return BaseType::begin_sn();
}

template <typename T, typename RefT>
int64_t ObExtendibleRingBuffer<T, RefT>::end_sn() const
{
  return BaseType::end_sn();
}

}
}

#endif
