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

#ifndef OCEANBASE_LIB_OB_QUEUE_
#define OCEANBASE_LIB_OB_QUEUE_

#include <stdlib.h>     // NULL
#include <errno.h>
#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{
struct ObLink
{
  ObLink() : next_(NULL) {}
  ~ObLink() { next_ = NULL; }

  ObLink *next_;

  void reset() { next_ = NULL; }
  bool is_deleted() const { return ((uint64_t)next_) & 1; }
};

inline bool is_last_bit_set(uint64_t x)
{
  return x & 1;
}

inline uint64_t clear_last_bit(uint64_t x)
{
  return x & ~1;
}

// try until last_bit is set; return value before CAS
inline uint64_t set_last_bit(uint64_t *addr)
{
  uint64_t ov = 0;
  uint64_t nv  = ATOMIC_LOAD(addr); //newest value before CAS
  while (0 == ((ov = nv) & 1)
         && ov != (nv = ATOMIC_VCAS(addr, ov, ov | 1))) {
    // do nothing
  }
  return nv;
}

inline void unset_last_bit(uint64_t *addr)
{
  ATOMIC_STORE(addr, clear_last_bit(ATOMIC_LOAD(addr)));
}

inline ObLink *link_next(ObLink *cur)
{
  return (ObLink *)clear_last_bit((uint64_t)ATOMIC_LOAD(&cur->next_));
}

// return prev->next_ before CAS
// success is return == next
inline ObLink *link_insert(ObLink *prev, ObLink *target, ObLink *next)
{
  target->next_ = next;
  return ATOMIC_VCAS(&prev->next_, next, target);
}

// return prev->next_ before CAS
// success is return == target
inline ObLink *link_del(ObLink *prev, ObLink *target, ObLink *&next)
{
  ObLink *ret = NULL;
  if (!is_last_bit_set((uint64_t)(next = (ObLink *)set_last_bit((uint64_t *)(&target->next_))))) {
    if (target != (ret = ATOMIC_VCAS(&prev->next_, target, next))) {
      unset_last_bit((uint64_t *)(&target->next_));
    }
  }
  return ret;
}

// return first node that >= key
// make sure start > key
template<typename T>
T *ol_search(T *start, T *key, T *&prev)
{
  T *next = NULL;
  prev = start;
  while (NULL != (next = (T *)link_next(prev))
         && next->compare(key) < 0) {
    prev = next;
  }
  return next;
}

template<typename T, typename Comparator>
T *ol_search(T *start, Comparator &cmp, T *&prev)
{
  T *next = NULL;
  prev = start;
  while (NULL != (next = (T *)link_next(prev))
         && cmp(next) < 0) {
    prev = next;
  }
  return next;
}

// return first node that > key
// make sure start > key
template<typename T>
T *ol_search_next(T *start, T *key, T *&prev)
{
  T *next = NULL;
  prev = start;
  while (NULL != (next = (T *)link_next(prev))
         && next->compare(key) <= 0) {
    prev = next;
  }
  return next;
}

template<typename T>
int ol_get(T *start, T *key, T *&target)
{
  int err = 0;
  T *prev = NULL;
  target = ol_search(start, key, prev);
  if (NULL == target || 0 != target->compare(key)) {
    err = -ENOENT;
  }
  return err;
}

template<typename T, typename Comparator>
int ol_get(T *start, Comparator &cmp, T *&target)
{
  int err = 0;
  T *prev = NULL;
  target = ol_search(start, cmp, prev);
  if (NULL == target || 0 != cmp(target)) {
    err = -ENOENT;
  }
  return err;
}

template<typename T>
    int _ol_insert(T* start, T* target)
{
  int err = 0;
  T* prev = NULL;
  T* next = ol_search(start, target, prev);
  if (NULL != next && 0 == next->compare(target)) {
    err = -EEXIST;
  } else if (next != link_insert(prev, target, next)) {
    err = -EAGAIN;
  }
  return err;
}

template<typename T>
int _ol_del(T* start, T* key, T*& target)
{
  int err = 0;
  T* prev = NULL;
  T* next = NULL;
  target = ol_search(start, key, prev);
  if (NULL == target || 0 != target->compare(key)) {
    err = -ENOENT;
  } else if (target == link_del(prev, target, (ObLink*&)next)) {
    err = 0;
  } else if (is_last_bit_set((uint64_t)next)) {
    err = -ENOENT;
  } else {
    err = -EAGAIN;
  }
  return err;
}

template<typename T, typename Comparator>
int _ol_del(T* start, Comparator &cmp, T*& target)
{
  int err = 0;
  T* prev = NULL;
  T* next = NULL;
  target = ol_search(start, cmp, prev);
  if (NULL == target || 0 != cmp(target)) {
    err = -ENOENT;
  } else if (target == link_del(prev, target, (ObLink*&)next)) {
    err = 0;
  } else if (is_last_bit_set((uint64_t)next)) {
    err = -ENOENT;
  } else {
    err = -EAGAIN;
  }
  return err;
}

// make sure start is valid all time
template<typename T>
int ol_insert(T* start, T* target)
{
  int err = 0;
  while(-EAGAIN == (err = _ol_insert(start, target)))
    ;
  return err;
}

// make sure start is valid all time
template<typename T>
int ol_del(T* start, T* key, T*& target)
{
  int err = 0;
  while(-EAGAIN == (err = _ol_del(start, key, target)))
    ;
  return err;
}

template<typename T, typename Comparator>
int ol_del(T* start, Comparator &cmp, T*& target)
{
  int err = 0;
  while(-EAGAIN == (err = _ol_del(start, cmp, target)))
    ;
  return err;
}

struct ObDLink: public ObLink
{
  ObDLink(): ObLink(), prev_(NULL) {}
  ~ObDLink() {}
  void reset()
  {
    ObLink::reset();
    prev_ = NULL;
  }
  ObDLink* prev_;
};

// correct target->prev_ link
inline void try_correct_prev_link(ObDLink* target, ObDLink* prev)
{
  if (NULL != target) {
    while(true) {
      // all threads DO FAS to make sure that one operation will correct the target->prev_ after all operation stop.
      ObDLink* old_prev = ATOMIC_TAS(&target->prev_, prev);
      if (ATOMIC_LOAD(&prev->next_) == target) {
        // make sure that the last operation can return.
        break;
      } else if (ATOMIC_LOAD(&old_prev->next_) != target) {
        // not this operation cover the last operation
        break;
      } else {
        // make sure that none operation cound cover the last operation.
        prev = old_prev;
      }
    }
  }
}

// make sure prev is valid all time
// must succed
inline int dl_insert(ObDLink* prev, ObDLink* target)
{
  int err = 0;
  target->prev_ = prev;
  while(true) {
    ObLink* next = link_next(prev);
    if (next == link_insert(prev, target, next)) {
      try_correct_prev_link((ObDLink*)next, target);
      break;
    }
  }
  return err;
}

// make sure next is valid all time
// must succed
inline int dl_insert_before(ObDLink* next, ObDLink* target)
{
  int err = 0;
  while(true) {
    ObDLink* prev = ATOMIC_LOAD(&next->prev_);
    target->prev_ = prev;
    if (next == link_insert(prev, target, next)) {
      try_correct_prev_link((ObDLink*)next, target);
      break;
    }
  }
  return err;
}

inline ObDLink* search_direct_prev(ObDLink* prev, ObDLink* target)
{
  ObDLink* next = NULL;
  while((next = (ObDLink*)link_next(prev)) != target) {
    prev = next;
  }
  return prev;
}

// -ENOENT: target is deleted by other thread
inline int dl_del(ObDLink* target)
{
  int err = 0;
  while(0 == err) {
    ObDLink* prev = search_direct_prev(ATOMIC_LOAD(&target->prev_), target);
    ObDLink* next = NULL;
    if (target == link_del(prev, target, (ObLink*&)next)) {
      try_correct_prev_link(next, prev);
      break;
    } else if (is_last_bit_set((uint64_t)next)) {
      err = -ENOENT;
    }
  }
  return err;
}
} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_LIB_OB_QUEUE_
