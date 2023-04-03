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

#ifndef OB_LIB_ATOMIC_LIST_H_
#define OB_LIB_ATOMIC_LIST_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

//#define OB_HAS_128BIT_CAS 1
#define DOUBLE_FREE_CHECK 1

// Generic atomic list Implementation (for pointer data types only). Uses
// atomic memory operations to avoid blocking.
// Warning: it only supports x86_64, doesn't support i386

// For information on the structure of the x86_64 memory map:
//
// http://en.wikipedia.org/wiki/X86-64#Linux
//
// Essentially, in the current 48-bit implementations, the top bit as well as
// the  lower 47 bits are used, leaving the upper-but one 16 bits free to be
// used for the version. We will use the top-but-one 15 and sign extend when
// generating the pointer was required by the standard.

#define QUEUE_LD64(dst,src) \
  *(reinterpret_cast<volatile uint64_t *>(&((dst).data_))) = *(reinterpret_cast<volatile uint64_t *>(&((src).data_)))

#if OB_HAS_128BIT_CAS
#define QUEUE_LD(dst, src) \
do { \
  *reinterpret_cast<volatile  __int128_t *>(&(dst)) = \
    __sync_val_compare_and_swap(reinterpret_cast<volatile __int128_t *>(&(src)), 0, 0); \
} while (0)
#else
#define QUEUE_LD(dst,src) QUEUE_LD64(dst,src)
#endif

// Warning: ObHeadNode is read and written in multiple threads without
// a lock, use QUEUE_LD to read safely.
union ObHeadNode
{
  ObHeadNode() : data_(0) { }
  ~ObHeadNode() { }

#if OB_HAS_128BIT_CAS
  struct
  {
    void *pointer_;
    int64_t version_;
  } s_;
  __int128_t data_;
#else
  int64_t data_;
#endif
};

// Why is version required? One scenario is described below
// Think of a list like this -> A -> C -> D
// and you are popping from the list
// Between the time you take the ptr(A) and swap the head pointer
// the list could start looking like this
// -> A -> B -> C -> D
// If the version check is not there, the list will look like
// -> C -> D after the pop, which will result in the loss of "B"

#ifdef DOUBLE_FREE_CHECK
#define FROM_PTR(x) reinterpret_cast<void *>(((uintptr_t)(x)) + 1)
#define TO_PTR(x) reinterpret_cast<void *>(((uintptr_t)(x)) - 1)
#else
#define FROM_PTR(x) (reinterpret_cast<void *>(x))
#define TO_PTR(x) (reinterpret_cast<void *>(x))
#endif

#if OB_HAS_128BIT_CAS
#define FREELIST_POINTER(x) (x).s_.pointer_
#define FREELIST_VERSION(x) (x).s_.version_
#define SET_FREELIST_POINTER_VERSION(x,p,v) \
  (x).s_.pointer_ = p; (x).s_.version_ = v
#elif defined(__x86_64__) || defined(__ia64__)
#define FREELIST_POINTER(x) (reinterpret_cast<void *>((((static_cast<intptr_t>((x).data_)) <<16) >> 16) | \
 (((~(((static_cast<intptr_t>((x).data_)) << 16 >> 63) - 1)) >> 48) << 48)))  // sign extend
#define FREELIST_VERSION(x) ((static_cast<intptr_t>((x).data_)) >> 48)
#define SET_FREELIST_POINTER_VERSION(x,p,v) \
  (x).data_ = (((reinterpret_cast<intptr_t>(p))&0x0000FFFFFFFFFFFFULL) | (((v)&0xFFFFULL) << 48))
#elif defined(__aarch64__)
#define FREELIST_POINTER(x) (reinterpret_cast<void *>(((((static_cast<intptr_t>((x).data_)) <<16) >> 16) | \
  (((~(((static_cast<intptr_t>((x).data_)) << 16 >> 63) - 1)) >> 48) << 48))&0x0000FFFFFFFFFFFFULL))
#define FREELIST_VERSION(x) ((static_cast<intptr_t>((x).data_)) >> 48)
#define SET_FREELIST_POINTER_VERSION(x,p,v) \
  (x).data_ = (((reinterpret_cast<intptr_t>(p))&0x0000FFFFFFFFFFFFULL) | (((v)&0xFFFFULL) << 48))
#else
#error "unsupported processor"
#endif

#define ATOMICLIST_EMPTY(x) (NULL == (TO_PTR(FREELIST_POINTER((x.head_)))))
#define ADDRESS_OF_NEXT(x, offset) (reinterpret_cast<void **>(reinterpret_cast<char *>(x) + offset))

struct ObAtomicList
{
  ObAtomicList()
  {
    memset(this, 0, sizeof(ObAtomicList));
  }
  ~ObAtomicList() { }

  int init(const char *name, const int64_t offset_to_next);
  void *push(void *item);
  void *batch_push(void *head_item, void *tail_item);
  void *pop();
  void *popall();

  void *head() { return TO_PTR(FREELIST_POINTER(head_)); }
  void *next(void *item) { return TO_PTR(*ADDRESS_OF_NEXT(item, offset_)); }
  bool empty() { return (NULL == head()); }

  // WARNING: only if only one thread is doing pops it is possible to have a
  // "remove" which only that thread can use as well.
  void *remove(void *item);

  volatile ObHeadNode head_;
  const char *name_;
  int64_t offset_;
};

inline int ObAtomicList::init(const char *name, const int64_t offset_to_next)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name) || OB_UNLIKELY(offset_to_next < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    name_ = name;
    offset_ = offset_to_next;
    SET_FREELIST_POINTER_VERSION(head_, FROM_PTR(0), 0);
  }
  return ret;
}

typedef volatile void *volatile_void_p;
inline void *ObAtomicList::push(void *item)
{
  volatile_void_p *adr_of_next = (volatile_void_p *)(ADDRESS_OF_NEXT(item, offset_));
  ObHeadNode head;
  ObHeadNode item_pair;
  bool result = false;
  volatile void *h = NULL;

  do {
    QUEUE_LD(head, head_);
    h = FREELIST_POINTER(head);
    *adr_of_next = h;

#ifdef DOUBLE_FREE_CHECK
    if (TO_PTR(h) == item) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list push: trying to free item twice");
    }
    if (((reinterpret_cast<uint64_t>(TO_PTR(h))) & 3) != 0) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list push: bad list");
    }
#endif

    SET_FREELIST_POINTER_VERSION(item_pair, FROM_PTR(item), FREELIST_VERSION(head));

#if OB_HAS_128BIT_CAS
    result = ATOMIC_BCAS(&head_.data_, head.data_, item_pair.data_);
#else
    result = ATOMIC_BCAS(&head_.data_, head.data_, item_pair.data_);
#endif
  } while (!result);

  return TO_PTR((void *)(h));
}

inline void *ObAtomicList::batch_push(void *head_item, void *tail_item)
{
  volatile_void_p *adr_of_next = (volatile_void_p *)(ADDRESS_OF_NEXT(tail_item, offset_));
  ObHeadNode head;
  ObHeadNode item_pair;
  bool result = false;
  volatile void *h = NULL;

  do {
    QUEUE_LD(head, head_);
    h = FREELIST_POINTER(head);
    *adr_of_next = h;

#ifdef DOUBLE_FREE_CHECK
    if (TO_PTR(h) == tail_item) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list push: trying to free item twice");
    }
    if ((reinterpret_cast<uint64_t>(TO_PTR(h))) & 3) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list push: bad list");
    }
#endif

    SET_FREELIST_POINTER_VERSION(item_pair, FROM_PTR(head_item), FREELIST_VERSION(head));

#if OB_HAS_128BIT_CAS
    result = ATOMIC_BCAS(&head_.data_, head.data_, item_pair.data_);
#else
    result = ATOMIC_BCAS(&head_.data_, head.data_, item_pair.data_);
#endif
  } while (!result);

  return TO_PTR((void *)(h));
}

inline void *ObAtomicList::pop()
{
  ObHeadNode item;
  ObHeadNode next;
  bool result = false;
  bool finish = false;
  void *ret = NULL;

  do {
    QUEUE_LD(item, head_);
    if (OB_ISNULL(TO_PTR(FREELIST_POINTER(item)))) {
      finish = true;
    } else {
      SET_FREELIST_POINTER_VERSION(next, *ADDRESS_OF_NEXT(TO_PTR(FREELIST_POINTER(item)), offset_),
                                   FREELIST_VERSION(item) + 1);
#if OB_HAS_128BIT_CAS
      result = ATOMIC_BCAS(&head_.data_, item.data_, next.data_);
#else
      result = ATOMIC_BCAS(&head_.data_, item.data_, next.data_);
#endif

#ifdef DOUBLE_FREE_CHECK
      if (result) {
        if (FREELIST_POINTER(item) == TO_PTR(FREELIST_POINTER(next))) {
          OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list pop: loop detected");
        }
        if (((reinterpret_cast<uint64_t>(TO_PTR(FREELIST_POINTER(next)))) & 3) != 0) {
          OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "atomic list pop: bad list");
        }
      }
#endif
    }
  } while (!result && !finish);

  if (result) {
    ret = TO_PTR(FREELIST_POINTER(item));
    *ADDRESS_OF_NEXT(ret, offset_) = NULL;
  }

  return ret;
}

inline void *ObAtomicList::popall()
{
  ObHeadNode item;
  ObHeadNode next;
  bool result = false;
  bool finish = false;
  void *ret = NULL;

  do {
    QUEUE_LD(item, head_);
    if (OB_ISNULL(TO_PTR(FREELIST_POINTER(item)))) {
      finish = true;
    } else {
      SET_FREELIST_POINTER_VERSION(next, FROM_PTR(NULL), FREELIST_VERSION(item) + 1);
#if OB_HAS_128BIT_CAS
      result = ATOMIC_BCAS(&head_.data_, item.data_, next.data_);
#else
      result = ATOMIC_BCAS(&head_.data_, item.data_, next.data_);
#endif
    }
  } while (!result && !finish);

  if (result) {
    ret = TO_PTR(FREELIST_POINTER(item));
    void *element = ret;
    void *next = NULL;
    // fixup forward pointers
    while (NULL != element) {
      next = TO_PTR(*ADDRESS_OF_NEXT(element, offset_));
      *ADDRESS_OF_NEXT(element, offset_) = next;
      element = next;
    }
  }

  return ret;
}

// WARNING: only if only one thread is doing pops it is possible to have a
// "remove" which only that thread can use as well.
inline void *ObAtomicList::remove(void *item)
{
  ObHeadNode head;
  void *prev = NULL;
  void **addr_next = ADDRESS_OF_NEXT(item, offset_);
  void *item_next = *addr_next;
  bool result = false;
  bool finish = false;
  void *ret = NULL;
  ObHeadNode next;

  // first, try to pop it if it is first
  QUEUE_LD(head, head_);
  while (TO_PTR(FREELIST_POINTER(head)) == item && !finish) {
    SET_FREELIST_POINTER_VERSION(next, item_next, FREELIST_VERSION(head) + 1);
#if OB_HAS_128BIT_CAS
    result = ATOMIC_BCAS(&head_.data_, head.data_, next.data_);
#else
    result = ATOMIC_BCAS(&head_.data_, head.data_, next.data_);
#endif

    if (result) {
      *addr_next = NULL;
      ret = item;
      finish = true;
    }
    QUEUE_LD(head, head_);
  }

  // then, go down the list, trying to remove it
  if (!result) {
    void **prev_adr_of_next = NULL;
    finish = false;
    prev = TO_PTR(FREELIST_POINTER(head));
    while (NULL != prev && !finish) {
      prev_adr_of_next = ADDRESS_OF_NEXT(prev, offset_);
      prev = TO_PTR(*prev_adr_of_next);
      if (prev == item) {
        *prev_adr_of_next = item_next;
        *addr_next = NULL;
        ret = item;
        finish = true;
      }
    }
  }

  return ret;
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_ATOMIC_LIST_H_
