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

#ifndef OB_LIB_INTRUSIVE_LIST_H_
#define OB_LIB_INTRUSIVE_LIST_H_

#include <stdint.h>
#include "lib/list/ob_atomic_list.h"

namespace oceanbase
{
namespace common
{

// Link cell for singly-linked list of objects of type T.
template <class T>
class SLink
{
 public:
  T *next_;
  SLink() : next_(NULL) {};
};
#define SLINK(c,f) class Link##_##f : public oceanbase::common::SLink<c> { public:    \
    static c *& next_link(c *e) { return e->f.next_; }             \
    static const c * next_link(const c *e) { return e->f.next_; }  \
  }; oceanbase::common::SLink<c> f
#define SLINKM(c,m,f) class Link##_##m##f : public oceanbase::common::SLink<c> { public: \
    static c *& next_link(c *e) { return e->m.f.next_; }              \
  };

// Link cell for doubly-linked list of objects of type T.
template <class T>
struct Link : public SLink<T>
{
  T *prev_;
  Link() : prev_(NULL) {}
};
#define LINK(c,f) class Link##_##f : public oceanbase::common::Link<c> { public:         \
    static c *& next_link(c *e) { return e->f.next_; }                \
    static c *& prev_link(c *e) { return e->f.prev_; }                \
    static const c * next_link(const c *e) { return e->f.next_; }     \
    static const c * prev_link(const c *e) { return e->f.prev_; }     \
  }; oceanbase::common::Link<c> f
#define LINKM(c,m,f) class Link##_##m##f : public oceanbase::common::Link<c> { public:  \
    static c *& next_link(c *e) { return e->m.f.next_; }             \
    static c *& prev_link(c *e) { return e->m.f.prev_; }             \
  };
#define LINK_FORWARD_DECLARATION(c,f) class Link##_##c##_##f : public oceanbase::common::Link<c> { public:     \
    static c *& next_link(c *e);                                      \
    static c *& prev_link(c *e);                                      \
  };
#define LINK_DEFINITION(c,f)  \
  inline c *& Link##_##c##_##f::next_link(c *e) { return e->f.next_; } \
  inline c *& Link##_##c##_##f::prev_link(c *e) { return e->f.prev_; } \

// List descriptor for singly-linked list of objects of type T.
template <class T, class L = typename T::Link_link_>
class SLL
{
 public:
  T *head_;
  bool empty() const { return head_ == NULL; }
  void push(T *e);
  T *pop();
  void reset() { head_ = NULL; }
  T *& next(T *e) { return L::next_link(e); }
  const T * next(const T *e) const { return L::next_link(e); }

  SLL() : head_(NULL) {}
  explicit SLL(T *c) : head_(c) {}
};
#define SList(c, f)  oceanbase::common::SLL<c, c::Link##_##f>
#define SListM(c, m, ml, l) oceanbase::common::SLL<c, c::Link##_##ml##l>
#define forl_LL(c, p, l) for (c *p = (l).head_; p; p = (l).next(p))

template <class T, class L>
inline void SLL<T,L>::push(T *e)
{
  next(e) = head_;
  head_ = e;
}

template <class T, class L>
inline T *SLL<T,L>::pop()
{
  T *ret = head_;
  if (NULL != ret) {
    head_ = next(ret);
    next(ret) = NULL;
  }
  return ret;
}

// List descriptor for doubly-linked list of objects of type T.
template <class T, class L = typename T::Link_link_>
struct DLL
{
  T *head_;
  bool empty() const { return head_ == NULL; }
  void push(T *e);
  T *pop();
  void remove(T *e);
  void insert(T *e, T *after);
  bool in(T *e) { return head_ == e || next(e) || prev(e); }
  void reset() { head_ = NULL; }
  static T *&next(T *e) { return reinterpret_cast<T*&>(L::next_link(e)); }
  static T *&prev(T *e) { return reinterpret_cast<T*&>(L::prev_link(e)); }
  static T const* next(const T *e) { return L::next_link(e); }
  static T const* prev(const T *e) { return L::prev_link(e); }

  DLL() : head_(NULL) {}
};
#define ObDLList(c, f)  oceanbase::common::DLL<c, c::Link##_##f>
#define ObDLListM(c, m, ml, l) oceanbase::common::DLL<c, c::Link##_##ml##l>

template <class T, class L>
inline void DLL<T,L>::push(T *e)
{
  if (NULL != head_) {
    prev(head_) = e;
  }
  next(e) = head_;
  head_ = e;
}

template <class T, class L>
inline void DLL<T,L>::remove(T *e)
{
  if (NULL != head_) {
    if (e == head_) {
      head_ = next(e);
    }
    if (NULL != prev(e)) {
      next(prev(e)) = next(e);
    }
    if (NULL != next(e)) {
      prev(next(e)) = prev(e);
    }
    prev(e) = NULL;
    next(e) = NULL;
  }
}

template <class T, class L>
inline T *DLL<T,L>::pop()
{
  T *ret = head_;
  if (NULL != ret) {
    head_ = next(ret);
    if (NULL != head_) {
      prev(head_) = NULL;
    }
    next(ret) = NULL;
  } else {
    ret = NULL;
  }
  return ret;
}

template <class T, class L>
inline void DLL<T,L>::insert(T *e, T *after)
{
  if (NULL == after) {
    push(e);
  } else {
    prev(e) = after;
    next(e) = next(after);
    next(after) = e;
    if (NULL != next(e)) {
      prev(next(e)) = e;
    }
  }
}

// List descriptor for queue of objects of type T.
template <class T, class L = typename T::Link_link_>
class Queue : public DLL<T,L>
{
public:
  using DLL<T,L>::head_;
  T *tail_;
  void push(T *e);
  T *pop();
  void enqueue(T *e);
  void in_or_enqueue(T *e);
  T *dequeue();
  void remove(T *e);
  void insert(T *e, T *after);
  void append(Queue<T,L> q);
  void append(DLL<T,L> q);
  void reset() { head_ = NULL; tail_ = NULL; }
  bool empty() const { return head_ == NULL; }

  Queue() : tail_(NULL) {}
};
#define Que(c, f) oceanbase::common::Queue<c, c::Link##_##f>
#define QueM(c, m, mf, f) oceanbase::common::Queue<c, c::Link##_##mf##f>

template <class T, class L>
inline void Queue<T,L>::push(T *e)
{
  DLL<T,L>::push(e);
  if (NULL == tail_) {
    tail_ = head_;
  }
}

template <class T, class L>
inline T *Queue<T,L>::pop()
{
  T *ret = DLL<T,L>::pop();
  if (NULL == head_) {
    tail_ = NULL;
  }
  return ret;
}

template <class T, class L>
inline void Queue<T, L>::insert(T *e, T *after)
{
  DLL<T, L>::insert(e, after);
  if (NULL == tail_) {
    tail_ = head_;
  } else if (tail_ == after) {
    tail_ = e;
  }
}

template <class T, class L>
inline void Queue<T, L>::remove(T *e)
{
  if (tail_ == e) {
    tail_ = (T*)this->prev(e);
  }
  DLL<T,L>::remove(e);
}

template <class T, class L>
inline void Queue<T, L>::append(DLL<T, L> q)
{
  T *qtail = q.head_;
  if (NULL != qtail) {
    while (NULL != this->next(qtail)) {
      qtail = this->next(qtail);
    }
  }
  if (NULL == head_) {
    head_ = q.head_;
    tail_ = qtail;
  } else {
    if (NULL != q.head_) {
      this->next(tail_) = q.head_;
      this->prev(q.head_) = tail_;
      tail_ = qtail;
    }
  }
}

template <class T, class L>
inline void Queue<T, L>::append(Queue<T, L> q)
{
  if (NULL == head_) {
    head_ = q.head_;
    tail_ = q.tail_;
  } else {
    if (NULL != q.head_) {
      this->next(tail_) = q.head_;
      this->prev(q.head_) = tail_;
      tail_ = q.tail_;
    }
  }
}

template <class T, class L>
inline void Queue<T, L>::enqueue(T *e)
{
  if (NULL != tail_) {
    insert(e, tail_);
  } else {
    push(e);
  }
}

template <class T, class L>
inline void Queue<T, L>::in_or_enqueue(T *e)
{
  if (!this->in(e)) {
    enqueue(e);
  }
}

template <class T, class L>
inline T *Queue<T, L>::dequeue()
{
  return pop();
}

// Adds sorting, but requires that elements implement <
template<class T, class L = typename T::Link_link_>
struct SortableQueue: public Queue<T, L>
{
  using DLL<T, L>::head_;
  using Queue<T, L>::tail_;
  void sort()
  {
    if (NULL != head_) {
      bool clean = false;
      while (!clean) {
        clean = true;
        T *v = head_;
        T *n = this->next(head_);
        while (n) {
          T *f = this->next(n);
          if (*n < *v) {
            clean = false;
            // swap 'em
            if (head_ == v) {
              head_ = n;
            }
            if (tail_ == n) {
              tail_ = v;
            }
            // fix prev (p)
            T *p = this->prev(v);
            if (NULL != p) {
              this->next(p) = n;
              this->prev(n) = p;
            } else {
              this->prev(n) = NULL;
            }
            // fix follow (f)
            if (NULL != f) {
              this->prev(f) = v;
              this->next(v) = f;
            } else {
              this->next(v) = NULL;
            }
            // fix interior
            this->prev(v) = n;
            this->next(n) = v;
          } else {
            v = n;
          }
          n = f;
        }
      }
    }
  }
};
#define SortableQue(c, l) oceanbase::common::SortableQueue<c, c::Link##_##f>

// Adds counting to the Queue
template <class T, class L = typename T::Link_link_>
struct CountQueue : public Queue<T, L>
{
  int size_;
  inline CountQueue(void) : size_(0) {}
  inline void push(T *e);
  inline T *pop();
  inline void enqueue(T *e);
  inline T *dequeue();
  inline void remove(T *e);
  inline void insert(T *e, T *after);
  inline void append(CountQueue<T,L> &q);
  inline void append_clear(CountQueue<T, L> &q);
};
#define CountQue(c, f) oceanbase::common::CountQueue<c, c::Link##_##f>
#define CountQueM(c, m, mf, f) oceanbase::common::CountQueue<c, c::Link##_##mf##f>

template <class T, class L>
inline void CountQueue<T, L>::push(T *e)
{
  Queue<T, L>::push(e);
  size_++;
}

template <class T, class L>
inline T *CountQueue<T, L>::pop()
{
  T *ret = Queue<T, L>::pop();
  if (NULL != ret) {
    size_--;
  }
  return ret;
}

template <class T, class L>
inline void CountQueue<T,L>::remove(T *e)
{
  Queue<T,L>::remove(e);
  size_--;
}

template <class T, class L>
inline void CountQueue<T, L>::enqueue(T *e)
{
  Queue<T, L>::enqueue(e);
  size_++;
}

template <class T, class L>
inline T *CountQueue<T, L>::dequeue()
{
  return pop();
}

template <class T, class L>
inline void CountQueue<T, L>::insert(T *e, T *after)
{
  Queue<T, L>::insert(e, after);
  size_++;
}

template <class T, class L>
inline void CountQueue<T,L>::append(CountQueue<T,L> &q)
{
  Queue<T,L>::append(q);
  size_ += q.size_;
}

template <class T, class L>
inline void CountQueue<T,L>::append_clear(CountQueue<T,L> &q)
{
  append(q);
  q.head_ = q.tail_ = 0;
  q.size_ = 0;
}

// Atomic intrusive lists
template<class T, class L = typename T::Link_link_>
struct ObAtomicSLL
{
  ObAtomicSLL();
  explicit ObAtomicSLL(const char *name);

  void push(T *c) { al_.push(c); }
  T *pop() { return reinterpret_cast<T *>(al_.pop()); }
  T *popall() { return reinterpret_cast<T *>(al_.popall()); }
  bool empty() { return al_.empty(); }

  // only if only one thread is doing pops it is possible to have a "remove"
  // which only that thread can use as well.
  T *remove(T *c) { return reinterpret_cast<T *>(al_.remove(c)); }
  T *head() { return reinterpret_cast<T *>(al_.head()); }
  T *next(T *c) { return reinterpret_cast<T *>(al_.next(c)); }

  ObAtomicList al_;
};

#define ASLL(c, l) oceanbase::common::ObAtomicSLL<c, c::Link##_##l>
#define ASLLM(c, m, ml, l) oceanbase::common::ObAtomicSLL<c, c::Link##_##ml##l>

template<class T, class L>
inline ObAtomicSLL<T, L>::ObAtomicSLL()
{
  (void)al_.init("ObAtomicSLL", reinterpret_cast<int64_t>(&L::next_link(reinterpret_cast<T *>(0))));
}

template<class T, class L>
inline ObAtomicSLL<T,L>::ObAtomicSLL(const char *name)
{
  (void)al_.init(name, reinterpret_cast<int64_t>(&L::next_link(reinterpret_cast<T *>(0))));
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_INTRUSIVE_LIST_H_
