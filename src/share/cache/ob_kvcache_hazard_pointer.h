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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_HAZARD_POINTER_H_
#define OCEANBASE_CACHE_OB_KVCACHE_HAZARD_POINTER_H_

#include "lib/alloc/alloc_struct.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace common {
class ObKVMemBlockHandle;
class HazptrHolder;

class HazardPointer final {
public:
  DISABLE_COPY_ASSIGN(HazardPointer);
  bool protect(ObKVMemBlockHandle* mb_handle, int32_t seq_num);
  bool protect(ObKVMemBlockHandle* mb_handle);
  ObKVMemBlockHandle* get_mb_handle() const;
  void reset_protect(ObKVMemBlockHandle* mb_handle);
  void release();
  HazardPointer* get_next() const;
  HazardPointer* get_next_atomic() const;
  void set_next(HazardPointer* next);
  void set_next_atomic(HazardPointer* next);

private:
  template <typename T>
  friend class FixedTinyAllocator;

  HazardPointer() : next_{nullptr}, mb_handle_(nullptr) {}

  HazardPointer* next_;
  ObKVMemBlockHandle* mb_handle_;
};

// like std::shared<HazardPointer>
class SharedHazptr final {
public:
  SharedHazptr() = delete;
  static int make(HazardPointer& hazptr, SharedHazptr& shared_hazptr);
  SharedHazptr(const SharedHazptr& other);
  ~SharedHazptr();
  SharedHazptr& operator=(const SharedHazptr& other);
  void move_from(SharedHazptr& other);
  void reset();
  ObKVMemBlockHandle* get_mb_handle() const;
private:
  struct ControlPointer {
    ControlPointer(HazardPointer& hazptr) : refcnt_(1), hazptr_(&hazptr) {}
    ~ControlPointer();
    uint64_t refcnt_;
    HazardPointer* hazptr_;
  };
  ControlPointer* ctrl_ptr_;
  static lib::ObMemAttr attr_;
};

template<typename Node>
class SList;
using HazptrList = SList<HazardPointer>;
// Queue-like single linked list
// thread safe method is implemented like ObSpLinkQueue
template <typename Node>
// requires requires(Node node, const Node const_node, Node* ptr) {
//   { const_node.get_next() } -> std::same_as<Node*>;
//   { node.set_next(ptr) } -> std::same_as<void>;
//   { const_node.get_next_atomic() } -> std::same_as<Node*>;
//   { node.set_next_atomic(ptr) } -> std::same_as<void>;
// }
class SList {
public:
  class Iterator {
    public:
    Node& operator*()
    {
      return *curr_;
    } 
    Node* operator->()
    {
      return curr_;
    } 
    bool operator==(const Iterator& other) const
    {
      return curr_ == other.curr_;
    }
    bool operator!=(const Iterator& other) const
    {
      return curr_ != other.curr_;
    }
    Iterator& operator++()
    {
      curr_ = curr_->get_next();
      return *this;
    }
    Iterator operator++(int)
    {
      Iterator iter = *this;
      curr_ = curr_->get_next();
      return iter;
    }

  protected:
    Node* curr_;

  private:
    friend class SList;

    Iterator(Node* head) : curr_(head) {}
    Iterator() : Iterator(nullptr) {}
  };
  class ErasableIterator : public Iterator {
  public:
    ErasableIterator(Iterator iter) : Iterator(iter), prev_(nullptr) {}
    Iterator& operator++()
    {
      prev_ = Iterator::curr_;
      return Iterator::operator++();
    }
    Iterator operator++(int)
    {
      Iterator iter = *this;
      prev_ = Iterator::curr_;
      Iterator::curr_ = Iterator::curr_->get_next();
      return iter;
    }

  protected:
    friend class SList;

    Node* prev_;
  };
  
  SList<>() : head_(nullptr), tail_(nullptr), size_(0)
  {}
  SList<Node>(Node* head) : head_(head), size_(0)
  {
    Node *iter = head, *prev = nullptr;
    for (; iter != nullptr; prev = iter, iter = iter->get_next(), ++size_) {}
    if (OB_NOT_NULL(prev)) {
      tail_ = prev;
      tail_->set_next(nullptr);
    }
  }
  SList<Node>(Node* head, Node* tail, int64_t size) : head_(head), tail_(tail), size_(size)
  {
    if (size == 0) {
      reset();
    } else {
      tail_->set_next(nullptr);
    }
  }
  void reset();
  Node* top() const;
  void push(Node* node);
  void push(SList<Node>& list);
  void push_front(Node* node);
  void push_front(SList<Node>& list);
  Node* pop();
  SList<Node> pop(int64_t num);
  // if the list is empty, return false
  // else lock the list and return true
  bool lock_unless_empty();
  // thread-safe
  void push_ts(Node* node);
  void push_ts(SList<Node>& other);
  bool try_push_front_ts(Node* node);
  void push_front_ts(Node* node);
  Node* pop_ts();
  SList<Node> pop_ts(int64_t num);
  SList<Node> pop_all_ts();
  Iterator begin() const;
  Iterator end() const;
  Node* erase(ErasableIterator& iter);
  int64_t get_size() const
  {
    return size_;
  }
  int64_t get_size_ts() const
  {
    return ATOMIC_LOAD_RLX(&size_);
  }
  Node* const& get_head() const
  {
    return head_;
  }
  Node* get_head_locked() const
  {
    return (Node* const)clear_last_bit(*(uintptr_t*)&head_);
  }
  Node* const& get_tail() const
  {
    return tail_;
  }
  bool is_locked() const
  {
    return is_last_bit_set((uintptr_t)ATOMIC_LOAD_RLX(&head_));
  }
  bool is_empty() const
  {
    return size_ == 0;
  }
  TO_STRING_KV(KP_(head), KP_(tail), K_(size));

protected:
  friend void check(const HazptrList& list);
  Node* head_; /* CACHE_ALIGNED */
  Node* tail_; /* CACHE_ALIGNED */
  int64_t size_; /* CACHE_ALIGNED */
};

template <typename Node>
void SList<Node>::reset()
{
  head_ = nullptr;
  tail_ = nullptr;
  size_ = 0;
}

template <typename Node>
Node* SList<Node>::top() const
{
  return head_;
}

template <typename Node>
void SList<Node>::push(Node* node)
{
  if (OB_NOT_NULL(node)) {
    node->set_next(nullptr);
    if (OB_ISNULL(head_)) {
      head_ = node;
    } else {
      tail_->set_next(node);
    }
    tail_ = node;
    ++size_;
  }
}

template <typename Node>
void SList<Node>::push_front(Node* node)
{
  if (OB_NOT_NULL(node)) {
    if (OB_ISNULL(head_)) {
      tail_ = node;
    }
    node->set_next(head_);
    head_ = node;
    ++size_;
  }
}

template <typename Node>
void SList<Node>::push_front(SList<Node>& list)
{
  if (OB_LIKELY(!list.is_empty())) {
    if (OB_UNLIKELY(is_empty())) {
      *this = list;
    } else {
      list.tail_->set_next(head_);
      head_ = list.get_head();
      size_ += list.get_size();
    }
    list.reset();
  }
}

template <typename Node>
void SList<Node>::push(SList<Node>& list)
{
  if (OB_LIKELY(!list.is_empty())) {
    if (OB_UNLIKELY(is_empty())) {
      *this = list;
    } else {
      tail_->set_next(list.get_head());
      tail_ = list.get_tail();
      size_ += list.get_size();
    }
    list.reset();
  }
}

template <typename Node>
Node* SList<Node>::pop()
{
  Node* node = nullptr;
  if (!is_empty()) {
    node = head_;
    head_ = head_->get_next();
    --size_;
    if (OB_ISNULL(head_)) {
      tail_ = nullptr;
    }
  }
  return node;
}

template <typename Node>
SList<Node> SList<Node>::pop(int64_t num)
{
  SList<Node> list;
  Node *iter, *new_head;
  int64_t size;
  if (num <= 0) {
  } else if (num >= get_size()) {
    list = *this;
    reset();
  } else {
    for (iter = head_, size = 1; size < num; iter = iter->get_next(), ++size) {}
    new_head = iter->get_next();
    list = SList<Node>(head_, iter, num);
    head_ = new_head;
    size_ -= num;
    if (OB_ISNULL(head_)) {
      tail_ = nullptr;
    }
  }
  return list;
}

template <typename Node>
bool SList<Node>::lock_unless_empty()
{
  bool b_ret = true;
  uintptr_t head;
  do {
    head = set_last_bit((uintptr_t*)&head_);
    if (head == 1) {
      b_ret = false;
      break;
    } else if (is_last_bit_set(head)) {
      PAUSE();
    }
  } while (is_last_bit_set(head));
  if (head == 0) {
    b_ret = false;
    ATOMIC_BCAS(&head_, (Node*)1, nullptr);
  }
  return b_ret;
}

template <typename Node>
void SList<Node>::push_ts(Node* node)
{
  Node* old_tail;
  if (OB_ISNULL(node)) {
  } else if (FALSE_IT(node->set_next(nullptr))) {
  } else if (FALSE_IT(old_tail = ATOMIC_TAS(&tail_, node))) {
  } else if (OB_ISNULL(old_tail)) {
    ATOMIC_STORE_RLX(&head_, node);
  } else {
    old_tail->set_next_atomic(node);
  }
  if (OB_NOT_NULL(node)) {
    ATOMIC_AAF(&size_, 1);
  }
}

template <typename Node>
void SList<Node>::push_ts(SList<Node>& list)
{
  Node* old_tail;
  if (OB_UNLIKELY(list.is_empty())) {
  } else if (FALSE_IT(old_tail = ATOMIC_TAS(&tail_, list.get_tail()))) {
  } else if (OB_ISNULL(old_tail)) {
    ATOMIC_STORE_RLX(&head_, list.get_head());
  } else {
    old_tail->set_next_atomic(list.get_head());
  }
  if (!list.is_empty()) {
    ATOMIC_AAF(&size_, list.get_size());
    list.reset();
  }
}

template <typename Node>
bool SList<Node>::try_push_front_ts(Node* node)
{
  bool b_ret = false;
  Node* head;
  if (OB_ISNULL(node)) {
    b_ret = true;
  } else if (FALSE_IT(node->set_next(nullptr))) {
  } else if (FALSE_IT(head = ATOMIC_LOAD_RLX(&head_))) {
  } else if (is_last_bit_set((uint64_t)head)) {
  } else if (OB_ISNULL(head)) {
    if (ATOMIC_BCAS(&tail_, nullptr, node)) {
      WEAK_BARRIER();
      ATOMIC_STORE_RLX(&head_, node);
      b_ret = true;
    }
  } else if (FALSE_IT(node->set_next(head))) {
  } else if (ATOMIC_BCAS(&head_, head, node)) {
    b_ret = true;
  }
  if (b_ret) {
	ATOMIC_AAF(&size_, 1);
  }
  return b_ret;
}

template <typename Node>
void SList<Node>::push_front_ts(Node* node)
{
  while(!try_push_front_ts(node)) {
    PAUSE();
  } 
}


template <typename Node>
Node* SList<Node>::pop_ts()
{
  Node *node, *next = nullptr;
  if (!lock_unless_empty()) {
    node = nullptr;
  } else if (FALSE_IT(node = get_head_locked())) {
  } else if (OB_NOT_NULL(next = node->get_next_atomic())) {
  } else if (ATOMIC_BCAS(&tail_, node, nullptr)) {
    ATOMIC_BCAS(&head_, (Node*)(1 | (uintptr_t)node), nullptr);
  } else {
    while (OB_ISNULL(next = node->get_next_atomic())) {
      PAUSE();
    }
  }
  if (OB_NOT_NULL(next)) {
    ATOMIC_STORE_RLX(&head_, next);
  }
  if (OB_NOT_NULL(node)) {
    ATOMIC_SAF(&size_, 1);
  }
  return node;
}

template <typename Node>
SList<Node> SList<Node>::pop_ts(int64_t num)
{
  Node *head = nullptr, *prev, *curr = nullptr;
  int64_t size = 0;
  SList<Node> list;
  if (!lock_unless_empty()) {
  } else if (FALSE_IT(head = get_head_locked())) {
  } else if (({
               for (prev = nullptr, curr = head; curr != nullptr && size < num;
                    prev = curr, curr = curr->get_next_atomic(), ++size) {}
               false;
             })) {
  } else if (OB_NOT_NULL(curr)) {
  } else if (ATOMIC_BCAS(&tail_, prev, nullptr)) {
    ATOMIC_BCAS(&head_, (Node*)(1 | (uintptr_t)head), nullptr);
  } else {
    while (OB_ISNULL(curr = prev->get_next_atomic())) {
      PAUSE();
    }
  }
  if (OB_NOT_NULL(curr)) {
    ATOMIC_STORE_RLX(&head_, curr);
  }
  if (OB_NOT_NULL(head)) {
    prev->set_next(nullptr);
    list = SList<Node>(head, prev, size);
    ATOMIC_SAF(&size_, list.get_size());
  }
  return list;
}

template <typename Node>
SList<Node> SList<Node>::pop_all_ts()
{
  return pop_ts(INT64_MAX);
}

template <typename Node>
typename SList<Node>::Iterator SList<Node>::begin() const
{
  return Iterator(get_head_locked());
}

template <typename Node>
typename SList<Node>::Iterator SList<Node>::end() const
{
  return Iterator();
}

template <typename Node>
Node* SList<Node>::erase(ErasableIterator& iter)
{
  Node* erased = nullptr;
  if (OB_ISNULL(iter.curr_)) {
  } else if (OB_ISNULL(iter.prev_)) {
    ob_assert(iter.curr_ == get_head());
    erased = pop();
    iter.curr_ = get_head();
  } else {
    erased = iter.curr_;
    iter.prev_->set_next(erased->get_next());
    iter.curr_ = iter.prev_->get_next();
    if (OB_ISNULL(iter.curr_)) {
      tail_ = iter.prev_;
    }
    --size_;
  }
  return erased;
}

};  // namespace common
};  // namespace oceanbase

#endif
