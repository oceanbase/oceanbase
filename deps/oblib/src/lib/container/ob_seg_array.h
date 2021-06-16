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

#ifndef OCEANBASE_LIG_CONTAINER_OB_SEG_ARRAY_
#define OCEANBASE_LIG_CONTAINER_OB_SEG_ARRAY_

#include "lib/allocator/ob_small_allocator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {

// for debug print
// print the item obj or derefer the obj ptr and print
template <typename T>
class PrintElementFunctor {
public:
  PrintElementFunctor()
  {}
  int operator()(const T& t) const
  {
    LIB_LOG(INFO, "print: ", K(t));
    return OB_SUCCESS;
  }
};

template <typename T>
class PrintElementFunctor<T*> {
public:
  PrintElementFunctor()
  {}
  int operator()(const T* t) const
  {
    LIB_LOG(INFO, "print: ", "t", *t);
    return OB_SUCCESS;
  }
};

// A fixed-size ring queue
// NOTE: NOT THREAD SAFE
// one bucket is reserved for distinguish empty and full
// Support binary search
template <typename SegPtr, int64_t SIZE>
class ObFixedRingDeque {
public:
  ObFixedRingDeque() : is_inited_(false), head_(0), tail_(0)
  {}
  ~ObFixedRingDeque()
  {
    reset();
  }
  int init();
  void reset();
  int push_back(const SegPtr& item);
  int push_front(const SegPtr& item);
  int pop_front(SegPtr& item);
  int pop_back(SegPtr& item);
  bool is_full() const
  {
    return (head_ + 1) % SIZE == tail_;
  }
  bool is_empty() const
  {
    return head_ == tail_;
  }
  int64_t size() const
  {
    return (head_ + SIZE - tail_) % SIZE;
  }
  int top_front(SegPtr& item);
  int top_back(SegPtr& item);

  // search for upper and lower bound
  // 1. lower bound: the first object which is less than or equal to the target
  // 2. upper bound: the first object which is more than the target
  //
  // return:
  // 1. OB_SUCCESS                    return upper and lower bound
  // 2. OB_ERR_OUT_OF_UPPER_BOUND     return lower bound, upper bound not exists
  // 3. OB_ERR_OUT_OF_LOWER_BOUND     return upper bound, lower bound not exists
  // 4. OB_EMPTY_RESULT               find nothing, return null
  // 4. other
  template <typename LessThanEqualtoFunctor>
  int search_boundary(const SegPtr& target, SegPtr& prev, SegPtr& next, const LessThanEqualtoFunctor& le_functor);

  template <typename Functor>
  int for_each(Functor fn) const;

  template <typename Functor>
  int reverse_for_each(Functor& fn) const;

  void debug_print() const;

private:
  inline void advance_(int64_t& v) const
  {
    v = (v + 1) % SIZE;
  }
  inline void decline_(int64_t& v) const
  {
    v = (v + SIZE - 1) % SIZE;
  }
  inline int64_t advanced_val(int64_t v) const
  {
    return (v + 1) % SIZE;
  }
  inline int64_t declined_val(int64_t v) const
  {
    return (v + SIZE - 1) % SIZE;
  }

  template <typename LessThanEqualtoFunctor>
  int binary_search_on_range_(const int64_t small, const int64_t large, const SegPtr& target, SegPtr& prev,
      SegPtr& next, const LessThanEqualtoFunctor& le_functor);

private:
  bool is_inited_;
  SegPtr arr_[SIZE];
  int64_t head_;
  int64_t tail_;
  DISALLOW_COPY_AND_ASSIGN(ObFixedRingDeque);
};

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    head_ = 0;
    tail_ = 0;
    is_inited_ = true;
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
void ObFixedRingDeque<SegPtr, SIZE>::reset()
{
  if (is_inited_) {
    is_inited_ = false;
    head_ = 0;
    tail_ = 0;
  }
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::push_back(const SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_full())) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    arr_[head_] = item;
    advance_(head_);
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::push_front(const SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_full())) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    decline_(tail_);
    arr_[tail_] = item;
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::pop_front(SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[tail_];
    advance_(tail_);
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::pop_back(SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    decline_(head_);
    item = arr_[head_];
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::top_front(SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[tail_];
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
int ObFixedRingDeque<SegPtr, SIZE>::top_back(SegPtr& item)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[declined_val(head_)];
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
template <typename LessThanEqualtoFunctor>
int ObFixedRingDeque<SegPtr, SIZE>::binary_search_on_range_(const int64_t small, const int64_t large,
    const SegPtr& target, SegPtr& prev, SegPtr& next, const LessThanEqualtoFunctor& le_functor)
{
  // notice: we assumed that the object small and large point to are not null.
  int ret = OB_SUCCESS;
  int64_t s = small - 1;
  int64_t l = large + 1;
  int64_t m = 0;
  while (s + 1 < l) {
    m = (s + l) / 2;
    if (le_functor(arr_[m], target)) {
      s = m;
    } else {
      l = m;
    }
  }

  if (s == small - 1) {
    // less than the smallest one, so lower bound not exists, and upper bound use the first object
    next = arr_[small];
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
  } else {
    prev = arr_[s];

    if (s + 1 <= large) {
      next = arr_[s + 1];
    } else {
      // more than the biggest one, so lower bound exists and upper bound not exists.
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    }
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
template <typename LessThanEqualtoFunctor>
int ObFixedRingDeque<SegPtr, SIZE>::search_boundary(
    const SegPtr& target, SegPtr& prev, SegPtr& next, const LessThanEqualtoFunctor& le_functor)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_empty())) {
    ret = OB_EMPTY_RESULT;
  } else if (1 == size()) {
    // there are only one object
    if (le_functor(arr_[tail_], target)) {
      // lower bound is valid, and upper bound not exists, more than the biggest object
      prev = arr_[tail_];
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    } else {
      // upper bound is valid, and lower bound not exists, less than the smallest object
      next = arr_[tail_];
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    }
  } else {
    // multi-object
    int64_t t = tail_;
    int64_t h = declined_val(head_);

    if (h > t) {
      // there is no reverse
      ret = binary_search_on_range_(t, h, target, prev, next, le_functor);
    } else {
      // there is reverse, search on two part
      if (le_functor(arr_[SIZE - 1], target)) {
        // 1. search on the second part
        ret = binary_search_on_range_(0, h, target, prev, next, le_functor);
        // if less than lower bound, lower bound is the last object in the first part.
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          prev = arr_[SIZE - 1];
          ret = OB_SUCCESS;
        }
      } else {
        // 2. search on the first part
        // the upper bound must be in this interval because of arr_[SIZE - 1] > target,
        // so no need to handle upper bound.
        ret = binary_search_on_range_(t, SIZE - 1, target, prev, next, le_functor);
      }
    }
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
template <typename Functor>
int ObFixedRingDeque<SegPtr, SIZE>::for_each(Functor fn) const
{
  int ret = OB_SUCCESS;
  int64_t t = tail_;
  for (; OB_SUCC(ret) && (t != head_); advance_(t)) {
    ret = fn(arr_[t]);
  }
  if (OB_FAIL(ret)) {
    LIB_LOG(ERROR, "ObFixedRingDeque for_each error", K(ret), K(t), K(head_), K(tail_));
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
template <typename Functor>
int ObFixedRingDeque<SegPtr, SIZE>::reverse_for_each(Functor& fn) const
{
  int ret = OB_SUCCESS;
  int64_t t = head_ - 1;
  for (; OB_SUCC(ret) && (t != declined_val(tail_)); decline_(t)) {
    ret = fn(arr_[t]);
  }
  if (OB_SUCCESS != ret && OB_CANCELED != ret) {
    LIB_LOG(ERROR, "ObFixedRingDeque reverse_for_each error", K(ret), K(t), K(head_), K(tail_));
  }
  return ret;
}

template <typename SegPtr, int64_t SIZE>
void ObFixedRingDeque<SegPtr, SIZE>::debug_print() const
{
  LIB_LOG(INFO, "Queue:", K(tail_), K(head_));
  PrintElementFunctor<SegPtr> print_fn;
  for_each(print_fn);
}

// ----------------------------------------------------------------------
enum SegCreationType {
  FRONT_SEG = 1,
  BACK_SEG = 2,
};

// Segment: a bulk of items
// Order of segments is defined as the order of their first items
template <typename T, int64_t SIZE>
class Seg {
public:
  explicit Seg(const SegCreationType type);
  explicit Seg(const T& item);
  ~Seg()
  {
    start_idx_ = 0;
    end_idx_ = 0;
  }

  inline int64_t get_count() const
  {
    return end_idx_ - start_idx_;
  }
  const T& get_delegate() const
  {
    return arr_[start_idx_];
  }
  T& get_min()
  {
    return arr_[start_idx_];
  }

  inline bool is_empty() const
  {
    return start_idx_ == end_idx_;
  }
  inline bool can_push_back() const
  {
    return end_idx_ < SIZE;
  }
  inline bool can_push_front() const
  {
    return start_idx_ > 0;
  }
  int push_back(const T& item);
  int push_front(const T& item);
  int pop_front(T& item);
  int top_front(T& item);
  int pop_back(T& item);
  int top_back(T& item);

  int search_boundary(const T& target, T*& prev_item, T*& next_item);
  template <typename Functor>
  int reverse_for_each(Functor& fn) const;

  DECLARE_VIRTUAL_TO_STRING;

private:
  T arr_[SIZE];
  int64_t start_idx_;  // push_front will place item at (start_idx_ - 1)
  int64_t end_idx_;    // push_back will place item at end_idx_
};

template <typename T, int64_t SIZE>
Seg<T, SIZE>::Seg(const SegCreationType type)
{
  if (FRONT_SEG == type) {
    start_idx_ = SIZE;
    end_idx_ = SIZE;
  } else if (BACK_SEG == type) {
    start_idx_ = 0;
    end_idx_ = 0;
  } else {
    // only used in this file, caller guarantee valid args
    LIB_LOG(ERROR, "unkown SegCreationType", K(type), K(lbt()));
  }
}

template <typename T, int64_t SIZE>
Seg<T, SIZE>::Seg(const T& item)
{
  if (SIZE <= 0) {
    LIB_LOG(ERROR, "SIZE invalid", K(SIZE));
  } else {
    start_idx_ = 0;
    end_idx_ = 0;
    arr_[end_idx_] = item;
    end_idx_++;
  }
}

template <typename T, int64_t SIZE>
int64_t Seg<T, SIZE>::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t count = get_count();
  databuff_printf(
      buf, buf_len, pos, "Seg={count=%ld, start_idx_=%ld, end_idx_ =%ld, arr=[", count, start_idx_, end_idx_);
  for (int64_t i = start_idx_; i < end_idx_; i++) {
    pos += arr_[i].to_string(buf + pos, buf_len - pos);
    if (i != count - 1) {
      databuff_printf(buf, buf_len, pos, ", ");
    }
  }
  databuff_printf(buf, buf_len, pos, "]}");
  return pos;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::push_back(const T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_push_back())) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    arr_[end_idx_] = item;
    end_idx_++;
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::push_front(const T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_push_front())) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    start_idx_--;
    arr_[start_idx_] = item;
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::pop_front(T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[start_idx_];
    start_idx_++;
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::top_front(T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[start_idx_];
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::pop_back(T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[end_idx_ - 1];
    end_idx_--;
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::top_back(T& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = arr_[end_idx_ - 1];
  }
  return ret;
}

template <typename T, int64_t SIZE>
int Seg<T, SIZE>::search_boundary(const T& target, T*& prev_item, T*& next_item)
{
  int ret = OB_SUCCESS;
  int64_t s = start_idx_ - 1;
  int64_t l = end_idx_;
  int64_t m = 0;
  while (s + 1 < l) {
    m = (s + l) / 2;
    if (arr_[m] <= target) {
      s = m;
    } else {
      l = m;
    }
  }

  // less than lower bound
  if (start_idx_ - 1 == s) {
    prev_item = NULL;
    next_item = NULL;

    // check if the upper bound is valid
    if (s + 1 < end_idx_) {
      next_item = arr_ + s + 1;
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    } else {
      // if upper bound is invalid either,  return null
      ret = OB_EMPTY_RESULT;
    }
  } else {
    // find lower bound
    prev_item = arr_ + s;
    next_item = NULL;
    // check if it is more than the upper bound
    if (s + 1 < end_idx_) {
      next_item = arr_ + s + 1;
    } else {
      // more than the upper bound
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    }
  }
  return ret;
}

template <typename T, int64_t SIZE>
template <typename Functor>
int Seg<T, SIZE>::reverse_for_each(Functor& fn) const
{
  int ret = OB_SUCCESS;
  int64_t t = end_idx_ - 1;
  for (; OB_SUCC(ret) && (t != start_idx_ - 1); t--) {
    ret = fn(arr_[t]);
  }
  if (OB_SUCCESS != ret && OB_CANCELED != ret) {
    LIB_LOG(ERROR, "Seg reverse_for_each error", K(ret), K(t), K(end_idx_), K(start_idx_));
  }
  return ret;
}

template <typename T>
class ObISegArray {
public:
  ObISegArray()
  {}
  virtual ~ObISegArray()
  {}

public:
  virtual int init(ObSmallAllocator* allocator) = 0;
  virtual void destroy() = 0;
  virtual int clear() = 0;

  virtual int push_back(const T& item) = 0;
  virtual int push_front(const T& item) = 0;
  virtual int pop_front(T& item) = 0;
  virtual int top_front(T& item) = 0;
  virtual int pop_back(T& item) = 0;
  virtual int top_back(T& item) = 0;

  virtual bool is_empty() const = 0;

  // search for upper and lower bound
  // 1. lower bound: the first object which is less than or equal to the target
  // 2. upper bound: the first object which is more than the target
  //
  // return:
  // 1. OB_SUCCESS                    return upper and lower bound
  // 2. OB_ERR_OUT_OF_UPPER_BOUND     return lower bound, upper bound not exists
  // 3. OB_ERR_OUT_OF_LOWER_BOUND     return upper bound, lower bound not exists
  // 4. OB_EMPTY_RESULT               find nothing, return null
  // 4. other
  virtual int search_boundary(const T& item, T& prev_item, T& next_item) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObISegArray);
};

// ----------------------------------------------------------------------
template <typename T, int64_t SEG_STEP = 100, int64_t SEG_COUNT = 200>
class ObSegArray : public ObISegArray<T> {
private:
  class DestroySegFunctor {
  public:
    explicit DestroySegFunctor(ObSmallAllocator* allocator) : allocator_(allocator)
    {}
    int operator()(const Seg<T, SEG_STEP>* seg)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(seg)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        allocator_->free(const_cast<Seg<T, SEG_STEP>*>(seg));
        seg = NULL;
      }
      return ret;
    }

  private:
    ObSmallAllocator* allocator_;
  };

  // Seg order functor, less than or equal to.
  class SegLEFunctor {
  public:
    SegLEFunctor()
    {}
    bool operator()(const Seg<T, SEG_STEP>* x, const Seg<T, SEG_STEP>* y) const
    {
      bool bret = false;
      if (OB_UNLIKELY(0 == x->get_count()) || OB_UNLIKELY(0 == y->get_count())) {
        LIB_LOG(ERROR, "invalid argument", "x.count", x->get_count(), "y.count", y->get_count());
      } else {
        bret = (x->get_delegate() <= y->get_delegate());
      }
      return bret;
    }
  };

  template <typename Functor>
  class ReverseForEachFunctor {
  public:
    explicit ReverseForEachFunctor(Functor& fn) : fn_(fn)
    {}
    int operator()(const Seg<T, SEG_STEP>* seg)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(seg)) {
        ret = OB_INVALID_ARGUMENT;
        LIB_LOG(ERROR, "seg is NULL", K(ret));
      } else {
        ret = seg->reverse_for_each(fn_);
      }
      return ret;
    }

  private:
    Functor& fn_;
  };

public:
  ObSegArray() : is_inited_(false), rwlock_(), dir_(), allocator_(NULL), total_count_(0)
  {}
  virtual int init(ObSmallAllocator* allocator) override;
  bool is_inited() const
  {
    return is_inited_;
  }
  virtual void destroy() override;
  virtual int clear() override;

  virtual int push_back(const T& item) override;
  virtual int push_front(const T& item) override;
  virtual int pop_front(T& item) override;
  virtual int top_front(T& item) override;
  virtual int pop_back(T& item) override;
  virtual int top_back(T& item) override;

  virtual bool is_empty() const override
  {
    return 0 == total_count_;
  }

  // find lower bound object, then call the specific functor
  template <typename Operator>
  int search_lower_bound_and_operate(const T& item, Operator& op);

  // search for upper and lower bound
  // 1. lower bound: the first object which is less than or equal to the target
  // 2. upper bound: the first object which is more than the target
  //
  // return:
  // 1. OB_SUCCESS                    return upper and lower bound
  // 2. OB_ERR_OUT_OF_UPPER_BOUND     return lower bound, upper bound not exists
  // 3. OB_ERR_OUT_OF_LOWER_BOUND     return upper bound, lower bound not exists
  // 4. OB_EMPTY_RESULT               find nothing, return null
  // 4. other
  virtual int search_boundary(const T& item, T& prev_item, T& next_item) override;

  inline static uint64_t get_seg_size()
  {
    Seg<T, SEG_STEP>* sample_seg = NULL;
    return sizeof(*sample_seg);
  }
  void debug_print() const;

  template <typename Functor>
  int reverse_for_each(Functor& fn) const;

private:
  inline int alloc_seg(Seg<T, SEG_STEP>*& seg, const SegCreationType type);
  void free_seg(Seg<T, SEG_STEP>* seg);
  int top_front_(T& top_item, const bool pop_it);
  int top_back_(T& top_item, const bool pop_it);
  int search_boundary_internal_(const T& item, T*& prev_item, T*& next_item);

private:
  bool is_inited_;
  mutable SpinRWLock rwlock_;
  ObFixedRingDeque<Seg<T, SEG_STEP>*, SEG_COUNT> dir_;
  ObSmallAllocator* allocator_;
  int64_t total_count_;
};

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::init(ObSmallAllocator* allocator)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wr_guard(rwlock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    allocator_ = allocator;
    total_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
void ObSegArray<T, SEG_STEP, SEG_COUNT>::destroy()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wr_guard(rwlock_);
  if (is_inited_) {
    DestroySegFunctor destory_fn(allocator_);
    if (OB_FAIL(dir_.for_each(destory_fn))) {
      LIB_LOG(ERROR, "destory segment error", K(ret));
    }
  }
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::clear()
{
  typedef Seg<T, SEG_STEP> SegT;
  int ret = OB_SUCCESS;
  SpinWLockGuard wr_guard(rwlock_);
  if (is_inited_) {
    while (OB_SUCC(ret)) {
      SegT* poped = NULL;
      if (OB_FAIL(dir_.pop_back(poped))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LIB_LOG(ERROR, "pop_back failed", K(ret));
        }
      } else {
        allocator_->free(poped);
      }
    };

    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      total_count_ = 0;
    }
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::alloc_seg(Seg<T, SEG_STEP>*& seg, const SegCreationType type)
{
  int ret = OB_SUCCESS;
  seg = NULL;
  char* buf = NULL;
  if (OB_UNLIKELY(NULL == (buf = (char*)allocator_->alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    seg = new (buf) Seg<T, SEG_STEP>(type);
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
void ObSegArray<T, SEG_STEP, SEG_COUNT>::free_seg(Seg<T, SEG_STEP>* seg)
{
  if (NULL != seg) {
    allocator_->free(seg);
    seg = NULL;
  }
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::push_back(const T& item)
{
  typedef Seg<T, SEG_STEP> SegT;
  int ret = OB_SUCCESS;
  SpinWLockGuard wr_guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    SegT* back_seg = NULL;
    bool alloc_new = false;
    ret = dir_.top_back(back_seg);
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(back_seg)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "top_back return null seg", K(ret), K(lbt()));
      } else {
        alloc_new = !back_seg->can_push_back();
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      alloc_new = true;
      ret = OB_SUCCESS;
    } else {
      LIB_LOG(ERROR, "top_back error", K(ret), K(lbt()));
    }
    if (OB_SUCC(ret) && alloc_new) {
      if (OB_FAIL(alloc_seg(back_seg, BACK_SEG))) {
      } else if (OB_FAIL(dir_.push_back(back_seg))) {
        free_seg(back_seg);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(back_seg->push_back(item))) {
        LIB_LOG(ERROR, "back_seg push_back error", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    total_count_++;
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::push_front(const T& item)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wr_guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    typedef Seg<T, SEG_STEP> SegT;
    SegT* front_seg = NULL;
    bool alloc_new = false;
    ret = dir_.top_front(front_seg);
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(front_seg)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "top_back return null seg", K(ret), K(lbt()));
      } else {
        alloc_new = !front_seg->can_push_front();
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      alloc_new = true;
      ret = OB_SUCCESS;
    } else {
      LIB_LOG(ERROR, "top_front error", K(ret), K(lbt()));
    }
    if (OB_SUCC(ret) && alloc_new) {
      if (OB_FAIL(alloc_seg(front_seg, FRONT_SEG))) {
      } else if (OB_FAIL(dir_.push_front(front_seg))) {
        free_seg(front_seg);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(front_seg->push_front(item))) {
        LIB_LOG(ERROR, "back_seg push_front error", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    total_count_++;
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::top_front_(T& top_item, const bool pop_it)
{
  typedef Seg<T, SEG_STEP> SegT;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 == total_count_)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    SegT* top_front_seg = NULL;
    if (OB_FAIL(dir_.top_front(top_front_seg))) {
      LIB_LOG(ERROR, "get dir top_front error", K(ret));
    } else if (OB_ISNULL(top_front_seg)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "top_front_seg is null", K(ret), KP(top_front_seg));
    } else {
      if (!pop_it) {
        if (OB_FAIL(top_front_seg->top_front(top_item))) {
          LIB_LOG(ERROR, "top_front_seg top_front error", K(ret));
        }
      } else {
        if (OB_FAIL(top_front_seg->pop_front(top_item))) {
          LIB_LOG(ERROR, "top_front_seg pop_front error", K(ret));
        } else if (top_front_seg->is_empty()) {
          SegT* poped = NULL;
          if (OB_FAIL(dir_.pop_front(poped))) {
            LIB_LOG(ERROR, "top_front_seg pop_front error", K(ret));
          } else {
            allocator_->free(top_front_seg);
          }
        }
        if (OB_SUCC(ret)) {
          total_count_--;
        }
      }
    }
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::top_front(T& top_item)
{
  const bool pop_it = false;
  SpinRLockGuard rd_guard(rwlock_);
  return top_front_(top_item, pop_it);
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::pop_front(T& top_item)
{
  const bool pop_it = true;
  SpinWLockGuard wr_guard(rwlock_);
  return top_front_(top_item, pop_it);
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::top_back_(T& top_item, const bool pop_it)
{
  typedef Seg<T, SEG_STEP> SegT;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 == total_count_)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    SegT* top_back_seg = NULL;
    if (OB_FAIL(dir_.top_back(top_back_seg))) {
      LIB_LOG(ERROR, "get dir top_back error", K(ret));
    } else if (OB_ISNULL(top_back_seg)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "top_back_seg is null", K(ret), KP(top_back_seg));
    } else {
      if (!pop_it) {
        if (OB_FAIL(top_back_seg->top_back(top_item))) {
          LIB_LOG(ERROR, "top_back_seg top_back error", K(ret));
        }
      } else {
        if (OB_FAIL(top_back_seg->pop_back(top_item))) {
          LIB_LOG(ERROR, "top_back_seg pop_back error", K(ret));
        } else if (top_back_seg->is_empty()) {
          SegT* poped = NULL;
          if (OB_FAIL(dir_.pop_back(poped))) {
            LIB_LOG(ERROR, "top_back_seg pop_back error", K(ret));
          } else {
            allocator_->free(top_back_seg);
          }
        }
        if (OB_SUCC(ret)) {
          total_count_--;
        }
      }
    }
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::top_back(T& top_item)
{
  const bool pop_it = false;
  SpinRLockGuard rd_guard(rwlock_);
  return top_back_(top_item, pop_it);
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::pop_back(T& top_item)
{
  const bool pop_it = true;
  SpinWLockGuard wr_guard(rwlock_);
  return top_back_(top_item, pop_it);
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
template <typename Operator>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::search_lower_bound_and_operate(const T& item, Operator& op)
{
  int ret = OB_SUCCESS;

  // rdlock
  SpinRLockGuard rd_guard(rwlock_);

  T* prev_item = NULL;
  T* next_item = NULL;

  // search lower and upper bound firstly
  ret = search_boundary_internal_(item, prev_item, next_item);

  // lower bound not exists, return null
  if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  // upper bound not exist or success
  else if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
    if (OB_ISNULL(prev_item)) {
      LIB_LOG(ERROR, "lower bound item is invalid", K(prev_item), K(item), K(ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OB_SUCCESS;

      // call operator
      op(*prev_item);
    }
  } else {
    // other failure
    LIB_LOG(WARN, "search_boundary_internal_ fail", K(item), K(ret));
  }

  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::search_boundary(const T& item, T& prev_item, T& next_item)
{
  int ret = OB_SUCCESS;

  // rdlock
  SpinRLockGuard rd_guard(rwlock_);

  T* prev_item_ptr = NULL;
  T* next_item_ptr = NULL;

  ret = search_boundary_internal_(item, prev_item_ptr, next_item_ptr);

  // check if the lower bound is valid
  if (OB_ISNULL(prev_item_ptr) && (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret)) {
    LIB_LOG(ERROR, "lower bound item is invalid", K(ret), K(prev_item_ptr), K(item));
    ret = OB_ERR_UNEXPECTED;
  }
  // check if the upper bound is valid
  else if (OB_ISNULL(next_item_ptr) && (OB_SUCCESS == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret)) {
    LIB_LOG(ERROR, "upper bound item is invalid", K(ret), K(next_item_ptr), K(item));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // get the value of the lower and the upper bound
    if (OB_NOT_NULL(prev_item_ptr)) {
      prev_item = *prev_item_ptr;
    }

    if (OB_NOT_NULL(next_item_ptr)) {
      next_item = *next_item_ptr;
    }
  }

  return ret;
}

// lock by caller to keep object not being modified during function called.
template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::search_boundary_internal_(const T& item, T*& prev_item, T*& next_item)
{
  typedef Seg<T, SEG_STEP> SegT;
  int ret = OB_SUCCESS;
  prev_item = NULL;
  next_item = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    SegT item_seg(item);
    SegT* item_base_seg = &item_seg;
    SegT* prev_base_seg = NULL;
    SegT* next_base_seg = NULL;
    SegLEFunctor seg_le_functor;

    // search for the lower and upper bound of segment
    ret = dir_.search_boundary(item_base_seg, prev_base_seg, next_base_seg, seg_le_functor);

    // the lower bound of segment not exists
    if (OB_EMPTY_RESULT == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      prev_item = NULL;
      next_item = NULL;

      // just set the upper bound of item, keep ret
      if (OB_NOT_NULL(next_base_seg)) {
        next_item = &(next_base_seg->get_min());
      }
    }
    // the lower bound of segment exists
    else if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      // check if the lower bound is valid
      if (OB_ISNULL(prev_base_seg)) {
        LIB_LOG(ERROR,
            "dir search_boundary return invalid lower bound",
            K(ret),
            K(item),
            K(prev_base_seg),
            K(next_base_seg),
            K(item_base_seg));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // reset ret
        ret = OB_SUCCESS;

        // search for the lower and upper bound of item in the lower bound of segment
        ret = prev_base_seg->search_boundary(item, prev_item, next_item);

        // if the upper bound of item not exists and the upper bound of segment exists,
        // set the first object of the upper bound of segment as the upper bound of item.
        if (OB_ERR_OUT_OF_UPPER_BOUND == ret && OB_NOT_NULL(next_base_seg)) {
          next_item = &(next_base_seg->get_min());
          ret = OB_SUCCESS;
        }
      }
    } else {
      LIB_LOG(
          ERROR, "dir search_boundary error", K(ret), K(item), K(prev_base_seg), K(next_base_seg), K(item_base_seg));
    }
  }
  return ret;
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
void ObSegArray<T, SEG_STEP, SEG_COUNT>::debug_print() const
{
  LIB_LOG(INFO, "/*******************************************************\\");
  LIB_LOG(INFO, "seg_array:", K(total_count_));
  dir_.debug_print();
  LIB_LOG(INFO, "\\******************************************************/");
}

template <typename T, int64_t SEG_STEP, int64_t SEG_COUNT>
template <typename Functor>
int ObSegArray<T, SEG_STEP, SEG_COUNT>::reverse_for_each(Functor& fn) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard rd_guard(rwlock_);
  ReverseForEachFunctor<Functor> reverse_for_each_fn(fn);
  ret = dir_.reverse_for_each(reverse_for_each_fn);
  return ret;
}

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIG_CONTAINER_OB_SEG_ARRAY_
