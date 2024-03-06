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

#ifndef OCEANBASE_COMMON_FIXED_QUEUE_
#define OCEANBASE_COMMON_FIXED_QUEUE_
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{
template <typename T>
class ObFixedQueue
{
public:
  ObFixedQueue();
  ~ObFixedQueue();
public:
  int init(const int64_t max_num, char *buf);
  int init(const int64_t max_num,
           ObIAllocator *allocator = global_default_allocator,
           const lib::ObLabel &label = ObModIds::OB_FIXED_QUEUE);
  int init(const int64_t max_num,
           ObIAllocator *allocator,
           const lib::ObMemAttr &attr);
  void destroy();
public:
  int push(T *ptr);
  int pop(T *&ptr);
  int head_unsafe(T *&ptr);
  inline int64_t get_total() const;
  inline int64_t get_free() const;
  inline int64_t get_curr_total() const;
  bool is_inited() const {return is_inited_;};
  int64_t capacity()const { return max_num_; }
private:
  struct ArrayItem
  {
    T *data;
  };
private:
  inline int64_t get_total_(const uint64_t consumer, const uint64_t producer) const;
  inline int64_t get_free_(const uint64_t consumer, const uint64_t producer) const;
private:
  bool is_inited_;
  int64_t max_num_;
  ArrayItem *array_;
  ObIAllocator *allocator_;
  uint64_t consumer_ CACHE_ALIGNED;
  uint64_t producer_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFixedQueue);
};

template <typename T>
ObFixedQueue<T>::ObFixedQueue() : is_inited_(false),
                                  max_num_(0),
                                  array_(NULL),
                                  allocator_(NULL),
                                  consumer_(0),
                                  producer_(0)
{
}

template <typename T>
ObFixedQueue<T>::~ObFixedQueue()
{
  destroy();
}

template <typename T>
int ObFixedQueue<T>::init(const int64_t max_num, ObIAllocator *allocator, const lib::ObLabel &label)
{
  lib::ObMemAttr attr;
  attr.label_ = label;
  return init(max_num, allocator, attr);
}

template <typename T>
int ObFixedQueue<T>::init(const int64_t max_num, ObIAllocator *allocator, const lib::ObMemAttr &attr)
{
  int ret = common::OB_SUCCESS;
  if (NULL == allocator || 0 >= max_num) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (NULL == (array_ = static_cast<ArrayItem *>(allocator->alloc(
                                                            sizeof(ArrayItem) * max_num, attr)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset(array_, 0, sizeof(ArrayItem) * max_num);
    max_num_ = max_num;
    allocator_ = allocator;
    consumer_ = 0;
    producer_ = 0;
    is_inited_ = true;
  }
  return ret;
}

template <typename T>
int ObFixedQueue<T>::init(const int64_t max_num, char *buf)
{
  int ret = common::OB_SUCCESS;
  if (NULL == buf || 0 >= max_num) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else {
    array_ = reinterpret_cast<ArrayItem *>(buf);
    memset(array_, 0, sizeof(ArrayItem) * max_num);
    max_num_ = max_num;
    allocator_ = NULL;
    consumer_ = 0;
    producer_ = 0;
    is_inited_ = true;
  }
  return ret;
}

template <typename T>
void ObFixedQueue<T>::destroy()
{
  if (is_inited_) {
    if (NULL != allocator_) {
      allocator_->free(array_);
      array_ = NULL;
    }
    array_ = NULL;
    max_num_ = 0;
    consumer_ = 0;
    producer_ = 0;
    allocator_ = NULL;
    is_inited_ = false;
  }
}

template <typename T>
inline int64_t ObFixedQueue<T>::get_total() const
{
  return get_total_(consumer_, producer_);
}

template <typename T>
inline int64_t ObFixedQueue<T>::get_curr_total() const
{
  return get_total_(ATOMIC_LOAD(&consumer_), ATOMIC_LOAD(&producer_));
}

template <typename T>
inline int64_t ObFixedQueue<T>::get_free() const
{
  return get_free_(consumer_, producer_);
}

template <typename T>
inline int64_t ObFixedQueue<T>::get_total_(const uint64_t consumer, const uint64_t producer) const
{
  return (producer - consumer);
}

template <typename T>
inline int64_t ObFixedQueue<T>::get_free_(const uint64_t consumer, const uint64_t producer) const
{
  return max_num_ - get_total_(consumer, producer);
}

template <typename T>
int ObFixedQueue<T>::push(T *ptr)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else if (NULL == ptr) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    uint64_t push = ATOMIC_LOAD(&producer_);
    uint64_t push_limit = ATOMIC_LOAD(&consumer_) + max_num_;
    uint64_t old_push = 0;
    while (((old_push = push) < push_limit || push < (push_limit = ATOMIC_LOAD(&consumer_) + max_num_))
           && old_push != (push = ATOMIC_CAS(&producer_, old_push, old_push + 1))) {
      PAUSE();
    }
    if (push < push_limit) {
      T **pdata = &array_[push % max_num_].data;
      while (NULL != ATOMIC_CAS(pdata, NULL, ptr)) {
        PAUSE();
      }
    } else {
      ret = common::OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

template <typename T>
int ObFixedQueue<T>::pop(T *&ptr)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else {
    uint64_t pop = ATOMIC_LOAD(&consumer_);
    uint64_t pop_limit = ATOMIC_LOAD(&producer_);
    uint64_t old_pop = 0;
    while (((old_pop = pop) < pop_limit || pop < (pop_limit = ATOMIC_LOAD(&producer_)))
           && old_pop != (pop = ATOMIC_CAS(&consumer_, old_pop, old_pop + 1))) {
      PAUSE();
    }
    if (pop < pop_limit) {
      T **pdata = &array_[(pop % max_num_)].data;
      while (NULL == (ptr = static_cast<T *>(ATOMIC_SET(pdata, NULL)))) {
        PAUSE();
      }
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

//not thread safe
template <typename T>
int ObFixedQueue<T>::head_unsafe(T *&ptr)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else {
    uint64_t head = ATOMIC_LOAD(&consumer_);
    uint64_t head_limit = ATOMIC_LOAD(&producer_);

    if (head < head_limit) {
      ptr = array_[(head % max_num_)].data;
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}
template <typename T>
class ObOrderedFixedQueue
{
public:
  ObOrderedFixedQueue() : is_inited_(false),
                                  max_num_(0),
                                  array_(NULL),
                                  allocator_(NULL),
                                  consumer_(0),
                                  producer_(0)
  {}
  ~ObOrderedFixedQueue()
  {
    destroy();
  }
public:
  int init(const int64_t max_num,
           ObIAllocator *allocator = global_default_allocator,
           const lib::ObLabel &label = ObModIds::OB_FIXED_QUEUE);
  int init(const int64_t max_num, ObIAllocator *allocator, const lib::ObMemAttr &attr);
  void destroy();
public:
  int push(T *ptr);
  int pop(T *&ptr);
  inline int64_t get_total() const
  {
    return producer_ - consumer_;
  }
  bool is_inited() const {return is_inited_;};
  int64_t capacity()const { return max_num_; }
private:
  struct ArrayItem
  {
    int64_t idx_;
    T *data_;
  };
private:
  bool is_inited_;
  int64_t max_num_;
  ArrayItem *array_;
  ObIAllocator *allocator_;
  uint64_t consumer_ CACHE_ALIGNED;
  uint64_t producer_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOrderedFixedQueue);
};
template <typename T>
int ObOrderedFixedQueue<T>::init(const int64_t max_num, ObIAllocator *allocator, const lib::ObLabel &label)
{
  lib::ObMemAttr attr;
  attr.label_ = label;
  return init(max_num, allocator, attr);
}
template <typename T>
int ObOrderedFixedQueue<T>::init(const int64_t max_num, ObIAllocator *allocator, const lib::ObMemAttr &attr)
{
  int ret = common::OB_SUCCESS;
  if (NULL == allocator || 0 >= max_num) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (NULL == (array_ = static_cast<ArrayItem *>(allocator->alloc(
                                                            sizeof(ArrayItem) * max_num, attr)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int i = 0; i < max_num; i++) {
      array_[i].data_ = NULL;
      array_[i].idx_ = i;
    }
    max_num_ = max_num;
    allocator_ = allocator;
    consumer_ = 0;
    producer_ = 0;
    is_inited_ = true;
  }
  return ret;
}
template <typename T>
void ObOrderedFixedQueue<T>::destroy()
{
  if (is_inited_) {
    if (NULL != allocator_) {
      allocator_->free(array_);
      array_ = NULL;
    }
    array_ = NULL;
    max_num_ = 0;
    consumer_ = 0;
    producer_ = 0;
    allocator_ = NULL;
    is_inited_ = false;
  }
}
template <typename T>
int ObOrderedFixedQueue<T>::push(T *ptr)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else if (NULL == ptr) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    uint64_t push = ATOMIC_LOAD(&producer_);
    uint64_t push_limit = ATOMIC_LOAD(&consumer_) + max_num_;
    uint64_t old_push = 0;
    while (((old_push = push) < push_limit || push < (push_limit = ATOMIC_LOAD(&consumer_) + max_num_))
           && old_push != (push = ATOMIC_CAS(&producer_, old_push, old_push + 1))) {
      PAUSE();
    }
    if (push < push_limit) {
      while (push != ATOMIC_LOAD(&array_[push % max_num_].idx_)) {
        PAUSE(); // ensure that only one push thread holding the array slot
      }
      T **pdata = &array_[push % max_num_].data_;
      while (NULL != ATOMIC_CAS(pdata, NULL, ptr)) {
        PAUSE();
      }
    } else {
      ret = common::OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

template <typename T>
int ObOrderedFixedQueue<T>::pop(T *&ptr)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else {
    uint64_t pop = ATOMIC_LOAD(&consumer_);
    uint64_t pop_limit = ATOMIC_LOAD(&producer_);
    uint64_t old_pop = 0;
    while (((old_pop = pop) < pop_limit || pop < (pop_limit = ATOMIC_LOAD(&producer_)))
           && old_pop != (pop = ATOMIC_CAS(&consumer_, old_pop, old_pop + 1))) {
      PAUSE();
    }
    if (pop < pop_limit) {
      while (pop != ATOMIC_LOAD(&array_[pop % max_num_].idx_)) {
        PAUSE(); // ensure that only one pop thread holding the array slot
      }
      T **pdata = &array_[(pop % max_num_)].data_;
      while (NULL == (ptr = static_cast<T *>(ATOMIC_TAS(pdata, NULL)))) {
        PAUSE();
      }
      ATOMIC_AAF(&array_[(pop % max_num_)].idx_, max_num_);
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
#endif //OCEANBASE_COMMON_FIXED_QUEUE_
