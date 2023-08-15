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

#pragma once

#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace table
{

template <class T>
class ObTableLoadIterator
{
  typedef ObTableLoadIterator<T> self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef int64_t difference_type;
  typedef T value_type;
  typedef T *value_ptr_t;
  typedef T *pointer;
  typedef T &reference;
public:
  ObTableLoadIterator() : value_ptr_(nullptr) {}
  explicit ObTableLoadIterator(value_ptr_t value_ptr) { value_ptr_ = value_ptr; }
  ObTableLoadIterator(const self_t &other) { *this = other; }
  ObTableLoadIterator &operator=(const self_t &other)
  {
    value_ptr_ = other.value_ptr_;
    return *this;
  }
public:
  reference operator*() const { return *value_ptr_; }
  value_ptr_t operator->() const { return value_ptr_; }
  operator value_ptr_t() const { return value_ptr_; }
  bool operator==(const self_t &other) const { return (value_ptr_ == (other.value_ptr_)); }
  bool operator!=(const self_t &other) const { return (value_ptr_ != (other.value_ptr_)); }
  bool operator<(const self_t &other) const { return (value_ptr_ < (other.value_ptr_)); }
  difference_type operator-(const self_t &rhs) { return value_ptr_ - rhs.value_ptr_; }
  self_t operator-(difference_type step) { return self_t(value_ptr_ - step); }
  self_t operator+(difference_type step) { return self_t(value_ptr_ + step); }
  self_t &operator+=(difference_type step)
  {
    value_ptr_ += step;
    return *this;
  }
  self_t &operator-=(difference_type step)
  {
    value_ptr_ += step;
    return *this;
  }
  self_t &operator++()
  {
    value_ptr_++;
    return *this;
  }
  self_t operator++(int)
  {
    self_t tmp = *this;
    value_ptr_++;
    return tmp;
  }
  self_t &operator--()
  {
    value_ptr_--;
    return *this;
  }
  self_t operator--(int)
  {
    self_t tmp = *this;
    value_ptr_--;
    return tmp;
  }
private:
  value_ptr_t value_ptr_;
};

// 由于allocator不是ObTableLoadArray自有的, 所以这里不能通过引用计数释放成员
template <class T>
class ObTableLoadArray final
{
  OB_UNIS_VERSION(1);
public:
  typedef ObTableLoadIterator<T> iterator;
  typedef const ObTableLoadIterator<T> const_iterator;
  ObTableLoadArray() : count_(0), ptr_(nullptr), allocator_(nullptr), is_ref_(false) {}
  ~ObTableLoadArray() { reset(); }

  void reset()
  {
    if (nullptr != ptr_ && !is_ref_) {
      // for (int64_t i = 0; i < count_; ++i) {
      //   ptr_[i].~T();
      // }
      if (nullptr != allocator_) {
        allocator_->free(ptr_);
      }
    }
    count_ = 0;
    ptr_ = nullptr;
    // allocator_ = nullptr;
    is_ref_ = false;
  }

  int create(int64_t count, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(count <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret), K(count));
    } else {
      T *ptr = nullptr;
      if (OB_ISNULL(ptr = static_cast<T *>(allocator.alloc(sizeof(T) * count)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocate memory", KR(ret));
      } else {
        for (int64_t i = 0; i < count; ++i) {
          new (ptr + i) T();
        }
        reset();
        count_ = count;
        ptr_ = ptr;
        allocator_ = &allocator;
        is_ref_ = false;
      }
    }
    return ret;
  }

  int assign(T *ptr, int64_t count, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    reset();
    count_ = count;
    ptr_ = ptr;
    allocator_ = &allocator;
    is_ref_ = false;
    return ret;
  }

  int ref(T *ptr, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    reset();
    count_ = count;
    ptr_ = ptr;
    allocator_ = nullptr;
    is_ref_ = true;
    return ret;
  }

  // 转移所有权
  int move_to(ObTableLoadArray &dest)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(dest.assign(ptr_, count_, allocator_))) {
      OB_LOG(WARN, "fail to assign", KR(ret));
    } else {
      is_ref_ = true;
    }
    return ret;
  }

  // for deserialize
  void set_allocator(common::ObIAllocator &allocator)
  {
    allocator_ = &allocator;
  }

  int64_t count() const { return count_; }
  bool empty() const { return 0 == count_; }
  T *ptr() const { return ptr_; }
  T &at(int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return ptr_[idx];
  }
  const T &at(int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return ptr_[idx];
  }
  iterator begin() { return iterator(ptr_); }
  iterator end() { return iterator(ptr_ + count_); }
  const_iterator begin() const { return const_iterator(ptr_); }
  const_iterator end() const { return const_iterator(ptr_ + count_); }
  common::ObIAllocator *get_allocator() const { return allocator_; }

  // 获取一份引用
  ObTableLoadArray &operator=(const ObTableLoadArray &rhs)
  {
    reset();
    count_ = rhs.count_;
    ptr_ = rhs.ptr_;
    allocator_ = rhs.allocator_;
    is_ref_ = true;
    return *this;
  }

  T &operator[](int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return ptr_[idx];
  }
  const T &operator[](int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count_);
    return ptr_[idx];
  }

  // {"array":[xxx, xxx], "is_ref":true}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME("array");
    J_COLON();
    J_ARRAY_START();
    if (count_ > 0) {
      BUF_PRINTO(ptr_[0]);
    }
    for (int64_t i = 1; i < count_; ++i) {
      J_COMMA();
      BUF_PRINTO(ptr_[i]);
    }
    J_ARRAY_END();
    J_COMMA();
    J_KV("is_ref", is_ref_);
    J_OBJ_END();
    return pos;
  }

private:
  int64_t count_;
  T *ptr_;
  common::ObIAllocator *allocator_;
  bool is_ref_; // 是否是引用, 不参与序列化
};

template <class T>
int ObTableLoadArray<T>::serialize(SERIAL_PARAMS) const
{
  int ret = common::OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(ptr_, count_);
  return ret;
}

template <class T>
int ObTableLoadArray<T>::deserialize(DESERIAL_PARAMS)
{
  int ret = common::OB_SUCCESS;
  reset();
  OB_UNIS_DECODE(count_);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(allocator_)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "allocator is null", KR(ret));
    } else if (count_ > 0) {
      if (OB_FAIL(create(count_, *allocator_))) {
        OB_LOG(WARN, "fail to create", KR(ret));
      }
    }
  }
  OB_UNIS_DECODE_ARRAY(ptr_, count_);
  return ret;
}

template <class T>
int64_t ObTableLoadArray<T>::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(ptr_, count_);
  return len;
}

} // namespace table
} // namespace oceanbase
