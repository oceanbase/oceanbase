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

#ifndef OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#define OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#include <stdint.h>
#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{
template<class T>
class ObArrayHelper final : public ObIArray<T>
{
public:
  using ObIArray<T>::count;
  using ObIArray<T>::at;

  ObArrayHelper(int64_t capacity, T *base_address, int64_t index = 0)
      : ObIArray<T>(base_address, index), capacity_(capacity)
  {
  }

  ObArrayHelper()
      : ObIArray<T>(), capacity_(0)
  {
  }

  void init(int64_t capacity, T *base_address, int64_t index = 0)
  {
    capacity_ = capacity;
    data_ = base_address;
    count_ = index;
  }

  inline bool check_inner_stat() const
  {
    return (NULL != data_) && (count_ >= 0) && (count_ <= capacity_);
  }

  inline void extra_access_check(void) const override { OB_ASSERT(check_inner_stat()); }

  int push_back(const T &obj)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (OB_UNLIKELY(count_ == capacity_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LIB_LOG(WARN, "array is full", K_(count), K_(capacity), K(ret));
    } else if (OB_FAIL(copy_assign(data_[count_], obj))) {
      LIB_LOG(WARN, "assign failed", K(ret));
    } else {
      ++count_;
    }
    return ret;
  }

  void pop_back()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (count_ > 0) {
      --count_;
    } else {
      // do nothing
    }
  }

  int pop_back(T &obj)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (OB_UNLIKELY(count_ <= 0)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LIB_LOG(WARN, "array is empty", K_(count), K(ret));
    } else if (OB_FAIL(copy_assign(obj, data_[count_ - 1]))) {
      LIB_LOG(WARN, "assign failed", K(ret));
    } else {
      --count_;
    }
    return ret;
  }

  int remove(int64_t idx)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (OB_UNLIKELY(idx < 0 || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LIB_LOG(WARN, "idx out of array range", K(idx), K_(count), K(ret));
    } else {
      for (int64_t i = idx; OB_SUCC(ret) && i < count_ - 1; ++i) {
        if (OB_FAIL(copy_assign(data_[i], data_[i + 1]))) {
          LIB_LOG(WARN, "assign failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        --count_;
      }
    }
    return ret;
  }

  int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (OB_UNLIKELY(idx < 0 || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LIB_LOG(WARN, "idx out of array range", K(idx), K_(count), K(ret));
    } else if (OB_FAIL(copy_assign(obj, data_[idx]))) {
      LIB_LOG(WARN, "assign failed", K(ret));
    }
    return ret;
  }

  inline int64_t capacity() const { return capacity_; }
  inline T *get_base_address() const { return data_; }

  void reset()
  {
    data_ = NULL;
    count_ = 0;
    capacity_ = 0;
  }

  void clear() { count_ = 0; }
  inline void reuse() { count_ = 0; }
  void destroy() { reset(); }
  int reserve(int64_t capacity) { UNUSED(capacity); return OB_SUCCESS;}
  int prepare_allocate(int64_t capacity) { UNUSED(capacity); return OB_NOT_SUPPORTED; }
  int assign(const ObIArray<T> &other)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_INNER_STAT_ERROR;
      LIB_LOG(WARN, "inner stat error", K_(data), K_(count), K_(capacity), K(ret));
    } else if (this != &other) {
      reuse();
      if (OB_FAIL(append(*this, other))) {
        LIB_LOG(WARN, "append failed", K(ret));
      }
    }
    return ret;
  }
  ObArrayHelper &operator=(const ObArrayHelper &other)
  {
    data_ = other.data_;
    count_ = other.count_;
    capacity_ = other.capacity_;
    return *this;
  }

protected:
  using ObIArray<T>::data_;
  using ObIArray<T>::count_;

private:
  int64_t capacity_;
};


template<class T>
class ObArrayHelpers
{
public:
  ObArrayHelpers()
  {
    memset(arrs_, 0x00, sizeof(arrs_));
    arr_count_ = 0;
  }

  int add_array_helper(ObArrayHelper<T> &helper)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(arr_count_ >= MAX_ARR_COUNT)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      arrs_[arr_count_] = &helper;
      arr_count_ ++;
    }
    return ret;
  }

  T *at(const int64_t index)
  {
    const ObArrayHelpers *pthis = static_cast<const ObArrayHelpers *>(this);
    return const_cast<T *>(pthis->at(index));
  }

  const T *at(const int64_t index) const
  {
    int64_t counter = 0;
    T *res = NULL;
    if (OB_UNLIKELY(index < 0)) {
      res = NULL;
    } else {
      for (int64_t i = 0; i < arr_count_; i++) {
        if (index < counter + arrs_[i]->get_array_index()) {
          res = &arrs_[i]->at(index - counter);
          break;
        } else {
          counter += arrs_[i]->get_array_index();
        }
      }
    }
    return res;
  }

  void clear()
  {
    for (int64_t i = 0; i < arr_count_; i++) {
      arrs_[i]->clear();
    }
  }

  int64_t get_array_index()const
  {
    int64_t counter = 0;
    for (int64_t i = 0; i < arr_count_; i++) {
      counter += arrs_[i]->get_array_index();
    }
    return counter;
  }
private:
  static const int MAX_ARR_COUNT = 16;
  ObArrayHelper<T> *arrs_[MAX_ARR_COUNT];
  int64_t arr_count_;
};
}
}
#endif
