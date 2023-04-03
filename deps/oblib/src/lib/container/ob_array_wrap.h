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

#ifndef _OB_ARRAY_WRAP_H
#define _OB_ARRAY_WRAP_H 1
#include <cstdint>
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
template<class T>
    int databuff_print_obj_array(char *buf, const int64_t buf_len, int64_t &pos,
                                 const T *obj, const int64_t size);  // from ob_print_utils.h
/// Utility tempalte class to adapt C array to an object, which can be used with databuff_print_obj
template <typename T>
class ObIArrayWrap
{
public:
  ObIArrayWrap() : data_(NULL), count_(0) {}
  ObIArrayWrap(T *data, const int64_t count) : data_(data), count_(count) {}

  virtual ~ObIArrayWrap() {}
  inline const T &at(int64_t idx) const
  {
#ifndef NDEBUG
    extra_access_check();
#endif
    OB_ASSERT(idx >= 0 && idx < count_);
    return data_[idx];
  }
  inline T &at(int64_t idx)
  {
#ifndef NDEBUG
    extra_access_check();
#endif
    OB_ASSERT(idx >= 0 && idx < count_);
    return data_[idx];
  }
  inline int64_t count() const { return count_; }
  inline bool empty() const { return 0 == count(); }

  // extra check for at() in DEBUG building.
  virtual void extra_access_check() const = 0;

  T *get_data() const { return data_; }
protected:
  // Expose %data_ and %count_ here to make at(), count() be inlined,
  // derived class should set then appropriatly
  T *data_;
  int64_t count_;
  DISALLOW_COPY_AND_ASSIGN(ObIArrayWrap);
};

template <typename T>
class ObArrayWrap final : public ObIArrayWrap<T>
{
public:
  using ObIArrayWrap<T>::count;
  using ObIArrayWrap<T>::at;

  ObArrayWrap()
  {
  }
  ObArrayWrap(const T *objs, const int64_t num)
      : ObIArrayWrap<T>(const_cast<T*>(objs), num)
  {
  }
  virtual ~ObArrayWrap() {}

  inline void reset()
  {
    data_ = NULL;
    count_ = 0;
  }

  int allocate_array(ObIAllocator &allocator, int64_t num)
  {
    int ret = OB_SUCCESS;
    void *ptr = NULL;
    int64_t size = num * sizeof(T);
    if (OB_UNLIKELY(num <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid argument", K(num), K(ret));
    } else if (OB_ISNULL(ptr = allocator.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "allocate memory failed", K(size), K(num));
    } else {
      data_ = new(ptr) T[num];
      count_ = num;
    }
    return ret;
  }

  virtual void extra_access_check() const override {}

  void release_array()
  {
    if (data_ != NULL) {
      for (int64_t i = 0; i < count_; ++i) {
        data_[i].~T();
      }
      count_ = 0;
      data_ = NULL;
    }
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (OB_FAIL(databuff_print_obj_array(buf, buf_len, pos, data_, count_))) {
    } else {}
    return pos;
  }

  ObArrayWrap &operator=(const ObArrayWrap<T> &other)
  {
    data_ = other.data_;
    count_ = other.count_;
    return *this;
  }

  int assign(const ObArrayWrap<T> &other)
  {
    int ret = OB_SUCCESS;
    data_ = other.data_;
    count_ = other.count_;
    return ret;
  }
protected:
  using ObIArrayWrap<T>::data_;
  using ObIArrayWrap<T>::count_;
};

}  // common
}  // oceanbase

#endif /* _OB_ARRAY_WRAP_H */
