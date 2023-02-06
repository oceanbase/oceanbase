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

#ifndef OCEANBASE_LIB_CONTAINER_IARRAY_
#define OCEANBASE_LIB_CONTAINER_IARRAY_
#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_wrap.h"

namespace oceanbase
{
namespace common
{
// this interface has three derived classes: ObArray, ObSEArray and Ob2DArray.
template <typename T>
class ObIArray : public ObIArrayWrap<T>
{
public:
  using ObIArrayWrap<T>::at;
  using ObIArrayWrap<T>::count;
  ObIArray() : ObIArrayWrap<T>() {}
  ObIArray(T *data, const int64_t count) : ObIArrayWrap<T>(data, count) {}

  virtual ~ObIArray() {}
  virtual int push_back(const T &obj) = 0;
  virtual void pop_back() = 0;
  virtual int pop_back(T &obj) = 0;
  virtual int remove(int64_t idx) = 0;

  virtual int at(int64_t idx, T &obj) const = 0;

  /// reset is the same as clear()
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual void destroy() = 0;
  virtual int reserve(int64_t capacity) = 0;
  virtual int assign(const ObIArray &other) = 0;
  virtual int prepare_allocate(int64_t capacity) = 0;
  virtual T *alloc_place_holder()
  { OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "Not supported"); return NULL; }

  virtual int64_t to_string(char* buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    int64_t N = count();
    for (int64_t index = 0; index < N - 1; ++index) {
      BUF_PRINTO(at(index));
      J_COMMA();
    }
    if (0 < N) {
      BUF_PRINTO(at(N - 1));
    }
    J_ARRAY_END();
    return pos;
  }
protected:
  using ObIArrayWrap<T>::data_;
  using ObIArrayWrap<T>::count_;
  DISALLOW_COPY_AND_ASSIGN(ObIArray);
};

template<typename T>
bool is_contain(const ObIArray<T> &arr, const T &item)
{
  bool bret = false;
  for (int64_t i = 0; i < arr.count(); ++i) {
    bret = (arr.at(i) == item);
    if (bret) {
      break;
    }
  }
  return bret;
}

template<typename T>
int get_difference(const ObIArray<T> &a, const ObIArray<T> &b, ObIArray<T> &dest)
{
  int ret = OB_SUCCESS;
  dest.reset();
  for (int64_t i = 0; OB_SUCCESS == ret && i < a.count(); i++) {
    if (!is_contain(b, a.at(i))) {
      ret = dest.push_back(a.at(i));
    }
  }
  return ret;
}

template<typename DstArrayT, typename SrcArrayT>
int append(DstArrayT &dst, const SrcArrayT &src)
{
  int ret = OB_SUCCESS;
  int64_t N = src.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = dst.push_back(src.at(i));
  } // end for
  return ret;
}

template<typename DstArrayT, typename SrcArrayT>
bool is_array_equal(DstArrayT &left, const SrcArrayT &right)
{
  int bret = true;

  if (&left == &right) {
    // same
  } else if (left.count() != right.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; bret && i < left.count(); ++i) {
      bret = left.at(i) == right.at(i);
    }
  }
  return bret;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_CONTAINER_IARRAY_
