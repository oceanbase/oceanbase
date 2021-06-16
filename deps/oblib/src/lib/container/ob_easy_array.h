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

#ifndef _OB_EASY_ARRAY_H
#define _OB_EASY_ARRAY_H

#include "lib/container/ob_array.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace common {
template <class T>
class EasyArray {
public:
  EasyArray();
  EasyArray(T element);

  // Overload () to support the use of EasyArray("name")("value")("info")
  EasyArray& operator()(T element);
  int at(int32_t index, T& element) const;
  const T& at(int32_t index) const;
  int64_t count() const;  // Number of column names

  inline int get_exec_status() const
  {
    return exec_status_;
  }

private:
  ObArray<T> array_;
  int exec_status_;
};

template <class T>
EasyArray<T>::EasyArray() : exec_status_(OB_SUCCESS)
{}

template <class T>
EasyArray<T>::EasyArray(T element)
{
  exec_status_ = OB_SUCCESS;

  if (OB_SUCCESS == exec_status_) {
    exec_status_ = array_.push_back(element);
    if (OB_SUCCESS != exec_status_) {
      _OB_LOG(WARN, "push back fail:exec_status_[%d]", exec_status_);
    }
  }
}

template <class T>
EasyArray<T>& EasyArray<T>::operator()(T element)
{
  if (OB_SUCCESS == exec_status_) {
    exec_status_ = array_.push_back(element);
    if (OB_SUCCESS != exec_status_) {
      _OB_LOG(WARN, "push back fail:exec_status_[%d]", exec_status_);
    }
  }
  return *this;
}

template <class T>
int EasyArray<T>::at(int32_t index, T& element) const
{
  int ret = OB_SUCCESS;
  ret = array_.at(index, element);
  return ret;
}

template <class T>
const T& EasyArray<T>::at(int32_t index) const
{
  return array_.at(index);
}

template <class T>
int64_t EasyArray<T>::count() const
{
  return array_.count();
}
}  // namespace common
}  // namespace oceanbase

#endif /* _OB_EASY_ARRAY_H */
