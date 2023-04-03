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

#ifndef __OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
#define __OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
// 支持并发读写
template<typename T>
struct ObAtomicType
{
  volatile int64_t modify_version_;
  ObAtomicType(): modify_version_(0)
  {}
  ~ObAtomicType()
  {}
  int copy(T &that)
  {
    int err = OB_EAGAIN;
    while (OB_EAGAIN == err) {
      int64_t old_version = modify_version_;
      if (0 > old_version) {
        err = OB_EAGAIN;
      } else {
        that = *(T *)this;
        err = OB_SUCCESS;
      }
      if (OB_SUCCESS == err && old_version != modify_version_) {
        err = OB_EAGAIN;
      }
    }
    return err;
  }

  int set(T &that)
  {
    int err = OB_EAGAIN;
    while (OB_EAGAIN == err) {
      int64_t old_version = modify_version_;
      if (0 > old_version
          || !__sync_bool_compare_and_swap(&modify_version_, old_version, -1)) {
        err = OB_EAGAIN;
      } else {
        that.modify_version_ = -1;
        *(T *)this = that;
        err = OB_SUCCESS;
      }
      if (OB_SUCCESS == err
          && !__sync_bool_compare_and_swap(&modify_version_, -1, old_version + 1)) {
        err = OB_ERR_UNEXPECTED;
      }
    }
    return err;
  }
};

template<typename T>
struct AtomicWrapper: public common::ObAtomicType<AtomicWrapper<T> >
{
  AtomicWrapper(): t_() {}
  ~AtomicWrapper() {}
  int read(T &t)
  {
    int err = OB_SUCCESS;
    AtomicWrapper wrapper;
    if (OB_SUCCESS != (err = copy(wrapper)))
    {}
    else {
      t = wrapper.t_;
    }
    return err;
  }
  int write(const T &t)
  {
    AtomicWrapper wrapper;
    wrapper.t_ = t;
    return set(wrapper);
  }
  T t_;
};
}; // end namespace common
}; // end namespace oceanbase

#endif //__OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
