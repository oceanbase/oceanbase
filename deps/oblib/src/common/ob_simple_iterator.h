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

#ifndef OCEANBASE_COMMON_OB_SIMPLE_ITERATOR_
#define OCEANBASE_COMMON_OB_SIMPLE_ITERATOR_

#include <stdint.h>
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "common/ob_simple_iterator.h"

namespace oceanbase
{
namespace common
{
template <typename T, const char *LABEL, int64_t LOCAL_ARRAY_SIZE>
class ObSimpleIterator
{
public:
  ObSimpleIterator() : item_arr_(LABEL, OB_MALLOC_NORMAL_BLOCK_SIZE) { reset(); }
  ~ObSimpleIterator() {};
  void reset();

  int push(const T &item);
  int set_ready();
  bool is_ready() const { return is_ready_; }
  int get_next(T &item);
private:
  bool is_ready_;
  typename common::ObSEArray<T, LOCAL_ARRAY_SIZE> item_arr_;
  typename common::ObSEArray<T, LOCAL_ARRAY_SIZE>::iterator it_;
};

template <typename T, const char *LABEL, int64_t LOCAL_ARRAY_SIZE>
void ObSimpleIterator<T, LABEL, LOCAL_ARRAY_SIZE>::reset()
{
  is_ready_ = false;
  item_arr_.reset();
}

template <typename T, const char *LABEL, int64_t LOCAL_ARRAY_SIZE>
int ObSimpleIterator<T, LABEL, LOCAL_ARRAY_SIZE>::push(const T &item)
{
  int ret = OB_SUCCESS;

  if (is_ready_) {
    OB_LOG(WARN, "ObSimpleIterator already ready, cannot push element");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(item_arr_.push_back(item))) {
    OB_LOG(WARN, "item push back error", K(ret), K(item));
  } else {
    // do nothing
  }

  return ret;
}

template <typename T, const char *LABEL, int64_t LOCAL_ARRAY_SIZE>
int ObSimpleIterator<T, LABEL, LOCAL_ARRAY_SIZE>::set_ready()
{
  int ret = OB_SUCCESS;

  if (is_ready_) {
    OB_LOG(WARN, "ObSimpleIterator is already ready");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_ready_ = true;
    //First record the first element that needs to be traversed in preparation for the iterative operation
    it_ = item_arr_.begin();
  }

  return ret;
}

/*
 * 遍历的过程中，第一行返回的是item_arr_..begin()；
 * 接下来返回的是++it，这样有利于判断it是否到达尾部,避免数组越界的风险
 */
template <typename T, const char *LABEL, int64_t LOCAL_ARRAY_SIZE>
int ObSimpleIterator<T, LABEL, LOCAL_ARRAY_SIZE>::get_next(T &item)
{
  int ret = OB_SUCCESS;

  if (!is_ready_) {
    OB_LOG(WARN, "ObSimpleIterator is not ready");
    ret = OB_ERR_UNEXPECTED;
  } else if (item_arr_.end() == it_) {
//    OB_LOG(DEBUG, "array iterate end", "part_cnt", item_arr_.count(),
//        K_(item_arr));
    ret = OB_ITER_END;
  } else {
    item = *it_;
    it_++;
  }

  return ret;
}

} // common
} // oceanbase

#endif //OCEANBASE_COMMON_OB_SIMPLE_ITERATOR_
