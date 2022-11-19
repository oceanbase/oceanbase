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
 *
 * LightyList
 */

#ifndef OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__
#define OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__

#include "share/ob_define.h"

namespace oceanbase
{
namespace libobcdc
{

template <class Type>
struct LightyList
{
  Type *head_;
  Type *tail_;
  int64_t num_;

  LightyList() : head_(NULL), tail_(NULL), num_(0)
  {}

  ~LightyList() { reset(); }

  void reset()
  {
    head_ = NULL;
    tail_ = NULL;
    num_ = 0;
  }

  int add(Type *node)
  {
    int ret = common::OB_SUCCESS;

    if (NULL == node) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (NULL == head_) {
      head_ = node;
      tail_ = node;
      node->set_next(NULL);
      num_ = 1;
    } else {
      tail_->set_next(node);
      tail_ = node;
      node->set_next(NULL);
      num_++;
    }
    return ret;
  }

  TO_STRING_KV(KP_(head), KP_(tail), K_(num));
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__ */
