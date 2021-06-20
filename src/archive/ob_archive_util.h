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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_UTILS_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_UTILS_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_partition_key.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase {
namespace archive {
template <class Type>
struct SimpleSortList {
  Type* head_;
  Type* tail_;
  int64_t num_;

  SimpleSortList() : head_(NULL), tail_(NULL), num_(0)
  {}

  ~SimpleSortList()
  {
    reset();
  }

  void reset()
  {
    head_ = NULL;
    tail_ = NULL;
    num_ = 0;
  }

  // First locate pre one
  // Then insert after the pre one
  int insert(Type* node, Type* location)
  {
    int ret = common::OB_SUCCESS;

    if (NULL == node) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (NULL == location) {
      if (NULL == head_) {
        head_ = node;
        tail_ = node;
        node->set_next(NULL);
        num_ = 1;
      } else {
        node->set_next(head_);
        head_ = node;
        num_++;
      }
    } else {
      node->set_next(location->get_next());
      location->set_next(node);
      num_++;
    }

    return ret;
  }

  Type* pop()
  {
    Type* task = NULL;
    if (NULL == head_) {
      // do nothing
    } else {
      task = head_;
      head_ = head_->get_next();
      num_--;
    }

    return task;
  }

  TO_STRING_KV(KP_(head), KP_(tail), K_(num));
};

// simultaneous unsafe
struct SimpleQueue {
  common::ObLink* head_;
  common::ObLink* tail_;

public:
  SimpleQueue() : head_(NULL), tail_(NULL)
  {}
  ~SimpleQueue()
  {
    head_ = NULL;
    tail_ = NULL;
  }

public:
  bool is_empty()
  {
    return NULL == head_ && NULL == tail_;
  }

  int push(common::ObLink* p)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(p)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
      p->next_ = NULL;
      head_ = p;
      tail_ = p;
    } else {
      common::ObLink* tmp = tail_;
      tail_ = p;
      tmp->next_ = tail_;
      tail_->next_ = NULL;
    }
    return ret;
  }

  int pop(common::ObLink*& p)
  {
    int ret = common::OB_SUCCESS;
    p = NULL;
    if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
    } else if (head_ == tail_) {
      p = head_;
      head_ = NULL;
      tail_ = NULL;
    } else {
      p = head_;
      head_ = head_->next_;
    }
    return ret;
  }

  int top(common::ObLink*& p)
  {
    int ret = common::OB_SUCCESS;
    p = NULL;
    if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
    } else {
      p = head_;
    }
    return ret;
  }
};

void* ob_archive_malloc(const int64_t nbyte);

void ob_archive_free(void* ptr);

int check_is_leader(const common::ObPGKey& pg_key, const int64_t epoch, bool& is_leader);
bool is_valid_archive_compressor_type(const common::ObCompressorType compressor_type);
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_UTILS_H_ */
