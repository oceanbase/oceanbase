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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_QUEUE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_QUEUE_H_

#include "lib/queue/ob_link_queue.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileWriteCacheQueue
{
public:
  typedef common::ObSpLinkQueue::Link Link;

  ObTmpFileWriteCacheQueue() : is_inited_(false), size_(0), queue_() {}
  ~ObTmpFileWriteCacheQueue() { reset(); }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
    } else {
      size_ = 0;
      is_inited_ = true;
    }
    return ret;
  }

  void reset()
  {
    is_inited_ = false;
    size_ = 0;
  }

  template <typename T>
  int push(T *item)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(item)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(queue_.push(static_cast<Link *>(item)))) {
    } else {
      ATOMIC_INC(&size_);
    }
    return ret;
  }

  template <typename T>
  int pop(T *&item)
  {
    int ret = OB_SUCCESS;
    item = nullptr;
    Link *link = nullptr;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(queue_.pop(link))) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(link)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ATOMIC_DEC(&size_);
      item = static_cast<T *>(link);
    }
    return ret;
  }

  OB_INLINE int64_t size() const { return ATOMIC_LOAD(&size_); }
  OB_INLINE bool is_empty() const { return queue_.is_empty(); }

private:
  bool is_inited_;
  int64_t size_;
  common::ObSpLinkQueue queue_;

  DISALLOW_COPY_AND_ASSIGN(ObTmpFileWriteCacheQueue);
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
