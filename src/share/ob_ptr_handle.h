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
#ifndef OCEANBASE_SHARED_STORAGE_SHARE_OB_PTR_HANDLE_H_
#define OCEANBASE_SHARED_STORAGE_SHARE_OB_PTR_HANDLE_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace storage
{
/*
 * T must support 'inc_ref_count' & 'dec_ref_count'
 *
 * ObPtrHandle is used to decrease ptr's ref_cnt when destroyed.
 */
template <typename T>
struct ObPtrHandle {
public:
  T *ptr_;

  ObPtrHandle() : ptr_(nullptr) {}
  ObPtrHandle(const ObPtrHandle &other) : ptr_(nullptr)
  {
    if (other.is_valid()) {
      ptr_ = other.ptr_;
      ptr_->inc_ref_count();
    }
  }
  ~ObPtrHandle() { destroy(); }

  OB_INLINE bool is_valid() const { return nullptr != ptr_; }
  OB_INLINE T *get_ptr() const { return ptr_; }
  OB_INLINE T *operator()() const { return ptr_; }
  void destroy() { reset(); }

  TO_STRING_KV(KP_(ptr));

  void set_ptr(T *ptr)
  {
    if (nullptr != ptr && ptr_ != ptr) {
      ptr->inc_ref_count();
      reset();
      ptr_ = ptr;
    }
  }

  void reset()
  {
    if (nullptr != ptr_) {
      ptr_->dec_ref_count();
    }
    ptr_ = nullptr;
  }

  int assign(const ObPtrHandle<T> &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      if (OB_UNLIKELY(this->is_valid())) {
        ret = common::OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "can not assign a valid handle", KR(ret), KP_(ptr));
      } else if (OB_LIKELY(other.is_valid())) {
        ptr_ = other.ptr_;
        ptr_->inc_ref_count();
      }
    }
    return ret;
  }

  ObPtrHandle<T> &operator=(const ObPtrHandle<T> &other)
  {
    if (this != &other) {
      if (!this->is_valid() && other.is_valid()) {
        ptr_ = other.ptr_;
        ptr_->inc_ref_count();
      }
    }
    return *this;
  }
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARED_STORAGE_SHARE_OB_PTR_HANDLE_H_ */