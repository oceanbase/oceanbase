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

#ifndef OB_TABLE_LOAD_HANDLE_H_
#define OB_TABLE_LOAD_HANDLE_H_

#include "lib/allocator/ob_malloc.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace table
{

template<class T>
class ObTableLoadHandle
{
  class Object
  {
  public:
    template<class... Args>
    Object(Args... args) : ref_count_(0), object_(args...) {}
  public:
    int64_t ref_count_;
    T object_;
  };

public:
  ObTableLoadHandle() : ptr_(nullptr) {}
  virtual ~ObTableLoadHandle() {
    reset();
  }

  template<class... Args >
  static ObTableLoadHandle make_handle(Args... args)
  {
    ObMemAttr attr(MTL_ID(), "TLD_Handle");
    ObTableLoadHandle handle;
    handle.ptr_ = OB_NEW(Object, attr, args...);
    handle.ptr_->ref_count_ = 1;
    return handle;
  }

  ObTableLoadHandle(const ObTableLoadHandle &other) : ptr_(nullptr) {
    *this = other;
  }

  ObTableLoadHandle(ObTableLoadHandle &&other) : ptr_(nullptr) {
    if (this != &other) {
      reset();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
  }

  void operator = (const ObTableLoadHandle &other) {
    if (this != &other) {
      reset();
      ptr_ = other.ptr_;
      if (ptr_ != nullptr) {
        ATOMIC_AAF(&(ptr_->ref_count_), 1);
      }
    }
  }

  operator bool() const {
    return ptr_ != nullptr;
  }

  T *operator->() const {
    return &(ptr_->object_);
  }

  T &operator*() const {
    return ptr_->object_;
  }

  void reset() {
    if (ptr_ != nullptr) {
      int64_t ref_count = ATOMIC_AAF(&(ptr_->ref_count_), -1);
      if (ref_count == 0) {
        ptr_->~Object();
        ob_free(ptr_);
      }
      ptr_ = nullptr;
    }
  }

private:
  // data members
  Object *ptr_;
};

}
}

#endif /* OB_TABLE_LOAD_HANDLE_H_ */
