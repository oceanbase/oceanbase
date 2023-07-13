// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

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
