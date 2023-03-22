// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#ifndef OB_TABLE_LOAD_HANDLE_H_
#define OB_TABLE_LOAD_HANDLE_H_

#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace table
{

template<class T, class... Args>
class ObTableLoadHandle
{
  class Object
  {
  public:
    Object(Args... args) : ref_count_(0), object_(args...) {}
  public:
    int32_t ref_count_;
    T object_;
  };

public:
  ObTableLoadHandle() : ptr_(nullptr) {}
  virtual ~ObTableLoadHandle() {
    int32_t ref_count = ATOMIC_AAF(&(ptr_->ref_count_), -1);
    if (ref_count == 0) {
      if (ptr_ != nullptr) {
        ptr_->~Object();
        ob_free(ptr_);
      }
    }
  }

  static ObTableLoadHandle make_handle(Args... args)
  {
    ObTableLoadHandle handle;
    handle.ptr_ = OB_NEW(Object, "TLD_handle", args...);
    handle.ptr_->ref_count_ = 1;
    return handle;
  }

  ObTableLoadHandle(ObTableLoadHandle &other) {
    ptr_ = other.ptr_;
    ATOMIC_AAF(&(ptr_->ref_count_), 1);
  }

  ObTableLoadHandle(ObTableLoadHandle &&other) {
    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }

  void operator= (ObTableLoadHandle &other) {
    ptr_ = other.ptr_;
    ATOMIC_AAF(&(ptr_->ref_count_), 1);
  }

  operator bool() {
    return ptr_ != nullptr;
  }

  T *operator->() {
    return &(ptr_->object_);
  }

  T &operator*() {
    return ptr_->object_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadHandle);

private:
  // data members
  Object *ptr_;
};

}
}

#endif /* OB_TABLE_LOAD_HANDLE_H_ */
