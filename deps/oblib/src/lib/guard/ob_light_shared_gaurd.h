/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_GUARD_OB_LIGHT_SHARED_GUARD_H
#define OCEANBASE_LIB_GUARD_OB_LIGHT_SHARED_GUARD_H
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_errno.h"
#include "ob_shared_guard.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
namespace guard
{
struct IData {
  virtual ~IData() {}
  virtual int64_t to_string(char *, const int64_t) const = 0;
};
struct LightControlBlock
{
  LightControlBlock(ObIAllocator &alloc) : ref_(1), alloc_(alloc), p_data_block_(nullptr) {}
  ~LightControlBlock() {
    OCCAM_LOG(DEBUG, "control block is dec to 0, execute destruct action", K(*this), K(lbt()));
    p_data_block_->~IData();
    p_data_block_ = nullptr;
    alloc_.free(this);
  }
  void inc_ref() { OB_ASSERT(ATOMIC_LOAD(&ref_) > 0); ATOMIC_AAF(&ref_, 1); }
  int64_t dec_ref() { return ATOMIC_AAF(&ref_, -1); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_); }
  TO_STRING_KV(K_(ref), KPC_(p_data_block));
  int64_t ref_;
  ObIAllocator &alloc_;
  IData *p_data_block_;
};
template <typename T>
struct LightDataBlock : public IData
{
  LightDataBlock() = default;
  virtual ~LightDataBlock() override {
    ((T *)(data_))->~T();
  }
  int64_t to_string(char *buf, const int64_t buf_len) const override {
    return ((T *)data_)->to_string(buf, buf_len);
  }
  char data_[sizeof(T)];
};
template <typename T>
struct LightCombinedBlock {
  LightCombinedBlock(ObIAllocator &alloc) : ctrl_block_(alloc) {
    ctrl_block_.p_data_block_ = &data_block_;
    new (&data_block_.data_) T();
    OB_ASSERT((void*)this == (void*)&ctrl_block_);
  }
  LightControlBlock ctrl_block_;
  LightDataBlock<T> data_block_;
};
}

template <typename T>
struct ObLightSharedPtr// RAII used
{
  ObLightSharedPtr() : ctrl_ptr_(nullptr), data_(nullptr) {}
  ObLightSharedPtr(const ObLightSharedPtr<T> &rhs) : ObLightSharedPtr() { (void)assign(rhs); }
  ObLightSharedPtr<T> &operator=(const ObLightSharedPtr<T> &rhs) {
    (void)assign(rhs);
    return *this;
  }
  template <typename IMPL, typename std::enable_if<std::is_base_of<T, IMPL>::value,
                                                   bool>::type = true>
  ObLightSharedPtr(const ObLightSharedPtr<IMPL> &rhs) : ObLightSharedPtr() { (void)assign(rhs); }
  template <typename IMPL, typename std::enable_if<std::is_base_of<T, IMPL>::value,
                                                   bool>::type = true>
  ObLightSharedPtr &operator=(const ObLightSharedPtr<IMPL> &rhs) {
    (void)assign(rhs);
    return *this;
  }
  template <typename IMPL, typename std::enable_if<std::is_base_of<T, IMPL>::value ||
                                                   std::is_same<T, IMPL>::value, bool>::type = true>
  int assign(const ObLightSharedPtr<IMPL> &rhs) {
    if ((void*)this != (void*)&rhs) {
      reset();
      if (OB_NOT_NULL(rhs.ctrl_ptr_)) {
        rhs.ctrl_ptr_->inc_ref();
        ctrl_ptr_ = rhs.ctrl_ptr_;
        data_ = static_cast<T*>(rhs.data_);
      }
    }
    return OB_SUCCESS;
  }
  ~ObLightSharedPtr() { reset(); }
  void reset() {
    if (OB_NOT_NULL(ctrl_ptr_)) {
      OCCAM_LOG(DEBUG, "ObLightSharedPtr destructed", KPC_(ctrl_ptr));
      if (0 == ctrl_ptr_->dec_ref()) {
        ctrl_ptr_->~LightControlBlock();
      }
      ctrl_ptr_ = nullptr;
      data_ = nullptr;
    }
  }
  bool operator==(const ObLightSharedPtr<T> &rhs) const { return ctrl_ptr_ == rhs.ctrl_ptr_; }
  int construct(ObIAllocator &alloc) {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(ctrl_ptr_)) {
      OCCAM_LOG(WARN, "ObLightSharedGuard is valid, need reset first", K(ret), K(lbt()));
      reset();
    }
    guard::LightCombinedBlock<T> *block_ptr = nullptr;
    if (OB_ISNULL(block_ptr = (guard::LightCombinedBlock<T> *)
        alloc.alloc(sizeof(guard::LightCombinedBlock<T>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(WARN, "alloc memory failed", K(ret), K(lbt()));
    } else {
      new (block_ptr) guard::LightCombinedBlock<T>(alloc);
      ctrl_ptr_ = &(block_ptr->ctrl_block_);
      data_ = (T*)block_ptr->data_block_.data_;
    }
    return ret;
  }
  int get_ref_cnt(int64_t &ref_cnt) const {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ctrl_ptr_)) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "get ref on not inited guard", K(ret), K(lbt()));
    } else {
      ref_cnt = ctrl_ptr_->get_ref();
    }
    return ret;
  }
  int sync_until_last() const {
    int ret = OB_SUCCESS;
    if (!is_valid()) {
      ret = OB_INVALID_DATA;
    } else {
      int64_t start_sync_time = ObClockGenerator::getClock();
      int64_t loop_times = 0;
      int64_t ref_cnt;
      while (1 != (ref_cnt = ctrl_ptr_->get_ref())) {
        if ((++loop_times % 100000) == 0) {
          OCCAM_LOG(WARN, "sync wait too long",
                    KTIME(start_sync_time), K(loop_times), K(ref_cnt), K(lbt()));
        }
        PAUSE();
      }
    }
    return ret;
  }
  T &operator*() const { return *data_; }
  T *operator->() const { return data_; }
  T *ptr() { return data_; }
  TO_STRING_KV(KPC_(ctrl_ptr));
  bool is_valid() const { return OB_NOT_NULL(ctrl_ptr_); }
  guard::LightControlBlock *ctrl_ptr_;
  T *data_;
};

}
}

#endif