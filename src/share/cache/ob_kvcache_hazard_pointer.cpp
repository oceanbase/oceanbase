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

#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kvcache_hazard_pointer.h"
#include "share/cache/ob_kvcache_hazard_domain.h"

namespace oceanbase {
namespace common {

bool HazardPointer::protect(ObKVMemBlockHandle* mb_handle, int32_t seq_num)
{
  bool b_ret = true;
  if (seq_num != mb_handle->get_seq_num()) {
    b_ret = false;
  } else {
    reset_protect(mb_handle);
    MEM_BARRIER();
    if (seq_num != ATOMIC_LOAD_RLX(&mb_handle->seq_num_)) {
      release();
      b_ret = false;
    }
  }
  return b_ret;
}

bool HazardPointer::protect(ObKVMemBlockHandle* mb_handle)
{
  bool b_ret = true;
  if (ObKVMBHandleStatus::FREE == mb_handle->get_status()) {
    b_ret = false;
  } else {
    reset_protect(mb_handle);
    MEM_BARRIER();
    if (ObKVMBHandleStatus::FREE == ATOMIC_LOAD_RLX(&mb_handle->status_)) {
      release();
      b_ret = false;
    }
  }
  return b_ret;
}

ObKVMemBlockHandle* HazardPointer::get_mb_handle() const
{
  return mb_handle_;
}

void HazardPointer::reset_protect(ObKVMemBlockHandle* mb_handle)
{
  ObKVMemBlockHandle** addr = &this->mb_handle_;
  ATOMIC_STORE_RLX(addr, mb_handle);
}

void HazardPointer::release()
{
  ObKVMemBlockHandle** addr = &this->mb_handle_;
  ATOMIC_STORE_RLX(addr, nullptr);
}

HazardPointer* HazardPointer::get_next() const
{
  return next_;
}

HazardPointer* HazardPointer::get_next_atomic() const
{
  HazardPointer* const* addr = &next_;
  return ATOMIC_LOAD_RLX(addr);
}

void HazardPointer::set_next(HazardPointer* next)
{
  next_ = next;
}
 
void HazardPointer::set_next_atomic(HazardPointer* next)
{
  HazardPointer** addr = &next_;
  ATOMIC_STORE_RLX(&this->next_, next);
}

ObMemAttr SharedHazptr::attr_ = SET_USE_500("shr_hazptr");

int SharedHazptr::make(HazardPointer& hazptr, SharedHazptr& shared_hazptr)
{
  INIT_SUCC(ret);
  shared_hazptr.reset();
  if (OB_ISNULL(shared_hazptr.ctrl_ptr_ =
                           (typeof(shared_hazptr.ctrl_ptr_))ob_malloc(sizeof(*shared_hazptr.ctrl_ptr_), attr_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate memory for ctrl_ptr");
  } else {
    new (shared_hazptr.ctrl_ptr_) ControlPointer(hazptr);
  }
  return ret;
}

SharedHazptr::SharedHazptr(const SharedHazptr& other)
{
  *this = other;
}

SharedHazptr::~SharedHazptr()
{
  reset();
}

SharedHazptr& SharedHazptr::operator=(const SharedHazptr& other)
{
  reset();
  ctrl_ptr_ = other.ctrl_ptr_;
  ATOMIC_INC(&ctrl_ptr_->refcnt_);
  return *this;
}

void SharedHazptr::move_from(SharedHazptr& other)
{
  reset();
  ctrl_ptr_ = other.ctrl_ptr_;
  other.ctrl_ptr_ = nullptr;
}

void SharedHazptr::reset()
{
  if (nullptr != ctrl_ptr_) {
    uint64_t refcnt = ATOMIC_SAF(&ctrl_ptr_->refcnt_, 1);
    if (0 == refcnt) {
      ctrl_ptr_->~ControlPointer();
      ob_free(ctrl_ptr_);
    }
    ctrl_ptr_ = nullptr;
  }
}

ObKVMemBlockHandle* SharedHazptr::get_mb_handle() const
{
  ObKVMemBlockHandle* mb_handle = nullptr;
  if (nullptr != ctrl_ptr_) {
    mb_handle = ctrl_ptr_->hazptr_->get_mb_handle();
  }
  return mb_handle;
}

SharedHazptr::ControlPointer::~ControlPointer()
{
  hazptr_->release();
  HazptrTLCache::get_instance().release_hazptr(hazptr_);
}

};  // namespace common
};  // namespace oceanbase