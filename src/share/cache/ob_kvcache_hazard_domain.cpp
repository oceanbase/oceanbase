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

#include "share/cache/ob_kvcache_hazard_domain.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "share/cache/ob_kvcache_hazard_pointer.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace common {

HazptrTLCache::HazptrTLCache(): HazptrList()
{}

HazptrTLCache::~HazptrTLCache()
{
  flush();
}

int HazptrTLCache::acquire_hazptr(HazardPointer*& hazptr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(get_size() > 0)) {
  } else if (OB_FAIL(HazardDomain::get_instance().acquire_hazptrs(*this, CACHE_ALLOC_BATCH_SIZE))) {
    COMMON_LOG(WARN, "failed to acquire hazard pointer", K(ret));
  }
  if (OB_SUCC(ret)) {
    hazptr = pop();
  } else {
    hazptr = nullptr;
  }
  return ret;
}

void HazptrTLCache::release_hazptr(HazardPointer* hazptr)
{
  push_front(hazptr);
  if (OB_UNLIKELY(size_ > CACHE_SIZE)) {
    // size_ == CACHE_SIZE + 1
    HazardDomain::get_instance().choose_and_release_hazptrs(*this, CACHE_FREE_BATCH_SIZE);
  }
}

HazptrTLCache& HazptrTLCache::get_instance() {
  thread_local HazptrTLCache tl_cache;
  return tl_cache;
}

void HazptrTLCache::flush()
{
  HazardDomain::get_instance().release_hazptrs(*this);
}

HazardDomain::HazardDomain()
    : bit_set_attr_(OB_SERVER_TENANT_ID, "CACHE_BITMAP"),
      hazptr_attr_(OB_SERVER_TENANT_ID, "CACHE_HAZPTR"),
      hazptr_allocator_(hazptr_attr_),
      bit_set_(),
      bit_set_mu_(),
      retire_list_(nullptr),
      retired_memory_size_(0)
{
  lib::DoNotUseMe(bit_set_attr_);
  lib::DoNotUseMe(hazptr_attr_);
  bit_set_.set_attr(bit_set_attr_);
}

int HazardDomain::init(int64_t mb_handle_num)
{
  int ret = OB_SUCCESS;
  if (!GCONF._enable_kvcache_hazard_pointer) {
  } else if (OB_FAIL(bit_set_.reserve(mb_handle_num))) {
    COMMON_LOG(WARN, "failed to reserve bits in bit_set_");
  }
  return ret;
}

void HazardDomain::reset_retire_list()
{
  retire_list_ = nullptr;
  retired_memory_size_ = 0;
}

int HazardDomain::acquire_hazptrs(HazptrList& list, int64_t num)
{
  return hazptr_allocator_.alloc(list, num);
}

void HazardDomain::release_hazptrs(HazptrList& list)
{
  hazptr_allocator_.free_slow(list);
}

void HazardDomain::choose_and_release_hazptrs(HazptrList& list, int64_t num_to_release)
{
  hazptr_allocator_.choose_and_free(list, num_to_release);
}

int HazardDomain::retire(ObKVMemBlockHandle* mb_handle)
{
  return retire(&mb_handle->retire_link_, &mb_handle->retire_link_, mb_handle->mem_block_->get_hold_size());
}

int HazardDomain::retire(ObLink* head, ObLink* tail, uint64_t retire_size)
{
  ObLink* retire_list = ATOMIC_LOAD_RLX(&retire_list_);
  int64_t* retired_size = &CONTAINER_OF(head, ObKVMemBlockHandle, retire_link_)->inst_->status_.retired_size_;
  do {
    tail->next_ = retire_list;
  } while (tail->next_ != (retire_list = ATOMIC_VCAS(&retire_list_, retire_list, head)));
  ATOMIC_FAA(&retired_memory_size_, retire_size);
  ATOMIC_FAA(retired_size, retire_size);
  return OB_SUCCESS;
}

HazardDomain& HazardDomain::get_instance()
{
  return ObKVGlobalCache::get_instance().get_hazard_domain();
}

void HazardDomain::print_info() const
{
  int ret = OB_SUCCESS;
  char buf[256];
  hazptr_allocator_.print_info(buf, 256);
  _OB_LOG(INFO, "[KVCACHE-HAZARD-POINTER] retired memory size: %ld, %s", ATOMIC_LOAD_RLX(&retired_memory_size_), buf);
}

void HazardDomain::wash()
{
  hazptr_allocator_.wash();
}

HazptrHolder::HazptrHolder() : hazptr_(nullptr) {}

HazptrHolder::~HazptrHolder()
{
  reset();
}

#define DISPATCH_HAZPTR_REFCNT(func, ...)                \
  if (OB_LIKELY(GCONF._enable_kvcache_hazard_pointer)) { \
    return hazptr_##func(__VA_ARGS__);                   \
  } else {                                               \
    return refcnt_##func(__VA_ARGS__);                   \
  }

int HazptrHolder::protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num)
{
  DISPATCH_HAZPTR_REFCNT(protect, success, mb_handle, seq_num);
}

int HazptrHolder::protect(bool& success, ObKVMemBlockHandle* mb_handle)
{
  DISPATCH_HAZPTR_REFCNT(protect, success, mb_handle);
}

void HazptrHolder::release() {
  DISPATCH_HAZPTR_REFCNT(release);
}

ObKVMemBlockHandle* HazptrHolder::get_mb_handle() const
{
  DISPATCH_HAZPTR_REFCNT(get_mb_handle);
}

bool HazptrHolder::is_valid() const
{
  return get_mb_handle() != nullptr;
}

void HazptrHolder::reset()
{
  DISPATCH_HAZPTR_REFCNT(reset);
}

void HazptrHolder::move_from(HazptrHolder& other)
{
  reset();
  this->hazptr_ = other.hazptr_;
  other.hazptr_ = nullptr;
}

int HazptrHolder::assign(const HazptrHolder& other)
{
  DISPATCH_HAZPTR_REFCNT(assign, other);
}

int HazptrHolder::hazptr_protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num)
{
  int ret = OB_SUCCESS;
  success = false;
  if (OB_ISNULL(mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", KP(mb_handle));
  } else if (mb_handle->get_seq_num() != seq_num) {
  } else if (OB_ISNULL(hazptr_) && OB_FAIL(HazptrTLCache::get_instance().acquire_hazptr(hazptr_))) {
    COMMON_LOG(WARN, "failed to allocate hazard pointer");
  } else if (OB_UNLIKELY(nullptr != hazptr_->get_mb_handle())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "release is not called!");
  } else {
    success = hazptr_->protect(mb_handle, seq_num);
  }
  return ret;
}

int HazptrHolder::hazptr_protect(bool& success, ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  success = false;
  if (OB_ISNULL(mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", KP(mb_handle));
  } else if (FREE == mb_handle->get_status()) {
  } else if (OB_ISNULL(hazptr_) && OB_FAIL(HazptrTLCache::get_instance().acquire_hazptr(hazptr_))) {
    COMMON_LOG(WARN, "failed to allocate hazard pointer");
  } else if (OB_UNLIKELY(nullptr != hazptr_->get_mb_handle())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "release is not called!");
  } else {
    success = hazptr_->protect(mb_handle);
  }
  return ret;
}

void HazptrHolder::hazptr_release()
{
  if (OB_NOT_NULL(hazptr_)) {
    hazptr_->release();
  }
}

void HazptrHolder::hazptr_reset()
{
  if (OB_NOT_NULL(hazptr_)) {
    hazptr_->release();
    HazptrTLCache::get_instance().release_hazptr(hazptr_);
    hazptr_ = nullptr;
  }
}

ObKVMemBlockHandle* HazptrHolder::hazptr_get_mb_handle() const
{
  ObKVMemBlockHandle* mb_handle = nullptr;
  if (OB_NOT_NULL(hazptr_)) {
    mb_handle = hazptr_->get_mb_handle();
  }
  return mb_handle;
}

int HazptrHolder::hazptr_assign(const HazptrHolder& other)
{
  int ret = OB_SUCCESS;
  bool protect_success;
  release();
  if (!other.is_valid()) {
  } else if (OB_FAIL(hazptr_protect(protect_success, other.get_mb_handle()))) {
      COMMON_LOG(WARN, "failed to protect");
  } else if (!protect_success) {
    // has been retired
    ret = OB_EAGAIN;
  }
  return ret;
}

int HazptrHolder::refcnt_protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num)
{
  int ret = OB_SUCCESS;
  success = false;
  if (OB_ISNULL(mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", KP(mb_handle));
  } else if (OB_UNLIKELY(mb_handle_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "release is not called!");
  } else {
    if ((success = ObKVGlobalCache::get_instance().store_.add_handle_ref(mb_handle, seq_num))) {
      this->mb_handle_ = mb_handle;
    }
  }
  return ret;
}

int HazptrHolder::refcnt_protect(bool& success, ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  success = false;
  if (OB_ISNULL(mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", KP(mb_handle));
  } else if (OB_UNLIKELY(mb_handle_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "release is not called!");
  } else {
    refcnt_reset();
    if ((success = ObKVGlobalCache::get_instance().store_.add_handle_ref(mb_handle))) {
      this->mb_handle_ = mb_handle;
    }
  }
  return ret;
}

void HazptrHolder::refcnt_release()
{
  if (OB_NOT_NULL(mb_handle_)) {
    ObKVGlobalCache::get_instance().store_.de_handle_ref(mb_handle_);
    mb_handle_ = nullptr;
  }
}

void HazptrHolder::refcnt_reset()
{
  refcnt_release();
}

ObKVMemBlockHandle* HazptrHolder::refcnt_get_mb_handle() const
{
  return mb_handle_;
}

int HazptrHolder::refcnt_assign(const HazptrHolder& other)
{
  refcnt_reset();
  if (OB_LIKELY(this != &other) && OB_NOT_NULL(other.mb_handle_)) {
    ATOMIC_INC(&other.mb_handle_->ref_cnt_);
    mb_handle_ = other.mb_handle_;
  }
  return OB_SUCCESS;
}

};  // namespace common
};  // namespace oceanbase