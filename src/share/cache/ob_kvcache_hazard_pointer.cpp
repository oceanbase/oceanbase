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
#define USING_LOG_PREFIX STORAGE

#include "share/ob_define.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kvcache_hazard_pointer.h"
#include "share/cache/ob_kvcache_store.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_force_print_log.h"

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

};  // namespace common
};  // namespace oceanbase