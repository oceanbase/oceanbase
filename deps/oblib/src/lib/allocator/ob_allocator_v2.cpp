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

#include "lib/allocator/ob_allocator_v2.h"
#include "lib/alloc/alloc_failed_reason.h"
#include "lib/alloc/memory_sanity.h"
#include "lib/allocator/ob_mem_leak_checker.h"

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{
void *ObAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  UNUSED(attr);
  void *ptr = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    ObMemAttr inner_attr = attr_;
    if (attr.label_.is_valid()) {
      inner_attr.label_ = attr.label_;
    }
    auto ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(inner_attr.tenant_id_,
                                                                                inner_attr.ctx_id_);
    if (OB_LIKELY(NULL != ta)) {
      ptr = ObTenantCtxAllocator::common_alloc(size, inner_attr, *(ta.ref_allocator()), os_);
    } else if (FORCE_MALLOC_FOR_ABSENT_TENANT()) {
      inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
      ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(inner_attr.tenant_id_,
                                                                            inner_attr.ctx_id_);
      if (NULL != ta) {
        ptr = ObTenantCtxAllocator::common_alloc(size, inner_attr, *(ta.ref_allocator()), nos_);
      }
    }
  }
  return ptr;
}

void ObAllocator::free(void *ptr)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  // directly free object instead of using tenant allocator.
  ObTenantCtxAllocator::common_free(ptr);
}

void *ObParallelAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  void *ptr = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    lib::ObLockGuard<decltype(mutex_)> guard(mutex_);
    if (!is_inited_) {
      ret = init();
    }
  }
  if (OB_SUCC(ret)) {
    bool found = false;
    const int64_t start = common::get_itid();
    for (int64_t i = 0; nullptr == ptr && i < sub_cnt_ && !found; i++) {
      int64_t idx = (start + i) % sub_cnt_;
      if (sub_allocators_[idx]->trylock()) {
        ptr = sub_allocators_[idx]->alloc(size, attr);
        sub_allocators_[idx]->unlock();
        found = true;
        break;
      }
    }
    if (!found && nullptr == ptr) {
      const int64_t idx = start % sub_cnt_;
      sub_allocators_[idx]->lock();
      ptr = sub_allocators_[idx]->alloc(size, attr);
      sub_allocators_[idx]->unlock();
    }
  }
  return ptr;
}

void ObParallelAllocator::free(void *ptr)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  if (OB_LIKELY(nullptr != ptr)) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - lib::AOBJECT_HEADER_SIZE);
    abort_unless(obj);
    abort_unless(obj->MAGIC_CODE_ == lib::AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == lib::BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);
    SANITY_POISON(obj->data_, obj->alloc_bytes_);

    get_mem_leak_checker().on_free(*obj);
    lib::ABlock *block = obj->block();
    abort_unless(block);
    abort_unless(block->is_valid());
    ObjectSet *os = block->obj_set_;
    // The locking process is driven by obj_set
    os->free_object(obj);
  }
}

} // end of namespace common
} // end of namespace oceanbase
