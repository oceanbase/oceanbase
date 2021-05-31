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
#include "lib/allocator/ob_mem_leak_checker.h"

using namespace oceanbase::lib;
namespace oceanbase {
namespace common {
void* ObAllocator::alloc(const int64_t size, const ObMemAttr& attr)
{
  UNUSED(attr);
  void* ptr = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    BACKTRACE(WARN, !attr.label_.is_valid(), "[OB_MOD_DO_NOT_USE_ME ALLOC]size:%ld", size);
    ObMemAttr inner_attr = attr_;
    if (attr.label_.is_valid()) {
      inner_attr.label_ = attr.label_;
    }
    AObject* obj = os_.alloc_object(size, inner_attr);
    if (NULL != obj) {
      ptr = obj->data_;
      get_mem_leak_checker().on_alloc(*obj);
    }
    if (OB_UNLIKELY(nullptr == obj) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ObTenantCtxAllocator& ta = os_.get_block_mgr()->get_tenant_ctx_allocator();
      _OB_LOG(WARN, "[OOPS] alloc failed reason: %s", alloc_failed_msg());
      _OB_LOG(WARN,
          "oops, alloc failed, tenant_id=%ld, ctx_id=%ld, ctx_name=%s, ctx_hold=%ld, "
          "ctx_limit=%ld, tenant_hold=%ld, tenant_limit=%ld",
          inner_attr.tenant_id_,
          inner_attr.ctx_id_,
          common::get_global_ctx_info().get_ctx_name(inner_attr.ctx_id_),
          ta.get_hold(),
          ta.get_limit(),
          ta.get_tenant_hold(),
          ta.get_tenant_limit());
      // 49 is the user defined signal to dump memory
      raise(49);
    }
  }
  return ptr;
}

void ObAllocator::free(void* ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    if (OB_LIKELY(nullptr != ptr)) {
      AObject* obj = reinterpret_cast<AObject*>((char*)ptr - lib::AOBJECT_HEADER_SIZE);
      abort_unless(NULL != obj);
      abort_unless(obj->MAGIC_CODE_ == lib::AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == lib::BIG_AOBJECT_MAGIC_CODE);
      abort_unless(obj->in_use_);

      get_mem_leak_checker().on_free(*obj);
      lib::ABlock* block = obj->block();
      abort_unless(block);
      abort_unless(block->check_magic_code());
      ObjectSet* os = block->obj_set_;
      abort_unless(os == &os_);
      os->free_object(obj);
    }
  }
}

void* ObParallelAllocator::alloc(const int64_t size, const ObMemAttr& attr)
{
  void* ptr = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    lib::ObMutexGuard guard(mutex_);
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

void ObParallelAllocator::free(void* ptr)
{
  if (OB_LIKELY(nullptr != ptr)) {
    AObject* obj = reinterpret_cast<AObject*>((char*)ptr - lib::AOBJECT_HEADER_SIZE);
    abort_unless(obj);
    abort_unless(obj->MAGIC_CODE_ == lib::AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == lib::BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);

    get_mem_leak_checker().on_free(*obj);
    lib::ABlock* block = obj->block();
    abort_unless(block);
    abort_unless(block->check_magic_code());
    ObjectSet* os = block->obj_set_;
    // The locking process is driven by obj_set
    os->free_object(obj);
  }
}

}  // end of namespace common
}  // end of namespace oceanbase
