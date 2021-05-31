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

#define USING_LOG_PREFIX LIB

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/rc/ob_rc.h"
#include "lib/rc/context.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

ObMallocAllocator* ObMallocAllocator::instance_ = NULL;

ObMallocAllocator::ObMallocAllocator() : locks_(), allocators_(), reserved_(0), urgent_(0)
{}

ObMallocAllocator::~ObMallocAllocator()
{}

int ObMallocAllocator::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObCtxIds::MAX_CTX_ID; i++) {
    if (OB_FAIL(create_tenant_ctx_allocator(OB_SYS_TENANT_ID, i))) {
      LOG_ERROR("create tenant allocator fail", K(ret), K(i));
    } else if (OB_FAIL(create_tenant_ctx_allocator(OB_SERVER_TENANT_ID, i))) {
      LOG_ERROR("create tenant allocator fail", K(ret), K(i));
    }
  }
  return ret;
}

void* ObMallocAllocator::alloc(const int64_t size)
{
  ObMemAttr attr;
  return alloc(size, attr);
}

void* ObMallocAllocator::alloc(const int64_t size, const oceanbase::lib::ObMemAttr& attr)
{
#if PERF_MODE
  UNUSED(_attr);
  return ::malloc(size);
#else
  int ret = E(EventTable::EN_4) OB_SUCCESS;
  void* ptr = NULL;
  ObIAllocator* allocator = NULL;
  oceanbase::lib::ObMemAttr inner_attr = attr;
  if (inner_attr.ctx_id_ == ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID ||
      inner_attr.ctx_id_ == ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID) {
    inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  if (OB_SUCCESS != ret) {
  } else if (OB_UNLIKELY(0 == inner_attr.tenant_id_) || OB_UNLIKELY(INT64_MAX == inner_attr.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(lbt()), K(inner_attr.tenant_id_), K(ret));
  } else if (OB_UNLIKELY(inner_attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = inner_attr.tenant_id_ % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
    if (!OB_ISNULL(allocator)) {
      ptr = allocator->alloc(size, inner_attr);
    }
  } else {
    allocator = allocators_[inner_attr.tenant_id_][inner_attr.ctx_id_];
    if (!OB_ISNULL(allocator)) {
      ptr = allocator->alloc(size, inner_attr);
    }
  }
  // If there isn't tenant allocator, create one than re-allocate.
  if (OB_SUCC(ret) && OB_ISNULL(ptr) && OB_ISNULL(allocator)) {
    if (OB_FAIL(create_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_))) {
      LOG_ERROR("create tenant allocator fail");
    } else {
      if (OB_UNLIKELY(inner_attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
        const int64_t slot = inner_attr.tenant_id_ % PRESERVED_TENANT_COUNT;
        obsys::CRLockGuard guard(locks_[slot]);
        allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
        if (NULL != allocator) {
          ptr = allocator->alloc(size, inner_attr);
        }
      } else {
        allocator = allocators_[inner_attr.tenant_id_][inner_attr.ctx_id_];
        if (!OB_ISNULL(allocator)) {
          ptr = allocator->alloc(size, inner_attr);
        }
      }
    }
  }

  if (OB_LIKELY(nullptr != ptr)) {
    AObject* obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    get_mem_leak_checker().on_alloc(*obj);
  }

  return ptr;
#endif  // PERF_MODE
}

void* ObMallocAllocator::realloc(const void* ptr, const int64_t size, const oceanbase::lib::ObMemAttr& attr)
{
#if PERF_MODE
  UNUSED(attr);
  return ::realloc(const_cast<void*>(ptr), size);
#else
  // Won't create tenant allocator!!
  void* nptr = NULL;
  int ret = E(EventTable::EN_4) OB_SUCCESS;
  if (NULL != ptr) {
    AObject* obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(NULL != obj);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);
    get_mem_leak_checker().on_free(*obj);
  }
  oceanbase::lib::ObMemAttr inner_attr = attr;
  if (inner_attr.ctx_id_ == ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID ||
      inner_attr.ctx_id_ == ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID) {
    inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  if (OB_SUCCESS != ret && NULL == ptr) {
    // do nothing
  } else if (OB_UNLIKELY(inner_attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = inner_attr.tenant_id_ % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObIAllocator* allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
    if (NULL != allocator) {
      nptr = allocator->realloc(ptr, size, inner_attr);
    }
  } else {
    ObIAllocator* allocator = allocators_[inner_attr.tenant_id_][inner_attr.ctx_id_];
    if (NULL != allocator) {
      nptr = allocator->realloc(ptr, size, inner_attr);
    }
  }
  if (NULL != nptr) {
    AObject* obj = reinterpret_cast<AObject*>((char*)nptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    get_mem_leak_checker().on_alloc(*obj);
  }
  return nptr;
  ;
#endif  // PERF_MODE
}

void ObMallocAllocator::free(void* ptr)
{
#if PERF_MODE
  ::free(ptr);
#else
  // directly free object instead of using tenant allocator.
  if (NULL != ptr) {
    AObject* obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(NULL != obj);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);

    get_mem_leak_checker().on_free(*obj);
    ABlock* block = obj->block();
    abort_unless(block);
    abort_unless(block->check_magic_code());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    ObjectSet* set = block->obj_set_;
    set->free_object(obj);
  }
#endif  // PERF_MODE
}

ObTenantCtxAllocator* ObMallocAllocator::get_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id) const
{
  ObTenantCtxAllocator* allocator = NULL;
  if (tenant_id < PRESERVED_TENANT_COUNT) {
    allocator = allocators_[tenant_id][ctx_id];
  } else {
    // TODO: lock slot
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    ObTenantCtxAllocator* const* cur = &allocators_[slot][ctx_id];
    while (NULL != *cur && (*cur)->get_tenant_id() < tenant_id) {
      cur = &(*cur)->get_next();
    }
    if (NULL != *cur && (*cur)->get_tenant_id() == tenant_id) {
      allocator = *cur;
    }
  }
  return allocator;
}

int ObMallocAllocator::create_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(lbt()), K(tenant_id), K(ret));
  } else if (tenant_id < PRESERVED_TENANT_COUNT) {
    if (NULL == allocators_[tenant_id][ctx_id]) {
      ObMemAttr attr;
      ObTenantCtxAllocator* allocer = get_tenant_ctx_allocator(tenant_id, 0);
      if (nullptr == allocer) {
        allocer = allocators_[OB_SERVER_TENANT_ID][0];
      }
      attr.tenant_id_ = allocer->get_tenant_id();
      attr.label_ = ObModIds::OB_TENANT_CTX_ALLOCATOR;
      void* buf = allocer->alloc(sizeof(ObTenantCtxAllocator), attr);
      if (NULL != buf) {
        ObTenantCtxAllocator* allocator = new (buf) ObTenantCtxAllocator(tenant_id, ctx_id);
        // if tenant's root allocator not exist, create it
        if (NULL == allocators_[tenant_id][0] && 0 != ctx_id) {
          if (OB_FAIL(create_tenant_ctx_allocator(tenant_id, 0))) {
            LOG_ERROR("fail to create tenant allocator for head", K(lbt()), K(tenant_id), K(ret));
          }
        }

        // set tenant memory mgr
        if (OB_SUCC(ret)) {
          if (OB_FAIL(allocator->set_tenant_memory_mgr())) {
            LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
          }
        }

        bool cas_succeed = false;
        if (OB_SUCC(ret)) {
          cas_succeed = ATOMIC_BCAS(&allocators_[tenant_id][ctx_id], NULL, allocator);
        }

        if (OB_FAIL(ret) || !cas_succeed) {
          allocator->~ObTenantCtxAllocator();
          allocer->free(buf);
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
  } else {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    if (NULL == allocators_[slot][0]) {
      ret = create_tenant_ctx_allocator(slot, ctx_id);
    }
    if (OB_SUCC(ret)) {
      obsys::CWLockGuard guard(locks_[slot]);
      ObTenantCtxAllocator** cur = &allocators_[slot][ctx_id];
      while ((NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
        cur = &((*cur)->get_next());
      }
      if (NULL != cur) {
        if (NULL == (*cur) || (*cur)->get_tenant_id() > tenant_id) {
          ObMemAttr attr;
          ObTenantCtxAllocator* allocer = get_tenant_ctx_allocator(tenant_id, 0);
          if (nullptr == allocer) {
            allocer = allocators_[OB_SERVER_TENANT_ID][0];
          }
          attr.tenant_id_ = allocer->get_tenant_id();
          attr.label_ = ObModIds::OB_TENANT_CTX_ALLOCATOR;
          void* buf = allocer->alloc(sizeof(ObTenantCtxAllocator), attr);
          if (NULL != buf) {
            ObTenantCtxAllocator* allocator = new (buf) ObTenantCtxAllocator(tenant_id, ctx_id);
            ObTenantCtxAllocator* ta = (0 == ctx_id ? allocator : get_tenant_ctx_allocator(tenant_id, 0));
            if (NULL == ta) {
              if (OB_FAIL(create_tenant_ctx_allocator(tenant_id, 0))) {
                LOG_ERROR("fail to create tenant allocator for head", K(lbt()), K(tenant_id), K(ret));
              } else {
                ta = get_tenant_ctx_allocator(tenant_id, 0);
                if (NULL == ta) {
                  LOG_ERROR("get created allcator but is NULL", K(tenant_id), K(ret));
                }
              }
            }

            // set tenant memory mgr
            if (OB_SUCC(ret)) {
              if (OB_FAIL(allocator->set_tenant_memory_mgr())) {
                LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ObTenantCtxAllocator* next_allocator = *cur;
              *cur = allocator;
              ((*cur)->get_next()) = next_allocator;
            } else {
              allocator->~ObTenantCtxAllocator();
              allocer->free(buf);
            }
          } else {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
      }
    }
  }

  return ret;
}

int ObMallocAllocator::delete_tenant_ctx_allocator(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  return ret;
}

int ObMallocAllocator::set_root_allocator(ObTenantCtxAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!OB_ISNULL(allocators_[OB_SERVER_TENANT_ID][0])) {
    ret = OB_ENTRY_EXIST;
  } else {
    allocators_[OB_SERVER_TENANT_ID][0] = allocator;
  }
  return ret;
}

ObMallocAllocator* ObMallocAllocator::get_instance()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ObMallocAllocator::instance_)) {
    ObMallocAllocator* malloc_allocator = NULL;
    ObTenantCtxAllocator* allocator = new (std::nothrow) ObTenantCtxAllocator(OB_SERVER_TENANT_ID);
    if (!OB_ISNULL(allocator)) {
      if (OB_FAIL(allocator->set_tenant_memory_mgr())) {
        LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
      } else {
        ObMemAttr attr;
        attr.tenant_id_ = OB_SERVER_TENANT_ID;
        attr.label_ = ObModIds::OB_TENANT_CTX_ALLOCATOR;
        void* buf = allocator->alloc(sizeof(ObMallocAllocator), attr);
        if (!OB_ISNULL(buf)) {
          malloc_allocator = new (buf) ObMallocAllocator();
          ret = malloc_allocator->set_root_allocator(allocator);
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }

    if (OB_FAIL(ret)) {
      if (!OB_ISNULL(malloc_allocator)) {
        malloc_allocator->~ObMallocAllocator();
        allocator->free(malloc_allocator);
        malloc_allocator = NULL;
      }
      if (!OB_ISNULL(allocator)) {
        delete allocator;
        allocator = NULL;
      }
    } else {
      ObMallocAllocator::instance_ = malloc_allocator;
    }
  }
  return ObMallocAllocator::instance_;
}

int ObMallocAllocator::with_resource_handle_invoke(uint64_t tenant_id, InvokeFunc func)
{
  int ret = OB_SUCCESS;
  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id, resource_handle))) {
    LIB_LOG(ERROR, "get_tenant_resource_mgr failed", K(ret), K(tenant_id));
  } else if (!resource_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "resource_handle is invalid", K(tenant_id));
  } else {
    ret = func(resource_handle.get_memory_mgr());
  }
  return ret;
}

int ObMallocAllocator::set_tenant_limit(uint64_t tenant_id, int64_t bytes)
{
  return with_resource_handle_invoke(tenant_id, [bytes](ObTenantMemoryMgr* mgr) {
    mgr->set_limit(bytes);
    return OB_SUCCESS;
  });
}

int64_t ObMallocAllocator::get_tenant_limit(uint64_t tenant_id)
{
  int64_t limit = 0;
  with_resource_handle_invoke(tenant_id, [&limit](ObTenantMemoryMgr* mgr) {
    limit = mgr->get_limit();
    return OB_SUCCESS;
  });
  return limit;
}

int64_t ObMallocAllocator::get_tenant_hold(uint64_t tenant_id)
{
  int64_t hold = 0;
  with_resource_handle_invoke(tenant_id, [&hold](ObTenantMemoryMgr* mgr) {
    hold = mgr->get_sum_hold();
    return OB_SUCCESS;
  });
  return hold;
}

int64_t ObMallocAllocator::get_tenant_rpc_hold(uint64_t tenant_id)
{
  int64_t rpc_hold = 0;
  with_resource_handle_invoke(tenant_id, [&rpc_hold](ObTenantMemoryMgr* mgr) {
    rpc_hold = mgr->get_rpc_hold();
    return OB_SUCCESS;
  });
  return rpc_hold;
}

int64_t ObMallocAllocator::get_tenant_ctx_hold(const uint64_t tenant_id, const uint64_t ctx_id) const
{
  int64_t hold = 0;
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObTenantCtxAllocator* allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (!OB_ISNULL(allocator)) {
      hold = allocator->get_hold();
    }
  } else {
    ObTenantCtxAllocator* allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (!OB_ISNULL(allocator)) {
      hold = allocator->get_hold();
    }
  }
  return hold;
}

void ObMallocAllocator::get_tenant_mod_usage(uint64_t tenant_id, int mod_id, ObModItem& item) const
{
  ObTenantCtxAllocator* allocator = NULL;
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
      allocator = get_tenant_ctx_allocator(tenant_id, i);
      if (!OB_ISNULL(allocator)) {
        item += allocator->get_mod_usage(mod_id);
      }
    }
  } else {
    for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
      allocator = allocators_[tenant_id][i];
      if (!OB_ISNULL(allocator)) {
        item += allocator->get_mod_usage(mod_id);
      }
    }
  }
}

void ObMallocAllocator::print_tenant_ctx_memory_usage(uint64_t tenant_id) const
{
  ObTenantCtxAllocator* allocator = NULL;
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
    allocator = get_tenant_ctx_allocator(tenant_id, i);
    if (OB_LIKELY(NULL != allocator)) {
      allocator->print_memory_usage();
    }
  }
}

void ObMallocAllocator::print_tenant_memory_usage(uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr* mgr) {
    CREATE_WITH_TEMP_CONTEXT(ContextParam().set_label(ObModIds::OB_TEMP_VARIABLES))
    {
      static const int64_t BUFLEN = 1 << 17;
      char* buf = (char*)ctxalp(BUFLEN);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "no memory", K(ret));
      } else {
        int64_t ctx_pos = 0;
        const volatile int64_t* ctx_hold_bytes = mgr->get_ctx_hold_bytes();
        for (uint64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
          if (ctx_hold_bytes[i] > 0) {
            ret = databuff_printf(buf,
                BUFLEN,
                ctx_pos,
                "[MEMORY] ctx_id=%25s hold_bytes=%'15ld\n",
                get_global_ctx_info().get_ctx_name(i),
                ctx_hold_bytes[i]);
          }
        }
        if (OB_SUCC(ret)) {
          ObPageManagerCenter::get_instance().print_tenant_stat(tenant_id, buf, BUFLEN, ctx_pos);
        }

        _LOG_INFO("[MEMORY] tenant: %lu, limit: %'lu hold: %'lu rpc_hold: %'lu cache_hold: %'lu "
                  "cache_used: %'lu cache_item_count: %'lu \n%s",
            tenant_id,
            mgr->get_limit(),
            mgr->get_sum_hold(),
            mgr->get_rpc_hold(),
            mgr->get_cache_hold(),
            mgr->get_cache_hold(),
            mgr->get_cache_item_count(),
            buf);
      }
    }
    return ret;
  });
  UNUSED(ret);
}

IBlockMgr* ObMallocAllocator::get_tenant_ctx_block_mgr(uint64_t tenant_id, uint64_t ctx_id)
{
  IBlockMgr* blk_mgr = nullptr;
  ObTenantCtxAllocator* allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
  if (OB_UNLIKELY(allocator != nullptr)) {
    blk_mgr = &allocator->get_block_mgr();
  }
  return blk_mgr;
}

void ObMallocAllocator::set_urgent(int64_t bytes)
{
  CHUNK_MGR.set_urgent(bytes);
}

int64_t ObMallocAllocator::get_urgent() const
{
  return CHUNK_MGR.get_urgent();
}

void ObMallocAllocator::set_reserved(int64_t bytes)
{
  reserved_ = bytes;
}

int64_t ObMallocAllocator::get_reserved() const
{
  return reserved_;
}

int ObMallocAllocator::set_tenant_ctx_idle(
    const uint64_t tenant_id, const uint64_t ctx_id, const int64_t size, const bool reserve /*=false*/)
{
  int ret = OB_SUCCESS;
  ObTenantCtxAllocator* allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
  if (NULL == allocator) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant or ctx not exist", K(ret), K(tenant_id), K(ctx_id));
  } else {
    allocator->set_idle(size, reserve);
  }
  return ret;
}

int ObMallocAllocator::get_chunks(AChunk** chunks, int cap, int& cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t slot = 0; OB_SUCC(ret) && slot < PRESERVED_TENANT_COUNT; ++slot) {
    obsys::CRLockGuard guard(locks_[slot]);
    for (int64_t ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ObTenantCtxAllocator* ta = allocators_[slot][ctx_id];
      while (OB_SUCC(ret) && ta != nullptr) {
        ta->get_chunks(chunks, cap, cnt);
        if (cnt >= cap) {
          ret = OB_SIZE_OVERFLOW;
        }
        ta = ta->get_next();
      }
    }
  }
  return ret;
}
