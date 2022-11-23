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
#include "lib/alloc/memory_sanity.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/rc/ob_rc.h"
#include "lib/rc/context.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

uint64_t ObMallocAllocator::max_used_tenant_id_ = 0;
bool ObMallocAllocator::is_inited_ = false;

ObMallocAllocator::ObMallocAllocator() : locks_(), allocators_(), reserved_(0), urgent_(0)
{
  set_root_allocator();
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObCtxIds::MAX_CTX_ID; i++) {
    if (OB_FAIL(create_tenant_ctx_allocator(OB_SYS_TENANT_ID, i))) {
      LOG_ERROR("create tenant allocator fail", K(ret), K(i));
    } else if (OB_FAIL(create_tenant_ctx_allocator(OB_SERVER_TENANT_ID, i))) {
      LOG_ERROR("create tenant allocator fail", K(ret), K(i));
    }
  }
  is_inited_ = true;
}

ObMallocAllocator::~ObMallocAllocator()
{
  is_inited_ = false;
}

void *ObMallocAllocator::alloc(const int64_t size)
{
  ObMemAttr attr;
  return alloc(size, attr);
}

void *ObMallocAllocator::alloc(const int64_t size, const oceanbase::lib::ObMemAttr &attr)
{
#ifdef OB_USE_ASAN
  UNUSED(attr);
  return ::malloc(size);
#else
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  int ret = E(EventTable::EN_4) OB_SUCCESS;
  void *ptr = NULL;
  ObIAllocator *allocator = NULL;
  oceanbase::lib::ObMemAttr inner_attr = attr;
  if (inner_attr.ctx_id_ == ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID
      || inner_attr.ctx_id_ == ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID) {
    inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  if (OB_SUCCESS != ret) {
  } else if (OB_UNLIKELY(0 == inner_attr.tenant_id_)
             || OB_UNLIKELY(INT64_MAX == inner_attr.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(inner_attr.tenant_id_), K(ret));
  } else if (OB_NOT_NULL(allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_))) {
    // do nothing
  } else if (OB_FAIL(create_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_))) {
    LOG_ERROR("create tenant allocator fail", K(ret));
  } else {
    allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
  }

  if (OB_SUCC(ret)) {
    ptr = allocator->alloc(size, inner_attr);
  }

  if (OB_NOT_NULL(ptr)) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    get_mem_leak_checker().on_alloc(*obj, inner_attr);
  }

  return ptr;
#endif
}

void *ObMallocAllocator::realloc(
  const void *ptr, const int64_t size, const oceanbase::lib::ObMemAttr &attr)
{
#ifdef OB_USE_ASAN
  UNUSED(attr);
  return ::realloc(const_cast<void *>(ptr), size);
#else
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  // Won't create tenant allocator!!
  void *nptr = NULL;
  int ret = E(EventTable::EN_4) OB_SUCCESS;
  if (NULL != ptr) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(NULL != obj);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);

    get_mem_leak_checker().on_free(*obj);
  }
  oceanbase::lib::ObMemAttr inner_attr = attr;
  ObIAllocator *allocator = nullptr;
  if (inner_attr.ctx_id_ == ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID
      || inner_attr.ctx_id_ == ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID) {
    inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  if (OB_FAIL(ret) && NULL == ptr) {
    // do nothing
  } else if (OB_ISNULL(allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_))) {
    // do nothing
  } else if (OB_ISNULL(nptr = allocator->realloc(ptr, size, inner_attr))) {
    // do nothing
  } else {
    AObject *obj = reinterpret_cast<AObject*>((char*)nptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    get_mem_leak_checker().on_alloc(*obj, inner_attr);
  }
  return nptr;;
#endif
}

void ObMallocAllocator::free(void *ptr)
{
#ifdef OB_USE_ASAN
  ::free(ptr);
#else
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  // directly free object instead of using tenant allocator.
  if (NULL != ptr) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(NULL != obj);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);
    SANITY_POISON(obj->data_, obj->alloc_bytes_);

    get_mem_leak_checker().on_free(*obj);
    ABlock *block = obj->block();
    abort_unless(block);
    abort_unless(block->is_valid());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    ObjectSet *set = block->obj_set_;
    set->free_object(obj);
  }
#endif // PERF_MODE
}

ObTenantCtxAllocator *ObMallocAllocator::get_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id) const
{
  ObTenantCtxAllocator *allocator = nullptr;
  if (OB_LIKELY(tenant_id < PRESERVED_TENANT_COUNT)) {
    allocator = allocators_[tenant_id][ctx_id];
  } else {
    // TODO: lock slot
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::ObRLockGuard guard(locks_[slot]);
    ObTenantCtxAllocator * const *cur = &allocators_[slot][ctx_id];
    while (NULL != *cur && (*cur)->get_tenant_id() < tenant_id) {
      cur = &(*cur)->get_next();
    }
    if (NULL != *cur && (*cur)->get_tenant_id() == tenant_id) {
      allocator = *cur;
    }
  }
  return allocator;
}

int ObMallocAllocator::create_tenant_ctx_allocator(
  uint64_t tenant_id,
  uint64_t ctx_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr;
  void *buf = nullptr;
  ObTenantCtxAllocator *allocator = nullptr;
  if (INT64_MAX == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KCSTRING(lbt()), K(tenant_id), K(ret));
  } else if (ObCtxIds::DEFAULT_CTX_ID == ctx_id) {
    // do nothing
  } else if (OB_ISNULL(get_tenant_ctx_allocator(tenant_id, ObCtxIds::DEFAULT_CTX_ID))) {
    ret = create_tenant_ctx_allocator(tenant_id, ObCtxIds::DEFAULT_CTX_ID);
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_NOT_NULL(get_tenant_ctx_allocator(tenant_id, ctx_id))) {
    // no need to create again, do nothing
  } else {
    if (tenant_id > max_used_tenant_id_) {
      UNUSED(ATOMIC_BCAS(&max_used_tenant_id_, max_used_tenant_id_, tenant_id));
    }
    auto *allocer = get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
    attr.tenant_id_ = allocer->get_tenant_id();
    attr.label_ = ObModIds::OB_TENANT_CTX_ALLOCATOR;
    if (OB_ISNULL(buf = allocer->alloc(sizeof(ObTenantCtxAllocator), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      bool cas_succeed = false;
      allocator = new (buf) ObTenantCtxAllocator(tenant_id, ctx_id);
      if (OB_FAIL(allocator->set_tenant_memory_mgr())) {
        LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
      } else if (tenant_id < PRESERVED_TENANT_COUNT) {
        cas_succeed = ATOMIC_BCAS(&allocators_[tenant_id][ctx_id], NULL, allocator);
      } else {
        const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
        obsys::ObWLockGuard guard(locks_[slot]);
        ObTenantCtxAllocator **cur = &allocators_[slot][ctx_id];
        while ((NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
          cur = &((*cur)->get_next());
        }
        if (OB_ISNULL(*cur)) {
          *cur = allocator;
          cas_succeed = true;
        } else if ((*cur)->get_tenant_id() > tenant_id) {
          ObTenantCtxAllocator *next_allocator = *cur;
          *cur = allocator;
          ((*cur)->get_next()) = next_allocator;
          cas_succeed = true;
        }
      }
      if (OB_FAIL(ret) || !cas_succeed) {
        allocator->~ObTenantCtxAllocator();
        allocer->free(buf);
      } else {
        LOG_INFO("tenant ctx allocator was created", K(tenant_id), K(ctx_id), KCSTRING(lbt()));
      }
    }
  }

  return ret;
}

void ObMallocAllocator::set_root_allocator()
{
  int ret = OB_SUCCESS;
  static ObTenantCtxAllocator allocator(OB_SERVER_TENANT_ID);
  if (OB_FAIL(allocator.set_tenant_memory_mgr())) {
    LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
  } else {
    allocators_[OB_SERVER_TENANT_ID][0] = &allocator;
  }
}

ObMallocAllocator *ObMallocAllocator::get_instance()
{
  static ObMallocAllocator instance;
  return &instance;
}

int ObMallocAllocator::with_resource_handle_invoke(uint64_t tenant_id, InvokeFunc func)
{
  int ret = OB_SUCCESS;
  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(
      tenant_id, resource_handle))) {
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
  return with_resource_handle_invoke(tenant_id, [bytes](ObTenantMemoryMgr *mgr) {
      mgr->set_limit(bytes);
      return OB_SUCCESS;
    });
}

int64_t ObMallocAllocator::get_tenant_limit(uint64_t tenant_id)
{
  int64_t limit = 0;
  with_resource_handle_invoke(tenant_id, [&limit](ObTenantMemoryMgr *mgr) {
      limit = mgr->get_limit();
      return OB_SUCCESS;
    });
  return limit;
}

int64_t ObMallocAllocator::get_tenant_hold(uint64_t tenant_id)
{
  int64_t hold = 0;
  with_resource_handle_invoke(tenant_id, [&hold](ObTenantMemoryMgr *mgr) {
      hold = mgr->get_sum_hold();
      return OB_SUCCESS;
    });
  return hold;
}

int64_t ObMallocAllocator::get_tenant_remain(uint64_t tenant_id)
{
  int64_t remain = 0;
  with_resource_handle_invoke(tenant_id, [&remain](ObTenantMemoryMgr *mgr) {
      remain = mgr->get_limit() - mgr->get_sum_hold() + mgr->get_cache_hold();
      return OB_SUCCESS;
    });
  return remain;
}

int64_t ObMallocAllocator::get_tenant_ctx_hold(const uint64_t tenant_id, const uint64_t ctx_id) const
{
  int64_t hold = 0;
  ObTenantCtxAllocator *allocator = nullptr;
  if (OB_ISNULL(allocator = get_tenant_ctx_allocator(tenant_id, ctx_id))) {
    // do nothing
  } else {
    hold = allocator->get_hold();
  }
  return hold;
}

void ObMallocAllocator::get_tenant_label_usage(
  uint64_t tenant_id, ObLabel &label, ObLabelItem &item) const
{
  ObTenantCtxAllocator *allocator = nullptr;
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
    if (OB_ISNULL(allocator = get_tenant_ctx_allocator(tenant_id, i))) {
      // do nothing
    } else {
      item += allocator->get_label_usage(label);
    }
  }
}

void ObMallocAllocator::print_tenant_ctx_memory_usage(uint64_t tenant_id) const
{
  ObTenantCtxAllocator *allocator = NULL;
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
  with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr *mgr) {
    CREATE_WITH_TEMP_CONTEXT(ContextParam().set_label(ObModIds::OB_TEMP_VARIABLES)) {
      static const int64_t BUFLEN = 1 << 17;
      char *buf = (char *)ctxalp(BUFLEN);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "no memory", K(ret));
      } else {
        int64_t ctx_pos = 0;
        const volatile int64_t *ctx_hold_bytes = mgr->get_ctx_hold_bytes();
        for (uint64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
          if (ctx_hold_bytes[i] > 0) {
            int64_t limit = 0;
            IGNORE_RETURN mgr->get_ctx_limit(i, limit);
            ret = databuff_printf(buf, BUFLEN, ctx_pos,
                "[MEMORY] ctx_id=%25s hold_bytes=%'15ld limit=%'26ld\n",
                get_global_ctx_info().get_ctx_name(i), ctx_hold_bytes[i], limit);
          }
        }
        if (OB_SUCC(ret)) {
          ObPageManagerCenter::get_instance().print_tenant_stat(tenant_id, buf, BUFLEN, ctx_pos);
        }
        buf[std::min(ctx_pos, BUFLEN - 1)] = '\0';

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

IBlockMgr *ObMallocAllocator::get_tenant_ctx_block_mgr(uint64_t tenant_id, uint64_t ctx_id)
{
  IBlockMgr *blk_mgr = nullptr;
  ObTenantCtxAllocator *allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
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

int ObMallocAllocator::set_tenant_ctx_idle(const uint64_t tenant_id,
                                           const uint64_t ctx_id,
                                           const int64_t size,
                                           const bool reserve /*=false*/)
{
  int ret = OB_SUCCESS;
  ObTenantCtxAllocator *allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
  if (NULL == allocator) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant or ctx not exist", K(ret), K(tenant_id), K(ctx_id));
  } else {
    allocator->set_idle(size, reserve);
  }
  return ret;
}

int ObMallocAllocator::get_chunks(AChunk **chunks, int cap, int &cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t slot = 0; OB_SUCC(ret) && slot < PRESERVED_TENANT_COUNT; ++slot) {
    obsys::ObRLockGuard guard(locks_[slot]);
    for (int64_t ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ObTenantCtxAllocator *ta = allocators_[slot][ctx_id];
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

int64_t ObMallocAllocator::sync_wash(uint64_t tenant_id, uint64_t from_ctx_id, int64_t wash_size)
{
  int64_t washed_size = 0;
  for (int64_t i = 0;
       washed_size < wash_size && i < ObCtxIds::MAX_CTX_ID;
       i++) {
    int64_t ctx_id = (from_ctx_id + i) % ObCtxIds::MAX_CTX_ID;
    ObTenantCtxAllocator *allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (NULL == allocator) {
      // do-nothing
    } else {
      washed_size += allocator->sync_wash(wash_size - washed_size);
    }
  }
  return washed_size;
}

int64_t ObMallocAllocator::sync_wash()
{
  int64_t washed_size = 0;
  for (uint64_t tenant_id = 1; tenant_id <= ObMallocAllocator::get_max_used_tenant_id(); ++tenant_id) {
    washed_size += sync_wash(tenant_id, 0, INT64_MAX);
  }
  return washed_size;
}
