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

#include "ob_malloc_allocator.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "common/ob_smart_var.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/alloc/ob_malloc_time_monitor.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

bool ObMallocAllocator::is_inited_ = false;

static bool g_malloc_v2_enabled = false;

void enable_malloc_v2(bool enable)
{
  g_malloc_v2_enabled = enable;
}
namespace oceanbase
{
namespace lib
{
__thread ObTenantCtxAllocator *tl_ta = NULL;

ObTLTaGuard::ObTLTaGuard()
  : restore_(false)
{}

ObTLTaGuard::ObTLTaGuard(const int64_t tenant_id)
  : restore_(false)
{
  switch_to(tenant_id);
}

ObTLTaGuard::~ObTLTaGuard()
{
  revert();
}

void ObTLTaGuard::switch_to(const int64_t tenant_id)
{
  revert();
  if (tl_ta != NULL && tl_ta->get_tenant_id() == tenant_id) {
    // do-nothing
  } else {
    ta_bak_ = tl_ta;
    ta_ = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_without_tlcache(
      tenant_id, ObCtxIds::DEFAULT_CTX_ID);
    tl_ta = ta_.ref_allocator();
    restore_ = true;
  }
}

void ObTLTaGuard::revert()
{
  if (restore_) {
    tl_ta = ta_bak_;
    ta_.revert();
    restore_ = false;
  }
}

ObMallocAllocator::ObMallocAllocator()
  : locks_(), allocators_(), unrecycled_lock_(false), unrecycled_allocators_(),
    reserved_(0), urgent_(0), max_used_tenant_id_(0), create_on_demand_(false)
{
  for (int64_t i = 0; i < PRESERVED_TENANT_COUNT; ++i) {
    locks_[i].enable_record_stat(false);
  }
  set_root_allocator();
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

void *ObMallocAllocator::alloc(const int64_t size, const oceanbase::lib::ObMemAttr &_attr)
{
#ifdef OB_USE_ASAN
  UNUSED(_attr);
  return ::malloc(size);
#else
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTenantCtxAllocatorGuard allocator = NULL;
  lib::ObMemAttr attr = _attr;
  if (OB_INVALID_TENANT_ID == attr.tenant_id_) {
    LOG_ERROR("invalid tenant id", K(attr.tenant_id_));
    attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  if (OB_SERVER_TENANT_ID == attr.tenant_id_ &&
      attr.use_500() && !attr.expect_500()) {
    attr.ctx_id_ = ObCtxIds::UNEXPECTED_IN_500;
  }
  lib::ObMemAttr inner_attr = attr;
  bool do_not_use_me = false;
  if (FORCE_EXPLICT_500_MALLOC()) {
    do_not_use_me = OB_SERVER_TENANT_ID == attr.tenant_id_ && !attr.use_500() &&
      ob_thread_tenant_id() != OB_SERVER_TENANT_ID;
    if (do_not_use_me) {
      inner_attr.tenant_id_ = ob_thread_tenant_id();
      inner_attr.ctx_id_ = ObCtxIds::DO_NOT_USE_ME;
    }
  }
  if (OB_SUCCESS != ret) {
  } else if (OB_UNLIKELY(0 == inner_attr.tenant_id_)
             || OB_UNLIKELY(INT64_MAX == inner_attr.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(inner_attr.tenant_id_), K(ret));
  } else if (OB_NOT_NULL(allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_))) {
    // do nothing
  } else if (inner_attr.tenant_id_ <= OB_USER_TENANT_ID || OB_UNLIKELY(create_on_demand_)) {
    if (OB_FAIL(create_and_add_tenant_allocator(inner_attr.tenant_id_))) {
      LOG_ERROR("create and add tenant allocator failed", K(ret), K(inner_attr.tenant_id_));
    } else {
      allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    tmp_ret = OB_ENTRY_NOT_EXIST;
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_ERROR("tenant allocator not exist", K(inner_attr.tenant_id_), K(inner_attr.ctx_id_),
              K(ret), K(tmp_ret));
  }

  if (OB_SUCC(ret)) {
    if (do_not_use_me) {
      ptr = allocator->alloc(size, inner_attr);
    }
    if (!ptr) {
      inner_attr = attr;
      allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
      if (OB_ISNULL(allocator)) {
      } else {
        ptr = allocator->alloc(size, inner_attr);
      }
    }
  } else if (OB_ENTRY_NOT_EXIST == ret && FORCE_MALLOC_FOR_ABSENT_TENANT()) {
    ret = OB_SUCCESS;
    LOG_INFO("force malloc when tenant allocator does not exist");
    inner_attr.tenant_id_ = OB_SERVER_TENANT_ID;
    allocator = get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
    if (OB_ISNULL(allocator)) {
    } else {
      ptr = allocator->alloc(size, inner_attr);
    }
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
  ObTenantCtxAllocatorGuard allocator = NULL;
  if (OB_ISNULL(allocator = get_tenant_ctx_allocator(attr.tenant_id_, attr.ctx_id_))) {
    // do nothing
  } else if (OB_ISNULL(nptr = allocator->realloc(ptr, size, attr))) {
    // do nothing
  }
  return nptr;
#endif
}

void ObMallocAllocator::free(void *ptr)
{
#ifdef OB_USE_ASAN
  ::free(ptr);
#else
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  // directly free object instead of using tenant allocator.
  ObTenantCtxAllocator::common_free(ptr);
#endif // OB_USE_ASAN
}

ObTenantCtxAllocatorGuard ObMallocAllocator::get_tenant_ctx_allocator_without_tlcache(
  uint64_t tenant_id, uint64_t ctx_id) const
{
  STATIC_ASSERT(OB_USER_TENANT_ID < PRESERVED_TENANT_COUNT, "preserved count is too small");
  if (tenant_id <= OB_USER_TENANT_ID) {
    const bool lock = false;
    if (OB_NOT_NULL(allocators_[tenant_id])) {
      return ObTenantCtxAllocatorGuard(&allocators_[tenant_id][ctx_id], lock);
    } else {
      return ObTenantCtxAllocatorGuard();
    }
  }
  const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  BucketRLockGuard guard(const_cast<BucketLock&>(locks_[slot]),
                         GETTID() % BucketLock::BUCKET_COUNT);
  if (OB_LIKELY(tenant_id < PRESERVED_TENANT_COUNT)) {
    if (OB_LIKELY(allocators_[slot] != NULL && allocators_[slot]->get_tenant_id() == tenant_id)) {
      return ObTenantCtxAllocatorGuard(&allocators_[slot][ctx_id]);
    } else {
      return ObTenantCtxAllocatorGuard();
    }
  }
  ObTenantCtxAllocator *allocator = nullptr;
  ObTenantCtxAllocator * const *cur = &allocators_[slot];
  while (NULL != *cur && (*cur)->get_tenant_id() < tenant_id) {
    cur = &(*cur)->get_next();
  }
  if (NULL != *cur && (*cur)->get_tenant_id() == tenant_id) {
    allocator = *cur;
  }
  if (OB_NOT_NULL(allocator)) {
    return ObTenantCtxAllocatorGuard(&allocator[ctx_id]);
  } else {
    return ObTenantCtxAllocatorGuard();
  }
}

ObTenantCtxAllocatorGuard ObMallocAllocator::get_tenant_ctx_allocator(uint64_t tenant_id,
                                                                      uint64_t ctx_id) const
{
  if (OB_LIKELY(tl_ta != NULL && tl_ta->get_tenant_id() == tenant_id)) {
    const bool lock = false;
    return ObTenantCtxAllocatorGuard(&tl_ta[ctx_id], lock);
  }
  return get_tenant_ctx_allocator_without_tlcache(tenant_id, ctx_id);
}

int ObMallocAllocator::create_and_add_tenant_allocator(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantCtxAllocator *allocator = NULL;
  if (OB_FAIL(create_tenant_allocator(tenant_id, allocator))) {
    LOG_ERROR("create tenant allocator failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(add_tenant_allocator(allocator))) {
    if (OB_ENTRY_EXIST == ret && tenant_id <= OB_USER_TENANT_ID) {
      LOG_INFO("tenant allocator already exists", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("add tenant allocator failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMallocAllocator::create_tenant_allocator(uint64_t tenant_id, ObTenantCtxAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = NULL;

  ObTenantCtxAllocator *unrecycled_allocator = take_off_tenant_allocator_unrecycled(tenant_id);
  if (unrecycled_allocator != NULL) {
    allocator = unrecycled_allocator;
  } else {
    auto allocer = get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
    void *buf = NULL;
    if (OB_ISNULL(allocer)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_ERROR("get root tenant allocator failed", K(ret));
    } else if (OB_ISNULL(buf = allocer->alloc(sizeof(ObTenantCtxAllocator) * ObCtxIds::MAX_CTX_ID,
        ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_TENANT_CTX_ALLOCATOR)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc tenant allocator failed", K(ret));
    } else if (OB_FAIL(create_tenant_allocator(tenant_id, buf, allocator))) {
      LOG_ERROR("create tenant allocator failed", K(ret), K(tenant_id));
    }
    if (OB_FAIL(ret) && buf != NULL) {
      allocer->free(buf);
      buf = NULL;
    }
  }
  return ret;
}

void ObMallocAllocator::destroy_tenant_allocator(ObTenantCtxAllocator *allocator)
{
  for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
    allocator[ctx_id].~ObTenantCtxAllocator();
  }
  auto allocer = get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
  if (allocer != NULL) {
    allocer->free(allocator);
  }
}

int ObMallocAllocator::create_tenant_allocator(uint64_t tenant_id, void *buf,
                                               ObTenantCtxAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = NULL;

  if (tenant_id > max_used_tenant_id_) {
    UNUSED(ATOMIC_BCAS(&max_used_tenant_id_, max_used_tenant_id_, tenant_id));
  }
  for (int ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
    auto *ctx_allocator = new ((char*)buf + ctx_id * sizeof(ObTenantCtxAllocator))
      ObTenantCtxAllocator(tenant_id, ctx_id);
    if (OB_FAIL(ctx_allocator->set_tenant_memory_mgr())) {
      LOG_ERROR("set_tenant_memory_mgr failed", K(ret));
    } else if (ObCtxIds::DO_NOT_USE_ME == ctx_id) {
      ctx_allocator->set_limit(256L<<20);
    }
  }
  if (OB_SUCC(ret)) {
    allocator = (ObTenantCtxAllocator*)buf;
  }
  return ret;
}

int ObMallocAllocator::add_tenant_allocator(ObTenantCtxAllocator *allocator)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = allocator->get_tenant_id();
  const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
  // critical area is extremely small, just wait without trylock
  ObDisableDiagnoseGuard disable_diagnose_guard;
  BucketWLockGuard guard(locks_[slot]);
  ObTenantCtxAllocator **cur = &allocators_[slot];
  while ((NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
    cur = &((*cur)->get_next());
  }
  if (OB_ISNULL(*cur) || (*cur)->get_tenant_id() > tenant_id) {
    ObTenantCtxAllocator *next_allocator = *cur;
    *cur = allocator;
    ((*cur)->get_next()) = next_allocator;
  } else {
    ret = OB_ENTRY_EXIST;
  }
  return ret;
}

ObTenantCtxAllocator *ObMallocAllocator::take_off_tenant_allocator(uint64_t tenant_id)
{
  ObTenantCtxAllocator *ta = NULL;
  const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  BucketWLockGuard guard(locks_[slot]);
  ObTenantCtxAllocator **cur = &allocators_[slot];
  while (*cur && (*cur)->get_tenant_id() < tenant_id) {
    cur = &(*cur)->get_next();
  }
  if (*cur != NULL && (*cur)->get_tenant_id() == tenant_id) {
    ta = *cur;
    *cur = (*cur)->get_next();
  }
  return ta;
}

void ObMallocAllocator::set_root_allocator()
{
  int ret = OB_SUCCESS;
  static char buf[sizeof(ObTenantCtxAllocator) * ObCtxIds::MAX_CTX_ID] __attribute__((__aligned__(16)));
  ObTenantCtxAllocator *allocator = NULL;
  abort_unless(OB_SUCCESS == create_tenant_allocator(OB_SERVER_TENANT_ID, buf, allocator));
  abort_unless(OB_SUCCESS == add_tenant_allocator(allocator));
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
int ObMallocAllocator::set_tenant_max_min(int64_t tenant_id, int64_t max_memory, int64_t min_memory)
{
  return with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr *mgr) {
      mgr->set_max(max_memory);
      mgr->set_min(min_memory);
      return OB_SUCCESS;
    });
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

int64_t ObMallocAllocator::get_tenant_cache_hold(uint64_t tenant_id)
{
  int64_t cache_hold = 0;
  with_resource_handle_invoke(tenant_id, [&cache_hold](ObTenantMemoryMgr *mgr) {
      cache_hold = mgr->get_cache_hold();
      return OB_SUCCESS;
    });
  return cache_hold;
}

int64_t ObMallocAllocator::get_tenant_remain(uint64_t tenant_id)
{
  int64_t remain = 0;
  with_resource_handle_invoke(tenant_id, [&remain](ObTenantMemoryMgr *mgr) {
      remain = mgr->get_limit() - mgr->get_sum_hold() + mgr->get_cache_hold();
      if (remain < 0) {
        remain = 0;
      }
      return OB_SUCCESS;
    });
  return remain;
}

int64_t ObMallocAllocator::get_tenant_ctx_hold(const uint64_t tenant_id, const uint64_t ctx_id) const
{
  int64_t hold = 0;
  ObTenantCtxAllocatorGuard allocator = NULL;
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
  ObTenantCtxAllocatorGuard allocator = NULL;
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
  ObTenantCtxAllocatorGuard allocator = NULL;
  for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
    allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (OB_LIKELY(NULL != allocator)) {
      allocator->print_memory_usage();
    } else {
      allocator = get_tenant_ctx_allocator_unrecycled(tenant_id, ctx_id);
      if (OB_LIKELY(NULL != allocator)) {
        allocator->print_memory_usage();
      }
    }
  }
}

void ObMallocAllocator::print_tenant_memory_usage(uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr *mgr) {
    static const int64_t BUFLEN = 1 << 16;
    SMART_VAR(char[BUFLEN], buf) {
      int64_t ctx_pos = 0;
      const volatile int64_t *ctx_hold_bytes = mgr->get_ctx_hold_bytes();
      for (uint64_t i = 0; OB_SUCC(ret) && i < ObCtxIds::MAX_CTX_ID; i++) {
        if (ctx_hold_bytes[i] > 0) {
          int64_t limit = 0;
          IGNORE_RETURN mgr->get_ctx_limit(i, limit);
          ret = databuff_printf(buf, BUFLEN, ctx_pos,
              "[MEMORY] ctx_id=%25s hold_bytes=%'15ld limit=%'26ld\n",
              get_global_ctx_info().get_ctx_name(i), ctx_hold_bytes[i], limit);
        }
      }
      buf[std::min(ctx_pos, BUFLEN - 1)] = '\0';
      allow_next_syslog();
      _LOG_INFO("[MEMORY] tenant: %lu, limit: %'lu hold: %'lu cache_hold: %'lu "
                "cache_used: %'lu cache_item_count: %'lu \n%s",
          tenant_id,
          mgr->get_limit(),
          mgr->get_sum_hold(),
          mgr->get_cache_hold(),
          mgr->get_cache_hold(),
          mgr->get_cache_item_count(),
          buf);
    }
    return ret;
  });
  UNUSED(ret);
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
  auto allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
  if (NULL == allocator) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant or ctx not exist", K(ret), K(tenant_id), K(ctx_id));
  } else {
    allocator->set_idle(size, reserve);
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
    auto allocator = get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (NULL != allocator && !(CTX_ATTR(ctx_id).disable_sync_wash_)) {
      washed_size += allocator->sync_wash(wash_size - washed_size);
    }
  }
  return washed_size;
}

int64_t ObMallocAllocator::sync_wash()
{
  int64_t washed_size = 0;
  for (uint64_t tenant_id = 1; tenant_id <= max_used_tenant_id_; ++tenant_id) {
    washed_size += sync_wash(tenant_id, 0, INT64_MAX);
  }
  return washed_size;
}

ObTenantCtxAllocatorGuard ObMallocAllocator::get_tenant_ctx_allocator_unrecycled(
  uint64_t tenant_id, uint64_t ctx_id) const
{
  ObTenantCtxAllocatorGuard ta;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  ObLatchRGuard guard(const_cast<ObLatch&>(unrecycled_lock_), ObLatchIds::OB_ALLOCATOR_LOCK);
  ObTenantCtxAllocator * const *cur = &unrecycled_allocators_;
  while (*cur) {
    if ((*cur)->get_tenant_id() == tenant_id) {
      ta = ObTenantCtxAllocatorGuard(&(*cur)[ctx_id]);
      break;
    }
    cur = &(*cur)->get_next();
  }
  return ta;
}

void ObMallocAllocator::add_tenant_allocator_unrecycled(ObTenantCtxAllocator *allocator)
{
#ifdef ENABLE_SANITY
  if (enable_tenant_leak_memory_protection_) {
    modify_tenant_memory_access_permission(allocator, false);
  }
#endif
  ObDisableDiagnoseGuard disable_diagnose_guard;
  ObLatchWGuard guard(unrecycled_lock_, ObLatchIds::OB_ALLOCATOR_LOCK);
  allocator->get_next() = unrecycled_allocators_;
  unrecycled_allocators_ = allocator;
}

ObTenantCtxAllocator *ObMallocAllocator::take_off_tenant_allocator_unrecycled(uint64_t tenant_id)
{
  ObTenantCtxAllocator *ta = NULL;
  {
    ObDisableDiagnoseGuard disable_diagnose_guard;
    ObLatchWGuard guard(unrecycled_lock_, ObLatchIds::OB_ALLOCATOR_LOCK);
    ObTenantCtxAllocator **cur = &unrecycled_allocators_;
    while (*cur) {
      if ((*cur)->get_tenant_id() == tenant_id) {
        ta = *cur;
        break;
      }
      cur = &(*cur)->get_next();
    }
    if (ta != NULL) {
      *cur = (*cur)->get_next();
    }
  }
  if (ta != NULL) {
#ifdef ENABLE_SANITY
    modify_tenant_memory_access_permission(ta, true);
#endif
  }
  return ta;
}

int ObMallocAllocator::recycle_tenant_allocator(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantCtxAllocator *ta = NULL;
  if (create_on_demand_) {
    // do-nothing
  } else if (tenant_id <= OB_USER_TENANT_ID) {
    ret = OB_OP_NOT_ALLOW;
  } else if (OB_ISNULL(ta = take_off_tenant_allocator(tenant_id)) &&
             OB_ISNULL(ta = take_off_tenant_allocator_unrecycled(tenant_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("tenant allocator not exist", K(ret), K(tenant_id));
  } else {
    // wash idle chunks
    for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ta[ctx_id].set_idle(0);
      ta[ctx_id].reset_req_chunk_mgr();
    }

    ObTenantCtxAllocator *tas[ObCtxIds::MAX_CTX_ID] = {NULL};
    for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      tas[ctx_id] = &ta[ctx_id];
    }

    // check references
    int wait_times = 3;
    int waiting_cnt = ObCtxIds::MAX_CTX_ID;
    while (wait_times--) {
      for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        if (NULL == tas[ctx_id]) continue;
        int64_t ref_cnt = tas[ctx_id]->get_ref_cnt();
        if (0 == ref_cnt) {
          LOG_INFO("wait tenant ctx allocator success", K(tenant_id), K(ctx_id),
                  "ctx_name", get_global_ctx_info().get_ctx_name(ctx_id));
          tas[ctx_id] = NULL;
          waiting_cnt--;
        }
      }
      if (waiting_cnt <= 0) break;
      usleep(1 * 1000 * 1000L); // 1s
    }
    for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ObTenantCtxAllocator *ctx_allocator = tas[ctx_id];
      if (ctx_allocator != NULL) {
        LOG_ERROR("tenant ctx allocator is still refered by upper-layer modules",
                  K(tenant_id), K(ctx_id),
                  "ctx_name", get_global_ctx_info().get_ctx_name(ctx_id),
                  K(ctx_allocator->get_ref_cnt()));
      }
    }

    // check unfree
    for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ObTenantCtxAllocator *ctx_allocator = tas[ctx_id];
      if (NULL == ctx_allocator) {
        ctx_allocator = &ta[ctx_id];
        char first_label[AOBJECT_LABEL_SIZE + 1] = {'\0'};
        char first_bt[MAX_BACKTRACE_LENGTH] = {'\0'};
        bool has_unfree = ctx_allocator->check_has_unfree(first_label, first_bt);
        if (has_unfree) {
          if (ObCtxIds::GLIBC == ctx_id
              && 0 == strncmp("Pl", first_label, 2)
              && pl_leaked_times_++ < 10) {
            LOG_WARN("tenant memory leak!!!", K(tenant_id), K(ctx_id),
                     "ctx_name", get_global_ctx_info().get_ctx_name(ctx_id),
                     "label", first_label,
                     "backtrace", first_bt);
          } else if (0 == strncmp("Diagnostic", first_label, 10) && di_leaked_times_++ < 10) {
            LOG_WARN("tenant memory leak!!!", K(tenant_id), K(ctx_id),
                      "ctx_name", get_global_ctx_info().get_ctx_name(ctx_id),
                      "label", first_label,
                      "backtrace", first_bt);
          } else {
            LOG_ERROR("tenant memory leak!!!", K(tenant_id), K(ctx_id),
                      "ctx_name", get_global_ctx_info().get_ctx_name(ctx_id),
                      "label", first_label,
                      "backtrace", first_bt);
          }
          tas[ctx_id] = ctx_allocator;
        }
      }
    }

    bool all_ready = true;
    for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      ObTenantCtxAllocator *ctx_allocator = tas[ctx_id];
      if (ctx_allocator != NULL) {
        all_ready = false;
      }
    }
    if (!all_ready) {
      ret = OB_ERROR;
      LOG_WARN("failed to recycle tenant allocator immediately", K(ret), K(tenant_id));
      add_tenant_allocator_unrecycled(ta);
    } else {
      destroy_tenant_allocator(ta);
      ta = NULL;
      LOG_INFO("recycle tenant allocator success", K(tenant_id));
    }
  }
  return ret;
}

void ObMallocAllocator::get_unrecycled_tenant_ids(uint64_t *ids, int cap, int &cnt) const
{
  cnt = 0;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  ObLatchRGuard guard(const_cast<ObLatch&>(unrecycled_lock_), ObLatchIds::OB_ALLOCATOR_LOCK);
  ObTenantCtxAllocator * const *cur = &unrecycled_allocators_;
  while (*cur && cnt < cap) {
    ids[cnt++] = (*cur)->get_tenant_id();
    cur = &(*cur)->get_next();
  }
}

bool ObMallocAllocator::is_tenant_allocator_exist(int64_t tenant_id)
{
  return NULL != get_tenant_ctx_allocator(tenant_id, ObCtxIds::DEFAULT_CTX_ID) ||
      NULL != get_tenant_ctx_allocator_unrecycled(tenant_id, ObCtxIds::DEFAULT_CTX_ID);
}

void ObMallocAllocator::set_tenant_parent_limiter(int64_t tenant_id, ObResourceLimiter& parent)
{
  with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr *mgr) {
    mgr->set_parent_limiter(parent);
    return OB_SUCCESS;
  });
}

ObResourceLimiter* ObMallocAllocator::get_tenant_parent_limiter(int64_t tenant_id)
{
  ObResourceLimiter* tenant_parent_limiter = NULL;
  with_resource_handle_invoke(tenant_id, [&](ObTenantMemoryMgr *mgr) {
    tenant_parent_limiter = mgr->get_parent_limiter();
    return OB_SUCCESS;
  });
  return tenant_parent_limiter;
}

#ifdef ENABLE_SANITY
int ObMallocAllocator::get_chunks(ObTenantCtxAllocator *ta, AChunk **chunks, int cap, int &cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
    ObTenantCtxAllocator *ctx_allocator = &ta[ctx_id];
    ctx_allocator->get_chunks(chunks, cap, cnt);
    if (cnt >= cap) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

void ObMallocAllocator::modify_tenant_memory_access_permission(ObTenantCtxAllocator *ta, bool accessible)
{
  AChunk *chunks[1024] = {nullptr};
  int chunk_cnt = 0;
  get_chunks(ta, chunks, sizeof(chunks)/sizeof(chunks[0]), chunk_cnt);
  for (int i = 0; i < chunk_cnt; i++) {
    AChunk *chunk = chunks[i];
    if (chunk != nullptr) {
      if (accessible) {
        SANITY_UNPOISON(chunk, chunk->aligned());
      } else {
        SANITY_POISON(chunk, chunk->aligned());
      }
    }
  }
}
#endif

ObMallocHook::ObMallocHook()
  : attr_(OB_SERVER_TENANT_ID, "glibc_malloc", ObCtxIds::GLIBC),
    mgr_(OB_SERVER_TENANT_ID, ObCtxIds::GLIBC)
{
  STRNCPY(label_, "glibc_malloc_v2", AOBJECT_LABEL_SIZE);
  label_[AOBJECT_LABEL_SIZE] = '\0';
}

ObMallocHook &ObMallocHook::get_instance()
{
  static char buffer[sizeof(ObMallocHook)] __attribute__((__aligned__(16)));
  static ObMallocHook *instance = new (buffer) ObMallocHook();
  return *instance;
}

void *ObMallocHook::alloc(const int64_t size)
{
  if (OB_UNLIKELY(!g_malloc_v2_enabled)) {
    ObMemAttr attr = ObMallocHookAttrGuard::get_tl_mem_attr();
    return ob_malloc(size, attr);
  }
  SANITY_DISABLE_CHECK_RANGE();
  int ret = EventTable::EN_4;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    const ObErrsimModuleType type = THIS_WORKER.get_module_type();
    if (is_errsim_module(attr_.tenant_id_, type.type_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
#endif
  void *ptr = NULL;
  if (OB_FAIL(ret)) {
    AllocFailedCtx &afc = g_alloc_failed_ctx();
    afc.reason_ = AllocFailedReason::ERRSIM_INJECTION;
  } else {
    AObject *obj = NULL;
    static thread_local ObMallocSampleLimiter sample_limiter;
    bool sample_allowed = sample_limiter.try_acquire(size);
    if (OB_UNLIKELY(sample_allowed)) {
      ObMemAttr inner_attr = attr_;
      inner_attr.alloc_extra_info_ = true;
      obj = mgr_.alloc_object(size, inner_attr);
    } else {
      obj = mgr_.alloc_object(size, attr_);
    }
    if (OB_UNLIKELY(NULL == obj)) {
      if (mgr_.sync_wash() >= size) {
        if (OB_UNLIKELY(sample_allowed)) {
          ObMemAttr inner_attr = attr_;
          inner_attr.alloc_extra_info_ = true;
          obj = mgr_.alloc_object(size, inner_attr);
        } else {
          obj = mgr_.alloc_object(size, attr_);
        }
      }
    }
    if (OB_LIKELY(NULL != obj)) {
      if (OB_UNLIKELY(sample_allowed)) {
        void *addrs[100] = {nullptr};
        backtrace(addrs, ARRAYSIZEOF(addrs));
        MEMCPY(obj->bt(), (char*)addrs, AOBJECT_BACKTRACE_SIZE);
        obj->on_malloc_sample_ = true;
      }
      MEMCPY(obj->label_, label_, AOBJECT_LABEL_SIZE + 1);
      obj->ignore_version_ = true;
      SANITY_POISON(obj, AOBJECT_HEADER_SIZE);
      SANITY_UNPOISON(obj->data_, obj->alloc_bytes_);
      SANITY_POISON(obj->data_ + obj->alloc_bytes_,
          AOBJECT_TAIL_SIZE + (obj->on_malloc_sample_ ? AOBJECT_BACKTRACE_SIZE : 0));
      ptr = (void*)obj->data_;
    }
  }
  if (OB_UNLIKELY(NULL == ptr)) {
    if (TC_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      const char *msg = alloc_failed_msg();
      LOG_DBA_WARN_V2(OB_LIB_ALLOCATE_MEMORY_FAIL, OB_ALLOCATE_MEMORY_FAILED, "[OOPS]: alloc failed reason is that ", msg);
    }
  }
  return ptr;
}

void ObMallocHook::free(void *ptr)
{
  SANITY_DISABLE_CHECK_RANGE();
  if (NULL != ptr) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->is_valid() && obj->in_use_);
    ABlock *block = obj->block();
    DEBUG_ASSERT(block->is_valid());
    DEBUG_ASSERT(block->in_use_);
    DEBUG_ASSERT(block->obj_set_ != NULL);
    if (OB_LIKELY(block->is_malloc_v2_)) {
      SANITY_POISON(obj->data_, obj->alloc_bytes_);
      ObjectSetV2 *os = (ObjectSetV2*)block->obj_set_;
      os->free_object(obj, block);
    } else {
      ob_free(ptr);
    }
  }
}
} // end of namespace lib
} // end of namespace oceanbase
