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


#include "ob_tenant_vector_allocator.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"


namespace oceanbase {
namespace share {

int64_t ObTenantVectorAllocator::resource_unit_size()
{
  static const int64_t VECTOR_RESOURCE_UNIT_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE; /* 8KB */
  return VECTOR_RESOURCE_UNIT_SIZE;
}

void ObTenantVectorAllocator::init_throttle_config(int64_t &resource_limit, int64_t &trigger_percentage, int64_t &max_duration)
{
  // define some default value
  trigger_percentage = 100;
  get_vector_mem_config(resource_limit, max_duration);
}

int64_t ObTenantVectorAllocator::get_vector_mem_limit_percentage(omt::ObTenantConfigGuard &tenant_config, uint64_t tenant_id /*default is MTL_ID()*/)
{
  const int64_t SMALL_TENANT_MEMORY_LIMIT = 8 * 1024 * 1024 * 1024L; // 8G
  const int64_t SMALL_VECTOR_LIMIT_PERCENTAGE = 40;
  const int64_t LARGE_VECTOR_LIMIT_PERCENTAGE = 50;
  const int64_t tenant_memory = lib::get_tenant_memory_limit(tenant_id);
  int64_t tenant_memstore_limit_percent = 0;
  int64_t percent = 0;
  if (tenant_config.is_valid()) {
    tenant_memstore_limit_percent = tenant_config->ob_vector_memory_limit_percentage;
  }
  if (tenant_memstore_limit_percent != 0) {
    percent = tenant_memstore_limit_percent;
  } else {
    // both is default value, adjust automatically
    if (tenant_memory <= SMALL_TENANT_MEMORY_LIMIT) {
      percent = SMALL_VECTOR_LIMIT_PERCENTAGE;
    } else {
      percent = LARGE_VECTOR_LIMIT_PERCENTAGE;
    }
  }
  return percent;
}

void ObTenantVectorAllocator::get_vector_mem_config(int64_t &resource_limit, int64_t &max_duration)
{
  const int64_t VECTOR_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours
  const int64_t tenant_memory = lib::get_tenant_memory_limit(MTL_ID());
  int64_t tenant_memstore_limit_percent = 0;
  int64_t percent = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    max_duration = VECTOR_THROTTLE_MAX_DURATION;
  }
  percent = get_vector_mem_limit_percentage(tenant_config);
  resource_limit = tenant_memory * percent / 100;
}


void ObTenantVectorAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                 const int64_t holding_size,
                                                 const int64_t config_specify_resource_limit,
                                                 int64_t &resource_limit,
                                                 int64_t &last_update_limit_ts,
                                                 bool &is_updated)
{
  // do nothing
}

int ObTenantVectorAllocator::init()
{
  int ret = OB_SUCCESS;

  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "VectorIndex", ObCtxIds::VECTOR_CTX_ID)
    .set_properties(lib::ADD_CHILD_THREAD_SAFE | lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_label("VectorIndex")
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  throttle_tool_ = &(share_mem_alloc_mgr->share_resource_throttle_tool());
  MDS_TG(10_ms);
  if (IS_INIT){
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init tenant vector allocator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(throttle_tool_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "throttle tool is unexpected null", KP(throttle_tool_), KP(share_mem_alloc_mgr));
  } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(memory_context_, param))) {
    SHARE_LOG(WARN, "create memory entity failed", K(ret));
  } else if (OB_FAIL(ObVectorMemContext::init(memory_context_, throttle_tool_))) {
    SHARE_LOG(WARN, "vector mem context init failed", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObTenantVectorAllocator::destroy()
{
  is_inited_ = false;
  if (memory_context_ != nullptr) {
    DESTROY_CONTEXT(memory_context_);
    memory_context_ = nullptr;
  }
}

int64_t ObTenantVectorAllocator::hold()
{
  return lib::get_tenant_memory_hold(MTL_ID(), ObCtxIds::VECTOR_CTX_ID) + get_rb_mem_used();
}

int64_t ObTenantVectorAllocator::get_rb_mem_used()
{
  ObRbMemMgr *rb_mgr = MTL(ObRbMemMgr *);
  return rb_mgr != nullptr ? rb_mgr->get_vec_idx_used() : 0;
}

int64_t ObTenantVectorAllocator::used()
{
  return all_used_mem_ + get_rb_mem_used();
}

void *ObTenantVectorAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  UNUSED(attr);
  return alloc(size);
}

void *ObTenantVectorAllocator::alloc(const int64_t size, const int64_t abs_expire_time)
{
  UNUSED(abs_expire_time);
  return alloc(size);
}

// !!!!! NOTICE
// This function will throw an exception when memory allocation fails,
// so it can only be called within vsag and cannot be used elsewhere
void *ObVsagMemContext::Allocate(size_t size)
{
  void *ret_ptr = nullptr;
  int ret = OB_SUCCESS;
  if (size != 0) {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + size;
    void *ptr = ObVectorMemContext::alloc(actual_size);
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_AAF(all_vsag_use_mem_, actual_size);

      *(int64_t*)ptr = actual_size;
      ret_ptr = (char*)ptr + MEM_PTR_HEAD_SIZE;
    } else {
      // NOTICE: ObVsagMemContext is used in vsag lib. And may be used for some c++ std container.
      // For this scenario, if memory allocation fails, an exception should be thrown instead of returning a null pointer
      // or will access null point in std container, eg std::vector
      SHARE_LOG(WARN, "fail to allocate memory", K(size), K(actual_size), K(lbt()));
      throw std::bad_alloc();
    }
  }

  return ret_ptr;
}

void ObVsagMemContext::Deallocate(void* p)
{
  if (OB_NOT_NULL(p)) {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t size = *(int64_t *)size_ptr;

    ATOMIC_SAF(all_vsag_use_mem_, size);
    ObVectorMemContext::free((char*)p - MEM_PTR_HEAD_SIZE);
  }
}

void *ObVsagMemContext::Reallocate(void* p, size_t size)
{
  void *new_ptr = nullptr;
  if (size == 0) {
    if (OB_NOT_NULL(p)) {
      Deallocate(p);
      p = nullptr;
    }
  } else if (OB_ISNULL(p)) {
    new_ptr = Allocate(size);
  } else {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t old_size = *(int64_t *)size_ptr - MEM_PTR_HEAD_SIZE;
    if (old_size >= size) {
      new_ptr = p;
    } else {
      new_ptr = Allocate(size);
      if (OB_ISNULL(new_ptr) || OB_ISNULL(p)) {
      } else {
        MEMCPY(new_ptr, p, old_size);
        Deallocate(p);
        p = nullptr;
      }
    }
  }
  return new_ptr;
}

int ObVsagMemContext::init(lib::MemoryContext &parent_mem_context,
                           uint64_t *all_vsag_use_mem,
                           uint64_t tenant_id)
{
  INIT_SUCC(ret);
  lib::ContextParam param;
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  ObMemAttr attr(tenant_id, "VIndexVsagADP", ObCtxIds::VECTOR_CTX_ID);
  SET_IGNORE_MEM_VERSION(attr);
  param.set_mem_attr(attr)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_parallel(8)
    .set_properties(lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT);
  if (OB_FAIL(parent_mem_context->CREATE_CONTEXT(mem_context_, param))) {
    OB_LOG(WARN, "create memory entity failed", K(ret));
  } else if (OB_FAIL(ObVectorMemContext::init(mem_context_, &(share_mem_alloc_mgr->share_resource_throttle_tool())))) {
    SHARE_LOG(WARN, "vector mem context init failed", K(ret));
  } else {
    all_vsag_use_mem_ = all_vsag_use_mem;
  }

  return ret;
}

void* ObVectorMemContext::alloc(int64_t size)
{
  int ret = OB_SUCCESS;
  void *ret_ptr = nullptr;
  if (ATOMIC_LOAD(&check_cnt_) >= ObVectorMemContext::CHECK_USAGE_INTERVAL ||
      size >= ObVectorMemContext::CHECK_RESOURCE_UNIT_SIZE) {
    if (throttle_tool_->exceeded_resource_limit<ObTenantVectorAllocator>(size)) {
      // need check next time
      ATOMIC_STORE(&check_cnt_, ObVectorMemContext::CHECK_USAGE_INTERVAL);
      ret = OB_ERR_VSAG_MEM_LIMIT_EXCEEDED;
      OB_LOG(WARN,"Memory usage exceeds user limit.", K(ret));
    } else {
      ATOMIC_STORE(&check_cnt_, 0);
    }
  }

  if (OB_SUCC(ret)) {
    ret_ptr = memory_context_->get_malloc_allocator().alloc(size);
    if (OB_NOT_NULL(ret_ptr)) {
      ATOMIC_INC(&check_cnt_);
    }
  }
  return ret_ptr;
}

void ObVectorMemContext::free(void *ptr)
{
  if (OB_NOT_NULL(ptr)) {
    memory_context_->get_malloc_allocator().free(ptr);
  }
}

int ObVectorMemContext::init(lib::MemoryContext &mem_context, share::TxShareThrottleTool *throttle_tool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context) || OB_ISNULL(throttle_tool)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "mem_context or throttle_tool is null.", K(ret), KPC(mem_context), KPC(throttle_tool));
  } else {
    memory_context_ = mem_context;
    throttle_tool_ = throttle_tool;
  }
  return ret;
}

const char* ObIvfMemContext::IVF_CACHE_LABEL = "IvfCacheCtx";
const char* ObIvfMemContext::IVF_BUILD_LABEL = "IvfBuildCtx";

int ObIvfMemContext::init(lib::MemoryContext &parent_mem_context, uint64_t *all_vsag_use_mem, uint64_t tenant_id,
                          const char *label /*IVF_CACHE_LABEL*/)
{
  INIT_SUCC(ret);
  lib::ContextParam param;
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  ObMemAttr attr(tenant_id, label, ObCtxIds::VECTOR_CTX_ID);
  SET_IGNORE_MEM_VERSION(attr);
  param.set_mem_attr(attr)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_parallel(8)
    .set_properties(lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT);
  if (OB_FAIL(parent_mem_context->CREATE_CONTEXT(mem_context_, param))) {
    OB_LOG(WARN, "create memory entity failed", K(ret));
  } else if (OB_FAIL(ObVectorMemContext::init(mem_context_, &(share_mem_alloc_mgr->share_resource_throttle_tool())))) {
    SHARE_LOG(WARN, "vector mem context init failed", K(ret));
  } else {
    all_vsag_use_mem_ = all_vsag_use_mem;
  }

  return ret;
}

void *ObIvfMemContext::Allocate(size_t size)
{
  void *ret_ptr = nullptr;
  int ret = OB_SUCCESS;
  if (size != 0) {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + size;
    void *ptr = ObVectorMemContext::alloc(actual_size);
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_AAF(all_vsag_use_mem_, actual_size);

      *(int64_t*)ptr = actual_size;
      ret_ptr = (char*)ptr + MEM_PTR_HEAD_SIZE;
    }
  }

  return ret_ptr;
}

void ObIvfMemContext::Deallocate(void* p)
{
  if (OB_NOT_NULL(p)) {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t size = *(int64_t *)size_ptr;

    ATOMIC_SAF(all_vsag_use_mem_, size);
    ObVectorMemContext::free((char*)p - MEM_PTR_HEAD_SIZE);
  }
}

}  // namespace share
}  // namespace oceanbase
