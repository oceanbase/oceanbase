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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_service.h"
#include "observer/ob_srv_mem_info.h"
#include "lib/alloc/memory_dump.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
namespace observer
{

int ObService::get_tenant_memstore_info(const uint64_t tenant_id, share::ObTenantMemoryInfoOperator::TenantMenstoreInfo &menstore_info)
{
  int ret = OB_SUCCESS;
  storage::ObTenantFreezer *freezer = nullptr;
  if (OB_ISNULL(freezer = MTL(storage::ObTenantFreezer *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("freezer is null", KR(ret), K(tenant_id));
  } else {
    int64_t active_memstore_used = 0;
    int64_t total_memstore_used = 0;
    int64_t memstore_freeze_trigger = 0;
    int64_t memstore_limit = 0;
    int64_t freeze_cnt = 0;
    int64_t throttle_trigger_percentage = 0;
    if (OB_FAIL(freezer->get_tenant_memstore_cond(active_memstore_used,
                                                  total_memstore_used,
                                                  memstore_freeze_trigger,
                                                  memstore_limit,
                                                  freeze_cnt,
                                                  throttle_trigger_percentage))) {
      LOG_WARN("fail to get tenant memstore cond", KR(ret), K(tenant_id));
    } else {
      menstore_info.total_memstore_used_ = total_memstore_used;
      menstore_info.memstore_limit_ = memstore_limit;
    }
  }
  return ret;
}

int ObService::get_tenant_vector_mem_info(const uint64_t tenant_id, share::ObTenantMemoryInfoOperator::TenantVectorMemInfo &vector_mem_info)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
  ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
  if (OB_NOT_NULL(service) && OB_NOT_NULL(shared_mem_mgr)) {
    int64_t glibc_used = 0;
    lib::ObMallocSampleMap malloc_sample_map;
    if (OB_FAIL(malloc_sample_map.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
      LOG_WARN("create memory info map failed", KR(ret));
    } else if (OB_FAIL(ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map))) {
      LOG_WARN("load memory info map failed", KR(ret));
    } else {
      for (lib::ObMallocSampleMap::const_iterator it = malloc_sample_map.begin();
           it != malloc_sample_map.end(); ++it) {
        if (tenant_id == it->first.tenant_id_) {
          if (0 == STRNCMP("VIndex", it->first.label_, strlen("VIndex")) &&
              common::ObCtxIds::GLIBC == it->first.ctx_id_) {
            glibc_used += it->second.alloc_bytes_;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      vector_mem_info.raw_malloc_size_ = glibc_used;
      vector_mem_info.index_metadata_size_ = service->get_allocator().used();
      vector_mem_info.vector_mem_hold_ = shared_mem_mgr->vector_allocator().hold();
      vector_mem_info.vector_mem_used_ = shared_mem_mgr->vector_allocator().used();
      vector_mem_info.vector_mem_limit_ = shared_mem_mgr->share_resource_throttle_tool().get_resource_limit<ObTenantVectorAllocator>();
    }
  }
  return ret;
}

int ObService::get_tenant_memory_info(const obrpc::ObGetTenantMemoryInfoArg &arg, obrpc::ObGetTenantMemoryInfoResult &result)
{
  int ret = OB_SUCCESS;
  const ObAddr &my_addr = GCONF.self_addr_;
  const uint64_t tenant_id = arg.get_tenant_id();
  result.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    share::ObTenantMemoryInfoOperator::TenantMenstoreInfo menstore_info;
    share::ObTenantMemoryInfoOperator::TenantVectorMemInfo vector_mem_info;
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(get_tenant_memstore_info(tenant_id, menstore_info))) {
        LOG_WARN("fail to get tenant memstore info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(get_tenant_vector_mem_info(tenant_id, vector_mem_info))) {
        LOG_WARN("fail to get tenant vector mem info", KR(ret), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result.init(tenant_id, my_addr, menstore_info, vector_mem_info))) {
      LOG_WARN("fail to init result", KR(ret), K(tenant_id), K(my_addr));
    }
  }
  FLOG_INFO("get tenant memory info", KR(ret), K(arg), K(result));
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
