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

#include "ob_mds_allocator.h"

#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/throttle/ob_share_throttle_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

using namespace oceanbase::storage::mds;

namespace oceanbase {
namespace share {

int64_t ObTenantMdsAllocator::resource_unit_size()
{
  static const int64_t MDS_RESOURCE_UNIT_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE; /* 8KB */
  return MDS_RESOURCE_UNIT_SIZE;
}

void ObTenantMdsAllocator::init_throttle_config(int64_t &resource_limit, int64_t &trigger_percentage, int64_t &max_duration)
{
  // define some default value
  const int64_t MDS_LIMIT_PERCENTAGE = 5;
  const int64_t MDS_THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t MDS_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());

  // Use tenant config to init throttle config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    resource_limit = total_memory * tenant_config->_mds_memory_limit_percentage / 100LL;
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    resource_limit = total_memory * MDS_LIMIT_PERCENTAGE / 100;
    trigger_percentage = MDS_THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = MDS_THROTTLE_MAX_DURATION;
  }
}
void ObTenantMdsAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                 const int64_t holding_size,
                                                 const int64_t config_specify_resource_limit,
                                                 int64_t &resource_limit,
                                                 int64_t &last_update_limit_ts,
                                                 bool &is_updated)
{
  // do nothing
}

int ObTenantMdsAllocator::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  // TODO : @gengli new ctx id?
  mem_attr.tenant_id_ = MTL_ID();
  mem_attr.ctx_id_ = ObCtxIds::MDS_DATA_ID;
  mem_attr.label_ = "MdsTable";
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  throttle_tool_ = &(share_mem_alloc_mgr->share_resource_throttle_tool());
  MDS_TG(10_ms);
  if (IS_INIT){
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init tenant mds allocator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(throttle_tool_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "throttle tool is unexpected null", KP(throttle_tool_), KP(share_mem_alloc_mgr));
  } else if (MDS_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    MDS_LOG(WARN, "init vslice allocator failed", K(ret), K(OB_MALLOC_NORMAL_BLOCK_SIZE), KP(this), K(mem_attr));
  } else {
    allocator_.set_nway(MDS_ALLOC_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

void *ObTenantMdsAllocator::alloc(const int64_t size)
{
  int64_t abs_expire_time = THIS_WORKER.get_timeout_ts();
  return alloc(size, abs_expire_time);
}

void *ObTenantMdsAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  UNUSED(attr);
  void *obj = alloc(size);
  MDS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "VSLICE Allocator not support mark attr", KP(obj), K(size), K(attr));
  return obj;
}

void *ObTenantMdsAllocator::alloc(const int64_t size, const int64_t abs_expire_time)
{
  bool is_throttled = false;

  // record alloc resource in throttle tool, but do not throttle immediately
  // ObMdsThrottleGuard calls the real throttle logic
  (void)throttle_tool_->alloc_resource<ObTenantMdsAllocator>(size, abs_expire_time, is_throttled);

  // if is throttled, do throttle
  if (OB_UNLIKELY(is_throttled)) {
    share::mds_throttled_alloc() += size;
  }

  void *obj = allocator_.alloc(size);
  MDS_LOG(DEBUG, "mds alloc ", K(size), KP(obj), K(abs_expire_time));
  if (OB_NOT_NULL(obj)) {
    MTL(storage::mds::ObTenantMdsService *)
        ->record_alloc_backtrace(obj,
                                 __thread_mds_tag__,
                                 __thread_mds_alloc_type__,
                                 __thread_mds_alloc_file__,
                                 __thread_mds_alloc_func__,
                                 __thread_mds_alloc_line__);  // for debug mem leak
  }
  return obj;
}


void ObTenantMdsAllocator::free(void *ptr)
{
  allocator_.free(ptr);
  MTL(storage::mds::ObTenantMdsService *)->erase_alloc_backtrace(ptr);
}

void ObTenantMdsAllocator::set_attr(const ObMemAttr &attr) { allocator_.set_attr(attr); }

void *ObTenantBufferCtxAllocator::alloc(const int64_t size)
{
  void *obj = share::mtl_malloc(size, ObMemAttr(MTL_ID(), "MDS_CTX_DEFAULT", ObCtxIds::MDS_CTX_ID));
  if (OB_NOT_NULL(obj)) {
    MTL(ObTenantMdsService*)->record_alloc_backtrace(obj,
                                                     __thread_mds_tag__,
                                                     __thread_mds_alloc_type__,
                                                     __thread_mds_alloc_file__,
                                                     __thread_mds_alloc_func__,
                                                     __thread_mds_alloc_line__);// for debug mem leak
  }
  return obj;
}

void *ObTenantBufferCtxAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  void *obj = share::mtl_malloc(size, attr);
  if (OB_NOT_NULL(obj)) {
    MTL(ObTenantMdsService*)->record_alloc_backtrace(obj,
                                                     __thread_mds_tag__,
                                                     __thread_mds_alloc_type__,
                                                     __thread_mds_alloc_file__,
                                                     __thread_mds_alloc_func__,
                                                     __thread_mds_alloc_line__);// for debug mem leak
  }
  return obj;
}

void ObTenantBufferCtxAllocator::free(void *ptr)
{
  share::mtl_free(ptr);
  MTL(ObTenantMdsService*)->erase_alloc_backtrace(ptr);
}

ObMdsThrottleGuard::ObMdsThrottleGuard(const bool for_replay, const int64_t abs_expire_time) : for_replay_(for_replay), abs_expire_time_(abs_expire_time)
{
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  if (0 == abs_expire_time) {
    abs_expire_time_ =
        ObClockGenerator::getClock() + ObThrottleUnit<ObMdsThrottleGuard>::DEFAULT_MAX_THROTTLE_TIME;
  }
  share::mds_throttled_alloc() = 0;
}

ObMdsThrottleGuard::~ObMdsThrottleGuard()
{
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;

  if (OB_ISNULL(throttle_tool_)) {
    MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "throttle tool is unexpected nullptr", KP(throttle_tool_));
  } else if (throttle_tool_->is_throttling<ObTenantMdsAllocator>(share_ti_guard, module_ti_guard)) {
    (void)TxShareMemThrottleUtil::do_throttle<ObTenantMdsAllocator>(
        for_replay_, abs_expire_time_, share::mds_throttled_alloc(), *throttle_tool_, share_ti_guard, module_ti_guard);

    if (throttle_tool_->still_throttling<ObTenantMdsAllocator>(share_ti_guard, module_ti_guard)) {
      (void)throttle_tool_->skip_throttle<ObTenantMdsAllocator>(
          share::mds_throttled_alloc(), share_ti_guard, module_ti_guard);

      if (module_ti_guard.is_valid()) {
        module_ti_guard.throttle_info()->reset();
      }
    }

    // reset mds throttled alloc size
    share::mds_throttled_alloc() = 0;
  } else {
    // do not need throttle, exit directly
  }
}

}  // namespace share
}  // namespace oceanbase