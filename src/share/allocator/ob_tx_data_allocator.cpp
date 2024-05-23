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

#define USING_LOG_PREFIX SHARE

#include "ob_tx_data_allocator.h"

#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase {

namespace share {

thread_local int64_t ObTenantTxDataOpAllocator::local_alloc_size_ = 0;

int64_t ObTenantTxDataAllocator::resource_unit_size()
{
  static const int64_t TX_DATA_RESOURCE_UNIT_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE; /* 8KB */
  return TX_DATA_RESOURCE_UNIT_SIZE;
}

void ObTenantTxDataAllocator::init_throttle_config(int64_t &resource_limit,
                                                   int64_t &trigger_percentage,
                                                   int64_t &max_duration)
{
  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());

  // Use tenant config to init throttle config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    resource_limit = total_memory * tenant_config->_tx_data_memory_limit_percentage / 100LL;
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    resource_limit = total_memory * TX_DATA_LIMIT_PERCENTAGE / 100;
    trigger_percentage = TX_DATA_THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = TX_DATA_THROTTLE_MAX_DURATION;
  }
}
void ObTenantTxDataAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                    const int64_t holding_size,
                                                    const int64_t config_specify_resource_limit,
                                                    int64_t &resource_limit,
                                                    int64_t &last_update_limit_ts,
                                                    bool &is_updated)
{
  // do nothing
}

int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = label;
  mem_attr.tenant_id_ = MTL_ID();
  mem_attr.ctx_id_ = ObCtxIds::TX_DATA_TABLE;
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  throttle_tool_ = &(share_mem_alloc_mgr->share_resource_throttle_tool());
  if (IS_INIT){
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init tenant mds allocator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(throttle_tool_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "throttle tool is unexpected null", KP(throttle_tool_), KP(share_mem_alloc_mgr));
  } else if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

void ObTenantTxDataAllocator::reset()
{
  is_inited_ = false;
  slice_allocator_.purge_extra_cached_block(0);
}

void *ObTenantTxDataAllocator::alloc(const bool enable_throttle, const int64_t abs_expire_time)
{
  // do throttle if needed
  if (OB_LIKELY(enable_throttle)) {
    bool is_throttled = false;
    (void)throttle_tool_->alloc_resource<ObTenantTxDataAllocator>(
        storage::TX_DATA_SLICE_SIZE, abs_expire_time, is_throttled);

    if (OB_UNLIKELY(is_throttled)) {
      share::tx_data_throttled_alloc() += storage::TX_DATA_SLICE_SIZE;
    }
  }

  // allocate memory
  void *res = slice_allocator_.alloc();
  return res;
}

ObTxDataThrottleGuard::ObTxDataThrottleGuard(const bool for_replay, const int64_t abs_expire_time)
    : for_replay_(for_replay), abs_expire_time_(abs_expire_time)
{
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  if (0 == abs_expire_time) {
    abs_expire_time_ =
        ObClockGenerator::getClock() + ObThrottleUnit<ObTenantTxDataAllocator>::DEFAULT_MAX_THROTTLE_TIME;
  }
  share::tx_data_throttled_alloc() = 0;
}

ObTxDataThrottleGuard::~ObTxDataThrottleGuard()
{
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;

  if (OB_ISNULL(throttle_tool_)) {
    MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "throttle tool is unexpected nullptr", KP(throttle_tool_));
  } else if (throttle_tool_->is_throttling<ObTenantTxDataAllocator>(share_ti_guard, module_ti_guard)) {
    (void)TxShareMemThrottleUtil::do_throttle<ObTenantTxDataAllocator>(for_replay_,
                                                                       abs_expire_time_,
                                                                       share::tx_data_throttled_alloc(),
                                                                       *throttle_tool_,
                                                                       share_ti_guard,
                                                                       module_ti_guard);

    if (throttle_tool_->still_throttling<ObTenantTxDataAllocator>(share_ti_guard, module_ti_guard)) {
      (void)throttle_tool_->skip_throttle<ObTenantTxDataAllocator>(
          share::tx_data_throttled_alloc(), share_ti_guard, module_ti_guard);

      if (module_ti_guard.is_valid()) {
        module_ti_guard.throttle_info()->reset();
      }
    }

    // reset tx data throttled alloc size
    share::tx_data_throttled_alloc() = 0;
  } else {
    // do not need throttle, exit directly
  }
}

int ObTenantTxDataOpAllocator::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = MTL_ID();
  mem_attr.ctx_id_ = ObCtxIds::MDS_DATA_ID;
  mem_attr.label_ = "TX_OP";
  ObSharedMemAllocMgr *share_mem_alloc_mgr = MTL(ObSharedMemAllocMgr *);
  throttle_tool_ = &(share_mem_alloc_mgr->share_resource_throttle_tool());
  if (IS_INIT){
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init tenant mds allocator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(throttle_tool_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "throttle tool is unexpected null", KP(throttle_tool_), KP(share_mem_alloc_mgr));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    MDS_LOG(WARN, "init vslice allocator failed", K(ret), K(OB_MALLOC_NORMAL_BLOCK_SIZE), KP(this), K(mem_attr));
  } else {
    allocator_.set_nway(MDS_ALLOC_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

void *ObTenantTxDataOpAllocator::alloc(const int64_t size)
{
  int64_t abs_expire_time = THIS_WORKER.get_timeout_ts();
  void * buf = alloc(size, abs_expire_time);
  if (OB_NOT_NULL(buf)) {
    local_alloc_size_ += size;
  }
  return buf;
}

void *ObTenantTxDataOpAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  UNUSED(attr);
  void *obj = alloc(size);
  return obj;
}

void *ObTenantTxDataOpAllocator::alloc(const int64_t size, const int64_t abs_expire_time)
{
  void *obj = allocator_.alloc(size);
  return obj;
}

void ObTenantTxDataOpAllocator::free(void *ptr)
{
  allocator_.free(ptr);
}

void ObTenantTxDataOpAllocator::set_attr(const ObMemAttr &attr) { allocator_.set_attr(attr); }

}  // namespace share
}  // namespace oceanbase
