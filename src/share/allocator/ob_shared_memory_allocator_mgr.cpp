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

#include "ob_shared_memory_allocator_mgr.h"

namespace oceanbase {
namespace share {

#define THROTTLE_CONFIG_LOG(ALLOCATOR, LIMIT, TRIGGER_PERCENTAGE, MAX_DURATION) \
          "Unit Name",                                                          \
          ALLOCATOR::throttle_unit_name(),                                      \
          "Memory Limit(MB)",                                                   \
          LIMIT / 1024 / 1024,                                                  \
          "Throttle Trigger(MB)",                                               \
          LIMIT * trigger_percentage / 100 / 1024 / 1024,                       \
          "Trigger Percentage",                                                 \
          TRIGGER_PERCENTAGE,                                                   \
          "Max Alloc Duration",                                                 \
          MAX_DURATION

void ObSharedMemAllocMgr::update_throttle_config()
{
  int64_t tenant_id = MTL_ID();
  int64_t total_memory = lib::get_tenant_memory_limit(tenant_id);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    int64_t share_mem_limit_percentage = tenant_config->_tx_share_memory_limit_percentage;
    int64_t memstore_limit_percentage = tenant_config->memstore_limit_percentage;
    int64_t tx_data_limit_percentage = tenant_config->_tx_data_memory_limit_percentage;
    int64_t mds_limit_percentage = tenant_config->_mds_memory_limit_percentage;
    int64_t trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    int64_t max_duration = tenant_config->writing_throttling_maximum_duration;
    if (0 == share_mem_limit_percentage) {
      // 0 means use (memstore_limit + 10)
      share_mem_limit_percentage = memstore_limit_percentage + 10;
    }

    int64_t share_mem_limit = total_memory * share_mem_limit_percentage / 100LL;
    int64_t memstore_limit = total_memory * memstore_limit_percentage / 100LL;
    int64_t tx_data_limit = total_memory * tx_data_limit_percentage / 100LL;
    int64_t mds_limit = total_memory * mds_limit_percentage / 100LL;

    (void)share_resource_throttle_tool_.update_throttle_config<FakeAllocatorForTxShare>(
        share_mem_limit, trigger_percentage, max_duration);
    (void)share_resource_throttle_tool_.update_throttle_config<ObMemstoreAllocator>(
        memstore_limit, trigger_percentage, max_duration);
    (void)share_resource_throttle_tool_.update_throttle_config<ObTenantTxDataAllocator>(
        tx_data_limit, trigger_percentage, max_duration);
    (void)share_resource_throttle_tool_.update_throttle_config<ObTenantMdsAllocator>(
        mds_limit, trigger_percentage, max_duration);

    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              K(share_mem_limit_percentage),
              K(memstore_limit_percentage),
              K(tx_data_limit_percentage),
              K(mds_limit_percentage),
              K(trigger_percentage),
              K(max_duration));
    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              THROTTLE_CONFIG_LOG(FakeAllocatorForTxShare, share_mem_limit, trigger_percentage, max_duration));
    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              THROTTLE_CONFIG_LOG(ObMemstoreAllocator, memstore_limit, trigger_percentage, max_duration));
    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              THROTTLE_CONFIG_LOG(ObTenantTxDataAllocator, tx_data_limit, trigger_percentage, max_duration));
    SHARE_LOG(INFO,
              "[Throttle] Update Config",
              THROTTLE_CONFIG_LOG(ObTenantMdsAllocator, mds_limit, trigger_percentage, max_duration));
  } else {
    SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "invalid tenant config", K(tenant_id), K(total_memory));
  }
}

#undef UPDATE_BY_CONFIG_NAME

}  // namespace share
}  // namespace oceanbase
