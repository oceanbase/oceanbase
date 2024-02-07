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

#ifndef OCEANBASE_ALLOCATOR_OB_SHARED_MEMORY_ALLOCATOR_MGR_H_
#define OCEANBASE_ALLOCATOR_OB_SHARED_MEMORY_ALLOCATOR_MGR_H_

#include "share/allocator/ob_memstore_allocator.h"
#include "share/allocator/ob_tx_data_allocator.h"
#include "share/allocator/ob_mds_allocator.h"
#include "share/throttle/ob_share_resource_throttle_tool.h"

namespace oceanbase {
namespace share {

class ObSharedMemAllocMgr {
public:
  ObSharedMemAllocMgr()
      : share_resource_throttle_tool_(),
        memstore_allocator_(),
        tx_data_allocator_(),
        mds_allocator_() {}
  ObSharedMemAllocMgr(ObSharedMemAllocMgr &rhs) = delete;
  ObSharedMemAllocMgr &operator=(ObSharedMemAllocMgr &rhs) = delete;
  ~ObSharedMemAllocMgr() {}

  static int mtl_init(ObSharedMemAllocMgr *&shared_mem_alloc_mgr) { return shared_mem_alloc_mgr->init(); }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tx_data_allocator_.init("TX_DATA_SLICE"))) {
      SHARE_LOG(ERROR, "init tx data allocator failed", KR(ret));
    } else if (OB_FAIL(memstore_allocator_.init())) {
      SHARE_LOG(ERROR, "init memstore allocator failed", KR(ret));
    } else if (OB_FAIL(mds_allocator_.init())) {
      SHARE_LOG(ERROR, "init mds allocator failed", KR(ret));
    } else if (OB_FAIL(
                   share_resource_throttle_tool_.init(&memstore_allocator_, &tx_data_allocator_, &mds_allocator_))) {
      SHARE_LOG(ERROR, "init share resource throttle tool failed", KR(ret));
    } else {
      share_resource_throttle_tool_.enable_adaptive_limit<FakeAllocatorForTxShare>();
      SHARE_LOG(INFO, "finish init mtl share mem allocator mgr", K(MTL_ID()), KP(this));
    }
    return ret;
  }

  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy() {}
  void update_throttle_config();

  ObMemstoreAllocator &memstore_allocator() { return memstore_allocator_; }
  ObTenantTxDataAllocator &tx_data_allocator() { return tx_data_allocator_; }
  ObTenantMdsAllocator &mds_allocator() { return mds_allocator_; }
  TxShareThrottleTool &share_resource_throttle_tool() { return share_resource_throttle_tool_; }

private:
  void update_share_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_memstore_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_tx_data_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_mds_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);

private:
  TxShareThrottleTool share_resource_throttle_tool_;
  ObMemstoreAllocator memstore_allocator_;
  ObTenantTxDataAllocator tx_data_allocator_;
  ObTenantMdsAllocator mds_allocator_;
};

}  // namespace share
}  // namespace oceanbase

#endif