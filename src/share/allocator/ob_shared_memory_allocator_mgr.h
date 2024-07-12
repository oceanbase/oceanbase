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
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase {
namespace share {

class ObSharedMemAllocMgr {
public:
  ObSharedMemAllocMgr(): share_resource_throttle_tool_(),
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
    } else if (OB_FAIL(tx_data_op_allocator_.init())) {
      SHARE_LOG(ERROR, "init tx data op allocator failed", KR(ret));
    } else if (OB_FAIL(
                   share_resource_throttle_tool_.init(&memstore_allocator_, &tx_data_allocator_, &mds_allocator_))) {
      SHARE_LOG(ERROR, "init share resource throttle tool failed", KR(ret));
    } else {
      tenant_id_ = MTL_ID();
      share_resource_throttle_tool_.enable_adaptive_limit<FakeAllocatorForTxShare>();
      SHARE_LOG(INFO, "finish init mtl share mem allocator mgr", K(tenant_id_), KP(this));
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
  ObTenantTxDataOpAllocator &tx_data_op_allocator() { return tx_data_op_allocator_; }

private:
  void update_share_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_memstore_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_tx_data_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);
  void update_mds_throttle_config_(const int64_t total_memory, omt::ObTenantConfigGuard &config);

private:
  int64_t tenant_id_;
  TxShareThrottleTool share_resource_throttle_tool_;
  ObMemstoreAllocator memstore_allocator_;
  ObTenantTxDataAllocator tx_data_allocator_;
  ObTenantMdsAllocator mds_allocator_;
  ObTenantTxDataOpAllocator tx_data_op_allocator_;
};

class TxShareMemThrottleUtil
{
public:
  static const int64_t SLEEP_INTERVAL_PER_TIME = 20 * 1000; // 20ms;
  template <typename ALLOCATOR>
  static int do_throttle(const bool for_replay,
                         const int64_t abs_expire_time,
                         const int64_t throttle_memory_size,
                         TxShareThrottleTool &throttle_tool,
                         ObThrottleInfoGuard &share_ti_guard,
                         ObThrottleInfoGuard &module_ti_guard)
  {
    int ret = OB_SUCCESS;
    bool has_printed_lbt = false;
    int64_t sleep_time = 0;
    int64_t left_interval = share::ObThrottleUnit<ALLOCATOR>::DEFAULT_MAX_THROTTLE_TIME;

    if (!for_replay) {
      left_interval = min(left_interval, abs_expire_time - ObClockGenerator::getClock());
    }

    uint64_t timeout = 10000;  // 10s
    common::ObWaitEventGuard wait_guard(
        common::ObWaitEventIds::MEMSTORE_MEM_PAGE_ALLOC_WAIT, timeout, 0, 0, left_interval);

    while (throttle_tool.still_throttling<ALLOCATOR>(share_ti_guard, module_ti_guard) &&
           (left_interval > 0)) {
      int64_t expected_wait_time = 0;
      if (for_replay && MTL(ObTenantFreezer *)->exist_ls_throttle_is_skipping()) {
        // skip throttle if ls freeze exists
        break;
      } else if ((expected_wait_time =
                      throttle_tool.expected_wait_time<ALLOCATOR>(share_ti_guard, module_ti_guard)) <= 0) {
        if (expected_wait_time < 0) {
          SHARE_LOG(ERROR,
                    "expected wait time should not smaller than 0",
                    K(expected_wait_time),
                    KPC(share_ti_guard.throttle_info()),
                    KPC(module_ti_guard.throttle_info()),
                    K(clock),
                    K(left_interval));
        }
        break;
      }

      // do sleep when expected_wait_time and left_interval are not equal to 0
      int64_t sleep_interval = min(SLEEP_INTERVAL_PER_TIME, expected_wait_time);
      if (0 < sleep_interval) {
        sleep_time += sleep_interval;
        left_interval -= sleep_interval;
        ::usleep(sleep_interval);
      }

      PrintThrottleUtil::pirnt_throttle_info(ret,
                                             ALLOCATOR::throttle_unit_name(),
                                             sleep_time,
                                             left_interval,
                                             expected_wait_time,
                                             abs_expire_time,
                                             share_ti_guard,
                                             module_ti_guard,
                                             has_printed_lbt);
    }
    PrintThrottleUtil::print_throttle_statistic(ret, ALLOCATOR::throttle_unit_name(), sleep_time, throttle_memory_size);

    if (for_replay && sleep_time > 0) {
      // avoid print replay_timeout
      get_replay_is_writing_throttling() = true;
    }
    return ret;
  }
};

}  // namespace share
}  // namespace oceanbase

#endif
