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
#include "ob_table_throttle_manager.h"
#include "ob_table_throttle.h"
#include "ob_table_hotkey.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::table;



/**
 * -----------------------------------ObTableThrottleTimerTask-----------------------------------
 */

/**
  * every periodic_delay_, timer will run this function
  */
void ObTableThrottleTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  // next epoch using new infra
  ++OBTableThrottleMgr::epoch_;

  ObTableHotKeyThrottle &hotkey_throttle = ObTableHotKeyThrottle::get_instance();
  ObTableHotKeyMgr &hotkey_mgr = ObTableHotKeyMgr::get_instance();

  // add throttle key, get each tenant topk hot key
  for (ObTableTenantHotKeyMap::iterator iter = hotkey_mgr.get_tenant_map().begin();
       OB_SUCC(ret) && iter != hotkey_mgr.get_tenant_map().end(); ++iter) {
    uint64_t tenant_id = iter->first;
    ObTableTenantHotKey *tenant_hotkey = iter->second;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;
    ObTableHotkeyPair *hotkey_pair;
    double average_cnt = 0.0;
    int64_t hotkey_cnt = 0;

    if (throttle_threshold <= 0) {
      // throttle function is closed in this tenant
      continue;
    }
    ObTableTenantHotKey::ObTableTenantHotKeyInfra &infra = tenant_hotkey->infra_[(OBTableThrottleMgr::epoch_ - 1) % ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM];
    DRWLock::WRLockGuard guard(infra.rwlock_);
    // traverse all hotkey in a tenant's hotkey heap, need to deal with all hotkey from heap
    for (int64_t item = 0; item < infra.hotkey_heap_.get_count(); ++item) {
      hotkey_pair = infra.hotkey_heap_.get_heap()[item];
      ObTableHotKey *hotkey = nullptr;
      if (OB_ISNULL(hotkey = hotkey_pair->first)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unpected null hotkey pointer", K(OBTableThrottleMgr::epoch_), K(ret));
      } else if (hotkey->type_ != TABLE_HOTKEY_ALL || hotkey->epoch_ != OBTableThrottleMgr::epoch_-1) {
        // Now try_add_throttle_key will not recognize hotkey type
        // To avoid add throttle repeatly, count TABLE_HOTKEY_ALL only
        continue;
      }

      // refresh all hotkey in heap, since data from heap is not the true value
      if (OB_NOT_NULL(hotkey) && OB_FAIL(infra.hotkey_map_.get_refactored(hotkey, hotkey_cnt))) {
        LOG_ERROR("fail to get hotkey from map", K(hotkey), K(ret));
        // if could not get latest value from map, then deal with next key
        continue;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(tenant_hotkey->add_hotkey_slide_window(*hotkey, hotkey_cnt))) {
          // add hotkey to slide window
          LOG_WARN("fail to add hotkey into slide_window", K(ret));
        } else if (OB_FAIL(tenant_hotkey->get_slide_window_cnt(hotkey, average_cnt))){
          // get average from slide window
          LOG_WARN("fail to get the average cnt of hotkey", KPC(hotkey), K(ret));
        } else if (average_cnt >= (throttle_threshold * hotkey_throttle.KV_HOTKEY_STAT_RATIO)) {
          // since we only count KV_HOTKEY_STAT_RATIO of input, we should multi threshold by KV_HOTKEY_STAT_RATIO
          // add new hotkey / refresh old hotkey
          ObTableThrottleKey throttle_hotkey;

          if (OB_FAIL(hotkey_throttle.create_throttle_hotkey(*hotkey, throttle_hotkey))) {
            LOG_WARN("fail to create throttle key from table hotkey", K(ret));
          } else if (OB_FAIL(hotkey_throttle.try_add_throttle_key(throttle_hotkey))) {
            LOG_WARN("fail to add throttle key into hotkey throttle", K(ret));
          }
        }
      }
    }
  }

  // remove old throttle key
  hotkey_throttle.refresh(OBTableThrottleMgr::epoch_ - 1);

  // clean hotkey managers' heap and map
  // Also, refresh slide window here
  hotkey_mgr.refresh();

  // add new tenant
  if (OB_SUCC(ret) && OB_FAIL(hotkey_mgr.refresh_tenant_map())) {
    LOG_WARN("fail to refresh tenant map", K(OBTableThrottleMgr::epoch_), K(ret));
  }
}

/**
 * -----------------------------------OBTableThrottleMgr-----------------------------------
 */

uint64_t OBTableThrottleMgr::epoch_ = 0;
int64_t OBTableThrottleMgr::inited_ = 0;
OBTableThrottleMgr *OBTableThrottleMgr::instance_ = nullptr;

OBTableThrottleMgr& OBTableThrottleMgr::get_instance()
{
  OBTableThrottleMgr *instance = NULL;
  while(OB_UNLIKELY(inited_ < 2)) {
    if (ATOMIC_BCAS(&inited_, 0, 1)) {
      instance = OB_NEW(OBTableThrottleMgr, ObModIds::OB_TABLE_THROTTLE_MGR);
      if (OB_LIKELY(OB_NOT_NULL(instance))) {
        if (common::OB_SUCCESS != instance->init()) {
          LOG_WARN("failed to init OBTableThrottle Manager");
          OB_DELETE(OBTableThrottleMgr, ObModIds::OB_TABLE_THROTTLE_MGR, instance);
          instance = NULL;
          ATOMIC_BCAS(&inited_, 1, 0);
        } else if (common::OB_SUCCESS != instance->start()) {
          LOG_WARN("failed to init OBTableThrottle Manager");
          OB_DELETE(OBTableThrottleMgr, ObModIds::OB_TABLE_THROTTLE_MGR, instance);
          instance = NULL;
          ATOMIC_BCAS(&inited_, 1, 0);
        } else {
          instance_ = instance;
          (void)ATOMIC_BCAS(&inited_, 1, 2);
        }
      } else {
        (void)ATOMIC_BCAS(&inited_, 1, 0);
      }
    }
  }
  return *(OBTableThrottleMgr *)instance_;
}

int OBTableThrottleMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(throttle_timer_.init("TableTrtTimer"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    epoch_ = 0;
    LOG_INFO("success to init table throttle timer manager");
  }
  return ret;
}

int OBTableThrottleMgr::start()
{
  int ret = OB_SUCCESS;
  if (is_timer_start_) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("fail to start throttle timer manager again", K(ret));
  } else if (OB_FAIL(throttle_timer_.schedule(periodic_task_, periodic_delay_, true))) {
    LOG_WARN("fail to schedule periodic task", K(ret));
  } else {
    is_timer_start_ = true;
    LOG_INFO("table throttle manager is started");
  }
  return ret;
}

void OBTableThrottleMgr::stop()
{
  int ret = OB_SUCCESS;
  if (inited_ != 2) {
    LOG_WARN("throttle timer manager not init", K(ret));
  } else if (is_timer_start_) {
    throttle_timer_.cancel_all();
    is_timer_start_ = false;
    LOG_INFO("throttle timer manager is stoped");
  }
}

void OBTableThrottleMgr::destroy()
{
  stop();
  throttle_timer_.destroy();
  ObTableHotKeyThrottle::get_instance().destroy();
  OB_DELETE(OBTableThrottleMgr, ObModIds::OB_TABLE_THROTTLE_MGR, instance_);
  inited_ = 0;
  LOG_INFO("throttle timer manager is destoried");
}