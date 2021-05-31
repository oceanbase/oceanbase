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

#define USING_LOG_PREFIX RS
#include "ob_freeze_info_updater.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "common/ob_role.h"
#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/ob_debug_sync.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace rootserver {
ObFreezeInfoUpdater::ObFreezeInfoUpdater()
    : ObRsReentrantThread(true), inited_(false), last_gc_timestamp_(0), freeze_info_manager_(NULL)
{}

ObFreezeInfoUpdater::~ObFreezeInfoUpdater()
{
  if (inited_) {
    stop();
    wait();
  }
}

int ObFreezeInfoUpdater::init(ObFreezeInfoManager& freeze_info_manager)
{
  int ret = OB_SUCCESS;
  static const int64_t freeze_info_updater_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(create(freeze_info_updater_thread_cnt, "FrzInfoUpd"))) {
    LOG_WARN("create thread failed", K(ret), K(freeze_info_updater_thread_cnt));
  } else {
    last_gc_timestamp_ = ObTimeUtility::current_time();
    freeze_info_manager_ = &freeze_info_manager;
    inited_ = true;
  }
  return ret;
}

void ObFreezeInfoUpdater::run3()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("freeze info updater start");
    ObThreadCondGuard guard(get_cond());
    while (!stop_) {
      update_last_run_timestamp();
      ObCurTraceId::init(GCONF.self_addr_);
      if (OB_FAIL(try_gc_snapshot())) {
        LOG_WARN("fail to gc snapshot", K(ret));
      } else if (OB_FAIL(process_invalid_schema_version())) {
        LOG_WARN("fail to process invalid schema version", KR(ret));
      } else if (OB_FAIL(try_update_major_schema_version())) {
        // When the previous round of schema is not determined, do not set the next round of schema;
        LOG_WARN("fail to update major freeze version", K(ret));
      }
      if (OB_FAIL(try_broadcast_freeze_info())) {
        LOG_WARN("fail to broadcase freeze info", K(ret));
      }

      if (OB_FAIL(try_reload_freeze_info())) {
        LOG_WARN("fail to reload freeze info", KR(ret));
      }

      if (OB_FAIL(freeze_info_manager_->check_snapshot_gc_ts())) {
        LOG_WARN("fail to check_snapshot_gc_ts", KR(ret));
      }

      if (!stop_) {
        get_cond().wait(get_schedule_interval() / 1000);
      }
    }
  }
  LOG_INFO("freeze info updater stop");
}

int ObFreezeInfoUpdater::try_broadcast_freeze_info()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(freeze_info_manager_->broadcast_frozen_info())) {
    LOG_WARN("fail to broadcase freeze info", K(ret));
  }
  return ret;
}

int ObFreezeInfoUpdater::try_gc_snapshot()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (now - last_gc_timestamp_ < MODIFY_GC_SNAPSHOT_INTERVAL) {
    // nothing todo
  } else if (OB_FAIL(freeze_info_manager_->renew_snapshot_gc_ts())) {
    LOG_WARN("fail to set snapshot", K(ret));
  } else {
    last_gc_timestamp_ = now;
  }
  return ret;
}

int ObFreezeInfoUpdater::process_invalid_schema_version()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(freeze_info_manager_->process_invalid_schema_version())) {
    LOG_WARN("fail to update schema version", K(ret));
  }
  return ret;
}

int ObFreezeInfoUpdater::try_update_major_schema_version()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(freeze_info_manager_->try_update_major_schema_version())) {
    LOG_WARN("fail to update schema version", K(ret));
  }
  return ret;
}

int ObFreezeInfoUpdater::try_reload_freeze_info()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(freeze_info_manager_->try_reload())) {
    LOG_WARN("fail to update schema version", K(ret));
  }
  return ret;
}

int64_t ObFreezeInfoUpdater::get_schedule_interval() const
{
  return TRY_UPDATER_INTERVAL_US;
}
}  // namespace rootserver
}  // namespace oceanbase
