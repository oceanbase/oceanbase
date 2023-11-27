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

#include "rootserver/freeze/ob_tenant_major_freeze.h"

#include "share/ob_errno.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

ObTenantMajorFreeze::ObTenantMajorFreeze(const uint64_t tenant_id)
  : is_inited_(false), tenant_id_(tenant_id), is_primary_service_(true),
    major_merge_info_mgr_(), major_merge_info_detector_(tenant_id),
    merge_scheduler_(tenant_id), daily_launcher_(tenant_id), schema_service_(nullptr)
{
}

ObTenantMajorFreeze::~ObTenantMajorFreeze()
{
}

int ObTenantMajorFreeze::init(
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy,
    ObServerConfig &config,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObIServerTrace &server_trace)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_mgr_.init(tenant_id_, sql_proxy))) {
    LOG_WARN("fail to init major merge info mgr", KR(ret));
  } else if (OB_FAIL(merge_scheduler_.init(is_primary_service, major_merge_info_mgr_,
             schema_service, server_trace, config, sql_proxy))) {
    LOG_WARN("fail to init merge_scheduler", KR(ret), K(is_primary_service));
  }  else if (OB_FAIL(major_merge_info_detector_.init(is_primary_service, sql_proxy,
              major_merge_info_mgr_, merge_scheduler_.get_major_scheduler_idling()))) {
    LOG_WARN("fail to init freeze_info_detector", KR(ret), K(is_primary_service));
  } else if (is_primary_service) {
    if (OB_FAIL(daily_launcher_.init(config, sql_proxy, major_merge_info_mgr_))) {
      LOG_WARN("fail to init daily_launcher", KR(ret), K(is_primary_service));
    }
  }
  if (OB_SUCC(ret)) {
    is_primary_service_ = is_primary_service;
    schema_service_ = &schema_service;
    is_inited_ = true;
  }

  return ret;
}

int ObTenantMajorFreeze::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(major_merge_info_detector_.start())) {
    LOG_WARN("fail to start freeze_info_detector", KR(ret));
  } else if (OB_FAIL(merge_scheduler_.start())) {
    LOG_WARN("fail to start merge_scheduler", KR(ret));
  } else if (is_primary_service()) {
    if (OB_FAIL(daily_launcher_.start())) {
      LOG_WARN("fail to start daily_launcher", KR(ret), K_(tenant_id), K_(is_primary_service));
    }
  }
  return ret;
}

void ObTenantMajorFreeze::stop()
{
  if (is_primary_service()) {
    LOG_INFO("daily_launcher start to stop", K_(tenant_id), K_(is_primary_service));
    daily_launcher_.stop();
  }
  LOG_INFO("freeze_info_detector start to stop", K_(tenant_id), K_(is_primary_service));
  major_merge_info_detector_.stop();
  LOG_INFO("merge_scheduler start to stop", K_(tenant_id), K_(is_primary_service));
  merge_scheduler_.stop();
}

int ObTenantMajorFreeze::wait()
{
  int ret = OB_SUCCESS;
  if (is_primary_service()) {
    LOG_INFO("daily_launcher start to wait", K_(tenant_id), K_(is_primary_service));
    daily_launcher_.wait();
  }
  LOG_INFO("freeze_info_detector start to wait", K_(tenant_id), K_(is_primary_service));
  major_merge_info_detector_.wait();
  LOG_INFO("merge_scheduler start to wait", K_(tenant_id), K_(is_primary_service));
  merge_scheduler_.wait();
  return ret;
}

int ObTenantMajorFreeze::destroy()
{
  int ret = OB_SUCCESS;
  if (is_primary_service()) {
    LOG_INFO("daily_launcher start to destroy", K_(tenant_id), K_(is_primary_service));
    if (OB_FAIL(daily_launcher_.destroy())) {
      LOG_WARN("fail to destroy daily_launcher", KR(ret), K_(tenant_id), K_(is_primary_service));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("freeze_info_detector start to destroy", K_(tenant_id), K_(is_primary_service));
    if (OB_FAIL(major_merge_info_detector_.destroy())) {
      LOG_WARN("fail to destroy freeze_info_detector", KR(ret), K_(tenant_id), K_(is_primary_service));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("merge_scheduler start to destroy", K_(tenant_id), K_(is_primary_service));
    if (OB_FAIL(merge_scheduler_.destroy())) {
      LOG_WARN("fail to destroy merge_scheduler", KR(ret), K_(tenant_id), K_(is_primary_service));
    }
  }
  return ret;
}

void ObTenantMajorFreeze::pause()
{
  if (is_primary_service()) {
    daily_launcher_.pause();
  }
  major_merge_info_detector_.pause();
  merge_scheduler_.pause();
}

void ObTenantMajorFreeze::resume()
{
  if (is_primary_service()) {
    daily_launcher_.resume();
  }
  major_merge_info_detector_.resume();
  merge_scheduler_.resume();
}

bool ObTenantMajorFreeze::is_paused() const
{
  bool is_paused = (major_merge_info_detector_.is_paused() || merge_scheduler_.is_paused());
  if (is_primary_service()) {
    is_paused = (is_paused || daily_launcher_.is_paused());
  }
  return is_paused;
}

int ObTenantMajorFreeze::set_freeze_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_mgr_.set_freeze_info())) {
    LOG_WARN("fail to set_freeze_info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTenantMajorFreeze::launch_major_freeze()
{
  int ret = OB_SUCCESS;
  LOG_INFO("launch_major_freeze", K_(tenant_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_tenant_status())) {
    LOG_WARN("fail to check tenant status", KR(ret), K_(tenant_id));
  } else if (!GCONF.enable_major_freeze) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("enable_major_freeze is off, refuse to to major_freeze",
             K_(tenant_id), KR(ret));
  } else if (merge_scheduler_.is_paused()) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader may switch", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_scheduler_.try_update_epoch_and_reload())) {
    LOG_WARN("fail to try_update_epoch_and_reload", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_freeze_info())) {
    LOG_WARN("fail to check freeze info", KR(ret), K_(tenant_id));
    if ((OB_MAJOR_FREEZE_NOT_FINISHED == ret) || (OB_FROZEN_INFO_ALREADY_EXIST == ret)) {
      LOG_INFO("should not launch major freeze again", KR(ret), K_(tenant_id));
    } else {
      LOG_WARN("fail to check freeze info", KR(ret), K_(tenant_id));
    }
  } else if (OB_FAIL(set_freeze_info())) {
    LOG_WARN("fail to set_freeze_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_detector_.signal())) {
    LOG_WARN("fail to signal", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTenantMajorFreeze::suspend_merge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (merge_scheduler_.is_paused()) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader may switch", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().try_reload())) {
    LOG_WARN("fail to try reload zone_merge_mgr", KR(ret), K_(tenant_id));
  } else {
    const int64_t expected_epoch = merge_scheduler_.get_epoch();
    // in case of observer start or restart, before the MajorMergeScheduler background thread
    // successfully update freeze_service_epoch, the epoch in memory is equal to -1.
    if (-1 == expected_epoch) {
      ret = OB_EAGAIN;
      LOG_WARN("epoch has not been updated, will retry", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().suspend_merge(expected_epoch))) {
      LOG_WARN("fail to suspend merge", KR(ret), K_(tenant_id), K(expected_epoch));
    }
  }
  return ret;
}

int ObTenantMajorFreeze::resume_merge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (merge_scheduler_.is_paused()) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader may switch", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().try_reload())) {
    LOG_WARN("fail to try reload zone_merge_mgr", KR(ret), K_(tenant_id));
  } else {
    const int64_t expected_epoch = merge_scheduler_.get_epoch();
    // in case of observer start or restart, before the MajorMergeScheduler background thread
    // successfully update freeze_service_epoch, the epoch in memory is equal to -1.
    if (-1 == expected_epoch) {
      ret = OB_EAGAIN;
      LOG_WARN("epoch has not been updated, will retry", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().resume_merge(expected_epoch))) {
      LOG_WARN("fail to resume merge", KR(ret), K_(tenant_id), K(expected_epoch));
    }
  }
  return ret;
}

int ObTenantMajorFreeze::clear_merge_error()
{
  int ret = OB_SUCCESS;
  const ObZoneMergeInfo::ObMergeErrorType error_type = ObZoneMergeInfo::ObMergeErrorType::NONE_ERROR;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (merge_scheduler_.is_paused()) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader may switch", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().try_reload())) {
    LOG_WARN("fail to try reload zone_merge_mgr", KR(ret), K_(tenant_id));
  } else {
    const int64_t expected_epoch = merge_scheduler_.get_epoch();
    // in case of observer start or restart, before the MajorMergeScheduler background thread
    // successfully update freeze_service_epoch, the epoch in memory is equal to -1.
    if (-1 == expected_epoch) {
      ret = OB_EAGAIN;
      LOG_WARN("epoch has not been updated, will retry", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_status(tenant_id_,
                                                                         expected_epoch))) {
      LOG_WARN("fail to batch update status", KR(ret), K_(tenant_id), K(expected_epoch));
    } else if (OB_FAIL(major_merge_info_mgr_.get_zone_merge_mgr().set_merge_error(error_type, expected_epoch))) {
      LOG_WARN("fail to set merge error", KR(ret), K_(tenant_id), K(error_type), K(expected_epoch));
    }
  }
  return ret;
}

int ObTenantMajorFreeze::get_uncompacted_tablets(
    ObArray<ObTabletReplica> &uncompacted_tablets,
    ObArray<uint64_t> &uncompacted_table_ids) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(merge_scheduler_.get_uncompacted_tablets(uncompacted_tablets, uncompacted_table_ids))) {
      LOG_WARN("fail to get uncompacted tablets", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObTenantMajorFreeze::check_tenant_status() const
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get simple tenant schema", KR(ret));
  } else if ((nullptr == tenant_schema) || !tenant_schema->is_normal()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("tenant is not normal status", KR(ret), K_(tenant_id), KPC(tenant_schema));
  }
  return ret;
}

int ObTenantMajorFreeze::check_freeze_info()
{
  int ret = OB_SUCCESS;
  SCN latest_frozen_scn;
  SCN global_last_merged_scn;
  ObZoneMergeInfo::MergeStatus global_merge_status = ObZoneMergeInfo::MergeStatus::MERGE_STATUS_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(major_merge_info_mgr_.get_local_latest_frozen_scn(latest_frozen_scn))) {
    LOG_WARN("fail to get local latest frozen_scn", KR(ret), K_(tenant_id));
  } else {
    ObZoneMergeManager &zone_merge_mgr = major_merge_info_mgr_.get_zone_merge_mgr();
    if (OB_FAIL(zone_merge_mgr.try_reload())) {
      LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(zone_merge_mgr.get_global_last_merged_scn(global_last_merged_scn))) {
      LOG_WARN("fail to get global_last_merged_scn", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(zone_merge_mgr.get_global_merge_status(global_merge_status))) {
      LOG_WARN("fail to get_global_merge_status", KR(ret), K_(tenant_id));
    } else {
      // check pending freeze_info
      if (latest_frozen_scn > global_last_merged_scn) {
        if (global_merge_status == ObZoneMergeInfo::MergeStatus::MERGE_STATUS_IDLE) {
          ret = OB_FROZEN_INFO_ALREADY_EXIST;
        } else {
          ret = OB_MAJOR_FREEZE_NOT_FINISHED;
        }
        LOG_WARN("cannot do major freeze now, need wait current major_freeze finish", KR(ret),
                K(global_last_merged_scn), K(latest_frozen_scn), K_(tenant_id));
      } else if (merge_scheduler_.is_paused()) {
        ret = OB_LEADER_NOT_EXIST;
        LOG_WARN("leader may switch", KR(ret), K_(tenant_id));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
