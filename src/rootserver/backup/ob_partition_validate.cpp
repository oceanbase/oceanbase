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
#include "rootserver/backup/ob_partition_validate.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_validate_task_updater.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_tenant_validate_task_updater.h"
#include "lib/container/ob_iarray.h"
#include "lib/random/ob_random.h"
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace rootserver {

int ObBackupValidateLoadBalancer::init(rootserver::ObServerManager& server_mgr, rootserver::ObZoneManager& zone_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_SUCCESS;
    LOG_WARN("init twice", K(ret));
  } else {
    server_mgr_ = &server_mgr;
    zone_mgr_ = &zone_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateLoadBalancer::get_next_server(common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  char backup_region[MAX_REGION_LENGTH] = "";
  ObServerManager::ObServerArray server_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GCONF.backup_region.copy(backup_region, sizeof(backup_region)))) {
    LOG_WARN("failed to copy backup region", K(ret));
  } else if (OB_FAIL(get_all_server_in_region(backup_region, server_list))) {
    LOG_WARN("failed to get all server in region", K(ret));
  } else if (server_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server list is empty", K(ret));
  } else if (OB_FAIL(choose_server(server_list, server))) {
    LOG_WARN("failed to choose server");
  }
  return ret;
}

int ObBackupValidateLoadBalancer::get_all_server_in_region(
    const common::ObRegion& region, common::ObIArray<ObAddr>& server_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate load balancer do not init", K(ret));
  } else if (OB_FAIL(get_zone_list(region, zone_list))) {
    LOG_WARN("failed to get zone list", K(ret), K(region));
  } else {
    int64_t alive_server_count = 0;
    ObArray<ObAddr> tmp_server_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      const ObZone& zone = zone_list.at(i);
      if (OB_FAIL(server_mgr_->get_alive_servers(zone, tmp_server_list))) {
        LOG_WARN("failed to get alive servers", K(ret), K(zone));
      } else if (OB_FAIL(server_mgr_->get_alive_server_count(zone, alive_server_count))) {
        LOG_WARN("failed to get alive server count", K(ret), K(zone));
      } else if (alive_server_count != tmp_server_list.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected", K(ret), K(alive_server_count), K(tmp_server_list.count()));
      } else if (OB_FAIL(append(server_list, tmp_server_list))) {
        LOG_WARN("failed to add array", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateLoadBalancer::get_zone_list(
    const common::ObRegion& region, common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  if (region.is_empty()) {
    if (OB_FAIL(zone_mgr_->get_zone(zone_list))) {
      LOG_WARN("failed to get zone list", K(ret));
    }
  } else {
    if (OB_FAIL(zone_mgr_->get_zone(region, zone_list))) {
      LOG_WARN("failed to get zone list in region", K(ret), K(region));
    }
  }
  return ret;
}

int ObBackupValidateLoadBalancer::choose_server(
    const common::ObIArray<common::ObAddr>& server_list, common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  bool choose = false;
  ObRandom rand;
  const int64_t server_cnt = server_list.count();
  while (!choose) {
    int64_t res = rand.get(0, server_cnt - 1);
    common::ObAddr tmp_server;
    tmp_server = server_list.at(res);
    server = tmp_server;
    choose = true;
    LOG_INFO("choose server to do validation task success", K(ret), K(server));
  }
  return ret;
}

ObPartitionValidate::ObPartitionValidate()
    : is_inited_(false), sql_proxy_(NULL), task_mgr_(NULL), server_mgr_(NULL), zone_mgr_(NULL)
{}

ObPartitionValidate::~ObPartitionValidate()
{}

int ObPartitionValidate::init(
    common::ObMySQLProxy& sql_proxy, ObRebalanceTaskMgr& task_mgr, ObServerManager& server_mgr, ObZoneManager& zone_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition validate init twice", K(ret));
  } else if (OB_FAIL(pg_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init pg task updater", K(ret));
  } else if (OB_FAIL(load_balancer_.init(server_mgr, zone_mgr))) {
    LOG_WARN("failed to init load balancer", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    task_mgr_ = &task_mgr;
    server_mgr_ = &server_mgr;
    zone_mgr_ = &zone_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionValidate::partition_validate(const uint64_t tenant_id, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  task_cnt = 0;
  ObBackupValidateTenant validate_tenant;
  validate_tenant.tenant_id_ = tenant_id;
  validate_tenant.is_dropped_ = false;
  ObArray<int64_t> backup_set_ids;
  ObArray<ObTenantValidateTaskInfo> tenant_task_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition validate get invalid argument", K(ret), K(validate_tenant));
  } else if (OB_FAIL(get_not_finished_tenant_validate_task(validate_tenant, tenant_task_infos))) {
    LOG_WARN("failed to get not finished tenant validate task", K(ret), K(validate_tenant));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_task_infos.count(); ++i) {
      const int64_t job_id = tenant_task_infos.at(i).job_id_;
      if (OB_FAIL(validate_pg(job_id, validate_tenant))) {
        LOG_WARN("failed to validate all pg", K(ret), K(job_id), K(validate_tenant));
      } else {
        LOG_INFO("do partition validate success", K(ret), K(job_id), K(validate_tenant));
      }
    }
    if (OB_SUCC(ret)) {
      task_cnt = tenant_task_infos.count();
    }
  }
  return ret;
}

int ObPartitionValidate::get_backup_validate_task_info(const int64_t job_id, ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupValidateTaskUpdater validate_updater;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup validate job info get invalid argument", K(ret), K(job_id));
  } else if (OB_FAIL(validate_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup validate task updater", K(ret));
  } else if (OB_FAIL(validate_updater.get_task(job_id, task_info))) {
    LOG_WARN("failed to get backup validate task info", K(ret), K(job_id));
  }
  return ret;
}

int ObPartitionValidate::get_log_archive_backup_info_(const int64_t snapshot_version,
    const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos, share::ObLogArchiveBackupInfo& log_info)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObLogArchiveBackupInfo>::const_iterator ArrayIter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (log_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log infos should not be empty", K(ret));
  } else {
    CompareLogArchiveSnapshotVersion cmp;
    ArrayIter iter = std::lower_bound(log_infos.begin(), log_infos.end(), snapshot_version, cmp);
    if (iter == log_infos.end()) {
      --iter;
    } else if (iter != log_infos.begin() && iter->status_.start_ts_ > snapshot_version) {
      --iter;
    }
    log_info = *iter;
  }
  return ret;
}

int ObPartitionValidate::get_tenant_backup_task_info_(const int64_t backup_set_id,
    const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, share::ObTenantBackupTaskInfo& backup_info)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObTenantBackupTaskInfo>::const_iterator ArrayIter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (backup_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup infos should not be empty", K(ret));
  } else {
    CompareTenantBackupTaskBackupSetId cmp;
    ArrayIter iter = std::lower_bound(backup_infos.begin(), backup_infos.end(), backup_set_id, cmp);
    if (iter == backup_infos.end() || backup_set_id != iter->backup_set_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup info should exist", K(ret), K(backup_set_id));
    } else {
      backup_info = *iter;
    }
  }
  return ret;
}

int ObPartitionValidate::get_all_tenant_log_archive_infos(
    const uint64_t tenant_id, common::ObArray<share::ObLogArchiveBackupInfo>& log_infos)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  ObLogArchiveBackupInfoMgr log_archive_mgr;
  ObLogArchiveBackupInfo cur_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(log_archive_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, cur_info))) {
    LOG_WARN("failed to get log achive backup info", K(ret), K(tenant_id));
  } else if (OB_FAIL(log_archive_mgr.get_log_archvie_history_infos(*sql_proxy_, tenant_id, for_update, log_infos))) {
    LOG_WARN("failed to get log archive history infos", K(ret), K(tenant_id));
  } else if (OB_FAIL(log_infos.push_back(cur_info))) {
    LOG_WARN("failed to push back current log archive backup info", K(ret), K(tenant_id), K(cur_info));
  } else {
    CompareLogArchiveBackupInfo cmp;
    std::sort(log_infos.begin(), log_infos.end(), cmp);
  }
  return ret;
}

int ObPartitionValidate::get_all_tenant_backup_task_infos(
    const uint64_t tenant_id, common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos)
{
  int ret = OB_SUCCESS;
  backup_infos.reset();
  ObTenantBackupTaskUpdater task_updater;
  ObBackupTaskHistoryUpdater history_updater;
  ObTenantBackupTaskInfo tenant_backup_task;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (OB_FAIL(task_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant backup task updater", K(ret), K(tenant_id));
  } else if (OB_FAIL(history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(task_updater.get_tenant_backup_task(tenant_id, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id));
  } else if (OB_FAIL(history_updater.get_tenant_backup_tasks(tenant_id, backup_infos))) {
    LOG_WARN("failed to get tenant backup history tasks", K(ret));
  } else if (OB_FAIL(backup_infos.push_back(tenant_backup_task))) {
    LOG_WARN("failed to push back tenant backup task", K(ret), K(tenant_id), K(tenant_backup_task));
  } else {
    CompareTenantBackupTaskInfo cmp;
    std::sort(backup_infos.begin(), backup_infos.end(), cmp);
  }
  return ret;
}

int ObPartitionValidate::get_extern_backup_set_infos(const int64_t backup_set_id,
    const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, int64_t& full_backup_set_id,
    int64_t& inc_backup_set_id, int64_t& cluster_version)
{
  int ret = OB_SUCCESS;
  full_backup_set_id = 0;
  inc_backup_set_id = 0;
  cluster_version = 0;
  typedef ObArray<ObTenantBackupTaskInfo>::const_iterator ArrayIter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_id));
  } else {
    CompareTenantBackupTaskBackupSetId cmp;
    ArrayIter iter = std::lower_bound(backup_infos.begin(), backup_infos.end(), backup_set_id, cmp);
    if (iter == backup_infos.end()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find previous backup set info", K(ret), K(backup_set_id), K(backup_infos));
    } else {
      if (ObBackupType::FULL_BACKUP == iter->backup_type_.type_) {
        full_backup_set_id = backup_set_id;
        inc_backup_set_id = backup_set_id;
        cluster_version = iter->cluster_version_;
      } else if (ObBackupType::INCREMENTAL_BACKUP == iter->backup_type_.type_) {
        full_backup_set_id = iter->prev_full_backup_set_id_;
        inc_backup_set_id = backup_set_id;
        cluster_version = iter->cluster_version_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ObBackupType", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionValidate::check_need_validate_clog(const share::ObBackupValidateTaskInfo& task_info,
    const share::ObPGValidateTaskInfo& pg_task_info, const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos,
    const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, bool& need_validate_clog)
{
  int ret = OB_SUCCESS;
  need_validate_clog = true;
  bool in_log_archive_round = false;
  bool in_same_log_archive_round = false;
  int64_t snapshot_version = 0;
  share::ObTenantBackupTaskInfo backup_info;
  const int64_t backup_set_id = pg_task_info.backup_set_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (OB_ALL_BACKUP_SET_ID != task_info.backup_set_id_) {
    LOG_INFO("need validate clog when only one backup set id is specified", K(ret), K(task_info));
  } else if (OB_FAIL(get_tenant_backup_task_info_(backup_set_id, backup_infos, backup_info))) {
    LOG_WARN("failed to get tenant backup task info", K(ret), K(pg_task_info));
  } else if (FALSE_IT(snapshot_version = backup_info.snapshot_version_)) {
    // set snapshot version of backup set id
  } else if (OB_FAIL(check_snapshot_version_in_log_archive_round(snapshot_version, log_infos, in_log_archive_round))) {
    LOG_WARN("failed to check snapshot version in log archive round", K(ret));
  } else if (OB_FAIL(check_prev_backup_set_id_in_same_log_archive_round(
                 pg_task_info.backup_set_id_, log_infos, backup_infos, in_same_log_archive_round))) {
    LOG_WARN("failed to check prev backup set id in same log archive round", K(ret));
  } else {
    if (in_same_log_archive_round || !in_log_archive_round) {
      need_validate_clog = false;
    }
  }
  LOG_INFO("need validate clog",
      K(ret),
      K(need_validate_clog),
      K(in_same_log_archive_round),
      K(in_log_archive_round),
      K(pg_task_info.table_id_),
      K(pg_task_info.partition_id_));
  return ret;
}

int ObPartitionValidate::check_snapshot_version_in_log_archive_round(const int64_t snapshot_version,
    const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos, bool& in_log_archive_round)
{
  int ret = OB_SUCCESS;
  in_log_archive_round = false;
  share::ObLogArchiveBackupInfo log_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (log_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(get_log_archive_backup_info_(snapshot_version, log_infos, log_info))) {
    LOG_WARN("failed to get log archive backup info of snapshot version");
  } else {
    if (log_info.status_.start_ts_ <= snapshot_version && log_info.status_.checkpoint_ts_ >= snapshot_version) {
      in_log_archive_round = true;
    }
  }
  return ret;
}

int ObPartitionValidate::get_prev_validate_backup_set_id(const share::ObTenantBackupTaskInfo& backup_info,
    const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, int64_t& prev_backup_set_id)
{
  int ret = OB_SUCCESS;
  prev_backup_set_id = 0;
  typedef ObArray<ObTenantBackupTaskInfo>::const_iterator ArrayIter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (backup_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup infos should not be empty", K(ret));
  } else {
    CompareTenantBackupTaskBackupSetId cmp;
    ArrayIter iter;
    bool is_mark_deleted = true;
    int64_t backup_set_id = backup_info.backup_set_id_;
    iter = std::lower_bound(backup_infos.begin(), backup_infos.end(), backup_set_id, cmp);
    if (iter == backup_infos.end()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find previous backup set info", K(ret));
    } else {
      --iter;
      while (OB_SUCC(ret) && is_mark_deleted && iter >= backup_infos.begin()) {
        is_mark_deleted = iter->is_mark_deleted_;
        if (!is_mark_deleted) {
          prev_backup_set_id = iter->backup_set_id_;
        } else {
          --iter;
        }
      }
    }
  }
  return ret;
}

int ObPartitionValidate::check_prev_backup_set_id_in_same_log_archive_round(const int64_t cur_backup_set_id,
    const common::ObArray<share::ObLogArchiveBackupInfo>& log_infos,
    const common::ObArray<share::ObTenantBackupTaskInfo>& backup_infos, bool& in_same_log_archive_round)
{
  int ret = OB_SUCCESS;
  in_same_log_archive_round = false;
  ObTenantBackupTaskInfo backup_info;
  ObLogArchiveBackupInfo log_info;
  ObTenantBackupTaskInfo prev_backup_info;
  ObLogArchiveBackupInfo prev_log_info;
  int64_t prev_backup_set_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (OB_FAIL(get_tenant_backup_task_info_(cur_backup_set_id, backup_infos, backup_info))) {
    LOG_WARN("failed to get tenant backup task info", K(ret), K(cur_backup_set_id));
  } else if (OB_FAIL(get_prev_validate_backup_set_id(backup_info, backup_infos, prev_backup_set_id))) {
    LOG_WARN("failed to get prev validate backup set id", K(ret));
  } else if (0 == prev_backup_set_id) {
    // do nothing
  } else if (OB_FAIL(get_tenant_backup_task_info_(prev_backup_set_id, backup_infos, prev_backup_info))) {
    LOG_WARN("failed to get prev tenant backup task info", K(ret), K(cur_backup_set_id));
  } else if (OB_FAIL(get_log_archive_backup_info_(backup_info.snapshot_version_, log_infos, log_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (OB_FAIL(get_log_archive_backup_info_(prev_backup_info.snapshot_version_, log_infos, prev_log_info))) {
    LOG_WARN("failed to get prev log archive backup info", K(ret));
  } else {
    if (log_info.status_.round_ == prev_log_info.status_.round_) {
      in_same_log_archive_round = true;
    }
  }
  return ret;
}

int ObPartitionValidate::get_need_validate_backup_set_ids(
    const int64_t job_id, const ObBackupValidateTenant& validate_tenant, common::ObIArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  ObArray<ObTenantValidateTaskInfo> tenant_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(validate_tenant));
  } else if (OB_FAIL(get_tenant_validate_infos(job_id, validate_tenant, OB_START_INCARNATION, tenant_infos))) {
    LOG_WARN("failed to get tenant validate infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_infos.count(); ++i) {
      const ObTenantValidateTaskInfo& task_info = tenant_infos.at(i);
      if (ObTenantValidateTaskInfo::SCHEDULE == task_info.status_) {
        if (OB_FAIL(backup_set_ids.push_back(task_info.backup_set_id_))) {
          LOG_WARN("failed to push back backup set id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionValidate::get_tenant_validate_info(const int64_t job_id, const ObBackupValidateTenant& validate_tenant,
    const int64_t incarnation, share::ObTenantValidateTaskInfo& tenant_task_info)
{
  int ret = OB_SUCCESS;
  tenant_task_info.reset();
  ObTenantValidateTaskUpdater tenant_task_updater;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid() || job_id <= 0 || incarnation <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(validate_tenant), K(job_id), K(incarnation));
  } else if (OB_FAIL(tenant_task_updater.init(*sql_proxy_, is_dropped_tenant))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(tenant_task_updater.get_task(job_id, validate_tenant.tenant_id_, incarnation, tenant_task_info))) {
    LOG_WARN("failed to get tenant validate task info", K(ret), K(validate_tenant.tenant_id_));
  }
  return ret;
}

int ObPartitionValidate::get_tenant_validate_infos(const int64_t job_id, const ObBackupValidateTenant& validate_tenant,
    const int64_t incarnation, common::ObIArray<share::ObTenantValidateTaskInfo>& tenant_task_infos)
{
  int ret = OB_SUCCESS;
  tenant_task_infos.reset();
  ObTenantValidateTaskUpdater tenant_task_updater;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid() || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(validate_tenant), K(incarnation));
  } else if (OB_FAIL(tenant_task_updater.init(*sql_proxy_, is_dropped_tenant))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(tenant_task_updater.get_tasks(job_id, validate_tenant.tenant_id_, tenant_task_infos))) {
    LOG_WARN("failed to get tenant validate task info", K(ret), K(validate_tenant.tenant_id_));
  }
  return ret;
}

int ObPartitionValidate::validate_pg(const int64_t job_id, const ObBackupValidateTenant& validate_tenant)
{
  int ret = OB_SUCCESS;
  ObBackupValidateTaskInfo validate_job;
  ObTenantValidateTaskInfo tenant_task_info;
  ObArray<ObPGValidateTaskInfo> pg_task_infos;
  ObArray<ObBackupSetPGTaskList> backup_set_pg_list;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid() || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job_id), K(validate_tenant));
  } else if (OB_FAIL(get_tenant_validate_info(job_id, validate_tenant, OB_START_INCARNATION, tenant_task_info))) {
    LOG_WARN("failed to get tenant validate info", K(ret), K(job_id), K(validate_tenant));
  } else if (ObTenantValidateTaskInfo::FINISHED == tenant_task_info.status_) {
    // do nothing
  } else if (OB_FAIL(get_pending_pg_validate_task(job_id, validate_tenant, pg_task_infos))) {
    LOG_WARN("failed to get pg validate task", K(ret), K(job_id), K(validate_tenant));
  } else if (pg_task_infos.empty()) {
    // nothing to do
  } else if (OB_FAIL(get_backup_validate_task_info(job_id, validate_job))) {
    LOG_WARN("failed to get backup validate task info", K(ret), K(job_id));
  } else if (OB_FAIL(generate_batch_pg_task(is_dropped_tenant, validate_job, tenant_task_info, pg_task_infos))) {
    LOG_WARN("failed to generate batch pg task", K(ret), K(validate_tenant));
  }
  return ret;
}

int ObPartitionValidate::get_not_finished_tenant_validate_task(
    const ObBackupValidateTenant& validate_tenant, common::ObIArray<share::ObTenantValidateTaskInfo>& tenant_task_infos)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater tenant_task_updater;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get not finished tenant validate task get invalid argument", K(validate_tenant));
  } else if (OB_FAIL(tenant_task_updater.init(*sql_proxy_, is_dropped_tenant))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(tenant_task_updater.get_not_finished_tasks(validate_tenant.tenant_id_, tenant_task_infos))) {
    LOG_WARN("failed to get not finished tasks", K(ret));
  }
  return ret;
}

int ObPartitionValidate::get_pending_pg_validate_task(const int64_t job_id,
    const ObBackupValidateTenant& validate_tenant, common::ObIArray<ObBackupSetPGTaskList>& backup_set_pg_list)
{
  int ret = OB_SUCCESS;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  ObArray<share::ObPGValidateTaskInfo> pg_task_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid() || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get extern pg list get invalid arguments", K(ret), K(validate_tenant));
  } else if (FALSE_IT(pg_task_updater_.set_dropped_tenant(is_dropped_tenant))) {
    // set dropped tenant
  } else if (OB_FAIL(pg_task_updater_.get_pending_pg_tasks(
                 job_id, validate_tenant.tenant_id_, OB_START_INCARNATION, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret));
  } else if (pg_task_infos.empty()) {
    // do nothing
  } else {
    int64_t prev_backup_set_id = pg_task_infos.at(0).backup_set_id_;
    int64_t cur_backup_set_id = pg_task_infos.at(0).backup_set_id_;
    ObBackupSetPGTaskList backup_set_pg_task;
    ObArray<ObPGValidateTaskInfo> tmp_pg_task_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      const ObPGValidateTaskInfo& pg_task_info = pg_task_infos.at(i);
      cur_backup_set_id = pg_task_info.backup_set_id_;
      if (prev_backup_set_id != cur_backup_set_id) {
        if (OB_FAIL(backup_set_pg_task.pg_task_infos_.assign(tmp_pg_task_infos))) {
          LOG_WARN("failed to assign pg task infos", K(ret));
        } else if (FALSE_IT(backup_set_pg_task.backup_set_id_ = prev_backup_set_id)) {
        } else if (OB_FAIL(backup_set_pg_list.push_back(backup_set_pg_task))) {
          LOG_WARN("failed to push back pg task info", K(ret));
        } else {
          backup_set_pg_task.reset();
          tmp_pg_task_infos.reset();
        }
      } else {
        if (OB_FAIL(tmp_pg_task_infos.push_back(pg_task_info))) {
          LOG_WARN("failed to push back pg task info", K(ret));
        }
      }
      prev_backup_set_id = cur_backup_set_id;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(backup_set_pg_task.pg_task_infos_.assign(tmp_pg_task_infos))) {
        LOG_WARN("failed to assign pg task infos", K(ret));
      } else if (FALSE_IT(backup_set_pg_task.backup_set_id_ = prev_backup_set_id)) {
      } else if (OB_FAIL(backup_set_pg_list.push_back(backup_set_pg_task))) {
        LOG_WARN("failed to push back pg task info", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionValidate::get_pending_pg_validate_task(const int64_t job_id,
    const ObBackupValidateTenant& validate_tenant, common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get extern pg list get invalid arguments", K(ret), K(validate_tenant));
  } else if (FALSE_IT(pg_task_updater_.set_dropped_tenant(is_dropped_tenant))) {
    // set dropped tenant
  } else if (OB_FAIL(pg_task_updater_.get_pending_pg_tasks(
                 job_id, validate_tenant.tenant_id_, OB_START_INCARNATION, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret));
  } else {
    LOG_INFO("get pending pg validate task success", K(ret), K(job_id), K(validate_tenant), K(pg_task_infos.count()));
  }
  return ret;
}

int ObPartitionValidate::get_pending_pg_validate_task(const int64_t job_id,
    const ObBackupValidateTenant& validate_tenant, const int64_t backup_set_id,
    common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  bool is_dropped_tenant = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!validate_tenant.is_valid() || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get extern pg list get invalid arguments", K(ret), K(validate_tenant), K(backup_set_id));
  } else if (FALSE_IT(pg_task_updater_.set_dropped_tenant(is_dropped_tenant))) {
    // set dropped tenant
  } else if (OB_FAIL(pg_task_updater_.get_pending_pg_tasks(
                 job_id, validate_tenant.tenant_id_, OB_START_INCARNATION, backup_set_id, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret));
  } else {
    LOG_INFO("get pending pg validate task success", K(ret));
  }
  return ret;
}

int ObPartitionValidate::get_log_archive_max_next_time(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t log_archive_round, int64_t& max_next_time)
{
  int ret = OB_SUCCESS;
  // __all_tenant_backup_log_archive_status
  // __all_backup_log_archive_status_history
  ObLogArchiveBackupInfoMgr log_archive_mgr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation <= 0 || log_archive_round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition validate get invalid argument", K(ret), K(tenant_id), K(incarnation), K(log_archive_round));
  } else if (OB_FAIL(log_archive_mgr.get_log_archive_checkpoint(*sql_proxy_, tenant_id, max_next_time))) {
    LOG_WARN("failed to get log archive checkpoint", K(ret), K(tenant_id));
  }
  UNUSED(incarnation);
  UNUSED(log_archive_round);
  return ret;
}

int ObPartitionValidate::generate_batch_pg_task(const bool is_dropped_tenant,
    const share::ObBackupValidateTaskInfo& validate_job, const share::ObTenantValidateTaskInfo& tenant_task_info,
    const common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_PG_COUNT_PER_TASK = 128;
  const uint64_t tenant_id = tenant_task_info.tenant_id_;
  int64_t sub_task_count = pg_task_infos.count() / MAX_PG_COUNT_PER_TASK;
  common::ObArray<share::ObLogArchiveBackupInfo> log_infos;
  common::ObArray<share::ObTenantBackupTaskInfo> backup_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition validate get invalid argument", K(ret), K(tenant_task_info));
  } else if (pg_task_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected, pg task infos count should greater then 0", K(ret));
  } else if (OB_FAIL(get_all_tenant_log_archive_infos(tenant_id, log_infos))) {
    LOG_WARN("failed to get all log archive infos", K(ret), K(tenant_task_info));
  } else if (OB_FAIL(get_all_tenant_backup_task_infos(tenant_id, backup_infos))) {
    LOG_WARN("failed to get all tenant backup task infos", K(ret), K(tenant_task_info));
  } else {
    LOG_INFO("get pg validation task to do", K(ret), K(pg_task_infos));
    if (pg_task_infos.count() % MAX_PG_COUNT_PER_TASK > 0) {
      ++sub_task_count;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_task_count; ++i) {
      int64_t pg_count = std::min(MAX_PG_COUNT_PER_TASK, pg_task_infos.count() - i * MAX_PG_COUNT_PER_TASK);
      ObArray<ObValidateTaskInfo> task_infos;
      ObArray<ObPGValidateTaskInfo> update_pg_task_infos;
      common::ObAddr addr;
      share::ObTaskId trace_id;
      trace_id.set(*ObCurTraceId::get_trace_id());
      if (OB_FAIL(load_balancer_.get_next_server(addr))) {
        LOG_WARN("failed to get next server", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < pg_count; ++j) {
          const int64_t pg_idx = i * MAX_PG_COUNT_PER_TASK + j;
          LOG_INFO("current pg idx is ", K(pg_idx));
          bool need_validate_clog = true;
          const ObPGValidateTaskInfo& pg_info = pg_task_infos.at(pg_idx);
          ObPartitionKey pg_key(pg_info.table_id_, pg_info.partition_id_, 0);
          ObPhysicalValidateArg arg;
          ObValidateTaskInfo task_info;
          ObPGValidateTaskInfo pg_task_info;
          ObReplicaMember replica_dst = ObReplicaMember(addr, ObTimeUtility::current_time(), REPLICA_TYPE_FULL, 0);
          int64_t full_backup_set_id = 0;
          int64_t inc_backup_set_id = 0;
          int64_t cluster_version = 0;
          if (OB_FAIL(check_need_validate_clog(validate_job, pg_info, log_infos, backup_infos, need_validate_clog))) {
            LOG_WARN("failed to check need validate clog", K(ret), K(validate_job), K(pg_info));
          } else if (OB_FAIL(get_extern_backup_set_infos(pg_info.backup_set_id_,
                         backup_infos,
                         full_backup_set_id,
                         inc_backup_set_id,
                         cluster_version))) {
            LOG_WARN("failed to get prev full backup set id and prev inc backup set id", K(ret));
          } else if (OB_FAIL(build_physical_validate_arg(is_dropped_tenant,
                         need_validate_clog,
                         pg_info.backup_set_id_,
                         pg_info.archive_round_,
                         tenant_task_info,
                         pg_key,
                         addr,
                         trace_id,
                         cluster_version,
                         arg))) {
            LOG_WARN("failed to build physical validate arg", K(ret));
          } else if (FALSE_IT(task_info.set_transmit_data_size(2 * 1024 * 1024))) {
            // will never be here
          } else if (OB_FAIL(task_info.build(pg_key, replica_dst, arg))) {
            LOG_WARN("failed to build validate task info", K(ret));
          } else if (OB_FAIL(task_infos.push_back(task_info))) {
            LOG_WARN("failed to push back task info");
          } else if (OB_FAIL(build_pg_validate_task_info(addr, trace_id, pg_info, pg_task_info))) {
            LOG_WARN("failed to build pg validate task info", K(ret));
          } else if (OB_FAIL(update_pg_task_infos.push_back(pg_task_info))) {
            LOG_WARN("failed to push back pg task info", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObValidateTask task;
        // TODO: need ensure below success
        if (OB_FAIL(batch_update_pg_task_infos(is_dropped_tenant, update_pg_task_infos))) {
          LOG_WARN("failed to batch update pg task infos", K(ret));
        } else if (OB_FAIL(task.build(task_infos, addr, ""))) {
          LOG_WARN("failed to build validate task");
        } else if (OB_FAIL(task_mgr_->add_task(task))) {
          LOG_WARN("failed to add task to task mgr", K(ret));
        } else {
          LOG_INFO("batch update pg task info success", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionValidate::build_physical_validate_arg(const bool is_dropped_tenant, const bool need_validate_clog,
    const int64_t backup_set_id, const int64_t log_archive_round,
    const share::ObTenantValidateTaskInfo& tenant_task_info, const common::ObPartitionKey& pg_key,
    const common::ObAddr& server, const share::ObTaskId& trace_id, const int64_t cluster_version,
    share::ObPhysicalValidateArg& arg)
{
  int ret = OB_SUCCESS;
  char storage_info[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = "";
  int64_t max_next_time = 0;
  const uint64_t log_archive_tenant_id = is_dropped_tenant ? OB_SYS_TENANT_ID : tenant_task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate load balancer do not init", K(ret));
  } else if (OB_FAIL(get_log_archive_max_next_time(
                 log_archive_tenant_id, tenant_task_info.incarnation_, log_archive_round, max_next_time))) {
    LOG_WARN("failed to get max_next_time", K(ret), K(tenant_task_info));
  } else {
    if (OB_FAIL(GCONF.cluster.copy(arg.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH))) {
      LOG_WARN("failed to copy cluster name", K(ret));
    } else {
      STRNCPY(arg.backup_dest_, tenant_task_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH);
      STRNCPY(arg.storage_info_, storage_info, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
      arg.backup_dest_[OB_MAX_BACKUP_DEST_LENGTH - 1] = '\0';
      arg.storage_info_[OB_MAX_BACKUP_STORAGE_INFO_LENGTH - 1] = '\0';
      arg.job_id_ = tenant_task_info.job_id_;
      arg.task_id_ = tenant_task_info.task_id_;
      arg.trace_id_.set(trace_id);
      arg.server_ = server;
      arg.cluster_id_ = GCONF.cluster_id;
      arg.pg_key_ = pg_key;
      arg.table_id_ = pg_key.get_table_id();
      arg.partition_id_ = pg_key.get_partition_id();
      arg.tenant_id_ = tenant_task_info.tenant_id_;
      arg.incarnation_ = OB_START_INCARNATION;
      arg.archive_round_ = log_archive_round;
      arg.backup_set_id_ = backup_set_id;
      arg.total_partition_count_ = 0;
      arg.total_macro_block_count_ = 0;
      arg.clog_end_timestamp_ = max_next_time;
      arg.start_log_id_ = 0;
      arg.end_log_id_ = 0;
      arg.log_size_ = 0;
      arg.is_dropped_tenant_ = is_dropped_tenant;
      arg.need_validate_clog_ = need_validate_clog;
      // arg.full_backup_set_id_ = full_backup_set_id;
      // arg.inc_backup_set_id_ = inc_backup_set_id;
      arg.cluster_version_ = cluster_version;
    }
  }
  return ret;
}

int ObPartitionValidate::build_pg_validate_task_info(const common::ObAddr& addr, const share::ObTaskId& trace_id,
    const share::ObPGValidateTaskInfo& src_pg_info, share::ObPGValidateTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  pg_task_info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (!src_pg_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate validate task info get invalid argument", K(ret));
  } else {
    pg_task_info = src_pg_info;
    pg_task_info.total_partition_count_ = 0;
    pg_task_info.finish_partition_count_ = 0;
    pg_task_info.total_macro_block_count_ = 0;
    pg_task_info.finish_macro_block_count_ = 0;
    pg_task_info.server_ = addr;
    pg_task_info.status_ = ObPGValidateTaskInfo::DOING;
    pg_task_info.trace_id_.set(trace_id);
    pg_task_info.comment_.assign("validating pg");
  }
  return ret;
}

int ObPartitionValidate::batch_update_pg_task_infos(
    const bool is_dropped_tenant, const ObIArray<ObPGValidateTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition validate do not init", K(ret));
  } else if (OB_UNLIKELY(task_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch update pg task infos get invalid arguments", K(ret));
  } else if (FALSE_IT(pg_task_updater_.set_dropped_tenant(is_dropped_tenant))) {
    // set dropped tenant
  } else if (OB_FAIL(pg_task_updater_.batch_report_pg_task(task_infos))) {
    LOG_WARN("failed to batch update pg task infos", K(ret));
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
