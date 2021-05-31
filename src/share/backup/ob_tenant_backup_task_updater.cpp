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
#include "ob_tenant_backup_task_updater.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase;
using namespace common;
using namespace share;

ObTenantBackupTaskUpdater::ObTenantBackupTaskUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObTenantBackupTaskUpdater::init(common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pg backup task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupTaskUpdater::insert_tenant_backup_task(
    const ObBaseBackupInfoStruct& info, const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo tenant_backup_task;
  ObBackupDest backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret), K(info));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup struct info is invalid", K(ret), K(info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else {
    tenant_backup_task.tenant_id_ = info.tenant_id_;
    tenant_backup_task.backup_set_id_ = info.backup_set_id_;
    tenant_backup_task.incarnation_ = info.incarnation_;
    tenant_backup_task.backup_type_ = info.backup_type_;
    tenant_backup_task.device_type_ = backup_dest.device_type_;
    tenant_backup_task.start_time_ = info.backup_snapshot_version_;
    tenant_backup_task.end_time_ = 0;
    tenant_backup_task.prev_full_backup_set_id_ = extern_backup_info.prev_full_backup_set_id_;
    tenant_backup_task.prev_inc_backup_set_id_ = extern_backup_info.prev_inc_backup_set_id_;
    tenant_backup_task.prev_backup_data_version_ = extern_backup_info.prev_backup_data_version_;
    tenant_backup_task.compatible_ = extern_backup_info.compatible_;
    tenant_backup_task.cluster_version_ = extern_backup_info.cluster_version_;
    tenant_backup_task.status_ =
        OB_SYS_TENANT_ID != info.tenant_id_ ? ObTenantBackupTaskInfo::GENERATE : ObTenantBackupTaskInfo::DOING;
    tenant_backup_task.snapshot_version_ = info.backup_snapshot_version_;
    tenant_backup_task.cluster_id_ = GCONF.cluster_id;
    tenant_backup_task.backup_dest_ = backup_dest;
    tenant_backup_task.backup_data_version_ = extern_backup_info.backup_data_version_;
    tenant_backup_task.backup_schema_version_ = info.backup_schema_version_;
    tenant_backup_task.encryption_mode_ = info.encryption_mode_;
    tenant_backup_task.passwd_ = info.passwd_;
    if (OB_FAIL(insert_tenant_backup_task(tenant_backup_task))) {
      LOG_WARN("failed to insert tenant backup task", K(ret), K(info), K(tenant_backup_task));
    }
  }
  return ret;
}

int ObTenantBackupTaskUpdater::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::get_tenant_backup_task(
                 tenant_id, backup_set_id, incarnation, tenant_backup_task, *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::get_tenant_backup_task(
    const uint64_t tenant_id, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, *sql_proxy_, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::get_tenant_backup_task(
    const uint64_t tenant_id, common::ObISQLClient& trans, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::get_tenant_backup_task(tenant_id, tenant_backup_task, trans))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::update_tenant_backup_task(
    const ObTenantBackupTaskInfo& src_task_info, const ObTenantBackupTaskInfo& dest_task_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (!src_task_info.is_valid() || !dest_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup task get invalid argument", K(ret), K(src_task_info), K(dest_task_info));
  } else if (OB_FAIL(update_tenant_backup_task(*sql_proxy_, src_task_info, dest_task_info))) {
    LOG_WARN("failed to update tenant backup task", K(ret), K(src_task_info), K(dest_task_info));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::update_tenant_backup_task(common::ObISQLClient& trans,
    const ObTenantBackupTaskInfo& src_task_info, const ObTenantBackupTaskInfo& dest_task_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (!src_task_info.is_valid() || !dest_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup task get invalid argument", K(ret), K(src_task_info), K(dest_task_info));
  } else if (OB_FAIL(check_can_update_backup_task(src_task_info.status_, dest_task_info.status_))) {
    LOG_WARN("failed to check can update backup task", K(ret), K(src_task_info), K(dest_task_info));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::report_task(dest_task_info, trans))) {
    LOG_WARN("failed to update tenant backup task", K(ret), K(dest_task_info));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::remove_task(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::remove_task(tenant_id, incarnation, backup_set_id, *sql_proxy_))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::check_can_update_backup_task(
    const ObTenantBackupTaskInfo::BackupStatus& src_status, const ObTenantBackupTaskInfo::BackupStatus& dest_status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (src_status > ObTenantBackupTaskInfo::MAX || dest_status > ObTenantBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src or dest status is invalid", K(ret), K(src_status), K(dest_status));
  } else {
    switch (src_status) {
      case ObTenantBackupTaskInfo::GENERATE:
        if (ObTenantBackupTaskInfo::GENERATE != dest_status && ObTenantBackupTaskInfo::DOING != dest_status &&
            ObTenantBackupTaskInfo::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup task", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObTenantBackupTaskInfo::DOING:
        if (ObTenantBackupTaskInfo::DOING != dest_status && ObTenantBackupTaskInfo::FINISH != dest_status &&
            ObTenantBackupTaskInfo::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup task", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObTenantBackupTaskInfo::FINISH:
        if (ObTenantBackupTaskInfo::FINISH != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup task", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObTenantBackupTaskInfo::CANCEL:
        if (ObTenantBackupTaskInfo::CANCEL != dest_status && ObTenantBackupTaskInfo::FINISH != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup task", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObTenantBackupTaskInfo::MAX:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup task", K(ret), K(src_status), K(dest_status));
        break;
    }
  }
  return ret;
}

int ObTenantBackupTaskUpdater::insert_tenant_backup_task(const ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (!tenant_backup_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup task get invalid argument", K(ret), K(tenant_backup_task));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::insert_task(tenant_backup_task, *sql_proxy_))) {
    LOG_WARN("failed to insert tenant backup task", K(ret), K(tenant_backup_task));
  }
  return ret;
}

ObBackupTaskHistoryUpdater::ObBackupTaskHistoryUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObBackupTaskHistoryUpdater::init(common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pg backup task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::insert_tenant_backup_task(const ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret), K(task_info));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup struct info is invalid", K(ret), K(task_info));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::insert_task(task_info, *sql_proxy_))) {
    LOG_WARN("failed to insert tenant backup task", K(ret), K(task_info));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_task(
                 tenant_id, backup_set_id, incarnation, *sql_proxy_, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_backup_tasks(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObBackupTaskHistoryOperator::get_tenant_backup_tasks(tenant_id, *sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::remove_task(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::remove_task(tenant_id, incarnation, backup_set_id, *sql_proxy_))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_need_mark_deleted_backup_tasks(const uint64_t tenant_id,
    const int64_t backup_set_id, const int64_t incarnation, const ObBackupDest& backup_dest,
    common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0 || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_dest));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_need_mark_deleted_backup_tasks(
                 tenant_id, backup_set_id, incarnation, backup_dest, *sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::mark_backup_task_deleted(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::mark_backup_task_deleted(
                 tenant_id, incarnation, backup_set_id, *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(incarnation), K(backup_set_id), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::delete_marked_task(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::delete_marked_task(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_mark_deleted_backup_tasks(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_mark_deleted_backup_tasks(
                 tenant_id, *sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::delete_backup_task(const ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (!tenant_backup_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_backup_task));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::remove_task(tenant_backup_task.tenant_id_,
                 tenant_backup_task.incarnation_,
                 tenant_backup_task.backup_set_id_,
                 *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_all_tenant_backup_tasks_in_time_range(
    const int64_t start_time, const int64_t end_time, common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (start_time < 0 || end_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(start_time), K(end_time));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_task_in_time_range(
                 start_time, end_time, *sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant backup task in time range", K(ret), K(start_time), K(end_time));
  }

  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_full_backup_tasks(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant full backup tasks get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_full_backup_tasks(
                 tenant_id, *sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant full backup tasks", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_backup_task(
    const uint64_t tenant_id, const int64_t backup_set_id, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_task(
                 tenant_id, backup_set_id, *sql_proxy_, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_max_succeed_backup_task(
    const uint64_t tenant_id, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_max_succeed_backup_task(
                 tenant_id, *sql_proxy_, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_all_tenant_backup_tasks(
    common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_all_tenant_backup_tasks(*sql_proxy_, tenant_backup_tasks))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret));
  }
  return ret;
}
