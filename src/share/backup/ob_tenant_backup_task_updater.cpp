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
#include "share/backup/ob_backup_backupset_operator.h"
#include "share/backup/ob_extern_backup_info_mgr.h"

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
    tenant_backup_task.backup_type_.type_ = extern_backup_info.backup_type_;
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
    tenant_backup_task.date_ = extern_backup_info.date_;
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
    const uint64_t tenant_id, const bool for_update, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, for_update, *sql_proxy_, tenant_backup_task))) {
    LOG_WARN("failed to get tenant backup task", K(ret));
  }
  return ret;
}

int ObTenantBackupTaskUpdater::get_tenant_backup_task(const uint64_t tenant_id, const bool for_update,
    common::ObISQLClient& trans, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupTaskOperator::get_tenant_backup_task(
                 tenant_id, for_update, tenant_backup_task, trans))) {
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

int ObBackupTaskHistoryUpdater::get_tenant_backup_tasks(const uint64_t tenant_id, const bool is_backup_backup,
    common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id));
  } else if (is_backup_backup) {
    ObArray<share::ObTenantBackupBackupsetTaskInfo> tenant_his_backupset_infos;
    ObTenantBackupTaskInfo task_info;

    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_task_items(
            tenant_id, tenant_his_backupset_infos, *sql_proxy_))) {
      LOG_WARN("failed to get full backupset task items", K(ret), K(tenant_id));
    } else {
      for (int64_t i = tenant_his_backupset_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        const ObTenantBackupBackupsetTaskInfo& prev_backupset_info = tenant_his_backupset_infos.at(i);
        for (int64_t j = i - 1; OB_SUCC(ret) && j >= 0; --j) {
          const ObTenantBackupBackupsetTaskInfo& backup_backupset_info = tenant_his_backupset_infos.at(j);
          if (backup_backupset_info.incarnation_ == prev_backupset_info.incarnation_ &&
              backup_backupset_info.backup_set_id_ == prev_backupset_info.backup_set_id_ &&
              backup_backupset_info.copy_id_ == prev_backupset_info.copy_id_) {
            if (OB_FAIL(tenant_his_backupset_infos.remove(j))) {
              LOG_WARN("failed to remove tenant backup set info", K(ret), K(i), K(backup_backupset_info));
            }
          }
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_his_backupset_infos.count(); ++i) {
        const ObTenantBackupBackupsetTaskInfo& backup_backupset_info = tenant_his_backupset_infos.at(i);
        task_info.reset();
        if (OB_FAIL(backup_backupset_info.convert_to_backup_task_info(task_info))) {
          LOG_WARN("failed to convert to backup task info", K(ret), K(backup_backupset_info));
        } else if (OB_FAIL(tenant_backup_tasks.push_back(task_info))) {
          LOG_WARN("failed to push tenant backup tasks into array", K(ret), K(backup_backupset_info));
        }
      }
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_tasks(tenant_id, *sql_proxy_, tenant_backup_tasks))) {
      LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
    }
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
    const int64_t backup_set_id, const int64_t incarnation, const ObBackupDest& backup_dest, const bool for_update,
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
                 tenant_id, backup_set_id, incarnation, backup_dest, for_update, *sql_proxy_, tenant_backup_tasks))) {
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
  } else if (tenant_backup_task.copy_id_ > 0) {
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::delete_task_item(tenant_backup_task.tenant_id_,
            tenant_backup_task.incarnation_,
            tenant_backup_task.backup_set_id_,
            tenant_backup_task.copy_id_,
            *sql_proxy_))) {
      LOG_WARN("failed to delete task item", K(ret), K(tenant_backup_task));
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::remove_task(tenant_backup_task.tenant_id_,
            tenant_backup_task.incarnation_,
            tenant_backup_task.backup_set_id_,
            *sql_proxy_))) {
      LOG_WARN("failed to get tenant backup task", K(ret));
    }
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

int ObBackupTaskHistoryUpdater::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t copy_id, const bool for_update, ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  tenant_backup_tasks.reset();
  ObTenantBackupTaskInfo tenant_backup_task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (copy_id > 0) {
    ObArray<ObTenantBackupBackupsetTaskItem> items;
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_task_item(
            tenant_id, backup_set_id, copy_id, for_update, items, *sql_proxy_))) {
      LOG_WARN("failed to get task item", K(ret), K(tenant_id), K(backup_set_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
        const ObTenantBackupBackupsetTaskItem& tmp_item = items.at(i);
        tenant_backup_task.reset();
        if (OB_FAIL(tmp_item.convert_to_backup_task_info(tenant_backup_task))) {
          LOG_WARN("failed to convert to backup task info", K(ret), K(tenant_id), K(backup_set_id));
        } else if (OB_FAIL(tenant_backup_tasks.push_back(tenant_backup_task))) {
          LOG_WARN("failed to push tennat backup task into array", K(ret), K(tenant_backup_task));
        }
      }
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_task(
            tenant_id, backup_set_id, for_update, *sql_proxy_, tenant_backup_task))) {
      LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_backup_tasks.push_back(tenant_backup_task))) {
      LOG_WARN("failed to push tenant backup task into array", K(ret), K(tenant_backup_task));
    }
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

int ObBackupTaskHistoryUpdater::get_tenant_ids_with_backup_set_id(
    const int64_t backup_set_id, const int64_t copy_id, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (backup_set_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(backup_set_id), K(copy_id));
  } else if (copy_id > 0) {
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_tenant_ids_with_backup_set_id(
            copy_id, backup_set_id, tenant_ids, *sql_proxy_))) {
      LOG_WARN("failed to get backup backupset tenant ids", K(ret), K(backup_set_id), K(copy_id));
    }
  } else {
    if (OB_FAIL(
            ObBackupTaskHistoryOperator::get_tenant_ids_with_backup_set_id(backup_set_id, *sql_proxy_, tenant_ids))) {
      LOG_WARN("failed to get backup tenant ids", K(ret), K(backup_set_id));
    }
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_tenant_ids_with_snapshot_version(
    const int64_t snapshot_version, const bool is_backup_backup, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(snapshot_version));
  } else if (is_backup_backup) {
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_tenant_ids_with_snapshot_version(
            snapshot_version, tenant_ids, *sql_proxy_))) {
      LOG_WARN("failed to get backup backupset tenant ids", K(ret), K(snapshot_version));
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_ids_with_snapshot_version(
            snapshot_version, *sql_proxy_, tenant_ids))) {
      LOG_WARN("failed to get backup tenant ids", K(ret), K(snapshot_version));
    }
  }

  return ret;
}

int ObBackupTaskHistoryUpdater::get_backup_set_file_info(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t copy_id, const bool for_update, ObBackupSetFileInfo& backup_set_file_info)
{
  int ret = OB_SUCCESS;
  backup_set_file_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup set file info get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (ObBackupSetFilesOperator::get_tenant_backup_set_file_info(tenant_id,
                 backup_set_id,
                 OB_START_INCARNATION,
                 copy_id,
                 for_update,
                 *sql_proxy_,
                 backup_set_file_info)) {
    LOG_WARN("failed to get tenant backup set file info", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::update_backup_task_info(const ObTenantBackupTaskInfo& backup_task_info)
{
  int ret = OB_SUCCESS;
  const bool fill_mark_delete_item = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (!backup_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup task info get invalid argument", K(ret), K(backup_task_info));
  } else if (backup_task_info.copy_id_ > 0) {
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::update_backup_task_info(
            backup_task_info, fill_mark_delete_item, *sql_proxy_))) {
      LOG_WARN("failed to update backup task info", K(ret), K(backup_task_info));
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::update_backup_task_info(
            backup_task_info, fill_mark_delete_item, *sql_proxy_))) {
      LOG_WARN("failed toupdate backup task info", K(ret), K(backup_task_info));
    }
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::update_backup_set_file_info(
    const ObBackupSetFileInfo& src_backup_set_file_info, const ObBackupSetFileInfo& dst_backup_set_file_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (!src_backup_set_file_info.is_valid() || !dst_backup_set_file_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup set file info get invalid argument",
        K(ret),
        K(src_backup_set_file_info),
        K(dst_backup_set_file_info));
  } else if (OB_FAIL(ObBackupSetFilesOperator::update_tenant_backup_set_file(
                 src_backup_set_file_info, dst_backup_set_file_info, *sql_proxy_))) {
    LOG_WARN(
        "failed to update tenant backup set file", K(ret), K(src_backup_set_file_info), K(dst_backup_set_file_info));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::mark_backup_set_info_deleting(
    const int64_t tenant_id, const int64_t backup_set_id, const int64_t copy_id)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  ObTenantBackupTaskInfo backup_task_info;
  ObBackupSetFileInfo backup_set_file_info;
  ObTenantBackupTaskInfo dest_backup_task_info;
  ObBackupSetFileInfo dest_backup_set_file_info;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark backup set info deleting get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, backup_set_id, copy_id, for_update, backup_task_infos))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
  } else if (backup_task_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup task infos should not be empty", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (FALSE_IT(backup_task_info = backup_task_infos.at(backup_task_infos.count() - 1))) {
  } else if (OB_FAIL(get_backup_set_file_info(tenant_id, backup_set_id, copy_id, for_update, backup_set_file_info))) {
    LOG_WARN("failed to get backup set file info", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    dest_backup_task_info = backup_task_info;
    dest_backup_task_info.is_mark_deleted_ = true;
    dest_backup_set_file_info = backup_set_file_info;
    dest_backup_set_file_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING;
    if (OB_FAIL(mark_backup_task_info_deleted(dest_backup_task_info))) {
      LOG_WARN("failed to update backup task info", K(ret), K(dest_backup_set_file_info));
    } else if (OB_FAIL(update_backup_set_file_info(backup_set_file_info, dest_backup_set_file_info))) {
      LOG_WARN("failed to update backup set file info", K(ret), K(backup_set_file_info), K(dest_backup_set_file_info));
    }
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::delete_backup_set_info(
    const int64_t tenant_id, const int64_t backup_set_id, const int64_t copy_id)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  ObTenantBackupTaskInfo backup_task_info;
  ObBackupSetFileInfo backup_set_file_info;
  ObBackupSetFileInfo dest_backup_set_file_info;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark backup set info deleting get invalid argument", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, backup_set_id, copy_id, for_update, backup_task_infos))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
  } else if (backup_task_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup task infos should not be empty", K(ret), K(tenant_id), K(backup_set_id), K(copy_id));
  } else if (FALSE_IT(backup_task_info = backup_task_infos.at(backup_task_infos.count() - 1))) {
  } else if (OB_FAIL(get_backup_set_file_info(tenant_id, backup_set_id, copy_id, for_update, backup_set_file_info))) {
    LOG_WARN("failed to get backup set file info", K(ret), K(tenant_id), K(backup_set_id));
  } else if (!backup_task_info.is_mark_deleted_ ||
             ObBackupFileStatus::BACKUP_FILE_DELETING != backup_set_file_info.file_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "backup set info status is unexpected, can not delete", K(ret), K(backup_task_info), K(backup_set_file_info));
  } else if (OB_FAIL(delete_backup_task(backup_task_info))) {
    LOG_WARN("failed to delete backup task", K(ret), K(backup_task_info));
  } else {
    dest_backup_set_file_info = backup_set_file_info;
    dest_backup_set_file_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETED;
    if (OB_FAIL(update_backup_set_file_info(backup_set_file_info, dest_backup_set_file_info))) {
      LOG_WARN("failed to update backup set file info", K(ret), K(backup_set_file_info), K(dest_backup_set_file_info));
    }
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_backup_set_file_info_copies(const int64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, common::ObIArray<ObBackupSetFileInfo>& backup_set_file_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark backup set info deleting get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_file_info_copies(
                 tenant_id, incarnation, backup_set_id, *sql_proxy_, backup_set_file_infos))) {
    LOG_WARN("failed to get backup set file info copies", K(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::get_original_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const bool for_update, ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  const int64_t copy_id = 0;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;
  tenant_backup_task.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup tasks get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, backup_set_id, copy_id, for_update, backup_task_infos))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
  } else if (1 != backup_task_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant backup task infos count is unexpected", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    tenant_backup_task = backup_task_infos.at(0);
  }
  return ret;
}

int ObBackupTaskHistoryUpdater::mark_backup_task_info_deleted(const ObTenantBackupTaskInfo& backup_task_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task history updater do not init", K(ret));
  } else if (!backup_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup task info get invalid argument", K(ret), K(backup_task_info));
  } else if (backup_task_info.copy_id_ > 0) {
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::mark_task_item_deleted(backup_task_info.tenant_id_,
            backup_task_info.incarnation_,
            backup_task_info.copy_id_,
            backup_task_info.backup_set_id_,
            *sql_proxy_))) {
      LOG_WARN("failed to update backup task info", K(ret), K(backup_task_info));
    }
  } else {
    if (OB_FAIL(ObBackupTaskHistoryOperator::mark_backup_task_deleted(backup_task_info.tenant_id_,
            backup_task_info.incarnation_,
            backup_task_info.backup_set_id_,
            *sql_proxy_))) {
      LOG_WARN("failed toupdate backup task info", K(ret), K(backup_task_info));
    }
  }
  return ret;
}
