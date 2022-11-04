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
#include "lib/thread/ob_thread_name.h"
#include "ob_backup_info_mgr.h"
#include "ob_log_archive_backup_info_mgr.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "share/ob_cluster_version.h"
#include "share/ob_force_print_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_manager.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/utility/utility.h"
#include "ob_backup_manager.h"
using namespace oceanbase;
using namespace common;
using namespace share;
using namespace common;

void ObBackupInfoMgr::ObBackupInfoUpdateTask::runTimerTask()
{
  /*
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ObBackupInfoMgr::get_instance().reload())) {
    LOG_WARN("failed to reload backup info", K(tmp_ret));
  }*/
}

ObBackupInfoMgr &ObBackupInfoMgr::get_instance()
{
  static ObBackupInfoMgr backup_info;
  return backup_info;
}

ObBackupInfoMgr::ObBackupInfoMgr()
  :is_inited_(false),
   sql_proxy_(nullptr),
   timer_(),
   update_task_(),
   cur_backup_info_(nullptr),
   cur_backup_piece_(nullptr),
   cur_restore_job_(nullptr),
   lock_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
   mutex_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
   is_backup_loaded_(false),
   is_restore_loaded_(false)
{
}

ObBackupInfoMgr::~ObBackupInfoMgr()
{
  destroy();
}

int ObBackupInfoMgr::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(timer_.init("BackupInfoUpdate"))) {
    LOG_WARN("failed to init backup info update task", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    cur_backup_info_ = &backup_infos_[0];
    cur_backup_piece_ = &backup_pieces_[0];
    is_inited_ = true;
  }
  return ret;
}

int ObBackupInfoMgr::start()
{
  int ret = OB_SUCCESS;
  // TODO(chognrong.th) redesign backup info later
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } /*else if (OB_FAIL(timer_.schedule(update_task_, DEFAULT_UPDATE_INTERVAL_US, true))) {
    LOG_WARN("Failed to schedule update task", K(ret));
  } */
  return ret;
}

void ObBackupInfoMgr::stop()
{
  timer_.stop();
}

void ObBackupInfoMgr::wait()
{
  timer_.wait();
}

void ObBackupInfoMgr::destroy()
{
  FLOG_INFO("ObBackupInfoMgr::destroy");
  timer_.destroy();
  is_inited_ = false;
}


int ObBackupInfoMgr::get_restore_info_from_cache(
    const uint64_t tenant_id, ObSimplePhysicalRestoreJob &simple_job_info)
{
  int ret = OB_ENTRY_NOT_EXIST;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (is_restore_loaded_) {
      for (int64_t i = 0; i < cur_restore_job_->count(); ++i) {
        ObPhysicalRestoreJob &cur_job = cur_restore_job_->at(i);
        if (cur_job.get_tenant_id() == tenant_id) {
          if (OB_FAIL(cur_job.copy_to(simple_job_info))) {
            LOG_WARN("failed to copy to simple job info", K(ret), K(cur_job));
          }
          break;
        }
      }
    }
  }

  return ret;
}

int ObBackupInfoMgr::get_restore_status_from_cache(
    const uint64_t tenant_id, PhysicalRestoreStatus &status)
{
  int ret = OB_ENTRY_NOT_EXIST;
  status = PHYSICAL_RESTORE_MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (is_restore_loaded_) {
      for (int64_t i = 0; i < cur_restore_job_->count(); ++i) {
        ObPhysicalRestoreJob &cur_job = cur_restore_job_->at(i);
        if (cur_job.get_tenant_id() == tenant_id) {
          status = cur_job.get_status();
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  }

  return ret;
}

int ObBackupInfoMgr::get_restore_status(
    const uint64_t tenant_id, PhysicalRestoreStatus &status)
{
  int ret = OB_SUCCESS;
  bool found = false;
  status = PHYSICAL_RESTORE_MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_restore_status_from_cache(tenant_id, status))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_restore_status_from_cache", K(ret), K(tenant_id));
    } else if (OB_FAIL(reload())) {
      LOG_WARN("failed to reload", K(ret));
    } else if (OB_FAIL(get_restore_status_from_cache(tenant_id, status))) {
      LOG_WARN("failed to get restore info from cache again", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_status_from_cache", K(ret), K(tenant_id), K(status));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_backup_snapshot_version(
    int64_t &backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  backup_snapshot_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cluster_version < CLUSTER_VERSION_2250) {
    backup_snapshot_version = 0;
    LOG_INFO("old cluster version, not check delay delete snapshot version", K(cluster_version));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::get_backup_snapshot_version(
      *sql_proxy_, real_tenant_id, backup_snapshot_version))) {
    LOG_WARN("failed to get backup snapshot_version", K(ret));
  } else if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {// for each 60s
    LOG_INFO("get_backup_snapshot_version", K(ret), K(real_tenant_id), K(backup_snapshot_version));
  }

  return ret;
}

int ObBackupInfoMgr::get_delay_delete_schema_version(const uint64_t tenant_id,
    share::schema::ObMultiVersionSchemaService &schema_service,
    bool &is_backup, int64_t &reserved_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t tenant_name_backup_schema_version = 0;
  bool is_restore = false;
  const int64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  reserved_schema_version = 0;
  is_backup = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (CLUSTER_VERSION_2250 > cluster_version) {
    LOG_INFO("skip get_delay_delete_schema_version for old cluster version", K(cluster_version));
  } else if (OB_FAIL(schema_service.check_tenant_is_restore(nullptr, tenant_id, is_restore))) {
    LOG_WARN("failed to check tenant is restore", K(ret), K(tenant_id), K(is_restore));
  } else if (is_restore) {
    LOG_TRACE("get_delay_delete_schema_version for restore", K(ret), K(tenant_id),
        K(reserved_schema_version), K(is_backup));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::get_backup_schema_version(
      *sql_proxy_, tenant_id, reserved_schema_version))) {
    LOG_WARN("failed to get backup snapshot_version", K(ret));
  } else if (OB_FAIL(check_if_doing_backup(is_backup))) {
    LOG_WARN("failed to check_if_doing_backup", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(
            *sql_proxy_, tenant_name_backup_schema_version))) {
        LOG_WARN("failed to get_tenant_name_backup_schema_version", K(ret));
      } else if (tenant_name_backup_schema_version < reserved_schema_version
          || 0 == reserved_schema_version) {
        reserved_schema_version = tenant_name_backup_schema_version;
      }
    }

    LOG_INFO("get_delay_delete_schema_version", K(ret), K(tenant_id),
        K(tenant_name_backup_schema_version), K(reserved_schema_version), K(is_backup));
  }

  return ret;
}

int ObBackupInfoMgr::check_if_doing_backup(bool &is_doing)
{
  int ret = OB_SUCCESS;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  const bool for_update = false;
  ObBackupInfoItem item;
  item.name_ = "backup_status";
  is_doing = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cluster_version < CLUSTER_VERSION_2250) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("old cluster version, should not call check_if_doing_backup",
        K(ret), K(cluster_version));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(
      *sql_proxy_, real_tenant_id, item, for_update))) {
    LOG_WARN("failed to get backup status", K(ret));
  } else if (0 != STRCMP("STOP", item.get_value_ptr())) {
    is_doing = true;
    FLOG_INFO("doing base data backup", K(item));
  }

  FLOG_INFO("check_if_doing_backup", K(ret), K(is_doing), K(item));
  return ret;
}

int ObBackupInfoMgr::reload()
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_operator;
  ObBackupInfoManager info_manager;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  lib::ObMutexGuard mutex_guard(mutex_);
  ObLogArchiveBackupInfo *new_backup_info = &backup_infos_[0];
  ObNonFrozenBackupPieceInfo *new_backup_piece = &backup_pieces_[0];
  RestoreJobArray *new_restore_job = &restore_jobs_[0];

  if (new_backup_info == cur_backup_info_) {
    new_backup_info = &backup_infos_[1];
  }
  if (new_backup_piece == cur_backup_piece_) {
    new_backup_piece = &backup_pieces_[1];
  }

  if (new_restore_job == cur_restore_job_) {
    new_restore_job = &restore_jobs_[1];
  }

  // reload backup info
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret));
  } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)
      || new_backup_info->status_.status_ != cur_backup_info_->status_.status_
      || new_backup_info->status_.round_ != cur_backup_info_->status_.round_
      || new_backup_info->status_.backup_piece_id_ != cur_backup_info_->status_.backup_piece_id_) {
    FLOG_INFO("succeed to reload backup info", K(ret), K(*new_backup_info), K(*new_backup_piece));
  }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(lock_);
    cur_backup_info_ = new_backup_info;
    cur_backup_piece_ = new_backup_piece;
    is_backup_loaded_ = true;
  }

  // reload restore info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_operator.init(sql_proxy_, tenant_id))) {
      LOG_WARN("failed to init restore operator", K(ret), K(tenant_id));
    } else if (OB_FAIL(restore_operator.get_jobs(*new_restore_job))) {
      LOG_WARN("failed to get new restore info", K(ret));
    } else {
      if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)
          || new_restore_job->count() != cur_restore_job_->count()) {
        FLOG_INFO("succeed to reload restore job", K(*new_restore_job));
      }
      SpinWLockGuard guard(lock_);
      cur_restore_job_ = new_restore_job;
      is_restore_loaded_ = true;
    }
  }
  return ret;
}

