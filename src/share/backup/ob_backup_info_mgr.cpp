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
#include "ob_extern_backup_info_mgr.h"
#include "ob_physical_restore_table_operator.h"
#include "share/ob_cluster_version.h"
#include "ob_tenant_name_mgr.h"
#include "share/ob_force_print_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_manager.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/utility/utility.h"
using namespace oceanbase;
using namespace common;
using namespace share;
using namespace common;

ObLogArchiveInfoMgr::ObLogArchiveSimpleInfo::ObLogArchiveSimpleInfo()
    : update_ts_(0),
      checkpoint_ts_(0),
      start_ts_(0),
      status_(share::ObLogArchiveStatus::INVALID),
      tenant_id_(OB_INVALID_ID)
{}

void ObLogArchiveInfoMgr::ObLogArchiveSimpleInfo::reset()
{
  update_ts_ = 0;
  checkpoint_ts_ = 0;
  start_ts_ = 0;
  status_ = share::ObLogArchiveStatus::INVALID;
  tenant_id_ = OB_INVALID_ID;
}

bool ObLogArchiveInfoMgr::ObLogArchiveSimpleInfo::is_valid() const
{
  return update_ts_ > 0 && checkpoint_ts_ >= 0 && start_ts_ >= 0 && ObLogArchiveStatus::is_valid(status_) &&
         OB_INVALID_ID != tenant_id_;
}

ObLogArchiveInfoMgr::ObLogArchiveInfoMgr()
    : is_inited_(false),
      sql_proxy_(nullptr),
      lock_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      mutex_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      status_map_(),
      update_count_(0)
{}

ObLogArchiveInfoMgr::~ObLogArchiveInfoMgr()
{}

ObLogArchiveInfoMgr& ObLogArchiveInfoMgr::get_instance()
{
  static ObLogArchiveInfoMgr mgr;
  return mgr;
}

int ObLogArchiveInfoMgr::init(common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  lib::ObLabel label("BACKUP_LR_INFO");

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(status_map_.create(OB_MAX_SERVER_TENANT_CNT, label))) {
    LOG_WARN("failed to create status map", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObLogArchiveInfoMgr::get_log_archive_status(
    const uint64_t tenant_id, const int64_t need_ts, ObLogArchiveSimpleInfo& status)
{
  int ret = OB_SUCCESS;
  const int64_t CACHE_EXPIRED_TIME = 1 * 1000 * 1000;  // 1s
  bool need_renew = false;
  int64_t cur_ts = ObTimeUtility::current_time();

  status.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || need_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(need_ts), K(tenant_id));
  } else {
    if (OB_FAIL(get_log_archive_status_(tenant_id, status))) {
      if (OB_HASH_NOT_EXIST == ret) {
        need_renew = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get log archive status", K(ret), K(tenant_id));
      }
    } else if (status.update_ts_ < need_ts || status.update_ts_ + CACHE_EXPIRED_TIME <= cur_ts) {
      need_renew = true;
    }
  }

  if (OB_SUCC(ret) && need_renew) {
    if (OB_FAIL(renew_log_archive_status_(tenant_id, status))) {
      LOG_WARN("failed to renew log archive status", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret) && need_ts > status.update_ts_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid update ts", K(ret), K(need_ts), K(status));
  }

  return ret;
}

int ObLogArchiveInfoMgr::get_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo& status)
{
  int ret = OB_SUCCESS;

  SpinRLockGuard guard(lock_);
  if (OB_FAIL(status_map_.get_refactored(tenant_id, status))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get status", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObLogArchiveInfoMgr::try_retire_status_()
{
  int ret = OB_SUCCESS;
  uint64_t del_tenant_id = 0;
  int64_t del_ts = INT64_MAX;

  if (status_map_.size() >= OB_MAX_SERVER_TENANT_CNT) {
    for (STATUS_MAP::const_iterator it = status_map_.begin(); OB_SUCC(ret) && it != status_map_.end(); ++it) {
      const uint64_t tenant_id = it->first;
      const ObLogArchiveSimpleInfo& status = it->second;
      if (status.update_ts_ < del_ts) {
        del_tenant_id = tenant_id;
        del_ts = status.update_ts_;
      }
    }

    if (OB_SUCC(ret)) {
      SpinWLockGuard guard(lock_);
      if (OB_FAIL(status_map_.erase_refactored(del_tenant_id))) {
        LOG_WARN("failed to erase status", K(ret), K(del_tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      FLOG_INFO("retire log archive status", K(del_tenant_id));
    }
  }

  return ret;
}

int ObLogArchiveInfoMgr::renew_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo& status)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  const bool for_update = false;
  ObLogArchiveBackupInfo info;
  status.reset();

  lib::ObMutexGuard mutex_guard(mutex_);

  if (OB_FAIL(try_retire_status_())) {
    LOG_WARN("failed to try_retire_status_", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
    usleep(100 * 1000);  // sleep 100ms
  } else if (info.status_.tenant_id_ != tenant_id) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid tennat_id", K(ret), K(tenant_id), K(info));
  } else {
    status.update_ts_ = ObTimeUtility::current_time();
    status.checkpoint_ts_ = info.status_.checkpoint_ts_;
    status.start_ts_ = info.status_.start_ts_;
    status.status_ = info.status_.status_;
    status.tenant_id_ = tenant_id;
    ++update_count_;
    FLOG_INFO("succeed to renew log archive status", K_(update_count), K(tenant_id), K(status));
    SpinWLockGuard guard(lock_);
    if (!status.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid status", K(ret), K(status));
    } else if (OB_FAIL(status_map_.set_refactored(tenant_id, status, 1 /*overwrite*/))) {
      LOG_WARN("failed to set tenant_id", K(ret));
    }
  }
  return ret;
}

void ObBackupInfoMgr::ObBackupInfoUpdateTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ObBackupInfoMgr::get_instance().reload())) {
    LOG_WARN("failed to reload backup info", K(tmp_ret));
  }
}

ObBackupInfoMgr& ObBackupInfoMgr::get_instance()
{
  static ObBackupInfoMgr backup_info;
  return backup_info;
}

ObBackupInfoMgr::ObBackupInfoMgr()
    : is_inited_(false),
      sql_proxy_(nullptr),
      timer_(),
      update_task_(),
      cur_backup_info_(nullptr),
      cur_restore_job_(nullptr),
      cur_base_backup_started_(nullptr),
      lock_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      mutex_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      is_loaded_(false),
      backup_dest_detector_(nullptr)
{}

ObBackupInfoMgr::~ObBackupInfoMgr()
{
  destroy();
}

int ObBackupInfoMgr::init(common::ObMySQLProxy& sql_proxy, ObBackupDestDetector& backup_dest_detector)
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
    cur_base_backup_started_ = &is_base_backup_started_[0];
    backup_dest_detector_ = &backup_dest_detector;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupInfoMgr::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(timer_.schedule(update_task_, DEFAULT_UPDATE_INTERVAL_US, true /*repeate*/))) {
    LOG_WARN("Failed to schedule update task", K(ret));
  }
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

int ObBackupInfoMgr::get_log_archive_backup_info(ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  bool is_loaded = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    is_loaded = is_loaded_;
    if (is_loaded) {
      info = *cur_backup_info_;
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_loaded) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
        LOG_WARN("not loaded yet, try again", K(ret));
      }
    } else if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid info", K(ret), K(info));
    } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_log_archive_backup_info", K(ret), K(info));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_info_from_cache(const uint64_t tenant_id, ObSimplePhysicalRestoreJob& simple_job_info)
{
  int ret = OB_ENTRY_NOT_EXIST;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (is_loaded_) {
      for (int64_t i = 0; i < cur_restore_job_->count(); ++i) {
        ObPhysicalRestoreJob& cur_job = cur_restore_job_->at(i);
        if (cur_job.tenant_id_ == tenant_id) {
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

int ObBackupInfoMgr::get_restore_status_from_cache(const uint64_t tenant_id, PhysicalRestoreStatus& status)
{
  int ret = OB_ENTRY_NOT_EXIST;
  status = PHYSICAL_RESTORE_MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (is_loaded_) {
      for (int64_t i = 0; i < cur_restore_job_->count(); ++i) {
        ObPhysicalRestoreJob& cur_job = cur_restore_job_->at(i);
        if (cur_job.tenant_id_ == tenant_id) {
          status = cur_job.status_;
          break;
        }
      }
    }
  }

  return ret;
}

int ObBackupInfoMgr::get_restore_info(const uint64_t tenant_id, ObPhysicalRestoreInfo& info)
{
  int ret = OB_SUCCESS;
  ObSimplePhysicalRestoreJob simple_job_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_restore_info_from_cache", K(ret), K(tenant_id));
    } else if (OB_FAIL(reload())) {
      LOG_WARN("failed to reload", K(ret));
    } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
      LOG_WARN("failed to get restore info from cache again", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(simple_job_info.copy_to(info))) {
      LOG_WARN("failed to copy physical restore info", K(ret), K(simple_job_info));
    }

    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_info", K(ret), K(tenant_id), K(simple_job_info));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_job_id(const uint64_t tenant_id, int64_t& job_id)
{
  int ret = OB_SUCCESS;
  ObSimplePhysicalRestoreJob simple_job_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_restore_info_from_cache", K(ret), K(tenant_id));
    } else if (OB_FAIL(reload())) {
      LOG_WARN("failed to reload", K(ret));
    } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
      LOG_WARN("failed to get restore info from cache again", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    job_id = simple_job_info.job_id_;
    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_job_id", K(ret), K(tenant_id), K(job_id), K(simple_job_info));
    }
  }
  return ret;
}
int ObBackupInfoMgr::get_restore_status(const uint64_t tenant_id, PhysicalRestoreStatus& status)
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

int ObBackupInfoMgr::get_backup_snapshot_version(int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
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
  } else {
    LOG_INFO("get_backup_snapshot_version", K(ret), K(real_tenant_id), K(backup_snapshot_version));
  }

  return ret;
}

int ObBackupInfoMgr::get_log_archive_checkpoint(int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  snapshot_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cluster_version < CLUSTER_VERSION_2250) {
    snapshot_version = 0;
    LOG_INFO("old cluster version, not check delay delete snapshot version", K(cluster_version));
  } else if (OB_FAIL(info_mgr.get_log_archive_checkpoint(*sql_proxy_, real_tenant_id, snapshot_version))) {
    LOG_WARN("failed to get log archive checkpoint", K(ret));
  } else {
    LOG_INFO("get_log_archive_checkpoint", K(ret), K(real_tenant_id), K(snapshot_version));
  }

  return ret;
}

int ObBackupInfoMgr::record_drop_tenant_log_archive_history(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo info;
  ObFakeBackupLeaseService backup_lease_service;
  const int64_t cluster_version = GET_MIN_CLUSTER_VERSION();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (CLUSTER_VERSION_2250 > cluster_version) {
    LOG_INFO("skip record_drop_tenant_log_archive_history for old cluster version", K(cluster_version));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(*sql_proxy_, false /*for update*/, tenant_id, info))) {
    LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
  } else if (OB_FAIL(info_mgr.update_log_archive_status_history(*sql_proxy_, info, backup_lease_service))) {
    LOG_WARN("failed to update log archive status history", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupInfoMgr::get_delay_delete_schema_version(const uint64_t tenant_id,
    share::schema::ObMultiVersionSchemaService& schema_service, bool& is_backup, int64_t& reserved_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
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
    LOG_TRACE(
        "get_delay_delete_schema_version for restore", K(ret), K(tenant_id), K(reserved_schema_version), K(is_backup));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::get_backup_schema_version(
                 *sql_proxy_, tenant_id, reserved_schema_version))) {
    LOG_WARN("failed to get backup snapshot_version", K(ret));
  } else if (OB_FAIL(info_mgr.is_doing_log_archive(*sql_proxy_, OB_SYS_TENANT_ID, is_backup))) {
    LOG_WARN("failed to get log archive checkpoint", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(
              *sql_proxy_, tenant_name_backup_schema_version))) {
        LOG_WARN("failed to get_tenant_name_backup_schema_version", K(ret));
      } else if (tenant_name_backup_schema_version < reserved_schema_version || 0 == reserved_schema_version) {
        reserved_schema_version = tenant_name_backup_schema_version;
      }
    }

    LOG_TRACE("get_delay_delete_schema_version",
        K(ret),
        K(tenant_id),
        K(tenant_name_backup_schema_version),
        K(reserved_schema_version),
        K(is_backup));
  }

  return ret;
}

int ObBackupInfoMgr::check_if_doing_backup(bool& is_doing)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo log_archive_info;
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
    LOG_ERROR("old cluster version, should not call check_if_doing_backup", K(ret), K(cluster_version));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(*sql_proxy_, real_tenant_id, item, for_update))) {
    LOG_WARN("failed to get backup status", K(ret));
  } else if (0 != STRCMP("STOP", item.get_value_ptr())) {
    is_doing = true;
    FLOG_INFO("doing base data backup", K(item));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, real_tenant_id, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (log_archive_info.status_.status_ != ObLogArchiveStatus::STOP) {
    is_doing = true;
    FLOG_INFO("doing log archive", K(ret), K(log_archive_info));
  }

  FLOG_INFO("check_if_doing_backup", K(ret), K(is_doing), K(item), K(log_archive_info));
  return ret;
}

int ObBackupInfoMgr::reload()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObPhysicalRestoreTableOperator restore_operator;
  ObBackupInfoManager info_manager;
  const bool for_update = false;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  lib::ObMutexGuard mutex_guard(mutex_);
  ObLogArchiveBackupInfo* new_backup_info = &backup_infos_[0];
  RestoreJobArray* new_restore_job = &restore_jobs_[0];
  bool* new_is_backup_started = &is_base_backup_started_[0];

  if (new_backup_info == cur_backup_info_) {
    new_backup_info = &backup_infos_[1];
  }
  if (new_restore_job == cur_restore_job_) {
    new_restore_job = &restore_jobs_[1];
  }

  if (new_is_backup_started == is_base_backup_started_) {
    new_is_backup_started = &is_base_backup_started_[1];
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, *new_backup_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (OB_FAIL(restore_operator.init(sql_proxy_))) {
    LOG_WARN("failed to init restore operator", K(ret));
  } else if (OB_FAIL(restore_operator.get_jobs(*new_restore_job))) {
    LOG_WARN("failed to get new restore info", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret));
  } else if (OB_FAIL(info_manager.is_backup_started(*new_is_backup_started))) {
    LOG_WARN("failed to check is backup started", K(ret));
  } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL) ||
             new_backup_info->status_.status_ != cur_backup_info_->status_.status_ ||
             new_backup_info->status_.round_ != cur_backup_info_->status_.round_) {
    FLOG_INFO("succeed to reload backup info", K(ret), K(*new_backup_info), K(*new_restore_job));
  }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(lock_);
    cur_backup_info_ = new_backup_info;
    cur_restore_job_ = new_restore_job;
    cur_base_backup_started_ = new_is_backup_started;
    is_loaded_ = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_dest_detector_->update_backup_info(*cur_backup_info_))) {
      LOG_WARN("failed to update backup info", K(ret));
    }
  }
  return ret;
}

int ObBackupInfoMgr::is_base_backup_start(bool& is_started)
{
  int ret = OB_SUCCESS;
  bool is_loaded = true;
  is_started = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    is_loaded = is_loaded_;
    if (is_loaded) {
      is_started = *cur_base_backup_started_;
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_loaded) {
      if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
        LOG_WARN("not loaded yet, try again", K(ret));
      }
    } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("is_base_backup_start", K(ret), K(is_started));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_base_data_restore_schema_version(const uint64_t tenant_id, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObSimplePhysicalRestoreJob simple_job_info;
  schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_restore_info_from_cache", K(ret), K(tenant_id));
    } else if (OB_FAIL(reload())) {
      LOG_WARN("failed to reload", K(ret));
    } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
      LOG_WARN("failed to get restore info from cache again", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    schema_version = simple_job_info.schema_version_;
    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_info", K(ret), K(tenant_id), K(simple_job_info));
    }
  }
  return ret;
}

ObRestoreBackupInfoUtil::GetRestoreBackupInfoParam::GetRestoreBackupInfoParam()
    : backup_dest_(nullptr),
      backup_cluster_name_(nullptr),
      cluster_id_(0),
      incarnation_(0),
      backup_tenant_name_(nullptr),
      restore_timestamp_(0),
      passwd_array_(nullptr)
{}

int ObRestoreBackupInfoUtil::get_restore_backup_info(const GetRestoreBackupInfoParam& param, ObRestoreBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest dest;
  ObExternBackupInfoMgr backup_info_mgr;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObExternTenantInfo tenant_info;
  ObExternBackupInfo backup_info;
  ObExternLogArchiveBackupInfo log_archive_backup_info;
  ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;
  ObTenantLogArchiveStatus log_archive_status;
  ObExternTenantLocalityInfoMgr tenant_locality_info_mgr;
  ObExternTenantLocalityInfo tenant_locality_info;
  const int64_t cluster_version = ObClusterVersion::get_instance().get_cluster_version();
  ObTenantNameSimpleMgr tenant_name_mgr;
  ObExternPGListMgr pg_list_mgr;
  uint64_t backup_tenant_id = 0;
  const char* backup_dest = param.backup_dest_;
  const char* backup_cluster_name = param.backup_cluster_name_;
  const int64_t cluster_id = param.cluster_id_;
  const int64_t incarnation = param.incarnation_;
  const char* backup_tenant_name = param.backup_tenant_name_;
  const int64_t restore_timestamp = param.restore_timestamp_;
  const char* passwd_array = param.passwd_array_;
  ObFakeBackupLeaseService fake_backup_lease;

  if (OB_ISNULL(backup_dest) || OB_ISNULL(backup_cluster_name) || cluster_id <= 0 || incarnation < 0 ||
      OB_ISNULL(backup_tenant_name) || restore_timestamp <= 0 || OB_ISNULL(passwd_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get restore backup info get invalid argument",
        K(ret),
        KP(backup_dest),
        KP(backup_cluster_name),
        K(cluster_id),
        KP(backup_tenant_name),
        K(restore_timestamp),
        KP(passwd_array));
  } else if (strlen(backup_dest) >= share::OB_MAX_BACKUP_DEST_LENGTH ||
             strlen(backup_cluster_name) >= OB_MAX_CLUSTER_NAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("backup dest or backup cluster name size over flow", K(ret), K(backup_dest), K(backup_cluster_name));
  } else if (OB_FAIL(dest.set(backup_dest, backup_cluster_name, cluster_id, incarnation))) {
    LOG_WARN(
        "failed to set backup dest", K(ret), K(backup_dest), K(backup_cluster_name), K(cluster_id), K(incarnation));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("faiuiled to init tenant_name mgr", K(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(dest))) {
    LOG_WARN("failed to read backup tenant name mgr", K(ret), K(dest));
  } else if (OB_FAIL(tenant_name_mgr.get_tenant_id(backup_tenant_name, restore_timestamp, backup_tenant_id))) {
    LOG_WARN("failed to backup tenant id", K(ret), K(backup_tenant_name), K(restore_timestamp));
  } else if (OB_FAIL(tenant_info_mgr.init(dest, fake_backup_lease))) {
    LOG_WARN("failed to init tenant info mgr", K(ret), K(dest));
  } else if (OB_FAIL(tenant_info_mgr.find_tenant_info(backup_tenant_id, tenant_info))) {
    LOG_WARN("failed to find tenant info", K(ret), K(backup_tenant_id));
  } else if (OB_FAIL(backup_info_mgr.init(tenant_info.tenant_id_, dest, fake_backup_lease))) {
    LOG_WARN("failed to init backup info mgr", K(ret), K(dest), K(tenant_info));
  } else if (OB_FAIL(backup_info_mgr.find_backup_info(restore_timestamp, passwd_array, backup_info))) {
    LOG_WARN("failed to find backup info", K(ret), K(restore_timestamp), K(tenant_info));
  } else if (OB_FAIL(log_archive_backup_info_mgr.read_extern_log_archive_backup_info(
                 dest, tenant_info.tenant_id_, log_archive_backup_info))) {
    LOG_WARN("failed to read extern log archive backup info", K(ret), K(dest), K(tenant_info));
  } else if (OB_FAIL(log_archive_backup_info.get_log_archive_status(restore_timestamp, log_archive_status))) {
    LOG_WARN("failed to get log_archive_status", K(ret), K(restore_timestamp));
  } else if (backup_info.backup_snapshot_version_ < log_archive_status.start_ts_) {
    ret = OB_ISOLATED_BACKUP_SET;
    LOG_WARN("log archive status is not continues with backup info", K(ret), K(backup_info));
  } else if (OB_FAIL(tenant_locality_info_mgr.init(tenant_info.tenant_id_,
                 backup_info.full_backup_set_id_,
                 backup_info.inc_backup_set_id_,
                 dest,
                 fake_backup_lease))) {
    LOG_WARN("failed to init tenant locality info mgr", K(ret), K(tenant_info));
  } else if (OB_FAIL(tenant_locality_info_mgr.get_extern_tenant_locality_info(tenant_locality_info))) {
    LOG_WARN("failed to find tenant locality info", K(ret), K(tenant_info));
  } else if (backup_info.cluster_version_ > cluster_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot restore from newer cluster to older cluster", K(ret), K(cluster_version), K(backup_info));
  } else if (OB_FAIL(pg_list_mgr.init(backup_tenant_id,
                 backup_info.full_backup_set_id_,
                 backup_info.inc_backup_set_id_,
                 dest,
                 fake_backup_lease))) {
    LOG_WARN("failed to init pg list mgr", K(ret), K(backup_info), K(dest));
  } else if (OB_FAIL(pg_list_mgr.get_sys_pg_list(info.sys_pg_key_list_))) {
    LOG_WARN("failed to get sys pg list", K(ret), K(backup_info), K(dest));
  } else {
    info.compat_mode_ = tenant_info.compat_mode_;
    info.frozen_data_version_ = backup_info.backup_data_version_;
    info.frozen_schema_version_ = backup_info.frozen_schema_version_;
    info.frozen_snapshot_version_ = backup_info.frozen_snapshot_version_;
    info.schema_version_ = backup_info.backup_schema_version_;
    info.snapshot_version_ = backup_info.backup_snapshot_version_;
    STRCPY(info.locality_, tenant_locality_info.locality_.ptr());
    STRCPY(info.primary_zone_, tenant_locality_info.primary_zone_.ptr());
    STRCPY(info.physical_restore_info_.backup_dest_, backup_dest);
    STRCPY(info.physical_restore_info_.cluster_name_, backup_cluster_name);
    info.physical_restore_info_.cluster_id_ = cluster_id;
    info.physical_restore_info_.full_backup_set_id_ = backup_info.full_backup_set_id_;
    info.physical_restore_info_.inc_backup_set_id_ = backup_info.inc_backup_set_id_;
    info.physical_restore_info_.incarnation_ = incarnation;
    info.physical_restore_info_.tenant_id_ = tenant_info.tenant_id_;
    info.physical_restore_info_.log_archive_round_ = log_archive_status.round_;
    info.physical_restore_info_.compatible_ = backup_info.compatible_;
    info.physical_restore_info_.cluster_version_ = backup_info.cluster_version_;
    FLOG_INFO("get_restore_backup_info", K(info), K(log_archive_status), K(backup_info));
  }
  return ret;
}

int ObRestoreBackupInfoUtil::get_restore_sys_table_ids(
    const ObPhysicalRestoreInfo& info, common::ObIArray<common::ObPartitionKey>& pkey_list)
{
  UNUSEDx(info, pkey_list);
  return OB_SUCCESS;
}

ObRestoreFatalErrorReporter& ObRestoreFatalErrorReporter::get_instance()
{
  static ObRestoreFatalErrorReporter reporter;
  return reporter;
}

ObRestoreFatalErrorReporter::ObRestoreFatalErrorReporter()
    : is_inited_(false),
      rpc_proxy_(nullptr),
      rs_mgr_(nullptr),
      mutex_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      report_results_()
{}

ObRestoreFatalErrorReporter::~ObRestoreFatalErrorReporter()
{}

int ObRestoreFatalErrorReporter::init(obrpc::ObCommonRpcProxy& rpc_proxy, share::ObRsMgr& rs_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    rpc_proxy_ = &rpc_proxy;
    rs_mgr_ = &rs_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreFatalErrorReporter::add_restore_error_task(const uint64_t tenant_id, const PhysicalRestoreMod& mod,
    const int32_t result, const int64_t job_id, const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  bool found = false;

  obrpc::ObPhysicalRestoreResult restore_result;
  restore_result.tenant_id_ = tenant_id;
  restore_result.mod_ = mod;
  restore_result.return_ret_ = result;
  restore_result.job_id_ = job_id;
  restore_result.trace_id_ = *ObCurTraceId::get_trace_id();
  restore_result.addr_ = addr;

  lib::ObMutexGuard mutex_guard(mutex_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!restore_result.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < report_results_.count(); ++i) {
    if (report_results_.at(i).tenant_id_ == tenant_id) {
      found = true;
      break;
    }
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(report_results_.push_back(restore_result))) {
      LOG_WARN("failed to add report item", K(ret));
    } else {
      LOG_INFO("succeed to add report item", K(restore_result));
    }
  }
  return ret;
}

int ObRestoreFatalErrorReporter::start()
{
  int ret = OB_SUCCESS;

  FLOG_INFO("ObRestoreFatalErrorReporter start");
  if (OB_FAIL(ObThreadPool::start())) {
    LOG_INFO("ObRestoreFatalErrorReporter start fail", K(ret));
  }
  return ret;
}

void ObRestoreFatalErrorReporter::stop()
{
  FLOG_INFO("ObRestoreFatalErrorReporter stop");
  ObThreadPool::stop();
}

void ObRestoreFatalErrorReporter::wait()
{
  FLOG_INFO("ObRestoreFatalErrorReporter begin wait");
  ObThreadPool::wait();
  FLOG_INFO("ObRestoreFatalErrorReporter finish wait");
}

void ObRestoreFatalErrorReporter::run1()
{
  int tmp_ret = OB_SUCCESS;
  const int64_t REPORT_INTERVAL_US = 10 * 1000 * 1000;
  lib::set_thread_name("RestoreReporter");
  ObCurTraceId::init(GCONF.self_addr_);

  while (!has_set_stop()) {
    if (OB_SUCCESS != (tmp_ret = report_restore_errors())) {
      LOG_WARN("failed to report restore errors", K(tmp_ret));
    }
    if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {  // per 600s
      LOG_INFO("finish report_restore_errors", K(tmp_ret));
    }
    usleep(REPORT_INTERVAL_US);
  }
}

int ObRestoreFatalErrorReporter::report_restore_errors()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObSEArray<obrpc::ObPhysicalRestoreResult, 16> report_results;
  bool remove_report_task = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    lib::ObMutexGuard mutex_guard(mutex_);
    if (OB_FAIL(report_results.assign(report_results_))) {
      LOG_WARN("failed to copy report items", K(ret));
    }
  }

  if (OB_SUCC(ret) && report_results.count() > 0) {
    for (int64_t i = 0; !has_set_stop() && i < report_results.count(); ++i) {  // ignore ret
      obrpc::ObPhysicalRestoreResult& result = report_results.at(i);
      if (OB_SUCCESS != (tmp_ret = report_restore_error_(result))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to report restore error", K(tmp_ret), K(ret), K(result));
      } else if (OB_SUCCESS != (tmp_ret = remove_restore_error_task_(result))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to remove report result", K(tmp_ret), K(result));
      }
    }
  }
  return ret;
}

int ObRestoreFatalErrorReporter::remove_restore_error_task_(const obrpc::ObPhysicalRestoreResult& result)
{
  int ret = OB_SUCCESS;

  lib::ObMutexGuard mutex_guard(mutex_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < report_results_.count(); ++i) {
    if (report_results_.at(i).tenant_id_ == result.tenant_id_) {
      if (OB_FAIL(report_results_.remove(i))) {
        LOG_WARN("failed to remove restore report task", K(ret), K(i), K(result));
      } else {
        LOG_INFO("succeed to remove restore report task", K(ret), K(i), K(result));
      }
      break;
    }
  }
  return ret;
}

int ObRestoreFatalErrorReporter::report_restore_error_(const obrpc::ObPhysicalRestoreResult& result)
{
  int ret = OB_SUCCESS;
  common::ObAddr rs_addr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    STORAGE_LOG(WARN, "get master root service failed", K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(rs_addr).send_physical_restore_result(result))) {
    LOG_WARN("failed to send_physical_restore_result", K(ret), K(rs_addr), K(result));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("restore task not exist, skip report", K(result));
    }
  }
  FLOG_INFO("finish report_restore_error_", K(ret), K(rs_addr), K(result));
  return ret;
}

ObBackupDestDetector& ObBackupDestDetector::get_instance()
{
  static ObBackupDestDetector detector;
  return detector;
}

int ObBackupDestDetector::start()
{
  int ret = OB_SUCCESS;

  FLOG_INFO("ObBackupDestDetector start");
  if (OB_FAIL(ObThreadPool::start())) {
    LOG_INFO("ObBackupDestDetector start fail", K(ret));
  }
  return ret;
}

void ObBackupDestDetector::stop()
{
  FLOG_INFO("ObBackupDestDetector stop");
  ObThreadPool::stop();
  wakeup();
}

void ObBackupDestDetector::wait()
{
  FLOG_INFO("ObBackupDestDetector begin wait");
  ObThreadPool::wait();
  FLOG_INFO("ObBackupDestDetector finish wait");
}

ObBackupDestDetector::ObBackupDestDetector()
    : is_inited_(false), lock_(ObLatchIds::BACKUP_INFO_MGR_LOCK), info_(), is_bad_(false), wakeup_count_(0)
{}

ObBackupDestDetector::~ObBackupDestDetector()
{}

int ObBackupDestDetector::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::BACKUP_INFO_MGR_LOCK_WAIT))) {
    LOG_WARN("failed to init cond", K(ret));
  } else {
    is_bad_ = false;
    wakeup_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupDestDetector::run1()
{
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name("BackupDestDetect");
  ObCurTraceId::init(GCONF.self_addr_);

  while (!has_set_stop()) {
    if (OB_SUCCESS != (tmp_ret = check_backup_dest())) {
      LOG_WARN("failed to check_backup_dest", K(tmp_ret));
    }
    if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {  // per 600s
      LOG_INFO("[BACKUP_MOUNT_FILE]finish check_backup_dest", K_(is_bad), K_(info));
    }
    idle();
  }
}

void ObBackupDestDetector::idle()
{
  const int32_t IDLE_TIME_MS = 10 * 1000;
  const int32_t PER_IDLE_TIME_MS = 1000;
  int32_t left_time_ms = IDLE_TIME_MS;
  ObThreadCondGuard guard(cond_);
  while (!has_set_stop() && wakeup_count_ == 0) {
    const int64_t wait_time_ms = MIN(PER_IDLE_TIME_MS, left_time_ms);
    if (wait_time_ms <= 0) {
      break;
    } else {
      left_time_ms -= wait_time_ms;
      (void)cond_.wait(wait_time_ms);  // ignore ret
    }
  }
  wakeup_count_ = 0;
}

void ObBackupDestDetector::wakeup()
{
  ObThreadCondGuard guard(cond_);
  wakeup_count_++;
  (void)cond_.broadcast();  // ignore ret
}

int ObBackupDestDetector::get_is_backup_dest_bad(const int64_t round_id, bool& is_bad)
{
  int ret = OB_SUCCESS;
  is_bad = false;

  SpinRLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (round_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(round_id));
  } else if (!is_bad_) {
    // not bad
  } else if (info_.status_.round_ != round_id) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // per 10s
      LOG_INFO("[BACKUP_MOUNT_FILE]round not match, treat it as not bad", K(round_id), K_(is_bad), K_(info));
    }
  } else if (ObLogArchiveStatus::BEGINNING != info_.status_.status_ &&
             ObLogArchiveStatus::DOING != info_.status_.status_) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // per 10s
      LOG_INFO("[BACKUP_MOUNT_FILE]log archive status is not beginning and doing, treat it as not bad",
          K(round_id),
          K_(is_bad),
          K_(info));
    }
  } else {
    is_bad = is_bad_;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // per 10s
      LOG_INFO("backup dest maybe is not mount properly", K(round_id), K_(is_bad), K_(info));
    }
  }

  return ret;
}

int ObBackupDestDetector::update_backup_info(const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  bool is_changed = false;

  {
    SpinWLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (!info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(info));
    } else if (0 != STRNCMP(info.backup_dest_, info_.backup_dest_, sizeof(info_.backup_dest_)) ||
               info.status_.round_ != info_.status_.round_) {
      is_changed = true;
      FLOG_INFO("[BACKUP_MOUNT_FILE]backup dest or round changed, clear backup mount file bad flag", K(info_), K(info));
      is_bad_ = false;
    }

    if (OB_SUCC(ret)) {
      info_ = info;
    }
  }

  if (is_changed) {
    wakeup();
  }
  return ret;
}

int ObBackupDestDetector::check_backup_dest()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo info;
  bool is_bad;

  {
    SpinRLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else {
      info = info_;
      is_bad = is_bad_;
    }
  }

  if (OB_SUCC(ret) && info.is_valid() && ObLogArchiveStatus::STOP != info.status_.status_) {
    if (is_bad) {
      LOG_ERROR("backup dest is bad, skip check backup dest", K(info));
    } else if (OB_FAIL(check_backup_dest_(info, is_bad))) {
      LOG_WARN("failed to check backup dest", K(ret), K(info));
    } else if (is_bad) {
      SpinWLockGuard guard(lock_);
      if (0 == STRNCMP(info.backup_dest_, info_.backup_dest_, sizeof(info_.backup_dest_)) &&
          info.status_.round_ == info_.status_.round_) {
        is_bad_ = true;
        LOG_ERROR("[BACKUP_MOUNT_FILE]backup mount file not file, mark backup dest bad", K(info));
      } else {
        FLOG_WARN("[BACKUP_MOUNT_FILE]backup info is changed, cannot set bad", K(info), K(info_));
      }
    }
  }

  return ret;
}

int ObBackupDestDetector::check_backup_dest_(ObLogArchiveBackupInfo& backup_info, bool& is_bad)
{
  int ret = OB_SUCCESS;
  bool need_check = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ObBackupMountFile::need_check_mount_file(backup_info, need_check))) {
    LOG_WARN("failed to need check mount file", K(ret), K(backup_info));
  } else if (!need_check || !backup_info.status_.is_mount_file_created_) {
    // no need check
  } else if (OB_FAIL(ObBackupMountFile::check_mount_file(backup_info))) {
    if (OB_BACKUP_MOUNT_FILE_NOT_VALID == ret) {
      LOG_ERROR(
          "[BACKUP_MOUNT_FILE]backup mount file is not valid, maybe nfs is not mount properly", K(ret), K(backup_info));
      ret = OB_SUCCESS;
      is_bad = true;
    } else {
      LOG_WARN("failed to check backup mount file", K(ret), K(backup_info));
    }
  }
  return ret;
}
