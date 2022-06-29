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
#include "share/backup/ob_backup_backupset_operator.h"
#include "share/backup/ob_backup_backuppiece_operator.h"
#include "lib/utility/utility.h"
#include "ob_backup_manager.h"
using namespace oceanbase;
using namespace common;
using namespace share;
using namespace common;

ObLogArchiveSimpleInfo::ObLogArchiveSimpleInfo()
    : update_ts_(0),
      checkpoint_ts_(0),
      start_ts_(0),
      status_(share::ObLogArchiveStatus::INVALID),
      tenant_id_(OB_INVALID_ID),
      cur_piece_id_(0),
      cur_piece_create_date_(0),
      is_piece_freezing_(false),
      prev_piece_id_(0),
      prev_piece_create_date_(0)
{}

void ObLogArchiveSimpleInfo::reset()
{
  update_ts_ = 0;
  checkpoint_ts_ = 0;
  start_ts_ = 0;
  status_ = ObLogArchiveStatus::INVALID;
  tenant_id_ = OB_INVALID_ID;
  cur_piece_id_ = 0;
  cur_piece_create_date_ = 0;
  is_piece_freezing_ = false;
  prev_piece_id_ = 0;
  prev_piece_create_date_ = 0;
}

bool ObLogArchiveSimpleInfo::is_valid() const
{
  return update_ts_ > 0 && checkpoint_ts_ >= 0 && start_ts_ >= 0 && ObLogArchiveStatus::is_valid(status_) &&
         OB_INVALID_ID != tenant_id_ && cur_piece_id_ >= 0 && cur_piece_create_date_ >= 0 &&
         (!is_piece_freezing_ || (prev_piece_id_ > 0 && prev_piece_create_date_ > 0));
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

ObLogArchiveInfoMgr &ObLogArchiveInfoMgr::get_instance()
{
  static ObLogArchiveInfoMgr mgr;
  return mgr;
}

int ObLogArchiveInfoMgr::init(common::ObMySQLProxy &sql_proxy)
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
    const uint64_t tenant_id, const int64_t need_ts, ObLogArchiveSimpleInfo &status)
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

int ObLogArchiveInfoMgr::get_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo &status)
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
      const ObLogArchiveSimpleInfo &status = it->second;
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

int ObLogArchiveInfoMgr::renew_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo &status)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfo sys_backup_info;
  ObNonFrozenBackupPieceInfo sys_backup_piece;
  bool got_tenant_info = false;
  status.reset();

  lib::ObMutexGuard mutex_guard(mutex_);
  status.update_ts_ = ObTimeUtility::current_time();
  status.tenant_id_ = tenant_id;

  if (OB_FAIL(try_retire_status_())) {
    LOG_WARN("failed to try_retire_status_", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info_compatible(*sql_proxy_, tenant_id, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
      usleep(100 * 1000);  // sleep 100ms
    } else {
      LOG_INFO("new tenant log archive info maybe not exist, treat is as beginning status", K(tenant_id));
      status.checkpoint_ts_ = 0;
      status.start_ts_ = 0;
      status.status_ = ObLogArchiveStatus::BEGINNING;
      ret = OB_SUCCESS;
    }
  } else if (info.status_.tenant_id_ != tenant_id) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid tennat_id", K(ret), K(tenant_id), K(info));
  } else {
    got_tenant_info = true;
    status.checkpoint_ts_ = info.status_.checkpoint_ts_;
    status.start_ts_ = info.status_.start_ts_;
    status.status_ = info.status_.status_;
  }

  if (FAILEDx(ObBackupInfoMgr::fetch_sys_log_archive_backup_info_and_piece(
          *sql_proxy_, sys_backup_info, sys_backup_piece))) {
    LOG_WARN("Failed to fetch_sys_log_archive_backup_info_and_piece", K(ret));
    usleep(100 * 1000);  // sleep 100ms
  } else if (got_tenant_info && sys_backup_piece.cur_piece_info_.key_.round_id_ != info.status_.round_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("round id not match", K(ret), K(info), K(sys_backup_piece), K(sys_backup_info));
  } else {
    status.cur_piece_id_ = sys_backup_piece.cur_piece_info_.key_.backup_piece_id_;
    status.cur_piece_create_date_ = sys_backup_piece.cur_piece_info_.create_date_;
    if (sys_backup_piece.has_prev_piece_info_) {
      status.is_piece_freezing_ = true;
      status.prev_piece_id_ = sys_backup_piece.prev_piece_info_.key_.backup_piece_id_;
      status.prev_piece_create_date_ = sys_backup_piece.prev_piece_info_.create_date_;
    } else {
      status.is_piece_freezing_ = false;
      status.prev_piece_id_ = -1;
      status.prev_piece_create_date_ = -1;
    }
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

ObBackupInfoMgr &ObBackupInfoMgr::get_instance()
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
      cur_backup_piece_(nullptr),
      cur_restore_job_(nullptr),
      lock_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      mutex_(ObLatchIds::BACKUP_INFO_MGR_LOCK),
      is_backup_loaded_(false),
      is_restore_loaded_(false),
      backup_dest_detector_(nullptr),
      log_archive_checkpoint_interval_()
{}

ObBackupInfoMgr::~ObBackupInfoMgr()
{
  destroy();
}

int ObBackupInfoMgr::init(common::ObMySQLProxy &sql_proxy, ObBackupDestDetector &backup_dest_detector)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(timer_.init("BackupInfoUpdate"))) {
    LOG_WARN("failed to init backup info update task", K(ret));
  } else if (OB_FAIL(update_log_archive_checkpoint_interval_())) {
    LOG_WARN("failed to update_log_archive_checkpoint_interval_", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    cur_backup_info_ = &backup_infos_[0];
    cur_backup_piece_ = &backup_pieces_[0];
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

int ObBackupInfoMgr::get_log_archive_backup_info(ObLogArchiveBackupInfo &info)
{
  int ret = OB_SUCCESS;
  ObNonFrozenBackupPieceInfo backup_piece;
  if (OB_FAIL(get_log_archive_backup_info_and_piece(info, backup_piece))) {
    if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      LOG_WARN("failed to get_log_archive_backup_info_and_piece", K(ret));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_log_archive_backup_info_and_piece(
    ObLogArchiveBackupInfo &info, ObNonFrozenBackupPieceInfo &piece)
{
  int ret = OB_SUCCESS;
  bool is_loaded = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    is_loaded = is_backup_loaded_;
    if (is_loaded) {
      info = *cur_backup_info_;
      piece = *cur_backup_piece_;
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_loaded) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
        LOG_WARN("not loaded yet, try again", K(ret));
      }
    } else if (!info.is_valid() ||
               (ObLogArchiveStatus::STOP != info.status_.status_ &&
                   info.status_.compatible_ >= ObTenantLogArchiveStatus::COMPATIBLE_VERSION_2 && !piece.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid info", K(ret), K(info), K(piece));
    } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_log_archive_backup_info", K(ret), K(info), K(piece));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_info_from_cache(const uint64_t tenant_id, ObSimplePhysicalRestoreJob &simple_job_info)
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

int ObBackupInfoMgr::get_restore_status_from_cache(const uint64_t tenant_id, PhysicalRestoreStatus &status)
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
        if (cur_job.tenant_id_ == tenant_id) {
          status = cur_job.status_;
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  }

  return ret;
}

int ObBackupInfoMgr::get_restore_info(const uint64_t tenant_id, ObPhysicalRestoreInfo &info)
{
  return get_restore_info(false /*need_valid_restore_schema_version*/, tenant_id, info);
}

int ObBackupInfoMgr::get_restore_info(
    const bool need_valid_restore_schema_version, const uint64_t tenant_id, ObPhysicalRestoreInfo &info)
{
  int ret = OB_SUCCESS;
  ObSimplePhysicalRestoreJob simple_job_info;

  bool need_reload = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_restore_info_from_cache", K(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
      need_reload = true;
    }
  } else if (need_valid_restore_schema_version && (simple_job_info.restore_info_.restore_schema_version_ <= 0)) {
    need_reload = true;
    simple_job_info.reset();
  }

  if (OB_SUCC(ret) && need_reload) {
    if (OB_FAIL(reload())) {
      LOG_WARN("failed to reload", K(ret));
    } else if (OB_FAIL(get_restore_info_from_cache(tenant_id, simple_job_info))) {
      LOG_WARN("failed to get restore info from cache again", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (need_valid_restore_schema_version && simple_job_info.restore_info_.restore_schema_version_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid restore_info", K(ret), K(simple_job_info), K(need_valid_restore_schema_version));
    } else if (OB_FAIL(simple_job_info.copy_to(info))) {
      LOG_WARN("failed to copy physical restore info", K(ret), K(simple_job_info));
    }

    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_info", K(ret), K(tenant_id), K(simple_job_info), K(need_valid_restore_schema_version));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_job_id(const uint64_t tenant_id, int64_t &job_id)
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

int ObBackupInfoMgr::get_restore_piece_list(
    const uint64_t tenant_id, common::ObIArray<share::ObSimpleBackupPiecePath> &piece_list)
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
    if (OB_FAIL(piece_list.assign(simple_job_info.restore_info_.get_backup_piece_path_list()))) {
      LOG_WARN("failed to assign piece list", KR(ret));
    }
    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_job_id", K(ret), K(tenant_id), K(simple_job_info));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_set_list(
    const uint64_t tenant_id, common::ObIArray<share::ObSimpleBackupSetPath> &set_list)
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
    if (OB_FAIL(set_list.assign(simple_job_info.restore_info_.get_backup_set_path_list()))) {
      LOG_WARN("failed to assign set list", KR(ret));
    }
    if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL)) {
      FLOG_INFO("get_restore_job_id", K(ret), K(tenant_id), K(simple_job_info));
    }
  }
  return ret;
}

int ObBackupInfoMgr::get_restore_status(const uint64_t tenant_id, PhysicalRestoreStatus &status)
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

int ObBackupInfoMgr::get_backup_snapshot_version(int64_t &backup_snapshot_version)
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
  } else if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // for each 60s
    LOG_INFO("get_backup_snapshot_version", K(ret), K(real_tenant_id), K(backup_snapshot_version));
  }

  return ret;
}

int ObBackupInfoMgr::get_log_archive_checkpoint(int64_t &snapshot_version)
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

int ObBackupInfoMgr::get_delay_delete_schema_version(const uint64_t tenant_id,
    share::schema::ObMultiVersionSchemaService &schema_service, bool &is_backup, int64_t &reserved_schema_version)
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
  } else if (OB_FAIL(check_if_doing_backup(is_backup))) {
    LOG_WARN("failed to check_if_doing_backup", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(
              *sql_proxy_, tenant_name_backup_schema_version))) {
        LOG_WARN("failed to get_tenant_name_backup_schema_version", K(ret));
      } else if (tenant_name_backup_schema_version < reserved_schema_version || 0 == reserved_schema_version) {
        reserved_schema_version = tenant_name_backup_schema_version;
      }
    }

    LOG_INFO("get_delay_delete_schema_version",
        K(ret),
        K(tenant_id),
        K(tenant_name_backup_schema_version),
        K(reserved_schema_version),
        K(is_backup));
  }

  return ret;
}

int ObBackupInfoMgr::check_if_doing_backup(bool &is_doing)
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
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info_compatible(*sql_proxy_, real_tenant_id, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (log_archive_info.status_.status_ != ObLogArchiveStatus::STOP) {
    is_doing = true;
    FLOG_INFO("doing log archive", K(ret), K(log_archive_info));
  }

  FLOG_INFO("check_if_doing_backup", K(ret), K(is_doing), K(item), K(log_archive_info));
  return ret;
}

int ObBackupInfoMgr::check_if_doing_backup_backup(bool &is_doing)
{
  int ret = OB_SUCCESS;
  is_doing = false;
  ObBackupInfoManager manager;
  bool enable_backup_archivelog = false;
  ObArray<ObBackupBackupsetJobInfo> backup_set_jobs;
  ObArray<ObBackupBackupPieceJobInfo> backup_piece_jobs;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (OB_FAIL(manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
      LOG_WARN("failed to init backup info manager", KR(ret));
    } else if (OB_FAIL(manager.get_enable_auto_backup_archivelog(
                   OB_SYS_TENANT_ID, *sql_proxy_, enable_backup_archivelog))) {
      LOG_WARN("failed to update enable auto backup archivelog", KR(ret));
    } else if (OB_FAIL(ObBackupBackupsetOperator::get_one_task(backup_set_jobs, *sql_proxy_))) {
      LOG_WARN("failed to get all task items", K(ret));
    } else if (OB_FAIL(ObBackupBackupPieceJobOperator::get_one_job(*sql_proxy_, backup_piece_jobs))) {
      LOG_WARN("failed to get all job items", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_doing = !backup_set_jobs.empty() || !backup_piece_jobs.empty() || enable_backup_archivelog;
    }
  }
  return ret;
}

int ObBackupInfoMgr::update_log_archive_checkpoint_interval_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t log_archive_checkpoint_interval = GCONF.log_archive_checkpoint_interval;
  ObBackupDestOpt backup_dest_opt;

  if (OB_SUCCESS != (tmp_ret = backup_dest_opt.init(false /*is_backup_backup*/))) {
    LOG_WARN("failed to get_backup_dest_opt", K(tmp_ret), K(lbt()));
  } else {
    log_archive_checkpoint_interval = backup_dest_opt.log_archive_checkpoint_interval_;
  }

  const int64_t cur_log_archive_checkpoint_interval = ATOMIC_LOAD(&log_archive_checkpoint_interval_);
  if (log_archive_checkpoint_interval != cur_log_archive_checkpoint_interval) {
    FLOG_INFO("update log_archive_checkpoint_interval",
        K(cur_log_archive_checkpoint_interval),
        K(log_archive_checkpoint_interval));
    ATOMIC_STORE(&log_archive_checkpoint_interval_, log_archive_checkpoint_interval);
  }
  return ret;
}

int ObBackupInfoMgr::reload()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
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
  } else if (OB_FAIL(update_log_archive_checkpoint_interval_())) {
    LOG_WARN("failed to update_log_archive_checkpoint_interval_", K(ret));
  } else if (OB_FAIL(fetch_sys_log_archive_backup_info_and_piece(*sql_proxy_, *new_backup_info, *new_backup_piece))) {
    LOG_WARN("failed to get log archive backup info and piece", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret));
  } else if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL) ||
             new_backup_info->status_.status_ != cur_backup_info_->status_.status_ ||
             new_backup_info->status_.round_ != cur_backup_info_->status_.round_ ||
             new_backup_info->status_.backup_piece_id_ != cur_backup_info_->status_.backup_piece_id_) {
    FLOG_INFO("succeed to reload backup info", K(ret), K(*new_backup_info), K(*new_backup_piece));
  }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(lock_);
    cur_backup_info_ = new_backup_info;
    cur_backup_piece_ = new_backup_piece;
    is_backup_loaded_ = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_dest_detector_->update_backup_info(*cur_backup_info_))) {
      LOG_WARN("failed to update backup info", K(ret));
    }
  }

  // reload restore info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_operator.init(sql_proxy_))) {
      LOG_WARN("failed to init restore operator", K(ret));
    } else if (OB_FAIL(restore_operator.get_jobs(*new_restore_job))) {
      LOG_WARN("failed to get new restore info", K(ret));
    } else {
      if (REACH_TIME_INTERVAL(OB_DEFAULT_BACKUP_LOG_INTERVAL) ||
          new_restore_job->count() != cur_restore_job_->count()) {
        FLOG_INFO("succeed to reload restore job", K(*new_restore_job));
      }
      SpinWLockGuard guard(lock_);
      cur_restore_job_ = new_restore_job;
      is_restore_loaded_ = true;
    }
  }
  return ret;
}

int ObBackupInfoMgr::fetch_sys_log_archive_backup_info_and_piece(common::ObMySQLProxy &sql_proxy,
    ObLogArchiveBackupInfo &new_backup_info, ObNonFrozenBackupPieceInfo &new_backup_piece)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  const bool for_update = false;
  bool need_retry = true;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  int64_t retry_count = 0;
  const int64_t MAX_RETRY_COUNT = 3;

  while (OB_SUCC(ret) && need_retry) {
    new_backup_info.reset();
    new_backup_piece.reset();
    need_retry = false;

    if (OB_FAIL(info_mgr.get_log_archive_backup_info_compatible(sql_proxy, tenant_id, new_backup_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret));
    } else if (ObLogArchiveStatus::STOP == new_backup_info.status_.status_) {
      LOG_INFO("log archive is stop, no need to get non frozen piece", K(ret), K(new_backup_info));
    } else if (new_backup_info.status_.compatible_ < ObTenantLogArchiveStatus::COMPATIBLE_VERSION_1) {
      LOG_INFO("old version log archive has no piece info", K(ret), "compatible", new_backup_info.status_.compatible_);
      if (OB_FAIL(new_backup_info.get_piece_key(new_backup_piece.cur_piece_info_.key_))) {
        LOG_WARN("Failed to get piece key", K(ret), K(new_backup_info));
      } else if (OB_FAIL(new_backup_piece.cur_piece_info_.backup_dest_.assign(new_backup_info.backup_dest_))) {
        LOG_WARN("failed to copy backup dest", K(ret), K(new_backup_info));
      } else {
        new_backup_piece.cur_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE;
        new_backup_piece.cur_piece_info_.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
        new_backup_piece.cur_piece_info_.start_ts_ = new_backup_info.status_.start_ts_;
        new_backup_piece.cur_piece_info_.checkpoint_ts_ = new_backup_info.status_.checkpoint_ts_;
        new_backup_piece.cur_piece_info_.create_date_ = 0;
        new_backup_piece.cur_piece_info_.max_ts_ = INT64_MAX;
        new_backup_piece.cur_piece_info_.compatible_ = ObTenantLogArchiveStatus::NONE;
        new_backup_piece.cur_piece_info_.start_piece_id_ = 0;
      }
    } else if (OB_FAIL(
                   info_mgr.get_non_frozen_backup_piece(sql_proxy, for_update, new_backup_info, new_backup_piece))) {
      LOG_WARN("failed to get non frozen backup piece", K(ret), K(new_backup_info));
    } else if (ObLogArchiveStatus::STOPPING != new_backup_info.status_.status_ &&
               ObBackupPieceStatus::BACKUP_PIECE_ACTIVE != new_backup_piece.cur_piece_info_.status_) {
      ++retry_count;
      if (retry_count > MAX_RETRY_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("backup info and piece not match, expect retry will get matched info piece and once",
            K(ret),
            K(new_backup_info),
            K(new_backup_piece));
      } else {
        need_retry = true;
        FLOG_INFO(
            "backup info and piece not match, need retry", K(retry_count), K(new_backup_info), K(new_backup_piece));
      }
    } else {
      LOG_INFO("fetch_sys_log_archive_backup_info_and_piece", K(retry_count), K(new_backup_info), K(new_backup_piece));
    }
  }

  return ret;
}

int64_t ObBackupInfoMgr::get_log_archive_checkpoint_interval() const
{
  return ATOMIC_LOAD(&log_archive_checkpoint_interval_);
}

int ObBackupInfoMgr::get_base_data_restore_schema_version(const uint64_t tenant_id, int64_t &schema_version)
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

int ObBackupInfoMgr::get_restore_backup_snapshot_version(const uint64_t tenant_id, int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObSimplePhysicalRestoreJob simple_job_info;
  snapshot_version = 0;

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
    snapshot_version = simple_job_info.snapshot_version_;
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

int ObRestoreBackupInfoUtil::GetRestoreBackupInfoParam::get_largest_backup_set_path(
    share::ObSimpleBackupSetPath &simple_path) const
{
  int ret = OB_SUCCESS;
  simple_path.reset();
  int64_t idx = -1;
  int64_t largest_backup_set_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_path_list_.count(); ++i) {
    const share::ObSimpleBackupSetPath &path = backup_set_path_list_.at(i);
    if (path.backup_set_id_ > largest_backup_set_id) {
      largest_backup_set_id = path.backup_set_id_;
      idx = i;
    }
  }
  if (OB_SUCC(ret)) {
    if (idx >= 0) {
      simple_path = backup_set_path_list_.at(idx);
      LOG_INFO("largest backup set path", K(simple_path));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("backup set path not found", K(backup_set_path_list_));
    }
  }
  return ret;
}

int ObRestoreBackupInfoUtil::GetRestoreBackupInfoParam::get_smallest_backup_piece_path(
    share::ObSimpleBackupPiecePath &simple_path) const
{
  int ret = OB_SUCCESS;
  simple_path.reset();
  if (backup_piece_path_list_.empty()) {
    ret = OB_ERR_SYS;
    LOG_WARN("backup piece path list should not be empty", K(ret));
  } else {
    simple_path = backup_piece_path_list_.at(0);
  }
  return ret;
}

int ObRestoreBackupInfoUtil::get_restore_backup_info(const GetRestoreBackupInfoParam &param, ObRestoreBackupInfo &info)
{
  int ret = OB_SUCCESS;
  bool is_cluster_level = param.backup_set_path_list_.empty();
  if (is_cluster_level) {
    if (OB_FAIL(get_restore_backup_info_v1_(param, info))) {
      LOG_WARN("failed to get restore backup info v1", KR(ret));
    }
  } else {
    if (OB_FAIL(get_restore_backup_info_v2_(param, info))) {
      LOG_WARN("failed to get restore backup info v1", KR(ret));
    }
  }
  return ret;
}

int ObRestoreBackupInfoUtil::get_restore_backup_info_v1_(
    const GetRestoreBackupInfoParam &param, ObRestoreBackupInfo &info)
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
  const char *backup_dest = param.backup_dest_;
  const char *backup_cluster_name = param.backup_cluster_name_;
  const int64_t cluster_id = param.cluster_id_;
  const int64_t incarnation = param.incarnation_;
  const char *backup_tenant_name = param.backup_tenant_name_;
  const int64_t restore_timestamp = param.restore_timestamp_;
  const char *passwd_array = param.passwd_array_;
  ObFakeBackupLeaseService fake_backup_lease;
  bool is_snapshot_restore = false;

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
  } else if (OB_FAIL(check_is_snapshot_restore(backup_info.backup_snapshot_version_,
                 restore_timestamp,
                 backup_info.cluster_version_,
                 is_snapshot_restore))) {
    LOG_WARN("failed to check is snapshot restore", K(ret), K(backup_info), K(restore_timestamp));
  } else if (is_snapshot_restore) {
    LOG_INFO("backup info backup snapshot version equal to restore timestamp", K(backup_info), K(restore_timestamp));
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
                 backup_info.date_,
                 backup_info.compatible_,
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
                 backup_info.date_,
                 backup_info.compatible_,
                 fake_backup_lease))) {
    LOG_WARN("failed to init pg list mgr", K(ret), K(backup_info), K(dest));
  } else if (OB_FAIL(pg_list_mgr.get_sys_pg_list(info.sys_pg_key_list_))) {
    LOG_WARN("failed to get sys pg list", K(ret), K(backup_info), K(dest));
  } else if (info.sys_pg_key_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys pg key list should not be empty", K(ret), K(backup_info), K(dest));
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
    info.physical_restore_info_.backup_date_ = backup_info.date_;
    FLOG_INFO("get_restore_backup_info", K(info), K(log_archive_status), K(backup_info));
  }
  return ret;
}

int ObRestoreBackupInfoUtil::get_restore_backup_info_v2_(
    const GetRestoreBackupInfoParam &param, ObRestoreBackupInfo &info)
{
  int ret = OB_SUCCESS;
  const char *backup_dest = param.backup_dest_;
  const char *backup_cluster_name = param.backup_cluster_name_;
  const int64_t cluster_id = param.cluster_id_;
  const int64_t incarnation = param.incarnation_;
  const int64_t restore_timestamp = param.restore_timestamp_;
  ObBackupPieceInfo piece_info;
  ObBackupSetFileInfo backup_set_info;
  ObExternTenantLocalityInfo tenant_locality_info;
  bool is_snapshot_restore = false;
  if (OB_FAIL(
          inner_get_restore_backup_set_info_(param, backup_set_info, tenant_locality_info, info.sys_pg_key_list_))) {
    LOG_WARN("failed to inner get restore backup set info", K(ret));
  } else if (OB_FAIL(check_is_snapshot_restore(backup_set_info.snapshot_version_,
                 param.restore_timestamp_,
                 backup_set_info.cluster_version_,
                 is_snapshot_restore))) {
    LOG_WARN("failed to check is snapshot backup", K(ret), K(backup_set_info));
  } else if (!is_snapshot_restore && OB_FAIL(inner_get_restore_backup_piece_info_(param, piece_info))) {
    LOG_WARN("failed to inner get restore backup piece info", K(ret));
  } else if (!is_snapshot_restore && backup_set_info.snapshot_version_ < piece_info.start_ts_) {
    ret = OB_ISOLATED_BACKUP_SET;
    LOG_WARN("log archive status is not continues with backup info", K(ret), K(backup_set_info));
  } else {
    info.compat_mode_ = tenant_locality_info.compat_mode_;
    info.frozen_data_version_ = backup_set_info.backup_data_version_;
    info.frozen_schema_version_ = backup_set_info.backup_schema_version_;
    info.frozen_snapshot_version_ = backup_set_info.snapshot_version_;
    info.schema_version_ = backup_set_info.backup_schema_version_;
    info.snapshot_version_ = backup_set_info.snapshot_version_;
    STRCPY(info.locality_, tenant_locality_info.locality_.ptr());
    STRCPY(info.primary_zone_, tenant_locality_info.primary_zone_.ptr());
    STRCPY(info.physical_restore_info_.backup_dest_, backup_dest);
    STRCPY(info.physical_restore_info_.cluster_name_, backup_cluster_name);
    info.physical_restore_info_.cluster_id_ = cluster_id;
    info.physical_restore_info_.full_backup_set_id_ = ObBackupType::FULL_BACKUP == backup_set_info.backup_type_.type_
                                                          ? backup_set_info.backup_set_id_
                                                          : backup_set_info.prev_full_backup_set_id_;
    info.physical_restore_info_.inc_backup_set_id_ = backup_set_info.backup_set_id_;
    info.physical_restore_info_.incarnation_ = incarnation;
    info.physical_restore_info_.tenant_id_ = tenant_locality_info.tenant_id_;
    info.physical_restore_info_.compatible_ = backup_set_info.compatible_;
    info.physical_restore_info_.cluster_version_ = backup_set_info.cluster_version_;
    info.physical_restore_info_.backup_date_ = backup_set_info.date_;
    if (!is_snapshot_restore) {
      info.physical_restore_info_.log_archive_round_ = piece_info.key_.round_id_;
    }
    FLOG_INFO("get_restore_backup_info", K(info), K(piece_info), K(backup_set_info));
  }
  return ret;
}

int ObRestoreBackupInfoUtil::inner_get_restore_backup_set_info_(const GetRestoreBackupInfoParam &param,
    ObBackupSetFileInfo &backup_set_info, ObExternTenantLocalityInfo &tenant_locality_info,
    common::ObIArray<common::ObPGKey> &sys_pg_key_list)
{
  int ret = OB_SUCCESS;
  ObFakeBackupLeaseService fake_backup_lease;
  ObExternPGListMgr pg_list_mgr;
  ObExternSingleBackupSetInfoMgr backup_info_mgr;
  ObExternTenantLocalityInfoMgr tenant_locality_info_mgr;
  const char *passwd_array = param.passwd_array_;
  const int64_t cluster_version = ObClusterVersion::get_instance().get_cluster_version();
  ObSimpleBackupSetPath simple_set_path;  // largest backup set path
  if (OB_FAIL(param.get_largest_backup_set_path(simple_set_path))) {
    LOG_WARN("failed to get smallest largest backup set path", K(ret));
  } else if (OB_FAIL(backup_info_mgr.init(simple_set_path, fake_backup_lease))) {
    LOG_WARN("failed to init backup info mgr", K(ret), K(simple_set_path));
  } else if (OB_FAIL(backup_info_mgr.get_extern_backup_set_file_info(passwd_array, backup_set_info))) {
    LOG_WARN("failed to find backup info", K(ret), K(passwd_array));
  } else if (OB_FAIL(tenant_locality_info_mgr.init(simple_set_path, fake_backup_lease))) {
    LOG_WARN("failed to init tenant locality info mgr", K(ret), K(simple_set_path));
  } else if (OB_FAIL(tenant_locality_info_mgr.get_extern_tenant_locality_info(tenant_locality_info))) {
    LOG_WARN("failed to find tenant locality info", K(ret));
  } else if (backup_set_info.cluster_version_ > cluster_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot restore from newer cluster to older cluster", K(ret), K(cluster_version), K(backup_set_info));
  } else if (OB_FAIL(pg_list_mgr.init(simple_set_path, fake_backup_lease))) {
    LOG_WARN("failed to get sys pg list", K(ret), K(backup_set_info), K(simple_set_path));
  } else if (OB_FAIL(pg_list_mgr.get_sys_pg_list(sys_pg_key_list))) {
    LOG_WARN("failed to get sys pg list", K(ret), K(backup_set_info));
  } else if (sys_pg_key_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys pg key list should not be empty", K(ret), K(backup_set_info));
  }
  return ret;
}

int ObRestoreBackupInfoUtil::inner_get_restore_backup_piece_info_(
    const GetRestoreBackupInfoParam &param, ObBackupPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  ObFakeBackupLeaseService fake_backup_lease;
  ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;
  ObSimpleBackupPiecePath simple_piece_path;  // smallest backup piece path
  ObBackupPath piece_backup_path;
  if (OB_FAIL(param.get_smallest_backup_piece_path(simple_piece_path))) {
    LOG_WARN("failed to get smallest simple backup piece path", K(ret));
  } else if (OB_FAIL(piece_backup_path.init(simple_piece_path.get_simple_path()))) {
    LOG_WARN("failed to init piece backup path", K(ret));
  } else if (OB_FAIL(piece_backup_path.join(OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO))) {
    LOG_WARN("failed to join single backup piece info");
  } else if (OB_FAIL(log_archive_backup_info_mgr.read_external_single_backup_piece_info(
                 piece_backup_path, simple_piece_path.get_storage_info(), piece_info, fake_backup_lease))) {
    LOG_WARN("failed to read external single backup piece info", K(ret), K(piece_backup_path), K(simple_piece_path));
  }
  return ret;
}

int ObRestoreBackupInfoUtil::get_restore_sys_table_ids(
    const ObPhysicalRestoreInfo &info, common::ObIArray<common::ObPartitionKey> &pkey_list)
{
  UNUSEDx(info, pkey_list);
  return OB_SUCCESS;
}

int ObRestoreBackupInfoUtil::check_is_snapshot_restore(const int64_t backup_snapshot, const int64_t restore_timestamp,
    const uint64_t cluster_version, bool &is_snapshot_restore)
{
  int ret = OB_SUCCESS;
  is_snapshot_restore = false;

  if (backup_snapshot <= 0 || restore_timestamp <= 0 || 0 == cluster_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check is restore snapshot restore get invalid argument",
        K(ret),
        K(backup_snapshot),
        K(restore_timestamp),
        K(cluster_version));
  } else {
    is_snapshot_restore = backup_snapshot == restore_timestamp && cluster_version > CLUSTER_VERSION_3000;
  }
  LOG_INFO("check is snapshot restore",
      K(is_snapshot_restore),
      K(backup_snapshot),
      K(restore_timestamp),
      K(cluster_version));
  return ret;
}

ObRestoreFatalErrorReporter &ObRestoreFatalErrorReporter::get_instance()
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

int ObRestoreFatalErrorReporter::init(obrpc::ObCommonRpcProxy &rpc_proxy, share::ObRsMgr &rs_mgr)
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

int ObRestoreFatalErrorReporter::add_restore_error_task(const uint64_t tenant_id, const PhysicalRestoreMod &mod,
    const int32_t result, const int64_t job_id, const common::ObAddr &addr)
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
      obrpc::ObPhysicalRestoreResult &result = report_results.at(i);
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

int ObRestoreFatalErrorReporter::remove_restore_error_task_(const obrpc::ObPhysicalRestoreResult &result)
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

int ObRestoreFatalErrorReporter::report_restore_error_(const obrpc::ObPhysicalRestoreResult &result)
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

ObBackupDestDetector &ObBackupDestDetector::get_instance()
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

int ObBackupDestDetector::get_is_backup_dest_bad(const int64_t round_id, bool &is_bad)
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

int ObBackupDestDetector::update_backup_info(const ObLogArchiveBackupInfo &info)
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
      LOG_ERROR("backup dest is bad, skip check backup dest. Please check if nfs is mounted and granted W/R permission",
          K(info));
    } else if (OB_FAIL(check_backup_dest_(info, is_bad))) {
      LOG_WARN("failed to check backup dest", K(ret), K(info));
    } else if (is_bad) {
      SpinWLockGuard guard(lock_);
      if (0 == STRNCMP(info.backup_dest_, info_.backup_dest_, sizeof(info_.backup_dest_)) &&
          info.status_.round_ == info_.status_.round_) {
        is_bad_ = true;
        LOG_ERROR("[BACKUP_MOUNT_FILE]backup mount file not found, mark backup dest bad", K(info));
      } else {
        FLOG_WARN("[BACKUP_MOUNT_FILE]backup info is changed, cannot set bad", K(info), K(info_));
      }
    }
  }

  return ret;
}

int ObBackupDestDetector::check_backup_dest_(ObLogArchiveBackupInfo &backup_info, bool &is_bad)
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
