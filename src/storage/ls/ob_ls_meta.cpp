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

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_meta.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_log_replayer.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace logservice;
using namespace share;
using namespace transaction;
namespace storage
{

typedef common::ObFunction<int(ObLSMeta &)> WriteSlog;
WriteSlog ObLSMeta::write_slog_ = [](ObLSMeta &ls_meta) {
  int ret = OB_SUCCESS;
  ObLSMetaLog slog_entry(ls_meta);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_UPDATE_LS);
  ObStorageLogger *slogger = nullptr;
  if (OB_ISNULL(slogger = MTL(ObStorageLogger *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_log_service failed", K(ret));
  } else if (OB_FAIL(slogger->write_log(log_param))) {
    LOG_WARN("fail to write ls meta slog", K(log_param), K(ret));
  }
  return ret;
};

ObLSMeta::ObLSMeta()
  : lock_(common::ObLatchIds::LS_META_LOCK),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    unused_replica_type_(REPLICA_TYPE_FULL),
    ls_create_status_(ObInnerLSStatus::CREATING),
    clog_checkpoint_scn_(ObScnRange::MIN_SCN),
    clog_base_lsn_(PALF_INITIAL_LSN_VAL),
    rebuild_seq_(-1),
    migration_status_(ObMigrationStatus::OB_MIGRATION_STATUS_MAX),
    gc_state_(LSGCState::INVALID_LS_GC_STATE),
    offline_scn_(),
    restore_status_(ObLSRestoreStatus::LS_RESTORE_STATUS_MAX),
    replayable_point_(),
    tablet_change_checkpoint_scn_(SCN::min_scn()),
    all_id_meta_(),
    saved_info_(),
    transfer_scn_(SCN::min_scn()),
    rebuild_info_()
{
}

ObLSMeta::ObLSMeta(const ObLSMeta &ls_meta)
  : lock_(),
    tenant_id_(ls_meta.tenant_id_),
    ls_id_(ls_meta.ls_id_),
    unused_replica_type_(ls_meta.unused_replica_type_),
    ls_create_status_(ls_meta.ls_create_status_),
    clog_checkpoint_scn_(ls_meta.clog_checkpoint_scn_),
    clog_base_lsn_(ls_meta.clog_base_lsn_),
    rebuild_seq_(ls_meta.rebuild_seq_),
    migration_status_(ls_meta.migration_status_),
    gc_state_(ls_meta.gc_state_),
    offline_scn_(ls_meta.offline_scn_),
    restore_status_(ls_meta.restore_status_),
    replayable_point_(ls_meta.replayable_point_),
    tablet_change_checkpoint_scn_(ls_meta.tablet_change_checkpoint_scn_),
    saved_info_(ls_meta.saved_info_),
    transfer_scn_(ls_meta.transfer_scn_),
    rebuild_info_(ls_meta.rebuild_info_)
{
  all_id_meta_.update_all_id_meta(ls_meta.all_id_meta_);
}

void ObLSMeta::set_ls_create_status(const ObInnerLSStatus &status)
{
  ObSpinLockTimeGuard guard(lock_);
  ls_create_status_ = status;
}

ObInnerLSStatus ObLSMeta::get_ls_create_status() const
{
  return ls_create_status_;
}

ObLSMeta &ObLSMeta::operator=(const ObLSMeta &other)
{
  ObSpinLockTimeGuard guard(other.lock_);
  ObSpinLockTimeGuard guard_myself(lock_);
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    unused_replica_type_ = other.unused_replica_type_;
    ls_create_status_ = other.ls_create_status_;
    rebuild_seq_ = other.rebuild_seq_;
    migration_status_ = other.migration_status_;
    clog_base_lsn_ = other.clog_base_lsn_;
    clog_checkpoint_scn_ = other.clog_checkpoint_scn_;
    gc_state_ = other.gc_state_;
    offline_scn_ = other.offline_scn_;
    restore_status_ = other.restore_status_;
    replayable_point_ = other.replayable_point_;
    tablet_change_checkpoint_scn_ = other.tablet_change_checkpoint_scn_;
    all_id_meta_.update_all_id_meta(other.all_id_meta_);
    saved_info_ = other.saved_info_;
    transfer_scn_ = other.transfer_scn_;
    rebuild_info_ = other.rebuild_info_;
  }
  return *this;
}

void ObLSMeta::reset()
{
  ObSpinLockTimeGuard guard(lock_);
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  unused_replica_type_ = REPLICA_TYPE_FULL;
  clog_base_lsn_.reset();
  clog_checkpoint_scn_ = ObScnRange::MIN_SCN;
  rebuild_seq_ = -1;
  migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  gc_state_ = LSGCState::INVALID_LS_GC_STATE;
  offline_scn_.reset();
  restore_status_ = ObLSRestoreStatus::LS_RESTORE_STATUS_MAX;
  replayable_point_.reset();
  tablet_change_checkpoint_scn_ = SCN::min_scn();
  saved_info_.reset();
  transfer_scn_ = SCN::min_scn();
  rebuild_info_.reset();
}

LSN &ObLSMeta::get_clog_base_lsn()
{
  ObSpinLockTimeGuard guard(lock_);
  return clog_base_lsn_;
}

SCN ObLSMeta::get_clog_checkpoint_scn() const
{
  ObSpinLockTimeGuard guard(lock_);
	return clog_checkpoint_scn_;
}

int ObLSMeta::set_clog_checkpoint(const LSN &clog_checkpoint_lsn,
                                  const SCN &clog_checkpoint_scn,
                                  const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else {
    ObLSMeta tmp(*this);
    tmp.clog_base_lsn_ = clog_checkpoint_lsn;
    tmp.clog_checkpoint_scn_ = clog_checkpoint_scn;

    if (write_slog) {
      if (OB_FAIL(write_slog_(tmp))) {
        LOG_WARN("clog_checkpoint write slog failed", K(ret));
      }
    }

    clog_base_lsn_ = clog_checkpoint_lsn;
    clog_checkpoint_scn_ = clog_checkpoint_scn;
  }

  return ret;
}

SCN ObLSMeta::get_tablet_change_checkpoint_scn() const
{
	return tablet_change_checkpoint_scn_;
}

int ObLSMeta::set_tablet_change_checkpoint_scn(const SCN &tablet_change_checkpoint_scn)
{
  ObSpinLockTimeGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (tablet_change_checkpoint_scn_ > tablet_change_checkpoint_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_change_checkpoint_ts is small", KR(ret), K(tablet_change_checkpoint_scn),
             K_(tablet_change_checkpoint_scn));
  } else {
    ObLSMeta tmp(*this);
    tmp.tablet_change_checkpoint_scn_ = tablet_change_checkpoint_scn;

    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret));
    } else {
      LOG_INFO("update tablet change checkpoint scn", K(tenant_id_), K(ls_id_),
          "old_scn", tablet_change_checkpoint_scn_, "new_scn", tablet_change_checkpoint_scn);
      tablet_change_checkpoint_scn_ = tablet_change_checkpoint_scn;
    }
  }

  return ret;
}

share::SCN ObLSMeta::get_transfer_scn() const
{
  ObSpinLockTimeGuard guard(lock_);
  return transfer_scn_;
}

int ObLSMeta::inc_update_transfer_scn(const share::SCN &transfer_scn)
{
  ObSpinLockTimeGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (transfer_scn_ > transfer_scn) {
    LOG_INFO("transfer scn is small",  K_(tenant_id), K_(ls_id), K(transfer_scn), K_(transfer_scn));
  } else {
    ObLSMeta tmp(*this);
    tmp.transfer_scn_ = transfer_scn;

    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret), K(*this));
    } else {
      transfer_scn_ = transfer_scn;
    }
  }
  return ret;
}

bool ObLSMeta::is_valid() const
{
  return is_valid_id(tenant_id_)
      && ls_id_.is_valid()
      && OB_MIGRATION_STATUS_MAX != migration_status_
      && ObGCHandler::is_valid_ls_gc_state(gc_state_)
      && restore_status_.is_valid()
      && rebuild_seq_ >= 0;
}

int64_t ObLSMeta::get_rebuild_seq() const
{
  ObSpinLockTimeGuard guard(lock_);
  return rebuild_seq_;
}

int ObLSMeta::set_migration_status(const ObMigrationStatus &migration_status,
                                   const bool write_slog)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!ObMigrationStatusHelper::is_valid(migration_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(migration_status), KPC(this));
  } else if (migration_status_ == migration_status) {
    //do nothing
  } else if (OB_FAIL(ObMigrationStatusHelper::check_can_change_status(migration_status_,
                                                                      migration_status,
                                                                      can_change))) {
    LOG_WARN("failed to check can change stauts", K(ret), K(migration_status_),
             K(migration_status));
  } else if (!can_change) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ls can not change to migrate status", K(ret), K(migration_status_),
             K(migration_status));
  } else {
    ObLSMeta tmp(*this);
    tmp.migration_status_ = migration_status;
    if (write_slog && OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("migration_status write slog failed", K(ret));
    } else {
      migration_status_ = migration_status;
    }
  }
  return ret;
}

int ObLSMeta::get_migration_status(ObMigrationStatus &migration_status) const
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get migration status", K(ret), K(*this));
  } else {
    migration_status = migration_status_;
  }
  return ret;
}

int ObLSMeta::set_gc_state(const logservice::LSGCState &gc_state, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!ObGCHandler::is_valid_ls_gc_state(gc_state)
             || (ObGCHandler::is_ls_offline_gc_state(gc_state) && !scn.is_valid())
             || (!ObGCHandler::is_ls_offline_gc_state(gc_state) && scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_state invalid", K(ret), K(gc_state));
  } else {
    ObLSMeta tmp(*this);
    tmp.gc_state_ = gc_state;
    tmp.offline_scn_ = scn;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("gc_state write slog failed", K(ret));
    } else {
      gc_state_ = gc_state;
      offline_scn_ = scn;
    }
  }
  return ret;
}

int ObLSMeta::get_gc_state(logservice::LSGCState &gc_state)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get_gc_state", K(ret), K(*this));
  } else {
    gc_state = gc_state_;
  }
  return ret;
}

int ObLSMeta::get_offline_scn(SCN &offline_scn)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get_offline_scn", K(ret), K(*this));
  } else {
    offline_scn = offline_scn_;
  }
  return ret;
}

int ObLSMeta::set_restore_status(const ObLSRestoreStatus &restore_status)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!restore_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore status", K(ret), K(restore_status_), K(restore_status));
  } else if (restore_status_ == restore_status) {
    //do nothing
  } else {
    ObLSMeta tmp(*this);
    tmp.restore_status_ = restore_status;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("restore_status write slog failed", K(ret));
    } else {
      restore_status_ = restore_status;
    }
  }
  return ret;
}

int ObLSMeta::get_restore_status(ObLSRestoreStatus &restore_status) const
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get restore status", K(ret), K(*this));
  } else {
    restore_status = restore_status_;
  }
  return ret;
}

int ObLSMeta::update_ls_replayable_point(const SCN &replayable_point)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!replayable_point.is_valid()
             || (replayable_point_.is_valid() && replayable_point < replayable_point_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replayable_point invalid", K(ret), K(replayable_point), K(replayable_point_));
  } else if (replayable_point_ == replayable_point) {
    // do nothing
  } else {
    ObLSMeta tmp(*this);
    tmp.replayable_point_ = replayable_point;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("replayable_point_ write slog failed", K(ret));
    } else {
      replayable_point_ = replayable_point;
    }
  }
  return ret;
}

int ObLSMeta::get_ls_replayable_point(SCN &replayable_point)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get_gc_state", K(ret), K(*this));
  } else {
    replayable_point = replayable_point_;
  }
  return ret;
}

//This interface for ha. Add parameters should check meta value need to update from src
int ObLSMeta::update_ls_meta(
    const bool update_restore_status,
    const ObLSMeta &src_ls_meta)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus ls_restore_status;

  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!src_ls_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update ls meta get invalid argument", K(ret), K(src_ls_meta));
  } else if (update_restore_status
      && OB_FAIL(src_ls_meta.get_restore_status(ls_restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(src_ls_meta));
  } else {
    ObLSMeta tmp(*this);
    tmp.clog_base_lsn_ = src_ls_meta.clog_base_lsn_;
    tmp.clog_checkpoint_scn_ = src_ls_meta.clog_checkpoint_scn_;
    tmp.replayable_point_ = src_ls_meta.replayable_point_;
    tmp.tablet_change_checkpoint_scn_ = src_ls_meta.tablet_change_checkpoint_scn_;
    tmp.transfer_scn_ = src_ls_meta.transfer_scn_;
    tmp.rebuild_seq_++;
    if (update_restore_status) {
      tmp.restore_status_ = ls_restore_status;
    }
    tmp.gc_state_ = src_ls_meta.gc_state_;
    tmp.offline_scn_ = src_ls_meta.offline_scn_;
    guard.click();
    tmp.all_id_meta_.update_all_id_meta(src_ls_meta.all_id_meta_);
    if (tmp.clog_checkpoint_scn_ < clog_checkpoint_scn_) {
  // TODO(muwei.ym): now do not allow clog checkpoint ts rollback, may support it in 4.3
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("do not allow clog checkpoint ts rollback", K(ret), K(src_ls_meta), KPC(this));
    } else if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret));
    } else {
      guard.click();
      clog_base_lsn_ = src_ls_meta.clog_base_lsn_;
      clog_checkpoint_scn_ = src_ls_meta.clog_checkpoint_scn_;
      replayable_point_ = src_ls_meta.replayable_point_;
      tablet_change_checkpoint_scn_ = src_ls_meta.tablet_change_checkpoint_scn_;
      all_id_meta_.update_all_id_meta(src_ls_meta.all_id_meta_);
      rebuild_seq_ = tmp.rebuild_seq_;
      gc_state_ = tmp.gc_state_;
      offline_scn_ = src_ls_meta.offline_scn_;
      transfer_scn_ = src_ls_meta.transfer_scn_;
      if (update_restore_status) {
        restore_status_ = ls_restore_status;
      }
    }
    LOG_INFO("update ls meta", K(ret), K(tmp), K(src_ls_meta), K(*this));
  }
  return ret;
}

int ObLSMeta::set_ls_rebuild()
{
  int ret = OB_SUCCESS;
  const ObMigrationStatus change_status = ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD;
  bool can_change = false;

  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (change_status == migration_status_) {
    //do nothing
  } else {
    ObLSMeta tmp(*this);
    if (OB_FAIL(ObMigrationStatusHelper::check_can_change_status(tmp.migration_status_, change_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(migration_status_), K(change_status));
    } else if (!can_change) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ls can not change to rebuild status", K(ret), K(tmp), K(change_status));
    } else {
      tmp.migration_status_ = change_status;
      tmp.rebuild_seq_++;
      if (OB_FAIL(write_slog_(tmp))) {
        LOG_WARN("clog_checkpoint write slog failed", K(ret));
      } else {
        migration_status_ = change_status;
        rebuild_seq_ = tmp.rebuild_seq_;
      }
    }
  }
  return ret;
}

int ObLSMeta::check_valid_for_backup() const
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid", K(ret), KPC(this));
  } else if (!restore_status_.is_restore_none()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore status is not none", K(ret), KPC(this));
  } else if (OB_MIGRATION_STATUS_NONE != migration_status_) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("ls replica not valid for backup", K(ret), KPC(this));
  }
  return ret;
}

int ObLSMeta::get_saved_info(ObLSSavedInfo &saved_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream meta is not valid, cannot get_offline_ts_ns", K(ret), K(*this));
  } else {
    saved_info = saved_info_;
  }
  return ret;
}

int ObLSMeta::set_saved_info(const ObLSSavedInfo &saved_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!saved_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set saved info get invalid argument", K(ret), K(saved_info));
  } else {
    ObLSMeta tmp(*this);
    tmp.saved_info_ = saved_info;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret));
    } else {
      saved_info_ = saved_info;
    }
  }
  return ret;
}

int ObLSMeta::build_saved_info()
{
  int ret = OB_SUCCESS;
  ObLSSavedInfo saved_info;

  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!saved_info_.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("saved info is not empty, can not build saved info", K(ret), K(*this));
  } else {
    saved_info.clog_checkpoint_scn_ = clog_checkpoint_scn_;
    saved_info.clog_base_lsn_ = clog_base_lsn_;
    saved_info.tablet_change_checkpoint_scn_ = tablet_change_checkpoint_scn_;
    ObLSMeta tmp(*this);
    tmp.saved_info_ = saved_info;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret));
    } else {
      saved_info_ = saved_info;
    }
  }
  return ret;
}

int ObLSMeta::clear_saved_info()
{
  int ret = OB_SUCCESS;
  ObLSSavedInfo saved_info;

  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else {
    saved_info.reset();
    ObLSMeta tmp(*this);
    tmp.saved_info_ = saved_info;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("clog_checkpoint write slog failed", K(ret));
    } else {
      saved_info_ = saved_info;
    }
  }
  return ret;
}

int ObLSMeta::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObMigrationStatus &migration_status,
    const share::ObLSRestoreStatus &restore_status,
    const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
      || !ObMigrationStatusHelper::is_valid(migration_status)
      || !restore_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls meta get invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(migration_status), K(restore_status));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    ls_create_status_ = ObInnerLSStatus::CREATING;
    clog_checkpoint_scn_ = create_scn;
    clog_base_lsn_.val_ = PALF_INITIAL_LSN_VAL;
    rebuild_seq_ = 0;
    migration_status_ = migration_status;
    gc_state_ = LSGCState::NORMAL;
    restore_status_ = restore_status;
    transfer_scn_ = SCN::min_scn();
  }
  return ret;
}

void ObLSMeta::set_write_slog_func_(WriteSlog write_slog)
{
  write_slog_ = write_slog;
}

int ObLSMeta::update_id_meta(const int64_t service_type,
                             const int64_t limited_id,
                             const SCN &latest_scn,
                             const bool write_slog)
{
  int ret = OB_SUCCESS;

  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else {
    // TODO: write slog may failed, but the content is updated.
    all_id_meta_.update_id_meta(service_type, limited_id, latest_scn);
    guard.click();
    if (write_slog) {
      if (OB_FAIL(write_slog_(*this))) {
        LOG_WARN("id service flush write slog failed", K(ret));
      }
    }
  }
  LOG_INFO("update id meta", K(ret), K(service_type), K(limited_id), K(latest_scn),
           K(*this));

  return ret;
}

int ObLSMeta::get_all_id_meta(ObAllIDMeta &all_id_meta) const
{
  int ret = OB_SUCCESS;

  ObSpinLockTimeGuard guard(lock_);
  all_id_meta.update_all_id_meta(all_id_meta_);
  return ret;
}

int ObLSMeta::check_can_update_()
{
  int ret = OB_SUCCESS;
  if (!can_update_ls_meta(ls_create_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("state not match, cannot update ls meta", K(ret), KPC(this));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta is not valid, cannot update", K(ret), K(*this));
  } else {
  }
  return ret;
}

int ObLSMeta::get_migration_and_restore_status(
    ObMigrationStatus &migration_status,
    share::ObLSRestoreStatus &ls_restore_status)
{
  int ret = OB_SUCCESS;
  migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  ls_restore_status = ObLSRestoreStatus::LS_RESTORE_STATUS_MAX;

  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta is not valid, cannot get", K(ret), K(*this));
  } else {
    migration_status = migration_status_;
    ls_restore_status = restore_status_;
  }
  return ret;
}

int ObLSMeta::set_rebuild_info(const ObLSRebuildInfo &rebuild_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (OB_FAIL(check_can_update_())) {
    LOG_WARN("ls meta cannot update", K(ret), K(*this));
  } else if (!rebuild_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rebuild info", K(ret), K(rebuild_info_), K(rebuild_info));
  } else if (rebuild_info_ == rebuild_info) {
    //do nothing
  } else if (ObLSRebuildStatus::CLEANUP == rebuild_info.status_
      && ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status_
      && ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL != migration_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration status in unexpected, can not set rebuild info to cleanup", K(ret),
        K(rebuild_info), K(migration_status_), KPC(this));
  } else {
    ObLSMeta tmp(*this);
    tmp.rebuild_info_ = rebuild_info;
    if (OB_FAIL(write_slog_(tmp))) {
      LOG_WARN("rebuild_info write slog failed", K(ret));
    } else {
      rebuild_info_ = rebuild_info;
    }
  }
  return ret;
}

int ObLSMeta::get_rebuild_info(ObLSRebuildInfo &rebuild_info) const
{
  int ret = OB_SUCCESS;
  ObSpinLockTimeGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta is not valid, cannot get rebuild info", K(ret), K(*this));
  } else {
    rebuild_info = rebuild_info_;
  }
  return ret;
}


ObLSMeta::ObSpinLockTimeGuard::ObSpinLockTimeGuard(common::ObSpinLock &lock,
                                                   const int64_t warn_threshold)
  : time_guard_("ls_meta", warn_threshold),
    lock_guard_(lock)
{
  time_guard_.click("after lock");
}

// add field should also consider ObLSMeta::update_ls_meta function
OB_SERIALIZE_MEMBER(ObLSMeta,
                    tenant_id_,
                    ls_id_,
                    unused_replica_type_,
                    ls_create_status_,
                    clog_checkpoint_scn_,
                    clog_base_lsn_,
                    rebuild_seq_,
                    migration_status_,
                    gc_state_,
                    offline_scn_,
                    restore_status_,
                    replayable_point_,
                    tablet_change_checkpoint_scn_,
                    all_id_meta_,
                    saved_info_,
                    transfer_scn_,
                    rebuild_info_);

}
}
