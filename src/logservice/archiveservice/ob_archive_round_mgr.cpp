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

#include "ob_archive_round_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"   // SERVER_EVENT
#include "share/backup/ob_backup_connectivity.h"

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::common;
using namespace oceanbase::share;

ObArchiveRoundMgr::ObArchiveRoundMgr() :
  key_(),
  round_start_scn_(),
  compatible_(false),
  log_archive_state_(),
  backup_dest_(),
  backup_dest_id_(0),
  rwlock_(common::ObLatchIds::ARCHIVE_ROUND_MGR_LOCK)
{
}

ObArchiveRoundMgr::~ObArchiveRoundMgr()
{
  destroy();
}

int ObArchiveRoundMgr::init()
{
  ARCHIVE_LOG(INFO, "ObArchiveRoundMgr init succ");
  return OB_SUCCESS;
}

void ObArchiveRoundMgr::destroy()
{
  WLockGuard guard(rwlock_);

  key_.reset();
  round_start_scn_.reset();
  compatible_ = false;
  log_archive_state_.status_ = ObArchiveRoundState::Status::INVALID;
  backup_dest_.reset();
  backup_dest_id_ = 0;
}


int ObArchiveRoundMgr::set_archive_start(const ArchiveKey &key,
    const share::SCN &round_start_scn,
    const int64_t piece_switch_interval,
    const SCN &genesis_scn,
    const int64_t base_piece_id,
    const share::ObTenantLogArchiveStatus::COMPATIBLE compatible,
    const share::ObBackupDest &dest,
    const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!key.is_valid()
        || ! round_start_scn.is_valid()
        || piece_switch_interval <= 0
        || base_piece_id < 1
        || !genesis_scn.is_valid()
        || !dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", K(ret), K(key), K(piece_switch_interval),
        K(genesis_scn), K(base_piece_id), K(compatible), K(dest));
  } else if (share::ObTenantLogArchiveStatus::COMPATIBLE::NONE != compatible
      && share::ObTenantLogArchiveStatus::COMPATIBLE::COMPATIBLE_VERSION_1 != compatible
      && share::ObTenantLogArchiveStatus::COMPATIBLE::COMPATIBLE_VERSION_2 != compatible) {
    ret = OB_NOT_SUPPORTED;
    ARCHIVE_LOG(ERROR, "compatible not support", K(ret), K(key), K(compatible));
  } else if (OB_FAIL(backup_dest_.deep_copy(dest))) {
    ARCHIVE_LOG(WARN, "backup dest set failed", K(ret), K(dest));
  } else {
    key_ = key;
    round_start_scn_ = round_start_scn;
    piece_switch_interval_ = piece_switch_interval;
    genesis_scn_ = genesis_scn;
    base_piece_id_ = base_piece_id;
    compatible_ = compatible >= share::ObTenantLogArchiveStatus::COMPATIBLE::COMPATIBLE_VERSION_2;
    log_archive_state_.status_ = ObArchiveRoundState::Status::DOING;
    backup_dest_id_ = dest_id;
    ARCHIVE_LOG(INFO, "set_archive_start succ", KPC(this));
  }

  return ret;
}

void ObArchiveRoundMgr::set_archive_force_stop(const ArchiveKey &key)
{
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(! key.is_valid())) {
    ARCHIVE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid arguments", K(key));
  } else {
    key_ = key;
    log_archive_state_.status_ = ObArchiveRoundState::Status::STOP;
  }
}

void ObArchiveRoundMgr::set_archive_interrupt(const ArchiveKey &key)
{
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(! key.is_valid())) {
    ARCHIVE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid arguments", K(key));
  } else {
    key_ = key;
    log_archive_state_.status_ = ObArchiveRoundState::Status::INTERRUPTED;
  }
}

void ObArchiveRoundMgr::set_archive_suspend(const ArchiveKey &key)
{
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(! key.is_valid())) {
    ARCHIVE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid arguments", K(key));
  } else {
    key_ = key;
    log_archive_state_.status_ = ObArchiveRoundState::Status::SUSPEND;
  }
}

int ObArchiveRoundMgr::get_backup_path_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (!backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "backup_dest is invalid", KR(ret));
  } else if (OB_FAIL(backup_dest_.get_backup_path_str(buf, buf_size))) {
    ARCHIVE_LOG(WARN, "get backup path str failed", KR(ret));
  }
  return ret;
}

int ObArchiveRoundMgr::reset_backup_dest(const ArchiveKey &key) 
{
  int ret = OB_SUCCESS;

  ObBackupPathString backup_path;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const uint64_t tenant_id = gen_user_tenant_id(MTL_ID());
  share::ObBackupDest backup_dest;
  bool is_equal = false;
  if (key != key_) {
    ret = OB_EAGAIN;
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "sql_proxy is null", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(get_backup_path_str_(backup_path.ptr(), backup_path.capacity()))) {
    ARCHIVE_LOG(WARN, "fail to get backup path str", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, backup_path, backup_dest))) {
    ARCHIVE_LOG(WARN, "fail to get backup dest", K(ret), K(tenant_id), K(backup_path));
  } else {
    WLockGuard guard(rwlock_);
    if (!backup_dest_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "backup_dest is invalid", KR(ret));
    } else if (OB_FAIL(backup_dest_.is_backup_path_equal(backup_dest, is_equal))) {
      ARCHIVE_LOG(WARN, "fail to compare backup path", K(ret), K(backup_dest), K_(backup_dest), K(is_equal));
    } else if(is_equal) {
      if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
        ARCHIVE_LOG(WARN, "fail to deep copy dest", K(ret), K(backup_dest));
      } else {
        ARCHIVE_LOG(INFO, "reset_backup_dest succ", K(ret), K_(backup_dest));
      }
    } else {
      ARCHIVE_LOG(INFO, "backup dest has changed, do not need update", K(ret));
    }
  }

  return ret;
}

// Note: The member variable backup_dest_ in ObArchiveRoundMgr may not be up-to-date.
// If you encounter the error ret == OB_OBJECT_STORAGE_PERMISSION_DENIED when accessing the archive path 
//with the backup_dest_ obtained from the ObArchiveRoundMgr::get_backup_dest_and_id function,
// you need to call ObArchiveRoundMgr::reset_backup_dest to refresh backup_dest_.
// For details, refer to the use of ObArchiveRoundMgr::reset_backup_dest in #define ADD_LS_RECORD_TASK(CLASS, type).
int ObArchiveRoundMgr::get_backup_dest_and_id(const ArchiveKey &key,
    share::ObBackupDest &dest,
    int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (key != key_) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(dest.deep_copy(backup_dest_))) {
    ARCHIVE_LOG(WARN, "backup dest deep_copy failed", K(ret), K(backup_dest_));
  } else {
    dest_id = backup_dest_id_;
  }
  return ret;
}

void ObArchiveRoundMgr::get_archive_round_info(ArchiveKey &key,
    ObArchiveRoundState &state) const
{
  RLockGuard guard(rwlock_);
  key = key_;
  state = log_archive_state_;
}

int ObArchiveRoundMgr::get_piece_info(const ArchiveKey &key,
      int64_t &piece_switch_interval,
      SCN &genesis_scn,
      int64_t &base_piece_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (key != key_) {
    ret = OB_EAGAIN;
  } else {
    piece_switch_interval = piece_switch_interval_;
    genesis_scn = genesis_scn_;
    base_piece_id = base_piece_id_;
  }
  return ret;
}

int ObArchiveRoundMgr::get_archive_start_scn(const ArchiveKey &key, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (key != key_) {
    ret = OB_EAGAIN;
  } else {
    scn = round_start_scn_;
  }
  return ret;
}

void ObArchiveRoundMgr::get_archive_round_compatible(ArchiveKey &key,
    bool &compatible)
{
  RLockGuard guard(rwlock_);
  key = key_;
  compatible = compatible_;
}

bool ObArchiveRoundMgr::is_in_archive_status(const ArchiveKey &key) const
{
  RLockGuard guard(rwlock_);
  return key == key_ && log_archive_state_.is_doing();
}

bool ObArchiveRoundMgr::is_in_suspend_status(const ArchiveKey &key) const
{
  RLockGuard guard(rwlock_);
  return key == key_ && log_archive_state_.is_suspend();
}

bool ObArchiveRoundMgr::is_in_archive_stopping_status(const ArchiveKey &key) const
{
  RLockGuard guard(rwlock_);
  return key == key_ && log_archive_state_.is_stopping();
}

bool ObArchiveRoundMgr::is_in_archive_stop_status(const ArchiveKey &key) const
{
  RLockGuard guard(rwlock_);
  return key == key_ && log_archive_state_.is_stop();
}

void ObArchiveRoundMgr::update_log_archive_status(const ObArchiveRoundState::Status status)
{
  WLockGuard guard(rwlock_);
  log_archive_state_.status_ = status;
}

int ObArchiveRoundMgr::mark_fatal_error(const ObLSID &id,
    const ArchiveKey &key,
    const ObArchiveInterruptReason &reason)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (key != key_) {
    ARCHIVE_LOG(WARN, "encount error with different archive key, just skip", K(key), KPC(this));
  } else {
    ARCHIVE_LOG(ERROR, "archive mark_fatal_error", K(key), K(id), K(reason));
    if (!log_archive_state_.is_interrupted()) {
      log_archive_state_.set_interrupted();
      SERVER_EVENT_ADD("log_archive", "mark_fatal_error",
                       "id", id.id(),
                       "reason", reason.get_str(),
                       "ret_code", reason.get_code(),
                       "lbt", reason.get_lbt());
    }
  }
  return ret;
}

} // namespace archive
} // namespace oceanbase
