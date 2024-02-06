//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/ls/ob_ls_reserved_snapshot_mgr.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_base_header.h"

namespace oceanbase
{
using namespace logservice;
namespace storage
{

ObLSReservedSnapshotMgr::ObLSReservedSnapshotMgr()
 : ObIStorageClogRecorder(),
   is_inited_(false),
   allocator_("ResvSnapMgr"),
   min_reserved_snapshot_(0),
   next_reserved_snapshot_(0),
   snapshot_lock_(),
   sync_clog_lock_(),
   ls_(nullptr),
   ls_handle_(),
   dependent_tablet_set_(),
   clog_cb_(*this),
   last_print_log_ts_(ObTimeUtility::fast_current_time()),
   clog_buf_()
{
}

ObLSReservedSnapshotMgr::~ObLSReservedSnapshotMgr()
{
  destroy();
}

int ObLSReservedSnapshotMgr::init(const int64_t tenant_id, ObLS *ls, ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "DepTabletSet");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSReservedSnapshotMgr is inited", K(ret), KP(ls));
  } else if (OB_UNLIKELY(nullptr == ls || nullptr == log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls), K(log_handler));
  } else if (OB_FAIL(ObIStorageClogRecorder::init(0/*max_saved_version*/, log_handler))) {
    LOG_WARN("failed to init", K(ret), KP(ls), K(log_handler));
  } else if (OB_FAIL(dependent_tablet_set_.create(HASH_BUCKET, attr, attr))) {
    LOG_WARN("failed to create hash set", K(ret), K(ls));
  } else {
    ls_ = ls;
    is_inited_ = true;
    LOG_INFO("success to init snapshot mgr", K(ret), KP(ls), "ls_id", ls_->get_ls_id(), KP(this));
  }
  return ret;
}

void ObLSReservedSnapshotMgr::destroy()
{
  is_inited_ = false;
  ObIStorageClogRecorder::destroy();
  clog_cb_.reset();
  min_reserved_snapshot_ = 0;
  next_reserved_snapshot_ = 0;
  ls_ = nullptr;
  ls_handle_.reset();
  last_print_log_ts_ = 0;
  if (dependent_tablet_set_.created()) {
    dependent_tablet_set_.destroy();
  }
}

int ObLSReservedSnapshotMgr::add_dependent_medium_tablet(const ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSReservedSnapshotMgr is not inited", K(ret), K(tablet_id));
  } else {
    common::TCWLockGuard lock_guard(snapshot_lock_);
    if (OB_HASH_EXIST == (hash_ret = dependent_tablet_set_.exist_refactored(tablet_id.id()))) {
      ret = OB_ENTRY_EXIST; // tablet exist
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check exist in tablet set", K(ret), K(hash_ret),
          "ls_id", ls_->get_ls_id(), K(tablet_id));
    } else if (OB_FAIL(dependent_tablet_set_.set_refactored(tablet_id.id()))) {
      LOG_WARN("failed to set tablet_id", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id));
    }
  }
  return ret;
}

int ObLSReservedSnapshotMgr::del_dependent_medium_tablet(const ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t new_snapshot_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSReservedSnapshotMgr is not inited", K(ret), K(tablet_id));
  } else {
    common::TCWLockGuard lock_guard(snapshot_lock_);
    if (OB_FAIL(dependent_tablet_set_.erase_refactored(tablet_id.id()))) {
      LOG_WARN("failed to erase tablet id", K(ret), "ls_id", ls_->get_ls_id(),
          K(tablet_id), K(dependent_tablet_set_.size()), KP(this));
    } else if (0 == dependent_tablet_set_.size() && next_reserved_snapshot_ > 0) {
      min_reserved_snapshot_ = next_reserved_snapshot_;
      new_snapshot_version = next_reserved_snapshot_;
      next_reserved_snapshot_ = 0;
    }
  } // end of lock

  if (OB_SUCC(ret) && new_snapshot_version > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ls_->try_sync_reserved_snapshot(new_snapshot_version, false/*update_flag*/))) {
      LOG_WARN("failed to send update reserved snapshot log", K(tmp_ret), K(new_snapshot_version));
    }
  }
  return ret;
}

int64_t ObLSReservedSnapshotMgr::get_min_reserved_snapshot()
{
  common::TCRLockGuard lock_guard(snapshot_lock_);
  return min_reserved_snapshot_;
}

const int64_t ObLSReservedSnapshotMgr::CLOG_BUF_LEN;
int ObLSReservedSnapshotMgr::submit_log(
    const int64_t reserved_snapshot,
    const char *clog_buf,
    const int64_t clog_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == clog_buf || clog_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clog_buf or clog_len is invalid", K(ret), KP(clog_buf), K(clog_len));
  } else if (OB_FAIL(write_clog(clog_buf, clog_len))) {
    LOG_WARN("fail to submit log", K(ret), "ls_id", ls_->get_ls_id());
  } else {
    LOG_DEBUG("submit reserved snapshot log success", "ls_id", ls_->get_ls_id(), K(reserved_snapshot));
  }

  return ret;
}

int ObLSReservedSnapshotMgr::update_min_reserved_snapshot_for_leader(const int64_t new_snapshot_version)
{
  int ret = OB_SUCCESS;
  bool send_log_flag = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSReservedSnapshotMgr is not inited", K(ret), KP(ls_));
  } else {
    common::TCWLockGuard lock_guard(snapshot_lock_);
    if (0 == dependent_tablet_set_.size()) {
      if (new_snapshot_version < min_reserved_snapshot_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to update min reserved snapshot", K(ret), "ls_id", ls_->get_ls_id(),
          K(new_snapshot_version), K(min_reserved_snapshot_));
      } else if (new_snapshot_version > min_reserved_snapshot_) {
        // update min_reserved_snapshot and send clog
        min_reserved_snapshot_ = new_snapshot_version;
        next_reserved_snapshot_ = 0;
        send_log_flag = true;
      }
    } else if (new_snapshot_version > next_reserved_snapshot_) {
      // wait for next call
      next_reserved_snapshot_ = new_snapshot_version;
    }
  } // end of lock

  if (OB_SUCC(ret) && send_log_flag) {
    if (OB_FAIL(sync_clog(new_snapshot_version))) {
      LOG_WARN("failed to send update reserved snapshot log", K(ret), K(new_snapshot_version));
    } else if (need_print_log()) {
      LOG_INFO("submit reserved snapshot log success", "ls_id", ls_->get_ls_id(),
          K(new_snapshot_version));
    }
  }
  return ret;
}

int ObLSReservedSnapshotMgr::try_sync_reserved_snapshot(
    const int64_t new_reserved_snapshot,
    const bool update_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSReservedSnapshotMgr not inited", K(ret), KP(ls_));
  } else if (OB_UNLIKELY(new_reserved_snapshot < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_reserved_snapshot));
  } else if (update_flag) {
    if (OB_FAIL(update_min_reserved_snapshot_for_leader(new_reserved_snapshot))) {
      LOG_WARN("failed to update min_reserved_snapshot", K(ret), "ls_id", ls_->get_ls_id(), K(new_reserved_snapshot));
    }
  } else if (OB_FAIL(sync_clog(new_reserved_snapshot))) {
    LOG_WARN("failed to send update reserved snapshot log", K(ret), K(new_reserved_snapshot));
  } else if (need_print_log()) {
    LOG_INFO("submit reserved snapshot log success", "ls_id", ls_->get_ls_id(),
        K(new_reserved_snapshot));
  }
  return ret;
}

int ObLSReservedSnapshotMgr::sync_clog(const int64_t new_reserved_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing, should sync clog
  } else {
    ObMutexGuard guard(sync_clog_lock_);
    if (OB_FAIL(try_update_for_leader(new_reserved_snapshot, nullptr/*allocator*/))) {
      LOG_WARN("failed to send update reserved snapshot log", K(ret), "ls_id", ls_->get_ls_id(), K(new_reserved_snapshot));
    }
  }
  return ret;
}

int ObLSReservedSnapshotMgr::replay_reserved_snapshot_log(
    const share::SCN &scn, const char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t reserved_snapshot = OB_INVALID_VERSION;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSReservedSnapshotMgr not inited", K(ret), KP(ls_));
  } else if (OB_FAIL(serialization::decode_i64(buf, size, pos, &reserved_snapshot))) {
    LOG_WARN("fail to deserialize reserved_snapshot", K(ret), "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(ObIStorageClogRecorder::replay_clog(reserved_snapshot, scn, buf, size, pos))) {
    LOG_WARN("failed to update reserved snapshot by log", K(ret), "ls_id", ls_->get_ls_id(),
        K(min_reserved_snapshot_), K(reserved_snapshot));
  }
  return ret;
}

// replay after get update_version
int ObLSReservedSnapshotMgr::inner_replay_clog(
    const int64_t update_version,
    const share::SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  UNUSEDx(scn, buf, size, pos);
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_update_reserved_snapshot(update_version))) {
    LOG_WARN("failed to update reserved_snapshot", K(ret), K(scn), K(update_version));
  }
  return ret;
}

int ObLSReservedSnapshotMgr::sync_clog_succ_for_leader(const int64_t update_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_update_reserved_snapshot(update_version))) {
    LOG_WARN("failed to update reserved_snapshot", K(ret), K(update_version));
  }
  return ret;
}

int ObLSReservedSnapshotMgr::inner_update_reserved_snapshot(const int64_t reserved_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(reserved_snapshot < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(reserved_snapshot));
  } else {
    common::TCWLockGuard lock_guard(snapshot_lock_);
    if (reserved_snapshot > min_reserved_snapshot_) {
      min_reserved_snapshot_ = reserved_snapshot;
      LOG_INFO("success to update reserved snapshot", K(ret), "ls_id", ls_->get_ls_id(),
          K(min_reserved_snapshot_));
    }
  }
  return ret;
}

int ObLSReservedSnapshotMgr::prepare_struct_in_lock(
    int64_t &update_version,
    ObIAllocator *allocator,
    char *&clog_buf,
    int64_t &clog_len)
{
  UNUSED(allocator);
  clog_buf = nullptr;
  clog_len = 0;
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  const ObLogBaseHeader log_header(
      ObLogBaseType::RESERVED_SNAPSHOT_LOG_BASE_TYPE,
      ObReplayBarrierType::PRE_BARRIER/*need_replay_pre_barrier*/);

  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_->get_ls_id(), ls_handle_, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(log_header.serialize(clog_buf_, CLOG_BUF_LEN, pos))) {
    LOG_WARN("failed to serialize log header", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(clog_buf_, CLOG_BUF_LEN, pos, update_version))) {
    LOG_WARN("generate reserved snapshot log", K(ret), "ls_id", ls_->get_ls_id(), K(pos), K(CLOG_BUF_LEN));
  } else {
    logcb_ptr_ = &clog_cb_;
    clog_buf = clog_buf_;
    clog_len = pos;
  }
  return ret;
}

void ObLSReservedSnapshotMgr::free_struct_in_lock()
{
  ls_handle_.reset();
  clog_cb_.reset();
}

} // namespace storage
} // namespace oceanbase
