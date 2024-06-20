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
#include "ob_storage_clog_recorder.h"

#include "lib/utility/ob_tracepoint.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace common;
using namespace clog;

namespace storage
{

int ObIStorageClogRecorder::ObStorageCLogCb::on_success()
{
  int ret = OB_SUCCESS;
  int64_t update_version = ATOMIC_LOAD(&update_version_);
  bool finish_flag = false;

  LOG_DEBUG("clog succ callback", KPC(this));

  if (OB_UNLIKELY(OB_INVALID_VERSION == update_version)) {
    LOG_ERROR("table version is invalid", K(update_version));
  } else {
    // clear table_version whether success or failed, make sure next time can update
    ATOMIC_SET(&update_version_, OB_INVALID_VERSION);
    WEAK_BARRIER();
    recorder_.clog_update_succ(update_version, finish_flag);
    if (!finish_flag) {
      LOG_ERROR("update failed", K(update_version_), K(finish_flag));
      recorder_.clog_update_fail();
    }
  }
  return ret;
}

int ObIStorageClogRecorder::ObStorageCLogCb::on_failure()
{
  int ret = OB_SUCCESS;
  LOG_INFO("clog failure callback", KPC(this));
  ATOMIC_SET(&update_version_, OB_INVALID_VERSION);
  WEAK_BARRIER();
  recorder_.clog_update_fail();
  return ret;
}

ObIStorageClogRecorder::ObIStorageClogRecorder()
  : lock_(false),
    logcb_finish_flag_(true),
    logcb_ptr_(nullptr),
    log_handler_(nullptr),
    max_saved_version_(OB_INVALID_VERSION),
    clog_scn_()
{
}

ObIStorageClogRecorder::~ObIStorageClogRecorder()
{
  destroy();
}

void ObIStorageClogRecorder::destroy()
{
  max_saved_version_ = OB_INVALID_VERSION;
  lock_ = false;
  logcb_finish_flag_ = true;
  log_handler_ = NULL;
  clog_scn_.reset();
}

void ObIStorageClogRecorder::reset()
{
  wait_to_lock(OB_INVALID_VERSION); // lock
  max_saved_version_ = 0;
  ATOMIC_STORE(&lock_, false); // unlock
}

int ObIStorageClogRecorder::init(
    const int64_t max_saved_version,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_saved_version < 0 || NULL == log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(max_saved_version), KP(log_handler));
  } else {
    max_saved_version_ = max_saved_version;
    log_handler_ = log_handler;
  }
  return ret;
}

OB_INLINE void ObIStorageClogRecorder::wait_to_lock(const int64_t update_version)
{
  while (true) {
    int64_t last_time = ObTimeUtility::fast_current_time();
    while (true == ATOMIC_LOAD(&lock_)) {
      usleep(100);
      if (ObTimeUtility::fast_current_time() + 100 * 1000 > last_time) {
        last_time = ObTimeUtility::fast_current_time();
        LOG_DEBUG("waiting to lock", K(update_version), K(max_saved_version_), KPC(this));
      }
      WEAK_BARRIER();
    }

    if (ATOMIC_BCAS(&lock_, false, true)) { // success to lock
      break;
    }
  } // end of while
}

OB_INLINE void ObIStorageClogRecorder::wait_for_logcb(const int64_t update_version)
{
  int64_t last_time = ObTimeUtility::fast_current_time();
  while (false == ATOMIC_LOAD(&logcb_finish_flag_)) {
    if (ObTimeUtility::fast_current_time() + 100 * 1000 > last_time) {
      last_time = ObTimeUtility::fast_current_time();
      LOG_DEBUG("waiting for clog callback", K(update_version), K(max_saved_version_), KPC(this));
    }
    usleep(100);
    WEAK_BARRIER();
  }
}

int ObIStorageClogRecorder::try_update_with_lock(
    const int64_t update_version,
    const char *clog_buf,
    const int64_t clog_len,
    const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  while ((OB_SUCC(ret) || OB_BLOCK_FROZEN == ret)
      && update_version > ATOMIC_LOAD(&max_saved_version_)) {
    logcb_ptr_->set_update_version(update_version);
    if (OB_FAIL(submit_log(update_version, clog_buf, clog_len))) {
      if (OB_BLOCK_FROZEN != ret) {
        LOG_WARN("fail to submit log", K(ret), K(update_version), K(max_saved_version_));
      } else if (ObTimeUtility::fast_current_time() >= expire_ts) {
        ret = OB_EAGAIN;
        LOG_WARN("failed to sync clog", K(ret), K(update_version),
            K(max_saved_version_), K(expire_ts));
      }
    } else {
      wait_for_logcb(update_version);  // wait clog callback
    }
    WEAK_BARRIER();
  } // end of while

  return ret;
}

int ObIStorageClogRecorder::try_update_for_leader(
    const int64_t update_version,
    ObIAllocator *allocator,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(update_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input version is invalid", K(ret), KPC(this), K(update_version));
  } else if (update_version > ATOMIC_LOAD(&max_saved_version_)) {

    wait_to_lock(update_version); // lock
    const int64_t expire_ts = ObTimeUtility::fast_current_time() + timeout_ts;
    int64_t cur_update_version = update_version;
    char *clog_buf = nullptr;
    int64_t clog_len = 0;
    if (cur_update_version > ATOMIC_LOAD(&max_saved_version_)) {
      // may change cur_update_version in prepare_struct_in_lock
      if (OB_FAIL(prepare_struct_in_lock(cur_update_version, allocator, clog_buf, clog_len))) {
        LOG_WARN("failed to get struct", K(ret), K(update_version));
      } else if (OB_UNLIKELY(cur_update_version < update_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update version is smaller", K(ret), K(cur_update_version), K(update_version));
      } else if (OB_FAIL(try_update_with_lock(cur_update_version, clog_buf, clog_len, expire_ts))) {
        LOG_WARN("retry failed", K(ret), KPC(this), K(cur_update_version));
      } else { // sync clog success
        LOG_DEBUG("sync clog success", KPC(this), K(cur_update_version), K(max_saved_version_));
      }
    }

    // clear state no matter success or failed
    ATOMIC_STORE(&logcb_finish_flag_, true);
    free_struct_in_lock();
    WEAK_BARRIER();
    ATOMIC_STORE(&lock_, false); // unlock
  }
  if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObIStorageClogRecorder::replay_clog(
    const int64_t update_version,
    const share::SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (update_version <= ATOMIC_LOAD(&max_saved_version_)) {
    LOG_INFO("skip clog with smaller version", K(update_version), K(max_saved_version_), KPC(this));
  } else if (OB_FAIL(inner_replay_clog(update_version, scn, buf, size, pos))) {
    if (OB_NO_NEED_UPDATE == ret) { // not update max_saved_version_
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to replay clog", K(ret), KPC(this));
    }
  } else {
    ATOMIC_STORE(&max_saved_version_, update_version);
    LOG_DEBUG("success to replay clog", K(ret), KPC(this), K(max_saved_version_));
  }

  return ret;
}

void ObIStorageClogRecorder::clog_update_fail()
{
  sync_clog_failed_for_leader();
  WEAK_BARRIER();
  ATOMIC_STORE(&logcb_finish_flag_, true);
}

void ObIStorageClogRecorder::clog_update_succ(
    const int64_t update_version,
    bool &finish_flag)
{
  int ret = OB_SUCCESS;
  finish_flag = false;
  if (OB_UNLIKELY(update_version <= ATOMIC_LOAD(&max_saved_version_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("clog with smaller version", K(ret), K(update_version), K(max_saved_version_));
  } else if (OB_UNLIKELY(clog_scn_.get_val_for_tx() <= 0)) {
    // clog_scn_ may be invalid because of concurrency in rare situation
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clog ts is invalid", K(ret), K_(clog_scn));
  } else {
    if (OB_FAIL(sync_clog_succ_for_leader(update_version))) {
      LOG_WARN("failed to save for leader", K(ret), KPC(this));
    } else {
      finish_flag = true;
      ATOMIC_STORE(&max_saved_version_, update_version);
      LOG_DEBUG("update success", K(ret), KPC(this));
    }
  }
  ATOMIC_STORE(&logcb_finish_flag_, true);
}

int ObIStorageClogRecorder::write_clog(
  const char *buf,
  const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const bool need_nonblock = false;
  const bool allow_compression = false;
  palf::LSN lsn;
  clog_scn_.reset();
  if (OB_UNLIKELY(nullptr == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len));
  } else if (OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("palf handle is null", K(ret), KP(log_handler_));
  } else if (FALSE_IT(ATOMIC_STORE(&logcb_finish_flag_, false))) {
  } else if (OB_FAIL(log_handler_->append(buf, buf_len, share::SCN::min_scn(), need_nonblock, allow_compression, logcb_ptr_, lsn, clog_scn_))) {
    LOG_WARN("fail to submit log", K(ret), KPC(this));
  }
  return ret;
}

int ObIStorageClogRecorder::get_tablet_handle(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObIStorageClogRecorder::replay_get_tablet_handle(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &scn,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  const bool is_update_mds_table = false;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->replay_get_tablet(tablet_id, scn, is_update_mds_table, tablet_handle))) {
    if (OB_OBSOLETE_CLOG_NEED_SKIP == ret) {
      LOG_INFO("clog is obsolete, should skip replay", K(ret), K(ls_id), K(tablet_id), K(scn));
      ret = OB_SUCCESS;
    } else if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("retry get tablet for timeout error", K(ret), K(ls_id), K(tablet_id), K(scn));
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id), K(scn));
    }
  }
  return ret;
}

} // storage
} // oceanbase
