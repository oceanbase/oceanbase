/**
 * Copyright (c) 2023-present OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "log_sync_mode_mgr.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/serialization.h"
#include "../ob_log_base_type.h"
#include "../ob_log_base_header.h"
#include "../ob_log_handler.h"
#include "../applyservice/ob_log_apply_service.h"
#include "../transportservice/ob_log_transport_service.h"
#include "storage/ls/ob_ls.h"
#include "lib/wait_event/ob_wait_event.h"
#include "share/ob_sync_standby_dest_parser.h"
#include "share/ob_share_util.h"

namespace oceanbase
{
namespace logservice
{
 // ==================== ObSyncModeLog 实现 ====================

ObSyncModeLog::ObSyncModeLog()
  : header_(),
    version_(SYNC_MODE_LOG_VERSION),
    log_type_(0),
    protection_info_()
{
}

ObSyncModeLog::ObSyncModeLog(const int16_t log_type)
  : header_(ObLogBaseType::SYNC_MODE_LOG_BASE_TYPE, STRICT_BARRIER),
    version_(SYNC_MODE_LOG_VERSION),
    log_type_(log_type),
    protection_info_()
{
}

ObSyncModeLog::~ObSyncModeLog()
{
  reset();
}

void ObSyncModeLog::reset()
{
  header_.reset();
  version_ = SYNC_MODE_LOG_VERSION;
  log_type_ = 0;
  protection_info_.reset();
}

int16_t ObSyncModeLog::get_log_type() const
{
  return log_type_;
}

void ObSyncModeLog::set_protection_info(const share::ObSyncStandbyStatusAttr &protection_info)
{
  protection_info_ = protection_info;
}

DEFINE_SERIALIZE(ObSyncModeLog)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, pos))) {
    CLOG_LOG(WARN, "serialize header_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    CLOG_LOG(WARN, "serialize version_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, log_type_))) {
    CLOG_LOG(WARN, "serialize log_type_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(protection_info_.serialize(buf, buf_len, pos))) {
    CLOG_LOG(WARN, "serialize protection_info_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSyncModeLog)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, pos))) {
    CLOG_LOG(WARN, "deserialize header_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    CLOG_LOG(WARN, "deserialize version_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &log_type_))) {
    CLOG_LOG(WARN, "deserialize log_type_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(protection_info_.deserialize(buf, data_len, pos))) {
    CLOG_LOG(WARN, "deserialize protection_info_ failed", K(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSyncModeLog)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i16(log_type_);
  size += protection_info_.get_serialize_size();
  return size;
}

// ==================== ObSyncModeLogHandler 实现 ====================

int ObSyncModeLogHandler::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObSyncModeLogHandler has already been inited", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(ls), K(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObSyncModeLogHandler init success", K(ret), K(ls->get_ls_id()));
  }
  return ret;
}

void ObSyncModeLogHandler::reset()
{
  is_inited_ = false;
  ls_ = nullptr;
}

int ObSyncModeLogHandler::replay(const void *buffer,
                                  const int64_t nbytes,
                                  const palf::LSN &lsn,
                                  const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeLogHandler not inited", K(ret));
  } else if (OB_ISNULL(buffer) || nbytes <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buffer), K(nbytes));
  } else {
    ret = replay_sync_mode_log(buffer, nbytes, lsn, scn);
  }
  return ret;
}

int ObSyncModeLogHandler::replay_sync_mode_log(const void *buffer,
                                                const int64_t nbytes,
                                                const palf::LSN &lsn,
                                                const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObSyncModeLog sync_mode_log;
  int64_t pos = 0;

  if (OB_FAIL(sync_mode_log.deserialize(static_cast<const char*>(buffer), nbytes, pos))) {
    CLOG_LOG(WARN, "deserialize sync_mode_log failed", K(ret), K(lsn), K(scn));
  } else {
    CLOG_LOG(INFO, "replay sync_mode_log success", K(lsn), K(scn), K(sync_mode_log));
    // TODO by ziqi: 根据 log_type 处理
  }
  return ret;
}

void ObSyncModeLogHandler::switch_to_follower_forcedly()
{
  CLOG_LOG(INFO, "ObSyncModeLogHandler switch_to_follower_forcedly", K(ls_->get_ls_id()));
}

int ObSyncModeLogHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "ObSyncModeLogHandler switch_to_leader", K(ls_->get_ls_id()));
  return ret;
}

int ObSyncModeLogHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "ObSyncModeLogHandler switch_to_follower_gracefully", K(ls_->get_ls_id()));
  return ret;
}

int ObSyncModeLogHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "ObSyncModeLogHandler resume_leader", K(ls_->get_ls_id()));
  return ret;
}

int ObSyncModeLogHandler::flush(share::SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  // Sync mode log handler doesn't need checkpoint, return max scn
  rec_scn.set_max();
  return ret;
}

share::SCN ObSyncModeLogHandler::get_rec_scn()
{
  // Sync mode log handler doesn't need checkpoint, return max scn
  return share::SCN::max_scn();
}

// ==================== ObSyncModeManager 实现 ====================

ObSyncModeManager::ObSyncModeManager()
  : is_inited_(false), log_handler_(nullptr)
{
}

void ObSyncModeManager::reset()
{
  is_inited_ = false;
  log_handler_ = nullptr;
}

int ObSyncModeManager::init(ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObSyncModeManager has already been inited", K(ret));
  } else if (OB_ISNULL(log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(log_handler), K(ret));
  } else {
    log_handler_ = log_handler;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObSyncModeManager init success", K(ret));
  }
  return ret;
}

int ObSyncModeManager::write_sync_mode_log(const ObSyncModeLogType log_type,
                                           const share::SCN &ref_scn,
                                           const share::ObSyncStandbyStatusAttr &protection_info,
                                           palf::LSN &lsn,
                                           share::SCN &scn,
                                           const int64_t expected_proposal_id,
                                           const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (expected_proposal_id <= 0 || palf::INVALID_PROPOSAL_ID == expected_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(expected_proposal_id));
  } else if (abs_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(abs_timeout_us));
  } else if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    CLOG_LOG(WARN, "failed to set timeout ctx", KR(ret), K(abs_timeout_us));
  } else {
    const int64_t ls_id = log_handler_->id_;
    bool need_push_cb = true;
    ObSyncModeLog sync_mode_log(log_type);
    sync_mode_log.set_protection_info(protection_info);
    const int64_t log_size = sync_mode_log.get_serialize_size();
    char *buffer = static_cast<char*>(ob_malloc(log_size, "SyncModeLog")); //TODO by ziqi: 内存申请优化

    CLOG_LOG(INFO, "write sync_mode_log", K(ls_id), K(sync_mode_log), K(log_type), K(lsn), K(scn), K(log_size));

    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "allocate memory failed", K(ret), K(ls_id), K(log_size));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(sync_mode_log.serialize(buffer, log_size, pos))) {
        CLOG_LOG(WARN, "serialize sync_mode_log failed", K(ret), K(ls_id));
      } else {
        // Create callback on heap with reference counting
        SyncModeLogCb *cb = nullptr;
        if (log_type == ObSyncModeLogType::PRE_ASYNC
            || log_type == ObSyncModeLogType::SYNC
            || log_type == ObSyncModeLogType::ASYNC) {
          need_push_cb = (log_type == ObSyncModeLogType::SYNC);

          // Create callback with reference counting (ref_cnt = 1 after creation)
          if (OB_FAIL(SyncModeLogCb::create(cb))) {
            CLOG_LOG(WARN, "create SyncModeLogCb failed", K(ret), K(ls_id));
          } else {
            // Add extra reference if callback will be pushed to async queue
            // This ensures callback remains valid even if caller times out
            if (need_push_cb) {
              cb->inc_ref();  // ref_cnt = 2 (caller + applyservice)
            }

            // Submit callback to emergency_append
            ret = log_handler_->emergency_append(buffer, log_size, ref_scn, false, cb, lsn, scn,
                                                 logservice::SYNC_MODE_LOG_BASE_TYPE, expected_proposal_id, need_push_cb);

            if (OB_FAIL(ret)) {
              CLOG_LOG(WARN, "append sync_mode_log failed", K(ret), K(ls_id), K(log_type));
              // Rollback extra reference on failure
              if (need_push_cb) {
                cb->dec_ref();
              }
              // Release caller's reference
              cb->dec_ref();
              cb = nullptr;
            } else {
              // Wait synchronously for callback completion
              const palf::LSN cb_lsn = cb->__get_lsn();
              bool is_finished = false;
              const int64_t WAIT_TIME = 500L; // 500us

              while (!is_finished && OB_SUCC(ret)) {
                // For PRE_ASYNC/ASYNC: check if log is committed and trigger on_success
                if (!need_push_cb && nullptr != cb && !cb->is_succeed() && !cb->is_failed()) {
                  // Only check if callback is still in INIT state to avoid duplicate on_success calls
                  palf::LSN committed_end_lsn;
                  if (OB_FAIL(log_handler_->get_end_lsn(committed_end_lsn))) {
                    if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
                      CLOG_LOG(WARN, "failed to get end lsn", KR(ret), K(ls_id));
                    }
                  } else if (committed_end_lsn > cb_lsn) {
                    // Log has been committed, call on_success to release reference
                    // Note: on_success() will set state to SUCCESS and dec_ref(), so it should only be called once
                    if (OB_SUCC(cb->on_success())) {
                      CLOG_LOG(INFO, "on_success success", K(ls_id), K(log_type), K(lsn), K(scn), K(committed_end_lsn), K(cb_lsn));
                      cb = nullptr;  // on_success() releases the reference
                      is_finished = true;  // Directly set finished since on_success was called
                    } else {
                      CLOG_LOG(WARN, "on_success failed", K(ls_id), K(log_type), K(cb_lsn), K(committed_end_lsn));
                    }
                  }
                }

                // Check callback state
                if (nullptr == cb) {
                  // Already handled above (on_success was called), skip
                } else if (cb->is_succeed()) {
                  is_finished = true;
                  CLOG_LOG(TRACE, "write sync_mode_log success", K(ls_id), K(log_type), K(lsn), K(scn));
                } else if (cb->is_failed()) {
                  is_finished = true;
                  ret = OB_EAGAIN;
                  CLOG_LOG(WARN, "write sync_mode_log failed", K(ret), K(ls_id), K(log_type), K(lsn), K(scn));
                } else if (ctx.is_timeouted()) {
                  // Timeout: return OB_TIMEOUT and ensure proper cleanup
                  // Caller's reference will be released after loop completes
                  // For need_push_cb=true: applyservice still holds a reference and will release it via on_success/on_failure
                  // For need_push_cb=false: this will be the last reference, cb will be released
                  ret = OB_TIMEOUT;
                  is_finished = true;
                  CLOG_LOG(WARN, "wait on_success timeout", K(ret), K(ls_id), K(log_type), K(lsn), K(scn), K(ctx), K(need_push_cb));
                } else {
                  ob_usleep(WAIT_TIME);
                  if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
                    CLOG_LOG(WARN, "write sync_mode_log wait cb too much time", K(ls_id), K(log_type), K(lsn), K(scn));
                  }
                }
              }

              // Release caller's reference after waiting completes
              // Note: If callback was pushed to async queue (need_push_cb=true),
              //       applyservice still holds a reference and will release it via on_success/on_failure
              //       If callback was not pushed (need_push_cb=false), this releases the last reference
              //       If on_success was already called, cb is already nullptr
              if (nullptr != cb) {
                cb->dec_ref();
                cb = nullptr;
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "unexpected log type", K(ret), K(ls_id), K(log_type));
        }
      }
      ob_free(buffer);
    }
  }
  return ret;
}

// TODO by ziqi: 确认是否delete，通知 apply_service 清除备库位点
int ObSyncModeManager::notify_apply_service_with_standby_lsn()
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard tp_guard;
  LogTransportStatus *transport_status = nullptr;
  ObLSID ls_id;
  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (OB_FALSE_IT(ls_id = log_handler_->id_)) {
  } else if (OB_ISNULL(log_handler_->transport_service_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_service is null", K(ret), K(ls_id));
  } else if (OB_ISNULL(log_handler_->apply_status_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "apply_status_ is NULL", K(ret), K(ls_id));
  } else if (OB_FAIL(log_handler_->transport_service_->get_transport_status(ls_id, tp_guard))) {
    CLOG_LOG(WARN, "get_transport_status failed", K(ret), K(ls_id));
  } else if (NULL == (transport_status = tp_guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_status is NULL", K(ret), K(ls_id));
  } else {
    const palf::LSN standby_committed_end_lsn = transport_status->get_standby_committed_end_lsn();
    const share::SCN standby_committed_end_scn = transport_status->get_standby_committed_end_scn();

    if (standby_committed_end_lsn.is_valid() && standby_committed_end_scn.is_valid()) {
      if (OB_FAIL(log_handler_->transport_service_->notify_apply_service(ls_id, standby_committed_end_lsn, standby_committed_end_scn))) {
        CLOG_LOG(WARN, "notify_apply_service failed", K(ret), K(ls_id), K(standby_committed_end_lsn));
      } else {
        CLOG_LOG(INFO, "notify_apply_service_with_standby_lsn success", K(ls_id), K(standby_committed_end_lsn));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "cannot get valid standby or palf committed end lsn", K(ret), K(ls_id), K(standby_committed_end_lsn),
               K(standby_committed_end_scn));
    }
  }
  return ret;
}

int ObSyncModeManager::handle_sync_mode_upgrade(const int64_t mode_version,
                                                const bool need_write_log,
                                                const share::ObLSID &ls_id,
                                                const share::SCN &ref_scn,
                                                const share::ObSyncStandbyStatusAttr &protection_info,
                                                share::SCN &end_scn,
                                                int64_t &new_mode_version,
                                                const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t new_proposal_id = palf::INVALID_PROPOSAL_ID;
  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (!need_write_log) { // For Standby
    if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::SYNC, new_mode_version, new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to SYNC failed", K(ret), K(ls_id), K(mode_version));
    } else {
      CLOG_LOG(INFO, "change_sync_mode success", K(ls_id));
    }
  } else { // For Primary
    // 1. 修改日志流的 sync_mode 为 SYNC
    if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::SYNC, new_mode_version, new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to SYNC failed", K(ret), K(ls_id), K(mode_version));
    }
    // 2. 等待 log_handler 成为 leader，强同步升级日志需要依赖log_handler有leader
    // 此处使用change_sync_mode返回的proposal id，确保没有切主
    else if (OB_FAIL(wait_log_handler_leader_(new_proposal_id, abs_timeout_us))) {
      CLOG_LOG(WARN, "wait_log_handler_leader_ failed", K(ret), K(ls_id), K(new_proposal_id), K(abs_timeout_us));
    // }
    // 3. 修改transport_service的sync_mode为SYNC
    // else if (OB_ISNULL(log_handler_->transport_service_)) {
    //   ret = OB_ERR_UNEXPECTED;
    //   CLOG_LOG(WARN, "transport_service is null", K(ret), K(ls_id));
    // } else if (OB_FAIL(log_handler_->transport_service_->enable_sync_mode(ls_id, new_proposal_id))) {
    //   CLOG_LOG(WARN, "transport_service enable_sync_mode failed", K(ret), K(ls_id));
    // } else if (OB_FAIL(log_handler_->apply_status_->enable_sync_mode(new_proposal_id))) {
    //   CLOG_LOG(WARN, "apply status enable_sync_mode failed", K(ret), K(ls_id), K(new_proposal_id));
    }
    // 4. 写 SYNC 特殊日志
    else {
      palf::LSN lsn;
      if (OB_FAIL(write_sync_mode_log(ObSyncModeLogType::SYNC, ref_scn, protection_info, lsn, end_scn, new_proposal_id,
                                      abs_timeout_us))) {
        CLOG_LOG(WARN, "write SYNC mode log failed", K(ret), K(ls_id));
      } else {
        CLOG_LOG(INFO, "write SYNC mode log success, unblocking append", K(ls_id), K(lsn), K(end_scn));
      }
    }
  }
  return ret;
}

int ObSyncModeManager::handle_sync_mode_downgrade(const int64_t mode_version,
                                                  const bool need_write_log,
                                                  const share::ObLSID &ls_id,
                                                  const share::SCN &ref_scn,
                                                  const share::ObSyncStandbyStatusAttr &protection_info,
                                                  share::SCN &end_scn,
                                                  int64_t &new_mode_version,
                                                  const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t new_proposal_id = palf::INVALID_PROPOSAL_ID;
  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (!need_write_log) { // For Standby
    if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::PRE_ASYNC, new_mode_version, new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to PRE_ASYNC failed", K(ret), K(log_handler_->id_), K(mode_version), K(new_mode_version));
    }
  } else if (need_write_log) { // For Primary
    // 1. 修改日志流的 sync_mode 为 PRE_ASYNC
    if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::PRE_ASYNC, new_mode_version, new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to PRE_ASYNC failed", K(ret), K(ls_id), K(mode_version), K(new_mode_version));
    } else if (OB_ISNULL(log_handler_->transport_service_) || OB_ISNULL(log_handler_->apply_status_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "transport_service is null", K(ret), K(ls_id));
    } else if (OB_FAIL(log_handler_->apply_status_->disable_sync_mode(new_proposal_id))) {
      CLOG_LOG(WARN, "apply status disable_sync_mode failed", K(ret), K(ls_id), K(new_proposal_id));
    } else if (OB_FAIL(log_handler_->transport_service_->disable_sync_mode(ls_id, new_proposal_id))) {
      CLOG_LOG(WARN, "transport service disable_sync_mode failed", K(ret), K(ls_id), K(new_proposal_id));
      // clear_standby_info 已在 disable_sync_mode 内部调用，无需单独调用
    }
    // 2. 写 PRE_ASYNC 日志
    else {
      palf::LSN lsn;
      if (OB_FAIL(write_sync_mode_log(ObSyncModeLogType::PRE_ASYNC, ref_scn, protection_info, lsn, end_scn,
                                      new_proposal_id, abs_timeout_us))) {
        CLOG_LOG(WARN, "write PRE_ASYNC mode log failed", K(ret), K(ls_id));
      } else {
        CLOG_LOG(INFO, "write PRE_ASYNC mode log success", K(ls_id), K(lsn), K(end_scn));
      }
    }
  }

  return ret;
}

int ObSyncModeManager::handle_sync_mode_downgrade_finish(const int64_t mode_version,
                                                           const bool need_write_log,
                                                           const share::ObLSID &ls_id,
                                                           const share::SCN &ref_scn,
                                                           const share::ObSyncStandbyStatusAttr &protection_info,
                                                           share::SCN &end_scn,
                                                           int64_t &new_mode_version,
                                                           const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (!need_write_log) { // For Standby
    int64_t new_proposal_id = palf::INVALID_PROPOSAL_ID;
    if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::ASYNC, new_mode_version, new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to ASYNC failed", K(ret), K(log_handler_->id_), K(mode_version));
    } else {
      CLOG_LOG(INFO, "change_sync_mode to ASYNC success", K(log_handler_->id_), K(mode_version));
    }
  } else { // For Primary
    // ASYNC降级先写ASYNC日志再修改sync_mode，防止
    palf::LSN lsn;
    int64_t palf_proposal_id = palf::INVALID_PROPOSAL_ID;
    int64_t new_proposal_id = palf::INVALID_PROPOSAL_ID;
    common::ObRole palf_role = common::FOLLOWER;
    bool is_pending_state = false;
    if (OB_FAIL(log_handler_->palf_handle_->get_role(palf_role, palf_proposal_id, is_pending_state))) {
      CLOG_LOG(WARN, "get_role failed", K(ret), K(ls_id));
    } else if (LEADER != palf_role || true == is_pending_state) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "not leader or pending state", K(ret), K(ls_id), K(palf_role), K(palf_proposal_id),
               K(is_pending_state));
    } else if (OB_FAIL(write_sync_mode_log(ObSyncModeLogType::ASYNC, ref_scn, protection_info, lsn, end_scn,
                                           palf_proposal_id, abs_timeout_us))) {
      CLOG_LOG(WARN, "write ASYNC mode log failed", K(ret), K(ls_id));
    } else if (OB_FAIL(log_handler_->change_sync_mode(mode_version, palf::SyncMode::ASYNC, new_mode_version,
                       new_proposal_id))) {
      CLOG_LOG(WARN, "change_sync_mode to ASYNC failed", K(ret), K(ls_id), K(mode_version));
    } else {
      // 收到ASYNC日志对log handler解锁
      CLOG_LOG(INFO, "write ASYNC mode log success, unblocking append", K(ls_id), K(lsn), K(end_scn));
    }

  }
  return ret;
}

int ObSyncModeManager::process_upgrade_and_downgrade(const int64_t mode_version,
                                                       const palf::SyncMode &sync_mode,
                                                       const share::SCN &ref_scn,
                                                       const share::ObSyncStandbyStatusAttr &protection_info,
                                                       share::SCN &end_scn,
                                                       int64_t &new_mode_version,
                                                       const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  palf::AccessMode curr_access_mode;
  bool need_write_log = true;
  int64_t curr_mode_version = palf::INVALID_PROPOSAL_ID;

  if (!is_inited_ || OB_ISNULL(log_handler_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (OB_FAIL(log_handler_->get_access_mode(curr_mode_version, curr_access_mode))) {
    CLOG_LOG(WARN, "get_access_mode failed", K(ret), K(log_handler_->id_));
  } else {
    need_write_log = palf::AccessMode::APPEND == curr_access_mode; // 备库升降级不需要写特殊日志
    share::ObLSID ls_id(log_handler_->id_);
    if (palf::SyncMode::SYNC == sync_mode) {
      ret = handle_sync_mode_upgrade(mode_version, need_write_log, ls_id, ref_scn, protection_info, end_scn, new_mode_version, abs_timeout_us);
    } else if (palf::SyncMode::PRE_ASYNC == sync_mode) {
      ret = handle_sync_mode_downgrade(mode_version, need_write_log, ls_id, ref_scn, protection_info, end_scn, new_mode_version, abs_timeout_us);
    } else if (palf::SyncMode::ASYNC == sync_mode) {
      ret = handle_sync_mode_downgrade_finish(mode_version, need_write_log, ls_id, ref_scn, protection_info, end_scn, new_mode_version, abs_timeout_us);
    } else {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid sync_mode", K(ret), K(log_handler_->id_), K(sync_mode));
    }
  }
  return ret;
}

int ObSyncModeManager::wait_log_handler_leader_(const int64_t proposal_id, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSyncModeManager not inited", K(ret));
  } else if (OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_handler is null", K(ret));
  } else if (abs_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(abs_timeout_us));
  } else if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    CLOG_LOG(WARN, "failed to set timeout ctx", K(ret), K(log_handler_->id_), K(abs_timeout_us));
  } else {
    share::ObLSID ls_id(log_handler_->id_);
    const int64_t WAIT_TIME = 500L; // 500us
    common::ObRole role = FOLLOWER;
    int64_t curr_proposal_id = palf::INVALID_PROPOSAL_ID;
    bool done = false;

    while (OB_SUCC(ret) && !done) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        CLOG_LOG(WARN, "wait log handler leader timeout",
                 K(ret), K(ls_id), K(abs_timeout_us), K(role), K(curr_proposal_id), K(proposal_id));
      } else if (OB_FAIL(log_handler_->get_role(role, curr_proposal_id))) {
        CLOG_LOG(WARN, "get_role failed", K(ret), K(ls_id));
      } else if (LEADER == role && curr_proposal_id == proposal_id) {
        done = true;
      } else if (curr_proposal_id > proposal_id) {
        ret = OB_NOT_MASTER;
        CLOG_LOG(WARN, "curr_proposal_id is greater than proposal_id", K(ret), K(ls_id), K(curr_proposal_id), K(proposal_id));
      } else {
        ob_usleep(WAIT_TIME);
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
          CLOG_LOG(WARN, "wait log handler leader too much time", K(ls_id), K(abs_timeout_us), K(role),
                  K(curr_proposal_id), K(proposal_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      CLOG_LOG(INFO, "wait_log_handler_leader_ success", K(ls_id));
    }
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase


