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

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX TRANS

#include "ob_ls_tx_service.h"
#include "share/throttle/ob_throttle_unit.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_replay_executor.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_retain_ctx_mgr.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
using namespace share;
using namespace transaction;
using namespace transaction::tablelock;
using namespace palf;

namespace storage
{
using namespace checkpoint;

int ObLSTxService::init(const ObLSID &ls_id,
                        ObLSTxCtxMgr *mgr,
                        ObTransService *trans_service)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || OB_ISNULL(mgr) || OB_ISNULL(trans_service)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(ls_id), KP(mgr), KP(trans_service));
  } else {
    ls_id_ = ls_id;
    mgr_ = mgr;
    trans_service_ = trans_service;

  }
  return ret;
}

int ObLSTxService::create_tx_ctx(ObTxCreateArg arg,
                                 bool &existed,
                                 ObPartTransCtx *&ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = mgr_->create_tx_ctx(arg, existed, ctx);
  }
  return ret;
}

int ObLSTxService::get_tx_ctx(const transaction::ObTransID &tx_id,
                              const bool for_replay,
                              ObPartTransCtx *&ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = mgr_->get_tx_ctx(tx_id, for_replay, ctx);
  }
  return ret;
}

int ObLSTxService::get_tx_ctx_with_timeout(const transaction::ObTransID &tx_id,
                                           const bool for_replay,
                                           transaction::ObPartTransCtx *&tx_ctx,
                                           const int64_t lock_timeout) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = mgr_->get_tx_ctx_with_timeout(tx_id, for_replay, tx_ctx, lock_timeout);
  }

  return ret;
}

int ObLSTxService::get_tx_scheduler(const transaction::ObTransID &tx_id,
                                    ObAddr &scheduler) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ObPartTransCtx *ctx;
    if (OB_FAIL(mgr_->get_tx_ctx_directly_from_hash_map(tx_id, ctx))) {
      // TRANS_LOG(WARN, "get ctx failed", K(ret), K(tx_id));
    } else if (OB_ISNULL(ctx)) {
      ret = OB_BAD_NULL_ERROR;
      TRANS_LOG(WARN, "get ctx is null", K(ret), K(tx_id));
    } else {
      scheduler = ctx->get_scheduler();
      if (OB_SUCCESS != (tmp_ret = mgr_->revert_tx_ctx(ctx))) {
        TRANS_LOG(ERROR, "fail to revert tx", K(ret), K(tmp_ret), K(tx_id), KPC(ctx));
      }
      if (!scheduler.is_valid()) {// follower ctx, not replay commit info yet, this may happed
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObLSTxService::get_tx_start_session_id(const transaction::ObTransID &tx_id, uint32_t &session_id) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ObPartTransCtx *ctx;
    if (OB_FAIL(mgr_->get_tx_ctx_directly_from_hash_map(tx_id, ctx))) {
      if (OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        TRANS_LOG(INFO, "ctx not existed on this LS", K(tx_id), K(ls_id_));
      } else {
        TRANS_LOG(WARN, "get ctx failed", K(ret), K(tx_id), K(ls_id_));
      }
    } else if (OB_ISNULL(ctx)) {
      ret = OB_BAD_NULL_ERROR;
      TRANS_LOG(WARN, "get ctx is null", K(ret), K(tx_id), K(ls_id_));
    } else {
      session_id = ctx->get_session_id();
      if (OB_TMP_FAIL(mgr_->revert_tx_ctx(ctx))) {
        TRANS_LOG(ERROR, "fail to revert tx", K(ret), K(tmp_ret), K(tx_id), KPC(ctx));
      }
    }
  }
  return ret;
}

int ObLSTxService::revert_tx_ctx(ObTransCtx *ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = mgr_->revert_tx_ctx(ctx);
  }
  return ret;
}

int ObLSTxService::get_read_store_ctx(const ObTxReadSnapshot &snapshot,
                                      const bool read_latest,
                                      const int64_t lock_timeout,
                                      ObStoreCtx &store_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_) || OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(trans_service_), KP(mgr_));
  } else if (OB_FAIL(mgr_->start_readonly_request())) {
    TRANS_LOG(WARN, "start readonly request failed", K(ret));
  } else {
    store_ctx.ls_id_ = ls_id_;
    store_ctx.is_read_store_ctx_ = true;
    ret = trans_service_->get_read_store_ctx(snapshot, read_latest, lock_timeout, store_ctx);
    if (OB_FAIL(ret)) {
      mgr_->end_readonly_request();
    }
  }
  return ret;
}

int ObLSTxService::start_request_for_transfer()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_) || OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(trans_service_), KP(mgr_));
  } else {
    mgr_->inc_total_request_by_transfer_dest();
  }
  return ret;
}

int ObLSTxService::end_request_for_transfer()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_) || OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(trans_service_), KP(mgr_));
  } else {
    mgr_->dec_total_request_by_transfer_dest();
  }
  return ret;
}

int ObLSTxService::get_read_store_ctx(const SCN &snapshot,
                                      const int64_t lock_timeout,
                                      ObStoreCtx &store_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_) || OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(trans_service_), KP(mgr_));
  } else if (OB_FAIL(mgr_->start_readonly_request())) {
    TRANS_LOG(WARN, "start readonly request failed", K(ret));
  } else {
    store_ctx.ls_id_ = ls_id_;
    store_ctx.is_read_store_ctx_ = true;
    ret = trans_service_->get_read_store_ctx(snapshot, lock_timeout, store_ctx);
    if (OB_FAIL(ret)) {
      mgr_->end_readonly_request();
    }
  }
  return ret;
}

int ObLSTxService::get_write_store_ctx(ObTxDesc &tx,
                                       const ObTxReadSnapshot &snapshot,
                                       const concurrent_control::ObWriteFlag write_flag,
                                       storage::ObStoreCtx &store_ctx,
                                       const ObTxSEQ &spec_seq_no) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    int64_t abs_expire_ts = ObClockGenerator::getClock() + tx.get_timeout_us();
    if (abs_expire_ts < 0) {
      abs_expire_ts = ObClockGenerator::getClock() + share::ObThrottleUnit<ObTenantTxDataAllocator>::DEFAULT_MAX_THROTTLE_TIME;
    }

    ObTxDataThrottleGuard tx_data_throttle_guard(false /* for_replay */, abs_expire_ts);
    ret = trans_service_->get_write_store_ctx(tx, snapshot, write_flag, store_ctx, spec_seq_no, false);
  }
  return ret;
}

int ObLSTxService::revert_store_ctx(storage::ObStoreCtx &store_ctx) const
{
  int ret = OB_SUCCESS;

  // Phase1: revert the read count of the transfer src read
  ObTxTableGuard src_tx_table_guard = store_ctx.mvcc_acc_ctx_.get_tx_table_guards().src_tx_table_guard_;
  if (src_tx_table_guard.is_valid()) {
    // do not overrite ret
    int tmp_ret = OB_SUCCESS;
    ObLSHandle ls_handle = store_ctx.mvcc_acc_ctx_.get_tx_table_guards().src_ls_handle_;
    if (!ls_handle.is_valid()) {
      TRANS_LOG(ERROR, "src tx guard is valid when src ls handle not valid", K(store_ctx));
      if (OB_TMP_FAIL(MTL(ObLSService*)->get_ls(src_tx_table_guard.get_ls_id(), ls_handle, ObLSGetMod::TRANS_MOD))) {
        TRANS_LOG(ERROR, "get_ls failed", KR(tmp_ret), K(src_tx_table_guard));
      } else if (OB_TMP_FAIL(ls_handle.get_ls()->get_tx_svr()->end_request_for_transfer())) {
        TRANS_LOG(ERROR, "end request for transfer", KR(tmp_ret), K(src_tx_table_guard));
      }
    } else {
      if (OB_TMP_FAIL(ls_handle.get_ls()->get_tx_svr()->end_request_for_transfer())) {
        TRANS_LOG(ERROR, "end request for transfer", KR(tmp_ret), K(src_tx_table_guard));
      }
    }
  }

  // Phase2: revert the read count of the normal read
  if (store_ctx.is_read_store_ctx()) {
    // do not overrite ret
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(mgr_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "mgr is null", K(tmp_ret), KP(this));
    } else {
      (void)mgr_->end_readonly_request();
    }
  }

  if (OB_ISNULL(trans_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = trans_service_->revert_store_ctx(store_ctx);
  }
  return ret;
}

int ObLSTxService::check_scheduler_status(SCN &min_start_scn,
                                          transaction::MinStartScnStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = mgr_->check_scheduler_status(min_start_scn, status);
  }
  return ret;
}

int ObLSTxService::check_all_tx_clean_up() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (mgr_->get_active_tx_count() > 0) {
    // there is some tx not finished, retry.
    ret = OB_EAGAIN;
  } else {
    TRANS_LOG(INFO, "wait_all_tx_cleaned_up cleaned up success", K_(ls_id));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_GC_CHECK_RD_TX);
int ObLSTxService::check_all_readonly_tx_clean_up() const
{
  int ret = OB_SUCCESS;
  int64_t active_readonly_request_count = 0;
  int64_t total_request_by_transfer_dest = 0;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if ((active_readonly_request_count = mgr_->get_total_active_readonly_request_count()) > 0) {
    if (REACH_TIME_INTERVAL(5000000)) {
      TRANS_LOG(INFO, "readonly requests are active", K(active_readonly_request_count));
      mgr_->dump_readonly_request(3);
    }
    ret = OB_EAGAIN;
  } else if ((total_request_by_transfer_dest = mgr_->get_total_request_by_transfer_dest()) > 0) {
    if (REACH_TIME_INTERVAL(5000000)) {
      TRANS_LOG(INFO, "readonly requests are active", K(total_request_by_transfer_dest));
    }
    ret = OB_EAGAIN;
  } else {
    TRANS_LOG(INFO, "wait_all_readonly_tx_cleaned_up cleaned up success", K_(ls_id));
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_GC_CHECK_RD_TX ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        TRANS_LOG(INFO, "fake EN_GC_CHECK_RD_TX", K(ret));
      }
    }
#endif
  return ret;
}

int ObLSTxService::block_tx()
{
  int ret = OB_SUCCESS;
  bool unused_is_all_tx_clean_up = false;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->block_tx(unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "block rw tx failed", K_(ls_id));
  } else {
    TRANS_LOG(INFO, "block rw tx success", K_(ls_id));
  }
  return ret;
}

int ObLSTxService::block_all()
{
  int ret = OB_SUCCESS;
  bool unused_is_all_tx_clean_up = false;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->block_all(unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "block all failed", K_(ls_id));
  } else {
    TRANS_LOG(INFO, "block all success", K_(ls_id));
  }
  return ret;
}

int ObLSTxService::kill_all_tx(const bool graceful)
{
  int ret = OB_SUCCESS;
  bool unused_is_all_tx_clean_up = false;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->kill_all_tx(graceful, unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "kill_all_tx failed", K_(ls_id));
  } else {
    TRANS_LOG(INFO, "kill_all_tx success", K_(ls_id));
  }
  return ret;
}

int ObLSTxService::check_modify_schema_elapsed(const ObTabletID &tablet_id,
                                               const int64_t schema_version,
                                               ObTransID &block_tx_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tablet_id), K(schema_version));
  } else if (OB_FAIL(mgr_->check_modify_schema_elapsed(tablet_id,
                                                       schema_version,
                                                       block_tx_id))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check modify schema elapsed failed", K(ret),
                K(tablet_id), K(schema_version));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLSTxService::check_modify_time_elapsed(const ObTabletID &tablet_id,
                                             const int64_t timestamp,
                                             ObTransID &block_tx_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(timestamp < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tablet_id), K(timestamp));
  } else if (OB_FAIL(mgr_->check_modify_time_elapsed(tablet_id,
                                                     timestamp,
                                                     block_tx_id))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check modify time elapsed failed", K(ret),
                K(tablet_id), K(timestamp));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLSTxService::iterate_tx_obj_lock_op(ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->iterate_tx_obj_lock_op(iter))) {
    TRANS_LOG(WARN, "get tx obj lock op iter failed", K(ret));
  } else if (OB_FAIL(iter.set_ready())) {
    TRANS_LOG(WARN, "iter set ready failed", K(ret));
  } else {
    TRANS_LOG(INFO, "iter set ready success", K(ret));
  }
  return ret;
}

int ObLSTxService::iterate_tx_ctx(ObLSTxCtxIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(iter.set_ready(mgr_))) {
    TRANS_LOG(WARN, "get tx obj lock op iter failed", K(ret), K_(ls_id));
  } else {
    TRANS_LOG(INFO, "iter set ready success", K(ret), K_(ls_id));
  }
  return ret;
}

int ObLSTxService::replay(const void *buffer,
                          const int64_t nbytes,
                          const palf::LSN &lsn,
                          const SCN &scn)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  int64_t tmp_pos = 0;
  const char *log_buf = static_cast<const char *>(buffer);
  if (OB_ISNULL(parent_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", KP(parent_));
  } else if (OB_FAIL(base_header.deserialize(log_buf, nbytes, tmp_pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else if (OB_FAIL(ObTxReplayExecutor::execute(parent_, this, log_buf, nbytes,
                                                 tmp_pos, lsn, scn,
                                                 base_header,
                                                 ls_id_))) {
    LOG_WARN("replay tx log error", K(ret), K(lsn), K(scn));
  }
  return ret;
}

int ObLSTxService::traverse_trans_to_submit_redo_log(ObTransID &fail_tx_id, const uint32_t freeze_clock)
{
  return mgr_->traverse_tx_to_submit_redo_log(fail_tx_id, freeze_clock);
}
int ObLSTxService::traverse_trans_to_submit_next_log() { return mgr_->traverse_tx_to_submit_next_log(); }

ObTxLSLogWriter *ObLSTxService::get_tx_ls_log_writer() { return mgr_->get_ls_log_writer(); }

ObITxLogAdapter *ObLSTxService::get_tx_ls_log_adapter() { return mgr_->get_ls_log_adapter(); }

int ObLSTxService::replay_start_working_log(const ObTxStartWorkingLog &log, SCN &log_ts_ns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K(ls_id_));
  } else if (OB_FAIL(mgr_->replay_start_working_log(log, log_ts_ns))) {
    TRANS_LOG(WARN, "replay start working log failed", KR(ret), K(log), K(ls_id_));
  }
  return ret;
}

void ObLSTxService::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K(ls_id_));
  } else if (OB_FAIL(mgr_->switch_to_follower_forcedly())) {
    TRANS_LOG(ERROR, "switch to follower forcedly failed", KR(ret), K(ls_id_));
  }
  // TRANS_LOG(INFO, "[ObLSTxService] switch_to_follower_forcedly", KR(ret), K(ls_id_));
  return;
}

int ObLSTxService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K(ls_id_));
  } else if (OB_FAIL(mgr_->switch_to_leader())) {
    TRANS_LOG(WARN, "switch to leader failed", KR(ret), K(ls_id_));
  }
  // TRANS_LOG(INFO, "[ObLSTxService] switch_to_leader", KR(ret), K(ls_id_));
  return ret;
}

int ObLSTxService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K(ls_id_));
  } else if (OB_FAIL(mgr_->switch_to_follower_gracefully())) {
    TRANS_LOG(WARN, "switch to follower gracefully failed", KR(ret), K(ls_id_));
  }
  // TRANS_LOG(INFO, "[ObLSTxService] switch_to_follower_gracefully", KR(ret), K(ls_id_));
  return ret;
}

int ObLSTxService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K(ls_id_));
  } else if (OB_FAIL(mgr_->resume_leader())) {
    TRANS_LOG(WARN, "resume leader failed", KR(ret), K(ls_id_));
  }
  // TRANS_LOG(INFO, "[ObLSTxService] resume_leader", KR(ret), K(ls_id_));
  return ret;
}

inline
void get_min_rec_scn_common_checkpoint_type_by_index_(int index,
                                                      char *common_checkpoint_type)
{
  int ret = OB_SUCCESS;
  if (index == 0) {
    strncpy(common_checkpoint_type, "ALL_EMPTY", common::MAX_CHECKPOINT_TYPE_BUF_LENGTH);
  } else if (OB_FAIL(common_checkpoint_type_to_string(ObCommonCheckpointType(index),
                                              common_checkpoint_type,
                                              common::MAX_CHECKPOINT_TYPE_BUF_LENGTH))) {
    TRANS_LOG(WARN, "common_checkpoint_type_to_string failed", K(index), K(ret));
    strncpy(common_checkpoint_type,
            "UNKNOWN_COMMON_CHECKPOINT_TYPE",
            common::MAX_CHECKPOINT_TYPE_BUF_LENGTH);
  }
}

SCN ObLSTxService::get_rec_scn()
{
  SCN min_rec_scn = SCN::max_scn();
  int min_rec_scn_common_checkpoint_type_index = 0;
  char common_checkpoint_type[common::MAX_CHECKPOINT_TYPE_BUF_LENGTH];
  RLockGuard guard(rwlock_);
  for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    if (OB_NOT_NULL(common_checkpoints_[i])) {
      SCN rec_scn = common_checkpoints_[i]->get_rec_scn();
      if (rec_scn.is_valid() && rec_scn < min_rec_scn) {
        min_rec_scn = rec_scn;
        min_rec_scn_common_checkpoint_type_index = i;
      }
    }
  }
  get_min_rec_scn_common_checkpoint_type_by_index_(min_rec_scn_common_checkpoint_type_index,
                                                   common_checkpoint_type);

  TRANS_LOG(INFO, "[CHECKPOINT] ObLSTxService::get_rec_scn",
            K(common_checkpoint_type),
            KPC(common_checkpoints_[min_rec_scn_common_checkpoint_type_index]),
            K(min_rec_scn), K(ls_id_));

  return min_rec_scn;
}

int ObLSTxService::flush(SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_gen_diagnose_trace = false;
  RLockGuard guard(rwlock_);
  int64_t trace_id = INVALID_TRACE_ID;
  for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    // only flush the common_checkpoint that whose clog need recycle
    if (OB_NOT_NULL(common_checkpoints_[i])
        && !common_checkpoints_[i]->is_flushing()
        && recycle_scn >= common_checkpoints_[i]->get_rec_scn()) {
      if (!has_gen_diagnose_trace) {
        has_gen_diagnose_trace = true;
        MTL(ObCheckpointDiagnoseMgr*)->acquire_trace_id(ls_id_, trace_id);
      }
      TRANS_LOG(INFO,
                "common_checkpoints flush",
                K(i),
                K(trace_id),
                K(ls_id_),
                K(has_gen_diagnose_trace),
                K(common_checkpoints_[i]));
      if (OB_SUCCESS != (tmp_ret = common_checkpoints_[i]->flush(recycle_scn, trace_id))) {
        TRANS_LOG(WARN, "obCommonCheckpoint flush failed", K(tmp_ret), K(common_checkpoints_[i]));
      }
    }
  }
  return ret;
}

int ObLSTxService::flush_ls_inner_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_ls_inner_tablet()) {
    TRANS_LOG(INFO, "not a ls inner tablet", KR(ret), K(tablet_id));
  } else {
    for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
      if (OB_NOT_NULL(common_checkpoints_[i]) && common_checkpoints_[i]->get_tablet_id() == tablet_id &&
          OB_FAIL(common_checkpoints_[i]->flush(SCN::max_scn(), true))) {
        TRANS_LOG(WARN, "obCommonCheckpoint flush failed", KR(ret), KP(common_checkpoints_[i]));
        break;
      }
    }
  }
  return ret;
}

int ObLSTxService::get_common_checkpoint_info(
    ObIArray<ObCommonCheckpointVTInfo> &common_checkpoint_array)
{
  int ret = OB_SUCCESS;
  common_checkpoint_array.reset();
  RLockGuard guard(rwlock_);
  for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    ObCommonCheckpoint *common_checkpoint = common_checkpoints_[i];
    if (OB_ISNULL(common_checkpoint)) {
      // ignore ret
      TRANS_LOG(WARN, "the common_checkpoint should not be null", K(i));
    } else {
      ObCommonCheckpointVTInfo info;
      info.rec_scn = common_checkpoint->get_rec_scn(info.tablet_id);
      info.checkpoint_type = i;
      info.is_flushing = common_checkpoint->is_flushing();
      common_checkpoint_array.push_back(info);
    }
  }

  return ret;
}

int ObLSTxService::register_common_checkpoint(const ObCommonCheckpointType &type,
                                              ObCommonCheckpoint* common_checkpoint)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type) || NULL == common_checkpoint) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(type), K(common_checkpoint));
  } else {
    WLockGuard guard(rwlock_);
    if (OB_NOT_NULL(common_checkpoints_[type])) {
      STORAGE_LOG(WARN, "repeat register common_checkpoint", K(ret), K(type), K(common_checkpoint));
    } else {
      common_checkpoints_[type] = common_checkpoint;
    }
  }

  return ret;
}

int ObLSTxService::unregister_common_checkpoint(const ObCommonCheckpointType &type,
                                                const ObCommonCheckpoint* common_checkpoint)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type) || OB_ISNULL(common_checkpoint)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(type), K(common_checkpoint));
  } else {
    WLockGuard guard(rwlock_);
    if (OB_ISNULL(common_checkpoints_[type])) {
      // ignore ret
      STORAGE_LOG(WARN, "common_checkpoint is null, no need unregister", K(type),
                  K(common_checkpoint));
    } else if (common_checkpoints_[type] != common_checkpoint) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "common checkpoint not equal, not unregister", K(type),
                  K(common_checkpoints_[type]), K(common_checkpoint));
    } else {
      common_checkpoints_[type] = nullptr;
    }
  }

  return ret;
}

int ObLSTxService::check_in_leader_serving_state(bool& bool_ret)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObLSTxService::mgr_ is nullptr", K(ls_id_));
  } else {
    bool_ret = mgr_->in_leader_serving_state();
  }
  return ret;
}

int ObLSTxService::traversal_flush()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    if (OB_NOT_NULL(common_checkpoints_[i]) &&
        OB_SUCCESS != (tmp_ret = common_checkpoints_[i]->flush(SCN::max_scn(), checkpoint::INVALID_TRACE_ID, false))) {
      TRANS_LOG(WARN, "obCommonCheckpoint flush failed", K(tmp_ret), KP(common_checkpoints_[i]));
    }
  }
  return ret;
}


void ObLSTxService::reset_() {
  WLockGuard guard(rwlock_);
  for (int i = 0; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    common_checkpoints_[i] = NULL;
  }
}

SCN ObLSTxService::get_ls_weak_read_ts() {
  return parent_->get_ls_wrs_handler()->get_ls_weak_read_ts();
}

ObTxRetainCtxMgr *ObLSTxService::get_retain_ctx_mgr()
{
  ObTxRetainCtxMgr *retain_ptr = nullptr;
  if (OB_ISNULL(mgr_)) {
    retain_ptr = nullptr;
  } else {
    retain_ptr = &mgr_->get_retain_ctx_mgr();
  }
  return retain_ptr;
}

int ObLSTxService::prepare_offline(const int64_t start_ts)
{
  int ret = OB_SUCCESS;
  const int64_t PRINT_LOG_INTERVAL = 1000 * 1000; // 1s
  const int64_t WAIT_READONLY_REQUEST_US = 60 * 1000 * 1000;
  bool unused_is_all_tx_clean_up = false;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->block_all(unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "block all failed", K_(ls_id));
  } else if (ObTimeUtility::current_time() > start_ts + WAIT_READONLY_REQUEST_US) {
    // dont care readonly request
  } else {
    const int64_t readonly_request_cnt = mgr_->get_total_active_readonly_request_count();
    const int64_t request_by_transfer_dest = mgr_->get_total_request_by_transfer_dest();
    if (readonly_request_cnt > 0) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        TRANS_LOG(WARN, "readonly requests are active", K(ret), KP(mgr_), K_(ls_id), K(readonly_request_cnt));
      }
    } else if (request_by_transfer_dest > 0) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        TRANS_LOG(WARN, "request by transfer_dest", K(ret), KP(mgr_), K_(ls_id), K(request_by_transfer_dest));
      }
    }
  }
  TRANS_LOG(INFO, "prepare offline ls", K(ret), K(start_ts), KP(mgr_), K_(ls_id));
  return ret;
}

int ObLSTxService::offline()
{
  int ret = OB_SUCCESS;
  const int64_t PRINT_LOG_INTERVAL = 1000 * 1000; // 1s
  const bool graceful = false;
  bool unused_is_all_tx_clean_up = false;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->block_all(unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "block all failed", K_(ls_id));
  } else if (OB_FAIL(mgr_->kill_all_tx(graceful, unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "kill_all_tx failed", K_(ls_id));
  } else if (OB_FAIL(MTL(ObTransService *)->get_ts_mgr()->interrupt_gts_callback_for_ls_offline(MTL_ID(),
        ls_id_))) {
    TRANS_LOG(WARN, "interrupt gts callback failed", KR(ret), K_(ls_id));
  } else if (mgr_->get_tx_ctx_count() > 0) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      TRANS_LOG(WARN, "transaction not empty, try again", K(ret), KP(mgr_), K_(ls_id), K(mgr_->get_tx_ctx_count()));
    }
  }
  return ret;
}

int ObLSTxService::online()
{
  int ret = OB_SUCCESS;
  // need reset block.
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->online())) {
    TRANS_LOG(WARN, "ls tx service online failed", K(ret), K_(ls_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObLSTxService::block_normal()
{
  int ret = OB_SUCCESS;
  bool unused_is_all_tx_clean_up = false;

  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->block_normal(unused_is_all_tx_clean_up))) {
    TRANS_LOG(WARN, "block normal tx failed", K_(ls_id));
  }
  return ret;
}

int ObLSTxService::unblock_normal()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if (OB_FAIL(mgr_->unblock_normal())) {
    TRANS_LOG(WARN, "ls tx service unblock normal failed", K(ret), K_(ls_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObLSTxService::get_tx_ctx_count(int64_t &tx_ctx_count)
{
  int ret = OB_SUCCESS;
  tx_ctx_count = -1;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    tx_ctx_count = mgr_->get_tx_ctx_count();
  }
  return ret;
}

int ObLSTxService::get_active_tx_count(int64_t &active_tx_count)
{
  int ret = OB_SUCCESS;
  active_tx_count = -1;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    active_tx_count = mgr_->get_active_tx_count();
  }
  return ret;
}

int ObLSTxService::print_all_tx_ctx(const int64_t print_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    const bool verbose = true;
    mgr_->print_all_tx_ctx(print_num, verbose);
  }
  return ret;
}

int ObLSTxService::retry_apply_start_working_log()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    ret = mgr_->retry_apply_start_working_log();
  }
  return ret;
}

int ObLSTxService::set_max_replay_commit_version(share::SCN commit_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    mgr_->update_max_replay_commit_version(commit_version);
    MTL(ObTransService *)->get_tx_version_mgr().update_max_commit_ts(commit_version, false /*elr*/);
    TRANS_LOG(INFO, "succ set max_replay_commit_version", K(commit_version));
  }
  return ret;
}

int ObLSTxService::check_tx_blocked(bool &tx_blocked) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else {
    tx_blocked = mgr_->is_tx_blocked();
  }
  return ret;
}
int ObLSTxService::filter_tx_need_transfer(ObIArray<ObTabletID> &tablet_list,
                                           const share::SCN data_end_scn,
                                           ObIArray<transaction::ObTransID> &move_tx_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mgr_->filter_tx_need_transfer(tablet_list, data_end_scn, move_tx_ids))) {
    TRANS_LOG(WARN, "for each tx ctx error", KR(ret));
  }
  return ret;
}

int ObLSTxService::transfer_out_tx_op(const ObTransferOutTxParam &param,
                                      int64_t &active_tx_count,
                                      int64_t &op_tx_count)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(mgr_->transfer_out_tx_op(param, active_tx_count, op_tx_count))) {
    TRANS_LOG(WARN, "for each tx ctx error", KR(ret));
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("transfer_out_tx_op", KR(ret), "cost", end_time - start_time, K(active_tx_count), K(op_tx_count));
  return ret;
}

int ObLSTxService::wait_tx_write_end(ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(mgr_->wait_tx_write_end(timeout_ctx))) {
    TRANS_LOG(WARN, "for each tx ctx error", KR(ret));
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("wait_tx_write_end", KR(ret), "cost", end_time - start_time);
  return ret;
}

int ObLSTxService::collect_tx_ctx(const ObLSID dest_ls_id,
                                  const SCN log_scn,
                                  const ObIArray<ObTabletID> &tablet_list,
                                  const ObIArray<ObTransID> &move_tx_ids,
                                  int64_t &collect_count,
                                  ObIArray<ObTxCtxMoveArg> &res)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(mgr_->collect_tx_ctx(dest_ls_id, log_scn, tablet_list, move_tx_ids, collect_count, res))) {
    TRANS_LOG(WARN, "for each tx ctx error", KR(ret));
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("collect_tx_ctx", KR(ret), K(ls_id_), "cost_us", end_time - start_time, K(collect_count));
  return ret;
}

int ObLSTxService::move_tx_op(const ObTransferMoveTxParam &move_tx_param,
                              const ObIArray<ObTxCtxMoveArg> &args)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(mgr_->move_tx_op(move_tx_param, args))) {
    TRANS_LOG(WARN, "for each tx ctx error", KR(ret));
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("move_tx_ctx", KR(ret), K(ls_id_),"cost_us", end_time - start_time,
      "count", args.count());
  return ret;

}


} // transaction
} // oceanbase
