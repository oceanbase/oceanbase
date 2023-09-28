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
#include "storage/ls/ob_ls.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_replay_executor.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_retain_ctx_mgr.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"

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
    ret = trans_service_->get_write_store_ctx(tx, snapshot, write_flag, store_ctx, spec_seq_no, false);
  }
  return ret;
}

int ObLSTxService::revert_store_ctx(storage::ObStoreCtx &store_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = trans_service_->revert_store_ctx(store_ctx);
  }
  // ignore ret
  if (store_ctx.is_read_store_ctx()) {
    if (OB_ISNULL(mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "mgr is null", K(ret), KP(this));
    } else {
      (void)mgr_->end_readonly_request();
    }
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
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), K_(ls_id));
  } else if ((active_readonly_request_count = mgr_->get_total_active_readonly_request_count()) > 0) {
    if (REACH_TIME_INTERVAL(5000000)) {
      TRANS_LOG(INFO, "readonly requests are active", K(active_readonly_request_count));
      mgr_->dump_readonly_request(3);
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
                                                 base_header.get_replay_hint(),
                                                 ls_id_, parent_->get_tenant_id()))) {
    LOG_WARN("replay tx log error", K(ret), K(lsn), K(scn));
  }
  return ret;
}

int ObLSTxService::traverse_trans_to_submit_redo_log(ObTransID &fail_tx_id)
{
  return mgr_->traverse_tx_to_submit_redo_log(fail_tx_id);
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
  RLockGuard guard(rwlock_);
  for (int i = 1; i < ObCommonCheckpointType::MAX_BASE_TYPE; i++) {
    // only flush the common_checkpoint that whose clog need recycle
    if (OB_NOT_NULL(common_checkpoints_[i]) && recycle_scn >= common_checkpoints_[i]->get_rec_scn()) {
      if (OB_SUCCESS != (tmp_ret = common_checkpoints_[i]->flush(recycle_scn))) {
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
      STORAGE_LOG(WARN, "common_checkpoint is null, no need unregister", K(type),
                  K(common_checkpoint));
    } else if (common_checkpoints_[type] != common_checkpoint) {
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
        OB_SUCCESS != (tmp_ret = common_checkpoints_[i]->flush(SCN::max_scn(), false))) {
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
    if (readonly_request_cnt > 0) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        TRANS_LOG(WARN, "readonly requests are active", K(ret), KP(mgr_), K_(ls_id), K(readonly_request_cnt));
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
} // transaction

} // oceanbase
