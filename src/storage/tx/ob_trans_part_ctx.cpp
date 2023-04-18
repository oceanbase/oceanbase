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
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include <cstdint>
#define USING_LOG_PREFIX TRANS
#include "ob_trans_part_ctx.h"

#include "common/storage/ob_sequence.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/utility/serialization.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_ts_mgr.h"
#include "ob_tx_log.h"
#include "ob_tx_msg.h"
#include "lib/worker.h"
#include "share/rc/ob_context.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx/ob_trans_service.h"
#include "share/ob_alive_server_tracer.h"

namespace oceanbase {

using namespace common;
using namespace memtable;
using namespace share;
using namespace sql;
using namespace storage;
using namespace palf;

namespace transaction {
using namespace tablelock;
typedef ObRedoLogSyncResponseMsg::ObRedoLogSyncResponseStatus ObRedoLogSyncResponseStatus;

#define INC_ELR_STATISTIC(item) \
  do {                          \
  } while (0);

bool ObPartTransCtx::is_inited() const { return ATOMIC_LOAD(&is_inited_); }

int ObPartTransCtx::init(const uint64_t tenant_id,
                         const common::ObAddr &scheduler,
                         const uint32_t session_id,
                         const ObTransID &trans_id,
                         const int64_t trans_expired_time,
                         const ObLSID &ls_id,
                         const uint64_t cluster_version,
                         ObTransService *trans_service,
                         const uint64_t cluster_id,
                         const int64_t epoch,
                         const bool can_elr,
                         ObLSTxCtxMgr *ls_ctx_mgr,
                         const bool for_replay)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  // default init : just reset immediately
  default_init_();

  // specified init : initialize with specified value
  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!trans_id.is_valid())
             || OB_UNLIKELY(trans_expired_time <= 0) || OB_UNLIKELY(cluster_version <= 0)
             || OB_ISNULL(trans_service) || OB_UNLIKELY(!ls_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(epoch), K(cluster_version),
              KP(trans_service), K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ls_id_ = ls_id;
    ls_tx_ctx_mgr_ = ls_ctx_mgr;
    trans_service_ = trans_service;
    if (OB_FAIL(lock_.init(this))) {
      TRANS_LOG(WARN, "init lock error", KR(ret));
    } else if (OB_ISNULL(rpc_ = trans_service->get_trans_rpc())
               || OB_ISNULL(timer_ = &(trans_service->get_trans_timer()))) {
      TRANS_LOG(ERROR, "ObTransService is invalid, unexpected error", KP(rpc_));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(timeout_task_.init(this))) {
      TRANS_LOG(WARN, "timeout task init error", KR(ret));
    } else if (OB_FAIL(init_memtable_ctx_(tenant_id, ls_id))) {
      TRANS_LOG(WARN, "ObPartTransCtx init memtable context error", KR(ret), K(trans_id), K(ls_id));
    } else if (OB_FAIL(clog_encrypt_info_.init())) {
      TRANS_LOG(WARN, "init clog encrypt info failed", K(ret), K(trans_id));
    } else if (OB_FAIL(init_log_cbs_(ls_id, trans_id))) {
      TRANS_LOG(WARN, "init log cbs failed", KR(ret), K(trans_id), K(ls_id));
    } else if (OB_FAIL(ctx_tx_data_.init(ls_ctx_mgr, trans_id))) {
      TRANS_LOG(WARN, "init ctx tx data failed",K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    session_id_ = session_id;
    addr_ = trans_service->get_server();
    trans_id_ = trans_id;
    trans_expired_time_ = trans_expired_time;
    ctx_create_time_ = ObClockGenerator::getClock();
    cluster_version_ = cluster_version;
    part_trans_action_ = ObPartTransAction::INIT;
    trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;

    exec_info_.scheduler_ = scheduler;
    exec_info_.trans_type_ = TransType::SP_TRANS;
    last_ask_scheduler_status_ts_ = 0;
    cluster_id_ = cluster_id;
    epoch_ = epoch;
    can_elr_ = can_elr;
    pending_write_ = 0;
    set_role_state(for_replay);

    if (is_follower_()) {
      mt_ctx_.trans_replay_begin();
    } else {
      mt_ctx_.trans_begin();
    }

    mt_ctx_.set_trans_ctx(this);
    mt_ctx_.set_for_replay(is_follower_());

    if (!GCONF.enable_sql_audit) {
      tlog_ = NULL;
    } else if (OB_ISNULL(tlog_ = ObTransTraceLogFactory::alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObTransTraceLog error", KR(ret), KP_(tlog));
    } else {
      // do nothing
    }

    is_inited_ = true;
  }

  REC_TRANS_TRACE_EXT2(tlog_, init,
                       OB_ID(addr), (void*)this,
                       OB_ID(id), ls_id_.id(),
                       OB_ID(trans_id), trans_id,
                       OB_ID(ctx_ref), get_ref());
  TRANS_LOG(TRACE, "part trans ctx init", K(ret), K(tenant_id), K(trans_id), K(trans_expired_time),
            K(ls_id), K(cluster_version), KP(trans_service), K(cluster_id), K(epoch), K(can_elr));
  return ret;
}

int ObPartTransCtx::init_memtable_ctx_(const uint64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 lock_memtable;
  if (OB_FAIL(mt_ctx_.init(tenant_id))) {
    TRANS_LOG(WARN, "memtable context init fail", KR(ret));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_lock_memtable(lock_memtable))) {
    TRANS_LOG(WARN, "get lock_memtable fail", KR(ret));
  } else if (OB_FAIL(mt_ctx_.enable_lock_table(lock_memtable))) {
    TRANS_LOG(WARN, "enable_lock_table fail", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table_guard(*(mt_ctx_.get_tx_table_guard())))) {
    TRANS_LOG(WARN, "get tx_table guard fail", KR(ret), KPC(this));
  } else {
    // the elr_handler.mt_ctx_ is used to notify the lock_wait_mgr for early lock release txn
    elr_handler_.set_memtable_ctx(&mt_ctx_);
  }
  TRANS_LOG(DEBUG, "init memtable ctx", KR(ret), K(ls_id), K(tenant_id), K(trans_id_));
  return ret;
}

void ObPartTransCtx::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    TRANS_LOG(DEBUG, "part_ctx_destroy", K(*this));

#ifdef ENABLE_DEBUG_LOG
    if (NULL != trans_service_->get_defensive_check_mgr()) {
      trans_service_->get_defensive_check_mgr()->del(trans_id_);
    }
#endif

    // Defensive Check 1 : earse ctx id descriptor
    mt_ctx_.reset();

    REC_TRANS_TRACE_EXT2(tlog_, destroy);

    // Defensive Check 2 : apply service callback
    if (!busy_cbs_.is_empty()) {
      TRANS_LOG(ERROR, "some BUG may happen !!!", K(lbt()), K(*this), K(trans_id_),
                K(busy_cbs_.get_size()));
    }

    ctx_tx_data_.destroy();

    if (NULL == ls_tx_ctx_mgr_) {
      TRANS_LOG(ERROR, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context",
                *this);
    } else {
      ls_tx_ctx_mgr_->dec_total_tx_ctx_count();
    }
    // Defensive Check 3 : missing to callback scheduler
    if (!is_follower_() && need_callback_scheduler_()) {
      int ret = OB_TRANS_UNKNOWN;
      if (0 == start_working_log_ts_) {
        TRANS_LOG(ERROR, "missing callback scheduler, callback with TRANS_UNKNOWN", K(ret), KPC(this));
      } else {
        TRANS_LOG(WARN, "missing callback scheduler maybe, callback with TRANS_UNKNOWN", K(ret), KPC(this));
      }
      // NOTE: callback scheduler may introduce deadlock, need take care
      trans_service_->handle_tx_commit_result(trans_id_, OB_TRANS_UNKNOWN, -1);
      FORCE_PRINT_TRACE(tlog_, "[missing callback scheduler] ");
    }

    if (NULL != tlog_) {
      print_trace_log_if_necessary_();
      ObTransTraceLogFactory::release(tlog_);
      tlog_ = NULL;
    }

    mds_cache_.destroy();
    exec_info_.destroy();

    timeout_task_.destroy();
    clog_encrypt_info_.destroy();
    trace_info_.reset();
    is_inited_ = false;
  }
}

void ObPartTransCtx::default_init_()
{
  // TODO for ObTransCtx
  // lock_.reset();
  stc_.reset();
  commit_cb_.reset();
  pending_callback_param_ = OB_SUCCESS;
  trans_need_wait_wrap_.reset();
  is_exiting_ = false;
  for_replay_ = false;
  has_pending_callback_ = false;

  request_id_ = OB_INVALID_TIMESTAMP;
  session_id_ = 0;
  timeout_task_.reset();
  trace_info_.reset();

  // TODO ObPartTransCtx
  clog_encrypt_info_.reset();
  ObTxCycleTwoPhaseCommitter::reset();
  is_inited_ = false;
  mt_ctx_.reset();
  end_log_ts_ = INT64_MAX;
  trans_expired_time_ = INT64_MAX;
  stmt_expired_time_ = INT64_MAX;
  cur_query_start_time_ = 0;
  clean_retain_cause_();

  upstream_state_ = ObTxState::INIT;
  exec_info_.reset();
  ctx_tx_data_.reset();
  sub_state_.reset();
  reset_log_cbs_();
  last_op_sn_ = 0;
  last_scn_ = 0;
  first_scn_ = 0;
  rec_log_ts_ = OB_INVALID_TIMESTAMP;
  prev_rec_log_ts_ = OB_INVALID_TIMESTAMP;
  is_ctx_table_merged_ = false;
  mds_cache_.reset();
  start_replay_ts_ = 0;
  is_incomplete_replay_ctx_ = false;
  is_submitting_redo_log_for_freeze_ = false;
  start_working_log_ts_ = 0;
  max_2pc_commit_scn_ = OB_INVALID_TIMESTAMP;
  coord_prepare_info_arr_.reset();
  reserve_allocator_.reset();
  elr_handler_.reset();
}

int ObPartTransCtx::init_log_cbs_(const ObLSID &ls_id, const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < OB_TX_MAX_LOG_CBS; ++i) {
    if (OB_FAIL(log_cbs_[i].init(ls_id, tx_id, this))) {
      TRANS_LOG(WARN, "log cb init failed", KR(ret));
    } else if (!free_cbs_.add_last(&log_cbs_[i])) {
      ret = OB_INIT_FAIL;
      TRANS_LOG(WARN, "add to free list failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(final_log_cb_.init(ls_id, tx_id, this))) {
      TRANS_LOG(WARN, "init commit log cb failed", K(ret));
    } else {
      TRANS_LOG(DEBUG, "init commit log cb success", K(ret), KP(&final_log_cb_), K(*this));
    }
  }
  return ret;
}

void ObPartTransCtx::reset_log_cbs_()
{
  for (int64_t i = 0; i < OB_TX_MAX_LOG_CBS; ++i) {
    if (OB_NOT_NULL(log_cbs_[i].get_tx_data())) {
      ObTxData *tx_data = log_cbs_[i].get_tx_data();
      ctx_tx_data_.free_tmp_tx_data(tx_data);
      log_cbs_[i].set_tx_data(nullptr);
    }
    log_cbs_[i].reset();
  }
  if (OB_NOT_NULL(final_log_cb_.get_tx_data())) {
    ObTxData *tx_data = final_log_cb_.get_tx_data();
    ctx_tx_data_.free_tmp_tx_data(tx_data);
    final_log_cb_.set_tx_data(nullptr);
  }
  final_log_cb_.reset();
  free_cbs_.reset();
  busy_cbs_.reset();
}

// thread-unsafe
int ObPartTransCtx::start_trans()
{
  int ret = OB_SUCCESS;
  const int64_t left_time = trans_expired_time_ - ObClockGenerator::getRealClock();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
    // leader expired before init ctx
  } else if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(left_time <= 0)) {
    ret = OB_TRANS_TIMEOUT;
    TRANS_LOG(WARN, "transaction is timeout", K(ret), K_(trans_expired_time), KPC(this));
  } else {
    part_trans_action_ = ObPartTransAction::START;
    (void)unregister_timeout_task_();
    if (OB_FAIL(register_timeout_task_(left_time))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), "context", *this);
    }
  }
  if (OB_FAIL(ret)) {
    set_exiting_();
    (void)unregister_timeout_task_();
  }
  TRANS_LOG(DEBUG, "start trans", K(ret), K(trans_id_), "ctx_ref", get_ref());
  REC_TRANS_TRACE_EXT2(tlog_, start_trans, OB_ID(ret), ret, OB_ID(left_time), left_time, OB_ID(ctx_ref),
                       get_ref());

  return ret;
}

int ObPartTransCtx::trans_kill_()
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "trans killed", K(trans_id_));

  mt_ctx_.set_tx_rollbacked();

  if (ctx_tx_data_.get_state() == ObTxData::RUNNING) {
    if (OB_FAIL(ctx_tx_data_.set_state(ObTxData::ABORT))) {
      TRANS_LOG(WARN, "set abort state in ctx_tx_data_ failed", K(ret));
    }
  }

  mt_ctx_.trans_kill();

  return ret;
}

int ObPartTransCtx::trans_clear_()
{
  int ret = OB_SUCCESS;

  // For the purpose of the durability(ACID) of the tx ctx, we need to store the
  // rec_log_ts even after the tx ctx has been released. So we decide to store
  // it into aggre_rec_log_ts in the ctx_mgr.
  //
  // While To meet the demand, we should obey two rules:
  // 1. We must push rec_log_ts to aggre_rec_log_ts before tx ctx has been
  //    removed from ctx_mgr(in trans_clear_)
  // 2. We must disallow dump tx_ctx_table after we already push rec_log_ts(in
  //    get_tx_ctx_table_info)
  //
  // What's more, we need not to care about the retain tx_ctx, because it has
  // already meet the durability requirement and is just used for multi-source
  // data.
  if (is_ctx_table_merged_
      && OB_FAIL(ls_tx_ctx_mgr_->update_aggre_log_ts_wo_lock(get_rec_log_ts_()))) {
    TRANS_LOG(ERROR, "update aggre log ts wo lock failed", KR(ret), "context", *this);
  } else {
    ret = mt_ctx_.trans_clear();
  }

  return ret;
}

int ObPartTransCtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();
  bool tx_expired = is_trans_expired_();
  bool commit_expired = now > stmt_expired_time_;
  common::ObTimeGuard timeguard("part_handle_timeout", 10 * 1000);
  if (OB_SUCC(lock_.lock(5000000 /*5 seconds*/))) {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    timeguard.click();
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (!timeout_task_.is_registered()) {
      // timer task is canceled, do nothing
    } else {
      timeguard.click();
      (void)unregister_timeout_task_();
      update_trans_2pc_timeout_();
      timeout_task_.set_running(true);
      if (ObTxState::INIT == exec_info_.state_ &&
          trans_expired_time_ > 0 && trans_expired_time_ < INT64_MAX &&
          now >= (trans_expired_time_ + OB_TRANS_WARN_USE_TIME)) {
        TRANS_LOG(ERROR, "transaction use too much time", KPC(this));
      }
      if (exec_info_.is_dup_tx_) {
        if (ObTxState::REDO_COMPLETE == exec_info_.state_) {
          if (OB_SUCCESS != (tmp_ret = dup_table_tx_redo_sync_())) {
            TRANS_LOG(WARN, "dup table tx redo sync error", K(tmp_ret));
          }
        } else if (ObTxState::PRE_COMMIT == exec_info_.state_) {
          if (OB_SUCCESS != (tmp_ret = dup_table_tx_pre_commit_())) {
            TRANS_LOG(WARN, "dup table tx pre commit error", K(tmp_ret));
          }
        }
      }

      // handle commit timeout on root node
      if (!is_sub2pc() && !is_follower_() && is_root() && part_trans_action_ == ObPartTransAction::COMMIT) {
        if (tx_expired) {
          tmp_ret = post_tx_commit_resp_(OB_TRANS_TIMEOUT);
          TRANS_LOG(INFO, "callback scheduler txn has timeout", K(tmp_ret), KPC(this));
        } else if (commit_expired) {
          tmp_ret = post_tx_commit_resp_(OB_TRANS_STMT_TIMEOUT);
          TRANS_LOG(INFO, "callback scheduler txn commit has timeout", K(tmp_ret), KPC(this));
        }
      }

      // go to preapre state when recover from redo complete
      if (!is_follower_() && !exec_info_.is_dup_tx_ && !is_sub2pc()) {
        if (ObTxState::REDO_COMPLETE == get_downstream_state()) {
          if (is_local_tx_()) {
            if (!is_logging_()) {
              if (OB_FAIL(one_phase_commit_())) {
                TRANS_LOG(WARN, "two phase commit failed", KR(ret), KPC(this));
              } else {
                part_trans_action_ = ObPartTransAction::COMMIT;
              }
            }
          } else {
            if (ObTxState::PREPARE > get_upstream_state()) {
              bool unused = false;
              if (is_2pc_logging()) {
                TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
              } else if (OB_FAIL(do_prepare(unused))) {
                TRANS_LOG(WARN, "do prepare failed", K(ret), K(*this));
              } else {
                set_upstream_state(ObTxState::PREPARE);
                collected_.reset();
                part_trans_action_ = ObPartTransAction::COMMIT;
              }
            }
          }
        }
      }

      // retry commiting for every node
      if (!is_follower_() && is_committing_()) {
        if (is_local_tx_()) {
          try_submit_next_log_();
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::handle_timeout())) {
          TRANS_LOG(WARN, "handle 2pc timeout failed", KR(ret), KPC(this));
        }
      }

      // if not committing, abort txn if it was expired
      if (!is_follower_() && !is_committing_() && tx_expired) {
        if (OB_FAIL(abort_(OB_TRANS_TIMEOUT))) {
          TRANS_LOG(WARN, "abort failed", KR(ret), KPC(this));
        }
      }

      // register timeout task again if need
      if (!is_follower_() && !is_exiting_) {
        const int64_t timeout_left = is_committing_() ? trans_2pc_timeout_ : 
            MIN(MAX_TRANS_2PC_TIMEOUT_US, MAX(trans_expired_time_ - now, 1000 * 1000));
        if (OB_FAIL(register_timeout_task_(timeout_left))) {
          TRANS_LOG(WARN, "register timeout task failed", KR(ret), KPC(this));
        }
      }

      timeout_task_.set_running(false);
      timeguard.click();
    }
    REC_TRANS_TRACE_EXT2(tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(used), timeguard, OB_ID(ctx_ref),
                         get_ref());

    TRANS_LOG(INFO,
              "handle timeout",
              K(ret),
              K(*this),
              K(tx_expired),
              K(commit_expired),
              K(delay));
    if (busy_cbs_.get_size() > 0) {
      TRANS_LOG(INFO, "trx is waiting log_cb", K(busy_cbs_.get_size()), KPC(busy_cbs_.get_first()),
                KPC(busy_cbs_.get_last()));
    }
  } else {
    TRANS_LOG(WARN, "failed to acquire lock in specified time", K_(trans_id));
    unregister_timeout_task_();
    register_timeout_task_(delay);
  }

  return ret;
}

int ObPartTransCtx::kill(const KillTransArg &arg, ObIArray<ObTxCommitCallback> &cb_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int cb_param = OB_TRANS_UNKNOWN;

  common::ObTimeGuard timeguard("part_kill", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    TRANS_LOG(INFO, "trans is existing when being killed", K(*this), K(arg));
  } else {
    if (arg.graceful_) {
      if (is_in_2pc_()) {
        ret = OB_TRANS_CANNOT_BE_KILLED;
      } else if (has_persisted_log_() || is_logging_()) {
        // submit abort_log and wait success
        if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
          TRANS_LOG(WARN, "kill trx with abort_log failed", KR(ret), "context", *this);
        } else {
          TRANS_LOG(INFO, "kill trx with abort_log success", "context", *this);
        }
        if (OB_SUCC(ret)) {
          ret = OB_TRANS_CANNOT_BE_KILLED;
        }
      } else {
        cb_param = OB_TRANS_KILLED;
      }
      if (OB_SUCCESS != ret) {
        TRANS_LOG(INFO, "transaction can not be killed", KR(ret), "context", *this);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_local_tx_end_(TxEndAction::KILL_TX_FORCEDLY))) {
        TRANS_LOG(WARN, "kill trx forcedly failed", "context", *this, K(arg));
      }

      // if ctx was killed gracefully or forcely killed
      // notify scheduler commit result, if in committing
      if (is_root() && !is_follower_() && part_trans_action_ == ObPartTransAction::COMMIT) {
      // notify scheduler only if commit callback has not been armed
        if (commit_cb_.is_enabled() && !commit_cb_.is_inited()) {
          if (exec_info_.scheduler_ == addr_) {
            ObTxCommitCallback cb;
            cb.init(trans_service_, trans_id_, cb_param, -1);
            if (OB_FAIL(cb_array.push_back(cb))) {
              TRANS_LOG(WARN, "push commit callback fail", K(ret), KPC(this));
            } else {
              commit_cb_.disable();
            }
          } else {
            post_tx_commit_resp_(cb_param);
          }
        }
      }
    }
  }
  TRANS_LOG(WARN, "trans is killed", K(ret), K(arg), K(cb_param), KPC(this));
  REC_TRANS_TRACE_EXT2(tlog_, kill, OB_ID(ret), ret, OB_ID(arg1), arg.graceful_, OB_ID(used),
                       timeguard.get_diff(), OB_ID(ctx_ref), get_ref());
  return ret;
}

/*
 * commit - start to commiting txn
 *
 * the commiting is asynchronous
 *
 * @parts:          participant list
 * @commit_time:    STC reference
 * @expire_ts:      timestamp in micorseconds after which
 *                  commit result is not concerned for the caller
 * @app_trace_info: application level tracing infor
 *                  will be recorded in txn CommitLog
 * @request_id:     commit request identifier
 *
 * Return:
 * OB_SUCCESS - request was accepted and promise result would be
 *              reported via calling back the caller (either
 *              in message or in procedure call)
 * OB_TRANS_COMMITED - has committed
 * OB_TRANS_KILLED   - has aborted
 * OB_ERR_XXX - the request was rejected, can not be handle
 *              caller can retry commit or choice to abort txn
 */
int ObPartTransCtx::commit(const ObLSArray &parts,
                           const MonotonicTs &commit_time,
                           const int64_t &expire_ts,
                           const common::ObString &app_trace_info,
                           const int64_t &request_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 >= expire_ts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(expire_ts), KPC(this));
  } else if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is replaying", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_2pc_logging_())) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "tx is 2pc logging", KPC(this));
  } else if (!(ObTxState::INIT == get_downstream_state() ||
      (ObTxState::REDO_COMPLETE == get_downstream_state() && part_trans_action_ < ObPartTransAction::COMMIT))) {
    ObTxState state = get_downstream_state();
    switch (state) {
    case ObTxState::ABORT:
      ret = OB_TRANS_KILLED;
      break;
    case ObTxState::PRE_COMMIT:
    case ObTxState::COMMIT:
    case ObTxState::CLEAR:
      ret = OB_TRANS_COMMITED;
      break;
    case ObTxState::PREPARE:
    default:
      ret = OB_SUCCESS;
    }
    TRANS_LOG(WARN, "tx is committing", K(state), KPC(this));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "transaction is exiting", K(ret), KPC(this));
  } else if (OB_UNLIKELY(pending_write_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "access in progress", K(ret), K_(pending_write), KPC(this));
  } else if (OB_FAIL(set_2pc_participants_(parts))) {
    TRANS_LOG(WARN, "set participants failed", K(ret), KPC(this));
  } else if (OB_FAIL(set_2pc_request_id_(request_id))) {
    TRANS_LOG(WARN, "set request id failed", K(ret), K(request_id), KPC(this));
  } else if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), KPC(this));
  } else if (FALSE_IT(stmt_expired_time_ = expire_ts)) {
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timeout handler error", K(ret), KPC(this));
  } else if (OB_FAIL(register_timeout_task_(ObServerConfig::get_instance().trx_2pc_retry_interval
                                            + trans_id_.hash() % USEC_PER_SEC))) {
    TRANS_LOG(WARN, "register timeout handler error", K(ret), KPC(this));
  } else {
    if (commit_time.is_valid()) {
      set_stc_(commit_time);
    } else {
      set_stc_by_now_();
    }
    if (parts.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "the size of participant is 0 when commit", KPC(this));
    } else if (parts.count() == 1 && parts[0] == ls_id_) {
      exec_info_.trans_type_ = TransType::SP_TRANS;
      if (OB_FAIL(one_phase_commit_())) {
        TRANS_LOG(WARN, "start sp coimit fail", K(ret), KPC(this));
      }
    } else {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
      if (OB_FAIL(two_phase_commit())) {
        TRANS_LOG(WARN, "start dist coimit fail", K(ret), KPC(this));
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_trans_action_ = ObPartTransAction::COMMIT;
  }
  REC_TRANS_TRACE_EXT2(tlog_, commit, OB_ID(ret), ret,
                       OB_ID(thread_id), GETTID(),
                       OB_ID(ctx_ref), get_ref());
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "trx commit failed", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::one_phase_commit_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(do_local_tx_end_(TxEndAction::COMMIT_TX))) {
    TRANS_LOG(WARN, "do local commit tx failed", K(ret));
  }

  return ret;
}

int ObPartTransCtx::check_modify_schema_elapsed(
    const ObTabletID &tablet_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(lock_.lock(100000 /*100 ms*/))) {
    CtxLockGuard guard(lock_, false);

    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(schema_version <= 0) ||
               OB_UNLIKELY(!tablet_id.is_valid())) {
      TRANS_LOG(WARN, "invalid argument", K(tablet_id), K(schema_version),
                "context", *this);
      ret = OB_INVALID_ARGUMENT;
    } else if (is_exiting_) {
      // do nothing
    } else if (is_follower_()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "current transaction not master, need retry later", K(ret),
                K(tablet_id), K(schema_version), "context", *this);
    } else if (OB_FAIL(mt_ctx_.check_modify_schema_elapsed(tablet_id,
                                                           schema_version))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "check modify schema elapsed failed", K(ret),
                  K(tablet_id), K(schema_version));
      } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(INFO, "current transaction not end, need retry", K(ret),
                  K(tablet_id), K(schema_version), "context", *this);
      }
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(WARN, "spin lock time out after 100 ms", K(ret));
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartTransCtx::check_modify_time_elapsed(
    const ObTabletID &tablet_id,
    const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(lock_.lock(100000 /*100 ms*/))) {
    CtxLockGuard guard(lock_, false);

    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(timestamp <= 0) ||
               OB_UNLIKELY(!tablet_id.is_valid())) {
      TRANS_LOG(WARN, "invalid argument", K(tablet_id), K(timestamp),
                "context", *this);
      ret = OB_INVALID_ARGUMENT;
    } else if (is_exiting_) {
      // do nothing
    } else if (is_follower_()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "current transaction not master, need retry later", K(ret),
                K(tablet_id), K(timestamp), "context", *this);
    } else if (OB_FAIL(mt_ctx_.check_modify_time_elapsed(tablet_id,
                                                         timestamp))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "check modify time elapsed failed", K(ret),
                  K(tablet_id), K(timestamp));
      } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(INFO, "current transaction not end, need retry", K(ret),
                  K(tablet_id), K(timestamp), "context", *this);
      }
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(WARN, "spin lock time out after 100 ms", K(ret));
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartTransCtx::iterate_tx_obj_lock_op(ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    // do nothing
    // we just consider the active trans
  } else if (OB_FAIL(mt_ctx_.iterate_tx_obj_lock_op(iter))) {
    TRANS_LOG(WARN, "iter tx obj lock op failed", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

bool ObPartTransCtx::need_update_schema_version(const int64_t log_id, const int64_t log_ts)
{
  // const int64_t restore_snapshot_version = ls_tx_ctx_mgr_->get_restore_snapshot_version();
  // const int64_t last_restore_log_id = ls_tx_ctx_mgr_->get_last_restore_log_id();
  bool need_update = true;
  // if (restore_snapshot_version > 0
  //    && (last_restore_log_id == OB_INVALID_ID || log_id <= last_restore_log_id)
  //    && (log_ts > restore_snapshot_version)) {
  //  need_update = false;
  //}
  return need_update;
}

int ObPartTransCtx::trans_replay_abort_(const int64_t final_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mt_ctx_.trans_replay_end(false, /*commit*/
                                       ctx_tx_data_.get_commit_version(), final_log_ts))) {
    TRANS_LOG(WARN, "transaction replay end error", KR(ret), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::trans_replay_commit_(const int64_t commit_version,
                                         const int64_t final_log_ts,
                                         const uint64_t log_cluster_version,
                                         const int64_t checksum)
{
  ObTimeGuard tg("trans_replay_commit", 50 * 1000);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(update_publish_version_(commit_version, true))) {
    TRANS_LOG(WARN, "update publish version error", K(ret), K(commit_version), "context", *this);
  } else {
    int64_t freeze_ts = 0;
    if (OB_FAIL(mt_ctx_.trans_replay_end(true, /*commit*/
                                         commit_version,
                                         final_log_ts,
                                         log_cluster_version,
                                         checksum))) {
      TRANS_LOG(WARN, "transaction replay end error", KR(ret), K(commit_version), K(checksum),
                "context", *this);
    }
  }

  return ret;
}

int ObPartTransCtx::update_publish_version_(const int64_t publish_version, const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= publish_version)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(publish_version));
  } else if (OB_FAIL(ctx_tx_data_.set_commit_version(publish_version))) {
    TRANS_LOG(WARN, "set commit version failed", K(ret));
  } else {
    trans_service_->get_tx_version_mgr().update_max_commit_ts(publish_version, false);
    REC_TRANS_TRACE_EXT2(tlog_, push_max_commit_version, OB_ID(trans_version), publish_version,
                         OB_ID(ctx_ref), get_ref());
  }

  return ret;
}

int ObPartTransCtx::replay_start_working_log(int64_t start_working_ts)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  exec_info_.data_complete_ = false;
  start_working_log_ts_ = start_working_ts;
  if (!has_persisted_log_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "trx has no persisten log", K(ret), K(*this));
  }

  return ret;
}

// The txn is in the state between prepare and clear
bool ObPartTransCtx::is_in_2pc_() const
{
  ObTxState state = exec_info_.state_;
  return state >= ObTxState::PREPARE;
}

bool ObPartTransCtx::is_logging_() const { return !busy_cbs_.is_empty(); }

bool ObPartTransCtx::has_persisted_log_() const
{
  return exec_info_.max_applying_log_ts_ != OB_INVALID_TIMESTAMP;
}

int ObPartTransCtx::gts_callback_interrupted(const int errcode)
{
  int ret = OB_SUCCESS;
  UNUSED(errcode);

  bool need_revert_ctx = false;
  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(ERROR, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(is_exiting_)) {
      need_revert_ctx = true;
      sub_state_.clear_gts_waiting();
      TRANS_LOG(INFO, "transaction is interruputed gts callback", KR(ret), "context", *this);
    } else {
      ret = OB_EAGAIN;
    }
  }
  if (need_revert_ctx) {
    ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
  }

  return ret;
}

int ObPartTransCtx::get_gts_callback(const MonotonicTs srr,
                                     const int64_t gts,
                                     const MonotonicTs receive_gts_ts)
{
  TRANS_LOG(DEBUG, "ObPartTransCtx get_gts_callback begin", KPC(this));
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    // TRANS_LOG(INFO, "get_gts_callback begin", K(*this));
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      ret = OB_TRANS_IS_EXITING;
      need_revert_ctx = true;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (OB_UNLIKELY(is_follower_())) {
      sub_state_.clear_gts_waiting();
      need_revert_ctx = true;
    } else if (srr < get_stc_()) {
      ret = OB_EAGAIN;
    } else {
      if (OB_LIKELY(sub_state_.is_gts_waiting())) {
        sub_state_.clear_gts_waiting();
      } else {
        TRANS_LOG(ERROR, "unexpected sub state", K(*this));
      }
      set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
      // the same as before prepare
      mt_ctx_.set_trans_version(gts);
      const int64_t max_read_ts = trans_service_->get_tx_version_mgr().get_max_read_ts();
      // TRANS_LOG(INFO, "get_gts_callback mid", K(*this), K(log_type));
      if (is_local_tx_()) {
        if (OB_FAIL(ctx_tx_data_.set_commit_version(max(gts, max_read_ts)))) {
          TRANS_LOG(WARN, "set commit_version failed", K(ret));
        } else if (part_trans_action_ != ObPartTransAction::COMMIT) {
          // one phase commit failed or abort
          TRANS_LOG(INFO, "one phase commit has not been successful, need retry", K(ret), KPC(this));
        } else if (sub_state_.is_state_log_submitted()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "the commit log has been submitted", K(ret), KPC(this));
        } else if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_COMMIT_LOG))) {
          TRANS_LOG(WARN, "submit commit log in gts callback failed", K(ret), KPC(this));
        }
      } else {
        const int64_t local_prepare_version = std::max(gts, max_read_ts);
        exec_info_.prepare_version_ = std::max(local_prepare_version, exec_info_.prepare_version_);

        bool no_need_submit_log = is_sub2pc() || exec_info_.is_dup_tx_;
        if (get_upstream_state() < ObTxState::PREPARE && OB_FAIL(do_prepare(no_need_submit_log))) {
          TRANS_LOG(WARN, "drive into prepare phase failed in gts callback", K(ret), KPC(this));
        } else {
          // no_need_submit_log must be false here
          collected_.reset();
          set_upstream_state(ObTxState::PREPARE);
        }
        if (OB_SUCC(ret) && !no_need_submit_log && OB_FAIL(submit_log_impl_(ObTxLogType::TX_PREPARE_LOG))) {
          TRANS_LOG(WARN, "submit prepare log in gts callback failed", K(ret), KPC(this));
        }
      }

      need_revert_ctx = true;
    }
    REC_TRANS_TRACE_EXT2(tlog_, get_gts_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts),  OB_ID(ctx_ref), get_ref());
  }
  // before revert self
  if (OB_FAIL(ret)) {
    if (OB_EAGAIN == ret) {
      TRANS_LOG(WARN, "ObPartTransCtx::get_gts_callback - retry gts task by TsMgr", KR(ret), K(*this),
                K(gts));
    } else {
      TRANS_LOG(WARN, "ObPartTransCtx::get_gts_callback", KR(ret), K(*this), K(gts));
      if (sub_state_.is_gts_waiting()) {
        sub_state_.clear_gts_waiting();
      }
    }
  }
  if (need_revert_ctx) {
    ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
  }
  TRANS_LOG(DEBUG, "ObPartTransCtx get_gts_callback end", KPC(this));
  return ret;
}

int ObPartTransCtx::gts_elapse_callback(const MonotonicTs srr, const int64_t gts)
{
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      need_revert_ctx = true;
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (ctx_tx_data_.get_commit_version() > gts) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "the commit version is still larger than gts", K(ret), K(this));
    } else {
      if (OB_UNLIKELY(!sub_state_.is_gts_waiting())) {
        TRANS_LOG(ERROR, "unexpected gts waiting flag", K(*this));
      } else {
        sub_state_.clear_gts_waiting();
      }
      if (is_local_tx_()) {
        if (OB_FAIL(after_local_commit_succ_())) {
          TRANS_LOG(WARN, "terminate trx after local commit failed", KR(ret), KPC(this));
        }
      } else {
        // distributed tx
        if (!is_root()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "gts_elapse_callback for distributed tx, but the part_ctx is not root",
              KR(ret), KPC(this));
        } else if (is_follower_()) {
          TRANS_LOG(INFO, "current state is follower, do nothing", KPC(this));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::enter_pre_commit_state())) {
          TRANS_LOG(WARN, "enter_pre_commit_state failed", K(ret), KPC(this));
        } else {
          // TODO, refine in 4.1
          if (is_sub2pc()) {
            if (OB_FAIL(reply_to_scheduler_for_sub2pc(SUBCOMMIT_RESP))) {
              TRANS_LOG(ERROR, "fail to do sub prepare", K(ret), K(*this));
            }
            TRANS_LOG(INFO, "apply prepare log for sub trans", K(ret), K(*this));
          }
          // TODO, currently, if a trans only has one participant,
          // the state can not be drived from pre commit to commit.
          // Therefore, enter commit state directly.
          const int64_t SINGLE_COUNT = 1;
          if (SINGLE_COUNT == get_participants_size()) {
            upstream_state_ = ObTxState::COMMIT;
            collected_.reset();
            // TODO, drive it and submit log via msg
            if (OB_FAIL(do_commit())) {
              TRANS_LOG(WARN, "do commit failed", K(ret), K(*this));
            } else {
              if (OB_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ))) {
                TRANS_LOG(WARN, "post commit request failed", K(ret), K(*this));
                ret = OB_SUCCESS;
              }
              if (OB_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT))) {
                TRANS_LOG(WARN, "submit commit log failed", K(ret), K(*this));
              }
            }
          }
        }
      }
      need_revert_ctx = true;
    }
    REC_TRANS_TRACE_EXT2(tlog_, gts_elapse_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts),  OB_ID(ctx_ref), get_ref());
  }
  // before revert self
  if (OB_FAIL(ret)) {
    if (OB_EAGAIN == ret) {
      TRANS_LOG(WARN, "ObPartTransCtx::gts_elapse_callback - retry gts task by TsMgr", KR(ret), K(*this),
                K(gts));
    } else {
      TRANS_LOG(WARN, "ObPartTransCtx::gts_elapse_callback", KR(ret), K(*this), K(gts));
      if (sub_state_.is_gts_waiting()) {
        sub_state_.clear_gts_waiting();
      }
    }
  }
  if (need_revert_ctx) {
    ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
  }
  return ret;
}

int ObPartTransCtx::get_prepare_version_if_prepared(bool &is_prepared, int64_t &prepare_version)
{
  int ret = OB_SUCCESS;
  ObTxState cur_state = exec_info_.state_;

  if (ObTxState::PREPARE == cur_state) {
    is_prepared = true;
    prepare_version = exec_info_.prepare_version_;
  } else if (ObTxState::COMMIT == cur_state || ObTxState::ABORT == cur_state
             || ObTxState::CLEAR == cur_state) {
    is_prepared = true;
    prepare_version = INT64_MAX;
  } else {
    is_prepared = false;
    prepare_version = INT64_MAX;
  }

  return ret;
}

int ObPartTransCtx::get_memtable_key_arr(ObMemtableKeyArray &memtable_key_arr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == lock_.try_lock()) {
    if (IS_NOT_INIT || is_follower_() || is_exiting_) {
      TRANS_LOG(DEBUG, "part_ctx not need to get memtable key");
    } else if (OB_FAIL(mt_ctx_.get_memtable_key_arr(memtable_key_arr))) {
      TRANS_LOG(WARN, "get_memtable_key_arr fail", K(ret), K(memtable_key_arr), K(mt_ctx_));
    } else {
      // do nothing
    }
    lock_.unlock();
  } else {
    ObMemtableKeyInfo info;
    info.init(1);
    memtable_key_arr.push_back(info);
  }

  return ret;
}

bool ObPartTransCtx::can_be_recycled_()
{
  bool bool_ret = true;
  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (is_exiting_) {
    bool_ret = false;
  } else if (is_follower_()) {
    bool_ret = false;
  } else if (ObTxState::REDO_COMPLETE < get_upstream_state()) {
    bool_ret = false;
  } else if (is_sub2pc() && ObTxState::REDO_COMPLETE == get_upstream_state()) {
    bool_ret = false;
  } else if (is_logging_()) { // FIXME. xiaoshi
    bool_ret = false;
  } else if (ObTxState::REDO_COMPLETE < get_downstream_state()) {
    bool_ret = false;
  } else {
    TRANS_LOG(DEBUG, "can be recycled", KPC(this));
  }
  return bool_ret;
}

bool ObPartTransCtx::need_to_ask_scheduler_status_()
{
  bool bool_ret = false;
  if (can_be_recycled_()) {
    if (ObTimeUtility::current_time() - last_ask_scheduler_status_ts_
        < CHECK_SCHEDULER_STATUS_INTERVAL) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObPartTransCtx::check_rs_scheduler_is_alive_(bool &is_alive)
{
  int ret = OB_SUCCESS;
  int64_t trace_time = 0;
  int64_t cur_time = ObTimeUtility::current_time();
  share::ObAliveServerTracer *server_tracer = NULL;

  is_alive = true;
  if (OB_ISNULL(trans_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret), K(*this));
  } else if (OB_ISNULL(server_tracer = trans_service_->get_server_tracer())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "server tracer is NULL", KR(ret), K(*this));
  } else if (OB_FAIL(server_tracer->is_alive(exec_info_.scheduler_, is_alive, trace_time))) {
    TRANS_LOG(WARN, "server tracer error", KR(ret), "context", *this);
    // To be conservative, if the server tracer reports an error, the scheduler
    // is alive by default
    is_alive = true;
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::gc_ctx_()
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;
  if (OB_FAIL(prepare_mul_data_source_tx_end_(false))) {
    TRANS_LOG(WARN, "trans gc need retry", K(ret), K(trans_id_), K(ls_id_));
  } else {
    TRANS_LOG(INFO, "[TRANS GC] part ctx abort", "context", *this);
    REC_TRANS_TRACE_EXT2(tlog_, tx_ctx_gc, OB_ID(ctx_ref), get_ref());
    if (need_callback_scheduler_()) {
      TRANS_LOG(INFO, "[TRANS GC] scheduler has down, skip callback scheduler", KP(this),
                K_(trans_id));
      commit_cb_.disable();
    }
    if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
      TRANS_LOG(WARN, "do local tx abort failed", K(ret));
    }
  }
  return ret;
}

int ObPartTransCtx::check_scheduler_status()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == lock_.try_lock()) {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    // 1. check the status of scheduler on rs
    bool is_alive = true;
    bool need_check_scheduler = need_to_ask_scheduler_status_();
    if (!need_check_scheduler) {
      TRANS_LOG(DEBUG, "don't need ask scheduler status", K(ret), K(*this));
    } else if (OB_FAIL(check_rs_scheduler_is_alive_(is_alive))) {
      TRANS_LOG(WARN, "check rs scheduler is alive error", KR(ret), K(is_alive), "context",
                *this);
      // scheduler已宕机
    } else if (!is_alive) {
      if (OB_FAIL(gc_ctx_())) {
        TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), "context", *this);
      }
    } else {
      // do nothing
    }
    // 2. Ask for the status of scheduler
    ret = OB_SUCCESS;
    if (is_alive && need_check_scheduler) {
      ObTxKeepaliveMsg msg;
      msg.cluster_version_ = cluster_version_;
      msg.tenant_id_ = tenant_id_;
      msg.cluster_id_ = cluster_id_;
      msg.request_id_ = ObClockGenerator::getClock();
      msg.tx_id_ = trans_id_;
      msg.sender_addr_ = addr_;
      msg.sender_ = ls_id_;
      msg.receiver_ = share::SCHEDULER_LS; // this just used to pass validation
      msg.epoch_ = epoch_;
      msg.status_ = OB_SUCCESS;
      if (OB_FAIL(rpc_->post_msg(exec_info_.scheduler_, msg))) {
        TRANS_LOG(WARN, "post tx keepalive msg fail", K(ret), K(msg), KPC(this));
      }
      // ignore msg postting result
      last_ask_scheduler_status_ts_ = ObClockGenerator::getClock();
    }
  }
  return OB_SUCCESS;
}

int ObPartTransCtx::recover_tx_ctx_table_info(const ObTxCtxTableInfo &ctx_info)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (!ctx_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ctx_info));
  // } else if (OB_FAIL(init_tx_data_(ctx_info.ls_id_, ctx_info.tx_id_))) {
  //   TRANS_LOG(WARN, "unexpected null ptr", K(*this));
  } else if (OB_FAIL(mt_ctx_.recover_from_table_lock_durable_info(ctx_info.table_lock_info_))) {
    TRANS_LOG(ERROR, "recover_from_table_lock_durable_info failed", K(ret));
  } else if (OB_FAIL(ctx_tx_data_.recover_tx_data(&ctx_info.state_info_))) {
    TRANS_LOG(WARN, "recover tx data failed", K(ret), K(ctx_tx_data_));
  } else {
    trans_id_ = ctx_info.tx_id_;
    ls_id_ = ctx_info.ls_id_;
    exec_info_ = ctx_info.exec_info_;
    if (ObTxState::REDO_COMPLETE == get_downstream_state()) {
      sub_state_.set_info_log_submitted();
    }
    if (exec_info_.prepare_version_ != ObTransVersion::INVALID_TRANS_VERSION) {
      mt_ctx_.set_trans_version(exec_info_.prepare_version_);
    }
    exec_info_.multi_data_source_.reset();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(deep_copy_mds_array(ctx_info.exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "deep copy ctx_info mds_array failed", K(ret));
    } else if (FALSE_IT(mt_ctx_.update_checksum(exec_info_.checksum_,
                                                exec_info_.checksum_log_ts_))) {
      TRANS_LOG(ERROR, "recover checksum failed", K(ret), KPC(this), K(ctx_info));
    } else if (!is_local_tx_() && OB_FAIL(ObTxCycleTwoPhaseCommitter::recover_from_tx_table())) {
      TRANS_LOG(ERROR, "recover_from_tx_table failed", K(ret), KPC(this));
    } else {
      is_ctx_table_merged_ = true;
    }

    //insert into retain ctx mgr if it will not replay commit or abort log
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (exec_info_.multi_data_source_.count() > 0
               && (ObTxState::COMMIT == exec_info_.state_ || ObTxState::ABORT == exec_info_.state_
                   || ObTxState::CLEAR == exec_info_.state_)) {
      if (OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG,
                                              exec_info_.max_applying_log_ts_,
                                              exec_info_.max_durable_lsn_))) {
        TRANS_LOG(WARN, "insert into retain ctx mgr failed", K(ret), KPC(this));
      } else if ((exec_info_.trans_type_ == TransType::SP_TRANS
                  && ObTxState::COMMIT == exec_info_.state_)
                 || (exec_info_.trans_type_ == TransType::DIST_TRANS
                     && ObTxState::CLEAR == exec_info_.state_)
                 || (ObTxState::ABORT == exec_info_.state_)) {
        set_exiting_();
      }
      if (OB_SUCC(ret)) {
        TRANS_LOG(INFO, "recover retain ctx into mgr success", K(ret), K(trans_id_), K(ls_id_));
      }
    }

    TRANS_LOG(INFO, "recover tx ctx table info succeed", K(ret), KPC(this), K(ctx_info));
  }

  REC_TRANS_TRACE_EXT2(tlog_,
                       recover_from_ctx_table,
                       OB_ID(ret),
                       ret,
                       OB_ID(max_applying_ts),
                       ctx_info.exec_info_.max_applying_log_ts_,
                       OB_ID(state),
                       ctx_info.exec_info_.state_);
  return ret;
}

// Checkpoint the tx ctx table
int ObPartTransCtx::get_tx_ctx_table_info(ObTxCtxTableInfo &info)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  // 1. Tx ctx has already exited, so it means that it may have no chance to
  //    push its rec_log_ts to aggre_rec_log_ts, so we must not persist it
  // NB: You need take note that retained tx ctx should be dumped due to
  //     multi-source data.
  if (is_exiting_ && get_retain_cause() == RetainCause::UNKOWN) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(INFO, "tx ctx has exited", K(ret), KPC(this));
  // 2. Tx ctx has no persisted log, so donot need persisting
  } else if (OB_INVALID_TIMESTAMP == exec_info_.max_applying_log_ts_) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(INFO, "tx ctx has no persisted log", K(ret), KPC(this));
  // 3. Fetch the current state of the tx ctx table
  } else if (OB_FAIL(get_tx_ctx_table_info_(info))) {
    TRANS_LOG(WARN, "get tx ctx table info failed", K(ret), K(*this));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx ctx info invalid", K(ret), K(info));
  // 4. Refresh the rec_log_ts for the next checkpoint
  } else if (OB_FAIL(refresh_rec_log_ts_())) {
    TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), K(*this));
  } else {
    is_ctx_table_merged_ = true;
  }

  return ret;
}

int64_t ObPartTransCtx::get_rec_log_ts() const
{
  CtxLockGuard guard(lock_);
  return get_rec_log_ts_();
}

int64_t ObPartTransCtx::get_rec_log_ts_() const
{
  int64_t ret = INT64_MAX;

  // Before the checkpoint of the tx ctx table is succeed, we should still use
  // the prev_log_ts. And after successfully checkpointed, we can use the new
  // rec_log_ts if exist
  if (OB_INVALID_TIMESTAMP != prev_rec_log_ts_) {
    ret = prev_rec_log_ts_;
  } else if (OB_INVALID_TIMESTAMP != rec_log_ts_) {
    ret = rec_log_ts_;
  }

  TRANS_LOG(DEBUG, "part ctx get rec log ts", K(*this), K(ret));

  return ret;
}

int ObPartTransCtx::on_tx_ctx_table_flushed()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  // To mark the checkpoint is succeed, we reset the prev_rec_log_ts
  prev_rec_log_ts_ = OB_INVALID_TIMESTAMP;

  return ret;
}

// for ob admin only
void ObPartTransCtx::set_trans_table_status(const ObTrxToolArg &arg)
{
  ObTransTableStatusType status = ObTransTableStatusType(arg.status_);
  int64_t trans_version = arg.trans_version_;
  const int64_t end_log_ts = arg.end_log_ts_;

  if (ObTransTableStatusType::COMMIT == status) {
    mt_ctx_.set_commit_version(trans_version);
  } else {
    mt_ctx_.set_trans_version(trans_version);
  }

  if (ObTransTableStatusType::COMMIT == status || ObTransTableStatusType::ABORT == status) {
    end_log_ts_ = end_log_ts;
  }
}

int64_t ObPartTransCtx::to_string(char* buf, const int64_t buf_len) const
{
  int64_t len1 = 0;
  int64_t len2 = 0;
  len1 = ObTransCtx::to_string(buf, buf_len);
  if (lock_.is_locked_by_self()) {
    len2 = to_string_(buf + len1, buf_len - len1);
  }
  return len1 + len2;
}

int ObPartTransCtx::remove_callback_for_uncommited_txn(ObMemtable *mt)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(mt));
  } else if (OB_UNLIKELY(is_exiting_)) {
  } else if (OB_FAIL(mt_ctx_.remove_callback_for_uncommited_txn(mt, exec_info_.max_applied_log_ts_))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(mt_ctx_), K(exec_info_.max_applied_log_ts_));
  }

  return ret;
}

// this function is only called by instant logging and freezing,
// they both view ret code OB_BLOCK_FROZEN as success.
int ObPartTransCtx::submit_redo_log(const bool is_freeze)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  bool try_submit = false;
  bool is_tx_committing = false;
  bool final_log_submitting = false;

  if (is_freeze) {
    // spin lock
    CtxLockGuard guard(lock_);
    ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, true);

    is_tx_committing = ObTxState::INIT != get_downstream_state();
    final_log_submitting = final_log_cb_.is_valid();
    if (!is_tx_committing && !final_log_submitting) {
      (void)mt_ctx_.merge_multi_callback_lists_for_immediate_logging();
      ret = submit_log_impl_(ObTxLogType::TX_REDO_LOG);
      if (OB_SUCC(ret) || OB_BLOCK_FROZEN == ret) {
        ret = submit_multi_data_source_();
      }
      try_submit = true;
    }
    ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, false);
    if (try_submit) {
      REC_TRANS_TRACE_EXT2(tlog_, submit_instant_log, Y(ret), OB_ID(arg2), is_freeze,
                           OB_ID(used), ObTimeUtility::fast_current_time() - start,
                           OB_ID(ctx_ref), get_ref());
    }
  } else if (OB_FAIL(lock_.try_lock())) {
    // try lock
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(ERROR, "try lock error, unexpected error", K(ret), K(*this));
    }
  } else {

    CtxLockGuard guard(lock_, false);

    if (mt_ctx_.pending_log_size_too_large()) {
      is_tx_committing = ObTxState::INIT != get_downstream_state();
      final_log_submitting = final_log_cb_.is_valid();

      if (!is_tx_committing && !final_log_submitting) {
        (void)mt_ctx_.merge_multi_callback_lists_for_immediate_logging();
        ret = submit_log_impl_(ObTxLogType::TX_REDO_LOG);
        try_submit = true;
      }
    }
    if (try_submit) {
      REC_TRANS_TRACE_EXT2(tlog_, submit_instant_log, Y(ret), OB_ID(arg2), is_freeze,
                           OB_ID(used), ObTimeUtility::fast_current_time() - start,
                           OB_ID(ctx_ref), get_ref());
    }
  }
  if (OB_BLOCK_FROZEN == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

void ObPartTransCtx::get_audit_info(int64_t &lock_for_read_elapse) const
{
  lock_for_read_elapse = mt_ctx_.get_lock_for_read_elapse();
}

int64_t ObPartTransCtx::get_part_trans_action() const
{
  return part_trans_action_;
}

bool ObPartTransCtx::is_table_lock_killed() const
{
  bool is_killed = false;
  is_killed = (mt_ctx_.is_table_lock_killed() ||
               (exec_info_.state_ == ObTxState::ABORT));
  return is_killed;
}

int ObPartTransCtx::compensate_abort_log_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_ABORT_LOG))) {
    TRANS_LOG(WARN, "submit abort log failed", KR(ret), K(*this));
  } else {
    sub_state_.set_force_abort();
  }
  TRANS_LOG(INFO, "compensate abort log", K(ret), KPC(this));
  return ret;
}

int ObPartTransCtx::abort_(int reason)
{
  int ret = OB_SUCCESS;
  REC_TRANS_TRACE_EXT2(tlog_, abort, OB_ID(reason), reason);
  if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
    TRANS_LOG(WARN, "do local tx abort failed", K(ret), K(reason));
  }
  TRANS_LOG(INFO, "tx abort", K(ret), K(reason), KR(reason), KPC(this));
  return ret;
}

int ObPartTransCtx::update_max_commit_version_()
{
  int ret = OB_SUCCESS;
  trans_service_->get_tx_version_mgr().update_max_commit_ts(
      ctx_tx_data_.get_commit_version(), false);
  return ret;
}

// Unified interface for normal transaction end(both commit and abort). We We
// want to integrate the following six things that all txn commits should do.
//
// 1.end_log_ts: We set end_log_ts during final log state is synced which must
// have been done, so we check the validation of end_log_ts here(Maybe set it in
// this function is better?)
// 2.commit_version: We set commit version after submit the commit log for local
// tx and during the do_prepare for dist tx which must have been done, so we
// check the validation of commit_version here(Maybe set it in this function is
// better?)
// 3.mt_ctx.tx_end: We need callback all txn ops for all data in txn after final
// state is synced. It must be called for all txns to clean and release its data
// resource.
// 4.set_status: We need set status to kill the concurrent read and write.
// 5.set_state: We need set state after final state is synced. It tells others
// that all data for this txn is decided and visible.
// 6.insert_tx_data: We need insert into tx_data in order to cleanot data which
// need be delay cleanout
//
// NB: You need pay much attention to the order of the following steps
// TODO: Integrate trans_kill and trans_replay_end into the same function
int ObPartTransCtx::tx_end_(const bool commit)
{
  int ret = OB_SUCCESS;

  // NB: The order of the following steps is critical
  int32_t state = commit ? ObTxData::COMMIT : ObTxData::ABORT;
  int64_t commit_version = ctx_tx_data_.get_commit_version();
  int64_t end_log_ts = ctx_tx_data_.get_end_log_ts();

  // STEP1: We need check whether the end_log_ts is valid before state is filled
  // in here because it will be used to cleanout the tnode if state is decided.
  // What's more the end_log_ts is also be used during mt_ctx_.trans_end to
  // backfill normal tnode.
  if (has_persisted_log_() && OB_INVALID_TIMESTAMP == end_log_ts) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "end log ts is invalid when tx end", K(ret), KPC(this));
  // STEP2: We need check whether the commi_version is valid before state is
  // filled in with commit here because it will be used to cleanout the tnode or
  // lock for read if state is decided. What's more the commit_version is also
  // be used during mt_ctx_.trans_end to backfill normal tnode..
  } else if (commit && ObTransVersion::INVALID_TRANS_VERSION == commit_version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "commit version is invalid when tx end", K(ret), KPC(this));
  // STEP3: We need set status in order to kill concurrent read and write. What
  // you need pay attention to is that we rely on the status to report the
  // suicide before the tnode can be cleanout by concurrent read using state in
  // ctx_tx_data.
  } else if (!commit && FALSE_IT(mt_ctx_.set_tx_rollbacked())) {
  // STEP4: We need set state in order to informing others of the final status
  // of my txn. What you need pay attention to is that after this action, others
  // can cleanout the unfinished txn state and see all your data. We currently
  // move set_state before mt_ctx_.trans_end for the commit state in order to
  // accelerate users to see the data state.
  } else if (OB_FAIL(ctx_tx_data_.set_state(state))) {
    TRANS_LOG(WARN, "set tx data state failed", K(ret), KPC(this));
  // STEP5: We need invoke mt_ctx_.trans_end before state is filled in here
  // because we relay on the state in the ctx_tx_data_ to callback all txn ops.
  } else if (OB_FAIL(mt_ctx_.trans_end(commit, commit_version, end_log_ts))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), K(commit), "context", *this);
  // STEP6: We need insert into the tx_data after all states are filled
  } else if (has_persisted_log_() && OB_FAIL(ctx_tx_data_.insert_into_tx_table())) {
    TRANS_LOG(WARN, "insert to tx table failed", KR(ret), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::on_dist_end_(const bool commit)
{
  int ret = OB_SUCCESS;

  int64_t start_us, end_us;
  start_us = end_us = 0;
  // Distributed transactions need to wait for the commit log majority successfully before
  // unlocking. If you want to know the reason, it is in the ::do_dist_commit
  start_us = ObTimeUtility::fast_current_time();

  if (OB_FAIL(tx_end_(commit))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), K(commit), "context", *this);
  } else if (FALSE_IT(end_us = ObTimeUtility::fast_current_time())) {
  } else if (commit) {
    // reset the early lock release stat after the txn commits
    elr_handler_.reset_elr_state();
  }

  ObTransStatistic::get_instance().add_trans_mt_end_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_trans_mt_end_time(tenant_id_, end_us - start_us);
  TRANS_LOG(DEBUG, "trans end", K(ret), K(trans_id_), K(commit), K(commit_version));

  return ret;
}

int ObPartTransCtx::on_success(ObTxLogCb *log_cb)
{
  common::ObTimeGuard tg("part_ctx::on_success", 100 * 1000);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cur_ts = ObTimeUtility::current_time();
  ObTransStatistic::get_instance().add_clog_sync_time(tenant_id_, cur_ts - log_cb->get_submit_ts());
  ObTransStatistic::get_instance().add_clog_sync_count(tenant_id_, 1);
  {
    CtxLockGuard guard(lock_);
    tg.click();
    const int64_t log_ts = log_cb->get_log_ts();
    const ObTxLogType last_log_type = log_cb->get_last_log_type();
    if (is_exiting_) {
      // maybe because commit log callbacked before redo log, and ctx is already released
      // if there is tx data, it would be released when reset_log_cbs_
#ifndef NDEBUG
      TRANS_LOG(INFO, "tx ctx exiting", K(*this));
#endif
    } else if (log_cb->is_callbacked()) {
#ifndef NDEBUG
      TRANS_LOG(INFO, "cb has been callbacked", K(*this), K(*log_cb));
#endif
    } else {
      // save the first error code
      int save_ret = OB_SUCCESS;
      ObTxLogCb *cur_cb = busy_cbs_.get_first();
      tg.click();
      for (int64_t i = 0; i < busy_cbs_.get_size(); i++) {
        if (cur_cb->is_callbacked()) {
          // do nothing
        } else {
          if (OB_FAIL(on_success_ops_(cur_cb))) {
            TRANS_LOG(ERROR, "invoke on_success_ops failed", K(ret), K(*this), K(*cur_cb));
            if (OB_SUCCESS == save_ret) {
              save_ret = ret;
            }
            // rewrite ret
            ret = OB_SUCCESS;

            usleep(1000*1000);
            ob_abort();
          }
          // ignore ret and set cur_cb callbacked
          cur_cb->set_callbacked();
        }
        if (cur_cb == log_cb) {
          break;
        } else {
          cur_cb = cur_cb->get_next();
        }
      }
      tg.click();
      if (cur_cb != log_cb) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected log callback", K(ret), K(*this), K(*cur_cb), K(*log_cb));
      } else {
        // return first error code
        ret = save_ret;
      }
    }
    // ignore ret
    return_log_cb_(log_cb);
    tg.click();
    if (need_record_log_()) {
      // ignore error
      if (OB_SUCCESS != (tmp_ret = submit_record_log_())) {
        TRANS_LOG(WARN, "failed to submit record log", K(tmp_ret), K(*this));
      }
    }
    tg.click();
    if (!ObTxLogTypeChecker::is_state_log(last_log_type)) {
      try_submit_next_log_();
    }
    tg.click();
    // REC_TRANS_TRACE_EXT(tlog_, on_succ_cb, OB_ID(ret), ret,
    //                                        OB_ID(t), log_ts,
    //                                        OB_ID(ctx_ref), get_ref());
    if (tg.get_diff() > 100000) {
      FORCE_PRINT_TRACE(tlog_, "[tx cb debug] ");
      TRANS_LOG(INFO, "on success cost too much time", K_(trans_id), K(last_log_type), K(log_ts), K(tg));
    }
  }
  if (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this))) {
    TRANS_LOG(ERROR, "release ctx ref failed", KR(tmp_ret));
  }
  return ret;
}

int ObPartTransCtx::on_success_ops_(ObTxLogCb *log_cb)
{
  common::ObTimeGuard tg("part_ctx::on_success_ops", 100 * 1000);
  int ret = OB_SUCCESS;
  const int64_t log_ts = log_cb->get_log_ts();
  const palf::LSN log_lsn = log_cb->get_lsn();
  const ObTxCbArgArray &cb_arg_array = log_cb->get_cb_arg_array();
  ObTxBufferNodeArray tmp_array;

  if (OB_FAIL(common_on_success_(log_cb))) {
    TRANS_LOG(WARN, "common_on_success_ failed", K(ret));
  }
  tg.click();
  for (int64_t i = 0; OB_SUCC(ret) && i < cb_arg_array.count(); i++) {
    const ObTxLogType log_type = cb_arg_array.at(i).get_log_type();
    if (ObTxLogType::TX_REDO_LOG == log_type) {
      // do nothing
    }  else if (ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type) {
      tmp_array.reset();
      if (OB_FAIL(log_cb->get_mds_range().copy_to(tmp_array))) {
        TRANS_LOG(WARN, "copy mds log array failed", K(ret));
      } else if (OB_FAIL(log_cb->get_mds_range().move_to(exec_info_.multi_data_source_))) {
        TRANS_LOG(WARN, "move MDS range into exec_info failed", K(ret));
      } else if (FALSE_IT(mds_cache_.clear_submitted_iterator())) {
        //do nothing
      } else if (OB_FAIL(notify_data_source_(NotifyType::ON_REDO, log_ts, false, tmp_array))) {
        TRANS_LOG(WARN, "notify data source for ON_REDO", K(ret));
      } else {
        log_cb->get_mds_range().reset();
      }
    } else if (ObTxLogType::TX_ACTIVE_INFO_LOG == log_type) {
      if (log_ts > start_working_log_ts_) {
        exec_info_.data_complete_ = true;
      }
    } else if (ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
      ObTwoPhaseCommitLogType two_phase_log_type;
      set_durable_state_(ObTxState::REDO_COMPLETE);
      if (exec_info_.is_dup_tx_ && OB_FAIL(dup_table_tx_redo_sync_())) {
        TRANS_LOG(WARN, "dup table redo sync error", K(ret));
      }
      if (is_sub2pc()) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
          TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this), K(log_type));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
          TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this), K(two_phase_log_type));
        }
        TRANS_LOG(INFO, "apply commit info log", KR(ret), K(*this), K(two_phase_log_type));
      }
      tg.click();
    } else if (ObTxLogType::TX_ROLLBACK_TO_LOG == log_type) {
      tg.click();
      ObTxData *tx_data = log_cb->get_tx_data();
      if (OB_ISNULL(tx_data)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected null ptr", KR(ret), K(*this));
      } else {
        // although logs may be callbacked out of order,
        // insert into tx table all the way, tx table will
        // filter out the obsolete one.
        tx_data->end_log_ts_ = log_ts;
        if (OB_FAIL(ctx_tx_data_.insert_tmp_tx_data(tx_data))) {
          TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
        } else {
          log_cb->set_tx_data(nullptr);
        }
      }
      tg.click();
    } else if (ObTxLogTypeChecker::is_state_log(log_type)) {
      sub_state_.clear_state_log_submitting();
      if (ObTxLogType::TX_PREPARE_LOG == log_type) {

        //must before apply log
        ObLSLogInfo info(ls_id_, log_cb->get_lsn());
        if (OB_FAIL(merge_prepare_log_info_(info))) {
          TRANS_LOG(WARN, "push log stream info failed", KR(ret), K(*this));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(notify_data_source_(NotifyType::ON_PREPARE,
                                               log_ts,
                                               false,
                                               exec_info_.multi_data_source_))) {
          TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
        }

        ObTwoPhaseCommitLogType two_phase_log_type;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
          TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
          TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this));
        }
        tg.click();
      } else if (ObTxLogType::TX_COMMIT_LOG == log_type) {
        tg.click();
        if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
            && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, log_ts, log_lsn))) {
          TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(log_cb), KPC(this));
        }
        if (is_local_tx_()) {
          if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
            TRANS_LOG(WARN, "set end log ts failed", K(ret));
          } else {
            tg.click();
            if (OB_FAIL(on_local_commit_tx_())) {
              TRANS_LOG(WARN, "on local commit failed", KR(ret), K(*this));
            }
          }
        } else {
          tg.click();
          const NotifyType type = NotifyType::ON_COMMIT;
          if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
            TRANS_LOG(WARN, "set end log ts failed", K(ret));
          } else if (OB_FAIL(notify_data_source_(type, log_ts, false, exec_info_.multi_data_source_))) {
            TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
          }
          tg.click();
          ObTwoPhaseCommitLogType two_phase_log_type;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
            TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this), K(log_type));
          } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
            TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this), K(two_phase_log_type));
          }
          tg.click();
        }
      } else if (ObTxLogType::TX_ABORT_LOG == log_type) {
        if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
            && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, log_ts, log_lsn))) {
          TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(log_cb), KPC(this));
        }
        if (is_local_tx_() || sub_state_.is_force_abort()) {
          if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
            TRANS_LOG(WARN, "set end log ts failed", K(ret));
          } else if (OB_FAIL(on_local_abort_tx_())) {
            TRANS_LOG(WARN, "on local abort failed", KR(ret), K(*this));
          }
          tg.click();
        } else {
          const NotifyType type = NotifyType::ON_ABORT;
          tmp_array.reset();
          if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
            TRANS_LOG(WARN, "set end log ts failed", K(ret));
          } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
            TRANS_LOG(WARN, "gen total mds array failed", K(ret));
          } else if (OB_FAIL(notify_data_source_(type, log_ts, false, tmp_array))) {
            TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
          }
          tg.click();
          ObTwoPhaseCommitLogType two_phase_log_type;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
            TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this), K(log_type));
          } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
            TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this), K(two_phase_log_type));
          }
          tg.click();
        }
      } else if (ObTxLogType::TX_CLEAR_LOG == log_type) {
        ObTwoPhaseCommitLogType two_phase_log_type;
        if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
          TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
          TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unknown log type", K(ret), K(*this));
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, log_sync_succ_cb,
                               OB_ID(ret), ret,
                               OB_ID(log_type), (void*)log_type,
                               OB_ID(t), log_ts,
                               OB_ID(offset), log_lsn,
                               OB_ID(ctx_ref), get_ref());
  }
  if (tg.get_diff() > 100000) {
    FORCE_PRINT_TRACE(tlog_, "[tx common cb debug] ");
    TRANS_LOG(INFO, "on succcess ops cost too much time", K(tg), K(*this));
  }
  return ret;
}

int ObPartTransCtx::common_on_success_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t log_ts = log_cb->get_log_ts();
  const palf::LSN lsn = log_cb->get_lsn();
  const ObTxLogType last_log_type = log_cb->get_last_log_type();
  if (log_ts > exec_info_.max_applying_log_ts_) {
    exec_info_.max_applying_log_ts_ = log_ts;
    exec_info_.max_applying_part_log_no_ = 0;
  }
  if (log_ts > exec_info_.max_applied_log_ts_) {
    exec_info_.max_applied_log_ts_ = log_ts;
  }
  if (!exec_info_.max_durable_lsn_.is_valid() || lsn > exec_info_.max_durable_lsn_) {
    exec_info_.max_durable_lsn_ = lsn;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mt_ctx_.sync_log_succ(log_ts, log_cb->get_callbacks()))) {
      TRANS_LOG(ERROR, "mt ctx sync log failed", KR(ret), K(*log_cb), K(*this));
    } else if (OB_SUCCESS != (tmp_ret = mt_ctx_.remove_callbacks_for_fast_commit())) {
      TRANS_LOG(WARN, "cleanout callbacks for fast commit", K(ret), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::try_submit_next_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogType log_type = ObTxLogType::UNKNOWN;
  if (ObPartTransAction::COMMIT == part_trans_action_ && !is_2pc_logging_() && !is_in_2pc_()) {
    if (is_sub2pc() || exec_info_.is_dup_tx_) {
      log_type = ObTxLogType::TX_COMMIT_INFO_LOG;
    } else {
      if (is_local_tx_()) {
        log_type = ObTxLogType::TX_COMMIT_LOG;
      } else {
        log_type = ObTxLogType::TX_PREPARE_LOG;
      }
    }
    if (OB_FAIL(submit_log_impl_(log_type))) {
      TRANS_LOG(WARN, "submit log for commit failed", K(ret), K(log_type), KPC(this));
    } else {
      TRANS_LOG(INFO, "submit log for commit success", K(log_type), KPC(this));
    }
  }
  return ret;
}

int ObPartTransCtx::fix_redo_lsns_(const ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  LSN lsn;
  ObRedoLSNArray &redo_lsns = exec_info_.redo_lsns_;
  while (!redo_lsns.empty()) {
    lsn = redo_lsns[redo_lsns.count() - 1];
    if (lsn >= log_cb->get_lsn()) {
      redo_lsns.pop_back();
    } else {
      break;
    }
  }
  return ret;
}

int ObPartTransCtx::on_failure(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  {
    CtxLockGuard guard(lock_);
    exec_info_.next_log_entry_no_--;
    const ObTxLogType log_type = log_cb->get_last_log_type();
    const int64_t log_ts = log_cb->get_log_ts();
    // TODO, dingxi
    mt_ctx_.sync_log_fail(log_cb->get_callbacks());
    log_cb->get_mds_range().range_sync_failed();
    if (log_ts == ctx_tx_data_.get_start_log_ts()) {
      ctx_tx_data_.set_start_log_ts(OB_INVALID_TIMESTAMP);
    }
    if (ObTxLogTypeChecker::is_state_log(log_type)) {
      sub_state_.clear_state_log_submitting();
    }
    if (OB_FAIL(fix_redo_lsns_(log_cb))) {
      TRANS_LOG(ERROR, "fix redo lsns failed", KR(ret), K(*this));
    }
    if (ObTxLogType::TX_ROLLBACK_TO_LOG == log_type) {
      ObTxData *tx_data = log_cb->get_tx_data();
      if (OB_FAIL(ctx_tx_data_.free_tmp_tx_data(tx_data))) {
        TRANS_LOG(WARN, "free tx data failed", KR(ret), K(*this));
      } else {
        log_cb->set_tx_data(nullptr);
      }
    }
    return_log_cb_(log_cb);
    log_cb = NULL;
    if (ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
      sub_state_.clear_info_log_submitted();
    }
    if (busy_cbs_.is_empty() && get_downstream_state() < ObTxState::PREPARE) {
      sub_state_.clear_state_log_submitted();
    }
    if (busy_cbs_.is_empty() && !has_persisted_log_()) {
      // busy callback array is empty and trx has not persisted any log, exit here
      TRANS_LOG(WARN, "log sync failed, txn aborted without persisted log", KPC(this));
      if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
        TRANS_LOG(WARN, "do local tx abort failed", K(ret));
      }
      if (need_callback_scheduler_()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(defer_callback_scheduler_(OB_TRANS_KILLED, -1))) {
          TRANS_LOG(WARN, "notify scheduler txn killed fail", K(tmp_ret), K_(trans_id));
        } else {
          commit_cb_.disable();
          TRANS_LOG(INFO, "notify scheduler txn killed success", K_(trans_id));
        }
        ret = COVER_SUCC(tmp_ret);
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, on_fail_cb,
                               OB_ID(ret), ret,
                               OB_ID(log_type), (void*)log_type,
                               OB_ID(t), log_ts,
                               OB_ID(ctx_ref), get_ref());
    TRANS_LOG(INFO, "ObPartTransCtx::on_failure end", KR(ret), K(*this), KPC(log_cb));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this))) {
    TRANS_LOG(ERROR, "release ctx ref failed", KR(tmp_ret));
  }
  return ret;
}

int ObPartTransCtx::get_local_max_read_version_(int64_t &local_max_read_version)
{
  int ret = OB_SUCCESS;
  local_max_read_version = trans_service_->get_tx_version_mgr().get_max_read_ts();
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get_local_max_read_version_", KR(ret), K(local_max_read_version), K(*this));
  }
  return ret;
}

int ObPartTransCtx::get_gts_(int64_t &gts)
{
  int ret = OB_SUCCESS;
  MonotonicTs receive_gts_ts;
  const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
  const MonotonicTs stc_ahead = get_stc_() - MonotonicTs(GET_GTS_AHEAD_INTERVAL);
  ObITsMgr *ts_mgr = trans_service_->get_ts_mgr();

  if (sub_state_.is_gts_waiting()) {
    ret = OB_EAGAIN;
    TRANS_LOG(INFO, "gts is waiting", K(ret), K(*this));
  } else if (OB_FAIL(ts_mgr->get_gts(tenant_id_, stc_ahead, this, gts, receive_gts_ts))) {
    if (OB_EAGAIN == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = acquire_ctx_ref_())) {
        TRANS_LOG(ERROR, "acquire ctx ref failed", KR(tmp_ret), K(*this));
      } else {
        // REC_TRANS_TRACE_EXT2(tlog_, wait_get_gts, Y(log_type));
      }
      sub_state_.set_gts_waiting();
    } else {
      TRANS_LOG(WARN, "get gts failed", KR(ret), K(*this), K(stc_ahead));
    }
  } else {
    set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
  }
  TRANS_LOG(DEBUG, "get_gts_", KR(ret), K(gts), K(*this));

  return ret;
}

int ObPartTransCtx::wait_gts_elapse_commit_version_(bool &need_wait)
{
  int ret = OB_SUCCESS;
  need_wait = false;
  common::ObTimeGuard tg("part_ctx::wait_gts", 100 * 1000);

  ObITsMgr *ts_mgr = trans_service_->get_ts_mgr();

  if (OB_FAIL(ts_mgr->wait_gts_elapse(tenant_id_,
                                      ctx_tx_data_.get_commit_version(),
                                      this,
                                      need_wait))) {
    TRANS_LOG(WARN, "wait gts elapse failed", KR(ret), K(*this));
  } else if (need_wait) {
    tg.click();
    sub_state_.set_gts_waiting();
    if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), K(*this));
    }
    tg.click();
    TRANS_LOG(INFO, "need wait gts elapse", KR(ret), K(*this));
    REC_TRANS_TRACE_EXT2(tlog_, wait_gts_elapse, OB_ID(ctx_ref), get_ref());
  }
  if (tg.get_diff() > 100000) {
    TRANS_LOG(INFO, "wait gts debug cost too much time", K(tg), K(*this));
  }

  return ret;
}

int ObPartTransCtx::generate_prepare_version_()
{
  int ret = OB_SUCCESS;

  if (!mt_ctx_.is_prepared()) {
    int64_t gts = 0;
    int64_t local_max_read_version = 0;
    bool is_gts_ok = false;
    // Only the root participant require to request gts
    const bool need_gts = is_root();

    if (need_gts) {
      if (OB_FAIL(get_gts_(gts))) {
        if (OB_EAGAIN == ret) {
          is_gts_ok = false;
          TRANS_LOG(INFO, "get gts eagain", KR(ret), KPC(this));
          ret = OB_SUCCESS;
        } else {
          is_gts_ok = false;
          TRANS_LOG(ERROR, "get gts failed", KR(ret), K(*this));
        }
      } else {
        is_gts_ok = true;
      }
    }

    if (OB_SUCC(ret)
        && ((need_gts && is_gts_ok)
            || !need_gts)) {
      // To order around the read-write conflict(anti dependency), we need push
      // the txn version upper than all previous read version. So we record all
      // read version each access begins and get the max read version to handle
      // the dependency conflict
      mt_ctx_.before_prepare(gts);
      if (OB_FAIL(get_local_max_read_version_(local_max_read_version))) {
        TRANS_LOG(WARN, "get local max read version failed", KR(ret), K(*this));
      } else {
        // should not overwrite the prepare version of other participants
        exec_info_.prepare_version_ = std::max(std::max(gts, local_max_read_version),
                                               exec_info_.prepare_version_);
        if (exec_info_.prepare_version_ > gts) {
          mt_ctx_.before_prepare(exec_info_.prepare_version_);
        }
      }
    }
  }

  return ret;
}

// for single-ls transaction, commit version may later be updated by log ts
int ObPartTransCtx::generate_commit_version_()
{
  int ret = OB_SUCCESS;
  if (ctx_tx_data_.get_commit_version() == ObTransVersion::INVALID_TRANS_VERSION) {
    int64_t gts = 0;
    if (OB_FAIL(get_gts_(gts))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "get gts failed", KR(ret), K(*this));
      }
    } else {
      // the same as before prepare
      mt_ctx_.set_trans_version(gts);
      const int64_t max_read_ts = trans_service_->get_tx_version_mgr().get_max_read_ts();
      if (OB_FAIL(ctx_tx_data_.set_commit_version(max(gts, max_read_ts)))) {
        TRANS_LOG(WARN, "set tx data commit version", K(ret));
      }
      TRANS_LOG(DEBUG, "generate_commit_version_", KR(ret), K(gts), K(max_read_ts), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::fill_redo_log_(char *buf,
                                   const int64_t buf_len,
                                   int64_t &pos,
                                   ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;
  const bool log_for_lock_node = true;
      // !(is_local_tx_() && (part_trans_action_ == ObPartTransAction::COMMIT));

  if (OB_UNLIKELY(NULL == buf || buf_len < 0 || pos < 0 || buf_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(buf), K(buf_len), K(pos), K(*this));
  } else if (OB_SUCCESS
             != (ret = mt_ctx_.fill_redo_log(buf, buf_len, pos, helper, log_for_lock_node))) {
    if (OB_EAGAIN != ret && OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "fill redo log failed", KR(ret), K(*this));
    }
  }

  return ret;
}

// starting from 0
int64_t ObPartTransCtx::get_redo_log_no_() const
{
  return exec_info_.redo_lsns_.count();
}

int ObPartTransCtx::submit_redo_log_(ObTxLogBlock &log_block,
                                     bool &has_redo,
                                     ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;
  int64_t mutator_size = 0;
  bool need_continue = true;
  bool need_submit_log = false;
  bool need_undo_log = false;
  ObTxLogCb *log_cb = NULL;

  while (OB_SUCC(ret) && need_continue) {
    ObTxRedoLog redo_log(clog_encrypt_info_, get_redo_log_no_(), cluster_version_);
    mutator_size = 0;
    need_undo_log = false;
    need_submit_log = false;
    log_cb = NULL;
    helper.reset();

    if (OB_FAIL(exec_info_.redo_lsns_.reserve(exec_info_.redo_lsns_.count() + 1))) {
      TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
    } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FAIL(log_block.prepare_mutator_buf(redo_log))) {
      TRANS_LOG(WARN, "prepare mutator buf failed", KR(ret));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else {
      ret = fill_redo_log_(redo_log.get_mutator_buf(), redo_log.get_mutator_size(), mutator_size, helper);
      if (OB_SUCCESS == ret) {
        has_redo = true;
        if (OB_FAIL(log_block.finish_mutator_buf(redo_log, mutator_size))) {
          TRANS_LOG(WARN, "finish mutator buf failed", KR(ret), K(*this));
        } else if (OB_FAIL(log_block.add_new_log(redo_log))) {
          TRANS_LOG(WARN, "add redo log failed", KR(ret), K(*this));
        } else {
          need_continue = false;
        }
        if (OB_FAIL(ret)) {
          return_log_cb_(log_cb);
          log_cb = NULL;
          need_undo_log = true;
        }
      } else if (OB_EAGAIN == ret) {
        has_redo = true;
        if (OB_FAIL(log_block.finish_mutator_buf(redo_log, mutator_size))) {
          TRANS_LOG(WARN, "finish mutator buf failed", KR(ret), K(*this));
        } else if (OB_FAIL(log_block.add_new_log(redo_log))) {
          TRANS_LOG(WARN, "add redo log failed", KR(ret), K(*this));
        } else {
          need_submit_log = true;
        }
        if (OB_FAIL(ret)) {
          return_log_cb_(log_cb);
          log_cb = NULL;
          need_undo_log = true;
        }
      } else if (OB_ENTRY_NOT_EXIST == ret || OB_BLOCK_FROZEN == ret) {
        // rewrite ret
        ret = (OB_ENTRY_NOT_EXIST == ret) ? OB_SUCCESS : ret;
        has_redo = false;
        if (OB_SUCC(ret) && OB_FAIL(log_block.finish_mutator_buf(redo_log, 0))) {
          TRANS_LOG(WARN, "finish mutator buf failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = NULL;
        }
        need_continue = false;
      } else if (OB_ERR_TOO_BIG_ROWSIZE == ret) {
        TRANS_LOG(ERROR, "too big row size", K(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
      } else {
        TRANS_LOG(WARN, "fill redo log failed", KR(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
      }
    }
    if (need_submit_log) {
      if (OB_FAIL(acquire_ctx_ref_())) {
        TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
      } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
              log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
        TRANS_LOG(WARN, "submit log failed", KR(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
        need_undo_log = true;
        release_ctx_ref_();
      } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
      } else {
        // TRANS_LOG(INFO, "submit redo log in clog adapter success", K(*log_cb));
        has_redo = false;
      }
    } else {
      if (NULL != log_cb) {
        return_log_cb_(log_cb);
        log_cb = NULL;
      }
    }

    if (need_undo_log) {
      has_redo = false;
    }
  }

  return ret;
}

int ObPartTransCtx::submit_redo_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  ObTxLogCb *log_cb = NULL;
  bool has_redo = false;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObRedoLogSubmitHelper helper;
  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_redo_log_(log_block, has_redo, helper))) {
    // this function is called by freezing and instant logging,
    // don't need to handle OB_BLOCK_FROZEN ret
    if (OB_BLOCK_FROZEN == ret) {
      TRANS_LOG(INFO, "submit log meets frozen memtable", KR(ret), K(*this));
    } else if (REACH_TIME_INTERVAL(100 * 1000)) {
      TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
    }
  } else if (!has_redo) {
    // do nothing
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
    }
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
    release_ctx_ref_();
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
  } else {
    // TRANS_LOG(INFO, "submit redolog in clog adapter success", K(*log_cb));
    log_cb = NULL;
  }

  return ret;
}

int ObPartTransCtx::submit_redo_commit_info_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  bool has_redo = false;
  ObTxLogCb *log_cb = NULL;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObRedoLogSubmitHelper helper;
  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (sub_state_.is_info_log_submitted()) {
    // state log already submitted, do nothing
  } else if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_redo_commit_info_log_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
    }
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
    release_ctx_ref_();
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
  } else {
    // TRANS_LOG(INFO, "submit redo and commit_info log in clog adapter success", K(*log_cb));
    reset_redo_lsns_();
    log_cb = NULL;
  }

  return ret;
}

int ObPartTransCtx::validate_commit_info_log_(const ObTxCommitInfoLog &commit_info_log)
{
  int ret = OB_SUCCESS;

  if (!commit_info_log.get_scheduler().is_valid()
      || commit_info_log.get_redo_lsns().count() != exec_info_.redo_lsns_.count()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid commit info log", K(ret), K(commit_info_log), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::submit_redo_commit_info_log_(ObTxLogBlock &log_block,
                                                 bool &has_redo,
                                                 ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;
  ObTxLogCb *log_cb = NULL;

  if (sub_state_.is_info_log_submitted()) {
    // state log already submitted, do nothing
  } else if (OB_FAIL(submit_redo_log_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
  } else {
    ObTxCommitInfoLog commit_info_log(
        exec_info_.scheduler_, exec_info_.participants_, exec_info_.upstream_,
        false, // sub2pc_
        exec_info_.is_dup_tx_, can_elr_, trace_info_.get_app_trace_id(),
        trace_info_.get_app_trace_info(), exec_info_.prev_record_lsn_, exec_info_.redo_lsns_,
        exec_info_.incremental_participants_, cluster_version_, exec_info_.xid_);

    if (OB_FAIL(validate_commit_info_log_(commit_info_log))) {
      TRANS_LOG(WARN, "invalid commit info log", K(ret), K(commit_info_log), K(trans_id_),
                K(ls_id_));
    } else if (OB_FAIL(log_block.add_new_log(commit_info_log))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        // TRANS_LOG(WARN, "buf not enough", K(ret), K(commit_info_log), KPC(this));
        if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
          if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
            TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
          }
        } else if (log_block.get_cb_arg_array().count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
          return_log_cb_(log_cb);
          log_cb = NULL;
          // acquire ctx ref before submit log
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                       log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
          TRANS_LOG(WARN, "submit log failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = NULL;
          release_ctx_ref_();
        } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        } else {
          log_cb = NULL;
          if (OB_FAIL(validate_commit_info_log_(commit_info_log))) {
            TRANS_LOG(WARN, "invalid commit info log", K(ret), K(commit_info_log), K(trans_id_),
                      K(ls_id_));
          } else if (OB_FAIL(log_block.add_new_log(commit_info_log))) {
            TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
          }
          has_redo = false;
        }
      } else {
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::submit_redo_active_info_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  bool has_redo = false;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObRedoLogSubmitHelper helper;
  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
    TRANS_LOG(WARN, "submit multi source data failed", K(ret), K(*this));
  } else if (OB_FAIL(submit_redo_log_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
  } else {
    int64_t cur_submitted_seq_no = max(exec_info_.max_submitted_seq_no_, helper.max_seq_no_);
    ObTxActiveInfoLog active_info_log(exec_info_.scheduler_, exec_info_.trans_type_, session_id_,
                                      trace_info_.get_app_trace_id(),
                                      mt_ctx_.get_min_table_version(), can_elr_,
                                      addr_,                 //
                                      cur_query_start_time_, //
                                      false,                 // sub2pc
                                      exec_info_.is_dup_tx_,
                                      trans_expired_time_, epoch_, last_op_sn_, first_scn_,
                                      last_scn_, cur_submitted_seq_no,
                                      cluster_version_);
    bool redo_log_submitted = false;
    ObTxLogCb *log_cb = nullptr;
    if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), KP(log_cb), K(*this));
    } else if (OB_FAIL(log_block.add_new_log(active_info_log))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        TRANS_LOG(WARN, "buf not enough", K(ret), K(active_info_log));
        if (log_block.get_cb_arg_array().count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
          return_log_cb_(log_cb);
          log_cb = NULL;
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
          TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = nullptr;
          release_ctx_ref_();
        } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        } else {
          redo_log_submitted = true;
          // TRANS_LOG(INFO, "submit redo log success", K(*log_cb));
          log_cb = nullptr;
          if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
            TRANS_LOG(WARN, "get log cb failed", KR(ret), KP(log_cb), K(*this));
          } else if (OB_FAIL(log_block.add_new_log(active_info_log))) {
            TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = nullptr;
          } else if (log_block.get_cb_arg_array().count() == 0) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (OB_FAIL(acquire_ctx_ref_())) {
            TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
          } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                         log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
            TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = nullptr;
            release_ctx_ref_();
          } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
          } else {
            TRANS_LOG(INFO, "submit active info success", K(*log_cb));
            log_cb = nullptr;
          }
        }
      } else {
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = nullptr;
      }
    } else if (log_block.get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                   log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
      TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
      return_log_cb_(log_cb);
      log_cb = nullptr;
      release_ctx_ref_();
    } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
    } else {
      TRANS_LOG(INFO, "submit active info success", K(*log_cb));
      log_cb = nullptr;
    }
  }

  return ret;
}

int ObPartTransCtx::submit_prepare_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  palf::LSN prev_lsn;
  bool has_redo = false;
  bool redo_log_submitted = false;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObRedoLogSubmitHelper helper;
  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (!sub_state_.is_info_log_submitted()) {
    prev_lsn.reset();
    if (OB_FAIL(submit_multi_data_source_(log_block))) {
      TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
    } else if (OB_FAIL(submit_redo_commit_info_log_(log_block, has_redo, helper))) {
      TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
    } else {
      // do nothing
    }
  }
  

  if (OB_SUCC(ret)) {
    ObTxLogCb *log_cb = NULL;

    if (OB_FAIL(get_prev_log_lsn_(log_block, ObTxLogType::TX_COMMIT_INFO_LOG, prev_lsn))) {
      TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
    } 

    ObTxPrepareLog prepare_log(exec_info_.incremental_participants_, prev_lsn);

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FAIL(log_block.add_new_log(prepare_log))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        // TRANS_LOG(WARN, "buf not enough", K(ret), K(prepare_log));
        if (log_block.get_cb_arg_array().count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
          return_log_cb_(log_cb);
          log_cb = NULL;
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
          TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = NULL;
          release_ctx_ref_();
        } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        } else {
          redo_log_submitted = true;
          prepare_log.set_prev_lsn(log_cb->get_lsn());
          // TRANS_LOG(INFO, "submit redo and commit_info log success", K(*log_cb));
          log_cb = NULL;
          if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
            if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
              TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
            }
          } else if (OB_FAIL(log_block.add_new_log(prepare_log))) {
            TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (log_block.get_cb_arg_array().count() == 0) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (OB_FAIL(acquire_ctx_ref_())) {
            TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
          } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                         log_block.get_buf(), log_block.get_size(), exec_info_.prepare_version_,
                         log_cb, false))) {
            TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = NULL;
            release_ctx_ref_();
          } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
          } else {
            // TRANS_LOG(INFO, "submit prepare log in clog adapter success", K(*log_cb));
            log_cb = NULL;
          }
        }
      } else {
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
      }
    } else if (log_block.get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                   log_block.get_buf(), log_block.get_size(), exec_info_.prepare_version_, log_cb,
                   false))) {
      TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
      return_log_cb_(log_cb);
      log_cb = NULL;
      release_ctx_ref_();
    } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
    } else {
      redo_log_submitted = true;
      // TRANS_LOG(INFO, "submit redo,commit_info and prepare log in clog adapter success", K(*log_cb));
      log_cb = NULL;
    }
  }

  return ret;
}

// For liboblog, the log_ts of commit log must be larger than commit_version_
int ObPartTransCtx::submit_commit_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  palf::LSN prev_lsn;
  bool has_redo = false;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObRedoLogSubmitHelper helper;
  ObTxBufferNodeArray multi_source_data;
  ObTxLogBlockHeader log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(gen_final_mds_array_(multi_source_data))) {
    TRANS_LOG(WARN, "gen total multi source data failed", KR(ret), K(*this));
  } else if (is_local_tx_()) {
    if (!sub_state_.is_info_log_submitted()) {
      prev_lsn.reset();
      if (OB_FAIL(submit_multi_data_source_(log_block))) {
        TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
      } else if (OB_FAIL(submit_redo_commit_info_log_(log_block, has_redo, helper))) {
        TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t checksum =
      (exec_info_.need_checksum_ && !is_incomplete_replay_ctx_ ? mt_ctx_.calc_checksum_all() : 0);
    int64_t log_commit_version = ObTransVersion::INVALID_TRANS_VERSION;

    if (!is_local_tx_()) {
      log_commit_version = ctx_tx_data_.get_commit_version();
      if (OB_FAIL(get_prev_log_lsn_(log_block, ObTxLogType::TX_PREPARE_LOG, prev_lsn))) {
        TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
      } else if (!prev_lsn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected prev lsn in 2pc commit log", K(ret), KPC(this));
      }
    } else {
      if (OB_FAIL(get_prev_log_lsn_(log_block, ObTxLogType::TX_COMMIT_INFO_LOG, prev_lsn))) {
        TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
      }
    }

    ObTxCommitLog commit_log(log_commit_version, checksum, exec_info_.incremental_participants_,
                             multi_source_data, exec_info_.trans_type_, prev_lsn,
                             coord_prepare_info_arr_);
    ObTxLogCb *log_cb = NULL;
    bool redo_log_submitted = false;

    if (OB_SUCC(ret)) {
      const ObTxData *tx_data = NULL;
      auto guard = ctx_tx_data_.get_tx_data();
      if (OB_FAIL(guard.get_tx_data(tx_data))) {
        TRANS_LOG(WARN, "get tx data failed", K(ret));
      } else if (OB_FAIL(commit_log.init_tx_data_backup(tx_data))) {
        TRANS_LOG(WARN, "init tx data backup failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(log_block.add_new_log(commit_log))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        TRANS_LOG(WARN, "buf not enough", K(ret), K(commit_log));
        if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
          if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
            TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
          }
        } else if (log_block.get_cb_arg_array().count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
          return_log_cb_(log_cb);
          log_cb = NULL;
          // acquire ctx ref before submit log
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                       log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
          TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = NULL;
          release_ctx_ref_();
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        } else {
          redo_log_submitted = true;
          commit_log.set_prev_lsn(log_cb->get_lsn());
          // TRANS_LOG(INFO, "submit redo and commit_info log in clog adapter success", K(*log_cb));
          log_cb = NULL;
          if (OB_FAIL(prepare_log_cb_(NEED_FINAL_CB, log_cb))) {
            if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
              TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
            }
          } else if (OB_FAIL(log_block.add_new_log(commit_log))) {
            TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = NULL;
          // } else if (need_commit_barrier()
          //            && OB_FAIL(log_block.rewrite_barrier_log_block(trans_id_.get_id(), true))) {
          //   TRANS_LOG(WARN, "rewrite commit log barrier failed", K(ret));
          //   return_log_cb_(log_cb);
          //   log_cb = NULL;
          } else if (log_block.get_cb_arg_array().count() == 0) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (OB_FAIL(acquire_ctx_ref_())) {
            TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
          } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                         log_block.get_buf(), log_block.get_size(),
                         ctx_tx_data_.get_commit_version(), log_cb, false))) {
            TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = NULL;
            release_ctx_ref_();
          } else {
            // sp trans update it's commit version
            if (OB_SUCC(ret) && is_local_tx_()) {
              int tmp_ret = OB_SUCCESS;
              if (OB_SUCCESS
                  != (tmp_ret = ctx_tx_data_.set_commit_version(final_log_cb_.get_log_ts()))) {
                TRANS_LOG(WARN, "set commit version failed", K(tmp_ret));
              }
            }
            if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
              // do nothing
            }
          }
        }
      } else {
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
      }
    // } else if (need_commit_barrier()
    //            && OB_FAIL(log_block.rewrite_barrier_log_block(trans_id_.get_id(), true))) {
    //   TRANS_LOG(WARN, "rewrite commit log barrier failed", K(ret));
    //   return_log_cb_(log_cb);
    //   log_cb = NULL;
    } else if (log_block.get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(prepare_log_cb_(NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                   log_block.get_buf(), log_block.get_size(), ctx_tx_data_.get_commit_version(),
                   log_cb, false))) {
      TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
      release_ctx_ref_();
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else {
      // sp trans update it's commit version
      if (OB_SUCC(ret) && is_local_tx_()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ctx_tx_data_.set_commit_version(final_log_cb_.get_log_ts()))) {
          TRANS_LOG(WARN, "set commit version failed", K(tmp_ret));
        }
      }
      if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        TRANS_LOG(WARN, "after submit log failed", K(ret));
      } else {
        redo_log_submitted = true;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::submit_abort_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogCb *log_cb = NULL;
  ObTxLogBlock log_block;
  ObTxBufferNodeArray tmp_array;

  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxLogBlockHeader
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(gen_final_mds_array_(tmp_array, false))) {
    TRANS_LOG(WARN, "gen abort mds array failed", K(ret));
  }

  ObTxAbortLog abort_log(tmp_array);

  if (OB_SUCC(ret)) {
    const ObTxData *tx_data = NULL;
    auto guard = ctx_tx_data_.get_tx_data();
    if (OB_FAIL(guard.get_tx_data(tx_data))) {
      TRANS_LOG(WARN, "get tx data failed", K(ret));
    } else if (OB_FAIL(abort_log.init_tx_data_backup(tx_data))) {
      TRANS_LOG(WARN, "init tx data backup failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(log_block.add_new_log(abort_log))) {
    TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(prepare_log_cb_(NEED_FINAL_CB, log_cb))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
    }
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
    release_ctx_ref_();
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
  } else {
    // TRANS_LOG(INFO, "submit abort log in clog adapter success", K(*log_cb));
    reset_redo_lsns_();
  }

  return ret;
}

int ObPartTransCtx::submit_clear_log_() {
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  ObTxClearLog clear_log(exec_info_.incremental_participants_);
  ObTxLogCb *log_cb = NULL;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxLogBlockHeader log_block_header(cluster_id_,
                                      exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
    }
  } else if (OB_FAIL(log_block.add_new_log(clear_log))) {
    TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(),
                 MAX(ctx_tx_data_.get_end_log_ts(), max_2pc_commit_scn_),
                 log_cb, false))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
    release_ctx_ref_();
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
  } else {
    // TRANS_LOG(INFO, "submit clear log in clog adapter success", K(*log_cb));
    log_cb = NULL;
  }

  return ret;
}

int ObPartTransCtx::submit_record_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  ObTxRecordLog record_log(exec_info_.prev_record_lsn_, exec_info_.redo_lsns_);
  ObTxLogCb *log_cb = NULL;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxLogBlockHeader
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
    }
  } else if (OB_FAIL(log_block.add_new_log(record_log))) {
    TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
    return_log_cb_(log_cb);
    log_cb = NULL;
    release_ctx_ref_();
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
  } else {
    TRANS_LOG(INFO, "submit record log", K(*this));
    reset_redo_lsns_();
    set_prev_record_lsn_(log_cb->get_lsn());
    log_cb = NULL;
  }

  return ret;
}

int ObPartTransCtx::submit_log_impl_(const ObTxLogType log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_) {
    switch (log_type) {
    // for instant logging during execution,
    // use non-block clog interface.
    case ObTxLogType::TX_REDO_LOG: {
      ret = submit_redo_log_();
      break;
    }
    // leader switch
    case ObTxLogType::TX_ACTIVE_INFO_LOG: {
      ret = submit_redo_active_info_log_();
      break;
    }
    // for xa and duplicate table
    // TODO. need submit log in sync log callback
    case ObTxLogType::TX_COMMIT_INFO_LOG: {
      ret = submit_redo_commit_info_log_();
      break;
    }
    case ObTxLogType::TX_PREPARE_LOG: {
      // try generate prepare verison
      ret = generate_prepare_version_();

      if (OB_SUCC(ret) && mt_ctx_.is_prepared()) {
        ret = submit_prepare_log_();
      }
      break;
    }
    case ObTxLogType::TX_COMMIT_LOG: {
      if (!mt_ctx_.is_prepared()) {
        if (!is_local_tx_()) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "not set prepare verison into mt_ctx, unexpected error", "ret", tmp_ret, K(*this));
          print_trace_log_();
        } else {
          // try generate commit version
          ret = generate_commit_version_();
        }
      }
      if (OB_SUCC(ret) && mt_ctx_.is_prepared()) {
        ret = submit_commit_log_();
      }
      break;
    }
    case ObTxLogType::TX_ABORT_LOG: {
      ret = submit_abort_log_();
      break;
    }
    case ObTxLogType::TX_CLEAR_LOG: {
      ret = submit_clear_log_();
      break;
    }
    case ObTxLogType::TX_MULTI_DATA_SOURCE_LOG: {
      ret = submit_multi_data_source_();
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown submit log type");
    }
    }
  }

  if (OB_FAIL(ret) && REACH_TIME_INTERVAL(100 * 1000)) {
    TRANS_LOG(WARN, "submit_log_impl_ failed", KR(ret), K(log_type), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "submit_log_impl_ end", KR(ret), K(log_type), K(*this));
#endif
  }
  if (OB_TX_NOLOGCB == ret) {
    if (REACH_COUNT_PER_SEC(10) && REACH_TIME_INTERVAL(100 * 1000)) {
      TRANS_LOG(INFO, "can not get log_cb when submit_log", KR(ret), K(log_type),
                KPC(busy_cbs_.get_first()));
    }
    if (ObTxLogType::TX_PREPARE_LOG == log_type ||
        ObTxLogType::TX_COMMIT_LOG == log_type ||
        ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
      // need submit log in log sync callback
      // rewrite ret
      ret = OB_SUCCESS;
      TRANS_LOG(DEBUG, "ignore OB_TX_NOLOGCB", K(log_type), K(*this));
    }
  }

  return ret;
}

void ObPartTransCtx::reset_redo_lsns_()
{
  exec_info_.redo_lsns_.reset();
}

void ObPartTransCtx::set_prev_record_lsn_(const LogOffSet &prev_record_lsn)
{
  exec_info_.prev_record_lsn_ = prev_record_lsn;
}

bool ObPartTransCtx::need_record_log_() const
{
  // Record Log will be generated if the number of log ids
  // is no less than the max size of prev_redo_log_ids_
  uint64_t prev_redo_lsns_count = MAX_PREV_LOG_IDS_COUNT;
   #ifdef ERRSIM
  // Error injection test, used for changing prev_redo_lsns_count for test
  int tmp_ret = E(EventTable::EN_LOG_IDS_COUNT_ERROR) OB_SUCCESS;
  if (tmp_ret != OB_SUCCESS) {
    prev_redo_lsns_count = 2;
    TRANS_LOG(INFO, "need_record_log: ", K(prev_redo_lsns_count));
  }
  #endif
  return get_redo_log_no_() >= prev_redo_lsns_count && !sub_state_.is_info_log_submitted();
}

//big row may use a unused log_block to invoke after_submit_log_
int ObPartTransCtx::after_submit_log_(ObTxLogBlock &log_block,
                                      ObTxLogCb *log_cb,
                                      ObRedoLogSubmitHelper *helper)
{
  int ret = OB_SUCCESS;
  const ObTxCbArgArray &cb_arg_array = log_block.get_cb_arg_array();
  if (cb_arg_array.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(*this));
  } else if (OB_FAIL(log_cb->get_cb_arg_array().assign(cb_arg_array))) {
    TRANS_LOG(WARN, "assign cb arg array failed", K(ret));
  } else {
    if (is_contain(cb_arg_array, ObTxLogType::TX_REDO_LOG) ||
        is_contain(cb_arg_array, ObTxLogType::TX_ROLLBACK_TO_LOG) ||
        is_contain(cb_arg_array, ObTxLogType::TX_MULTI_DATA_SOURCE_LOG)) {
      if (!is_contain(cb_arg_array, ObTxLogType::TX_COMMIT_INFO_LOG)) {
        ret = exec_info_.redo_lsns_.push_back(log_cb->get_lsn());
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(update_rec_log_ts_(false/*for_replay*/))) {
    TRANS_LOG(WARN, "update rec log ts failed", KR(ret), KPC(log_cb), K(*this));
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_REDO_LOG)) {
    log_cb->set_callbacks(helper->callbacks_);
    update_max_submitted_seq_no(helper->max_seq_no_);
    if (OB_FAIL(mt_ctx_.log_submitted(*helper))) {
      TRANS_LOG(ERROR, "fill to do log_submitted on redo log gen", K(ret), K(*this));
    }
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_ROLLBACK_TO_LOG)) {
    // do nothing
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_MULTI_DATA_SOURCE_LOG)) {
    // do nothing
    log_cb->get_mds_range().range_submitted(mds_cache_);
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_ACTIVE_INFO_LOG)) {
    // do nothing
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_COMMIT_INFO_LOG)) {
    sub_state_.set_info_log_submitted();
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_PREPARE_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
    exec_info_.prepare_version_ = std::max(log_cb->get_log_ts(), exec_info_.prepare_version_);
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_COMMIT_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
    // elr
    if (can_elr_) {
      if (OB_FAIL(ctx_tx_data_.set_state(ObTxData::ELR_COMMIT))) {
        TRANS_LOG(WARN, "set tx data state", K(ret));
      }
       elr_handler_.check_and_early_lock_release(this);
    }
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_ABORT_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_CLEAR_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_TIMESTAMP == ctx_tx_data_.get_start_log_ts()) {
      if (OB_FAIL(ctx_tx_data_.set_start_log_ts(log_cb->get_log_ts()))) {
        TRANS_LOG(WARN, "set tx data start log ts failed", K(ret), K(ctx_tx_data_));
      }
    }
  }
  if(OB_FAIL(ret)) {
    TRANS_LOG(WARN, "after submit log failed", K(ret), K(trans_id_), K(ls_id_), K(exec_info_), K(*log_cb));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "after submit log success", K(ret), K(trans_id_), K(ls_id_), K(exec_info_), K(*log_cb));
#endif
  }

  REC_TRANS_TRACE_EXT(tlog_,
                      after_submit_log,
                      OB_ID(ret),
                      ret,
                      OB_ID(log_no),
                      exec_info_.next_log_entry_no_,
                      OB_ID(t),
                      log_cb->get_log_ts(),
                      OB_ID(lsn),
                      log_cb->get_lsn());

  exec_info_.next_log_entry_no_++;
  ObTxLogBlockHeader
    block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);
  log_block.reuse(trans_id_, block_header);
  return ret;
}

int ObPartTransCtx::prepare_log_cb_(const bool need_final_cb, ObTxLogCb *&log_cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_log_cb_(need_final_cb, log_cb)) && REACH_TIME_INTERVAL(100 * 1000)) {
    TRANS_LOG(WARN, "failed to get log_cb", KR(ret), K(*this));
  }
  return ret;
}

int ObPartTransCtx::get_log_cb_(const bool need_final_cb, ObTxLogCb *&log_cb)
{
  int ret = OB_SUCCESS;
  if (need_final_cb) {
    if (final_log_cb_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trx is submitting final log", K(ret), K(*this));
    } else {
      log_cb = &final_log_cb_;
    }
  } else {
    if (free_cbs_.is_empty()) {
      ret = OB_TX_NOLOGCB;
      //TRANS_LOG(INFO, "all log cbs are busy now, try again later", K(ret), K(*this));
    } else if (free_cbs_.get_size() <= RESERVE_LOG_CALLBACK_COUNT_FOR_FREEZING &&
        ATOMIC_LOAD(&is_submitting_redo_log_for_freeze_) == false) {
      ret = OB_TX_NOLOGCB;
      //TRANS_LOG(INFO, "reserve log callback for freezing, try again later", K(ret), K(*this));
    } else if (OB_ISNULL(log_cb = free_cbs_.remove_first())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected null log cb", KR(ret), K(*this));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    log_cb->reuse();
    busy_cbs_.add_last(log_cb);
  }
  return ret;
}

int ObPartTransCtx::return_log_cb_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  if (NULL != log_cb) {
    busy_cbs_.remove(log_cb);
    log_cb->reuse();
    if ((&final_log_cb_) != log_cb) {
      free_cbs_.add_first(log_cb);
    }
  }
  return ret;
}

int ObPartTransCtx::get_max_submitting_log_info_(palf::LSN &lsn, int64_t &log_ts)
{
  int ret = OB_SUCCESS;
  ObTxLogCb *log_cb = NULL;
  lsn = LSN(palf::PALF_INITIAL_LSN_VAL);
  log_ts = OB_INVALID_TIMESTAMP;
  if (!busy_cbs_.is_empty()) {
    log_cb = busy_cbs_.get_last();
    if (OB_ISNULL(log_cb)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "log cb is NULL, unexpected error", K(ret));
    } else if (!log_cb->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "log cb is invalid", K(ret));
    } else {
      lsn = log_cb->get_lsn();
      log_ts = log_cb->get_log_ts();
    }
  }
  return ret;
}

int ObPartTransCtx::get_prev_log_lsn_(const ObTxLogBlock &log_block,
                                      ObTxLogType prev_log_type,
                                      palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  palf::LSN tmp_lsn;
  int64_t tmp_log_ts = 0;
  bool in_same_block = false;

  if (is_contain(log_block.get_cb_arg_array(), prev_log_type)) {
    // invalid lsn
    lsn.reset();
    in_same_block = true;
  } else if (OB_FAIL(get_max_submitting_log_info_(tmp_lsn, tmp_log_ts))) {
  } else if (tmp_lsn.is_valid()) {
    if (exec_info_.max_durable_lsn_.is_valid() && exec_info_.max_durable_lsn_ > tmp_lsn) {
      tmp_lsn = exec_info_.max_durable_lsn_;
    }
    lsn = tmp_lsn;
  } else {
    lsn = exec_info_.max_durable_lsn_;
  }

  if (!in_same_block && !lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected prev lsn", K(ret), K(log_block), K(prev_log_type), K(lsn), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::switch_log_type_(const ObTxLogType log_type,
                                     ObTwoPhaseCommitLogType &ret_log_type)
{
  int ret = OB_SUCCESS;
  switch (log_type) {
  case ObTxLogType::TX_COMMIT_INFO_LOG: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO;
    break;
  }
  case ObTxLogType::TX_PREPARE_LOG: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE;
    break;
  }
  case ObTxLogType::TX_COMMIT_LOG: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT;
    break;
  }
  case ObTxLogType::TX_ABORT_LOG: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT;
    break;
  }
  case ObTxLogType::TX_CLEAR_LOG: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR;
    break;
  }
  default: {
    ret_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_INIT;
    TRANS_LOG(WARN, "unknown log type", KR(ret), K(log_type), K(*this));
  }
  }
  return ret;
}

int ObPartTransCtx::switch_log_type_(const ObTwoPhaseCommitLogType &log_type,
                                     ObTxLogType &ret_log_type)
{
  int ret = OB_SUCCESS;
  switch (log_type) {
  case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO: {
    ret_log_type = ObTxLogType::TX_COMMIT_INFO_LOG;
    break;
  }
  case ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE: {
    ret_log_type = ObTxLogType::TX_PREPARE_LOG;
    break;
  }
  case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT: {
    ret_log_type = ObTxLogType::TX_COMMIT_LOG;
    break;
  }
  case ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT: {
    ret_log_type = ObTxLogType::TX_ABORT_LOG;
    break;
  }
  case ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR: {
    ret_log_type = ObTxLogType::TX_CLEAR_LOG;
    break;
  }
  default: {
    ret_log_type = ObTxLogType::UNKNOWN;
    TRANS_LOG(WARN, "unknown log type", KR(ret), K(log_type), K(*this));
  }
  }
  return ret;
}

int ObPartTransCtx::submit_log(const ObTwoPhaseCommitLogType &log_type)
{
  int ret = OB_SUCCESS;
  ObTxLogType tx_log_type = ObTxLogType::UNKNOWN;
  if (sub_state_.is_gts_waiting()) {
    TRANS_LOG(INFO, "wait for gts when logging", K(log_type), K(*this));
  } else if (OB_FAIL(switch_log_type_(log_type, tx_log_type))) {
    TRANS_LOG(WARN, "switch log type failed", KR(ret), K(log_type), K(*this));
  } else if (OB_FAIL(submit_log_impl_(tx_log_type))) {
    TRANS_LOG(WARN, "submit log failed", KR(ret), K(log_type), K(*this));
  }
  return ret;
}

int ObPartTransCtx::try_submit_next_log()
{
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  return try_submit_next_log_();
}

bool ObPartTransCtx::is_2pc_logging() const { return is_2pc_logging_(); }

uint64_t ObPartTransCtx::get_participant_id()
{
  int ret = OB_SUCCESS;
  uint64_t participant_id = UINT64_MAX;

  if (OB_FAIL(find_participant_id_(ls_id_, participant_id))) {
    TRANS_LOG(ERROR, "find participant id failed", K(*this));
  }

  return participant_id;
}

int ObPartTransCtx::find_participant_id_(const ObLSID &participant, uint64_t &participant_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  participant_id = UINT64_MAX;

  for (int64_t i = 0; !found && i < exec_info_.participants_.count(); i++) {
    if (participant == exec_info_.participants_[i]) {
      participant_id = i;
      found = true;
    }
  }

  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

bool ObPartTransCtx::is_root() const { return !exec_info_.upstream_.is_valid(); }

bool ObPartTransCtx::is_leaf() const
{
  return exec_info_.participants_.empty()
    // root must not be leaf, because the distributed txn must be composed by
    // more than one participants.
    && !is_root();
}

//***************************** for 4.0
int ObPartTransCtx::check_replay_avaliable_(const palf::LSN &offset,
                                            const int64_t &timestamp,
                                            const int64_t &part_log_no,
                                            bool &need_replay)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_follower_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(timestamp), K(offset), K(trans_id_));
    ret = OB_INVALID_ARGUMENT;
  // } else if (is_exiting_) {
  //   // ret = OB_TRANS_CTX_NOT_EXIST;
  } else {
    need_replay = true;
    // check state like ObPartTransCtx::is_trans_valid_for_replay_
    if (exec_info_.max_applying_log_ts_ == OB_INVALID_TIMESTAMP) {
      // do nothing
    } else if (exec_info_.max_applying_log_ts_ > timestamp) {
      need_replay = false;
    } else if (exec_info_.max_applying_log_ts_ == timestamp
               && exec_info_.max_applying_part_log_no_ > part_log_no) {
      need_replay = false;
    } else {
      // TODO check log_type and state
    }
  }

  if (need_replay && 0 == start_replay_ts_)
  {
    start_replay_ts_ = timestamp;
  }

  if (need_replay) {
    update_rec_log_ts_(true/*for_replay*/, timestamp);
  }

  return ret;
}

int ObPartTransCtx::push_repalying_log_ts(const int64_t log_ts_ns)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (log_ts_ns < exec_info_.max_applying_log_ts_) {
    TRANS_LOG(WARN,
              "[Replay Tx] replay a log with a older ts than part_ctx state, it will be ignored",
              K(exec_info_.max_applying_log_ts_), K(log_ts_ns));
  } else if (log_ts_ns > exec_info_.max_applying_log_ts_) {
    exec_info_.max_applying_log_ts_ = log_ts_ns;
    exec_info_.max_applying_part_log_no_ = 0;
  }

  return ret;
}

int ObPartTransCtx::push_replayed_log_ts(int64_t log_ts_ns, const palf::LSN &offset)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (exec_info_.max_applied_log_ts_ < log_ts_ns) {
    exec_info_.max_applied_log_ts_ = log_ts_ns;
  }

  if (exec_info_.max_durable_lsn_.is_valid()) {
    if (offset > exec_info_.max_durable_lsn_) {
      exec_info_.max_durable_lsn_ = offset;
    }
  } else {
    exec_info_.max_durable_lsn_ = offset;
  }

  return ret;
}

int ObPartTransCtx::update_replaying_log_no_(int64_t log_ts_ns, int64_t part_log_no)
{
  int ret = OB_SUCCESS;

  if (exec_info_.max_applying_log_ts_ != log_ts_ns) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected replaying log ts", K(ret), K(exec_info_.max_applying_log_ts_),
              K(log_ts_ns));
  } else if (exec_info_.max_applying_part_log_no_ != part_log_no
             && exec_info_.max_applying_part_log_no_ + 1 != part_log_no) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected replaying log no", K(ret), K(exec_info_.max_applying_part_log_no_),
              K(part_log_no));
  } else {
    exec_info_.max_applying_part_log_no_ = part_log_no;
  }

  return ret;
}

int ObPartTransCtx::check_trans_type_for_replay_(const int32_t &trans_type,
                                                 const int64_t &commit_log_ts)
{
  int ret = OB_SUCCESS;

  if (start_replay_ts_ == commit_log_ts) {
    // TRANS_LOG(INFO, "start replay from commit log", K(trans_type), K(commit_log_ts));
    exec_info_.trans_type_ = trans_type;
  } else if (exec_info_.trans_type_ != trans_type) {
    // ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "trans type not match", K(trans_type), K(commit_log_ts), K(*this));
  }
  return ret;
}

int ObPartTransCtx::replay_update_tx_data_(const bool commit,
                                           const int64_t log_ts,
                                           const int64_t commit_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
    TRANS_LOG(WARN, "set end log ts failed", K(ret));
  } else if (commit) {
    if (TransType::SP_TRANS == exec_info_.trans_type_
        && ObTransVersion::INVALID_TRANS_VERSION == commit_version) {
      if (OB_FAIL(ctx_tx_data_.set_commit_version(log_ts))) {
        TRANS_LOG(WARN, "set commit_version failed", K(ret));
      }
    } else if (TransType::DIST_TRANS == exec_info_.trans_type_
               && ObTransVersion::INVALID_TRANS_VERSION != commit_version) {
      if (OB_FAIL(ctx_tx_data_.set_commit_version(commit_version))) {
        TRANS_LOG(WARN, "set commit_version failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unkown trans type or commit_version", K(ret), K(log_ts), K(commit_version));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_tx_data_.set_state(ObTxData::COMMIT))) {
      TRANS_LOG(WARN, "set tx data state failed", K(ret));
    }
  } else {
    if (OB_FAIL(ctx_tx_data_.set_state(ObTxData::ABORT))) {
      TRANS_LOG(WARN, "set tx data state failed", K(ret));
    }
  }

  return ret;
}

int ObPartTransCtx::replace_tx_data_with_backup_(const ObTxDataBackup &backup, int64_t log_ts_ns)
{
  int ret = OB_SUCCESS;
  if (backup.get_start_log_ts() != ObTxDataBackup::PENDING_START_LOG_TS) {
    log_ts_ns = backup.get_start_log_ts();
  }

  if (OB_FAIL(ctx_tx_data_.set_start_log_ts(log_ts_ns))) {
    TRANS_LOG(WARN, "update tx data with backup failed.", KR(ret), K(ctx_tx_data_));
  }

  TRANS_LOG(DEBUG, "replace tx_data", K(ret), K(backup), K(ctx_tx_data_), K(*this));
  return ret;
}

void ObPartTransCtx::force_no_need_replay_checksum()
{
  exec_info_.need_checksum_ = false;
}

void ObPartTransCtx::check_no_need_replay_checksum(int64_t log_ts_ns)
{
  // TODO(handora.qc): How to lock the tx_ctx

  // checksum_log_ts_ means all data's checksum has been calculated before the
  // log of checksum_log_ts_(not included). So if the data with this log_ts is
  // not replayed with checksum_log_ts_ <= log_ts, it means may exist some data
  // will never be replayed because the memtable will filter the data.
  if (exec_info_.checksum_log_ts_ <= log_ts_ns) {
    exec_info_.need_checksum_ = false;
  }
}

int ObPartTransCtx::validate_replay_log_entry_no(bool first_created_ctx,
                                                 int64_t log_entry_no,
                                                 int64_t log_ts)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (first_created_ctx) {

    if (0 == log_entry_no) {
      if (OB_FAIL(ctx_tx_data_.set_start_log_ts(log_ts))) {
        TRANS_LOG(WARN, "set start ts failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      exec_info_.next_log_entry_no_ = log_entry_no + 1;
      if (exec_info_.next_log_entry_no_ > 1) {
        is_incomplete_replay_ctx_ = true;
        exec_info_.need_checksum_ = false;
        if (OB_FAIL(supplement_undo_actions_if_exist_())) {
          TRANS_LOG(WARN,
                    "supplement undo actions to a tx data when replaying a transaction from the "
                    "middle failed.",
                    K(ret));
        }

        TRANS_LOG(INFO,
                  "This is a incomplete trx which replay from the middle",
                  K(ret),
                  K(ls_id_),
                  K(trans_id_),
                  K(log_ts),
                  K(exec_info_.next_log_entry_no_),
                  K(is_incomplete_replay_ctx_),
                  K(ctx_tx_data_));
      }
    }
  } else if (log_entry_no > exec_info_.next_log_entry_no_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
              "unexpected log entry no",
              K(ret),
              K(trans_id_),
              K(ls_id_),
              K(log_ts),
              K(log_entry_no),
              K(exec_info_.next_log_entry_no_));
  } else if (log_entry_no < exec_info_.next_log_entry_no_) {
    // do nothing, filtered by max_applying_log_ts_
  } else {
    exec_info_.next_log_entry_no_ = log_entry_no + 1;
  }

  return ret;
}

int ObPartTransCtx::check_and_merge_redo_lsns_(const palf::LSN &offset)
{
  int ret = OB_SUCCESS;
  int64_t cnt = exec_info_.redo_lsns_.count();
  // TODO merge without same offset
  if (cnt != 0 && exec_info_.redo_lsns_[cnt - 1] == offset) {
    TRANS_LOG(INFO, "repeated redo log offset", K(offset), K(exec_info_.redo_lsns_), K(trans_id_));
  } else if (OB_FAIL(exec_info_.redo_lsns_.push_back(offset))) {
    TRANS_LOG(WARN, "redo log offset push back error", K(ret), K(offset), K(trans_id_));
  }
  return ret;
}

int ObPartTransCtx::replay_redo_in_ctx(const ObTxRedoLog &redo_log,
                                       const palf::LSN &offset,
                                       const int64_t &timestamp,
                                       const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_redo_in_ctx", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::INVALID;
  bool need_replay = true;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(redo_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (OB_FAIL((update_replaying_log_no_(timestamp, part_log_no)))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else {
    // TODO add clog_encrypt_info_
    UNUSED(set_upstream_state(ObTxState::INIT));
    cluster_version_ = redo_log.get_cluster_version();
    // if (0 == redo_log.get_log_no()) {
    //   if (OB_FAIL(ctx_tx_data_.set_start_log_ts(timestamp))) {
    //     TRANS_LOG(WARN, "set start ts failed", K(ret));
    //   }
    // }
    if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(offset))) {
      TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
    }
    // const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_redo_log_replay_count(tenant_id_, 1);
    ObTransStatistic::get_instance().add_redo_log_replay_time(tenant_id_, timeguard.get_diff());
  }
  REC_TRANS_TRACE_EXT(tlog_, replay_redo, OB_ID(ret), ret,
                                          OB_ID(used), timeguard.get_diff(),
                                          Y(need_replay), OB_ID(offset), offset.val_,
                                          OB_ID(t), timestamp, OB_ID(ctx_ref), get_ref());
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay Redo in TxCtx Failed", K(ret), K(timestamp), K(offset),
              K(need_replay), K(redo_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay Redo in TxCtx", K(ret), K(timestamp), K(offset),
              K(need_replay), K(redo_log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_rollback_to(const ObTxRollbackToLog &log,
                                       const palf::LSN &offset,
                                       const int64_t &timestamp,
                                       const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_rollback_to", 10 * 1000);
  // int64_t start = ObTimeUtility::fast_current_time();
  CtxLockGuard guard(lock_);
  bool need_replay = true;
  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(log), K(timestamp), K(offset), K(*this));
  } else if (OB_FAIL((update_replaying_log_no_(timestamp, part_log_no)))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(rollback_to_savepoint_(log.get_from(), log.get_to()))) {
    TRANS_LOG(WARN, "replay savepoint_rollback fail", K(ret), K(log), K(offset), K(timestamp),
              KPC(this));
  } else if (OB_FAIL(check_and_merge_redo_lsns_(offset))) {
    TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mt_ctx_.rollback(log.get_to(), log.get_from()))) {
      TRANS_LOG(WARN, "mt ctx rollback fail", K(ret), K(log), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay RollbackToLog in TxCtx Failed", K(timestamp), K(offset),
              K(ret), K(need_replay), K(log), KPC(this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay RollbackToLog in TxCtx", K(timestamp), K(offset), K(ret),
              K(need_replay), K(log), KPC(this));
#endif
  }
  REC_TRANS_TRACE_EXT(tlog_,
                      replay_rollback_to,
                      OB_ID(ret),
                      ret,
                      OB_ID(used),
                      timeguard.get_diff(),
                      Y(need_replay),
                      OB_ID(offset),
                      offset.val_,
                      OB_ID(t),
                      timestamp,
                      OB_ID(ctx_ref),
                      get_ref());
  return ret;
}

int ObPartTransCtx::replay_active_info(const ObTxActiveInfoLog &log,
                                       const palf::LSN &offset,
                                       const int64_t &timestamp,
                                       const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_active_info", 10 * 1000);
  bool need_replay = true;
  // int64_t start = ObTimeUtility::fast_current_time();
  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(set_scheduler_(log.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_app_trace_id_(log.get_app_trace_id()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(log), K(*this));
  } else {
    exec_info_.trans_type_ = log.get_trans_type();
    if (log.is_dup_tx()) {
      set_dup_table_tx();
    }
    trans_expired_time_ = log.get_tx_expired_time();
    session_id_ = log.get_session_id();
    // schema_version
    can_elr_ = log.is_elr();
    // cur_query_start_time
    // sub2pc
    epoch_ = log.get_epoch();
    last_op_sn_ = log.get_last_op_sn();
    first_scn_ = log.get_first_scn();
    last_scn_ = log.get_last_scn();
    cluster_version_ = log.get_cluster_version();
    update_max_submitted_seq_no(log.get_max_submitted_seq_no());
    exec_info_.data_complete_ = true;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay Active Info in TxCtx Failed", K(timestamp), K(offset), K(ret),
              K(need_replay), K(log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay Active Info in TxCtx", K(timestamp), K(offset), K(ret),
              K(need_replay), K(log), K(*this));
#endif
  }
  REC_TRANS_TRACE_EXT(tlog_, replay_active_info, OB_ID(ret), ret,
                             OB_ID(used), timeguard.get_diff(),
                             OB_ID(ctx_ref), get_ref());
  return ret;
}

int ObPartTransCtx::replay_commit_info(const ObTxCommitInfoLog &commit_info_log,
                                       const palf::LSN &offset,
                                       const int64_t &timestamp,
                                       const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_commit_info", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;
  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(commit_info_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(exec_info_.redo_lsns_.assign(commit_info_log.get_redo_lsns()))) {
    TRANS_LOG(WARN, "set redo log offsets error", K(commit_info_log), K(*this));
  } else if (OB_FAIL(set_scheduler_(commit_info_log.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), K(commit_info_log), K(*this));
  } else if (OB_FAIL(exec_info_.participants_.assign(commit_info_log.get_participants()))) {
    TRANS_LOG(WARN, "set participants error", K(ret), K(commit_info_log), K(*this));
  } else if (OB_FAIL(exec_info_.incremental_participants_.assign(
                 commit_info_log.get_incremental_participants()))) {
    TRANS_LOG(WARN, "set incremental_participants error", K(ret), K(commit_info_log), K(*this));
  } else if (OB_FAIL(set_app_trace_info_(commit_info_log.get_app_trace_info()))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(commit_info_log), K(*this));
  } else if (OB_FAIL(set_app_trace_id_(commit_info_log.get_app_trace_id()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(commit_info_log), K(*this));
  } else {
    set_durable_state_(ObTxState::REDO_COMPLETE);
    set_2pc_upstream_(commit_info_log.get_upstream());
    exec_info_.xid_ = commit_info_log.get_xid();
    can_elr_ = commit_info_log.is_elr();
    cluster_version_ = commit_info_log.get_cluster_version();
    sub_state_.set_info_log_submitted();
    // if (0 == redo_log_no_) {
    //   tx_data_->end_log_ts_ = redo_log_no_;
    // }
    if (exec_info_.participants_.count() > 1) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else if (exec_info_.upstream_.is_valid()) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else if (is_sub2pc()) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else {
      exec_info_.trans_type_ = TransType::SP_TRANS;
    }
    if (commit_info_log.is_dup_tx()) {
      set_dup_table_tx();
      mt_ctx_.before_prepare(timestamp);
    }
    reset_redo_lsns_();
    ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
    if (is_incomplete_replay_ctx_) {
      // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
    } else if (OB_FAIL(switch_log_type_(commit_info_log.LOG_TYPE, two_phase_log_type))) {
      TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
    } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
      TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END,
                                         timestamp,
                                         true,
                                         exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "notify data source for TX_END failed", KR(ret), K(*this));
  }

  const int64_t used_time = timeguard.get_diff();
  REC_TRANS_TRACE_EXT2(tlog_, replay_commit_info, OB_ID(ret), ret,
      OB_ID(used), used_time,
      OB_ID(offset), offset.val_, OB_ID(t), timestamp,
      OB_ID(ctx_ref), get_ref());
  // TODO add commit_state_log statistics
  // ObTransStatistic::get_instance().add_redo_log_replay_count(tenant_id_, 1);
  // ObTransStatistic::get_instance().add_redo_log_replay_time(tenant_id_, end - start);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] replay commit info Failed", K(ret), K(used_time), K(timestamp), K(offset),
              K(commit_info_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] replay commit info", K(ret), K(used_time), K(timestamp), K(offset),
              K(commit_info_log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_prepare(const ObTxPrepareLog &prepare_log,
                                   const palf::LSN &offset,
                                   const int64_t &timestamp,
                                   const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_prepare", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(prepare_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(exec_info_.incremental_participants_.assign(
                 prepare_log.get_incremental_participants()))) {
    TRANS_LOG(WARN, "set incremental_participants error", K(ret), K(*this));
  } else {
    exec_info_.prepare_version_ = timestamp;
    exec_info_.trans_type_ = TransType::DIST_TRANS;
    mt_ctx_.set_prepare_version(timestamp);
    ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
    if (is_incomplete_replay_ctx_) {
      // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
    } else if (OB_FAIL(switch_log_type_(prepare_log.LOG_TYPE, two_phase_log_type))) {
      TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
    } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
      TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
    }
    ObLSLogInfo temp_info(ls_id_, offset);
    if (OB_SUCC(ret) && OB_FAIL(merge_prepare_log_info_(temp_info))) {
      TRANS_LOG(WARN, "push log stream info failed", KR(ret), K(*this));
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay prepare log error", KR(ret), "context", *this, K(prepare_log));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_PREPARE, timestamp, true,
                                         exec_info_.multi_data_source_))) {

    TRANS_LOG(WARN, "notify data source for ON_PREPARE failed", KR(ret), K(*this));
  } else {
    sub_state_.set_state_log_submitted();
  }

  const int64_t used_time = timeguard.get_diff();
  REC_TRANS_TRACE_EXT2(tlog_, replay_prepare, OB_ID(ret), ret, OB_ID(used),
                       used_time, OB_ID(offset), offset.val_,
                       OB_ID(t), timestamp, OB_ID(ctx_ref), get_ref());
  ObTransStatistic::get_instance().add_prepare_log_replay_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_prepare_log_replay_time(tenant_id_, used_time);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] replay prepare log failed", K(ret), K(used_time), K(timestamp),
              K(offset), K(prepare_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] replay prepare log", K(ret), K(used_time), K(timestamp), K(offset),
              K(prepare_log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_commit(const ObTxCommitLog &commit_log,
                                  const palf::LSN &offset,
                                  const int64_t &timestamp,
                                  const int64_t &part_log_no,
                                  const int64_t replay_compact_version)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_commit", 10 * 1000);

  // const int64_t start = ObTimeUtility::fast_current_time();
  const int64_t commit_version = commit_log.get_commit_version();
  bool need_replay = true;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  // TODO replace participants_ with prepare_log_info_arr_ for transfer
  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (OB_FAIL(check_trans_type_for_replay_(commit_log.get_trans_type(), timestamp))) {
    TRANS_LOG(WARN, "check trans type for replay failed", K(ret), K(timestamp), K(commit_log),
              K(*this));
  } else if (OB_FAIL(replay_update_tx_data_(true, timestamp, commit_version))) {
    TRANS_LOG(WARN, "replay update tx data failed", KR(ret), K(*this));
  } else if (OB_FAIL(replace_tx_data_with_backup_(commit_log.get_tx_data_backup(), timestamp))) {
    TRANS_LOG(WARN, "replace tx data with backup failed", KR(ret), K(*this));
  } else if (!need_replay) {
    // TODO insert_into_tx_table before need_replay and give it the ability to retry
    TRANS_LOG(INFO, "need not replay log", K(commit_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(exec_info_.incremental_participants_.assign(
                 commit_log.get_incremental_participants()))) {
    TRANS_LOG(WARN, "set incremental_participants error", K(ret), K(*this));
  } else {
    if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
        && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, timestamp, offset))) {
      TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(this));
    }
    if (is_local_tx_()) {
      set_durable_state_(ObTxState::COMMIT);
      set_upstream_state(ObTxState::COMMIT);
    } else {
      ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
      if (is_incomplete_replay_ctx_) {
        // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on
        // 2PC
      } else if (OB_FAIL(switch_log_type_(commit_log.LOG_TYPE, two_phase_log_type))) {
        TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
      } else if (OB_FAIL(coord_prepare_info_arr_.assign(commit_log.get_ls_log_info_arr()))) {
        TRANS_LOG(WARN, "assign coord_prepare_info_arr_ failed", K(ret));
      } else if (is_root()
                 && OB_FAIL(exec_info_.prepare_log_info_arr_.assign(commit_log.get_ls_log_info_arr()))) {
        TRANS_LOG(WARN, "assigin prepare_log_info_arr_ failed", K(ret));
      } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
        TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t checksum =
        (exec_info_.need_checksum_ && !is_incomplete_replay_ctx_ ? commit_log.get_checksum() : 0);
    mt_ctx_.set_replay_compact_version(replay_compact_version);

    if (OB_FAIL(trans_replay_commit_(ctx_tx_data_.get_commit_version(),
                                     timestamp,
                                     cluster_version_,
                                     checksum))) {
      TRANS_LOG(WARN, "trans replay commit failed", KR(ret), "context", *this);
    } else if (OB_FAIL(notify_data_source_(NotifyType::ON_COMMIT,
                                         timestamp,
                                         true,
                                         exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(commit_log));
    } else if ((!ctx_tx_data_.is_read_only()) && OB_FAIL(ctx_tx_data_.insert_into_tx_table())) {
      TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
    } else if (is_local_tx_()) {
      if (OB_FAIL(trans_clear_())) {
        TRANS_LOG(WARN, "transaction clear error or trans_type is sp_trans", KR(ret), "context", *this);
      } else {
        set_exiting_();
      }
    }
    ls_tx_ctx_mgr_->update_max_replay_commit_version(ctx_tx_data_.get_commit_version());
  }
  if (OB_SUCC(ret)) {
    sub_state_.set_state_log_submitted();
  }

  const int64_t used_time = timeguard.get_diff();
  REC_TRANS_TRACE_EXT2(tlog_, replay_commit, OB_ID(ret), ret, OB_ID(used), used_time, OB_ID(offset),
                       offset.val_, OB_ID(t), timestamp, OB_ID(ctx_ref), get_ref());
  ObTransStatistic::get_instance().add_commit_log_replay_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_commit_log_replay_time(tenant_id_, used_time);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] replay commit log failed", K(ret), K(used_time), K(timestamp),
              K(offset), K(commit_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] replay commit log", K(ret), K(used_time), K(timestamp), K(offset),
              K(commit_log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_clear(const ObTxClearLog &clear_log,
                                 const palf::LSN &offset,
                                 const int64_t &timestamp,
                                 const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_clear", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(clear_log), K(timestamp), K(offset), K(*this));
    // no need to replay
    if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
    }
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(exec_info_.incremental_participants_.assign(
                 clear_log.get_incremental_participants()))) {
    TRANS_LOG(WARN, "set incremental_participants error", K(ret), K(*this));
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
  } else {
    if (is_local_tx_()) {
      //TODO  ignore err_code when replay from a middle log without tx_ctx_table_info
      TRANS_LOG(WARN, "unexpected clear log", KPC(this), K(clear_log));
      print_trace_log_();
      // ret = OB_ERR_UNEXPECTED;
    } else {
      ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
      if (is_incomplete_replay_ctx_) {
        // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
      } else if (OB_FAIL(switch_log_type_(clear_log.LOG_TYPE, two_phase_log_type))) {
        TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
      } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
        TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      set_exiting_();
    }
  }
  if (OB_SUCC(ret)) {
    sub_state_.set_state_log_submitted();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay clear log error", KR(ret), "context", *this, K(clear_log));
  } else {
    TRANS_LOG(DEBUG, "replay clear log success", "context", *this, K(clear_log));
  }
  const int64_t used_time = timeguard.get_diff();
  REC_TRANS_TRACE_EXT2(tlog_, replay_clear, OB_ID(ret), ret, OB_ID(used),
                       used_time, OB_ID(offset), offset.val_,
                       OB_ID(t), timestamp, OB_ID(ctx_ref), get_ref());
  ObTransStatistic::get_instance().add_clear_log_replay_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_clear_log_replay_time(tenant_id_, used_time);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] replay clear log failed", K(ret), K(used_time), K(timestamp), K(offset),
              K(clear_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] replay clear log", K(ret), K(used_time), K(timestamp), K(offset),
              K(clear_log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_abort(const ObTxAbortLog &abort_log,
                                 const palf::LSN &offset,
                                 const int64_t &timestamp,
                                 const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;

  common::ObTimeGuard timeguard("replay_abort", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = false;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (OB_FAIL(replay_update_tx_data_(false, timestamp, -1 /*unused*/))) {
    TRANS_LOG(WARN, "replay update tx data failed", KR(ret), K(*this));
  } else if (OB_FAIL(replace_tx_data_with_backup_(abort_log.get_tx_data_backup(), timestamp))) {
    TRANS_LOG(WARN, "replace tx data with backup failed", KR(ret), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(abort_log), K(timestamp), K(offset), K(*this));
    // no need to replay,
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else {
    if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
        && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, timestamp, offset))) {
      TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(this));
    }
    if (is_local_tx_()) {
      set_durable_state_(ObTxState::ABORT);
      set_upstream_state(ObTxState::ABORT);
    } else {
      ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
      if (is_incomplete_replay_ctx_) {
        // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
      } else if (OB_FAIL(switch_log_type_(abort_log.LOG_TYPE, two_phase_log_type))) {
        TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
      } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
        TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // we must notify mds tx_end before invoking trans_replay_abort_ for clearing tablet lock 
    ObTxBufferNodeArray tmp_array;
    if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
      TRANS_LOG(WARN, "gen total mds array failed", K(ret));
    } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END,
                                           timestamp,
                                           true,
                                           exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "notify data source for TX_END failed", KR(ret), K(*this));
    } else if (OB_FAIL(trans_replay_abort_(timestamp))) {
      TRANS_LOG(WARN, "transaction replay end error", KR(ret), "context", *this);
    } else if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
    } else if (OB_FAIL(notify_data_source_(NotifyType::ON_ABORT, timestamp, true, tmp_array))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(abort_log));
    } else if ((!ctx_tx_data_.is_read_only()) && OB_FAIL(ctx_tx_data_.insert_into_tx_table())) {
      TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
    } else {
      reset_redo_lsns_();
      set_exiting_();
    }
  }
  if (OB_SUCC(ret)) {
    sub_state_.set_state_log_submitted();
  }
  const int64_t used_time = timeguard.get_diff();
  REC_TRANS_TRACE_EXT2(tlog_, replay_abort, OB_ID(ret), ret, OB_ID(used),
                       used_time, OB_ID(offset), offset.val_,
                       OB_ID(t), timestamp, OB_ID(ctx_ref), get_ref());
  ObTransStatistic::get_instance().add_abort_log_replay_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_abort_log_replay_time(tenant_id_, used_time);

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] replay abort log failed", K(ret), K(used_time), K(timestamp),
              K(offset), K(abort_log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] replay abort log", K(ret), K(used_time), K(timestamp), K(offset),
              K(abort_log), K(*this));
#endif
  }

  return ret;
}

int ObPartTransCtx::replay_multi_data_source(const ObTxMultiDataSourceLog &log,
                                             const palf::LSN &lsn,
                                             const int64_t &timestamp,
                                             const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_multi_data_source", 10 * 1000);
  bool need_replay = false;
  CtxLockGuard guard(lock_);

  const int64_t start = ObTimeUtility::current_time();

  bool repeat_replay = (timestamp == exec_info_.max_applied_log_ts_);

  int64_t additional_index = exec_info_.multi_data_source_.count();
  if (OB_FAIL(check_replay_avaliable_(lsn, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(lsn), K(timestamp), K(*this));
  } else if (!need_replay || repeat_replay) {
    TRANS_LOG(INFO, "need not replay log", K(need_replay), K(repeat_replay), K(log), K(timestamp), K(lsn), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(deep_copy_mds_array(log.get_data(), false))) {
    TRANS_LOG(WARN, "deep copy mds array failed", K(ret));
  } 

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(
                 notify_data_source_(NotifyType::REGISTER_SUCC, timestamp, true, log.get_data()))) {
    TRANS_LOG(WARN, "notify data source for REGISTER_SUCC failed", K(ret));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_REDO, timestamp, true, log.get_data()))) {
    TRANS_LOG(WARN, "notify data source for ON_REDO failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(lsn))) {
    TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(lsn));
  }

  // rollback mds log replay
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = additional_index;
         i < exec_info_.multi_data_source_.count() && OB_SUCCESS == tmp_ret; i++) {

      ObTxBufferNode &node = exec_info_.multi_data_source_.at(i);
      if (nullptr != node.data_.ptr()) {
        mtl_free(node.data_.ptr());
        node.data_.assign_ptr(nullptr, 0);
      }
    }
    for (int64_t i = exec_info_.multi_data_source_.count() - 1;
         i >= additional_index && OB_SUCCESS == tmp_ret; i--) {
      if (OB_TMP_FAIL(exec_info_.multi_data_source_.remove(i))) {
        TRANS_LOG(WARN, "remove mds node in exec_info failed", K(ret));
      }
    }
  }
  REC_TRANS_TRACE_EXT2(tlog_, replay_multi_data_source, OB_ID(ret), ret, OB_ID(used),
                       timeguard.get_diff(), OB_ID(offset), lsn.val_, OB_ID(t), timestamp,
                       OB_ID(ctx_ref), get_ref());

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay MSD Redo in TxCtx Failed", K(ret), K(timestamp), K(lsn),
              K(need_replay), K(log), K(*this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay MSD Redo in TxCtx", K(ret), K(timestamp), K(lsn),
              K(need_replay), K(log), K(*this));
#endif
  }
  return ret;
}

int ObPartTransCtx::replay_record(const ObTxRecordLog &log,
                                  const palf::LSN &lsn,
                                  const int64_t &timestamp,
                                  const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_record", 10 * 1000);
  bool need_replay = false;
  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(lsn, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(lsn), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(log), K(timestamp), K(lsn), K(*this));
    // no need to replay
  } else if (update_replaying_log_no_(timestamp, part_log_no)) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else {
    reset_redo_lsns_();
    set_prev_record_lsn_(lsn);
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay Record in TxCtx Failed", K(ret), K(timestamp), K(lsn),
              K(need_replay), K(log), K(*this));
  } else {
    TRANS_LOG(INFO, "[Replay Tx] Replay Record in TxCtx", K(ret), K(timestamp), K(lsn),
              K(need_replay), K(log), K(*this));
  }

  return ret;
}

int64_t ObPartTransCtx::get_min_undecided_log_ts() const
{
  int64_t log_ts = INT64_MAX;
  CtxLockGuard guard(lock_);
  if (!busy_cbs_.is_empty()) {
    const ObTxLogCb *log_cb = busy_cbs_.get_first();
    if (OB_ISNULL(log_cb)) {
      TRANS_LOG(ERROR, "unexpected null ptr", K(*this));
    } else {
      log_ts = log_cb->get_log_ts();
    }
  }
  return log_ts;
}


bool ObPartTransCtx::is_committing_() const
{
  return ObTxState::INIT != exec_info_.state_ || ObPartTransAction::COMMIT == part_trans_action_
         || ObPartTransAction::ABORT == part_trans_action_;
}

int ObPartTransCtx::switch_to_leader(int64_t start_working_ts)
{
  int ret = OB_SUCCESS;

  common::ObTimeGuard timeguard("switch to leader", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  TxCtxStateHelper state_helper(role_state_);
  timeguard.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(DEBUG, "transaction is exiting", "context", *this);
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::TAKEOVER))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    const bool contain_table_lock = is_contain_mds_type_(ObTxDataSourceType::TABLE_LOCK);
    if (ObTxState::INIT == exec_info_.state_) {
      if (exec_info_.data_complete_ && !contain_table_lock) {
        if (OB_FAIL(mt_ctx_.replay_to_commit(false /*is_resume*/))) {
          TRANS_LOG(WARN, "replay to commit failed", KR(ret), K(*this));
        }
      } else {
        TRANS_LOG(WARN, "txn data incomplete, will be aborted", K(contain_table_lock), KPC(this));
        if (has_persisted_log_()) {
          if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
            TRANS_LOG(WARN, "abort tx failed", KR(ret), K(*this));
          }
        } else {
          TRANS_LOG(ERROR, "unexpected trx which has not persisted log", K(*this));
        }
      }
    } else {
      if (OB_FAIL(mt_ctx_.replay_to_commit(false /*is_resume*/))) {
        TRANS_LOG(WARN, "replay to commit failed", KR(ret), K(*this));
      }
    }
    timeguard.click();

    // The txn with is_exiting_ = true only need to maintain the state
    if (OB_SUCC(ret) && !is_exiting_) {
      // The request_id_ should be initialized to prevent the 2pc cannot be
      // driven if all participants transferring the leader
      generate_request_id_();
      if (ObTxState::INIT == exec_info_.state_ && exec_info_.data_complete_) {
        const int64_t left_time = trans_expired_time_ - ObClockGenerator::getRealClock();
        if (left_time < ObServerConfig::get_instance().trx_2pc_retry_interval) {
          trans_2pc_timeout_ = (left_time > 0 ? left_time : 100 * 1000);
        } else {
          trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
        }
      } else {
        trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
      }
      // do not post transaction message to avoid deadlock, bug#8257026
      // just register timeout task
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
        TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
      }
    }
    if (OB_SUCC(ret)) {
      exec_info_.data_complete_ = false;
      start_working_log_ts_ = start_working_ts;
    } else {
      state_helper.restore_state();
    }
    timeguard.click();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "switch to leader failed", KR(ret), K(ret), KPC(this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "switch to leader succeed", KPC(this));
#endif
  }
  REC_TRANS_TRACE_EXT2(tlog_, switch_to_leader,
                              OB_ID(ret), ret, OB_ID(used),
                              timeguard.get_diff(),
                              OB_ID(ctx_ref), get_ref());
  return ret;
}

inline bool ObPartTransCtx::need_callback_scheduler_() {
  return is_root()
    && !is_sub2pc()
    && ObPartTransAction::COMMIT == part_trans_action_
    && addr_ == exec_info_.scheduler_
    && commit_cb_.is_enabled()
    && !commit_cb_.is_inited();
}

// It's possile that a follower-state ctx experiences switch_to_follower_forcedly
// 1. in leader state
// 2. switch graceful and turned into follower state, but another sub-handler fails
// 3. resume_leader is called
// 4. resume_leader fails
// 5. revoke, and still in follower state
int ObPartTransCtx::switch_to_follower_forcedly(ObIArray<ObTxCommitCallback> &cb_array)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("switch_to_follower_forcely", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  TxCtxStateHelper state_helper(role_state_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    // do nothing
  } else if (is_follower_()) {
    TRANS_LOG(INFO, "current tx already follower", K(*this));
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::REVOKE))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    (void)unregister_timeout_task_();
    if (!has_persisted_log_() && !is_logging_()) {
      // has not persisted any log, exit here
      bool need_cb_scheduler = need_callback_scheduler_();
      if (need_cb_scheduler) {
        // disable commit_cb to prevent do_local_tx_end_ callback scheduler,
        // which cause deadlock
        commit_cb_.disable();
      }
      if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
        TRANS_LOG(WARN, "do local tx abort failed", K(ret));
      } else if (need_cb_scheduler) {
        ObTxCommitCallback cb;
        cb.init(trans_service_, trans_id_, OB_TRANS_KILLED, -1);
        if (OB_FAIL(cb_array.push_back(cb))) {
          TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
        }
      }
      if (OB_FAIL(ret) && need_cb_scheduler) {
        // recover commit_cb_ when failed
        commit_cb_.enable();
      }
      TRANS_LOG(INFO, "switch to follower forcely, txn aborted without persisted log", KPC(this));
    } else {
      // has persisted log, wait new leader to advance it
      if (OB_FAIL(mt_ctx_.commit_to_replay())) {
        TRANS_LOG(WARN, "commit to replay error", KR(ret), "context", *this);
      } else if (OB_FAIL(mt_ctx_.clean_unlog_callbacks())) {
        TRANS_LOG(WARN, "clear unlog callbacks", KR(ret), K(*this));
      } else {
        TRANS_LOG(DEBUG, "commit to replay success", "context", *this);
      }

      // special handle commit triggered by local call: coordinator colocate with scheduler
      // let scheduler retry commit with RPC if required
      if (need_callback_scheduler_()) {
        ObTxCommitCallback cb;
        // no CommitInfoLog has been submitted, txn must abort
        if (exec_info_.state_ == ObTxState::INIT && !sub_state_.is_info_log_submitted()) {
          cb.init(trans_service_, trans_id_, OB_TRANS_KILLED, -1);
        } else {
          // otherwise, txn either continue commit or abort, need retry to get final result
          cb.init(trans_service_, trans_id_, OB_NOT_MASTER, -1);
        }
        TRANS_LOG(INFO, "switch to follower forcely, notify txn commit result to scheduler",
                  "commit_result", cb.ret_, KPC(this));
        if (OB_FAIL(cb_array.push_back(cb))) {
          TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
        }
      }
    }
    if (OB_FAIL(ret)) {
      state_helper.restore_state();
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "switch to follower forcedly failed", KR(ret), KPC(this));
  } else {
    TRANS_LOG(INFO, "switch to follower forcedly success", KPC(this));
  }
  REC_TRANS_TRACE_EXT2(tlog_, switch_to_follower_forcely,
                              OB_ID(ret), ret,
                              OB_ID(used), timeguard.get_diff(),
                              OB_ID(ctx_ref), get_ref());
  return ret;
}

int ObPartTransCtx::switch_to_follower_gracefully(ObIArray<ObTxCommitCallback> &cb_array)
{
  int ret = OB_SUCCESS;
  bool need_submit_log = false;
  ObTxLogType log_type = ObTxLogType::TX_ACTIVE_INFO_LOG;
  common::ObTimeGuard timeguard("switch_to_follower_gracefully", 10 * 1000);
  CtxLockGuard guard(lock_);
  timeguard.click();
  TxCtxStateHelper state_helper(role_state_);
  if (is_exiting_) {
    // do nothing
  } else if (sub_state_.is_force_abort()) {
    // is aborting, skip
  } else if (is_follower_()) {
    TRANS_LOG(INFO, "current tx already follower", K(*this));
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::SWITCH_GRACEFUL))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    if (pending_write_) {
      TRANS_LOG(INFO, "current tx is executing stmt", K(*this));
      mt_ctx_.set_replay();
      need_submit_log = true;
    } else if (!is_committing_()) {
      need_submit_log = true;
    } else if (ObPartTransAction::COMMIT == part_trans_action_) {
      /* NOTE:
       * - If commitInfoLog has submitted, txn can continue on the new Leader
       *   the scheduler should retry commit, in order to:
       *   - detect commit timeout
       *   - query commit result when commit response message was lost
       * - If commitInfoLog hasn't submitted, commitInfoLog will be submitted
       *   the scheduler should retry commit, in order to:
       *   - found the txn has aborted or survived on the new Leader
       *   - detect commit timeout
       */
      if (exec_info_.state_ < ObTxState::REDO_COMPLETE && !sub_state_.is_info_log_submitted()) {
        need_submit_log = true;
        log_type = ObTxLogType::TX_COMMIT_INFO_LOG;
      }
      if (need_callback_scheduler_()) {
        ObTxCommitCallback cb;
        cb.init(trans_service_, trans_id_, OB_SWITCHING_TO_FOLLOWER_GRACEFULLY, -1);
        TRANS_LOG(INFO, "swtich to follower gracefully, notify scheduler retry", KPC(this));
        if (OB_FAIL(cb_array.push_back(cb))) {
          TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
        }
      }
    }
    timeguard.click();
    if (OB_SUCC(ret) && need_submit_log) {
      // We need merge all callbacklists before submitting active info
      (void)mt_ctx_.merge_multi_callback_lists_for_changing_leader();
      if (OB_FAIL(submit_log_impl_(log_type))) {
        // currently, if there is no log callback, switch leader would fail,
        // and resume leader would be called.
        // TODO dingxi, improve this logic
        TRANS_LOG(WARN, "submit active/commit info log failed", KR(ret), K(*this));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(mt_ctx_.commit_to_replay())) {
        TRANS_LOG(WARN, "commit to replay failed", KR(ret), K(*this));
      } else if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timeout handler error", KR(ret), KPC(this));
      }
    }

    timeguard.click();
    if (OB_FAIL(ret)) {
      state_helper.restore_state();
    }
  }
  REC_TRANS_TRACE_EXT2(tlog_, switch_to_follower_gracefully,
                              OB_ID(ret), ret,
                              OB_ID(log_type), log_type,
                              OB_ID(used), timeguard.get_diff(),
                              OB_ID(ctx_ref), get_ref());
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "switch to follower gracefully failed", KR(ret), K(ret), KPC(this), K(cb_array));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "switch to follower gracefully succeed", KPC(this), K(cb_array));
#endif
  }

  return ret;
}

// a ctx may be in leader state when resume_leader is called.
// assume 100 ctx in ctx mgr,
// 1. switch_to_follower_gracefully, traverse 100 ctx
//    and 50 of them switch to follower state, but the 51th fails,
//    at this time, there are 50 ctx in leader state and 50 ctx in follower state.
// 2. rollback the change, and would traverse all the 100 ctx, including
//    the 50 ctx in leader state, for such ctx, do nothing.
int ObPartTransCtx::resume_leader(int64_t start_working_ts)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("resume_leader", 10 * 1000);
  CtxLockGuard guard(lock_);
  TxCtxStateHelper state_helper(role_state_);
  if (is_exiting_) {
    // do nothing
  } else if (is_leader_()) {
    // do nothing
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::RESUME))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    if (OB_FAIL(mt_ctx_.replay_to_commit(true /*is_resume*/))) {
      TRANS_LOG(WARN, "replay to commit failed", KR(ret), K(*this));
    } else {
      if (ObTxState::INIT == exec_info_.state_) {
        const int64_t left_time = trans_expired_time_ - ObClockGenerator::getRealClock();
        if (left_time < ObServerConfig::get_instance().trx_2pc_retry_interval) {
          trans_2pc_timeout_ = (left_time > 0 ? left_time : 100 * 1000);
        } else {
          trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
        }
      } else {
        trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
      }
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
        TRANS_LOG(WARN, "register timeout handler error", KR(ret), KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      exec_info_.data_complete_ = false;
      start_working_log_ts_ = start_working_ts;
    }
    if (OB_FAIL(ret)) {
      state_helper.restore_state();
    }
  }
  REC_TRANS_TRACE_EXT2(tlog_, resume_leader, OB_ID(ret), ret,
                                             OB_ID(used), timeguard.get_diff(),
                                             OB_ID(ctx_ref), get_ref());
  TRANS_LOG(INFO, "resume leader", KR(ret), K(ret), KPC(this));
  return ret;
}
//*****************************

int ObPartTransCtx::check_with_tx_data(ObITxDataCheckFunctor &fn)
{
  // NB: You need notice the lock is not acquired during check
  int ret = OB_SUCCESS;
  const ObTxData *tx_data_ptr = NULL;
  auto guard = ctx_tx_data_.get_tx_data();
  if (OB_FAIL(guard.get_tx_data(tx_data_ptr))) {
  } else {
    // const ObTxData &tx_data = *tx_data_ptr;
    // NB: we must read the state then the version without lock. If you are interested in the
    // order, then you can read the comment in ob_tx_data_functor.cpp
    ObTxState state = exec_info_.state_;
    int64_t prepare_version = mt_ctx_.get_trans_version();
    ObTxCCCtx tx_cc_ctx(state, prepare_version);

    if (OB_FAIL(fn(*tx_data_ptr, &tx_cc_ctx))) {
      TRANS_LOG(WARN, "do data check function fail.", KR(ret), K(*this));
    } else {
      TRANS_LOG(DEBUG, "check with tx data", K(*this), K(fn));
    }
  }

  return ret;
}

int ObPartTransCtx::update_rec_log_ts_(bool for_replay, const int64_t rec_log_ts)
{
  int ret = OB_SUCCESS;

  // The semantic of the rec_log_ts means the log ts of the first state change
  // after previous checkpoint. So it is only updated if it is unset.
  if (OB_INVALID_TIMESTAMP == rec_log_ts_) {
    if (for_replay) {
      if (OB_INVALID_TIMESTAMP == rec_log_ts) {
        TRANS_LOG(WARN, "update rec log ts failed", K(*this), K(rec_log_ts));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // Case 1: As follower, the replay is in order, so we can simply set it as
        // the log ts of the latest replayed log. because all state changes in
        // logs before max_durable_log_ts all successfully contained in this
        // checkpoint and no new state changes after max_durable_log_ts is
        // contained in the checkpoint.
        rec_log_ts_ = rec_log_ts;
      }
    } else {
      // Case 2: As leader, the application is discrete and not in order, so we
      // should set it as the log ts of the first log submmitted during
      // continuous logging(we call it FCL later) because all log of the txn with
      // its log ts in front of the FCL must be contained in the checkpoint.
      //
      // NB(TODO(handora.qc)): Remember to reset it when replaying start working
      if (!busy_cbs_.is_empty()) {
        const ObTxLogCb *log_cb = busy_cbs_.get_first();
        if (OB_ISNULL(log_cb)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected null ptr", K(*this));
        } else {
          rec_log_ts_ = log_cb->get_log_ts();
        }
      } else {
        // there may exits if log cbs is empty
      }
    }
  }

  return ret;
}

// When checkpointing the tx ctx table, we need refresh the rec_log_ts for the
// next checkpoint. While we shouldnot return the refreshed rec_log_ts before
// the checkpoint of the tx ctx table is succeed. So we need remember the
// rec_log_ts using prev_rec_log_ts before successfully checkpointing
int ObPartTransCtx::refresh_rec_log_ts_()
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TIMESTAMP == prev_rec_log_ts_) {
    // We should remember the rec_log_ts before the tx ctx table is successfully
    // checkpointed
    prev_rec_log_ts_ = rec_log_ts_;

    if (is_follower_()) {
      // Case 1: As follower, the replay is indead in order, while we cannot
      // simply reset it because the replay is not atomic, and it may be in the
      // middle stage that the replay is currently on-going. So the necessary
      // state before the log ts that is replaying may not be contained, so we
      // need replay from the on-going log ts.
      if (exec_info_.max_applied_log_ts_ != exec_info_.max_applying_log_ts_) {
        rec_log_ts_ = exec_info_.max_applying_log_ts_;
      } else {
        rec_log_ts_ = OB_INVALID_TIMESTAMP;
      }
    } else {
      // Case 2: As leader, the application is discrete and not in order, so we
      // rely on the first lo(we call it FCL later)g hasnot been applied as
      // rec_log_ts if exists or reset it if not because all log of the txn with
      // its log ts in front of the FCL must be contained in the checkpoint.
      if (busy_cbs_.is_empty()) {
        rec_log_ts_ = OB_INVALID_TIMESTAMP;
      } else {
        const ObTxLogCb *log_cb = busy_cbs_.get_first();
        if (OB_ISNULL(log_cb)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected null ptr", K(*this));
        } else {
          rec_log_ts_ = log_cb->get_log_ts();
        }
      }
    }
  } else {
    // TODO(handora.qc): change to ERROR or enabling the exception
    TRANS_LOG(WARN, "we should not allow concurrent merge of tx ctx table", K(*this));
  }

  return ret;
}

int ObPartTransCtx::get_tx_ctx_table_info_(ObTxCtxTableInfo &info)
{
  int ret = OB_SUCCESS;
  {
    const ObTxData *tx_data = NULL;
    const ObTxCommitData *tx_commit_data = NULL;
    auto guard = ctx_tx_data_.get_tx_data();
    if (OB_FAIL(guard.get_tx_data(tx_data))) {
      TRANS_LOG(WARN, "get tx data failed", K(ret));
      // rewrite ret
      ret = OB_SUCCESS;
      if (OB_FAIL(ctx_tx_data_.get_tx_commit_data(tx_commit_data))) {
        TRANS_LOG(WARN, "get tx commit data failed", K(ret));
      } else {
        info.state_info_ = *tx_commit_data;
      }
    } else {
      info.state_info_ = *tx_data;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mt_ctx_.calc_checksum_before_log_ts(exec_info_.max_applied_log_ts_,
                                                    exec_info_.checksum_,
                                                    exec_info_.checksum_log_ts_))) {
      TRANS_LOG(ERROR, "calc checksum before log ts failed", K(ret), KPC(this));
    } else {
      info.tx_id_ = trans_id_;
      info.ls_id_ = ls_id_;
      info.exec_info_ = exec_info_;
      info.cluster_id_ = cluster_id_;
      if (OB_FAIL(mt_ctx_.get_table_lock_store_info(info.table_lock_info_))) {
        TRANS_LOG(WARN, "get_table_lock_store_info failed", K(ret), K(info));
      } else {
        TRANS_LOG(INFO, "store ctx_info: ", K(ret), K(info), KPC(this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::set_scheduler_(const common::ObAddr &scheduler)
{
  int ret = OB_SUCCESS;

  exec_info_.scheduler_ = scheduler;

  return ret;
}

const common::ObAddr &ObPartTransCtx::get_scheduler() const
{
  if (exec_info_.data_complete_ && !exec_info_.scheduler_.is_valid()) {
    TRANS_LOG(ERROR, "invalid scheduler addr", K(trans_id_));
  }
  return exec_info_.scheduler_;
}

int ObPartTransCtx::gen_final_mds_array_(ObTxBufferNodeArray &array, bool is_committing) const
{
  int ret = OB_SUCCESS;

  // If the is_committing is true, some redo log have not confirmed.
  if (OB_FAIL(array.assign(exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "assign multi source data failed", KR(ret), K(*this));
  } else if (is_committing && OB_FAIL(mds_cache_.copy_to(array))) {
    TRANS_LOG(WARN, "copy from mds_cache_ failed", K(ret));
  }

  if (array.get_serialize_size() > ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE) {
    TRANS_LOG(WARN, "MDS array is overflow, use empty MDS array", K(array.get_serialize_size()));
    array.reset();
  }

  return ret;
}

int ObPartTransCtx::gen_total_mds_array_(ObTxBufferNodeArray &mds_array) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mds_array.assign(exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "assign multi source data failed", KR(ret), K(*this));
  } else if (OB_FAIL(mds_cache_.copy_to(mds_array))) {
    TRANS_LOG(WARN, "assign multi source data failed", KR(ret), K(*this));
  }

  return ret;
}

int ObPartTransCtx::deep_copy_mds_array(const ObTxBufferNodeArray &mds_array, bool need_replace)
{
  int ret = OB_SUCCESS;

  const int64_t origin_count = exec_info_.multi_data_source_.count();
  const int64_t additional_count = mds_array.count();

  ObTxBufferNodeArray tmp_buf_arr;

  void *ptr = nullptr;
  int64_t len = 0;

  if (OB_FAIL(tmp_buf_arr.reserve(additional_count))) {
    TRANS_LOG(WARN, "reserve array space failed", K(ret));
  } else if (need_replace) {
    ret = exec_info_.multi_data_source_.reserve(additional_count);
  } else {
    ret = exec_info_.multi_data_source_.reserve(origin_count + additional_count);
  }

  if (OB_FAIL(ret)) {

  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < mds_array.count(); ++i) {
      const ObTxBufferNode &node = mds_array.at(i);
      len = node.data_.length();
      if (OB_ISNULL(ptr = mtl_malloc(len, "MultiTxData"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(*this), K(len));
      } else {
        MEMCPY(ptr, node.data_.ptr(), len);
        ObTxBufferNode new_node;
        ObString data;
        data.assign_ptr(reinterpret_cast<char *>(ptr), len);
        if (OB_FAIL(new_node.init(node.type_, data))) {
          TRANS_LOG(WARN, "init tx buffer node failed", KR(ret), K(new_node));
        } else if (OB_FAIL(tmp_buf_arr.push_back(new_node))) {
          TRANS_LOG(WARN, "push multi source data failed", KR(ret), K(*this));
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < tmp_buf_arr.count(); ++i) {
        mtl_free(tmp_buf_arr[i].data_.ptr());
      }
      tmp_buf_arr.reset();
    }
  }

  if (OB_FAIL(ret)) {

  } else if (need_replace) {

    for (int64_t i = 0; i < exec_info_.multi_data_source_.count(); ++i) {
      if (nullptr != exec_info_.multi_data_source_[i].data_.ptr()) {
        mtl_free(exec_info_.multi_data_source_[i].data_.ptr());
      }
    }
    exec_info_.multi_data_source_.reset();
  }

  if (OB_FAIL(ret)) {

  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_buf_arr.count(); ++i) {
      if (is_follower_()) {
        tmp_buf_arr[i].set_submitted();
        tmp_buf_arr[i].set_synced();
      }
      if (OB_FAIL(exec_info_.multi_data_source_.push_back(tmp_buf_arr[i]))) {
        TRANS_LOG(WARN, "push back exec_info_.multi_data_source_ failed", K(ret));
      }
    }
  }

  return ret;
}

bool ObPartTransCtx::is_contain_mds_type_(const ObTxDataSourceType target_type)
{
  bool is_contain = false;

  for (int64_t i = 0; i < exec_info_.multi_data_source_.count(); i++) {
    if (exec_info_.multi_data_source_[i].get_data_source_type() == target_type) {
      is_contain = true;
    }
  }

  if (!is_contain) {
    is_contain = mds_cache_.is_contain(target_type);
  }

  return is_contain;
}

int ObPartTransCtx::submit_multi_data_source_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxLogBlockHeader
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);
  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else {
    ret = submit_multi_data_source_(log_block);
  }
  return ret;
}

int ObPartTransCtx::submit_multi_data_source_(ObTxLogBlock &log_block)
{
  int ret = OB_SUCCESS;
  bool need_pre_replay_barrier = false;

  ObTxLogCb *log_cb = nullptr;
  if (mds_cache_.count() > 0) {
    ObTxMultiDataSourceLog log;
    ObTxMDSRange range;
    while (OB_SUCC(ret)) {
      log.reset();
      if (OB_FAIL(exec_info_.redo_lsns_.reserve(exec_info_.redo_lsns_.count() + 1))) {
        TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
      } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
        if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
          TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
        }
      } else {
        ret = mds_cache_.fill_mds_log(log, log_cb->get_mds_range(), need_pre_replay_barrier);
      }

      // TRANS_LOG(INFO, "after fill mds log", K(ret), K(trans_id_));
      // OB_EAGAIN will be overwritten
      if (OB_EMPTY_RANGE == ret) {
        // do nothing
      } else if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "fill MDS log failed", K(ret));
      } else if (OB_FAIL(exec_info_.multi_data_source_.reserve(
                     exec_info_.multi_data_source_.count() + log_cb->get_mds_range().count()))) {
        TRANS_LOG(WARN, "reserve mds space failed", K(ret));
      } else if (OB_FAIL(log_block.add_new_log(log))) {
        // do not handle ret code OB_BUF_NOT_ENOUGH, one log entry should be
        // enough to hold multi source data, if not, take it as an error.
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
      } else if (need_pre_replay_barrier
                 && OB_FAIL(log_block.rewrite_barrier_log_block(
                     trans_id_.get_id(), logservice::ObReplayBarrierType::PRE_BARRIER))) {

        TRANS_LOG(WARN, "rewrite multi data source log barrier failed", K(ret));
        return_log_cb_(log_cb);
        log_cb = NULL;
      } else if (log_block.get_cb_arg_array().count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      } else if (OB_FAIL(acquire_ctx_ref_())) {
        TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        log_cb = nullptr;
      } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                     log_block.get_buf(), log_block.get_size(), 0, log_cb, false))) {
        TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
        release_ctx_ref_();
      } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
        log_cb = nullptr;
      } else {
        log_cb = nullptr;
      }
      if (OB_NOT_NULL(log_cb) && OB_FAIL(ret)) {
        return_log_cb_(log_cb);
        log_cb = nullptr;
      }
    }
    if (OB_EMPTY_RANGE == ret) {
      ret = OB_SUCCESS;
    }
  }

  TRANS_LOG(TRACE, "submit MDS redo", K(ret), K(trans_id_), KPC(log_cb), K(mds_cache_),
            K(exec_info_.multi_data_source_));

  return ret;
}

int ObPartTransCtx::prepare_mul_data_source_tx_end_(bool is_commit)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObTxBufferNodeArray tmp_array;

    if (is_commit
      && mds_cache_.count() > 0
      && OB_FAIL(submit_multi_data_source_())) {
      TRANS_LOG(WARN, "submit multi data souce log failed", K(ret));
    } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
      TRANS_LOG(WARN, "copy total mds array failed", K(ret));
    } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END, OB_INVALID_TIMESTAMP, false,
                                           tmp_array))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::notify_data_source_(const NotifyType notify_type,
                                        const int64_t log_ts,
                                        const bool for_replay,
                                        const ObTxBufferNodeArray &notify_array)
{
  int ret = OB_SUCCESS;
  ObMulSourceDataNotifyArg arg;
  arg.tx_id_ = trans_id_;
  arg.log_ts_ = log_ts;
  arg.trans_version_ = ctx_tx_data_.get_commit_version();
  arg.for_replay_ = for_replay;
  arg.notify_type_ = notify_type;

  int64_t total_time = 0;

  if (OB_FAIL(ObMulSourceTxDataNotifier::notify(notify_array, notify_type, arg, this, total_time))) {
    TRANS_LOG(WARN, "notify data source failed", K(ret), K(arg));
  }
  if (notify_array.count() > 0) {
    TRANS_LOG(INFO, "notify MDS", K(ret), K(trans_id_), K(ls_id_), K(notify_type), K(log_ts),K(notify_array.count()),
              K(notify_array), K(total_time));
  }
  return ret;
}

int ObPartTransCtx::register_multi_data_source(const ObTxDataSourceType data_source_type,
                                               const char *buf,
                                               const int64_t len,
                                               const bool try_lock)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxBufferNode node;
  ObString data;
  void *ptr = nullptr;
  ObTxBufferNodeArray tmp_array;
  bool need_lock = true;

  if (try_lock) {
    // avoid deadlock, but give a timeout ts to avoid lock conflict short time.
    ret = lock_.lock(100000 /* 100ms */);
    // lock timeout need retry again outside.
    ret = OB_TIMEOUT == ret ? OB_EAGAIN : ret;
    need_lock = false;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    CtxLockGuard guard(lock_, need_lock);

    if (OB_UNLIKELY(nullptr == buf || len <= 0 || data_source_type <= ObTxDataSourceType::UNKNOWN
                    || data_source_type >= ObTxDataSourceType::MAX_TYPE)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(data_source_type), KP(buf), K(len));
    } else if (is_follower_()) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "can not register mds on a follower", K(ret), K(data_source_type), K(len), KPC(this));
    } else if (OB_ISNULL(ptr = mtl_malloc(len, "MultiTxData"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(data_source_type), K(len));
    } else if (FALSE_IT(MEMCPY(ptr, buf, len))) {
    } else if (FALSE_IT(data.assign_ptr(reinterpret_cast<char *>(ptr), len))) {
    } else {
      if (OB_FAIL(node.init(data_source_type, data))) {
        TRANS_LOG(WARN, "init tx buffer node failed", KR(ret), K(data_source_type), K(*this));
      } else if (OB_FAIL(tmp_array.push_back(node))) {
        TRANS_LOG(WARN, "push back notify node  failed", KR(ret));
      } else if (tmp_array.get_serialize_size() > ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "too large mds buf node", K(ret), K(tmp_array.get_serialize_size()));
      } else if (OB_FAIL(mds_cache_.insert_mds_node(node))) {
        TRANS_LOG(WARN, "register multi source data failed", KR(ret), K(data_source_type), K(*this));
      }

      if (OB_FAIL(ret)) {
        mtl_free(ptr);
      } else if (OB_FAIL(notify_data_source_(NotifyType::REGISTER_SUCC, OB_INVALID_TIMESTAMP, false,
                                             tmp_array))) {
        if (OB_SUCCESS != (tmp_ret = mds_cache_.rollback_last_mds_node())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "rollback last mds node failed", K(tmp_ret), K(ret));
        }
        TRANS_LOG(WARN, "notify data source for register_succ failed", K(tmp_ret));
      } else if (mds_cache_.get_unsubmitted_size() < ObTxMultiDataSourceLog::MAX_PENDING_BUF_SIZE) {
        // do nothing
      } else if (OB_SUCCESS != (tmp_ret = submit_multi_data_source_())) {
        TRANS_LOG(WARN, "submit mds log failed", K(tmp_ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN,
              "register MDS redo in part_ctx failed",
              K(ret),
              K(trans_id_),
              K(ls_id_),
              K(data_source_type),
              K(len),
              K(mds_cache_),
              K(exec_info_.multi_data_source_));
  }

  REC_TRANS_TRACE_EXT2(tlog_, register_multi_data_source, OB_ID(ret), ret, OB_ID(type), data_source_type);

  return ret;
}

int ObPartTransCtx::del_retain_ctx()
{
  int ret = OB_SUCCESS;
  bool need_del = false;

  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

    if (RetainCause::UNKOWN == get_retain_cause()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "can not del a normal ctx by retain inerface", K(ret), KPC(this));
    } else if (is_exiting_) {
      // +-----------------------------------------------+
      // |                tx ctx exiting                 |
      // |      but be retained because of MDS buf       |
      // +-----------------------------------------------+
      //   |
      //   | stop server and recover
      //   | ls_ckpt_ts < ctx_ckpt_ts
      //   v
      // +-----------------------------------------------+
      // |          recover from exiting state           |
      // +-----------------------------------------------+
      //   |
      //   | start replay from ls_ckpt_ts
      //   v
      // +-----------------------------------------------+
      // |        replay redo in the exiting ctx         |
      // +-----------------------------------------------+
      //   |
      //   |
      //   v
      // +-----------------------------------------------+
      // |               froce kill tx ctx               |
      // +-----------------------------------------------+
      //   |
      //   |
      //   v
      // +-----------------------------------------------+
      // | need clear memtable_ctx even if it is exiting |
      // +-----------------------------------------------+
      trans_kill_();  // clear memtable calliback for replay 

      clean_retain_cause_();
      print_trace_log_if_necessary_();

      const int64_t ctx_ref = get_ref();
      if (NULL == ls_tx_ctx_mgr_) {
        TRANS_LOG(ERROR, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context",
                  *this);
      } else {
        need_del = true;
        TRANS_LOG(INFO, "del retain ctx after exiting", K(ctx_ref), KPC(this));
      }
    } else {
      // MDS trans : allowed gc before clear log on_success
      // del ctx by set_exiting_()
      clean_retain_cause_();
      TRANS_LOG(INFO, "del retain ctx before exiting", KPC(this));
    }
  }
  if (need_del && NULL != ls_tx_ctx_mgr_) {
    ls_tx_ctx_mgr_->del_tx_ctx(this);
  }
  // TRANS_LOG(INFO, "after del retain ctx", K(ret), KPC(this));
  return ret;
}

int ObPartTransCtx::search_unsubmitted_dup_table_redo_()
{
  return mt_ctx_.get_redo_generator().search_unsubmitted_dup_tablet_redo();
}

int ObPartTransCtx::dup_table_tx_redo_sync_()
{
  int ret = OB_SUCCESS;

  // init state => submit commit info log
  // redo_complete state  => validate reod sync (follower replay ts)
  // redo sync finish -> submit prepare log

  return ret;
}

int ObPartTransCtx::dup_table_tx_pre_commit_()
{
  int ret = OB_SUCCESS;

  // dup table pre commit not finish => post ts sync request and return OB_EAGAIN
  // dup table pre commit finish => OB_SUCCESS

  return ret;
}

int ObPartTransCtx::sub_prepare(const ObLSArray &parts,
                                const MonotonicTs &commit_time,
                                const int64_t &expire_ts,
                                const common::ObString &app_trace_info,
                                const int64_t &request_id,
                                const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 >= expire_ts) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(expire_ts), K(xid), KPC(this));
  } else if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is replaying", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_2pc_logging_())) {
    TRANS_LOG(WARN, "tx is 2pc logging", KPC(this));
  } else if (ObTxState::INIT != upstream_state_) {
    // TODO, consider prepare state
    if (is_prepared_sub2pc()) {
      if (OB_FAIL(post_tx_sub_prepare_resp_(OB_SUCCESS))) {
        TRANS_LOG(WARN, "fail to post sub prepare response", K(ret), KPC(this));
      }
    }
    TRANS_LOG(WARN, "tx is committing", KPC(this));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "transaction is exiting", K(ret), KPC(this));
  } else if (OB_UNLIKELY(pending_write_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "access in progress", K(ret), K_(pending_write), KPC(this));
  } else if (OB_FAIL(set_2pc_participants_(parts))) {
    TRANS_LOG(WARN, "set participants failed", K(ret), KPC(this));
  } else if (OB_FAIL(set_2pc_request_id_(request_id))) {
    TRANS_LOG(WARN, "set request id failed", K(ret), K(request_id), KPC(this));
  } else if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), KPC(this));
  } else if (FALSE_IT(stmt_expired_time_ = expire_ts)) {
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timeout handler error", K(ret), KPC(this));
  } else if (OB_FAIL(register_timeout_task_(ObServerConfig::get_instance().trx_2pc_retry_interval
                                            + trans_id_.hash() % USEC_PER_SEC))) {
    TRANS_LOG(WARN, "register timeout handler error", K(ret), KPC(this));
  } else {
    if (commit_time.is_valid()) {
      set_stc_(commit_time);
    } else {
      set_stc_by_now_();
    }
    exec_info_.trans_type_ = TransType::DIST_TRANS;
    exec_info_.xid_ = xid;
    // (void)set_sub2pc_coord_state(Ob2PCPrepareState::REDO_PREPARING);
    if (OB_FAIL(prepare_redo())) {
      TRANS_LOG(WARN, "fail to execute sub prepare", K(ret), KPC(this));
    } else {
      part_trans_action_ = ObPartTransAction::COMMIT;
    }
    TRANS_LOG(INFO, "sub prepare", K(ret), K(xid), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::sub_end_tx(const int64_t &request_id,
                               const ObXATransID &xid,
                               const ObAddr &tmp_scheduler,
                               const bool is_rollback)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (xid.empty() || xid != exec_info_.xid_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), KPC(this));
  } else if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected trans ctx", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is replaying", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_2pc_logging_())) {
    TRANS_LOG(WARN, "tx is 2pc logging", KPC(this));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "transaction is exiting", K(ret), KPC(this));
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timeout handler error", K(ret), KPC(this));
  } else if (OB_FAIL(register_timeout_task_(ObServerConfig::get_instance().trx_2pc_retry_interval
                                            + trans_id_.hash() % USEC_PER_SEC))) {
    TRANS_LOG(WARN, "register timeout handler error", K(ret), KPC(this));
  } else if (ObTxState::REDO_COMPLETE != upstream_state_) {
    TRANS_LOG(WARN, "not in prepare state", K(ret), KPC(this));
    // TODO, check state
  } else {
    tmp_scheduler_= tmp_scheduler;
    // (void)set_sub2pc_coord_state(Ob2PCPrepareState::VERSION_PREPARING);
    if (OB_FAIL(continue_execution(is_rollback))) {
      TRANS_LOG(WARN, "fail to continue execution", KR(ret), KPC(this));
    }
  }
  TRANS_LOG(INFO, "sub end tx", K(ret), K(xid), K(is_rollback), K(tmp_scheduler), KPC(this));
  return ret;
}

int ObPartTransCtx::supplement_undo_actions_if_exist_()
{
  int ret = OB_SUCCESS;

  ObTxTable *tx_table = nullptr;
  ObTxData *tmp_tx_data = nullptr;
  ctx_tx_data_.get_tx_table(tx_table);

  if (OB_FAIL(ctx_tx_data_.deep_copy_tx_data_out(tmp_tx_data))) {
    TRANS_LOG(WARN, "deep copy tx data in ctx tx data failed.", KR(ret), K(ctx_tx_data_), KPC(this));
  } else if (OB_FAIL(tx_table->supplement_undo_actions_if_exist(tmp_tx_data))) {
    TRANS_LOG(
      WARN,
      "supplement undo actions to a tx data when replaying a transaction from the middle failed.",
      KR(ret), K(ctx_tx_data_), KPC(this));
  } else if (OB_FAIL(ctx_tx_data_.replace_tx_data(tmp_tx_data))) {
    TRANS_LOG(WARN, "replace tx data in ctx tx data failed.", KR(ret), K(ctx_tx_data_), KPC(this));
  }

  // tmp_tx_data should be null because replace_tx_data should set it as nullptr if succeed.
  // we free it if not null to avoid memory leak
  if (OB_NOT_NULL(tmp_tx_data)) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error happend when supplement undo actions", KR(ret),
                K(ctx_tx_data_), KPC(this));
    }
    int tmp_ret = ctx_tx_data_.free_tmp_tx_data(tmp_tx_data);
    TRANS_LOG(WARN, "error happend when supplement undo actions", KR(ret), KR(tmp_ret),
              K(ctx_tx_data_), KPC(this));

  }

  return ret;
}

int ObPartTransCtx::check_status()
{
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  return check_status_();
}

/* check_status_ - check ctx status is health
 *
 * it is used in three situations:
 * 1) before start to read/write:
 *    checks:
 *      a. is leader
 *      b. is active(not committing/aborting)
 *      c. is NOT exiting (due to concurrent created ctx, start_trans failed after created)
 * 2) after read:
 *    checks:
 *      a. is active(not aborted)
 * 3) savepoint rollback:
 *    checks: like 1)
 * in order to reuse this routine, do `check txn is active` at first, thus even if
 * an active txn which has switched into follower also pass these check
 */
int ObPartTransCtx::check_status_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_committing_())) {
    ret = OB_TRANS_HAS_DECIDED;
    if (is_trans_expired_()
        && ObPartTransAction::ABORT == part_trans_action_) {
      // rewrite error with TRANS_TIMEOUT to easy user
      ret = OB_TRANS_TIMEOUT;
    } else if (ObPartTransAction::ABORT == part_trans_action_ ||
               exec_info_.state_ == ObTxState::ABORT) {
      ret = OB_TRANS_KILLED;
    }
    TRANS_LOG(WARN, "tx has decided", K(ret), KPC(this));
  } else if (OB_UNLIKELY(sub_state_.is_force_abort())) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx force aborted due to data incomplete", K(ret), KPC(this));
  } else if (OB_UNLIKELY(is_follower_())) {
    ret =  OB_NOT_MASTER;
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "tx is exiting", K(ret), KPC(this));
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "check trx status", K(ret), KPC(this));
  }
  return ret;
}

/*
 * start transaction protected access
 *
 * purpose:
 * 1) verify transaction ctx is *writable*
 * 2) acquire memtable ctx's ref
 * 3) remember data_scn
 */
int ObPartTransCtx::start_access(const ObTxDesc &tx_desc,
                                 const int64_t data_scn)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  if (OB_FAIL(check_status_())) {
  } else if (tx_desc.op_sn_ < last_op_sn_) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "stale access operation", K(ret),
              K_(tx_desc.op_sn), K_(last_op_sn), KPC(this), K(tx_desc));
  } else if (FALSE_IT(++pending_write_)) {
  } else if (FALSE_IT(last_scn_ = std::max(data_scn, last_scn_))) {
  } else if (first_scn_ == 0 && FALSE_IT(first_scn_ = last_scn_)) {
  } else if (tx_desc.op_sn_ != last_op_sn_) {
    last_op_sn_ = tx_desc.op_sn_;
  }

  if (OB_SUCC(ret)) {
    mt_ctx_.inc_ref();
    mt_ctx_.acquire_callback_list();
  }
  TRANS_LOG(TRACE, "start_access", K(ret), KPC(this));
  REC_TRANS_TRACE_EXT(tlog_, start_access,
                      OB_ID(ret), ret,
                      OB_ID(trace_id), ObCurTraceId::get_trace_id_str(),
                      OB_ID(opid), tx_desc.op_sn_,
                      OB_ID(data_seq), data_scn,
                      OB_ID(pending), pending_write_,
                      OB_ID(ctx_ref), get_ref(),
                      OB_ID(thread_id), get_itid() + 1);
  return ret;
}

/*
 * end_access - end of txn protected access
 *
 * release memetable context lock
 * dec pending write num
 * dec ref of memtable context
 * merge provisional write's callbacks into the total final callback list
 */
int ObPartTransCtx::end_access()
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  --pending_write_;
  mt_ctx_.dec_ref();
  mt_ctx_.revert_callback_list();
  TRANS_LOG(TRACE, "end_access", K(ret), KPC(this));
  REC_TRANS_TRACE_EXT(tlog_, end_access,
                      OB_ID(opid), last_op_sn_,
                      OB_ID(pending), pending_write_,
                      OB_ID(ctx_ref), get_ref(),
                      OB_ID(thread_id), get_itid() + 1);
  return ret;
}

/*
 * rollback_to_savepoint - rollback to savepoint
 *
 * @op_sn    - operation sequence number, used to reject out of order msg
 * @from_scn - the start position of rollback, inclusive
 * @to_scn   - the end position of rollback, exclusive
 *
 * savepoint may be created in these ways:
 * 1) created at txn scheduler, named Global-Savepoint
 * 2) created at txn participant server, named Local-Savepoint
 * 3) created at txn participant logstream, named LS-Local-Savepoint
 * In Global-Savepoint, rollback will check in-flight write and
 *    reject with OB_NEED_RETRY. this is required, because global savepoint
 *    will cross over network, given up write may concurrency with rollback.
 * In other two types of savepoint, write always under control of local thread,
 *    so such check was skipped, caller should promise writing should not
 *    concurrency with rolling back.
 *
 * There is another flaw and should token care:
 *   the last_scn are not accurate when LS-Local-Savepoint was created. it is
 *   because of data's scn use a sequence after the savepoint, which is greater
 *   than the sequence passed to start_access. and the last_scn was only set
 *   when start_access was called
 */
int ObPartTransCtx::rollback_to_savepoint(const int64_t op_sn,
                                          const int64_t from_scn,
                                          const int64_t to_scn)
{
  int ret = OB_SUCCESS;
  bool need_write_log = false;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  if (OB_FAIL(check_status_())) {
  } else if(!busy_cbs_.is_empty()) {
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "rollback_to need retry because of logging", K(ret),
              K(trans_id_), K(busy_cbs_.get_size()));
  } else if (op_sn < last_op_sn_) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else if (op_sn > last_op_sn_ && last_scn_ <= to_scn) {
    last_op_sn_ = op_sn;
    TRANS_LOG(INFO, "rollback succeed trivially", K(op_sn), K(to_scn), K_(last_scn));
  } else if (op_sn > last_op_sn_ && pending_write_ > 0) {
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "has pending write, rollback blocked",
              K(ret), K(pending_write_), KPC(this));
  } else if (FALSE_IT(last_op_sn_ = op_sn)) {
  } else if (OB_FAIL(rollback_to_savepoint_(from_scn, to_scn))) {
    TRANS_LOG(WARN, "rollback_to_savepoint fail", K(ret),
              K(from_scn), K(to_scn), K(op_sn), KPC(this));
  } else {
    last_scn_ = to_scn;
  }
  REC_TRANS_TRACE_EXT(tlog_, rollback_savepoint,
                      OB_ID(ret), ret,
                      OB_ID(from), from_scn,
                      OB_ID(to), to_scn,
                      OB_ID(pending), pending_write_,
                      OB_ID(opid), op_sn,
                      OB_ID(thread_id), GETTID());
#ifndef NDEBUG
  TRANS_LOG(INFO, "rollback to savepoint", K(ret),
            K(from_scn), K(to_scn), KPC(this));
#endif
  return ret;
}

int ObPartTransCtx::rollback_to_savepoint_(const int64_t from_scn,
                                           const int64_t to_scn)
{
  int ret = OB_SUCCESS;

  // step 1: persistent 'UNDO' (if required)
  /*
   * Follower:
   *  1. add UndoAction into tx_ctx's tx_data
   *  2. insert UndoAction into tx_data_table
   * Leader:
   *  1. submit 'RollbackToLog'
   *  2. add UndoAction into tx_ctx's tx_data
   *  3. insert UndoAction into tx_data_table after log sync success
   */
  if (is_follower_()) { /* Follower */
    ObUndoAction undo_action(from_scn, to_scn);
    ObTxData *tmp_tx_data = nullptr;
    if (OB_FAIL(ctx_tx_data_.add_undo_action(undo_action))) {
      TRANS_LOG(WARN, "recrod undo info fail", K(ret), K(from_scn), K(to_scn), KPC(this));
    } else if (OB_FAIL(ctx_tx_data_.deep_copy_tx_data_out(tmp_tx_data))) {
      TRANS_LOG(WARN, "deep copy tx data failed", KR(ret), K(*this));
    } else if (FALSE_IT(tmp_tx_data->end_log_ts_ = exec_info_.max_applying_log_ts_)) {
    } else if (OB_FAIL(ctx_tx_data_.insert_tmp_tx_data(tmp_tx_data))) {
      TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ctx_tx_data_.free_tmp_tx_data(tmp_tx_data))) {
        TRANS_LOG(WARN, "free tmp tx data failed", KR(tmp_ret), KPC(this));
      }
    }
  } else if (OB_UNLIKELY(exec_info_.max_submitted_seq_no_ > to_scn)) { /* Leader */
    ObUndoAction undo_action(from_scn, to_scn);
    ObUndoStatusNode *undo_status = NULL;
    ObTxData *tmp_tx_data = NULL;
    if (OB_FAIL(ctx_tx_data_.prepare_add_undo_action(undo_action, tmp_tx_data, undo_status))) {
      TRANS_LOG(WARN, "prepare add undo action fail", K(ret), KPC(this));
    } else if (OB_FAIL(submit_rollback_to_log_(from_scn, to_scn, tmp_tx_data))) {
      TRANS_LOG(WARN, "submit undo redolog fail", K(ret), K(from_scn), K(to_scn), KPC(this));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ctx_tx_data_.cancel_add_undo_action(tmp_tx_data, undo_status))) {
        TRANS_LOG(ERROR, "cancel add undo action failed", KR(tmp_ret), KPC(this));
      }
    } else if (OB_FAIL(ctx_tx_data_.commit_add_undo_action(undo_action, *undo_status))) {
      TRANS_LOG(ERROR, "oops, commit add undo action fail", K(ret), KPC(this));
      ob_abort();
    }
  }

  // step 2: remove TxNode(s) from memtable

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mt_ctx_.rollback(to_scn, from_scn))) {
      TRANS_LOG(WARN, "undo provisinal modifies fail",
                K(ret), K(from_scn), K(to_scn), KPC(this));
    }
  }

  return ret;
}

int ObPartTransCtx::submit_rollback_to_log_(const int64_t from_scn,
                                            const int64_t to_scn,
                                            ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  const auto replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxRollbackToLog log(from_scn, to_scn);
  ObTxLogCb *log_cb = NULL;

  ObTxLogBlockHeader log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block fail", K(ret), KPC(this));
  } else if (OB_FAIL(exec_info_.redo_lsns_.reserve(exec_info_.redo_lsns_.count() + 1))) {
    TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    TRANS_LOG(WARN, "get log_cb fail", K(ret), KPC(this));
  } else if (OB_FAIL(log_block.add_new_log(log))) {
    TRANS_LOG(WARN, "logblock add log fail", K(ret), KPC(this));
  } else if (log_block.get_cb_arg_array().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                 log_block.get_buf(), log_block.get_size(), 0, log_cb, false /*nonblock on EAGAIN*/
                 ))) {
    TRANS_LOG(WARN, "submit log fail", K(ret), K(log_block), KPC(this));
    return_log_cb_(log_cb);
  } else if (OB_FAIL(acquire_ctx_ref())) {
    // inc for log_cb calling back
    TRANS_LOG(ERROR, "inc TxCtx ref fail", K(ret), KPC(this));
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
  } else {
    log_cb->set_tx_data(tx_data);
  }
  REC_TRANS_TRACE_EXT(tlog_, submit_rollback_log, OB_ID(ret), ret, OB_ID(from), from_scn, OB_ID(to),
                      to_scn);
  TRANS_LOG(INFO, "RollbackToLog submit", K(ret), K(from_scn), K(to_scn), KP(log_cb), KPC(this));
  return ret;
}

int ObPartTransCtx::abort(const int reason)
{
  UNUSED(reason);
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "not master", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_committing_())) {
    if (part_trans_action_ == ObPartTransAction::ABORT) {
      TRANS_LOG(INFO, "tx already aborting", KPC(this));
    } else if (part_trans_action_ == ObPartTransAction::COMMIT
               || exec_info_.state_ != ObTxState::ABORT) {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "tx already committing", KR(ret), KPC(this));
    } else {
      TRANS_LOG(INFO, "tx already aborting", KPC(this));
    }
  // } else if (OB_FAIL(prepare_mul_data_source_tx_end_(false))) {
  //   TRANS_LOG(WARN, "trans abort need retry", K(ret), K(trans_id_), K(ls_id_), K(reason));
  } else {
    if (OB_FAIL(abort_(reason))) {
      TRANS_LOG(WARN, "abort_ failed", KR(ret), K(*this));
    }
  }
  return ret;
}


int ObPartTransCtx::tx_keepalive_response_(const int64_t status)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_TRANS_CTX_NOT_EXIST == status && can_be_recycled_()) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(WARN, "[TRANS GC] tx has quit, local tx will be aborted",
                K(status), KPC(this));
    }
    if (OB_FAIL(gc_ctx_())) {
      TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), KPC(this));
    }
  } else if (OB_SUCCESS != status) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(WARN, "[TRANS GC] tx keepalive fail", K(status), KPC(this));
    }
  } else {
    TRANS_LOG(TRACE, "tx is alive", KPC(this));
  }
  return ret;
}

int ObPartTransCtx::insert_into_retain_ctx_mgr_(RetainCause cause, int64_t log_ts, palf::LSN lsn)
{
  int ret = OB_SUCCESS;
  ObMDSRetainCtxFunctor *retain_func_ptr = nullptr;

  if (OB_ISNULL(ls_tx_ctx_mgr_) || RetainCause::UNKOWN == cause) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(cause), KP(ls_tx_ctx_mgr_), KPC(this));
  } else {
    ObTxRetainCtxMgr &retain_ctx_mgr = ls_tx_ctx_mgr_->get_retain_ctx_mgr();

    if (OB_ISNULL(retain_func_ptr = static_cast<ObMDSRetainCtxFunctor *>(
                      retain_ctx_mgr.alloc_object((sizeof(ObMDSRetainCtxFunctor)))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory failed", K(ret), KPC(this));

    } else if (OB_FALSE_IT(new (retain_func_ptr) ObMDSRetainCtxFunctor())) {
    } else if (OB_FAIL(retain_func_ptr->init(this, cause, log_ts, lsn))) {
      TRANS_LOG(WARN, "init retain ctx functor failed", K(ret), KPC(this));
    } else if (OB_FAIL(retain_ctx_mgr.push_retain_ctx(retain_func_ptr))) {
      TRANS_LOG(WARN, "push into retain_ctx_mgr failed", K(ret), KPC(this));
    }
    // if (OB_FAIL(retain_ctx_mgr.reset()))
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "insert into retain_ctx_mgr error, retain ctx will not deleted from ctx_mgr",
              K(ret), KPC(this));
    // ob_abort();
  }

  return ret;
}

int ObPartTransCtx::do_local_tx_end_(TxEndAction tx_end_action)
{
  int ret = OB_SUCCESS;

  if (tx_end_action != TxEndAction::KILL_TX_FORCEDLY
      && OB_FAIL(prepare_mul_data_source_tx_end_(TxEndAction::COMMIT_TX == tx_end_action))) {
    TRANS_LOG(WARN, "prepare tx end notify failed", K(ret), KPC(this));
  } else {

    switch (tx_end_action) {
    case TxEndAction::COMMIT_TX: {
      ret = do_local_commit_tx_();
      // part_trans_action_ will be set as commit in ObPartTransCtx::commit function
      break;
    }
    case TxEndAction::ABORT_TX: {
      ret = do_local_abort_tx_();
      if (OB_SUCC(ret)) {
        part_trans_action_ = ObPartTransAction::ABORT;
      }
      break;
    }
    case TxEndAction::KILL_TX_FORCEDLY: {
      ret = do_force_kill_tx_();
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid tx_end_action", K(ret), K(tx_end_action));
      break;
    }
    }
  }
  return ret;
}

int ObPartTransCtx::do_local_commit_tx_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_commit_version_())) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "generate commit version failed", KR(ret), K(*this));
    }
  } else if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_COMMIT_LOG))) {
    // log submitting will retry in handle_timeout
    TRANS_LOG(WARN, "submit commit log fail, will retry later", KR(ret), KPC(this));
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObPartTransCtx::do_local_abort_tx_()
{
  int ret = OB_SUCCESS;

  if (has_persisted_log_() || is_logging_()) {
    // part_trans_action_ = ObPartTransAction::ABORT;
    if (OB_FAIL(compensate_abort_log_())) {
      TRANS_LOG(WARN, "submit abort log failed", KR(ret), K(*this));
    }
  } else {
    // if (part_trans_action_ < ObPartTransAction::COMMIT) {
    //   part_trans_action_ = ObPartTransAction::ABORT;
    // }
    if (OB_FAIL(on_local_abort_tx_())) {
      TRANS_LOG(WARN, "local tx abort failed", KR(ret), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::do_force_kill_tx_()
{
  int ret = OB_SUCCESS;

  trans_kill_();
  // Force kill cannot guarantee the consistency, so we just set end_log_ts
  // to zero
  end_log_ts_ = 0;
  (void)trans_clear_();
  if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
  }
  // Ignore ret
  set_exiting_();
  TRANS_LOG(INFO, "transaction killed success", "context", *this);

  return ret;
}

int ObPartTransCtx::on_local_commit_tx_()
{
  int ret = OB_SUCCESS;

  bool need_wait = false;

  int64_t start_us, end_us;

  start_us = end_us = 0;

  common::ObTimeGuard tg("part_ctx::on_local_commit", 100 * 1000);

  if (sub_state_.is_gts_waiting()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected gts waiting flag", K(ret), KPC(this));
  } else if (OB_UNLIKELY(ctx_tx_data_.get_commit_version() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid commit version", K(ret), KPC(this));
  } else if (OB_FAIL(wait_gts_elapse_commit_version_(need_wait))) {
    TRANS_LOG(WARN, "wait gts elapse commit version failed", KR(ret), KPC(this));
  } else if (FALSE_IT(tg.click())) {
  } else if (OB_FAIL(tx_end_(true /*commit*/))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), "context", *this);
  } else if (FALSE_IT(elr_handler_.reset_elr_state())) {
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "local tx clear error", KR(ret), K(*this));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_COMMIT, ctx_tx_data_.get_end_log_ts(),
                                         false, exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
  } else if (FALSE_IT(set_durable_state_(ObTxState::COMMIT))) {

  } else if (FALSE_IT(unregister_timeout_task_())) {

  } else if (need_wait) {
    REC_TRANS_TRACE_EXT2(tlog_, wait_gts_elapse, OB_ID(ctx_ref), get_ref());
  } else if (FALSE_IT(tg.click())) {
  }

  if (OB_FAIL(ret) || need_wait) {
    // do nothing
  } else if (OB_FAIL(after_local_commit_succ_())) {
    TRANS_LOG(WARN, "terminate trx after local commit failed", KR(ret), KPC(this));
  }

  if (tg.get_diff() > 100000) {
    TRANS_LOG(INFO, "on local commit cost too much time", K(ret), K(need_wait), K(tg), K(*this));
  }

  ObTransStatistic::get_instance().add_trans_mt_end_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_trans_mt_end_time(tenant_id_, end_us - start_us);

  return ret;
}

int ObPartTransCtx::after_local_commit_succ_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(update_max_commit_version_())) {
    TRANS_LOG(WARN, "update max commit version failed", KR(ret), KPC(this));
  } else {
    (void)post_tx_commit_resp_(OB_SUCCESS);
    set_exiting_();
  }

  return ret;
}

int ObPartTransCtx::on_local_abort_tx_()
{
  int ret = OB_SUCCESS;

  int64_t start_us, end_us;

  start_us = end_us = 0;

  ObTxBufferNodeArray tmp_array;

  if (OB_FALSE_IT(start_us = ObTimeUtility::fast_current_time())) {

  } else if (OB_FAIL(tx_end_(false /*commit*/))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), K(commit_version), "context", *this);
  } else if (FALSE_IT(end_us = ObTimeUtility::fast_current_time())) {

  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "local tx clear error", KR(ret), K(*this));
  } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
    TRANS_LOG(WARN, "gen total mds array failed", KR(ret), K(*this));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_ABORT, ctx_tx_data_.get_end_log_ts(), false,
                                         tmp_array))) {
    TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
  } else if (FALSE_IT(set_durable_state_(ObTxState::ABORT))) {

  } else if (FALSE_IT(unregister_timeout_task_())) {

  } else if (ObPartTransAction::COMMIT == part_trans_action_) {
    (void)post_tx_commit_resp_(OB_TRANS_KILLED);
  }

  if (OB_SUCC(ret)) {
    set_exiting_();
  }

  ObTransStatistic::get_instance().add_trans_mt_end_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_trans_mt_end_time(tenant_id_, end_us - start_us);

  return ret;
}
int ObPartTransCtx::dump_2_text(FILE *fd)
{
  int ret = OB_SUCCESS;
  
  const ObTxData *tx_data_ptr = NULL;
  const int64_t buf_len = 4096;
  char buf[buf_len];
  MEMSET(buf, 0, buf_len);

  int64_t str_len = to_string(buf, buf_len);

  fprintf(fd, "********** ObPartTransCtx ***********\n\n");
  fprintf(fd, "%s\n", buf);
  auto guard = ctx_tx_data_.get_tx_data();
  if (OB_FAIL(guard.get_tx_data(tx_data_ptr))) {
  } else if (OB_ISNULL(tx_data_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected nullptr", KR(ret));
  } else {
    tx_data_ptr->dump_2_text(fd);
  }

  fprintf(fd, "\n********** ObPartTransCtx ***********\n");
  return ret;
}

} // namespace transaction
} // namespace oceanbase
