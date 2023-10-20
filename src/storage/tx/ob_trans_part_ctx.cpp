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
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#define NEED_MDS_REGISTER_DEFINE
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE

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

static const int64_t STANDBY_STAT_ARRAY_SIZE = 10;
// cache line aligned
static int64_t STANDBY_STAT[STANDBY_STAT_ARRAY_SIZE][8];

static void statistics_for_standby()
{
  for (int64_t i = 0; i < STANDBY_STAT_ARRAY_SIZE; i++) {
    LOG_INFO("standby checking statistics", K(STANDBY_STAT[i][0]));
    STANDBY_STAT[i][0] = 0;
  }
}

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
                         ObLSTxCtxMgr *ls_ctx_mgr,
                         const bool for_replay)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

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
    last_request_ts_ = ctx_create_time_;

    exec_info_.scheduler_ = scheduler;
    exec_info_.trans_type_ = TransType::SP_TRANS;
    last_ask_scheduler_status_ts_ = ObClockGenerator::getClock();
    cluster_id_ = cluster_id;
    epoch_ = epoch;
    pending_write_ = 0;
    set_role_state(for_replay);
    block_frozen_memtable_ = nullptr;

    if (is_follower_()) {
      mt_ctx_.trans_replay_begin();
    } else {
      mt_ctx_.trans_begin();
    }

    mt_ctx_.set_trans_ctx(this);
    mt_ctx_.set_for_replay(is_follower_());

    if (!GCONF.enable_record_trace_log) {
      tlog_ = NULL;
    } else {
      tlog_ = &trace_log_;
    }

    is_inited_ = true;
  } else {
    // reset immediately
    default_init_();
  }

  REC_TRANS_TRACE_EXT2(tlog_, init,
                       OB_ID(addr), (void*)this,
                       OB_ID(id), ls_id_.id(),
                       OB_ID(trans_id), trans_id,
                       OB_ID(ref), get_ref());
  TRANS_LOG(TRACE, "part trans ctx init", K(ret), K(tenant_id), K(trans_id), K(trans_expired_time),
            K(ls_id), K(cluster_version), KP(trans_service), K(cluster_id), K(epoch));
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


    if (exec_info_.is_dup_tx_ && OB_NOT_NULL(ls_tx_ctx_mgr_)) {
      if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->remove_commiting_dup_trx(trans_id_))) {
        TRANS_LOG(WARN, "remove committing dup table trx failed", K(ret), KPC(this));
      }
    }

    if (NULL == ls_tx_ctx_mgr_) {
      TRANS_LOG(ERROR, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context",
                *this);
    } else {
      ls_tx_ctx_mgr_->dec_total_tx_ctx_count();
    }
    // Defensive Check 3 : missing to callback scheduler
    if (!is_follower_() && need_callback_scheduler_()) {
      int tx_result = OB_TRANS_UNKNOWN;
      switch (ctx_tx_data_.get_state()) {
      case ObTxCommitData::COMMIT: tx_result = OB_TRANS_COMMITED; break;
      case ObTxCommitData::ABORT: tx_result = OB_TRANS_KILLED; break;
      default:
        TRANS_LOG(ERROR, "oops! unexpected tx_state in tx data", K(ctx_tx_data_.get_state()));
      }
      if (SCN::min_scn() == start_working_log_ts_) {
        TRANS_LOG(ERROR, "missing callback scheduler, do callback", K(tx_result), KPC(this));
      } else {
        TRANS_LOG(WARN, "missing callback scheduler maybe, do callback", K(tx_result), KPC(this));
      }
      // NOTE: callback scheduler may introduce deadlock, need take care
      trans_service_->handle_tx_commit_result(trans_id_, tx_result, SCN());
      FORCE_PRINT_TRACE(tlog_, "[missing callback scheduler] ");
    }

    if (OB_NOT_NULL(retain_ctx_func_ptr_)) {
      TRANS_LOG(INFO, "the memory of retain_ctx_func_ptr is active, need free", K(ret),
                KP(retain_ctx_func_ptr_), KPC(retain_ctx_func_ptr_), KPC(this));
    }
    (void)try_gc_retain_ctx_func_();

    if (NULL != tlog_) {
      print_trace_log_if_necessary_();
      tlog_ = NULL;
    }

    ctx_tx_data_.destroy();
    mds_cache_.destroy();
    exec_info_.destroy();

    big_segment_info_.reset();

    reset_log_cbs_();

    timeout_task_.destroy();
    trace_info_.reset();
    block_frozen_memtable_ = nullptr;
    is_inited_ = false;
  }
}

void ObPartTransCtx::default_init_()
{
  // TODO for ObTransCtx
  // lock_.reset();
  stc_.reset();
  commit_cb_.reset();
  callback_scheduler_on_clear_ = false;
  pending_callback_param_ = OB_SUCCESS;
  trans_need_wait_wrap_.reset();
  is_exiting_ = false;
  for_replay_ = false;
  has_pending_callback_ = false;

  request_id_ = OB_INVALID_TIMESTAMP;
  session_id_ = 0;
  timeout_task_.reset();
  trace_info_.reset();
  can_elr_ = false;

  // TODO ObPartTransCtx
  ObTxCycleTwoPhaseCommitter::reset();
  is_inited_ = false;
  mt_ctx_.reset();
  end_log_ts_.set_max();
  trans_expired_time_ = INT64_MAX;
  stmt_expired_time_ = INT64_MAX;
  cur_query_start_time_ = 0;
  clean_retain_cause_();

  upstream_state_ = ObTxState::INIT;
  msg_2pc_cache_ = nullptr;
  exec_info_.reset();
  ctx_tx_data_.reset();
  sub_state_.reset();
  reset_log_cbs_();
  last_op_sn_ = 0;
  last_scn_.reset();
  first_scn_.reset();
  dup_table_follower_max_read_version_.reset();
  rec_log_ts_.reset();
  prev_rec_log_ts_.reset();
  big_segment_info_.reset();
  is_ctx_table_merged_ = false;
  mds_cache_.reset();
  retain_ctx_func_ptr_ = nullptr;
  start_replay_ts_.reset();
  start_recover_ts_.reset();
  is_incomplete_replay_ctx_ = false;
  is_submitting_redo_log_for_freeze_ = false;
  start_working_log_ts_ = SCN::min_scn();
  max_2pc_commit_scn_.reset();
  coord_prepare_info_arr_.reset();
  reserve_allocator_.reset();
  elr_handler_.reset();
  state_info_array_.reset();
  lastest_snapshot_.reset();
  standby_part_collected_.reset();
  trace_log_.reset();
}

int ObPartTransCtx::init_log_cbs_(const ObLSID &ls_id, const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < PREALLOC_LOG_CALLBACK_COUNT; ++i) {
    if (OB_FAIL(log_cbs_[i].init(ls_id, tx_id, this, false))) {
      TRANS_LOG(WARN, "log cb init failed", KR(ret));
    } else if (!free_cbs_.add_last(&log_cbs_[i])) {
      log_cbs_[i].destroy();
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "add to free list failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(final_log_cb_.init(ls_id, tx_id, this, false))) {
      TRANS_LOG(WARN, "init commit log cb failed", K(ret));
    } else {
      TRANS_LOG(DEBUG, "init commit log cb success", K(ret), KP(&final_log_cb_), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::extend_log_cbs_()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTxLogCb *cb = NULL;
  if (busy_cbs_.get_size() >= OB_TX_MAX_LOG_CBS + 1) {
    ret = OB_TX_NOLOGCB;
  } else if (ATOMIC_LOAD(&is_submitting_redo_log_for_freeze_) == false &&
      busy_cbs_.get_size() >= OB_TX_MAX_LOG_CBS + 1 - RESERVE_LOG_CALLBACK_COUNT_FOR_FREEZING) {
    ret = OB_TX_NOLOGCB;
  } else if (OB_ISNULL(ptr = reserve_allocator_.alloc(sizeof(ObTxLogCb)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (OB_ISNULL(cb = new(ptr) ObTxLogCb)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "log callback construct failed", K(ret));
    } else if (OB_FAIL(cb->init(ls_id_, trans_id_, this, true))) {
      TRANS_LOG(WARN, "log callback init failed", K(ret));
      cb->~ObTxLogCb();
    } else if (!free_cbs_.add_last(cb)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "add to free list failed", KR(ret));
      cb->~ObTxLogCb();
    } else {
      // do nothing
    }
    if (OB_FAIL(ret)) {
      reserve_allocator_.free(ptr);
    }
  }
  return ret;
}

void ObPartTransCtx::reset_log_cb_list_(common::ObDList<ObTxLogCb> &cb_list)
{
  ObTxLogCb *cb = NULL;
  while (OB_NOT_NULL(cb = cb_list.remove_first())) {
    const bool is_dynamic = cb->is_dynamic();
    if (OB_NOT_NULL(cb->get_tx_data())) {
      ObTxData *tx_data = cb->get_tx_data();
      ctx_tx_data_.free_tmp_tx_data(tx_data);
      cb->set_tx_data(nullptr);
    }
    if (!is_dynamic) {
      cb->reset();
    } else {
      cb->~ObTxLogCb();
      reserve_allocator_.free(cb);
    }
  }
}

void ObPartTransCtx::reset_log_cbs_()
{
  reset_log_cb_list_(free_cbs_);
  reset_log_cb_list_(busy_cbs_);
  final_log_cb_.reset();
}

// thread-unsafe
int ObPartTransCtx::start_trans()
{
  int ret = OB_SUCCESS;
  // first register task timeout = 10s,
  // no need to unregister/register task when sp transaction commit
  int64_t default_timeout_us = 10000000;
  const int64_t left_time = trans_expired_time_ - ObClockGenerator::getClock();

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
    if (left_time > 0 && left_time < default_timeout_us) {
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(left_time))) {
        TRANS_LOG(WARN, "register timeout task error", KR(ret), "context", *this);
      }
    }
  }
  if (OB_FAIL(ret)) {
    set_exiting_();
  }
  TRANS_LOG(DEBUG, "start trans", K(ret), K(trans_id_), "ref", get_ref());
  REC_TRANS_TRACE_EXT2(tlog_, start_trans, OB_ID(ret), ret, OB_ID(left_time), left_time, OB_ID(ref),
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
    CtxLockGuard guard(lock_, false);
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

      if (trans_expired_time_ > 0 && trans_expired_time_ < INT64_MAX) {
        if (part_trans_action_ == ObPartTransAction::COMMIT || part_trans_action_ == ObPartTransAction::ABORT) {
          if (now >= trans_expired_time_ + OB_TRANS_WARN_USE_TIME) {
            tmp_ret = OB_TRANS_COMMIT_TOO_MUCH_TIME;
            LOG_DBA_ERROR(OB_TRANS_COMMIT_TOO_MUCH_TIME,
                    "msg", "transaction commit cost too much time",
                    "ret", tmp_ret,
                    K(*this));
          }
        } else {
          if (now >= ctx_create_time_ + OB_TRANS_WARN_USE_TIME) {
            print_first_mvcc_callback_();
            tmp_ret = OB_TRANS_LIVE_TOO_MUCH_TIME;
            LOG_DBA_WARN(OB_TRANS_LIVE_TOO_MUCH_TIME,
                    "msg", "transaction live cost too much time without commit or abort",
                    "ret", tmp_ret,
                    K(*this));
          }
        }
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

      if (mds_cache_.need_retry_submit_mds()) {
        if (OB_TMP_FAIL(submit_multi_data_source_())) {
          TRANS_LOG(WARN, "retry submit mds log failed", K(tmp_ret), K(*this));
        } else {
          mds_cache_.set_need_retry_submit_mds(false);
        }
      }

      // handle commit timeout on root node
      if (!is_sub2pc() && !is_follower_() && part_trans_action_ == ObPartTransAction::COMMIT &&
          (is_local_tx_() || is_root())) {
        if (tx_expired) {
          tmp_ret = post_tx_commit_resp_(OB_TRANS_TIMEOUT);
          TRANS_LOG(INFO, "callback scheduler txn has timeout", K(tmp_ret), KPC(this));
        } else if (commit_expired) {
          tmp_ret = post_tx_commit_resp_(OB_TRANS_STMT_TIMEOUT);
          TRANS_LOG(INFO, "callback scheduler txn commit has timeout", K(tmp_ret), KPC(this));
        } else {
          // make scheduler retry commit if clog disk has fatal error
          logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
          if (NULL != detector && detector->is_clog_disk_has_fatal_error()) {
            tmp_ret = post_tx_commit_resp_(OB_NOT_MASTER);
            TRANS_LOG(WARN, "clog disk has fatal error, make scheduler retry commit", K(tmp_ret), KPC(this));
          }
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
              } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
                TRANS_LOG(WARN, "do prepare failed", K(ret), K(*this));
              } else {
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
    REC_TRANS_TRACE_EXT2(tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(used), timeguard, OB_ID(ref),
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
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    TRANS_LOG(INFO, "trans is existing when being killed", K(*this), K(arg));
  } else {
    bool notify_scheduler_tx_killed = false;
    if (arg.graceful_) {
      if (is_in_2pc_()) {
        ret = OB_TRANS_CANNOT_BE_KILLED;
      } else if (FALSE_IT(notify_scheduler_tx_killed = !is_follower_() && part_trans_action_ == ObPartTransAction::START)) {
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
      notify_scheduler_tx_killed = !is_follower_() && part_trans_action_ == ObPartTransAction::START;
      // if ctx was killed gracefully or forcely killed
      // notify scheduler commit result, if in committing
      if (!is_follower_() && part_trans_action_ == ObPartTransAction::COMMIT &&
          (is_local_tx_() || is_root())) {
      // notify scheduler only if commit callback has not been armed
        if (commit_cb_.is_enabled() && !commit_cb_.is_inited()) {
          if (exec_info_.scheduler_ == addr_) {
            ObTxCommitCallback cb;
            cb.init(trans_service_, trans_id_, cb_param, SCN());
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
    if (notify_scheduler_tx_killed) {
      notify_scheduler_tx_killed_(arg.graceful_ ? ObTxAbortCause::PARTICIPANT_KILLED_GRACEFULLY : ObTxAbortCause::PARTICIPANT_KILLED_FORCEDLY);
    }
  }
  TRANS_LOG(WARN, "trans is killed", K(ret), K(arg), K(cb_param), KPC(this));
  REC_TRANS_TRACE_EXT2(tlog_, kill, OB_ID(ret), ret, OB_ID(arg1), arg.graceful_, OB_ID(used),
                       timeguard.get_diff(), OB_ID(ref), get_ref());
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
  } else {
    if (commit_time.is_valid()) {
      set_stc_(commit_time);
    } else {
      set_stc_by_now_();
    }
    if (parts.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "the size of participant is 0 when commit", KPC(this));
    } else if (parts.count() == 1 && parts[0] == ls_id_ && !exec_info_.is_dup_tx_) {
      exec_info_.trans_type_ = TransType::SP_TRANS;
      can_elr_ = (trans_service_->get_tx_elr_util().is_can_tenant_elr() ? true : false);
      if (OB_FAIL(one_phase_commit_())) {
        TRANS_LOG(WARN, "start sp coimit fail", K(ret), KPC(this));
      }
    } else {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
      // set 2pc upstream to self
      set_2pc_upstream_(ls_id_);
      if (OB_FAIL(two_phase_commit())) {
        TRANS_LOG(WARN, "start dist coimit fail", K(ret), KPC(this));
      }
    }
  }
  if (OB_SUCC(ret)) {
    commit_cb_.enable();
    part_trans_action_ = ObPartTransAction::COMMIT;
    last_request_ts_ = ObClockGenerator::getClock();
  }
  REC_TRANS_TRACE_EXT2(tlog_, commit, OB_ID(ret), ret,
                       OB_ID(tid), GETTID(),
                       OB_ID(ref), get_ref());
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
      ret = OB_NOT_MASTER;
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

bool ObPartTransCtx::need_update_schema_version(const int64_t log_id, const SCN)
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

int ObPartTransCtx::trans_replay_abort_(const SCN &final_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mt_ctx_.trans_replay_end(false, /*commit*/
                                       ctx_tx_data_.get_commit_version(),
                                       final_log_ts))) {
    TRANS_LOG(WARN, "transaction replay end error", KR(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_) {
      if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->remove_commiting_dup_trx(trans_id_))) {
        TRANS_LOG(WARN, "remove committing dup table trx failed", K(ret), K(final_log_ts),
                  KPC(this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::trans_replay_commit_(const SCN &commit_version,
                                         const SCN &final_log_ts,
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

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_) {
      if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->remove_commiting_dup_trx(trans_id_))) {
        TRANS_LOG(WARN, "remove committing dup table trx failed", K(ret), K(commit_version), K(final_log_ts),
                  KPC(this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::update_publish_version_(const SCN &publish_version, const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (!publish_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(publish_version));
  } else if (OB_FAIL(ctx_tx_data_.set_commit_version(publish_version))) {
    TRANS_LOG(WARN, "set commit version failed", K(ret));
  } else {
    trans_service_->get_tx_version_mgr().update_max_commit_ts(publish_version, false);
    REC_TRANS_TRACE_EXT2(tlog_, push_max_commit_version, OB_ID(trans_version), publish_version,
                         OB_ID(ref), get_ref());
  }

  return ret;
}

int ObPartTransCtx::replay_start_working_log(const SCN start_working_ts)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);
  exec_info_.data_complete_ = false;
  start_working_log_ts_ = start_working_ts;
  if (!has_persisted_log_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "trx has no persisten log", K(ret), K(*this));
  }

  return ret;
}

bool ObPartTransCtx::is_in_2pc_() const
{
  // The order is important if you use it without lock.
  return is_2pc_logging_() || is_in_durable_2pc_();
}

// The txn is in the durable state between prepare and clear
bool ObPartTransCtx::is_in_durable_2pc_() const
{
  ObTxState state = exec_info_.state_;
  return state >= ObTxState::PREPARE;
}

bool ObPartTransCtx::is_logging_() const { return !busy_cbs_.is_empty(); }

bool ObPartTransCtx::need_force_abort_() const
{
  return sub_state_.is_force_abort() && !sub_state_.is_state_log_submitted();
}

bool ObPartTransCtx::is_force_abort_logging_() const
{
  return sub_state_.is_force_abort() && sub_state_.is_state_log_submitting();
}

bool ObPartTransCtx::has_persisted_log_() const
{
  return exec_info_.max_applying_log_ts_.is_valid();
}

int ObPartTransCtx::gts_callback_interrupted(const int errcode)
{
  int ret = OB_SUCCESS;
  UNUSED(errcode);

  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
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
                                     const SCN &gts,
                                     const MonotonicTs receive_gts_ts)
{
  TRANS_LOG(DEBUG, "ObPartTransCtx get_gts_callback begin", KPC(this));
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    // TRANS_LOG(INFO, "get_gts_callback begin", K(*this));
    CtxLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(!gts.is_valid())) {
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
      const SCN max_read_ts = trans_service_->get_tx_version_mgr().get_max_read_ts();
      // TRANS_LOG(INFO, "get_gts_callback mid", K(*this), K(log_type));
      if (is_local_tx_() && exec_info_.is_dup_tx_) {
        TRANS_LOG(ERROR, "invalid trans type for a local dup_table trx", K(ret), KPC(this));
        exec_info_.trans_type_ = TransType::DIST_TRANS;
      }

      if (is_local_tx_()) {
        if (OB_FAIL(ctx_tx_data_.set_commit_version(SCN::max(gts, max_read_ts)))) {
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
        const SCN local_prepare_version = SCN::max(gts, max_read_ts);
        // should not overwrite the prepare verison of other participants
        exec_info_.prepare_version_ = SCN::max(local_prepare_version, exec_info_.prepare_version_);

        if (get_upstream_state() <= ObTxState::PREPARE
            && OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
          TRANS_LOG(WARN, "drive into prepare phase failed in gts callback", K(ret), KPC(this));
        }
      }

      need_revert_ctx = true;
    }
    REC_TRANS_TRACE_EXT2(tlog_, get_gts_callback, OB_Y(ret), OB_ID(srr), srr.mts_, OB_Y(gts),  OB_ID(ref), get_ref());
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

int ObPartTransCtx::gts_elapse_callback(const MonotonicTs srr, const SCN &gts)
{
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(!gts.is_valid())) {
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

      if (is_local_tx_() && exec_info_.is_dup_tx_) {
        TRANS_LOG(ERROR, "invalid trans type for a local dup_table trx", K(ret), KPC(this));
        exec_info_.trans_type_ = TransType::DIST_TRANS;
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
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::try_enter_pre_commit_state())) {
          TRANS_LOG(WARN, "enter_pre_commit_state failed", K(ret), KPC(this));
        } else {
          // TODO, refine in 4.1
          if (is_sub2pc()) {
            if (OB_FAIL(reply_to_scheduler_for_sub2pc(SUBCOMMIT_RESP))) {
              TRANS_LOG(ERROR, "fail to do sub prepare", K(ret), K(*this));
            }
            TRANS_LOG(INFO, "apply prepare log for sub trans", K(ret), K(*this));
          }
        }
      }
      need_revert_ctx = true;
    }
    REC_TRANS_TRACE_EXT2(tlog_, gts_elapse_callback, OB_Y(ret), OB_ID(srr), srr.mts_, OB_Y(gts),  OB_ID(ref), get_ref());
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

int ObPartTransCtx::get_prepare_version_if_prepared(bool &is_prepared, SCN &prepare_version)
{
  int ret = OB_SUCCESS;
  ObTxState cur_state = exec_info_.state_;

  if (ObTxState::PREPARE == cur_state || ObTxState::PRE_COMMIT == cur_state) {
    is_prepared = true;
    prepare_version = exec_info_.prepare_version_;
  } else if (ObTxState::COMMIT == cur_state || ObTxState::ABORT == cur_state
             || ObTxState::CLEAR == cur_state) {
    is_prepared = true;
    prepare_version.set_max();
  } else {
    is_prepared = false;
    prepare_version.set_max();
  }
  if (is_prepared && OB_INVALID_SCN_VAL == prepare_version.get_val_for_gts()) {
    TRANS_LOG(ERROR, "invalid prepare version", K(cur_state));
    // try lock
    print_trace_log();
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
  } else if (need_force_abort_() && !exec_info_.scheduler_.is_valid()) {
    is_alive = false;
    TRANS_LOG(WARN, "a aborting trans will be gc with invalid scheduler", K(ret), K(is_alive), KPC(this));
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
    REC_TRANS_TRACE_EXT2(tlog_, tx_ctx_gc, OB_ID(ref), get_ref());
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
    CtxLockGuard guard(lock_, false);
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
      TRANS_LOG(WARN, "scheduler server is not alive, tx ctx will do gc");
      if (OB_FAIL(gc_ctx_())) {
        TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), "context", *this);
      }
    } else {
      // do nothing
    }
    // 2. Ask for the status of scheduler
    ret = OB_SUCCESS;
    int ctx_status = OB_SUCCESS;
    if (sub_state_.is_force_abort()) {
      ctx_status = OB_TRANS_KILLED;
    }

    if (is_alive && need_check_scheduler) {
      post_keepalive_msg_(ctx_status);
      last_ask_scheduler_status_ts_ = ObClockGenerator::getClock();
    }

    if (is_committing_() || ObClockGenerator::getClock() > trans_expired_time_) {
      (void)check_and_register_timeout_task_();
    }
  }
  return OB_SUCCESS;
}

int ObPartTransCtx::recover_tx_ctx_table_info(ObTxCtxTableInfo &ctx_info)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  ObTxBufferNodeArray _unused_;
  if (!ctx_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ctx_info));
  // } else if (OB_FAIL(init_tx_data_(ctx_info.ls_id_, ctx_info.tx_id_))) {
  //   TRANS_LOG(WARN, "unexpected null ptr", K(*this));
  } else if (OB_FAIL(mt_ctx_.recover_from_table_lock_durable_info(ctx_info.table_lock_info_))) {
    TRANS_LOG(ERROR, "recover_from_table_lock_durable_info failed", K(ret));
  } else if (OB_FAIL(ctx_tx_data_.recover_tx_data(ctx_info.tx_data_guard_))) {
    TRANS_LOG(WARN, "recover tx data failed", K(ret), K(ctx_tx_data_));
  } else if (OB_FAIL(exec_info_.assign(ctx_info.exec_info_))) {
    TRANS_LOG(WARN, "exec_info assign error", K(ret), K(ctx_info));
  } else {
    trans_id_ = ctx_info.tx_id_;
    ls_id_ = ctx_info.ls_id_;
    if (!exec_info_.upstream_.is_valid() &&
        !is_local_tx_() &&
       (ObTxState::REDO_COMPLETE == exec_info_.state_ ||
        ObTxState::PREPARE == exec_info_.state_ ||
        ObTxState::PRE_COMMIT == exec_info_.state_ ||
        ObTxState::COMMIT == exec_info_.state_ ||
        ObTxState::CLEAR == exec_info_.state_)) {
      set_2pc_upstream_(ls_id_);
      TRANS_LOG(INFO, "set upstream to self", K(*this));
    }
    // set upstream state when recover tx ctx table
    set_upstream_state(get_downstream_state());

    if (ObTxState::REDO_COMPLETE == get_downstream_state()) {
      sub_state_.set_info_log_submitted();
    }
    if (exec_info_.prepare_version_.is_valid()) {
      mt_ctx_.set_trans_version(exec_info_.prepare_version_);
    }
    exec_info_.multi_data_source_.reset();
    exec_info_.mds_buffer_ctx_array_.reset();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (FALSE_IT(ctx_info.exec_info_.mrege_buffer_ctx_array_to_multi_data_source())) {
    } else if (OB_FAIL(deep_copy_mds_array_(ctx_info.exec_info_.multi_data_source_, _unused_))) {
      TRANS_LOG(WARN, "deep copy ctx_info mds_array failed", K(ret));
    } else if (FALSE_IT(ctx_info.exec_info_.clear_buffer_ctx_in_multi_data_source())) {
      // clear it cause buffer ctx memory need released
      // and ObString in buffer node no need released cause it's just part reference of deserialized buffer
    } else if (FALSE_IT(mt_ctx_.update_checksum(exec_info_.checksum_,
                                                exec_info_.checksum_scn_))) {
      TRANS_LOG(ERROR, "recover checksum failed", K(ret), KPC(this), K(ctx_info));
    } else if (!is_local_tx_() && OB_FAIL(ObTxCycleTwoPhaseCommitter::recover_from_tx_table())) {
      TRANS_LOG(ERROR, "recover_from_tx_table failed", K(ret), KPC(this));
    } else {
      is_ctx_table_merged_ = true;
    }

    // insert into retain ctx mgr if it will not replay commit or abort log
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (exec_info_.multi_data_source_.count() > 0
               && (ObTxState::COMMIT == exec_info_.state_ || ObTxState::ABORT == exec_info_.state_
                   || ObTxState::CLEAR == exec_info_.state_)) {
      if (OB_FAIL(try_alloc_retain_ctx_func_())) {
        TRANS_LOG(WARN, "alloc retain ctx func failed", K(ret), KPC(this));
      } else if (OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG,
                                                     exec_info_.max_applying_log_ts_,
                                                     exec_info_.max_durable_lsn_, false))) {
        TRANS_LOG(WARN, "insert into retain ctx mgr failed", K(ret), KPC(this));
      }
      if (OB_SUCC(ret)) {
        TRANS_LOG(INFO, "recover retain ctx into mgr success", K(ret), K(trans_id_), K(ls_id_));
      }
    }

    if (exec_info_.is_dup_tx_ && get_downstream_state() == ObTxState::REDO_COMPLETE
        && exec_info_.max_applying_log_ts_ == exec_info_.max_applied_log_ts_) {
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(dup_table_before_preapre_(exec_info_.max_applied_log_ts_, true/*before_replay*/))) {
        TRANS_LOG(WARN, "set commit_info scn as before_prepare_version failed", K(ret), KPC(this));
      }
    }

    if (exec_info_.is_dup_tx_
        && (get_downstream_state() >= ObTxState::REDO_COMPLETE
            && get_downstream_state() < ObTxState::COMMIT)) {
      if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->add_commiting_dup_trx(trans_id_))) {
        TRANS_LOG(WARN, "add committing dup table trx failed", K(ret), KPC(this));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if ((exec_info_.trans_type_ == TransType::SP_TRANS && ObTxState::COMMIT == exec_info_.state_)
               || (exec_info_.trans_type_ == TransType::DIST_TRANS
                   && ObTxState::CLEAR == exec_info_.state_)
               || (ObTxState::ABORT == exec_info_.state_)) {
      set_exiting_();
      TRANS_LOG(INFO, "set exiting with a finished trx in recover", K(ret), KPC(this));
    }

    if (OB_SUCC(ret)) {
      if ((ObTxState::COMMIT == exec_info_.state_
           && exec_info_.max_applying_log_ts_ == exec_info_.max_applied_log_ts_)
          || ObTxState::CLEAR == exec_info_.state_) {
        ls_tx_ctx_mgr_->update_max_replay_commit_version(ctx_tx_data_.get_commit_version());
        //update max_commit_ts for dup table because of migrate or recover
        MTL(ObTransService *)
            ->get_tx_version_mgr()
            .update_max_commit_ts(ctx_tx_data_.get_commit_version(), false);
      }
      start_recover_ts_ = exec_info_.max_applying_log_ts_;
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
int ObPartTransCtx::serialize_tx_ctx_to_buffer(ObTxLocalBuffer &buffer, int64_t &serialize_size)
{
  int ret = OB_SUCCESS;
  ObTxCtxTableInfo ctx_info;

  CtxLockGuard guard(lock_);
  // 1. Tx ctx has already exited, so it means that it may have no chance to
  //    push its rec_log_ts to aggre_rec_log_ts, so we must not persist it
  // NB: You need take note that retained tx ctx should be dumped due to
  //     multi-source data.
  if (is_exiting_ && get_retain_cause() == RetainCause::UNKOWN) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(INFO, "tx ctx has exited", K(ret), KPC(this));
  // 2. Tx ctx has no persisted log, so donot need persisting
  } else if (!exec_info_.max_applying_log_ts_.is_valid()) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(INFO, "tx ctx has no persisted log", K(ret), KPC(this));
  // 3. Tx ctx has no persisted log, so donot need persisting
  } else if (is_incomplete_replay_ctx_) {
    // NB: we need refresh rec log ts for incomplete replay ctx
    if (OB_FAIL(refresh_rec_log_ts_())) {
      TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
      TRANS_LOG(INFO, "tx ctx is in complete replay ctx", K(ret), KPC(this));
    }
  // 3. Fetch the current state of the tx ctx table
  } else if (OB_FAIL(get_tx_ctx_table_info_(ctx_info))) {
    TRANS_LOG(WARN, "get tx ctx table info failed", K(ret), K(*this));
  } else if (OB_UNLIKELY(!ctx_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx ctx info invalid", K(ret), K(ctx_info));
  // 4. Refresh the rec_log_ts for the next checkpoint
  } else if (OB_FAIL(refresh_rec_log_ts_())) {
    TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), K(*this));
  } else {
    // 5. Do serialize
    int64_t pos = 0;
    serialize_size = ctx_info.get_serialize_size();
    if (OB_FAIL(buffer.reserve(serialize_size))) {
      TRANS_LOG(WARN, "Failed to reserve local buffer", KR(ret));
    } else if (OB_FAIL(ctx_info.serialize(buffer.get_ptr(), serialize_size, pos))) {
      TRANS_LOG(WARN, "failed to serialize ctx_info", KR(ret), K(ctx_info), K(pos));
    } else {
      is_ctx_table_merged_ = true;
    }
  }

  return ret;
}

const SCN ObPartTransCtx::get_rec_log_ts() const
{
  CtxLockGuard guard(lock_);
  return get_rec_log_ts_();
}

const SCN ObPartTransCtx::get_rec_log_ts_() const
{
  SCN log_ts = SCN::max_scn();

  // Before the checkpoint of the tx ctx table is succeed, we should still use
  // the prev_log_ts. And after successfully checkpointed, we can use the new
  // rec_log_ts if exist
  if (prev_rec_log_ts_.is_valid()) {
    log_ts = prev_rec_log_ts_;
  } else if (rec_log_ts_.is_valid()) {
    log_ts = rec_log_ts_;
  }

  TRANS_LOG(DEBUG, "part ctx get rec log ts", K(*this), K(log_ts));

  return log_ts;
}

int ObPartTransCtx::on_tx_ctx_table_flushed()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  // To mark the checkpoint is succeed, we reset the prev_rec_log_ts
  prev_rec_log_ts_.reset();

  return ret;
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

int ObPartTransCtx::remove_callback_for_uncommited_txn(
  const memtable::ObMemtableSet *memtable_set)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(memtable_set)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(memtable_set));
  } else if (OB_FAIL(mt_ctx_.remove_callback_for_uncommited_txn(memtable_set, exec_info_.max_applied_log_ts_))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(mt_ctx_),
              K(memtable_set), K(exec_info_.max_applied_log_ts_));
  }

  return ret;
}

// this function is only called by instant logging and freezing,
// they both view ret code OB_BLOCK_FROZEN as success.
int ObPartTransCtx::submit_redo_log(const bool is_freeze)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg("submit_redo_log", 100000);
  bool try_submit = false;

  if (is_freeze) {
    bool need_submit = !is_logging_blocked();
    if (need_submit) {
      // spin lock
      CtxLockGuard guard(lock_);
      tg.click();
      ret = submit_redo_log_for_freeze_(try_submit);
      tg.click();
      if (try_submit) {
        REC_TRANS_TRACE_EXT2(tlog_, submit_instant_log, OB_Y(ret), OB_ID(arg2), is_freeze,
                             OB_ID(used), tg.get_diff(), OB_ID(ref), get_ref());
      }
    }
  } else if (!mt_ctx_.pending_log_size_too_large()) {
  } else if (OB_FAIL(lock_.try_lock())) {
    // try lock
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(ERROR, "try lock error, unexpected error", K(ret), K(*this));
    }
  } else {
    CtxLockGuard guard(lock_, false);
    tg.click();
    ret = check_and_submit_redo_log_(try_submit);
    tg.click();
    if (try_submit) {
      REC_TRANS_TRACE_EXT2(tlog_, submit_instant_log, OB_Y(ret), OB_ID(arg2), is_freeze,
                           OB_ID(used), tg.get_diff(),
                           OB_ID(ref), get_ref());
    }
  }
  if (OB_BLOCK_FROZEN == ret) {
    ret = OB_SUCCESS;
  } else {
    clear_block_frozen_memtable();
  }

  return ret;
}

int ObPartTransCtx::check_and_submit_redo_log_(bool &try_submit)
{
  int ret = OB_SUCCESS;
  bool is_tx_committing = ObTxState::INIT != get_downstream_state();
  bool final_log_submitting = final_log_cb_.is_valid();

  if (!is_tx_committing && !final_log_submitting && !is_force_abort_logging_()) {
    (void)mt_ctx_.merge_multi_callback_lists_for_immediate_logging();
    ret = submit_log_impl_(ObTxLogType::TX_REDO_LOG);
    try_submit = true;
  }

  return ret;
}

int ObPartTransCtx::submit_redo_log_for_freeze_(bool &try_submit)
{
  int ret = OB_SUCCESS;

  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, true);
  if (OB_FAIL(check_and_submit_redo_log_(try_submit))) {
    TRANS_LOG(WARN, "fail to submit redo log for freeze", K(ret));
  }
  if (try_submit && (OB_SUCC(ret) || OB_BLOCK_FROZEN == ret)) {
    ret = submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG);
  }
  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, false);

  return ret;
}

int ObPartTransCtx::set_block_frozen_memtable(memtable::ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "memtable cannot be null", K(ret), KPC(this));
  } else {
    ATOMIC_STORE(&block_frozen_memtable_, memtable);
  }
  return ret;
}

void ObPartTransCtx::clear_block_frozen_memtable()
{
  ATOMIC_STORE(&block_frozen_memtable_, nullptr);
}

bool ObPartTransCtx::is_logging_blocked()
{
  memtable::ObMemtable *memtable = ATOMIC_LOAD(&block_frozen_memtable_);
  return OB_NOT_NULL(memtable) && memtable->get_logging_blocked();
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
  if (is_force_abort_logging_()) {
    // do nothing
  } else if(OB_FALSE_IT(sub_state_.set_force_abort())) {

  } else if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_ABORT_LOG))) {
    TRANS_LOG(WARN, "submit abort log failed", KR(ret), K(*this));
  } else {
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
  // if abort was caused by internal impl reason, don't disturb
  if (ObTxAbortCause::IMPLICIT_ROLLBACK != reason) {
    TRANS_LOG(INFO, "tx abort", K(ret), K(reason), "reason_str", ObTxAbortCauseNames::of(reason), KPC(this));
  }
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
  const SCN &commit_version = ctx_tx_data_.get_commit_version();
  const SCN &end_scn = ctx_tx_data_.get_end_log_ts();

  // STEP1: We need check whether the end_log_ts is valid before state is filled
  // in here because it will be used to cleanout the tnode if state is decided.
  // What's more the end_log_ts is also be used during mt_ctx_.trans_end to
  // backfill normal tnode.
  if (has_persisted_log_() && !end_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "end log ts is invalid when tx end", K(end_scn), K(ret), KPC(this));
  // STEP2: We need check whether the commi_version is valid before state is
  // filled in with commit here because it will be used to cleanout the tnode or
  // lock for read if state is decided. What's more the commit_version is also
  // be used during mt_ctx_.trans_end to backfill normal tnode..
  } else if (commit && !commit_version.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "commit version is invalid when tx end", K(ret), KPC(this));
  // STEP3: We need set status in order to kill concurrent read and write. What
  // you need pay attention to is that we rely on the status to report the
  // suicide before the tnode can be cleanout by concurrent read using state in
  // ctx_tx_data.
  } else if (!commit && FALSE_IT(mt_ctx_.set_tx_rollbacked())) {
  // STEP4: We need set state in order to inform others of the final status of
  // my txn. What you need pay attention to is that only after this action,
  // others can cleanout the unfinished txn state and see all your data. It
  // should guarantee that all necesary information(including commit_version and
  // end_scn) is settled. What's more, it accelerates the data visibility for
  // the user.
  } else if (OB_FAIL(ctx_tx_data_.set_state(state))) {
    TRANS_LOG(WARN, "set tx data state failed", K(ret), KPC(this));
  // STEP5: We need invoke mt_ctx_.trans_end after the ctx_tx_data is decided
  // and filled in because we obey the rule that ObMvccRowCallback::trans_commit
  // is callbacked from front to back so that if the read or write is standing
  // on one tx node, all previous tx node is decided or can be simply cleanout
  // (which depends on the state in the ctx_tx_data). In conclusion, the action
  // of callbacking is depended on all states in the ctx_tx_data.
  } else if (OB_FAIL(mt_ctx_.trans_end(commit, commit_version, end_scn))) {
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
  // Distributed transactions need to wait for the commit log majority successfully before
  // unlocking. If you want to know the reason, it is in the ::do_dist_commit
  if (OB_FAIL(tx_end_(commit))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), K(commit), "context", *this);
  } else if (commit) {
    // reset the early lock release stat after the txn commits
    elr_handler_.reset_elr_state();
  }

  TRANS_LOG(DEBUG, "trans end", K(ret), K(trans_id_), K(commit));

  return ret;
}

int ObPartTransCtx::on_success(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cur_ts = ObTimeUtility::current_time();
  ObTransStatistic::get_instance().add_clog_sync_time(tenant_id_, cur_ts - log_cb->get_submit_ts());
  ObTransStatistic::get_instance().add_clog_sync_count(tenant_id_, 1);
  {
    CtxLockGuard guard(lock_);
    const SCN log_ts = log_cb->get_log_ts();
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
    if (need_record_log_()) {
      // ignore error
      if (OB_SUCCESS != (tmp_ret = submit_record_log_())) {
        TRANS_LOG(WARN, "failed to submit record log", K(tmp_ret), K(*this));
      }
    }
    if (!ObTxLogTypeChecker::is_state_log(last_log_type)) {
      try_submit_next_log_();
    }
    // REC_TRANS_TRACE_EXT(tlog_, on_succ_cb, OB_ID(ret), ret,
    //                                        OB_ID(t), log_ts,
    //                                        OB_ID(ref), get_ref());
  }
  if (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this))) {
    TRANS_LOG(ERROR, "release ctx ref failed", KR(tmp_ret));
  }
  return ret;
}

int ObPartTransCtx::on_success_ops_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  const SCN log_ts = log_cb->get_log_ts();
  const palf::LSN log_lsn = log_cb->get_lsn();
  const ObTxCbArgArray &cb_arg_array = log_cb->get_cb_arg_array();
  ObTxBufferNodeArray tmp_array;

  if (OB_FAIL(common_on_success_(log_cb))) {
    TRANS_LOG(WARN, "common_on_success_ failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cb_arg_array.count(); i++) {
    const ObTxLogType log_type = cb_arg_array.at(i).get_log_type();
    if (ObTxLogType::TX_REDO_LOG == log_type) {
      // do nothing
    }  else if (ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type) {
      share::SCN notify_redo_scn =
          log_cb->get_first_part_scn().is_valid() ? log_cb->get_first_part_scn() : log_ts;
      if (OB_FAIL(log_cb->get_mds_range().move_from_cache_to_arr(mds_cache_,
                                                                 exec_info_.multi_data_source_))) {
        TRANS_LOG(WARN, "move from mds cache to durable arr failed", K(ret));
        // } else if (OB_FAIL(log_cb->get_mds_range().move_to(exec_info_.multi_data_source_))) {
        //   TRANS_LOG(WARN, "move MDS range into exec_info failed", K(ret));
      } else if (FALSE_IT(mds_cache_.clear_submitted_iterator())) {
        // do nothing
      } else if (OB_FAIL(notify_data_source_(NotifyType::ON_REDO,
                                             notify_redo_scn,
                                             false,
                                             log_cb->get_mds_range().get_range_array()))) {
        TRANS_LOG(WARN, "notify data source for ON_REDO", K(ret));
      } else {
        log_cb->get_mds_range().reset();
      }
    } else if (ObTxLogType::TX_BIG_SEGMENT_LOG == log_type) {
      remove_unsynced_segment_cb_(log_cb->get_log_ts());
    } else if (ObTxLogType::TX_ACTIVE_INFO_LOG == log_type) {
      if (log_ts > start_working_log_ts_) {
        exec_info_.data_complete_ = true;
      }
    } else if (ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
      ObTwoPhaseCommitLogType two_phase_log_type;
      set_durable_state_(ObTxState::REDO_COMPLETE);
      if (exec_info_.is_dup_tx_) {
        if (is_follower_()) {
          if (OB_FAIL(dup_table_before_preapre_(log_ts, true/*before_replay*/))) {
            TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(log_cb),
                      KPC(this));
          } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
            TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
          }
          TRANS_LOG(INFO, "need set before_prepare_version in on_success after switch_to_follower",
                    K(ret), KPC(log_cb));
        } else {
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(dup_table_tx_redo_sync_())) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "dup table redo sync error, need retry in trans_timer", K(ret), K(trans_id_), K(ls_id_));
              ret = OB_SUCCESS;
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
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
    } else if (ObTxLogType::TX_ROLLBACK_TO_LOG == log_type) {
      ObTxData *tx_data = log_cb->get_tx_data();
      if (OB_ISNULL(tx_data)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected null ptr", KR(ret), K(*this));
      } else {
        // although logs may be callbacked out of order,
        // insert into tx table all the way, tx table will
        // filter out the obsolete one.
        tx_data->end_scn_ = log_ts;
        if (OB_FAIL(ctx_tx_data_.insert_tmp_tx_data(tx_data))) {
          TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
        } else {
          log_cb->set_tx_data(nullptr);
        }
      }
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
      } else if (ObTxLogType::TX_COMMIT_LOG == log_type) {
        if (OB_SUCC(ret)) {
          if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
              && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, log_ts, log_lsn,
                                                     false))) {
            TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(log_cb), KPC(this));
          }
        }
        if (OB_SUCC(ret)) {
          if (is_local_tx_()) {
            if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
              TRANS_LOG(WARN, "set end log ts failed", K(ret));
            } else {
              if (OB_FAIL(on_local_commit_tx_())) {
                TRANS_LOG(WARN, "on local commit failed", KR(ret), K(*this));
              }
            }
          } else {
            const NotifyType type = NotifyType::ON_COMMIT;
            if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
              TRANS_LOG(WARN, "set end log ts failed", K(ret));
            } else if (OB_FAIL(notify_data_source_(type, log_ts, false, exec_info_.multi_data_source_))) {
              TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
            }
            ObTwoPhaseCommitLogType two_phase_log_type;
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
              TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this), K(log_type));
            } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
              TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this), K(two_phase_log_type));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (exec_info_.is_dup_tx_) {
            if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->remove_commiting_dup_trx(trans_id_))) {
              TRANS_LOG(WARN, "remove committing dup table trx failed", K(ret),
                        KPC(this));
            }
          }
        }
      } else if (ObTxLogType::TX_ABORT_LOG == log_type) {
        if (OB_SUCC(ret)) {
          if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN
              && OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, log_ts, log_lsn,
                                                     false))) {
            TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(log_cb), KPC(this));
          }
        }
        if (OB_SUCC(ret)) {
          if (is_local_tx_() || sub_state_.is_force_abort()) {
            if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
              TRANS_LOG(WARN, "set end log ts failed", K(ret));
            } else if (OB_FAIL(on_local_abort_tx_())) {
              TRANS_LOG(WARN, "on local abort failed", KR(ret), K(*this));
            }
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
            ObTwoPhaseCommitLogType two_phase_log_type;
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(switch_log_type_(log_type, two_phase_log_type))) {
              TRANS_LOG(WARN, "switch log type failed", KR(ret), K(*this), K(log_type));
            } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::apply_log(two_phase_log_type))) {
              TRANS_LOG(ERROR, "dist tx apply log failed", KR(ret), K(*this), K(two_phase_log_type));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (exec_info_.is_dup_tx_) {
            if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->remove_commiting_dup_trx(trans_id_))) {
              TRANS_LOG(WARN, "remove committing dup table trx failed", K(ret),
                        KPC(this));
            }
          }
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
                               OB_ID(ref), get_ref());
  }
  return ret;
}

int ObPartTransCtx::common_on_success_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const SCN log_ts = log_cb->get_log_ts();
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

void ObPartTransCtx::check_and_register_timeout_task_()
{
  int64_t tmp_ret = OB_SUCCESS;

  if (!timeout_task_.is_running()
      && !timeout_task_.is_registered()
      && !is_follower_()
      && !is_exiting_) {
    const int64_t timeout_left = is_committing_() ? trans_2pc_timeout_
        : MIN(MAX_TRANS_2PC_TIMEOUT_US, MAX(trans_expired_time_ - ObClockGenerator::getClock(), 1000 * 1000));
    if (OB_SUCCESS != (tmp_ret = register_timeout_task_(timeout_left))) {
      TRANS_LOG_RET(WARN, tmp_ret, "register timeout task failed", KR(tmp_ret), KPC(this));
    }
  }
}

int ObPartTransCtx::try_submit_next_log_(const bool for_freeze)
{
  int ret = OB_SUCCESS;
  ObTxLogType log_type = ObTxLogType::UNKNOWN;
  if (ObPartTransAction::COMMIT == part_trans_action_
      && !is_in_2pc_()
      && !need_force_abort_()) {
    if (is_follower_()) {
      ret = OB_NOT_MASTER;
    } else {
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
  }

  if (!for_freeze) {
    // ignore retcode
    (void)check_and_register_timeout_task_();
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
    const SCN log_ts = log_cb->get_log_ts();
    // TODO, dingxi
    mt_ctx_.sync_log_fail(log_cb->get_callbacks());
    log_cb->get_mds_range().range_sync_failed(mds_cache_);
    if (log_ts == ctx_tx_data_.get_start_log_ts()) {
      ctx_tx_data_.set_start_log_ts(SCN());
    }
    if (ObTxLogTypeChecker::is_state_log(log_type)) {
      sub_state_.clear_state_log_submitting();
    }
    if (OB_FAIL(fix_redo_lsns_(log_cb))) {
      TRANS_LOG(ERROR, "fix redo lsns failed", KR(ret), K(*this));
    }
    if (is_contain(log_cb->get_cb_arg_array(), ObTxLogType::TX_BIG_SEGMENT_LOG)) {
      remove_unsynced_segment_cb_(log_cb->get_log_ts());
    }
    if (ObTxLogType::TX_ROLLBACK_TO_LOG == log_type) {
      ObTxData *tx_data = log_cb->get_tx_data();
      if (OB_FAIL(ctx_tx_data_.free_tmp_tx_data(tx_data))) {
        TRANS_LOG(WARN, "free tx data failed", KR(ret), K(*this));
      } else {
        log_cb->set_tx_data(nullptr);
      }
    }
    if (ObTxLogType::TX_PREPARE_LOG == log_type) {
      if (!exec_info_.is_dup_tx_) {
        // do nothing
      } else if (OB_FAIL(dup_table_before_preapre_(exec_info_.max_applied_log_ts_, true/*before_replay*/))) {
        TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(this));
      } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
        TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
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
        if (OB_TMP_FAIL(defer_callback_scheduler_(OB_TRANS_KILLED, SCN::invalid_scn()))) {
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
                               OB_ID(ref), get_ref());
    TRANS_LOG(INFO, "ObPartTransCtx::on_failure end", KR(ret), K(*this), KPC(log_cb));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this))) {
    TRANS_LOG(ERROR, "release ctx ref failed", KR(tmp_ret));
  }
  return ret;
}

int ObPartTransCtx::get_local_max_read_version_(SCN &local_max_read_version)
{
  int ret = OB_SUCCESS;
  local_max_read_version = trans_service_->get_tx_version_mgr().get_max_read_ts();
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get_local_max_read_version_", KR(ret), K(local_max_read_version), K(*this));
  }
  return ret;
}

int ObPartTransCtx::get_gts_(SCN &gts)
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
        // REC_TRANS_TRACE_EXT2(tlog_, wait_get_gts, OB_Y(log_type));
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

  ObITsMgr *ts_mgr = trans_service_->get_ts_mgr();

  if (OB_FAIL(ts_mgr->wait_gts_elapse(tenant_id_,
                                      ctx_tx_data_.get_commit_version(),
                                      this,
                                      need_wait))) {
    TRANS_LOG(WARN, "wait gts elapse failed", KR(ret), K(*this));
  } else if (need_wait) {
    sub_state_.set_gts_waiting();
    if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), K(*this));
    }
    TRANS_LOG(INFO, "need wait gts elapse", KR(ret), K(*this));
    REC_TRANS_TRACE_EXT2(tlog_, wait_gts_elapse, OB_ID(ref), get_ref());
  }

  return ret;
}

int ObPartTransCtx::generate_prepare_version_()
{
  int ret = OB_SUCCESS;

  if (!mt_ctx_.is_prepared() || !exec_info_.prepare_version_.is_valid()) {
    SCN gts = SCN::min_scn();
    SCN local_max_read_version = SCN::min_scn();
    bool is_gts_ok = false;
    // Only the root participant require to request gts
    const bool need_gts = is_root() || exec_info_.is_dup_tx_;

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
      } else if(exec_info_.is_dup_tx_) {
        if (!dup_table_follower_max_read_version_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid dup_table_follower_max_read_version_", K(ret),
                    K(dup_table_follower_max_read_version_), KPC(this));
        } else {
          // should not overwrite the prepare version of other participants
          exec_info_.prepare_version_ = SCN::max(SCN::max(gts, local_max_read_version),
                                                 exec_info_.prepare_version_);
          exec_info_.prepare_version_ =
              SCN::max(exec_info_.prepare_version_, dup_table_follower_max_read_version_);
          TRANS_LOG(INFO,
                    "generate prepare version for dup table trx",
                    K(exec_info_.prepare_version_),
                    K(gts),
                    K(local_max_read_version),
                    K(dup_table_follower_max_read_version_),
                    K(trans_id_),
                    K(ls_id_));
        }
        if (exec_info_.prepare_version_ > gts) {
          mt_ctx_.before_prepare(exec_info_.prepare_version_);
        }
      } else {
        // should not overwrite the prepare version of other participants
        exec_info_.prepare_version_ = SCN::max(SCN::max(gts, local_max_read_version),
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
  if (!ctx_tx_data_.get_commit_version().is_valid()) {
    SCN gts;
    if (OB_FAIL(get_gts_(gts))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "get gts failed", KR(ret), K(*this));
      }
    } else {
      // the same as before prepare
      mt_ctx_.set_trans_version(gts);
      const SCN max_read_ts = trans_service_->get_tx_version_mgr().get_max_read_ts();
      if (OB_FAIL(ctx_tx_data_.set_commit_version(SCN::max(gts, max_read_ts)))) {
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
  // const bool log_for_lock_node = !(is_local_tx_() && (part_trans_action_ == ObPartTransAction::COMMIT));
  // // // //
  // Possible problems caused:
  // 1. block frozen : We wiil not add data_node_count if skip lock node in fill_redo. Then the next memtable
  //                  will return -4112 and the prev memtable will wait for lock node clearing unsubmitted_cnt.
  // 2. replay checksum error:
  //      |
  //      | part_trans_action != COMMIT
  //      v
  //    +-------------------------------------+
  //    |       start submit_commit_log       |
  //    +-------------------------------------+
  //      |
  //      |
  //      v
  //    +-------------------------------------+
  //    |     log_for_lock_node == false      |
  //    |     cal checksum with lock node     |
  //    +-------------------------------------+
  //      |
  //      |
  //      v
  //    +-------------------------------------+
  //    | submit commit log failed with -4038 |
  //    |        rewrite as OB_SUCCESS        |
  //    +-------------------------------------+
  //      |
  //      | part_trans_action = COMMIT
  //      v
  //    +-------------------------------------+
  //    |       retry submit commit log       |
  //    +-------------------------------------+
  //      |
  //      |
  //      v
  //    +-------------------------------------+
  //    |        replay checksum ERROR        |
  //    +-------------------------------------+

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
    ObTxRedoLog redo_log(get_redo_log_no_(), cluster_version_);
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
        TRANS_LOG(TRACE, "no redo log to be flushed", KR(ret), K(*this));
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
        TRANS_LOG(WARN, "encounter too big row size error, need extend log buffer", K(ret), K(*this));
        return_log_cb_(log_cb);
        log_cb = NULL;
        const int save_ret = ret;
        // rewrite ret
        ret = OB_SUCCESS;
        if (OB_FAIL(log_block.finish_mutator_buf(redo_log, 0))) {
          TRANS_LOG(WARN, "finish mutator buf failed", KR(ret), K(*this));
        } else if (OB_FAIL(log_block.extend_log_buf())) {
          TRANS_LOG(WARN, "extend log buffer failed", K(ret), K(*this));
          if (OB_ALLOCATE_MEMORY_FAILED != ret) {
            ret = save_ret;
          }
        } else {
          TRANS_LOG(INFO, "extend log buffer success", K(*this), K(log_block));
        }
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
              log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

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
                 log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  } else if (sub_state_.is_info_log_submitted()) {
    // state log already submitted, do nothing
  } else if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
      TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
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
                 log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
  } else if (OB_FAIL(check_dup_trx_with_submitting_all_redo(log_block, helper))) {
    TRANS_LOG(WARN, "check dup trx with submitting all redo failed", K(ret));
  } else {
    ObTxCommitInfoLog commit_info_log(
        exec_info_.scheduler_, exec_info_.participants_, exec_info_.upstream_,
        exec_info_.is_sub2pc_,
        exec_info_.is_dup_tx_, can_elr_, trace_info_.get_app_trace_id(),
        trace_info_.get_app_trace_info(), exec_info_.prev_record_lsn_, exec_info_.redo_lsns_,
        exec_info_.incremental_participants_, cluster_version_, exec_info_.xid_);

    if (OB_SUCC(ret)) {
      if (exec_info_.is_dup_tx_) {
        if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->add_commiting_dup_trx(trans_id_))) {
          TRANS_LOG(WARN, "add committing dup table trx failed", K(ret),
                    KPC(this));
        }
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(validate_commit_info_log_(commit_info_log))) {
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
                       log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

  if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
    TRANS_LOG(WARN, "submit multi source data failed", K(ret), K(*this));
  } else if (OB_FAIL(submit_redo_log_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
  } else {
    ObTxSEQ cur_submitted_seq_no = MAX(exec_info_.max_submitted_seq_no_, helper.max_seq_no_);
    ObTxActiveInfoLog active_info_log(exec_info_.scheduler_, exec_info_.trans_type_, session_id_,
                                      trace_info_.get_app_trace_id(),
                                      mt_ctx_.get_min_table_version(), can_elr_,
                                      addr_,                 //
                                      cur_query_start_time_, //
                                      false,                 // sub2pc
                                      exec_info_.is_dup_tx_,
                                      trans_expired_time_, epoch_, last_op_sn_, first_scn_,
                                      last_scn_, cur_submitted_seq_no,
                                      cluster_version_,
                                      exec_info_.xid_);
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
                log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
                         log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
                   log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  }

  if (OB_SUCC(ret)) {
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
  }


  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_ && !is_dup_table_redo_sync_completed_()) {
      if (OB_FAIL(submit_pending_log_block_(log_block, helper))) {
        TRANS_LOG(WARN, "submit pending log block failed", K(ret));
      } else {
        ret = OB_EAGAIN;
        TRANS_LOG(INFO, "need wait redo sync finish for a dup table trx", K(ret), K(trans_id_),
                  K(ls_id_));
      }
    }
  }


  if (OB_SUCC(ret)) {
    if (OB_FAIL(errism_submit_prepare_log_())) {
      TRANS_LOG(WARN, "errsim for submit prepare log", K(ret), KPC(this));
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
                log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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
  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);
  const bool local_tx = is_local_tx_();

  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  } else if (OB_FAIL(gen_final_mds_array_(multi_source_data))) {
    TRANS_LOG(WARN, "gen total multi source data failed", KR(ret), K(*this));
  } else {
    int64_t suggested_buf_size = ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE;
    if (local_tx &&
        multi_source_data.count() == 0 &&
        // 512B
        mt_ctx_.get_pending_log_size() < ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE / 4) {
      suggested_buf_size = ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE;
    }
    if (OB_FAIL(log_block.init(replay_hint, log_block_header, suggested_buf_size))) {
      TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
    } else if (local_tx) {
      if (!sub_state_.is_info_log_submitted()) {
        prev_lsn.reset();
        if (OB_FAIL(submit_multi_data_source_(log_block))) {
          TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
        } else if (OB_SUCC(submit_redo_commit_info_log_(log_block, has_redo, helper))) {
          // do nothing
        } else {
          TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_ && !is_dup_table_redo_sync_completed_()) {
      if (OB_FAIL(submit_pending_log_block_(log_block, helper))) {
        TRANS_LOG(WARN, "submit pending log block failed", K(ret));
      } else {
        ret = OB_EAGAIN;
        TRANS_LOG(INFO, "need wait redo sync finish for a dup table trx", K(ret), K(trans_id_),
                  K(ls_id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t checksum =
      (exec_info_.need_checksum_ && !is_incomplete_replay_ctx_ ? mt_ctx_.calc_checksum_all() : 0);
    SCN log_commit_version;
    if (!local_tx) {
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
    logservice::ObReplayBarrierType commit_log_barrier_type =  logservice::ObReplayBarrierType::NO_NEED_BARRIER;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_start_scn_in_commit_log_(commit_log))) {
        TRANS_LOG(WARN, "set start scn in commit log failed", K(ret), K(commit_log));
      } else if ((exec_info_.multi_data_source_.count() > 0 || mds_cache_.count() > 0)
                 && OB_FAIL(try_alloc_retain_ctx_func_())) {
        TRANS_LOG(WARN, "alloc retain ctx func for mds trans failed", K(ret), K(mds_cache_), KPC(this));
      } else if (OB_FAIL(decide_state_log_barrier_type_(ObTxLogType::TX_COMMIT_LOG,
                                                        commit_log_barrier_type))) {
        TRANS_LOG(WARN, "decide commit log barrier type failed", K(ret), K(commit_log_barrier_type),
                  KPC(this));
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (is_local_tx_() && exec_info_.upstream_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected trans type with valid upstream", K(ret), KPC(this));
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
                       log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
          TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
          return_log_cb_(log_cb);
          log_cb = NULL;
          release_ctx_ref_();
        } else if (OB_FAIL(after_submit_log_(log_block, log_cb, &helper))) {
        } else {
          redo_log_submitted = true;
          commit_log.set_prev_lsn(log_cb->get_lsn());
          // TRANS_LOG(INFO, "submit redo and commit_info log in clog adapter success", K(*log_cb));
          if (OB_SUCC(ret)) {
            if (OB_FAIL(set_start_scn_in_commit_log_(commit_log))) {
              TRANS_LOG(WARN, "set start scn in commit log failed", K(ret), K(commit_log));
            }
          }

          log_cb = NULL;

          if(OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(prepare_log_cb_(NEED_FINAL_CB, log_cb))) {
            if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
              TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
            }
          } else if (OB_FAIL(log_block.add_new_log(commit_log))) {
            TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (commit_log_barrier_type != logservice::ObReplayBarrierType::NO_NEED_BARRIER
                     && OB_FAIL(log_block.rewrite_barrier_log_block(trans_id_.get_id(), commit_log_barrier_type))) {
            TRANS_LOG(WARN, "rewrite commit log barrier failed", K(ret));
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
    } else if (commit_log_barrier_type != logservice::ObReplayBarrierType::NO_NEED_BARRIER
               && OB_FAIL(log_block.rewrite_barrier_log_block(trans_id_.get_id(), commit_log_barrier_type))) {
      TRANS_LOG(WARN, "rewrite commit log barrier failed", K(ret));
      return_log_cb_(log_cb);
      log_cb = NULL;
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
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

  if (OB_FAIL(gen_final_mds_array_(tmp_array, false))) {
    TRANS_LOG(WARN, "gen abort mds array failed", K(ret));
  }

  ObTxAbortLog abort_log(tmp_array);

  if (OB_SUCC(ret)) {
    if ((exec_info_.multi_data_source_.count() > 0 || mds_cache_.count() > 0)
        && OB_FAIL(try_alloc_retain_ctx_func_())) {
      TRANS_LOG(WARN, "alloc retain ctx func for mds trans failed", K(ret), K(mds_cache_), KPC(this));
    } else if (OB_FAIL(abort_log.init_tx_data_backup(ctx_tx_data_.get_start_log_ts()))) {
      TRANS_LOG(WARN, "init tx data backup failed", K(ret));
    } else if (exec_info_.redo_lsns_.count() > 0 || exec_info_.max_applying_log_ts_.is_valid()) {
      if (!abort_log.get_backup_start_scn().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected start scn in commit log", K(ret), K(abort_log), KPC(this));
      }
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
                 log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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

int ObPartTransCtx::submit_clear_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  ObTxClearLog clear_log(exec_info_.incremental_participants_);
  ObTxLogCb *log_cb = NULL;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxLogBlockHeader
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

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
                 share::SCN::max(ctx_tx_data_.get_end_log_ts(), max_2pc_commit_scn_), log_cb,
                 false))) {
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
    log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

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
                 log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false))) {
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

int ObPartTransCtx::submit_big_segment_log_()
{
  int ret = OB_SUCCESS;

  ObTxLogBlock log_block;

  ObTxLogCb *log_cb = nullptr;
  const int64_t replay_hint = static_cast<int64_t>(trans_id_.get_id());
  const ObTxLogType source_log_type =
      (big_segment_info_.submit_log_cb_template_->get_cb_arg_array())[0].get_log_type();

  // TODO set replay_barrier_type

  // if one part of big segment log submit into palf failed , the transaction must drive into abort
  // phase.
  while (OB_SUCC(ret) && big_segment_info_.segment_buf_.is_active()) {
    const char *submit_buf = nullptr;
    int64_t submit_buf_len = 0;
    ObTxLogBlockHeader log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);
    if (OB_FAIL(log_block.init(replay_hint, log_block_header))) {
      TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
    } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FALSE_IT(log_cb->copy(*big_segment_info_.submit_log_cb_template_))) {

    } else if (OB_FALSE_IT(ret = (log_block.acquire_segment_log_buf(
                               submit_buf, submit_buf_len, log_block_header, source_log_type,
                               &big_segment_info_.segment_buf_)))) {
    } else if (OB_EAGAIN != ret && OB_ITER_END != ret) {
      TRANS_LOG(WARN, "acquire one part of big segment log failed", KR(ret), K(*this));
      return_log_cb_(log_cb);
      log_cb = NULL;
//    } else if (OB_ITER_END == ret
//               && OB_FALSE_IT(*log_cb = *(big_segment_info_.submit_log_cb_template_))) {
    } else if (log_block.get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                   submit_buf, submit_buf_len, big_segment_info_.submit_base_scn_, log_cb,
                   false))) {
      TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
      return_log_cb_(log_cb);
      log_cb = NULL;
      release_ctx_ref_();
    } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
    } else {
      log_cb = NULL;
    }
  }

  return ret;
}

int ObPartTransCtx::prepare_big_segment_submit_(ObTxLogCb *segment_cb,
                                                const share::SCN &base_scn,
                                                logservice::ObReplayBarrierType barrier_type,
                                                const ObTxLogType &segment_log_type)
{
  int ret = OB_SUCCESS;

  if (!base_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(segment_cb), K(base_scn));
  } else if (!big_segment_info_.segment_buf_.is_active()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "inactive segment buf", K(ret), K(big_segment_info_), KPC(this));
  } else if (OB_NOT_NULL(big_segment_info_.submit_log_cb_template_)) {
  } else if (OB_ISNULL(big_segment_info_.submit_log_cb_template_ = static_cast<ObTxLogCb *>(
                           share::mtl_malloc(sizeof(ObTxLogCb), "BigSegmentCb")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc log cb template for big segment failed", K(ret), K(big_segment_info_));
  } else if (OB_FALSE_IT(new (big_segment_info_.submit_log_cb_template_) ObTxLogCb())) {
  }

  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(segment_cb)) {
      big_segment_info_.submit_log_cb_template_->get_cb_arg_array().reuse();
      if (OB_FAIL(big_segment_info_.submit_log_cb_template_->copy(*segment_cb))) {
        TRANS_LOG(WARN, "copy form log cb failed", K(ret), KPC(segment_cb));
      } else if (OB_FAIL(big_segment_info_.submit_log_cb_template_->get_cb_arg_array().push_back(
                     ObTxCbArg(segment_log_type, nullptr)))) {
        TRANS_LOG(WARN, "record segment log type in big_segment_info_", K(ret),
                  K(big_segment_info_));
      }
    }
    big_segment_info_.submit_base_scn_ = base_scn;
    big_segment_info_.submit_barrier_type_ = barrier_type;
  }

  return ret;
}

// void ObPartTransCtx::after_segment_part_submit_(ObTxLogCb * submit_log_cb,share::SCN log_scn)
// {
//
// }
int ObPartTransCtx::add_unsynced_segment_cb_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  ObTxLogCbRecord cb_record(*log_cb);
  if (!is_follower_()) {
    big_segment_info_.segment_buf_.set_prev_part_id(log_cb->get_log_ts().get_val_for_gts());
  }

  if (OB_FAIL(big_segment_info_.unsynced_segment_part_cbs_.push_back(cb_record))) {
    TRANS_LOG(WARN, "push back into unsynced_segment_part_cbs_ failed", K(ret), KPC(log_cb));
  }
  return ret;
}

int ObPartTransCtx::remove_unsynced_segment_cb_(const share::SCN &remove_scn)
{
  int ret = OB_SUCCESS;
  // big_segment_info_.unsynced_segment_part_cbs_.remove(log_cb);
  int remove_index = -1;
  for (int i = 0; i < big_segment_info_.unsynced_segment_part_cbs_.count() && OB_SUCC(ret); i++) {
    if (big_segment_info_.unsynced_segment_part_cbs_[i].self_scn_ == remove_scn) {
      remove_index = i;
    }
  }

  if (OB_SUCC(ret) && remove_index >= 0) {
    if (OB_FAIL(big_segment_info_.unsynced_segment_part_cbs_.remove(remove_index))) {
      TRANS_LOG(WARN, "remove unsynced log_cb failed", K(ret));
    }
  }

  return ret;
}

share::SCN ObPartTransCtx::get_min_unsyncd_segment_scn_()
{
  share::SCN min_scn;
  min_scn.invalid_scn();

  if (!big_segment_info_.unsynced_segment_part_cbs_.empty()) {
    const int64_t cb_cnt = big_segment_info_.unsynced_segment_part_cbs_.count();
    for (int64_t i = 0; i < cb_cnt; i++) {
      if (!min_scn.is_valid()) {
        min_scn = big_segment_info_.unsynced_segment_part_cbs_[i].self_scn_;
      } else {
        min_scn =
            share::SCN::min(min_scn, big_segment_info_.unsynced_segment_part_cbs_[i].self_scn_);
      }
      if (big_segment_info_.unsynced_segment_part_cbs_[i].first_part_scn_.is_valid()) {
        min_scn = share::SCN::min(min_scn,
                                  big_segment_info_.unsynced_segment_part_cbs_[i].first_part_scn_);
      }
    }
  }

  return min_scn;
}

ERRSIM_POINT_DEF(ERRSIM_DELAY_TX_SUBMIT_LOG);

int ObPartTransCtx::submit_log_impl_(const ObTxLogType log_type)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    if (big_segment_info_.segment_buf_.is_active()) {
      ret = OB_LOG_TOO_LARGE;
      TRANS_LOG(INFO, "can not submit any log before all big log submittted", K(ret), K(log_type),
                K(trans_id_), K(ls_id_), K(big_segment_info_));
    } else {
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
            int tmp_ret = OB_ERR_UNEXPECTED;
	    TRANS_LOG(ERROR, "not set prepare verison into mt_ctx, unexpected error", "ret", tmp_ret, K(*this));
	    print_trace_log_();
	  } else {
	    // try generate commit version
	    ret = generate_commit_version_();
	  }
	}
        if (OB_SWITCHING_TO_FOLLOWER_GRACEFULLY == ERRSIM_DELAY_TX_SUBMIT_LOG && 1002 == MTL_ID() && 1001 == ls_id_.id()) {
          ret = ERRSIM_DELAY_TX_SUBMIT_LOG;
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
    if (ObTxLogType::TX_PREPARE_LOG == log_type || ObTxLogType::TX_COMMIT_LOG == log_type
        || ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
      // need submit log in log sync callback
      // rewrite ret
      ret = OB_SUCCESS;
      TRANS_LOG(DEBUG, "ignore OB_TX_NOLOGCB", K(log_type), K(*this));
    }
  } else if (OB_LOG_TOO_LARGE == ret) {
    if (OB_FAIL(submit_big_segment_log_())) {
      TRANS_LOG(WARN, "submit big segment log failed", K(ret), K(log_type), KPC(this));
    }
  } else if (OB_ERR_TOO_BIG_ROWSIZE == ret) {
    int tmp_ret = OB_SUCCESS;
    if (ObPartTransAction::COMMIT == part_trans_action_
        || get_upstream_state() >= ObTxState::REDO_COMPLETE) {
      if (OB_TMP_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
        TRANS_LOG(WARN, "abort tx failed", KR(ret), KPC(this));
      } else {
        TRANS_LOG(WARN, "do abort tx end for committing txn", K(ret),
                  K(log_type), KPC(this));
      }
    } else {
      if (OB_TMP_FAIL(do_local_tx_end_(TxEndAction::DELAY_ABORT_TX))) {
        TRANS_LOG(WARN, "do local tx end failed", K(tmp_ret), K(log_type), KPC(this));
      } else {
        TRANS_LOG(WARN, "row size is too big for only one redo", K(ret),
                  K(log_type), KPC(this));
      }
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
  int tmp_ret = OB_E(EventTable::EN_LOG_IDS_COUNT_ERROR) OB_SUCCESS;
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
        is_contain(cb_arg_array, ObTxLogType::TX_BIG_SEGMENT_LOG) ||
        is_contain(cb_arg_array, ObTxLogType::TX_MULTI_DATA_SOURCE_LOG)) {
      if (!is_contain(cb_arg_array, ObTxLogType::TX_COMMIT_INFO_LOG)) {
        ret = exec_info_.redo_lsns_.push_back(log_cb->get_lsn());
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(update_rec_log_ts_(false/*for_replay*/, SCN()))) {
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
  if(OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_BIG_SEGMENT_LOG))
  {
    add_unsynced_segment_cb_(log_cb);
    if (big_segment_info_.segment_buf_.is_completed()) {
      TRANS_LOG(INFO, "reuse big_segment_info_",K(ret),K(big_segment_info_),KPC(log_cb));
      big_segment_info_.reuse();
    }
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
    exec_info_.prepare_version_ = SCN::max(log_cb->get_log_ts(), exec_info_.prepare_version_);
  }
  if (OB_SUCC(ret) && is_contain(cb_arg_array, ObTxLogType::TX_COMMIT_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
    // elr
    const bool has_row_updated = mt_ctx_.has_row_updated();
    if (can_elr_ && has_row_updated) {
      if (OB_FAIL(ctx_tx_data_.set_state(ObTxData::ELR_COMMIT))) {
        TRANS_LOG(WARN, "set tx data state", K(ret));
      }
      elr_handler_.check_and_early_lock_release(has_row_updated, this);
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
    if (!ctx_tx_data_.get_start_log_ts().is_valid()) {
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
                      OB_ID(base_ts),
                      log_cb->get_base_ts(),
                      OB_ID(t),
                      log_cb->get_log_ts(),
                      OB_ID(lsn),
                      log_cb->get_lsn());

  exec_info_.next_log_entry_no_++;
  ObTxLogBlockHeader
    block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);
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
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; i++) {
      if (!free_cbs_.is_empty()) {
        if (OB_ISNULL(log_cb = free_cbs_.remove_first())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected null log cb", KR(ret), K(*this));
        } else {
          break;
        }
      } else {
        if (OB_FAIL(extend_log_cbs_())) {
          TRANS_LOG(WARN, "extend log callback failed", K(ret));
          // rewrite ret
          ret = OB_TX_NOLOGCB;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(log_cb)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected log callback", K(ret));
    } else {
      log_cb->reuse();
      busy_cbs_.add_last(log_cb);
    }
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

int ObPartTransCtx::get_max_submitting_log_info_(palf::LSN &lsn, SCN &log_ts)
{
  int ret = OB_SUCCESS;
  ObTxLogCb *log_cb = NULL;
  lsn = LSN(palf::PALF_INITIAL_LSN_VAL);
  log_ts.reset();
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
  SCN tmp_log_ts;
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

int ObPartTransCtx::set_start_scn_in_commit_log_(ObTxCommitLog &commit_log)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(commit_log.init_tx_data_backup(ctx_tx_data_.get_start_log_ts()))) {
    TRANS_LOG(WARN, "init tx data backup failed", K(ret));
  } else if (exec_info_.next_log_entry_no_ > 0) {
    if (!commit_log.get_backup_start_scn().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected start scn in commit log", K(ret), K(commit_log), KPC(this));
    }
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
  CtxLockGuard guard(lock_);
  return try_submit_next_log_(true);
}

bool ObPartTransCtx::is_2pc_logging() const { return is_2pc_logging_(); }

// uint64_t ObPartTransCtx::get_participant_id()
// {
//   int ret = OB_SUCCESS;
//   uint64_t participant_id = UINT64_MAX;
//
//   if (OB_FAIL(find_participant_id_(ls_id_, participant_id))) {
//     TRANS_LOG(ERROR, "find participant id failed", K(*this));
//   }
//
//   return participant_id;
// }

int ObPartTransCtx::find_participant_id_(const ObLSID &participant, int64_t &participant_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  participant_id = INT64_MAX;

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

//***************************** for 4.0
int ObPartTransCtx::check_replay_avaliable_(const palf::LSN &offset,
                                            const SCN &timestamp,
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
  } else if (OB_UNLIKELY(!timestamp.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(timestamp), K(offset), K(trans_id_));
    ret = OB_INVALID_ARGUMENT;
  // } else if (is_exiting_) {
  //   // ret = OB_TRANS_CTX_NOT_EXIST;
  } else {
    need_replay = true;
    // check state like ObPartTransCtx::is_trans_valid_for_replay_
    if (!exec_info_.max_applying_log_ts_.is_valid()) {
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

  if (need_replay && !start_replay_ts_.is_valid()) {
    start_replay_ts_ = timestamp;
  }

  if (need_replay) {
    update_rec_log_ts_(true/*for_replay*/, timestamp);
  }

  return ret;
}

int ObPartTransCtx::push_repalying_log_ts(const SCN log_ts_ns)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

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

int ObPartTransCtx::push_replayed_log_ts(SCN log_ts_ns, const palf::LSN &offset)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

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

  if (OB_SUCC(ret)) {
    if (big_segment_info_.segment_buf_.is_completed()
        && big_segment_info_.unsynced_segment_part_cbs_.count() > 0) {
      // if (big_segment_info_.submit_log_cb_template_
      //     == big_segment_info_.unsynced_segment_part_cbs_.get_first()) {
        remove_unsynced_segment_cb_(big_segment_info_.unsynced_segment_part_cbs_[0].self_scn_);
        big_segment_info_.reuse();
      // } else {
      //   ret = OB_ERR_UNEXPECTED;
      //   TRANS_LOG(ERROR, "unexpectd unsynced_segment_part_cbs_", K(ret), K(log_ts_ns),
      //             K(big_segment_info_), KPC(this));
      // }
    }
  }
  return ret;
}

int ObPartTransCtx::iter_next_log_for_replay(ObTxLogBlock &log_block,
                                             ObTxLogHeader &log_header,
                                             const share::SCN log_scn)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (OB_FAIL(log_block.get_next_log(log_header, &big_segment_info_.segment_buf_))) {
    if (OB_START_LOG_CURSOR_INVALID == ret) {
      TRANS_LOG(WARN, "start replay from the mid of big segment", K(ret), K(log_scn), K(log_header),
                K(big_segment_info_), KPC(this));
      ret = OB_SUCCESS;
    } else if (OB_LOG_TOO_LARGE == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(big_segment_info_.submit_log_cb_template_)) {
        if (OB_ISNULL(big_segment_info_.submit_log_cb_template_ = static_cast<ObTxLogCb *>(
                          share::mtl_malloc(sizeof(ObTxLogCb), "BigSegmentCb")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "alloc log cb template for big segment failed", K(ret), K(log_scn),
                    K(big_segment_info_));
        } else {
          new (big_segment_info_.submit_log_cb_template_) ObTxLogCb();
        }
      }

      if (OB_SUCC(ret)) {
        big_segment_info_.submit_log_cb_template_->set_log_ts(log_scn);
        if (!big_segment_info_.submit_log_cb_template_->get_first_part_scn().is_valid()) {
          big_segment_info_.submit_log_cb_template_->set_first_part_scn(log_scn);
          add_unsynced_segment_cb_(big_segment_info_.submit_log_cb_template_);
        }
      }

    } else if (OB_NO_NEED_UPDATE == ret) {
      TRANS_LOG(INFO, "collect all part of big segment", K(ret), K(log_scn), K(log_header),
                K(big_segment_info_), KPC(this));
      ret = OB_SUCCESS;
    } else if (OB_ITER_END == ret) {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtx::replay_one_part_of_big_segment(const palf::LSN &offset,
                                                   const share::SCN &timestamp,
                                                   const int64_t &part_log_no)
{
  CtxLockGuard guard(lock_);

  int ret = OB_SUCCESS;
  bool need_replay = true;
  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  }

  if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(offset))) {
    TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
  }
  return ret;
}

int ObPartTransCtx::update_replaying_log_no_(const SCN &log_ts_ns, int64_t part_log_no)
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
                                                 const SCN &commit_log_ts)
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
                                           const SCN &log_ts,
                                           const SCN &commit_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
    TRANS_LOG(WARN, "set end log ts failed", K(ret));
  } else if (commit) {
    if (TransType::SP_TRANS == exec_info_.trans_type_
        && !commit_version.is_valid()) {
      if (OB_FAIL(ctx_tx_data_.set_commit_version(log_ts))) {
        TRANS_LOG(WARN, "set commit_version failed", K(ret));
      }
    } else if (TransType::DIST_TRANS == exec_info_.trans_type_
               && commit_version.is_valid()) {
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

int ObPartTransCtx::replace_tx_data_with_backup_(const ObTxDataBackup &backup, SCN log_ts)
{
  int ret = OB_SUCCESS;
  share::SCN tmp_log_ts = log_ts;
  if (backup.get_start_log_ts().is_valid()) {
    tmp_log_ts = backup.get_start_log_ts();
  } else if (exec_info_.next_log_entry_no_ > 1) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid start log ts with a applied log_entry", K(ret), K(backup), K(log_ts),
              KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_tx_data_.set_start_log_ts(tmp_log_ts))) {
      TRANS_LOG(WARN, "update tx data with backup failed.", KR(ret), K(ctx_tx_data_));
    }
  }

  TRANS_LOG(DEBUG, "replace tx_data", K(ret), K(backup), K(ctx_tx_data_), K(*this));
  return ret;
}

void ObPartTransCtx::force_no_need_replay_checksum()
{
  exec_info_.need_checksum_ = false;
}

void ObPartTransCtx::check_no_need_replay_checksum(const SCN &log_ts)
{
  // TODO(handora.qc): How to lock the tx_ctx

  // checksum_scn_ means all data's checksum has been calculated before the
  // log of checksum_scn_(not included). So if the data with this scn is
  // not replayed with checksum_scn_ <= scn, it means may exist some data
  // will never be replayed because the memtable will filter the data.
  if (exec_info_.checksum_scn_ <= log_ts) {
    exec_info_.need_checksum_ = false;
  }
}

int ObPartTransCtx::validate_replay_log_entry_no(bool first_created_ctx,
                                                 int64_t log_entry_no,
                                                 const SCN &log_ts)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

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
                                       const SCN &timestamp,
                                       const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_redo_in_ctx", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::INVALID;
  bool need_replay = true;

  CtxLockGuard guard(lock_);

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
                                          OB_Y(need_replay), OB_ID(offset), offset.val_,
                                          OB_ID(t), timestamp, OB_ID(ref), get_ref());
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
                                       const SCN &timestamp,
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
                      OB_Y(need_replay),
                      OB_ID(offset),
                      offset.val_,
                      OB_ID(t),
                      timestamp,
                      OB_ID(ref),
                      get_ref());
  return ret;
}

int ObPartTransCtx::replay_active_info(const ObTxActiveInfoLog &log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
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
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(set_scheduler_(log.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_app_trace_id_(log.get_app_trace_id()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(log), K(*this));
  } else {
    exec_info_.trans_type_ = log.get_trans_type();
    if (log.is_dup_tx()) {
      set_dup_table_tx_();
    }
    trans_expired_time_ = log.get_tx_expired_time();
    session_id_ = log.get_session_id();
    // schema_version
    can_elr_ = log.is_elr();
    // cur_query_start_time
    exec_info_.is_sub2pc_ = log.is_sub2pc();
    exec_info_.xid_ = log.get_xid();
    epoch_ = log.get_epoch();
    last_op_sn_ = log.get_last_op_sn();
    first_scn_ = log.get_first_seq_no();
    last_scn_ = log.get_last_seq_no();
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
                             OB_ID(ref), get_ref());
  return ret;
}

int ObPartTransCtx::replay_commit_info(const ObTxCommitInfoLog &commit_info_log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
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
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
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
    if (OB_SUCC(ret) && commit_info_log.is_dup_tx()) {
      set_dup_table_tx_();
    }
    // NOTE that set xa variables before set trans type
    set_2pc_upstream_(commit_info_log.get_upstream());
    exec_info_.xid_ = commit_info_log.get_xid();
    exec_info_.is_sub2pc_ = commit_info_log.is_sub2pc();
    if (exec_info_.participants_.count() > 1) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else if (exec_info_.upstream_.is_valid()) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else if (is_sub2pc()) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
    } else {
      exec_info_.trans_type_ = TransType::SP_TRANS;
    }

    if (!is_local_tx_() && !commit_info_log.get_upstream().is_valid()) {
      set_2pc_upstream_(ls_id_);
      TRANS_LOG(INFO, "set upstream to self", K(*this), K(commit_info_log));
    }
    can_elr_ = commit_info_log.is_elr();
    cluster_version_ = commit_info_log.get_cluster_version();
    sub_state_.set_info_log_submitted();
    reset_redo_lsns_();
    ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_local_tx_()) {
      set_durable_state_(ObTxState::REDO_COMPLETE);
      set_upstream_state(ObTxState::REDO_COMPLETE);
    } else if (is_incomplete_replay_ctx_) {
      set_durable_state_(ObTxState::REDO_COMPLETE);
      set_upstream_state(ObTxState::REDO_COMPLETE);
      // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
    } else if (OB_FAIL(switch_log_type_(commit_info_log.LOG_TYPE, two_phase_log_type))) {
      TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
    } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
      TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
    }

    if (OB_SUCC(ret) && commit_info_log.is_dup_tx()) {
      // 1. the tx ctx must be committed or aborted.
      // 2. a new lease log ts must be larger than commit log ts.
      //=> no need set before_preapre version for incomplete replay
      if (OB_FAIL(dup_table_before_preapre_(timestamp))) {
        TRANS_LOG(WARN, "set commit_info scn as before_prepare_version failed", K(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      if (exec_info_.is_dup_tx_) {
        if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->add_commiting_dup_trx(trans_id_))) {
          TRANS_LOG(WARN, "add committing dup table trx failed", K(ret), K(timestamp),
                    KPC(this));
        }
      }
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
      OB_ID(ref), get_ref());
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
                                   const SCN &timestamp,
                                   const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_prepare", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;

  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(prepare_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
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
      set_durable_state_(ObTxState::PREPARE);
      set_upstream_state(ObTxState::PREPARE);
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
                       OB_ID(t), timestamp, OB_ID(ref), get_ref());
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
                                  const SCN &timestamp,
                                  const int64_t &part_log_no,
                                  const SCN &replay_compact_version)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_commit", 10 * 1000);

  // const int64_t start = ObTimeUtility::fast_current_time();
  const SCN commit_version = commit_log.get_commit_version();
  bool need_replay = true;

  CtxLockGuard guard(lock_);

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
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else if (OB_FAIL(exec_info_.incremental_participants_.assign(
                 commit_log.get_incremental_participants()))) {
    TRANS_LOG(WARN, "set incremental_participants error", K(ret), K(*this));
  } else {
    if (OB_SUCC(ret)) {
      if ((!commit_log.get_multi_source_data().empty() || !exec_info_.multi_data_source_.empty())
          && is_incomplete_replay_ctx_) {
        // ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "mds part_ctx can not replay from the middle", K(ret), K(timestamp), K(offset),
                  K(commit_log), KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN) {
        if (OB_FAIL(try_alloc_retain_ctx_func_())) {
          TRANS_LOG(WARN, "alloc retain ctx func failed", K(ret), K(commit_log), K(timestamp),
                    K(offset), KPC(this));
        } else if (OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, timestamp,
                                                       offset, true))) {
          TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(this));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_local_tx_()) {
        set_durable_state_(ObTxState::COMMIT);
        set_upstream_state(ObTxState::COMMIT);
      } else {
        ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
        if (is_incomplete_replay_ctx_) {
          set_durable_state_(ObTxState::COMMIT);
          set_upstream_state(ObTxState::COMMIT);
          // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on
          // 2PC
        } else if (OB_FAIL(switch_log_type_(commit_log.LOG_TYPE, two_phase_log_type))) {
          TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
        } else if (OB_FAIL(coord_prepare_info_arr_.assign(commit_log.get_ls_log_info_arr()))) {
          TRANS_LOG(WARN, "assign coord_prepare_info_arr_ failed", K(ret));
        } else if (is_root()
                   && OB_FAIL(
                       exec_info_.prepare_log_info_arr_.assign(commit_log.get_ls_log_info_arr()))) {
          TRANS_LOG(WARN, "assigin prepare_log_info_arr_ failed", K(ret));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
          TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t checksum =
        (exec_info_.need_checksum_ && !is_incomplete_replay_ctx_ ? commit_log.get_checksum() : 0);
    mt_ctx_.set_replay_compact_version(replay_compact_version);

    if (OB_FAIL(notify_table_lock_(timestamp,
                                   true,
                                   exec_info_.multi_data_source_,
                                   false /* not a force kill */))) {
      TRANS_LOG(WARN, "notify table lock failed", KR(ret), "context", *this);
    } else if (OB_FAIL(trans_replay_commit_(ctx_tx_data_.get_commit_version(),
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
                       offset.val_, OB_ID(t), timestamp, OB_ID(ref), get_ref());
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
                                 const SCN &timestamp,
                                 const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_clear", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;
  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(clear_log), K(timestamp), K(offset), K(*this));
    // no need to replay
    if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
    }
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
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
        set_durable_state_(ObTxState::CLEAR);
        set_upstream_state(ObTxState::CLEAR);
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
                       OB_ID(t), timestamp, OB_ID(ref), get_ref());
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
                                 const SCN &timestamp,
                                 const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;

  common::ObTimeGuard timeguard("replay_abort", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = false;

  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (OB_FAIL(replay_update_tx_data_(false, timestamp, SCN() /*unused*/))) {
    TRANS_LOG(WARN, "replay update tx data failed", KR(ret), K(*this));
  } else if (OB_FAIL(replace_tx_data_with_backup_(abort_log.get_tx_data_backup(), timestamp))) {
    TRANS_LOG(WARN, "replace tx data with backup failed", KR(ret), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(abort_log), K(timestamp), K(offset), K(*this));
    // no need to replay,
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  } else {
    if (OB_SUCC(ret)) {
      if ((!abort_log.get_multi_source_data().empty() || !exec_info_.multi_data_source_.empty())
          && is_incomplete_replay_ctx_) {
        // ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "mds part_ctx can not replay from the middle", K(ret), K(timestamp), K(offset),
                  K(abort_log), KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      if (exec_info_.multi_data_source_.count() > 0 && get_retain_cause() == RetainCause::UNKOWN) {
        if (OB_FAIL(try_alloc_retain_ctx_func_())) {
          TRANS_LOG(WARN, "alloc retain ctx func failed", K(ret), K(abort_log), K(timestamp),
                    K(offset), KPC(this));
        } else if (OB_FAIL(insert_into_retain_ctx_mgr_(RetainCause::MDS_WAIT_GC_COMMIT_LOG, timestamp,
                                                       offset, true))) {
          TRANS_LOG(WARN, "insert into retain_ctx_mgr failed", K(ret), KPC(this));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_local_tx_()) {
        set_durable_state_(ObTxState::ABORT);
        set_upstream_state(ObTxState::ABORT);
      } else {
        ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
        if (is_incomplete_replay_ctx_) {
          set_durable_state_(ObTxState::ABORT);
          set_upstream_state(ObTxState::ABORT);
          // incomplete replay ctx will exiting by replay commit/abort/clear,  no need to depend on 2PC
        } else if (OB_FAIL(switch_log_type_(abort_log.LOG_TYPE, two_phase_log_type))) {
          TRANS_LOG(WARN, "switch log type failed", KR(ret), KPC(this));
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::replay_log(two_phase_log_type))) {
          TRANS_LOG(WARN, "replay_log failed", KR(ret), KPC(this));
        }
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
                       OB_ID(t), timestamp, OB_ID(ref), get_ref());
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
                                             const SCN &timestamp,
                                             const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_multi_data_source", 10 * 1000);
  bool need_replay = false;
  CtxLockGuard guard(lock_);

  const int64_t start = ObTimeUtility::current_time();

  bool repeat_replay = (timestamp == exec_info_.max_applied_log_ts_);

  ObTxBufferNodeArray increamental_array;
  int64_t additional_index = exec_info_.multi_data_source_.count();
  if (OB_FAIL(check_replay_avaliable_(lsn, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(lsn), K(timestamp), K(*this));
  } else if (!need_replay || repeat_replay) {
    TRANS_LOG(INFO, "need not replay log", K(need_replay), K(repeat_replay), K(log), K(timestamp), K(lsn), K(*this));
    // no need to replay
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
  // TODO: We need filter the replay of mds array after recovered from the tx_ctx_table
  //} else if (OB_FAIL(deep_copy_mds_array(log.get_data(), false))) {
  //TRANS_LOG(WARN, "deep copy mds array failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (is_incomplete_replay_ctx_) {
      // ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mds part_ctx can not replay from the middle", K(ret), K(timestamp), K(lsn),
                K(log), KPC(this));
    }
  }

  share::SCN notify_redo_scn =
      OB_NOT_NULL(big_segment_info_.submit_log_cb_template_)
              && big_segment_info_.submit_log_cb_template_->get_first_part_scn().is_valid()
          ? big_segment_info_.submit_log_cb_template_->get_first_part_scn()
          : timestamp;

  if (OB_FAIL(ret)) {
    // do nothing
  //TODO & ATTENTION: deep copy a part of the mds array in the log twice after recovered from the tx_ctx_table
  } else if (OB_FAIL(deep_copy_mds_array_(log.get_data(), increamental_array))) {
    TRANS_LOG(WARN, "deep copy mds array failed", K(ret));
  } else if (OB_FAIL(notify_data_source_(NotifyType::REGISTER_SUCC,
                                         timestamp,
                                         true,
                                         increamental_array))) {
    TRANS_LOG(WARN, "notify data source for REGISTER_SUCC failed", K(ret));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_REDO,
                                         timestamp,
                                         true,
                                         increamental_array))) {
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
        node.get_buffer_ctx_node().destroy_ctx();
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
                       OB_ID(ref), get_ref());

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
                                  const SCN &timestamp,
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
  } else if (OB_FAIL(update_replaying_log_no_(timestamp, part_log_no))) {
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

const SCN ObPartTransCtx::get_min_undecided_log_ts() const
{
  SCN log_ts;
  CtxLockGuard guard(lock_);
  if (!busy_cbs_.is_empty()) {
    const ObTxLogCb *log_cb = busy_cbs_.get_first();
    if (OB_ISNULL(log_cb)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected null ptr", K(*this));
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

int ObPartTransCtx::switch_to_leader(const SCN &start_working_ts)
{
  int ret = OB_SUCCESS;

  share::SCN append_mode_initial_scn;
  append_mode_initial_scn.set_invalid();

  common::ObTimeGuard timeguard("switch to leader", 10 * 1000);
  CtxLockGuard guard(lock_);
  TxCtxStateHelper state_helper(role_state_);
  timeguard.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(DEBUG, "transaction is exiting", "context", *this);
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->get_append_mode_initial_scn(
                 append_mode_initial_scn))) {
    /* We can not ensure whether there are some redo logs after the append_mode_initial_scn.
     * All running trans must be be killed by the append_mode_initial_scn.*/
    TRANS_LOG(WARN, "get append mode initial scn from the palf failed", K(ret), KPC(this));
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::TAKEOVER))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    const bool contain_mds_table_lock = is_contain_mds_type_(ObTxDataSourceType::TABLE_LOCK);
    const bool contain_mds_transfer_out =
        is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT);
    bool kill_by_append_mode_initial_scn = false;
    if (append_mode_initial_scn.is_valid()) {
      kill_by_append_mode_initial_scn = exec_info_.max_applying_log_ts_ <= append_mode_initial_scn;
    }

    if (ObTxState::INIT == exec_info_.state_) {
      if (exec_info_.data_complete_ && !contain_mds_table_lock && !contain_mds_transfer_out
          && !kill_by_append_mode_initial_scn) {
        if (OB_FAIL(mt_ctx_.replay_to_commit(false /*is_resume*/))) {
          TRANS_LOG(WARN, "replay to commit failed", KR(ret), K(*this));
        }
      } else {
        TRANS_LOG(WARN, "txn data incomplete, will be aborted", K(contain_mds_table_lock),
                  K(contain_mds_transfer_out), K(kill_by_append_mode_initial_scn),
                  K(append_mode_initial_scn), KPC(this));
        if (has_persisted_log_()) {
          if (ObPartTransAction::COMMIT == part_trans_action_
              || get_upstream_state() >= ObTxState::REDO_COMPLETE) {

            TRANS_LOG(WARN, "abort self instantly with a tx_commit request",
                      K(contain_mds_table_lock), K(contain_mds_transfer_out),
                      K(kill_by_append_mode_initial_scn), K(append_mode_initial_scn), KPC(this));
            if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
              TRANS_LOG(WARN, "abort tx failed", KR(ret), KPC(this));
            }
          } else {
            if (OB_FAIL(do_local_tx_end_(TxEndAction::DELAY_ABORT_TX))) {
              TRANS_LOG(WARN, "abort tx failed", KR(ret), K(*this));
            }
            notify_scheduler_tx_killed_(ObTxAbortCause::PARTICIPANT_SWITCH_LEADER_DATA_INCOMPLETE);
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
  if (OB_SUCC(ret)) {
    (void)try_submit_next_log_();
    big_segment_info_.reuse();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "switch to leader failed", KR(ret), K(ret), KPC(this));
  } else {
    last_request_ts_ = ObClockGenerator::getClock();
#ifndef NDEBUG
    TRANS_LOG(INFO, "switch to leader succeed", KPC(this));
#endif
  }
  REC_TRANS_TRACE_EXT2(tlog_, switch_to_leader, OB_ID(ret), ret, OB_ID(used), timeguard.get_diff(),
                       OB_ID(ref), get_ref());
  return ret;
}

inline bool ObPartTransCtx::need_callback_scheduler_() {
  return (is_local_tx_() || is_root())
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
  CtxLockGuard guard(lock_);
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
        cb.init(trans_service_, trans_id_, OB_TRANS_KILLED, SCN());
        if (OB_FAIL(cb_array.push_back(cb))) {
          TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
        }
      }
      if (OB_FAIL(ret) && need_cb_scheduler) {
        // recover commit_cb_ when failed
        commit_cb_.enable();
      }
      if (!need_cb_scheduler) {
        notify_scheduler_tx_killed_(ObTxAbortCause::PARTICIPANT_SWITCH_FOLLOWER_FORCEDLY);
      }
      TRANS_LOG(INFO, "switch to follower forcely, txn aborted without persisted log", KPC(this));
    } else {
      // has persisted log, wait new leader to advance it
      if (pending_write_ && OB_FALSE_IT(mt_ctx_.wait_pending_write())) {
        // do nothing
      } else if (OB_FALSE_IT(mt_ctx_.commit_to_replay())) {
        // do nothing
      } else if (OB_FALSE_IT(mt_ctx_.merge_multi_callback_lists_for_changing_leader())) {
        // do nothing
      } else if (!mt_ctx_.is_all_redo_submitted() && OB_FAIL(mt_ctx_.clean_unlog_callbacks())) {
        TRANS_LOG(WARN, "clear unlog callbacks", KR(ret), K(*this));
      }

      if (OB_SUCC(ret) && exec_info_.is_dup_tx_ && get_downstream_state() == ObTxState::REDO_COMPLETE
          && !sub_state_.is_state_log_submitted()) {
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(dup_table_before_preapre_(exec_info_.max_applied_log_ts_, true/*before_replay*/))) {
          TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(this));
        } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
          TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
        }
      }

      // special handle commit triggered by local call: coordinator colocate with scheduler
      // let scheduler retry commit with RPC if required
      if (need_callback_scheduler_()) {
        ObTxCommitCallback cb;
        // no CommitInfoLog has been submitted, txn must abort
        if (exec_info_.state_ == ObTxState::INIT && !sub_state_.is_info_log_submitted()) {
          cb.init(trans_service_, trans_id_, OB_TRANS_KILLED, SCN());
        } else {
          // otherwise, txn either continue commit or abort, need retry to get final result
          cb.init(trans_service_, trans_id_, OB_NOT_MASTER, SCN());
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
    big_segment_info_.reuse();
    TRANS_LOG(INFO, "switch to follower forcedly success", KPC(this));
  }
  REC_TRANS_TRACE_EXT2(tlog_, switch_to_follower_forcely,
                              OB_ID(ret), ret,
                              OB_ID(used), timeguard.get_diff(),
                              OB_ID(ref), get_ref());
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
  if (MTL_ID() != tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tenant not match", K(ret), K(*this));
  } else if (is_exiting_) {
    // do nothing
  } else if (is_force_abort_logging_()) {
    // is aborting, skip
  } else if (is_follower_()) {
    TRANS_LOG(INFO, "current tx already follower", K(*this));
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::SWITCH_GRACEFUL))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    if (pending_write_) {
      TRANS_LOG(INFO, "current tx is executing stmt", K(*this));
      mt_ctx_.wait_pending_write();
      timeguard.click();
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
        cb.init(trans_service_, trans_id_, OB_SWITCHING_TO_FOLLOWER_GRACEFULLY, SCN());
        TRANS_LOG(INFO, "swtich to follower gracefully, notify scheduler retry", KPC(this));
        if (OB_FAIL(cb_array.push_back(cb))) {
          TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
        }
      }
    }
    timeguard.click();
    if (OB_SUCC(ret) && need_submit_log && !need_force_abort_()) {
      // We need merge all callbacklists before submitting active info
      (void)mt_ctx_.merge_multi_callback_lists_for_changing_leader();
      if (ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
        if (OB_FAIL(submit_redo_commit_info_log_())) {
          // currently, if there is no log callback, switch leader would fail,
          // and resume leader would be called.
          TRANS_LOG(WARN, "submit commit info log failed", KR(ret), K(*this));
        }
      } else if (ObTxLogType::TX_ACTIVE_INFO_LOG == log_type) {
        if (OB_FAIL(submit_redo_active_info_log_())) {
          // currently, if there is no log callback, switch leader would fail,
          // and resume leader would be called.
          TRANS_LOG(WARN, "submit active info log failed", KR(ret), K(*this));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected log type", K(ret));
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
    if (exec_info_.is_dup_tx_ && get_downstream_state() == ObTxState::REDO_COMPLETE
        && !sub_state_.is_state_log_submitted()) {
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(dup_table_before_preapre_(exec_info_.max_applied_log_ts_, true/*before_replay*/))) {
        TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(this));
      } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
        TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
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
                              OB_ID(ref), get_ref());
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
int ObPartTransCtx::resume_leader(const SCN &start_working_ts)
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
  if (OB_SUCC(ret)) {
    (void)try_submit_next_log_();
  }
  REC_TRANS_TRACE_EXT2(tlog_, resume_leader, OB_ID(ret), ret,
                                             OB_ID(used), timeguard.get_diff(),
                                             OB_ID(ref), get_ref());
  TRANS_LOG(INFO, "resume leader", KR(ret), K(ret), KPC(this));
  return ret;
}
//*****************************

int ObPartTransCtx::check_with_tx_data(ObITxDataCheckFunctor &fn)
{
  // NB: You need notice the lock is not acquired during check
  int ret = OB_SUCCESS;
  ObTxData *tx_data_ptr = NULL;
  if (OB_FAIL(ctx_tx_data_.get_tx_data_ptr(tx_data_ptr))) {
  } else {
    // const ObTxData &tx_data = *tx_data_ptr;
    // NB: we must read the state then the version without lock. If you are interested in the
    // order, then you can read the comment in ob_tx_data_functor.cpp
    ObTxState state = exec_info_.state_;
    ObTxCCCtx tx_cc_ctx(state, mt_ctx_.get_trans_version());

    if (OB_FAIL(fn(*tx_data_ptr, &tx_cc_ctx))) {
      TRANS_LOG(WARN, "do data check function fail.", KR(ret), K(*this));
    } else {
      TRANS_LOG(DEBUG, "check with tx data", K(*this), K(fn));
    }
  }

  return ret;
}

int ObPartTransCtx::update_rec_log_ts_(bool for_replay, const SCN &rec_log_ts)
{
  int ret = OB_SUCCESS;

  share::SCN min_big_segment_rec_scn = get_min_unsyncd_segment_scn_();

  // The semantic of the rec_log_ts means the log ts of the first state change
  // after previous checkpoint. So it is only updated if it is unset.
  if (!rec_log_ts_.is_valid()) {
    if (for_replay) {
      if (!rec_log_ts.is_valid()) {
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


  if (min_big_segment_rec_scn.is_valid() && !rec_log_ts_.is_valid()) {
    rec_log_ts_ = min_big_segment_rec_scn;
  } else if (min_big_segment_rec_scn.is_valid() && rec_log_ts_.is_valid()) {
    rec_log_ts_ = share::SCN::min(min_big_segment_rec_scn, rec_log_ts_);
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

  if (!prev_rec_log_ts_.is_valid()) {
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
      } else if (busy_cbs_.is_empty()) {
        rec_log_ts_.reset();
      } else {
        // Case 1.1: As follower, there may also exist log which is proposed
        // while not committed because of the current leader's switch mechinism
        // (leader in txn layer will be switched from follower even before all
        // proposed log has been synced). So we need refer to the log ts in the
        // current log_cb.
        const ObTxLogCb *log_cb = busy_cbs_.get_first();
        if (OB_ISNULL(log_cb)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected null ptr", K(*this));
        } else {
          rec_log_ts_ = log_cb->get_log_ts();
        }
      }
    } else {
      // Case 2: As leader, the application is discrete and not in order, so we
      // rely on the first lo(we call it FCL later)g hasnot been applied as
      // rec_log_ts if exists or reset it if not because all log of the txn with
      // its log ts in front of the FCL must be contained in the checkpoint.
      if (busy_cbs_.is_empty()) {
        rec_log_ts_.reset();
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

  if (OB_FAIL(ctx_tx_data_.get_tx_data(info.tx_data_guard_))) {
    TRANS_LOG(WARN, "get tx data failed", K(ret));
  } else if (OB_FAIL(mt_ctx_.calc_checksum_before_scn(
                 exec_info_.max_applied_log_ts_, exec_info_.checksum_, exec_info_.checksum_scn_))) {
    TRANS_LOG(ERROR, "calc checksum before log ts failed", K(ret), KPC(this));
  } else if (OB_FAIL(exec_info_.generate_mds_buffer_ctx_array())) {
    TRANS_LOG(WARN, "fail to generate mds buffer ctx array", K(ret), KPC(this));
  } else if (OB_FAIL(info.exec_info_.assign(exec_info_))) {
    TRANS_LOG(WARN, "fail to assign exec_info", K(ret), KPC(this));
  } else {
    info.tx_id_ = trans_id_;
    info.ls_id_ = ls_id_;
    info.cluster_id_ = cluster_id_;
    if (OB_FAIL(mt_ctx_.get_table_lock_store_info(info.table_lock_info_))) {
      TRANS_LOG(WARN, "get_table_lock_store_info failed", K(ret), K(info));
    } else {
      TRANS_LOG(INFO, "store ctx_info: ", K(ret), K(info), KPC(this));
    }
  }
  exec_info_.mds_buffer_ctx_array_.reset();

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
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid scheduler addr", K(trans_id_));
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

int ObPartTransCtx::deep_copy_mds_array_(const ObTxBufferNodeArray &mds_array,
                                         ObTxBufferNodeArray &incremental_array,
                                         bool need_replace)
{
  auto process_with_buffer_ctx = [this](const ObTxBufferNode &old_node,
                                        mds::BufferCtx *&new_ctx) -> int {
    int ret = OB_SUCCESS;
    if (old_node.get_data_source_type() <= ObTxDataSourceType::UNKNOWN
        || old_node.get_data_source_type() >= ObTxDataSourceType::MAX_TYPE) {
      ret = OB_ERR_UNDEFINED;
      TRANS_LOG(ERROR, "unexpected mds type", KR(ret), K(*this));
    } else if (old_node.get_data_source_type() <= ObTxDataSourceType::BEFORE_VERSION_4_1
               && ObTxDataSourceType::CREATE_TABLET_NEW_MDS != old_node.get_data_source_type()
               && ObTxDataSourceType::DELETE_TABLET_NEW_MDS != old_node.get_data_source_type()
               && ObTxDataSourceType::UNBIND_TABLET_NEW_MDS != old_node.get_data_source_type()) {
      TRANS_LOG(INFO, "old mds type, no need process with buffer ctx",
                K(old_node.get_data_source_type()), K(*this));
    } else {
      if (OB_ISNULL(old_node.get_buffer_ctx_node().get_ctx())) { // this is replay path, create ctx
        if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(old_node.get_data_source_type(), trans_id_,
                                                       new_ctx))) {
          TRANS_LOG(WARN, "fail to create buffer ctx", KR(ret), KPC(new_ctx), K(*this),
                    K(old_node));
        }
      } else { // this is recover path, copy ctx
        if (OB_FAIL(mds::MdsFactory::deep_copy_buffer_ctx(
                trans_id_, *(old_node.buffer_ctx_node_.get_ctx()), new_ctx))) {
          TRANS_LOG(WARN, "fail to deep copy buffer ctx", KR(ret), KPC(new_ctx), K(*this),
                    K(old_node));
        }
      }
    }
    return ret;
  };
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
        mds::BufferCtx *new_ctx = nullptr;
        if (OB_FAIL(process_with_buffer_ctx(node, new_ctx))) {
          TRANS_LOG(WARN, "process_with_buffer_ctx failed", KR(ret), K(*this));
        } else if (OB_FAIL(new_node.init(node.get_data_source_type(), data, node.mds_base_scn_,
                                         new_ctx))) {
          mtl_free(data.ptr());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "init new node failed", KR(ret), K(*this));
        } else if (ObTxBufferNode::is_valid_register_no(node.get_register_no())
                   && OB_FAIL(new_node.set_mds_register_no(node.get_register_no()))) {
          mtl_free(data.ptr());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "set mds register_no failed", KR(ret), K(*this));
        } else if (OB_FAIL(tmp_buf_arr.push_back(new_node))) {
          mtl_free(data.ptr());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "push multi source data failed", KR(ret), K(*this));
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < tmp_buf_arr.count(); ++i) {
        mtl_free(tmp_buf_arr[i].data_.ptr());
        tmp_buf_arr[i].buffer_ctx_node_.destroy_ctx();
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
      exec_info_.multi_data_source_[i].buffer_ctx_node_.destroy_ctx();
    }
    exec_info_.multi_data_source_.reset();
  }

  if (OB_FAIL(ret)) {

  } else {

    const int64_t tmp_buf_array_cnt = tmp_buf_arr.count();
    const int64_t ctx_mds_array_cnt = exec_info_.multi_data_source_.count();
    int64_t max_register_no_in_ctx = 0;
    if (exec_info_.multi_data_source_.count() > 0) {
      max_register_no_in_ctx =
          exec_info_.multi_data_source_[ctx_mds_array_cnt - 1].get_register_no();
    }
    int64_t ctx_array_start_index = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_buf_array_cnt; ++i) {
      if (is_follower_()) {
        tmp_buf_arr[i].set_submitted();
        tmp_buf_arr[i].set_synced();
      }
      if (ObTxBufferNode::is_valid_register_no(max_register_no_in_ctx)
          && ObTxBufferNode::is_valid_register_no(tmp_buf_arr[i].get_register_no())
          && tmp_buf_arr[i].get_register_no() <= max_register_no_in_ctx) {
        while ((!ObTxBufferNode::is_valid_register_no(
                    exec_info_.multi_data_source_[ctx_array_start_index].get_register_no())
                || tmp_buf_arr[i].get_register_no()
                       > exec_info_.multi_data_source_[ctx_array_start_index].get_register_no())
               && ctx_array_start_index < ctx_mds_array_cnt) {
          ctx_array_start_index++;
        }
        if (tmp_buf_arr[i].get_register_no()
            == exec_info_.multi_data_source_[ctx_array_start_index].get_register_no()) {

          mtl_free(tmp_buf_arr[i].data_.ptr());
          tmp_buf_arr[i].buffer_ctx_node_.destroy_ctx();
          if (OB_FAIL(incremental_array.push_back(
                  exec_info_.multi_data_source_[ctx_array_start_index]))) {
            TRANS_LOG(WARN, "push back incremental_array failed", K(ret));
          }
          TRANS_LOG(INFO, "filter mds node replay by the register_no", K(ret), K(trans_id_),
                    K(ls_id_), K(i), K(ctx_array_start_index), K(tmp_buf_arr[i].get_register_no()),
                    K(exec_info_.multi_data_source_[ctx_array_start_index]));
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "we can not find a mds node in ctx with the same register_no", K(ret),
                    K(i), K(ctx_array_start_index), K(tmp_buf_arr[i].get_register_no()),
                    K(exec_info_.multi_data_source_[ctx_array_start_index]), KPC(this));
        }

      } else {
        if (OB_FAIL(exec_info_.multi_data_source_.push_back(tmp_buf_arr[i]))) {
          TRANS_LOG(WARN, "push back exec_info_.multi_data_source_ failed", K(ret));
        } else if (OB_FAIL(incremental_array.push_back(tmp_buf_arr[i]))) {
          TRANS_LOG(WARN, "push back incremental_array failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartTransCtx::decide_state_log_barrier_type_(
    const ObTxLogType &state_log_type,
    logservice::ObReplayBarrierType &final_barrier_type)
{
  int ret = OB_SUCCESS;

  final_barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  logservice::ObReplayBarrierType mds_cache_final_log_barrier_type =
      logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  logservice::ObReplayBarrierType tmp_state_log_barrier_type =
      logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mds_cache_.decide_cache_state_log_mds_barrier_type(
            state_log_type, mds_cache_final_log_barrier_type))) {
      TRANS_LOG(WARN, "decide mds cache state log barrier type failed", K(ret), K(state_log_type),
                K(mds_cache_final_log_barrier_type), KPC(this));
    } else {
      final_barrier_type = mds_cache_final_log_barrier_type;
    }
  }

  if (OB_SUCC(ret)) {
    for (int i = 0; i < exec_info_.multi_data_source_.count() && OB_SUCC(ret); i++) {

      tmp_state_log_barrier_type = ObTxLogTypeChecker::need_replay_barrier(
          state_log_type, exec_info_.multi_data_source_[i].get_data_source_type());
      if (OB_FAIL(ObTxLogTypeChecker::decide_final_barrier_type(tmp_state_log_barrier_type,
                                                                final_barrier_type))) {
        TRANS_LOG(WARN, "decide one mds node barrier type failed", K(ret),
                  K(tmp_state_log_barrier_type), K(final_barrier_type), K(state_log_type),
                  K(exec_info_.multi_data_source_[i]), KPC(this));
      }
    }
  }

  if (OB_SUCC(ret) && final_barrier_type != logservice::ObReplayBarrierType::NO_NEED_BARRIER
      && final_barrier_type != logservice::ObReplayBarrierType::INVALID_BARRIER) {
    TRANS_LOG(INFO, "decide a valid barrier type for state_log", K(ret),
              K(mds_cache_final_log_barrier_type), K(final_barrier_type), K(state_log_type),
              KPC(this));
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
  ObTxLogBlockHeader log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_,
                                      exec_info_.scheduler_);
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

  logservice::ObReplayBarrierType barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  share::SCN mds_base_scn;

  ObTxLogCb *log_cb = nullptr;
  if (mds_cache_.count() > 0) {
    ObTxMultiDataSourceLog log;
    ObTxMDSRange range;
    while (OB_SUCC(ret)) {
      log.reset();
      mds_base_scn.reset();
      barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
      if (OB_FAIL(exec_info_.redo_lsns_.reserve(exec_info_.redo_lsns_.count() + 1))) {
        TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
      } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
        if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
          TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
        }
      } else {
        ret = mds_cache_.fill_mds_log(this, log, log_cb->get_mds_range(), barrier_type, mds_base_scn);
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
      } else if (OB_FAIL(log_block.add_new_log(log, &big_segment_info_.segment_buf_))) {
        // do not handle ret code OB_BUF_NOT_ENOUGH, one log entry should be
        // enough to hold multi source data, if not, take it as an error.
        TRANS_LOG(WARN, "add new log failed", KR(ret), K(*this));

        if (OB_LOG_TOO_LARGE == ret) {
          share::SCN base_scn;
          base_scn.set_min();
          if (OB_FAIL(prepare_big_segment_submit_(log_cb, base_scn, barrier_type,
                                                  ObTxLogType::TX_MULTI_DATA_SOURCE_LOG))) {
            TRANS_LOG(WARN, "prepare big segment failed", K(ret), KPC(this));
          } else {
            ret = OB_LOG_TOO_LARGE;
            TRANS_LOG(INFO, "construct big multi data source",K(ret),K(trans_id_),K(ls_id_),K(log));
          }
        }
      } else if (barrier_type != logservice::ObReplayBarrierType::NO_NEED_BARRIER
                 && OB_FAIL(
                     log_block.rewrite_barrier_log_block(trans_id_.get_id(), barrier_type))) {
        TRANS_LOG(WARN, "rewrite multi data source log barrier failed", K(ret));
        return_log_cb_(log_cb);
        log_cb = NULL;
      } else if (log_block.get_cb_arg_array().count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      } else if (OB_FAIL(acquire_ctx_ref_())) {
        TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        log_cb = nullptr;
      } else if ((mds_base_scn.is_valid() ? OB_FALSE_IT(mds_base_scn = share::SCN::scn_inc(mds_base_scn)) : OB_FALSE_IT(mds_base_scn.set_min()))) {
        // do nothing
      } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                     log_block.get_buf(), log_block.get_size(), mds_base_scn, log_cb, false))) {
        TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
        release_ctx_ref_();
      } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
        log_cb = nullptr;
      } else {
        if (barrier_type != logservice::ObReplayBarrierType::NO_NEED_BARRIER || !mds_base_scn.is_min()) {
          TRANS_LOG(INFO, "submit MDS redo with barrier or base_scn successfully", K(ret), K(trans_id_),
                    K(ls_id_), KPC(log_cb), K(mds_cache_), K(exec_info_.multi_data_source_),
                    K(mds_base_scn), K(barrier_type));
        }
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
      && OB_FAIL(submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG))) {
      TRANS_LOG(WARN, "submit multi data souce log failed", K(ret));
    } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
      TRANS_LOG(WARN, "copy total mds array failed", K(ret));
    } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END, SCN(), false,
                                           tmp_array))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
    }
  }
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_DUP_TABLE_REDO_SYNC)
ERRSIM_POINT_DEF(EN_SUBMIT_TX_PREPARE_LOG)
#endif

int ObPartTransCtx::errism_dup_table_redo_sync_()
{

  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = EN_DUP_TABLE_REDO_SYNC;
#endif
  return ret;
}

int ObPartTransCtx::errism_submit_prepare_log_()
{

  int ret = OB_SUCCESS;

  if (OB_NOT_MASTER == ERRSIM_DELAY_TX_SUBMIT_LOG && is_root()) {
    ret = ERRSIM_DELAY_TX_SUBMIT_LOG;
  } else if (OB_BLOCK_FROZEN == ERRSIM_DELAY_TX_SUBMIT_LOG && !is_root()) {
    ret = ERRSIM_DELAY_TX_SUBMIT_LOG;
  }

#ifdef ERRSIM
  ret = EN_SUBMIT_TX_PREPARE_LOG;
#endif

  return ret;
}

int ObPartTransCtx::notify_table_lock_(const SCN &log_ts,
                                       const bool for_replay,
                                       const ObTxBufferNodeArray &notify_array,
                                       const bool is_force_kill)
{
  int ret = OB_SUCCESS;
  if (is_exiting_ && sub_state_.is_force_abort()) {
    // do nothing
  } else {
    ObMulSourceDataNotifyArg arg;
    arg.tx_id_ = trans_id_;
    arg.scn_ = log_ts;
    arg.trans_version_ = ctx_tx_data_.get_commit_version();
    arg.for_replay_ = for_replay;
    // table lock only need tx end
    arg.notify_type_ = NotifyType::TX_END;
    arg.is_force_kill_ = is_force_kill;

    int64_t total_time = 0;

    if (OB_FAIL(ObMulSourceTxDataNotifier::notify_table_lock(notify_array,
                                                             arg,
                                                             this,
                                                             total_time))) {
      TRANS_LOG(WARN, "notify data source failed", K(ret), K(arg));
    }
    if (notify_array.count() > 0) {
      TRANS_LOG(INFO, "notify MDS table lock", K(ret), K(trans_id_), K(ls_id_),
                K(log_ts), K(notify_array.count()), K(notify_array), K(total_time));
    }
  }
  return ret;
}

int ObPartTransCtx::notify_data_source_(const NotifyType notify_type,
                                        const SCN &log_ts,
                                        const bool for_replay,
                                        const ObTxBufferNodeArray &notify_array,
                                        const bool is_force_kill)
{
  int ret = OB_SUCCESS;
  if (is_exiting_ && sub_state_.is_force_abort()) {
    // do nothing
  } else {
    ObMulSourceDataNotifyArg arg;
    arg.tx_id_ = trans_id_;
    arg.scn_ = log_ts;
    arg.trans_version_ = ctx_tx_data_.get_commit_version();
    arg.for_replay_ = for_replay;
    arg.notify_type_ = notify_type;
    arg.is_force_kill_ = is_force_kill;

    int64_t total_time = 0;

    if (OB_FAIL(
            ObMulSourceTxDataNotifier::notify(notify_array, notify_type, arg, this, total_time))) {
      TRANS_LOG(WARN, "notify data source failed", K(ret), K(arg));
    }
    if (notify_array.count() > 0) {
      TRANS_LOG(INFO, "notify MDS", K(ret), K(trans_id_), K(ls_id_), K(notify_type), K(log_ts), K(notify_array.count()),
                K(notify_array), K(total_time));
    }
  }
  return ret;
}

int ObPartTransCtx::register_multi_data_source(const ObTxDataSourceType data_source_type,
                                               const char *buf,
                                               const int64_t len,
                                               const bool try_lock,
                                               const ObRegisterMdsFlag &register_flag)
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
    } else if (OB_UNLIKELY(is_committing_())) {
      ret = OB_TRANS_HAS_DECIDED;
      if (is_trans_expired_() && ObPartTransAction::ABORT == part_trans_action_) {
        // rewrite error with TRANS_TIMEOUT to easy user
        ret = OB_TRANS_TIMEOUT;
      } else if (ObPartTransAction::ABORT == part_trans_action_
                 || exec_info_.state_ == ObTxState::ABORT) {
        ret = OB_TRANS_KILLED;
      }
      TRANS_LOG(WARN, "tx has decided", K(ret), KPC(this));
    } else if (OB_UNLIKELY(sub_state_.is_force_abort())) {
      ret = OB_TRANS_KILLED;
      TRANS_LOG(WARN, "tx force aborted due to data incomplete", K(ret), KPC(this));
    } else if (is_follower_()) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "can not register mds on a follower", K(ret), K(data_source_type), K(len),
                KPC(this));
    } else if (is_committing_()) {
      ret = OB_TRANS_HAS_DECIDED;
      TRANS_LOG(WARN, "can not register mds in committing part_ctx", K(ret), KPC(this));
    } else if (OB_ISNULL(ptr = mtl_malloc(len, "MultiTxData"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(data_source_type), K(len));
    } else if (FALSE_IT(MEMCPY(ptr, buf, len))) {
    } else if (FALSE_IT(data.assign_ptr(reinterpret_cast<char *>(ptr), len))) {
    } else {
      mds::BufferCtx *buffer_ctx = nullptr;
      if (data_source_type > ObTxDataSourceType::BEFORE_VERSION_4_1
          || ObTxDataSourceType::CREATE_TABLET_NEW_MDS == data_source_type
          || ObTxDataSourceType::DELETE_TABLET_NEW_MDS == data_source_type
          || ObTxDataSourceType::UNBIND_TABLET_NEW_MDS == data_source_type) {
        ret = mds::MdsFactory::create_buffer_ctx(data_source_type, trans_id_, buffer_ctx);
      }
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "execute MDS frame code failed, execution interruped", KR(ret),
                  K(data_source_type), K(*this));
      } else if (OB_FAIL(
                     node.init(data_source_type, data, register_flag.mds_base_scn_, buffer_ctx))) {
        TRANS_LOG(WARN, "init tx buffer node failed", KR(ret), K(data_source_type), K(*this));
      } else if (OB_FAIL(tmp_array.push_back(node))) {
        TRANS_LOG(WARN, "push back notify node  failed", KR(ret));
//#ifndef OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF
      } else if (tmp_array.get_serialize_size() > ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE
                 && !node.allow_to_use_mds_big_segment()) {
        ret = OB_LOG_TOO_LARGE;
        TRANS_LOG(WARN, "too large mds buf node", K(ret), K(tmp_array.get_serialize_size()));
//#endif
      } else if (OB_FAIL(mds_cache_.try_recover_max_register_no(exec_info_.multi_data_source_))) {
        TRANS_LOG(WARN, "recover max register no failed", K(ret), K(mds_cache_), KPC(this));
      } else if (OB_FAIL(mds_cache_.insert_mds_node(node))) {
        TRANS_LOG(WARN, "register multi source data failed", KR(ret), K(data_source_type),
                  K(*this));
      }

      if (OB_FAIL(ret)) {
        mtl_free(ptr);
        if (OB_NOT_NULL(buffer_ctx)) {
          MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(buffer_ctx);
        }
      } else if (OB_FAIL(notify_data_source_(NotifyType::REGISTER_SUCC, SCN(), false, tmp_array))) {
        if (OB_SUCCESS != (tmp_ret = mds_cache_.rollback_last_mds_node())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "rollback last mds node failed", K(tmp_ret), K(ret));
        }

        TRANS_LOG(WARN, "notify data source for register_succ failed", K(tmp_ret));
      } else if (mds_cache_.get_unsubmitted_size() < ObTxMultiDataSourceLog::MAX_PENDING_BUF_SIZE
                 && !register_flag.need_flush_redo_instantly_) {
        // do nothing
      } else if (OB_SUCCESS
                 != (tmp_ret = submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG))) {
        if (tmp_ret == OB_NOT_MASTER) {
          ret = OB_TRANS_NEED_ROLLBACK;
        } else if (tmp_ret == OB_TX_NOLOGCB) {
          ret = OB_SUCCESS;
          if (register_flag.need_flush_redo_instantly_) {
            mds_cache_.set_need_retry_submit_mds(true);
          }
        }
        TRANS_LOG(WARN, "submit mds log failed", K(tmp_ret), K(ret), K(register_flag),
                  K(data_source_type), KPC(this));
      } else {
        TRANS_LOG(DEBUG, "submit mds log success", K(tmp_ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "register MDS redo in part_ctx failed", K(ret), K(trans_id_), K(ls_id_),
              K(data_source_type), K(len), K(register_flag), K(mds_cache_), K(*this), K(lbt()));
  }

  REC_TRANS_TRACE_EXT2(tlog_, register_multi_data_source, OB_ID(ret), ret, OB_ID(type),
                       data_source_type);

  return ret;
}

int ObPartTransCtx::del_retain_ctx()
{
  int ret = OB_SUCCESS;
  bool need_del = false;

  {
    CtxLockGuard guard(lock_);

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
        FLOG_INFO("del retain ctx after exiting", K(ctx_ref), KPC(this));
      }
    } else {
      // MDS trans : allowed gc before clear log on_success
      // del ctx by set_exiting_()
      clean_retain_cause_();
      FLOG_INFO("del retain ctx before exiting", KPC(this));
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
  int ret = OB_SUCCESS;

  if (ls_tx_ctx_mgr_->get_ls_log_adapter()->has_dup_tablet()) {
    if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_COMMIT_INFO_LOG))) {
      TRANS_LOG(WARN, "submit commit info log failed", K(ret), KPC(this));
    }
  }
  return ret;
  // return mt_ctx_.get_redo_generator().search_unsubmitted_dup_tablet_redo();
}

int ObPartTransCtx::dup_table_tx_redo_sync_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  bool redo_sync_finish = false;
  share::SCN tmp_max_read_version;
  tmp_max_read_version.set_invalid();

  if (busy_cbs_.get_size() > 0 && get_downstream_state() < ObTxState::REDO_COMPLETE) {
    ret = OB_EAGAIN;
    TRANS_LOG(INFO, "start redo sync after the on_success of commit info log ", K(ret), KPC(this));
  } else if (get_downstream_state() != ObTxState::REDO_COMPLETE || !exec_info_.is_dup_tx_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "invalid dup trx state", K(ret), KPC(this));
  } else if (is_2pc_logging_()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "the dup table participant is 2pc logging, need retry", K(ret), KPC(this));
  } else if (is_follower_()) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "can not execute redo sync on a follower", KPC(this));
  } else if (OB_FAIL(errism_dup_table_redo_sync_())) {
    TRANS_LOG(WARN, "errsim for dup table redo sync", K(ret), KPC(this));
  } else if (is_dup_table_redo_sync_completed_()) {
    ret = OB_SUCCESS;
    redo_sync_finish = true;
    bool no_need_submit_log = false;
    if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
      TRANS_LOG(WARN, "do prepare failed after redo sync", K(tmp_ret), KPC(this));
    } else {
    }
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->check_redo_sync_completed(
                 trans_id_, exec_info_.max_applied_log_ts_, redo_sync_finish,
                 tmp_max_read_version))) {
    TRANS_LOG(WARN, "check redo sync completed failed", K(ret), K(redo_sync_finish),
              K(tmp_max_read_version), KPC(this));
  } else if (redo_sync_finish) {
    if (!tmp_max_read_version.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid dup table follower's max_read_version", K(ret),
                K(tmp_max_read_version), KPC(this));
    } else {
      dup_table_follower_max_read_version_ = tmp_max_read_version;
      /*
       * drive into prepare state in the next do_prepare operation
       * */
      TRANS_LOG(INFO, "finish redo sync for dup table trx", K(ret), K(redo_sync_finish),
                K(dup_table_follower_max_read_version_), KPC(this));
    }
  } else {
    ret = OB_EAGAIN;
    TRANS_LOG(INFO, "redo sync need retry", K(ret), K(redo_sync_finish), K(tmp_max_read_version),
              K(dup_table_follower_max_read_version_), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::submit_pending_log_block_(ObTxLogBlock &log_block,
                                              memtable::ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;

  if (log_block.get_cb_arg_array().empty()) {
    TRANS_LOG(INFO, "no need to submit pending log block because of empty", K(ret), K(trans_id_),
              K(ls_id_), K(log_block));
  } else {
    bool need_final_cb = false;
    if (is_contain(log_block.get_cb_arg_array(), ObTxLogType::TX_COMMIT_LOG)
        || is_contain(log_block.get_cb_arg_array(), ObTxLogType::TX_COMMIT_LOG)) {
      need_final_cb = true;
    }

    ObTxLogCb *log_cb = NULL;
    if (OB_FAIL(prepare_log_cb_(need_final_cb, log_cb))) {
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
                   log_block.get_buf(), log_block.get_size(), share::SCN::min_scn(), log_cb,
                   false))) {
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

  return ret;
}

int ObPartTransCtx::check_dup_trx_with_submitting_all_redo(ObTxLogBlock &log_block,
                                                           memtable::ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;

  // 1. submit all redo in dup ls
  // 2. check modified tablet
  // 3. if not dup_tx, do nothing
  // 4. if dup_tx:
  //    a. set is_dup_tx and upstream
  //    b. submit commit info and start dup_table_redo_sync in on_success
  //    c. return OB_EAGAIN
  if (ls_tx_ctx_mgr_->get_ls_log_adapter()->has_dup_tablet()) {
    if (!sub_state_.is_info_log_submitted() && get_downstream_state() < ObTxState::REDO_COMPLETE) {
      if (exec_info_.redo_lsns_.count() > 0 || exec_info_.prev_record_lsn_.is_valid()
          || !helper.callbacks_.is_empty()) {
        set_dup_table_tx_();
      }

      // ret = submit_pending_log_block_(log_block, helper);
      //
      // TRANS_LOG(INFO, "submit all redo log for dup table check", K(ret),
      //           K(exec_info_.tablet_modify_record_.count()), KPC(this));
      // if (OB_FAIL(ret)) {
      //   // do nothing
      // } else if (OB_FAIL(check_tablet_modify_record_())) {
      //   TRANS_LOG(WARN, "check the modify tablet failed", K(ret));
      // }
    }

  } else {
    // do nothing
  }

  if (OB_SUCC(ret) && exec_info_.is_dup_tx_) {

    if (exec_info_.participants_.count() == 1 && !exec_info_.upstream_.is_valid()) {
      set_2pc_upstream_(ls_id_);
    }
  }

  return ret;
}

bool ObPartTransCtx::is_dup_table_redo_sync_completed_()
{
  bool redo_sync_completed = true;

  if (exec_info_.state_ <= ObTxState::REDO_COMPLETE) {
    redo_sync_completed = (dup_table_follower_max_read_version_.is_valid());
  }

  return redo_sync_completed;
}

int ObPartTransCtx::dup_table_tx_pre_commit_()
{
  int ret = OB_SUCCESS;

  // dup table pre commit not finish => post ts sync request and return OB_EAGAIN
  // dup table pre commit finish => OB_SUCCESS

  return ret;
}

// int ObPartTransCtx::merge_tablet_modify_record_(const common::ObTabletID &tablet_id)
// {
//   int ret = OB_SUCCESS;
//
//   if (exec_info_.tablet_modify_record_.count() >= MAX_TABLET_MODIFY_RECORD_COUNT) {
//     // do nothing
//   } else {
//     bool is_contain = false;
//     for (int i = 0; i < exec_info_.tablet_modify_record_.count(); i++) {
//       if (exec_info_.tablet_modify_record_[i] == tablet_id) {
//         is_contain = true;
//       }
//     }
//     if (!is_contain && OB_FAIL(exec_info_.tablet_modify_record_.push_back(tablet_id))) {
//       TRANS_LOG(WARN, "push back tablet id failed", K(ret), K(tablet_id),
//                 K(exec_info_.tablet_modify_record_));
//     }
//   }
//
//   return ret;
// }

// int ObPartTransCtx::check_tablet_modify_record_()
// {
//   int ret = OB_SUCCESS;
//
//   if (!exec_info_.is_dup_tx_) {
//     bool has_dup_tablet = false;
//     if (!ls_tx_ctx_mgr_->get_ls_log_adapter()->has_dup_tablet()) {
//       has_dup_tablet = false;
//       TRANS_LOG(INFO, "no dup tablet in this ls", K(has_dup_tablet), K(trans_id_), K(ls_id_));
//     } else if (exec_info_.tablet_modify_record_.count() >= MAX_TABLET_MODIFY_RECORD_COUNT) {
//       has_dup_tablet = true;
//       TRANS_LOG(INFO, "too much tablet, consider it as a dup trx", K(ret), K(has_dup_tablet),
//                 K(exec_info_.tablet_modify_record_), KPC(this));
//     } else {
//       has_dup_tablet = false;
//       for (int i = 0; i < exec_info_.tablet_modify_record_.count(); i++) {
//
//         if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->check_dup_tablet_in_redo(
//                 exec_info_.tablet_modify_record_[i], has_dup_tablet, share::SCN::min_scn(),
//                 share::SCN::max_scn()))) {
//           TRANS_LOG(WARN, "check dup tablet failed", K(ret), K(trans_id_), K(ls_id_),
//                     K(exec_info_.tablet_modify_record_[i]));
//         } else if (has_dup_tablet) {
//           TRANS_LOG(INFO, "modify a dup tablet, consider it as a dup trx", K(ret),
//                     K(has_dup_tablet), K(exec_info_.tablet_modify_record_[i]),
//                     K(exec_info_.tablet_modify_record_.count()), KPC(this));
//           break;
//         }
//       }
//     }
//     if (has_dup_tablet) {
//       set_dup_table_tx_();
//     }
//   }
//
//   return ret;
// }

int ObPartTransCtx::clear_dup_table_redo_sync_result_()
{
  int ret = OB_SUCCESS;

  dup_table_follower_max_read_version_.set_invalid();

  return ret;
}

int ObPartTransCtx::dup_table_before_preapre_(const share::SCN &before_prepare_version,
                                              const bool before_replay)
{
  int ret = OB_SUCCESS;

  if (get_downstream_state() != ObTxState::REDO_COMPLETE
      || (!before_replay && get_upstream_state() != ObTxState::REDO_COMPLETE)
      || (before_replay && get_upstream_state() > ObTxState::PREPARE) || !exec_info_.is_dup_tx_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "unexpected dup trx state", K(ret), KPC(this));
  } else if (!before_prepare_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid before_prepare version", K(ret), K(before_prepare_version),
              K(before_replay), KPC(this));
  } else if (mt_ctx_.get_trans_version().is_max() || !mt_ctx_.get_trans_version().is_valid()
             || mt_ctx_.get_trans_version() < before_prepare_version) {
    mt_ctx_.before_prepare(before_prepare_version);
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "set dup_table before prepare version successfully", K(ret),
                  K(before_prepare_version), K(before_replay), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::retry_dup_trx_before_prepare(const share::SCN &before_prepare_version)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (!is_follower_()) {
    ret = OB_NOT_FOLLOWER;
    TRANS_LOG(WARN, "leader need not handle a before_prepare retry request", K(ret),
              K(before_prepare_version), K(ls_id_), K(trans_id_));
  } else if (OB_FAIL(dup_table_before_preapre_(before_prepare_version))) {
    TRANS_LOG(WARN, "set dup table before_prepare_version failed", K(ret),
              K(before_prepare_version), KPC(this));
  }

  return ret;
}

// int ObPartTransCtx::merge_tablet_modify_record(const common::ObTabletID &tablet_id)
// {
//   int ret = OB_SUCCESS;
//
//   CtxLockGuard guard(lock_);
//
//   if (is_exiting_) {
//     // ret = OB_TRANS_CTX_NOT_EXIST;
//     TRANS_LOG(WARN, "merge tablet modify record into a exiting part_ctx", K(ret), K(tablet_id),
//               KPC(this));
//   } else if (!is_follower_()) {
//     ret = OB_NOT_FOLLOWER;
//     TRANS_LOG(WARN, "can not invoke on leader", K(ret), K(tablet_id), KPC(this));
//   } else if (OB_FAIL(merge_tablet_modify_record_(tablet_id))) {
//     TRANS_LOG(WARN, "merge tablet modify record failed", K(ret));
//   }
//   return ret;
// }

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
  } else if (sub_state_.is_force_abort()) {
    if (OB_FAIL(compensate_abort_log_())) {
      TRANS_LOG(WARN, "compensate abort log failed", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_KILLED;
    }
  } else if (OB_FAIL(set_2pc_participants_(parts))) {
    TRANS_LOG(WARN, "set participants failed", K(ret), KPC(this));
  } else if (OB_FAIL(set_2pc_request_id_(request_id))) {
    TRANS_LOG(WARN, "set request id failed", K(ret), K(request_id), KPC(this));
  } else if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), KPC(this));
  } else if (FALSE_IT(stmt_expired_time_ = expire_ts)) {
  } else {
    if (commit_time.is_valid()) {
      set_stc_(commit_time);
    } else {
      set_stc_by_now_();
    }
    // set 2pc upstream to self
    set_2pc_upstream_(ls_id_);
    exec_info_.trans_type_ = TransType::DIST_TRANS;
    exec_info_.xid_ = xid;
    exec_info_.is_sub2pc_ = true;
    // (void)set_sub2pc_coord_state(Ob2PCPrepareState::REDO_PREPARING);
    if (OB_FAIL(prepare_redo())) {
      TRANS_LOG(WARN, "fail to execute sub prepare", K(ret), KPC(this));
    } else if (OB_FAIL(unregister_timeout_task_())) {
      TRANS_LOG(WARN, "unregister timeout handler error", K(ret), KPC(this));
    } else if (OB_FAIL(register_timeout_task_(ObServerConfig::get_instance().trx_2pc_retry_interval
                                              + trans_id_.hash() % USEC_PER_SEC))) {
      TRANS_LOG(WARN, "register timeout handler error", K(ret), KPC(this));
    } else {
      part_trans_action_ = ObPartTransAction::COMMIT;
    }
    last_request_ts_ = ObClockGenerator::getClock();
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
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "transaction is exiting", K(ret), KPC(this));
  } else if (OB_UNLIKELY(is_2pc_logging_())) {
    TRANS_LOG(WARN, "tx is in 2pc logging", KPC(this));
  } else if (OB_UNLIKELY(is_follower_())) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "not handle this request for follower", KR(ret), KPC(this));
  } else if (xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(tmp_scheduler), KPC(this));
  } else if (ObTxState::INIT == get_upstream_state() && !is_committing_()) {
    // not in commit phase
    // if sub rollback, abort the trans
    // if sub commit, return an error
    if (is_rollback) {
      if (OB_FAIL(abort_(OB_TRANS_ROLLBACKED))) {
        TRANS_LOG(WARN, "abort trans failed", KR(ret), K(*this));
      }
      last_request_ts_ = ObClockGenerator::getClock();
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected request, not in sub prepare state", K(ret), KPC(this));
    }
  } else {
    // in commit phase
    if (xid != exec_info_.xid_) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), KPC(this));
    } else if (!is_sub2pc()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected trans ctx", KR(ret), K(is_rollback), K(xid), KPC(this));
    } else if (ObTxState::REDO_COMPLETE == get_upstream_state() && !is_prepared_sub2pc()) {
      // not finish in first phase
    } else if (ObTxState::REDO_COMPLETE < get_upstream_state()
        || ObTxState::REDO_COMPLETE < get_downstream_state()) {
      // already in second phase
      // in this case, drive by self
    } else {
      tmp_scheduler_ = tmp_scheduler;
      if (OB_FAIL(continue_execution(is_rollback))) {
        TRANS_LOG(WARN, "fail to continue execution", KR(ret), K(is_rollback), K(xid), KPC(this));
      } else if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timeout handler error", K(ret), KPC(this));
      } else if (OB_FAIL(register_timeout_task_(ObServerConfig::get_instance().trx_2pc_retry_interval
                                                + trans_id_.hash() % USEC_PER_SEC))) {
        TRANS_LOG(WARN, "register timeout handler error", K(ret), KPC(this));
      }
      last_request_ts_ = ObClockGenerator::getClock();
    }
  }
  TRANS_LOG(INFO, "sub end tx", K(ret), K(xid), K(is_rollback), K(tmp_scheduler), KPC(this));
  return ret;
}

int ObPartTransCtx::supplement_undo_actions_if_exist_()
{
  int ret = OB_SUCCESS;

  ObTxTable *tx_table = nullptr;
  ObTxDataGuard guard;
  ObTxDataGuard tmp_tx_data_guard;
  tmp_tx_data_guard.reset();
  ctx_tx_data_.get_tx_table(tx_table);
  const ObTxData *tx_data = nullptr;

  if (OB_FAIL(ctx_tx_data_.get_tx_data(guard))) {
    TRANS_LOG(ERROR, "get tx data from ctx tx data failed", KR(ret));
  } else if (OB_NOT_NULL(guard.tx_data()->undo_status_list_.head_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid ctx tx data", KR(ret), KPC(tx_data));
  } else if (OB_FAIL(ctx_tx_data_.deep_copy_tx_data_out(tmp_tx_data_guard))) {
    TRANS_LOG(WARN, "deep copy tx data in ctx tx data failed.", KR(ret),
              K(ctx_tx_data_), KPC(this));
  } else if (OB_FAIL(tx_table->supplement_undo_actions_if_exist(
                 tmp_tx_data_guard.tx_data()))) {
    TRANS_LOG(
      WARN,
      "supplement undo actions to a tx data when replaying a transaction from the middle failed.",
      KR(ret), K(ctx_tx_data_), KPC(this));
  } else if (OB_FAIL(
                 ctx_tx_data_.replace_tx_data(tmp_tx_data_guard.tx_data()))) {
    TRANS_LOG(WARN, "replace tx data in ctx tx data failed.", KR(ret), K(ctx_tx_data_), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::check_status()
{
  CtxLockGuard guard(lock_);
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
 * 3) alloc data_scn
 */
int ObPartTransCtx::start_access(const ObTxDesc &tx_desc, ObTxSEQ &data_scn)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (OB_FAIL(check_status_())) {
  } else if (tx_desc.op_sn_ < last_op_sn_) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "stale access operation", K(ret),
              K_(tx_desc.op_sn), K_(last_op_sn), KPC(this), K(tx_desc));
  } else {
    if (!data_scn.is_valid()) {
      data_scn = tx_desc.inc_and_get_tx_seq(0);
    }
    ++pending_write_;
    last_scn_ = MAX(data_scn, last_scn_);
    if (!first_scn_.is_valid()) {
      first_scn_ = last_scn_;
    }
    if (tx_desc.op_sn_ != last_op_sn_) {
      last_op_sn_ = tx_desc.op_sn_;
    }
    mt_ctx_.inc_ref();
    mt_ctx_.acquire_callback_list();
  }
  last_request_ts_ = ObClockGenerator::getClock();
  TRANS_LOG(TRACE, "start_access", K(ret), KPC(this));
  REC_TRANS_TRACE_EXT(tlog_, start_access,
                      OB_ID(ret), ret,
                      OB_ID(trace_id), ObCurTraceId::get_trace_id_str(),
                      OB_ID(opid), tx_desc.op_sn_,
                      OB_ID(data_seq), data_scn.cast_to_int(),
                      OB_ID(pending), pending_write_,
                      OB_ID(ref), get_ref(),
                      OB_ID(tid), get_itid() + 1);
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
  CtxLockGuard guard(lock_);
  --pending_write_;
  mt_ctx_.dec_ref();
  mt_ctx_.revert_callback_list();
  TRANS_LOG(TRACE, "end_access", K(ret), KPC(this));
  REC_TRANS_TRACE_EXT(tlog_, end_access,
                      OB_ID(opid), last_op_sn_,
                      OB_ID(pending), pending_write_,
                      OB_ID(ref), get_ref(),
                      OB_ID(tid), get_itid() + 1);
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
                                          const ObTxSEQ from_scn,
                                          const ObTxSEQ to_scn)
{
  int ret = OB_SUCCESS;
  bool need_write_log = false;
  CtxLockGuard guard(lock_);
  if (OB_FAIL(check_status_())) {
  } else if(is_logging_()) {
    ret = OB_NEED_RETRY;
    // check cur leader and fast fail
    int tmp_ret = OB_SUCCESS; bool leader = false; int64_t epoch = 0;
    if (OB_TMP_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->get_role(leader, epoch))) {
      TRANS_LOG(WARN, "get ls role failed", K(tmp_ret), K(trans_id_), K(ls_id_));
    } else if (!leader) {
      ret = OB_NOT_MASTER;
    }
    TRANS_LOG(WARN, "rollback_to need retry because of logging", K(ret),
              K(trans_id_), K(ls_id_), K(busy_cbs_.get_size()));
  } else if (op_sn < last_op_sn_) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else if (FALSE_IT(last_op_sn_ = op_sn)) {
  } else if (pending_write_ > 0) {
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "has pending write, rollback blocked",
              K(ret), K(pending_write_), KPC(this));
  } else if (last_scn_ <= to_scn) {
    TRANS_LOG(INFO, "rollback succeed trivially", K(op_sn), K(to_scn), K_(last_scn));
  } else if (OB_FAIL(rollback_to_savepoint_(from_scn, to_scn))) {
    TRANS_LOG(WARN, "rollback_to_savepoint fail", K(ret),
              K(from_scn), K(to_scn), K(op_sn), KPC(this));
  } else {
    last_scn_ = to_scn;
  }
  REC_TRANS_TRACE_EXT(tlog_, rollback_savepoint,
                      OB_ID(ret), ret,
                      OB_ID(from), from_scn.cast_to_int(),
                      OB_ID(to), to_scn.cast_to_int(),
                      OB_ID(pending), pending_write_,
                      OB_ID(opid), op_sn,
                      OB_ID(tid), GETTID());
#ifndef NDEBUG
  TRANS_LOG(INFO, "rollback to savepoint", K(ret),
            K(from_scn), K(to_scn), KPC(this));
#endif
  return ret;
}

int ObPartTransCtx::rollback_to_savepoint_(const ObTxSEQ from_scn,
                                           const ObTxSEQ to_scn)
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
  ObTxDataGuard tmp_tx_data_guard;
  tmp_tx_data_guard.reset();
  if (is_follower_()) { /* Follower */
    ObUndoAction undo_action(from_scn, to_scn);
    if (OB_FAIL(ctx_tx_data_.add_undo_action(undo_action))) {
      TRANS_LOG(WARN, "recrod undo info fail", K(ret), K(from_scn), K(to_scn), KPC(this));
    } else if (OB_FAIL(ctx_tx_data_.deep_copy_tx_data_out(tmp_tx_data_guard))) {
      TRANS_LOG(WARN, "deep copy tx data failed", KR(ret), K(*this));
    } else if (FALSE_IT(tmp_tx_data_guard.tx_data()->end_scn_ = exec_info_.max_applying_log_ts_)) {
    } else if (OB_FAIL(ctx_tx_data_.insert_tmp_tx_data(tmp_tx_data_guard.tx_data()))) {
      TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
    }
  } else if (OB_UNLIKELY(exec_info_.max_submitted_seq_no_ > to_scn)) { /* Leader */
    ObUndoAction undo_action(from_scn, to_scn);
    ObUndoStatusNode *undo_status = NULL;
    if (OB_FAIL(ctx_tx_data_.prepare_add_undo_action(undo_action, tmp_tx_data_guard, undo_status))) {
      TRANS_LOG(WARN, "prepare add undo action fail", K(ret), KPC(this));
    } else if (OB_FAIL(submit_rollback_to_log_(from_scn, to_scn, tmp_tx_data_guard.tx_data()))) {
      TRANS_LOG(WARN, "submit undo redolog fail", K(ret), K(from_scn), K(to_scn), KPC(this));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ctx_tx_data_.cancel_add_undo_action(undo_status))) {
        TRANS_LOG(ERROR, "cancel add undo action failed", KR(tmp_ret), KPC(this));
      }
    } else if (OB_FAIL(ctx_tx_data_.commit_add_undo_action(undo_action, undo_status))) {
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

int ObPartTransCtx::submit_rollback_to_log_(const ObTxSEQ from_scn,
                                            const ObTxSEQ to_scn,
                                            ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  const auto replay_hint = static_cast<int64_t>(trans_id_.get_id());
  ObTxRollbackToLog log(from_scn, to_scn);
  ObTxLogCb *log_cb = NULL;

  ObTxLogBlockHeader
      log_block_header(cluster_id_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);

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
                 log_block.get_buf(), log_block.get_size(), SCN::min_scn(), log_cb, false /*nonblock on EAGAIN*/
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
  REC_TRANS_TRACE_EXT(tlog_, submit_rollback_log,
                      OB_ID(ret), ret,
                      OB_ID(from), from_scn.cast_to_int(),
                      OB_ID(to), to_scn.cast_to_int());
  TRANS_LOG(INFO, "RollbackToLog submit", K(ret), K(from_scn), K(to_scn), KP(log_cb), KPC(this));
  return ret;
}

int ObPartTransCtx::abort(const int reason)
{
  UNUSED(reason);
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
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
    last_request_ts_ = ObClockGenerator::getClock();
  }
  return ret;
}


int ObPartTransCtx::tx_keepalive_response_(const int64_t status)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_SWITCHING_TO_FOLLOWER_GRACEFULLY == ERRSIM_DELAY_TX_SUBMIT_LOG) {
    return ret;
  }

  if ((OB_TRANS_CTX_NOT_EXIST == status || OB_TRANS_ROLLBACKED == status || OB_TRANS_KILLED == status) && can_be_recycled_()) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(WARN, "[TRANS GC] tx has quit, local tx will be aborted",
                K(status), KPC(this));
    }
    if (OB_FAIL(gc_ctx_())) {
      TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), KPC(this));
    }
  } else if (OB_TRANS_COMMITED == status && can_be_recycled_() && first_scn_ >= last_scn_ /*all changes were rollbacked*/) {
    TRANS_LOG(WARN, "txn has comitted on scheduler, but this particiapnt can be recycled", KPC(this));
    FORCE_PRINT_TRACE(tlog_, "[participant leaky] ");
  } else if (OB_SUCCESS != status) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(WARN, "[TRANS GC] tx keepalive fail", K(status), KPC(this));
    }
  } else {
    TRANS_LOG(TRACE, "tx is alive", KPC(this));
  }
  return ret;
}

int ObPartTransCtx::try_alloc_retain_ctx_func_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(retain_ctx_func_ptr_)) {

    if (OB_ISNULL(retain_ctx_func_ptr_ = static_cast<ObMDSRetainCtxFunctor *>(
                      ObTxRetainCtxMgr::alloc_object(sizeof(ObMDSRetainCtxFunctor))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory failed", K(ret), KPC(retain_ctx_func_ptr_), KPC(this));

    } else if (OB_FALSE_IT(new (retain_ctx_func_ptr_) ObMDSRetainCtxFunctor())) {
    }

  } else if (!is_follower_()) {
    TRANS_LOG(INFO, "no need to alloc a new retain_ctx_func", K(ret), KP(retain_ctx_func_ptr_),
              KPC(retain_ctx_func_ptr_), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::try_gc_retain_ctx_func_()
{
  int ret = OB_SUCCESS;

  if(OB_NOT_NULL(retain_ctx_func_ptr_))
  {
    ObTxRetainCtxMgr::free_object(retain_ctx_func_ptr_);
    retain_ctx_func_ptr_ = nullptr;
  }

  return ret;
}

int ObPartTransCtx::insert_into_retain_ctx_mgr_(RetainCause cause,
                                                const SCN &log_ts,
                                                palf::LSN lsn,
                                                bool for_replay)
{
  int ret = OB_SUCCESS;
  int64_t retain_lock_timeout = INT64_MAX;
  if (for_replay) {
    retain_lock_timeout = 10 * 1000;
  }

  ObTxRetainCtxMgr &retain_ctx_mgr = ls_tx_ctx_mgr_->get_retain_ctx_mgr();
  if (OB_ISNULL(ls_tx_ctx_mgr_) || RetainCause::UNKOWN == cause) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(cause), KP(ls_tx_ctx_mgr_), KPC(this));
  } else if (OB_ISNULL(retain_ctx_func_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpectd retain ctx function ptr error", K(cause), K(log_ts), K(lsn),
              K(for_replay), KPC(retain_ctx_func_ptr_), KPC(this));
  } else {

    ObMDSRetainCtxFunctor *mds_retain_func_ptr =
        static_cast<ObMDSRetainCtxFunctor *>(retain_ctx_func_ptr_);

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(mds_retain_func_ptr->init(this, cause, log_ts, lsn))) {
      TRANS_LOG(WARN, "init retain ctx functor failed", K(ret), KPC(this));
    } else if (OB_FAIL(retain_ctx_mgr.push_retain_ctx(retain_ctx_func_ptr_, retain_lock_timeout))) {
      TRANS_LOG(WARN, "push into retain_ctx_mgr failed", K(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    clean_retain_cause_();
    if (!(OB_EAGAIN == ret && for_replay)) {
      TRANS_LOG(ERROR,
                "insert into retain_ctx_mgr error, retain ctx will not be retained in ctx_mgr",
                K(ret), KPC(this));
      // ob_abort();
    }
  } else {
    // insert into retain_ctx_mgr successfully, no need to gc memory
    retain_ctx_func_ptr_ = nullptr;
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
      if (sub_state_.is_force_abort()) {
        if (OB_FAIL(compensate_abort_log_())) {
          TRANS_LOG(WARN, "compensate abort log failed", K(ret), K(ls_id_), K(trans_id_),
                    K(tx_end_action), K(sub_state_));
        } else {
          ret = OB_TRANS_KILLED;
        }
      } else {
        ret = do_local_commit_tx_();
      }
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
    case TxEndAction::DELAY_ABORT_TX: {
      sub_state_.set_force_abort();
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
    sub_state_.set_force_abort();
    if (OB_FAIL(on_local_abort_tx_())) {
      TRANS_LOG(WARN, "local tx abort failed", KR(ret), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::do_force_kill_tx_()
{
  int ret = OB_SUCCESS;

  ObTxBufferNodeArray tmp_array;

  if (get_downstream_state() >= ObTxState::COMMIT) {
    // do nothing
  } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
    TRANS_LOG(WARN, "gen total mds array failed", KR(ret), K(*this));
  // } else if (OB_FAIL(notify_data_source_(NotifyType::ON_ABORT,
  //                                        ctx_tx_data_.get_end_log_ts() /*invalid_scn*/, false,
  //                                        tmp_array, true /*is_force_kill*/))) {
  //   TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
  }

  if (OB_SUCC(ret)) {
    trans_kill_();
    // Force kill cannot guarantee the consistency, so we just set end_log_ts
    // to zero
    end_log_ts_.set_min();
    (void)trans_clear_();
    if (OB_FAIL(unregister_timeout_task_())) {
      TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
    }
    sub_state_.set_force_abort();
    // Ignore ret
    set_exiting_();
    TRANS_LOG(INFO, "transaction killed success", "context", *this);
  }
  return ret;
}

int ObPartTransCtx::on_local_commit_tx_()
{
  int ret = OB_SUCCESS;
  bool need_wait = false;

  if (sub_state_.is_gts_waiting()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected gts waiting flag", K(ret), KPC(this));
  } else if (!OB_UNLIKELY(ctx_tx_data_.get_commit_version().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid commit version", K(ret), KPC(this));
  } else if (OB_FAIL(wait_gts_elapse_commit_version_(need_wait))) {
    TRANS_LOG(WARN, "wait gts elapse commit version failed", KR(ret), KPC(this));
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
    REC_TRANS_TRACE_EXT2(tlog_, wait_gts_elapse, OB_ID(ref), get_ref());
  }

  if (OB_FAIL(ret) || need_wait) {
    // do nothing
  } else if (OB_FAIL(after_local_commit_succ_())) {
    TRANS_LOG(WARN, "terminate trx after local commit failed", KR(ret), KPC(this));
  }

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

  ObTxBufferNodeArray tmp_array;

  if (OB_FAIL(tx_end_(false /*commit*/))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), "context", *this);
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
  ObTxDataGuard tx_data_guard;
  ctx_tx_data_.get_tx_data(tx_data_guard);
  if (OB_ISNULL(tx_data_ptr = tx_data_guard.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected nullptr", KR(ret));
  } else {
    tx_data_ptr->dump_2_text(fd);
  }

  fprintf(fd, "\n********** ObPartTransCtx ***********\n");
  return ret;
}

int ObPartTransCtx::check_for_standby(const SCN &snapshot,
                                      bool &can_read,
                                      SCN &trans_version,
                                      bool &is_determined_state)
{
  int ret = OB_ERR_SHARED_LOCK_CONFLICT;
  int tmp_ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_local_tx_()) {
    can_read = false;
    trans_version.set_min();
    is_determined_state = false;
    ret = OB_SUCCESS;
  } else {
    SCN min_snapshot = SCN::max_scn();
    ObStateInfo tmp_state_info;
    // for all parts has been prepared
    ObTxState state = ObTxState::PREPARE;
    SCN version = SCN::min_scn();
    int count = state_info_array_.count();
    for (int i=0; i<count && OB_ERR_SHARED_LOCK_CONFLICT == ret; i++) {
      tmp_state_info = state_info_array_.at(i);
      min_snapshot = MIN(tmp_state_info.snapshot_version_, min_snapshot);
      if (tmp_state_info.state_ != ObTxState::PREPARE) {
        state = tmp_state_info.state_;
      }
      const int64_t stat_idx = (uint8_t)tmp_state_info.state_ / 10;
      if (stat_idx >= 0 && stat_idx < STANDBY_STAT_ARRAY_SIZE) {
        STANDBY_STAT[stat_idx][0]++;
      }
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        statistics_for_standby();
      }
      switch (tmp_state_info.state_) {
        case ObTxState::UNKNOWN: {
          // 当前为获取的日志流回放位点
          SCN readable_scn;
          if (tmp_state_info.snapshot_version_ < snapshot) {
            break;
          } else if (tmp_state_info.version_.is_valid()) {
            if (OB_SUCCESS != (tmp_ret = get_ls_replica_readable_scn_(ls_id_, readable_scn))) {
              TRANS_LOG(WARN, "get ls replica readable scn fail", K(ret), K(snapshot), KPC(this));
            } else if (readable_scn >= tmp_state_info.version_) {
              if (ObTxState::PREPARE == exec_info_.state_) {
                can_read = false;
                trans_version.set_min();
                is_determined_state = false;
                ret = OB_SUCCESS;
              }
            }
            TRANS_LOG(INFO, "check for standby for unknown", K(ret), K(snapshot), K(can_read), K(trans_version), K(is_determined_state), K(tmp_state_info), K(readable_scn));
          }
          break;
        }
        case ObTxState::INIT:
        case ObTxState::REDO_COMPLETE: {
          if (tmp_state_info.snapshot_version_ < snapshot) {
            break;
          } else if (tmp_state_info.version_ >= snapshot) {
            can_read = false;
            trans_version.set_min();
            is_determined_state = false;
            ret = OB_SUCCESS;
          }
          break;
        }
        case ObTxState::PREPARE: {
          if (tmp_state_info.version_ > snapshot) {
            can_read = false;
            trans_version.set_min();
            is_determined_state = false;
            ret = OB_SUCCESS;
          } else {
            version = MAX(version, tmp_state_info.version_);
          }
          break;
        }
        case ObTxState::ABORT: {
          can_read = false;
          trans_version.set_min();
          is_determined_state = true;
          ret = OB_SUCCESS;
          break;
        }
        case ObTxState::PRE_COMMIT:
        case ObTxState::COMMIT:
        case ObTxState::CLEAR: {
          if (tmp_state_info.version_ <= snapshot) {
            can_read = true;
          } else {
            can_read = false;
          }
          trans_version = tmp_state_info.version_;
          is_determined_state = true;
          ret = OB_SUCCESS;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
      }
    }
    if (count != 0 && OB_ERR_SHARED_LOCK_CONFLICT == ret && state == ObTxState::PREPARE && version <= snapshot) {
      can_read = true;
      trans_version = version;
      is_determined_state = true;
      ret = OB_SUCCESS;
    }
    if (count == 0 || (OB_ERR_SHARED_LOCK_CONFLICT == ret && min_snapshot <= snapshot)) {
      if (ask_state_info_interval_.reach()) {
        if (OB_SUCCESS != (tmp_ret = build_and_post_ask_state_msg_(snapshot))) {
          TRANS_LOG(WARN, "ask state from coord fail", K(ret), K(snapshot), KPC(this));
        }
      }
    }
  }
  TRANS_LOG(INFO, "check for standby", K(ret), K(snapshot), K(can_read), K(trans_version), K(is_determined_state), KPC(this));
  return ret;
}

int ObPartTransCtx::build_and_post_ask_state_msg_(const SCN &snapshot)
{
  int ret = OB_SUCCESS;
  if (is_root()) {
    if (!exec_info_.participants_.empty()) {
      handle_trans_ask_state_(snapshot);
    }
  } else {
    ObAskStateMsg msg;
    msg.snapshot_ = snapshot;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = trans_id_;
    msg.sender_addr_ = addr_;
    msg.sender_ = ls_id_;
    msg.request_id_ = ObTimeUtility::current_time();
    msg.cluster_id_ = cluster_id_;
    msg.receiver_ = exec_info_.upstream_;
    if (OB_FAIL(rpc_->post_msg(msg.receiver_, msg))) {
      TRANS_LOG(WARN, "post ask state msg fail", K(ret), K(msg), KPC(this));
      if (OB_LS_NOT_EXIST == ret) {
        ret = check_ls_state_(snapshot, msg.receiver_);
      }
    }
  }
  TRANS_LOG(INFO, "build and post ask state msg", K(ret), K(snapshot), KPC(this));
  return ret;
}

int ObPartTransCtx::check_ls_state_(const SCN &snapshot, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSExistState ls_state;
  if (OB_FAIL(ObLocationService::check_ls_exist(MTL_ID(), ls_id, ls_state))) {
    TRANS_LOG(WARN, "get ls state failed", K(ret));
  } else if (ls_state.is_uncreated()) {
    ObStateInfo state_info;
    state_info.ls_id_ = ls_id;
    state_info.snapshot_version_ = snapshot;
    state_info.state_ = ObTxState::INIT;
    state_info.version_ = snapshot;
    if (state_info_array_.empty()) {
      if (OB_FAIL(state_info_array_.push_back(state_info))) {
        TRANS_LOG(WARN, "push buck state info array fail", K(ret), K(state_info));
      }
    } else {
      bool is_contain = false;
      for (int j = 0; j<state_info_array_.count() && !is_contain; j++) {
        is_contain = state_info.ls_id_ == state_info_array_.at(j).ls_id_;
      }
      if (!is_contain) {
        if (OB_FAIL(state_info_array_.push_back(state_info))) {
          TRANS_LOG(WARN, "push back state info array fail", K(ret));
        }
      }
    }
  } else {
    ret = OB_TRANS_CTX_NOT_EXIST;
  }
  return ret;
}

int ObPartTransCtx::handle_trans_ask_state(const SCN &snapshot, ObAskStateRespMsg &resp)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (exec_info_.participants_.empty()) {
    // if coord not know any participants(before replay commit info log),
    // fill self state to resp
    ObStateInfo state_info;
    state_info.ls_id_ = ls_id_;
    state_info.state_ = exec_info_.state_;
    state_info.snapshot_version_ = snapshot;
    if (ObTxState::INIT == state_info.state_) {
      if (OB_FAIL(get_ls_replica_readable_scn_(state_info.ls_id_, state_info.version_))) {
        TRANS_LOG(WARN, "get replica readable scn failed", K(ret), K(state_info), K(snapshot));
      } else if (OB_FAIL(resp.state_info_array_.push_back(state_info))) {
        TRANS_LOG(WARN, "push back state info to resp msg failed", K(ret), K(snapshot), KPC(this));
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      TRANS_LOG(ERROR, "coord should not in other state befroe replay commit info log", K(ret),
                KPC(this));
    }
    TRANS_LOG(INFO, "coord not know any participants", K(ret), K(state_info), KPC(this));
  } else {
    handle_trans_ask_state_(snapshot);
    if (OB_FAIL(resp.state_info_array_.assign(state_info_array_))) {
      TRANS_LOG(WARN, "build ObAskStateRespMsg fail", K(ret), K(snapshot), KPC(this));
    }
  }
  TRANS_LOG(INFO, "handle ask state msg", K(ret), K(snapshot), KPC(this));
  return ret;
}

void ObPartTransCtx::handle_trans_ask_state_(const SCN &snapshot)
{
  if (snapshot > lastest_snapshot_) {
    build_and_post_collect_state_msg_(snapshot);
  } else if (snapshot <= lastest_snapshot_ && standby_part_collected_.num_members() != state_info_array_.count()) {
    if (refresh_state_info_interval_.reach()) {
      build_and_post_collect_state_msg_(snapshot);
    }
  } else {
    // do nothing
  }
}

void ObPartTransCtx::build_and_post_collect_state_msg_(const SCN &snapshot)
{
  int ret = OB_SUCCESS;
  ObCollectStateMsg msg;
  msg.snapshot_ = snapshot;
  msg.cluster_version_ = cluster_version_;
  msg.tenant_id_ = tenant_id_;
  msg.tx_id_ = trans_id_;
  msg.sender_addr_ = addr_;
  msg.sender_ = ls_id_;
  msg.request_id_ = ObTimeUtility::current_time();
  msg.cluster_id_ = cluster_id_;
  if (state_info_array_.empty() && OB_FAIL(set_state_info_array_())) {
    TRANS_LOG(WARN, "merge participants fail", K(ret), K(msg), KPC(this));
  }
  ARRAY_FOREACH(state_info_array_, i) {
    msg.receiver_ = state_info_array_.at(i).ls_id_;
    if (OB_FAIL(rpc_->post_msg(msg.receiver_, msg))) {
      TRANS_LOG(WARN, "post collect state msg fail", K(ret), K(msg), KPC(this));
      if (OB_LS_NOT_EXIST == ret) {
        ret = check_ls_state_(snapshot, msg.receiver_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    lastest_snapshot_ = snapshot;
    standby_part_collected_.clear_all();
  }
  TRANS_LOG(INFO, "build and post collect state msg", K(ret), K(snapshot), KPC(this));
}

int ObPartTransCtx::set_state_info_array_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (exec_info_.participants_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "participants array is empty", K(ret), KPC(this));
  } else {
    ObStateInfo state_info;
    ARRAY_FOREACH(exec_info_.participants_, i) {
      state_info.ls_id_ = exec_info_.participants_.at(i);
      if (OB_FAIL(state_info_array_.push_back(state_info))) {
        TRANS_LOG(WARN, "state info array push back fail", K(ret), K(state_info), KPC(this));
        break;
      }
    }
  }
  TRANS_LOG(INFO, "set state info array", K(ret), KPC(this));
  return ret;
}

int ObPartTransCtx::handle_trans_ask_state_resp(const ObAskStateRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (state_info_array_.empty()) {
    if (OB_FAIL(state_info_array_.assign(msg.state_info_array_))) {
      TRANS_LOG(WARN, "assign state info array fail", K(ret));
    }
  } else {
    bool is_contain = false;
    int j = 0;
    ARRAY_FOREACH(msg.state_info_array_, i) {
      for (j = 0, is_contain = false; j<state_info_array_.count() && !is_contain; j++) {
        is_contain = msg.state_info_array_.at(i).ls_id_ == state_info_array_.at(j).ls_id_;
      }
      if (!is_contain) {
        // maybe wrong
        if (OB_FAIL(state_info_array_.push_back(msg.state_info_array_.at(i)))) {
          TRANS_LOG(WARN, "push back state info array fail", K(ret));
        }
      } else if (state_info_array_.at(j-1).need_update(msg.state_info_array_.at(i))) {
        state_info_array_.at(j-1) = msg.state_info_array_.at(i);
      }
    }
  }
  TRANS_LOG(INFO, "handle trans ask state resp", K(ret), K(msg), KPC(this));
  return ret;
}

int ObPartTransCtx::handle_trans_collect_state(ObStateInfo &state_info, const SCN &snapshot)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    state_info.ls_id_ = ls_id_;
    bool need_loop = false;
    do {
      state_info.state_ = exec_info_.state_;
      switch (state_info.state_) {
        case ObTxState::INIT:
        case ObTxState::REDO_COMPLETE: {
          if (need_loop) {
            // The state is still init or redo complete at the second check, do nothing
            need_loop = false;
          } else if (OB_FAIL(get_ls_replica_readable_scn_(state_info.ls_id_, state_info.version_))) {
            TRANS_LOG(WARN, "get replica readable scn failed", K(ret), K(state_info), K(snapshot));
          } else if (snapshot <= state_info.version_) {
            need_loop = true;
          } else {
            need_loop = false;
          }
          break;
        }
        case ObTxState::ABORT: {
          state_info.version_.set_min();
          need_loop = false;
          break;
        }
        case ObTxState::COMMIT:
        case ObTxState::PRE_COMMIT:
        case ObTxState::CLEAR: {
          state_info.version_ = ctx_tx_data_.get_commit_version();
          need_loop = false;
          break;
        }
        case ObTxState::PREPARE: {
          state_info.version_ = exec_info_.prepare_version_;
          need_loop = false;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
      }
    } while(need_loop);
  }
  TRANS_LOG(INFO, "handle trans collect state", K(ret), K(state_info), K(snapshot), KPC(this));
  return ret;
}

int ObPartTransCtx::get_ls_replica_readable_scn_(const ObLSID &ls_id, SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr =  MTL(ObLSService *);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "log stream service is NULL", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::TRANS_MOD))) {
    TRANS_LOG(WARN, "get log stream failed", K(ret));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get ls failed", K(ret));
  } else if (OB_FAIL(ls->get_ls_replica_readable_scn(snapshot_version))) {
    TRANS_LOG(WARN, "get ls replica readable scn failed", K(ret), K(ls_id));
  } else {
    // do nothing
  }
  TRANS_LOG(INFO, "get ls replica readable scn", K(ret), K(snapshot_version), KPC(this));
  return ret;
}

int ObPartTransCtx::handle_trans_collect_state_resp(const ObCollectStateRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    bool is_contain = false;
    int i = 0;
    for (; i<state_info_array_.count() && !is_contain; i++) {
      is_contain = state_info_array_.at(i).ls_id_ == msg.state_info_.ls_id_;
    }
    if (!is_contain) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "state info array has wrong participiants", K(ret), K(msg), KPC(this));
    } else if (state_info_array_.at(i-1).need_update(msg.state_info_)) {
      state_info_array_.at(i-1) = msg.state_info_;
    }
    if (OB_SUCC(ret)) {
      standby_part_collected_.add_member(i-1);
    }
  }
  TRANS_LOG(INFO, "handle trans collect state resp", K(ret), K(msg), KPC(this));
  return ret;
}

int ObPartTransCtx::handle_ask_tx_state_for_4377(bool &is_alive)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    is_alive = false;
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
  } else {
    if (exec_info_.state_ == ObTxState::ABORT) {
      // Lost data read during transfer sources from reading aborted txn.
      // Because of the strong log synchronization semantic of transfer, we can
      // relay on the transfer src txn is already in a state of death with abort
      // log synchronized.
      is_alive = false;
    } else {
      is_alive = true;
    }
  }

  TRANS_LOG(INFO, "handle ask tx state for 4377", K(ret), KPC(this), K(is_alive));
  return ret;
}

void ObPartTransCtx::print_first_mvcc_callback_()
{
  mt_ctx_.print_first_mvcc_callback();
}

void ObPartTransCtx::post_keepalive_msg_(const int status)
{
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
  msg.status_ = status;
  int ret = OB_SUCCESS;
  if (OB_FAIL(rpc_->post_msg(exec_info_.scheduler_, msg))) {
    TRANS_LOG(WARN, "post tx keepalive msg fail", K(ret), K(msg), KPC(this));
  }
}

void ObPartTransCtx::notify_scheduler_tx_killed_(const int kill_reason)
{
  post_keepalive_msg_(kill_reason);
}

} // namespace transaction
} // namespace oceanbase
