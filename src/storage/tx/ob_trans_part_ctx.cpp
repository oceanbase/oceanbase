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
#include "lib/container/ob_array_helper.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_ts_mgr.h"
#include "ob_tx_log.h"
#include "ob_tx_msg.h"
#include "ob_tx_redo_submitter.h"
#include "lib/worker.h"
#include "share/rc/ob_context.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_leak_checker.h"
#include "storage/tx/ob_multi_data_source_printer.h"
#include "share/ob_alive_server_tracer.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#define NEED_MDS_REGISTER_DEFINE
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE
#include "storage/tablet/ob_tablet_transfer_tx_ctx.h"
#include "storage/tx/ob_ctx_tx_data.h"
#include "storage/multi_data_source/runtime_utility/mds_tlocal_info.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "logservice/ob_log_service.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/tx/ob_tx_log_operator.h"

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
                         const uint32_t associated_session_id,
                         const ObTransID &trans_id,
                         const int64_t trans_expired_time,
                         const ObLSID &ls_id,
                         const uint64_t cluster_version,
                         ObTransService *trans_service,
                         const uint64_t cluster_id,
                         const int64_t epoch,
                         ObLSTxCtxMgr *ls_ctx_mgr,
                         const bool for_replay,
                         const PartCtxSource ctx_source,
                         ObXATransID xid)
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
             || OB_UNLIKELY(trans_expired_time <= 0) || OB_UNLIKELY(cluster_version < 0)
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
    } else if (OB_FAIL(ctx_tx_data_.init(trans_expired_time, ls_ctx_mgr, trans_id))) {
      TRANS_LOG(WARN, "init ctx tx data failed",K(ret), K(trans_id), K(ls_id));
    } else if (OB_FAIL(mds_cache_.init(tenant_id, ls_id, trans_id))) {
      TRANS_LOG(WARN, "init mds cache failed", K(ret), K(trans_id), K(ls_id));
    }
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    session_id_ = session_id;
    associated_session_id_ = associated_session_id;
    addr_ = trans_service->get_server();
    trans_id_ = trans_id;
    trans_expired_time_ = trans_expired_time;
    ctx_create_time_ = ObClockGenerator::getClock();
    ctx_source_ = ctx_source;
    cluster_version_accurate_ = cluster_version > 0;
    cluster_version_ = cluster_version ?: LAST_BARRIER_DATA_VERSION;
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

    if (!xid.empty()) {
      exec_info_.xid_ = xid;
    }

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
#ifdef ENABLE_DEBUG_LOG
    tlog_ = &trace_log_;
#endif
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

int ObPartTransCtx::init_for_transfer_move(const ObTxCtxMoveArg &arg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (OB_FAIL(load_tx_op_if_exist_())) {
    TRANS_LOG(WARN, "load_tx_op failed", KR(ret), KPC(this));
  } else {
    exec_info_.is_sub2pc_ = arg.is_sub2pc_;
    mt_ctx_.set_trans_version(arg.trans_version_);
    exec_info_.trans_type_ = (arg.tx_state_ == ObTxState::INIT) ? TransType::SP_TRANS : TransType::DIST_TRANS;
    if (arg.tx_state_ >= ObTxState::PREPARE) {
      exec_info_.prepare_version_ = arg.prepare_version_;
      ctx_tx_data_.set_commit_version(arg.commit_version_);
    }
    set_durable_state_(arg.tx_state_);
  }
  return ret;
}

int ObPartTransCtx::init_memtable_ctx_(const uint64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mt_ctx_.init(tenant_id))) {
    TRANS_LOG(WARN, "memtable context init fail", KR(ret));
  } else if (OB_FAIL(mt_ctx_.enable_lock_table(ls_tx_ctx_mgr_))) {
    TRANS_LOG(WARN, "enable_lock_table fail", K(ret), K(ls_id));
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
    if (trans_service_ != NULL && NULL != trans_service_->get_defensive_check_mgr()) {
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

    (void)check_dli_batch_completed_(ObTxLogType::TX_CLEAR_LOG);

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

    exec_info_.destroy(mds_cache_);

    mds_cache_.destroy();

    if (mds_cache_.is_mem_leak()) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "mds memory leak!", K(trans_id_), K(ls_id_),
                    K(mds_cache_), K(exec_info_), K(ctx_tx_data_), K(create_ctx_scn_),
                    K(ctx_source_), K(ctx_create_time_));
      FORCE_PRINT_TRACE(tlog_, "[check mds mem leak] ");
    }

    ctx_tx_data_.destroy();

    big_segment_info_.reset();

    reset_log_cbs_();

    if (NULL != tlog_) {
      print_trace_log_if_necessary_();
      tlog_ = NULL;
    }

    timeout_task_.destroy();
    trace_info_.reset();
    block_frozen_memtable_ = nullptr;

    last_rollback_to_request_id_ = 0;
    last_rollback_to_timestamp_ = 0;
    last_transfer_in_timestamp_ = 0;

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
  associated_session_id_ = 0;
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
  create_ctx_scn_.reset();
  ctx_source_ = PartCtxSource::UNKNOWN;
  replay_completeness_.reset();
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
  transfer_deleted_ = false;
  last_rollback_to_request_id_ = 0;
  last_rollback_to_timestamp_ = 0;
  last_transfer_in_timestamp_ = 0;
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

int ObPartTransCtx::extend_log_cbs_(ObTxLogCb *&cb)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
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
    }
    if (OB_FAIL(ret)) {
      reserve_allocator_.free(ptr);
      cb = NULL;
    }
  }
  return ret;
}

void ObPartTransCtx::reset_log_cb_list_(common::ObDList<ObTxLogCb> &cb_list)
{
  ObTxLogCb *cb = NULL;
  while (OB_NOT_NULL(cb = cb_list.remove_first())) {
    const bool is_dynamic = cb->is_dynamic();
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
    replay_completeness_.set(true);
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
            LOG_DBA_ERROR_V2(OB_TRANS_COMMIT_COST_TOO_MUCH_TIME, tmp_ret,
                             "Transaction commit cost too much time. ",
                             "trans_id is ", trans_id_, ". ls_id is ", ls_id_, ". ",
                             "[suggestion] You can query GV$OB_PROCESSLIST view to get more information.");
          }
        } else {
          if (now >= ctx_create_time_ + OB_TRANS_WARN_USE_TIME) {
            print_first_mvcc_callback_();
            tmp_ret = OB_TRANS_LIVE_TOO_MUCH_TIME;
            LOG_DBA_WARN_V2(OB_TRANS_LIVE_TOO_LONG, tmp_ret,
                            "Transaction live too long without commit or abort. ",
                            "trans_id is ", trans_id_, ". ls_id is ", ls_id_, ". ",
                            "[suggestion] This may be normal and simply because the client hasn't executed the 'commit' command yet. ",
                            "You can query GV$OB_PROCESSLIST view to get more information ",
                            "and confirm whether you need to submit this transaction.");
          }
        }
      }

      if (exec_info_.is_dup_tx_) {
        if (!is_sub2pc() && ObTxState::REDO_COMPLETE == exec_info_.state_) {
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
        } else if (ObTxState::PREPARE > get_upstream_state()) {
          ObTxState next_state = (is_sub2pc() || exec_info_.is_dup_tx_) ?
                                    ObTxState::REDO_COMPLETE :
                                    ObTxState::PREPARE;
          if (OB_FAIL(drive_self_2pc_phase(next_state))) {
            TRANS_LOG(WARN, "drive to next phase failed", K(ret), K(*this), K(next_state));
          } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::handle_timeout())) {
            TRANS_LOG(WARN, "handle 2pc timeout failed", KR(ret), KPC(this));
          }
        } else if (OB_FAIL(ObTxCycleTwoPhaseCommitter::handle_timeout())) {
          TRANS_LOG(WARN, "handle 2pc timeout failed", KR(ret), KPC(this));
        }
      }

      // retry submit abort log for local tx abort
      //
      // NOTE: due to some local tx may set to dist-trans if it request 2pc msg
      //       while choose abort itself forcedly, hence need add the second condition
      if (!is_follower_() && (is_local_tx_() || sub_state_.is_force_abort())
          && get_upstream_state() == ObTxState::ABORT
          && get_upstream_state() != get_downstream_state()) {
        if (OB_FAIL(compensate_abort_log_())) {
          TRANS_LOG(WARN, "compensate abort log failed", KR(ret), KPC(this));
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

int ObPartTransCtx::kill(const KillTransArg &arg, ObTxCommitCallback *&cb_list)
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
            if (OB_TMP_FAIL(prepare_commit_cb_for_role_change_(cb_param, cb_list))) {
              TRANS_LOG(WARN, "prepare commit cb fail", K(tmp_ret), K(cb_param), KPC(this));
              ret = (ret == OB_SUCCESS) ? tmp_ret : ret;
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
int ObPartTransCtx::commit(const ObTxCommitParts &parts,
                           const MonotonicTs &commit_time,
                           const int64_t &expire_ts,
                           const common::ObString &app_trace_info,
                           const int64_t &request_id)
{
  TRANS_LOG(DEBUG, "tx.commit", K(parts), K(trans_id_), K(ls_id_));
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
    TRANS_LOG(WARN, "tx is 2pc logging", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_2pc_blocking())) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "tx is 2pc blocking", KR(ret), KPC(this));
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
    if (exec_info_.participants_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "the size of participant is 0 when commit", KPC(this));
    } else if (exec_info_.participants_.count() == 1 &&
               exec_info_.participants_[0] == ls_id_ &&
               0 == exec_info_.intermediate_participants_.count() &&
               !exec_info_.is_dup_tx_) {
      exec_info_.trans_type_ = TransType::SP_TRANS;
      can_elr_ = (trans_service_->get_tx_elr_util().is_can_tenant_elr() ? true : false);
      if (OB_FAIL(one_phase_commit_())) {
        TRANS_LOG(WARN, "start sp coimit fail", K(ret), KPC(this));
      }

      if ((OB_SUCC(ret) || OB_EAGAIN == ret) && exec_info_.is_dup_tx_) {
        set_2pc_upstream_(ls_id_);
        if (OB_FAIL(two_phase_commit())) {
          TRANS_LOG(WARN, "start dist commit for dup table fail", K(ret), KPC(this));
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          }
        }
      }

    } else {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
      // set 2pc upstream to self
      set_2pc_upstream_(ls_id_);
      if (OB_FAIL(two_phase_commit())) {
        TRANS_LOG(WARN, "start dist commit fail", K(ret), KPC(this));
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        }
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
  if (OB_FAIL(ret) && OB_EAGAIN != ret && OB_TRANS_COMMITED != ret) {
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
    // should not set iterator is ready here,
    // because it may iterate other tx_ctx then
  }

  return ret;
}

int ObPartTransCtx::iterate_tx_lock_stat(ObTxLockStatIterator &iter)
{
  int ret = OB_SUCCESS;
  ObMemtableKeyArray memtable_key_info_arr;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_memtable_key_arr(memtable_key_info_arr))) {
    TRANS_LOG(WARN, "get memtable key arr fail", KR(ret), K(memtable_key_info_arr));
  } else {
    // If the row has been dumped into sstable, we can not get the
    // memtable key info since the callback of it has been dropped.
    // So we need to judge whether the transaction has been dumped
    // into sstable here. Futhermore, we need to fitler out ratain
    // transactions by !tx_ctx->is_exiting().
    if (memtable_key_info_arr.empty() && !is_exiting() && get_memtable_ctx()->maybe_has_undecided_callback()) {
      ObMemtableKeyInfo key_info;
      memtable_key_info_arr.push_back(key_info);
    }
    int64_t count = memtable_key_info_arr.count();
    for (int i = 0; OB_SUCC(ret) && i < count; i++) {
      ObTxLockStat tx_lock_stat;
      if (OB_FAIL(tx_lock_stat.init(get_addr(),
                                    get_tenant_id(),
                                    get_ls_id(),
                                    memtable_key_info_arr.at(i),
                                    get_session_id(),
                                    0,
                                    get_trans_id(),
                                    get_ctx_create_time(),
                                    get_trans_expired_time()))) {
        TRANS_LOG(WARN, "trans lock stat init fail", KR(ret), KPC(this), K(memtable_key_info_arr.at(i)));
      } else if (OB_FAIL(iter.push(tx_lock_stat))) {
        TRANS_LOG(WARN, "tx_lock_stat_iter push item fail", KR(ret), K(tx_lock_stat));
      } else {
        // do nothing
      }
    }
  }

  return ret;
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
                                         const uint64_t checksum)
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
  return state >= ObTxState::PREPARE || (is_sub2pc() && state >= ObTxState::REDO_COMPLETE);
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
  return exec_info_.max_applying_log_ts_.is_valid() ||
    // for ctx created by parallel replay, and no serial log replayed
    // the max_applying_log_ts_ has not been set, in this case use
    // replay_completeness to distinguish with situations on leader
    (is_follower_() && replay_completeness_.is_unknown());
}

int ObPartTransCtx::gts_callback_interrupted(const int errcode,
    const share::ObLSID target_ls_id)
{
  int ret = OB_SUCCESS;

  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(ERROR, "ObPartTransCtx not inited", K(ret));
    } else if (OB_LS_OFFLINE == errcode) {
      if (target_ls_id != ls_id_) {
        ret = OB_EAGAIN;
      } else {
        need_revert_ctx = true;
        sub_state_.clear_gts_waiting();
        TRANS_LOG(INFO, "transaction is interruputed gts callback", KR(ret), K(errcode), "context", *this);
      }
    } else {
      // for OB_TENANT_NOT_EXIST
      need_revert_ctx = true;
      sub_state_.clear_gts_waiting();
      TRANS_LOG(INFO, "transaction is interruputed gts callback", KR(ret), K(errcode), "context", *this);
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
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
    TRANS_LOG(DEBUG, "ObPartTransCtx get_gts_callback begin", KPC(this));
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
      const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
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
        } else if (is_2pc_blocking()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(restart_2pc_trans_timer_())) {
            TRANS_LOG(WARN, "fail to restart 2pc trans timer", K(tmp_ret), KPC(this));
          } else {
            TRANS_LOG(WARN, "need not drive 2pc phase when 2pc blocking", K(ret), KPC(this));
          }
        } else if (OB_FAIL(submit_log_impl_(ObTxLogType::TX_COMMIT_LOG))) {
          TRANS_LOG(WARN, "submit commit log in gts callback failed", K(ret), KPC(this));
        }
      } else {
        const SCN local_prepare_version = SCN::max(gts, max_read_ts);
        // should not overwrite the prepare verison of other participants
        exec_info_.prepare_version_ = SCN::max(local_prepare_version, exec_info_.prepare_version_);

        if (is_2pc_blocking()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(restart_2pc_trans_timer_())) {
            TRANS_LOG(WARN, "fail to restart 2pc trans timer", K(tmp_ret), KPC(this));
          } else {
            TRANS_LOG(WARN, "need not drive 2pc phase when 2pc blocking", K(ret), KPC(this));
          }
        } else if (get_upstream_state() <= ObTxState::PREPARE) {
          int tmp_ret = OB_SUCCESS;
          if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
            TRANS_LOG(WARN, "drive into prepare phase failed in gts callback", K(ret), KPC(this));
          } else if (OB_TMP_FAIL(ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_())) {
            TRANS_LOG(WARN, "post prepare request failed", K(ret), KPC(this));
          }
        }
      }

      need_revert_ctx = true;
    }
    REC_TRANS_TRACE_EXT2(tlog_, get_gts_callback, OB_Y(ret), OB_ID(srr), srr.mts_, OB_Y(gts),  OB_ID(ref), get_ref());

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
    TRANS_LOG(DEBUG, "ObPartTransCtx get_gts_callback end", KPC(this));
  }

  if (need_revert_ctx) {
    ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
  }

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
  // strong memory barrier on ARM
  WEAK_BARRIER();

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
    TRANS_LOG(INFO, "[TRANS GC] participant will **abort** itself due to scheduler has quit", KPC(this));
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
      TRANS_LOG(DEBUG, "don't need ask scheduler status", K(ret), KPC(this));
    } else if (exec_info_.scheduler_ != addr_ && OB_FAIL(check_rs_scheduler_is_alive_(is_alive))) {
      TRANS_LOG(WARN, "check rs scheduler is alive error", KR(ret), K(is_alive), KPC(this));
      // scheduler已宕机
    } else if (!is_alive) {
      TRANS_LOG(WARN, "[TRANS GC] scheduler server is not alive, participant will GC", KPC(this));
      if (OB_FAIL(gc_ctx_())) {
        TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), KPC(this));
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
  } else if (OB_FAIL(ctx_tx_data_.recover_tx_data(ctx_info.tx_data_guard_.tx_data()))) {
    TRANS_LOG(WARN, "recover tx data failed", K(ret), K(ctx_tx_data_));
  } else if (OB_FAIL(exec_info_.assign(ctx_info.exec_info_))) {
    TRANS_LOG(WARN, "exec_info assign error", K(ret), K(ctx_info));
  } else {
    trans_id_ = ctx_info.tx_id_;
    ls_id_ = ctx_info.ls_id_;
    // set upstream state when recover tx ctx table
    set_upstream_state(get_downstream_state());

    if (ObTxState::REDO_COMPLETE == get_downstream_state()) {
      sub_state_.set_info_log_submitted();
    }
    if (exec_info_.prepare_version_.is_valid()) {
      mt_ctx_.set_trans_version(exec_info_.prepare_version_);
    }
    if (exec_info_.is_transfer_blocking_) {
      sub_state_.set_transfer_blocking();
    }
    exec_info_.multi_data_source_.reset();
    exec_info_.mds_buffer_ctx_array_.reset();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(deep_copy_mds_array_(ctx_info.exec_info_.multi_data_source_, _unused_))) {
      TRANS_LOG(WARN, "deep copy ctx_info mds_array failed", K(ret));
    } else if (exec_info_.need_checksum_ &&
               OB_FAIL(mt_ctx_.update_checksum(exec_info_.checksum_,
                                               exec_info_.checksum_scn_))) {
      TRANS_LOG(WARN, "recover checksum failed", K(ret), KPC(this), K(ctx_info));
    } else if (!is_local_tx_() && OB_FAIL(ObTxCycleTwoPhaseCommitter::recover_from_tx_table())) {
      TRANS_LOG(ERROR, "recover_from_tx_table failed", K(ret), KPC(this));
    } else {
      is_ctx_table_merged_ = true;
      replay_completeness_.set(true);
    }

    if (OB_SUCC(ret) && !exec_info_.need_checksum_) {
      mt_ctx_.set_skip_checksum_calc();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(recover_ls_transfer_status_())) {
      TRANS_LOG(WARN, "recover ls transfer status failed", KR(ret));
    } else {
      if (exec_info_.exec_epoch_ > 0) {
        epoch_ = exec_info_.exec_epoch_;
      }
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
      } else if (OB_FAIL(dup_table_before_preapre_(
                     exec_info_.max_applied_log_ts_, true /*after_redo_completed*/,
                     ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
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
      if (exec_info_.serial_final_scn_.is_valid()) {
        recovery_parallel_logging_();
      }
      if ((ObTxState::COMMIT == exec_info_.state_
           && exec_info_.max_applying_log_ts_ == exec_info_.max_applied_log_ts_)
          || ObTxState::CLEAR == exec_info_.state_) {
        ls_tx_ctx_mgr_->update_max_replay_commit_version(ctx_tx_data_.get_commit_version());
        //update max_commit_ts for dup table because of migrate or recover
        MTL(ObTransService *)
            ->get_tx_version_mgr()
            .update_max_commit_ts(ctx_tx_data_.get_commit_version(), false);
      }
      create_ctx_scn_ = exec_info_.max_applying_log_ts_;

      if(!exec_info_.transfer_parts_.empty()) {
        ctx_source_ = PartCtxSource::TRANSFER_RECOVER;
      }
    }
    TRANS_LOG(INFO, "[TRANS RECOVERY] recover tx ctx table info succeed", K(ret), KPC(this), K(ctx_info));
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
    // 3. Tx ctx replay incomplete, skip
  } else if (replay_completeness_.is_incomplete()) {
    // NB: we need refresh rec log ts for incomplete replay ctx
    if (OB_FAIL(refresh_rec_log_ts_())) {
      TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
      TRANS_LOG(INFO, "tx ctx is an incomplete replay ctx", K(ret), KPC(this));
    }
    // ctx created by replay redo of parallel replay, skip
  } else if (replay_completeness_.is_unknown()
             && !exec_info_.is_empty_ctx_created_by_transfer_) {
    if (OB_FAIL(refresh_rec_log_ts_())) {
      TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
      TRANS_LOG(INFO, "tx ctx replay completeness unknown, skip checkpoint", K(ret), KPC(this));
    }
  // 4. Fetch the current state of the tx ctx table
  } else if (OB_FAIL(get_tx_ctx_table_info_(ctx_info))) {
    TRANS_LOG(WARN, "get tx ctx table info failed", K(ret), K(*this));
  } else if (OB_UNLIKELY(!ctx_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx ctx info invalid", K(ret), K(ctx_info));
  // 5. Refresh the rec_log_ts for the next checkpoint
  } else if (OB_FAIL(refresh_rec_log_ts_())) {
    TRANS_LOG(WARN, "refresh rec log ts failed", K(ret), K(*this));
  } else {
    SpinRLockManualGuard tx_op_guard;
    if (ctx_info.tx_data_guard_.tx_data()->op_guard_.is_valid()) {
      tx_op_guard.lock(ctx_info.tx_data_guard_.tx_data()->op_guard_->get_lock());
    }
    // 6. Do serialize
    int64_t pos = 0;
    serialize_size = ctx_info.get_serialize_size();
    if (OB_FAIL(buffer.reserve(serialize_size))) {
      TRANS_LOG(WARN, "Failed to reserve local buffer", KR(ret));
    } else if (OB_FAIL(ctx_info.serialize(buffer.get_ptr(), serialize_size, pos))) {
      TRANS_LOG(WARN, "failed to serialize ctx_info", KR(ret), K(ctx_info), K(pos));
    } else {
      is_ctx_table_merged_ = true;
      serialize_size = pos;
    }
  }
  TRANS_LOG(INFO, "[TRANS CHECKPOINT] checkpoint trans ctx", K(ret), K_(trans_id), K_(ls_id), KP(this));
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
  } else if (OB_FAIL(mt_ctx_.remove_callback_for_uncommited_txn(memtable_set))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(mt_ctx_),
              K(memtable_set), K(exec_info_.max_applied_log_ts_));
  }

  return ret;
}

// the semantic of submit redo for freeze is
// should flush all redos bellow specified freeze_clock (inclusive)
// otherwise, need return some error to caller to indicate need retry
int ObPartTransCtx::submit_redo_log_for_freeze(const uint32_t freeze_clock)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(TRACE, "", K_(trans_id), K_(ls_id));
  ObTimeGuard tg("submit_redo_for_freeze_log", 100000);
  bool submitted = false;
  bool need_submit = fast_check_need_submit_redo_for_freeze_();
  if (need_submit) {
    CtxLockGuard guard(lock_);
    tg.click();
    ret = submit_redo_log_for_freeze_(submitted, freeze_clock);
    tg.click();
    if (submitted) {
      REC_TRANS_TRACE_EXT2(tlog_, submit_log_for_freeze, OB_Y(ret),
                           OB_ID(used), tg.get_diff(), OB_ID(ref), get_ref());
    }
    if (OB_TRANS_HAS_DECIDED == ret || OB_BLOCK_FROZEN == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObPartTransCtx::submit_redo_after_write(const bool force, const ObTxSEQ &write_seq_no)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(TRACE, "submit_redo_after_write", K(force), K(write_seq_no), K_(trans_id), K_(ls_id),
            K(mt_ctx_.get_pending_log_size()));
  ObTimeGuard tg("submit_redo_for_after_write", 100000);
  if (force || mt_ctx_.pending_log_size_too_large(write_seq_no)) {
    bool parallel_logging = false;
#define LOAD_PARALLEL_LOGGING parallel_logging = exec_info_.serial_final_scn_.atomic_load().is_valid()
    LOAD_PARALLEL_LOGGING;
    if (!parallel_logging) {
      int submitted_cnt = 0;
      if (force || OB_SUCCESS == lock_.try_lock()) {
        CtxLockGuard guard(lock_, force /* need lock */);
        // double check parallel_logging is on
        LOAD_PARALLEL_LOGGING;
        if (!parallel_logging) {
          ret = serial_submit_redo_after_write_(submitted_cnt);
        }
      }
      if (submitted_cnt > 0 && OB_EAGAIN == ret) {
        // has remains, try fill after switch to parallel logging
        LOAD_PARALLEL_LOGGING;
      }
    }
#undef LOAD_PARALLEL_LOGGING
    tg.click("serial_log");
    if (parallel_logging && OB_SUCC(lock_.try_rdlock_flush_redo())) {
      if (OB_SUCC(check_can_submit_redo_())) {
        if (is_committing_()) {
          ret = force ? OB_TRANS_HAS_DECIDED : OB_SUCCESS;
        } else {
          ObTxRedoSubmitter submitter(*this, mt_ctx_);
          if (OB_FAIL(submitter.parallel_submit(write_seq_no))) {
            if (!force && (OB_ITER_END == ret          // blocked by others, current remains
                           || OB_NEED_RETRY == ret     // acquire lock failed
                           )) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      lock_.unlock_flush_redo();
    }
    if (!force && (OB_TRANS_HAS_DECIDED == ret // do committing
                   || OB_BLOCK_FROZEN == ret   // memtable logging blocked
                   || OB_EAGAIN == ret         // partial submitted or submit to log-service fail
                   )) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartTransCtx::serial_submit_redo_after_write_(int &submitted_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(check_can_submit_redo_())) {
    int64_t before_submit_pending_size = mt_ctx_.get_pending_log_size();
    bool should_switch = should_switch_to_parallel_logging_();
    ObTxRedoSubmitter submitter(*this, mt_ctx_);
    ret = submitter.serial_submit(should_switch);
    submitted_cnt = submitter.get_submitted_cnt();
    if (should_switch && submitted_cnt > 0) {
      const share::SCN serial_final_scn = submitter.get_submitted_scn();
      int tmp_ret = switch_to_parallel_logging_(serial_final_scn, exec_info_.max_submitted_seq_no_);
      TRANS_LOG(INFO, "**leader switch to parallel logging**", K(tmp_ret),
                K_(ls_id), K_(trans_id),
                K(serial_final_scn),
                "serial_final_seq_no", exec_info_.serial_final_seq_no_,
                K(before_submit_pending_size),
                "curr_pending_size", mt_ctx_.get_pending_log_size());
    }
  }
  return ret;
}

bool ObPartTransCtx::should_switch_to_parallel_logging_()
{
  bool ok = false;
  if (GCONF._enable_parallel_redo_logging) {
    const int64_t switch_size = GCONF._parallel_redo_logging_trigger;
    ok = pending_write_ > 1 && mt_ctx_.get_pending_log_size() > switch_size;
#ifdef ENABLE_DEBUG_LOG
    if (!ok) {
      ok = trans_id_ % 5 == 0;  // force 20% transaction go parallel logging
    }
#endif
  }
 return ok;
}

int ObPartTransCtx::check_can_submit_redo_()
{
  int ret = OB_SUCCESS;
  bool is_tx_committing = ObTxState::INIT != get_downstream_state();
  bool final_log_submitting = final_log_cb_.is_valid();

  if (is_tx_committing
      ||final_log_submitting
      || is_force_abort_logging_()) {
    ret = OB_TRANS_HAS_DECIDED;
  }
  return ret;
}

// Concurrency safe annotation:
// init log_block_ is an local operation
// prepare_log_cb_ is protected by `log_cb_lock_`
int ObPartTransCtx::prepare_for_submit_redo(ObTxLogCb *&log_cb,
                                            ObTxLogBlock &log_block,
                                            const bool serial_final)
{
  int ret = OB_SUCCESS;
  if (!log_block.is_inited() && OB_FAIL(init_log_block_(log_block, ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE, serial_final))) {
    TRANS_LOG(WARN, "init log block fail", K(ret));
  } else if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb)) && OB_TX_NOLOGCB != ret) {
    TRANS_LOG(WARN, "prepare log_cb fail", K(ret));
  }
  return ret;
}

int ObPartTransCtx::submit_redo_log_for_freeze_(bool &submitted, const uint32_t freeze_clock)
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, true);
  if (OB_SUCC(check_can_submit_redo_())) {
    ObTxRedoSubmitter submitter(*this, mt_ctx_);
    if (OB_FAIL(submitter.submit_for_freeze(freeze_clock, true /*display blocked info*/))) {
      if (OB_BLOCK_FROZEN != ret) {
        TRANS_LOG(WARN, "fail to submit redo log for freeze", K(ret));
        // for some error, txn will be aborted immediately
        handle_submit_log_err_(ObTxLogType::TX_REDO_LOG, ret);
      }
    }
    submitted = submitter.get_submitted_cnt() > 0;
  }
  if (OB_SUCC(ret) || OB_BLOCK_FROZEN == ret) {
    ret = submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG);
  }
  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, false);
  return ret;
}

bool ObPartTransCtx::fast_check_need_submit_redo_for_freeze_() const
{
  bool has_pending_log = true;
  bool blocked = false;
  if (OB_SUCCESS == lock_.try_wrlock_flush_redo()) {
    blocked = mt_ctx_.is_logging_blocked(has_pending_log);
    lock_.unlock_flush_redo();
  }
  return has_pending_log && !blocked;
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
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(restart_2pc_trans_timer_())) {
      TRANS_LOG(WARN, "restart_2pc_trans_timer_ for submit abort log fail",
        KR(ret), KR(tmp_ret), KPC(this));
    }
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
  part_trans_action_ = ObPartTransAction::ABORT;
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
  // We need put abort_op into tx_data before trans_end to promise ctx_tx_data is writeable
  } else if (!commit && end_scn.is_valid() && OB_FAIL(ctx_tx_data_.add_abort_op(end_scn))) {
    TRANS_LOG(WARN, "add tx data abort_op failed", K(ret), KPC(this));
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

  const int64_t LOG_CB_ON_SUCC_TIME_LIMIT = 100 * 1000;
  share::SCN max_cost_cb_scn;
  int64_t max_cost_cb_time = 0;
  int64_t skip_on_succ_cnt = 0;
  int64_t invoke_on_succ_cnt = 0;
  int64_t invoke_on_succ_time = 0;
  int64_t submit_record_log_time = 0;
  int64_t fast_commit_time = 0;
  int64_t on_succ_ctx_lock_hold_time = 0;
  int64_t try_submit_next_log_cost_time = 0;

  int64_t log_sync_used_time = 0;
  int64_t ctx_lock_wait_time = 0;

  bool handle_fast_commit = false;
  bool try_submit_next_log = false;
  bool need_return_log_cb = false;
  {
    // allow fill redo concurrently with log callback
    CtxLockGuard guard(lock_, is_committing_() ? CtxLockGuard::MODE::ALL : CtxLockGuard::MODE::CTX);

    log_sync_used_time = cur_ts - log_cb->get_submit_ts();
    ObTransStatistic::get_instance().add_clog_sync_time(tenant_id_, log_sync_used_time);
    ObTransStatistic::get_instance().add_clog_sync_count(tenant_id_, 1);
    ctx_lock_wait_time = guard.get_lock_acquire_used_time();
    if (log_sync_used_time + ctx_lock_wait_time >= ObServerConfig::get_instance().clog_sync_time_warn_threshold) {
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "transaction log sync use too much time", KPC(log_cb),
                    K(log_sync_used_time), K(ctx_lock_wait_time));
    }
    if (log_cb->get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), KPC(this));
      print_trace_log_();
#ifdef ENABLE_DEBUG_LOG
      usleep(5000);
      ob_abort();
#endif
    }
    if (log_cb->is_callbacked()) {
      skip_on_succ_cnt++;
#ifndef NDEBUG
      TRANS_LOG(INFO, "cb has been callbacked", KPC(log_cb));
#endif
      busy_cbs_.remove(log_cb);
      return_log_cb_(log_cb);
    } else if (is_exiting_) {
      skip_on_succ_cnt++;
      // the TxCtx maybe has been killed forcedly by background GC thread
      // the log_cb process has been skipped
      if (sub_state_.is_force_abort()) {
        TRANS_LOG(WARN, "ctx has been aborted forcedly before log sync successfully", KPC(this));
        print_trace_log_();
        busy_cbs_.remove(log_cb);
        return_log_cb_(log_cb);
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "callback was missed when tx ctx exiting", K(ret), KPC(log_cb), KPC(this));
        print_trace_log_();
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
    } else {
      // save the first error code
      int save_ret = OB_SUCCESS;
      ObTxLogCb *cur_cb = busy_cbs_.get_first();
      // process all preceding log_cbs
      for (int64_t i = 0; i < busy_cbs_.get_size(); i++) {
        if (cur_cb->is_callbacked()) {
          skip_on_succ_cnt++;
          // do nothing
        } else {
          invoke_on_succ_cnt++;
          const int64_t before_invoke_ts = ObTimeUtility::fast_current_time();
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
          const int64_t after_invoke_ts = ObTimeUtility::fast_current_time();
          if (after_invoke_ts - before_invoke_ts > max_cost_cb_time) {
            max_cost_cb_time = after_invoke_ts - before_invoke_ts;
            max_cost_cb_scn = log_cb->get_log_ts();
          }
          if (after_invoke_ts - before_invoke_ts > LOG_CB_ON_SUCC_TIME_LIMIT) {
            TRANS_LOG(WARN, "invoke on_succ cost too much time", K(ret), K(trans_id_), K(ls_id_), K(cur_cb),
                      K(log_cb));
          }
          invoke_on_succ_time += (after_invoke_ts - before_invoke_ts);
        }
        if (cur_cb == log_cb) {
          break;
        } else {
          cur_cb = cur_cb->get_next();
        }
      }
      if (cur_cb != log_cb) {
        ob_abort();
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected log callback", K(ret), K(*this), K(*cur_cb), K(*log_cb));
      } else {
        // return first error code
        ret = save_ret;
      }
      // try submit record log under CtxLock
      if (need_record_log_()) {
        // ignore error
        const int64_t before_submit_record_ts = ObTimeUtility::fast_current_time();
        if (OB_SUCCESS != (tmp_ret = submit_record_log_())) {
          TRANS_LOG(WARN, "failed to submit record log", K(tmp_ret), K(*this));
        }
        submit_record_log_time = ObTimeUtility::fast_current_time() -  before_submit_record_ts;
      }
      handle_fast_commit = !(sub_state_.is_state_log_submitted() || log_cb->get_callbacks().count() == 0);
      try_submit_next_log = !ObTxLogTypeChecker::is_state_log(log_cb->get_last_log_type()) && is_committing_();
      busy_cbs_.remove(log_cb);
      need_return_log_cb = true;
    }

    on_succ_ctx_lock_hold_time = ObTimeUtility::fast_current_time() - guard.get_hold_ts();
  }
  // let fast commit out of ctx's lock, because it is time consuming in calculating checksum
  if (handle_fast_commit) {
    // acquire REDO_FLUSH_READ LOCK, which allow other thread flush redo
    // but disable other manage operation on ctx
    // FIXME: acquire CTX's READ lock maybe better
    const int64_t before_fast_commit_ts = ObTimeUtility::fast_current_time();
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::REDO_FLUSH_R);
    mt_ctx_.remove_callbacks_for_fast_commit(log_cb->get_callbacks());
    fast_commit_time = ObTimeUtility::fast_current_time() - before_fast_commit_ts;
  }
  if (need_return_log_cb) {
    return_log_cb_(log_cb);
  }
  // try submit log if txn is in commit phase
  if (try_submit_next_log) {
    // in commiting, acquire CTX lock is enough, because redo flushing must finished
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::CTX);
    try_submit_next_log_(false);
  }

  if (ObTimeUtility::fast_current_time() - cur_ts > LOG_CB_ON_SUCC_TIME_LIMIT) {
    TRANS_LOG(WARN, "on_success cost too much time", K(ret), K(trans_id_), K(ls_id_),
              K(max_cost_cb_scn), K(max_cost_cb_time), K(skip_on_succ_cnt), K(invoke_on_succ_cnt),
              K(invoke_on_succ_time), K(submit_record_log_time), K(fast_commit_time),
              K(on_succ_ctx_lock_hold_time), K(try_submit_next_log_cost_time), K(log_sync_used_time),
              K(ctx_lock_wait_time));
  }

  if (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this))) {
    TRANS_LOG(ERROR, "release ctx ref failed", KR(tmp_ret));
  }

  return ret;
}

int ObPartTransCtx::replay_mds_to_tx_table_(const ObTxBufferNodeArray &mds_node_array,
                                            const share::SCN op_scn)
{
  int ret = OB_SUCCESS;
  ObTxDataGuard tx_data_guard;
  ObTxDataGuard new_tx_data_guard;
  bool op_exist = false;
  if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
    TRANS_LOG(WARN, "get tx data failed", KR(ret));
  } else if (OB_FAIL(tx_data_guard.tx_data()->check_tx_op_exist(op_scn, op_exist))) {
    TRANS_LOG(WARN, "check_tx_op_exist failed", KR(ret));
  } else if (op_exist) {
    // do nothing
  } else if (OB_FAIL(tx_data_guard.tx_data()->init_tx_op())) {
    TRANS_LOG(WARN, "init tx op failed", KR(ret));
  } else {
    ObTxOpArray tx_op_batch;
    if (OB_FAIL(prepare_mds_tx_op_(mds_node_array,
                                   op_scn,
                                   *tx_data_guard.tx_data()->op_allocator_,
                                   tx_op_batch,
                                   true))) {
       TRANS_LOG(WARN, "preapre mds tx_op failed", KR(ret));
    } else if (OB_FAIL(tx_data_guard.tx_data()->op_guard_->add_tx_op_batch(trans_id_,
            ls_id_, op_scn, tx_op_batch))) {
       TRANS_LOG(WARN, "add_tx_op_batch failed", KR(ret));
    }
    // tx_op_batch not put into tx_data, need to release
    if (OB_FAIL(ret)) {
      for (int64_t idx = 0; idx < tx_op_batch.count(); idx++) {
        tx_op_batch.at(idx).release();
      }
    }
  }
  // tx_ctx and tx_data checkpoint independent
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->alloc_tx_data(new_tx_data_guard, true, INT64_MAX))){
    TRANS_LOG(WARN, "alloc tx data failed", KR(ret));
  } else {
    *new_tx_data_guard.tx_data() = *tx_data_guard.tx_data();
    ObTxData *new_tx_data = new_tx_data_guard.tx_data();
    new_tx_data->end_scn_ = op_scn;
    if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->insert(new_tx_data))) {
      TRANS_LOG(WARN, "insert tx data failed", KR(ret));
    }
  }
  TRANS_LOG(INFO, "replay mds to tx_table", KR(ret), K(mds_node_array.count()), K(trans_id_), K(ls_id_), K(op_scn), K(op_exist));
  return ret;
}

int ObPartTransCtx::insert_mds_to_tx_table_(ObTxLogCb &log_cb)
{
  int ret = OB_SUCCESS;
  const ObTxBufferNodeArray &node_array = log_cb.get_mds_range().get_range_array();
  bool all_big_segment = true;
  int64_t need_process_mds_count = 0;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < node_array.count(); idx++) {
    if (!node_array.at(idx).allow_to_use_mds_big_segment()) {
      all_big_segment = false;
      need_process_mds_count++;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (all_big_segment) {
    TRANS_LOG(INFO, "MDS big_segment not support tx_op just skip", K(trans_id_), K(ls_id_), KP(this));
    // big segment not support tx_op
    if (OB_NOT_NULL(log_cb.get_tx_op_array()) && log_cb.get_tx_op_array()->count() > 0) {
      TRANS_LOG(WARN, "MDS big_segment log_cb pre_alloc is not null", KPC(this), K(log_cb));
    }
  } else if (OB_ISNULL(log_cb.get_tx_op_array())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "log_cb tx_op is null", KR(ret), KPC(this), K(log_cb));
  } else if (need_process_mds_count != log_cb.get_tx_op_array()->count()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "log_cb mds size is not match", KR(ret), KPC(this), K(log_cb), K(need_process_mds_count));
  } else {
    SCN op_scn = log_cb.get_log_ts();
    ObTxOpArray &tx_op_array = *log_cb.get_tx_op_array();
    ObTxDataGuard tx_data_guard;
    // assign mds for pre_alloc node
    int64_t mds_idx = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tx_op_array.count(); idx++) {
      tx_op_array.at(idx).set_op_scn(op_scn);
      ObTxBufferNodeWrapper &wrapper = *(ObTxBufferNodeWrapper*)(tx_op_array.at(idx).get_op_val());
      // just for skip mds big_segment
      while (mds_idx < node_array.count()) {
        if (!node_array.at(mds_idx).allow_to_use_mds_big_segment()) {
          break;
        } else {
          mds_idx++;
        }
      }
      const ObTxBufferNode &mds_node = node_array.at(mds_idx);
      mds_idx++;
      if (mds_idx < idx ||
          wrapper.get_node().get_register_no() != mds_node.get_register_no() ||
          wrapper.get_node().get_data_source_type() != mds_node.get_data_source_type()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "mds not match", KR(ret), KPC(this));
      } else if (OB_FAIL(wrapper.assign(trans_id_, mds_node, MTL(ObSharedMemAllocMgr*)->tx_data_op_allocator(), true))) {
        TRANS_LOG(WARN, "assign mds failed", KR(ret), KPC(this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
      TRANS_LOG(WARN, "get tx data failed", KR(ret));
    } else if (OB_FAIL(tx_data_guard.tx_data()->init_tx_op())) {
      TRANS_LOG(WARN, "init tx op failed", KR(ret));
    } else if (OB_FAIL(tx_data_guard.tx_data()->op_guard_->add_tx_op_batch(trans_id_,
          ls_id_, op_scn, tx_op_array))) {
      TRANS_LOG(WARN, "add_tx_op_batch failed", KR(ret));
    } else {
      *log_cb.get_tx_data_guard().tx_data() = *tx_data_guard.tx_data();
      ObTxData *new_tx_data = log_cb.get_tx_data_guard().tx_data();
      new_tx_data->end_scn_ = op_scn;
      if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->insert(new_tx_data))) {
        TRANS_LOG(WARN, "insert tx data failed", KR(ret));
      } else {
        tx_op_array.reset();
      }
    }
  }
  TRANS_LOG(INFO, "insert mds to tx_table", KR(ret), K(trans_id_), K(ls_id_), K(exec_info_.multi_data_source_.count()), K(log_cb));
  return ret;
}

int ObPartTransCtx::insert_undo_action_to_tx_table_(ObUndoAction &undo_action,
                                                    ObTxDataGuard &new_tx_data_guard,
                                                    storage::ObUndoStatusNode *&undo_node,
                                                    const share::SCN op_scn)
{
  int ret = OB_SUCCESS;
  // tx_data on part_ctx has modified
  ObTxDataGuard tx_data_guard;
  if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
    TRANS_LOG(WARN, "get tx data failed", KR(ret));
  } else if (OB_FAIL(tx_data_guard.tx_data()->init_tx_op())) {
    TRANS_LOG(WARN, "init tx op failed", KR(ret));
  } else if (OB_FAIL(tx_data_guard.tx_data()->add_undo_action(ls_tx_ctx_mgr_->get_tx_table(), undo_action, undo_node))) {
    TRANS_LOG(WARN, "add undo action failed", KR(ret));
  } else {
    *new_tx_data_guard.tx_data() = *tx_data_guard.tx_data();
    ObTxData *new_tx_data = new_tx_data_guard.tx_data();
    new_tx_data->end_scn_ = op_scn;
    if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->insert(new_tx_data))) {
       TRANS_LOG(WARN, "insert tx data failed", KR(ret));
     }
  }
  TRANS_LOG(INFO, "insert undo_action to tx_table", KR(ret), K(undo_action), K(trans_id_), K(ls_id_), K(op_scn), KP(undo_node));
  return ret;
}

int ObPartTransCtx::replay_undo_action_to_tx_table_(ObUndoAction &undo_action,
                                                    const share::SCN op_scn)
{
  int ret = OB_SUCCESS;
  ObTxDataGuard tx_data_guard;
  ObTxDataGuard new_tx_data_guard;
  ObTxDataOp *tx_data_op = nullptr;
  int64_t tx_data_op_ref = 0;
  if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
    TRANS_LOG(WARN, "get tx data failed", KR(ret));
  } else if (OB_FAIL(tx_data_guard.tx_data()->init_tx_op())) {
    TRANS_LOG(WARN, "init tx op failed", KR(ret));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->alloc_tx_data(new_tx_data_guard, true, INT64_MAX))){
    TRANS_LOG(WARN, "alloc tx data failed", KR(ret));
  } else {
    *new_tx_data_guard.tx_data() = *tx_data_guard.tx_data();
    ObTxData *new_tx_data = new_tx_data_guard.tx_data();
    new_tx_data->end_scn_ = op_scn;
    tx_data_op = new_tx_data->op_guard_.ptr();
    if (OB_NOT_NULL(tx_data_op)) {
      tx_data_op_ref = tx_data_op->get_ref();
    }
    if (OB_FAIL(new_tx_data->add_undo_action(ls_tx_ctx_mgr_->get_tx_table(), undo_action))) {
      TRANS_LOG(WARN, "add undo action failed", KR(ret));
    } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->insert(new_tx_data))) {
      TRANS_LOG(WARN, "insert tx data failed", KR(ret));
    }
  }
  TRANS_LOG(INFO, "replay undo_action to tx_table", KR(ret), K(undo_action), K(trans_id_),
      K(ls_id_), K(op_scn), KP(tx_data_op), K(tx_data_op_ref));
  return ret;
}

int ObPartTransCtx::on_success_ops_(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  const SCN log_ts = log_cb->get_log_ts();
  const palf::LSN log_lsn = log_cb->get_lsn();
  const ObTxCbArgArray &cb_arg_array = log_cb->get_cb_arg_array();

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
      } else if (OB_FAIL(insert_mds_to_tx_table_(*log_cb))) {
        TRANS_LOG(WARN, "inert into tx table failed", KR(ret));
      } else {
        log_cb->get_mds_range().reset();
        log_cb->reset_tx_op_array();
      }
    } else if (ObTxLogType::TX_DIRECT_LOAD_INC_LOG == log_type) {
      ObTxCtxLogOperator<ObTxDirectLoadIncLog> dli_log_op(this, log_cb);
      if (OB_FAIL(dli_log_op(ObTxLogOpType::APPLY_SUCC))) {
        TRANS_LOG(WARN, "try to apply direct load inc log failed", K(ret), KPC(this));
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
          if (OB_FAIL(dup_table_before_preapre_(
                  log_ts,
                  true /*after_redo_completed*/,
                  ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
            TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(log_cb),
                      KPC(this));
          } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
            TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
          }
          TRANS_LOG(INFO,
                    "need set before_prepare_version in on_success after switch_to_follower",
                    K(ret),
                    KPC(log_cb));
        } else {
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (!is_sub2pc() && OB_FAIL(dup_table_tx_redo_sync_())) {
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
      if (OB_FAIL(insert_undo_action_to_tx_table_(log_cb->get_undo_action(),
                                                  log_cb->get_tx_data_guard(),
                                                  log_cb->get_undo_node(),
                                                  log_ts))) {
        TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
      } else {
        log_cb->set_tx_data(nullptr);
        log_cb->reset_undo_node();
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
            if (OB_FAIL(ctx_tx_data_.set_end_log_ts(log_ts))) {
              TRANS_LOG(WARN, "set end log ts failed", K(ret));
            } else if (OB_FAIL(mds_cache_.generate_final_notify_array(exec_info_.multi_data_source_,
                                                                      true /*need_merge_cache*/,
                                                                      true /*allow_log_overflow*/))) {
              TRANS_LOG(WARN, "gen total mds array failed", K(ret));
            } else if (OB_FAIL(notify_data_source_(type, log_ts, false, mds_cache_.get_final_notify_array()))) {
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
      && !need_force_abort_()
      && !is_2pc_blocking()) {
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
  share::SCN max_committed_scn;
  if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->get_palf_committed_max_scn(max_committed_scn))) {
    TRANS_LOG(ERROR, "get palf max committed scn fail, need retry", K(ret), KPC(this));
#ifdef ENABLE_DEBUG_LOG
    ob_abort(); // fast abort for easy debug
#endif
  } else {
    TRANS_LOG(INFO, "succ get palf max_commited_scn", K(max_committed_scn), KPC(log_cb));
  }
  if (OB_SUCC(ret)) {
    {
      const int64_t log_sync_used_time = ObTimeUtility::current_time() - log_cb->get_submit_ts();
      CtxLockGuard guard(lock_);
      const int64_t ctx_lock_wait_time = guard.get_lock_acquire_used_time();
      if (log_sync_used_time + ctx_lock_wait_time >= ObServerConfig::get_instance().clog_sync_time_warn_threshold) {
        TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "transaction log sync use too much time", KPC(log_cb),
                      K(log_sync_used_time), K(ctx_lock_wait_time));
      }
      if (log_cb->get_cb_arg_array().count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "cb arg array is empty", K(ret), KPC(this));
        print_trace_log_();
        usleep(5000);
        ob_abort();
      }
      exec_info_.next_log_entry_no_--;
      const ObTxLogType log_type = log_cb->get_last_log_type();
      const SCN log_ts = log_cb->get_log_ts();
      mt_ctx_.sync_log_fail(log_cb->get_callbacks(), max_committed_scn);
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
      if (ObTxLogType::TX_DIRECT_LOAD_INC_LOG == log_type) {
        ObTxCtxLogOperator<ObTxDirectLoadIncLog> dli_log_op(this, log_cb);
        if (OB_FAIL(dli_log_op(ObTxLogOpType::APPLY_FAIL))) {
          TRANS_LOG(WARN, "try to apply direct load inc log failed", K(ret), KPC(this));
        }
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
        } else if (OB_FAIL(dup_table_before_preapre_(
                       exec_info_.max_applied_log_ts_,
                       true /*after_redo_completed*/,
                       ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
          TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(this));
        } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
          TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
        }
      }
      if (ObTxLogType::TX_COMMIT_LOG == log_type) {
        // if local tx commit log callback on failure, reset trans_version to make standby read skip this
        if (is_local_tx_() && !mt_ctx_.get_trans_version().is_max()) {
          mt_ctx_.set_trans_version(SCN::max_scn());
          TRANS_LOG(INFO, "clear local trans version when commit log on failure", K(ret), KPC(this));
        }
      }
      busy_cbs_.remove(log_cb);
      return_log_cb_(log_cb, true);
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

// starting from 0
int64_t ObPartTransCtx::get_redo_log_no_() const
{
  return exec_info_.redo_lsns_.count();
}


inline
int ObPartTransCtx::submit_redo_if_serial_logging_(ObTxLogBlock &log_block,
                                                   bool &has_redo,
                                                   ObRedoLogSubmitHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_parallel_logging())) {
    ObTxRedoSubmitter submitter(*this, mt_ctx_);
    ret = submitter.fill(log_block, helper, true /*display blocked info*/);
    has_redo = submitter.get_submitted_cnt() > 0 || helper.callbacks_.count() > 0;
  } else {
    // sanity check, all redo must have been flushed
#ifndef NDEBUG
    mt_ctx_.check_all_redo_flushed();
#endif
  }
  return ret;
}

// when parallel logging, redo need submitted seperate with other txn's log
inline
int ObPartTransCtx::submit_redo_if_parallel_logging_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_parallel_logging())) {
    ObTxRedoSubmitter submitter(*this, mt_ctx_);
    if (OB_FAIL(submitter.submit_all(true /*display blocked info*/))) {
      TRANS_LOG(WARN, "submit redo log fail", K(ret));
    }
  }
  return ret;
}

// this function is thread safe, not need other lock's protection
inline
int ObPartTransCtx::init_log_block_(ObTxLogBlock &log_block,
                                    const int64_t suggested_buf_size,
                                    const bool serial_final)
{
  ObTxLogBlockHeader &header = log_block.get_header();
  // the log_entry_no will be backfill before log-block to be submitted
  header.init(cluster_id_, cluster_version_, -1 /*log_entry_no*/, trans_id_, exec_info_.scheduler_);
  if (OB_UNLIKELY(serial_final)) { header.set_serial_final(); }
  return log_block.init_for_fill(suggested_buf_size);
}

inline int ObPartTransCtx::reuse_log_block_(ObTxLogBlock &log_block)
{
  ObTxLogBlockHeader &header = log_block.get_header();
  header.init(cluster_id_, cluster_version_, exec_info_.next_log_entry_no_, trans_id_, exec_info_.scheduler_);
  return log_block.reuse_for_fill();
}

int ObPartTransCtx::submit_redo_commit_info_log_()
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  bool has_redo = false;
  ObTxLogCb *log_cb = NULL;
  ObRedoLogSubmitHelper helper;
  const int64_t replay_hint = trans_id_.get_id();
  using LogBarrierType = logservice::ObReplayBarrierType;
  LogBarrierType barrier = LogBarrierType::NO_NEED_BARRIER;
  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  } else if (sub_state_.is_info_log_submitted()) {
    // state log already submitted, do nothing
  } else if (OB_FAIL(submit_redo_if_parallel_logging_())) {
  } else if (OB_FAIL(init_log_block_(log_block))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
      TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_redo_commit_info_log_(log_block, has_redo, helper, barrier))) {
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
  } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
    TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb, replay_hint, barrier))) {
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
                                                 ObRedoLogSubmitHelper &helper,
                                                 logservice::ObReplayBarrierType &barrier)
{
  int ret = OB_SUCCESS;
  ObTxLogCb *log_cb = NULL;
  const int64_t replay_hint = trans_id_.get_id();
  barrier = logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  if (sub_state_.is_info_log_submitted()) {
    // state log already submitted, do nothing
  } else if (OB_FAIL(submit_redo_if_serial_logging_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
  } else if (OB_FAIL(check_dup_trx_with_submitting_all_redo(log_block, helper))) {
    TRANS_LOG(WARN, "check dup trx with submitting all redo failed", K(ret));
  } else if (OB_FAIL(check_dli_batch_completed_(ObTxLogType::TX_COMMIT_INFO_LOG))) {
    TRANS_LOG(WARN, "check direct load inc batch completed", K(ret), KPC(this));
  } else if (OB_FAIL(decide_state_log_barrier_type_(ObTxLogType::TX_COMMIT_INFO_LOG, barrier))) {
    TRANS_LOG(WARN, "decide commit info log barrier failed", K(ret), K(barrier), KPC(this));
  } else {
    ObTxCommitInfoLog commit_info_log(
        exec_info_.scheduler_, exec_info_.participants_, exec_info_.upstream_,
        exec_info_.is_sub2pc_, exec_info_.is_dup_tx_, can_elr_, trace_info_.get_app_trace_id(),
        trace_info_.get_app_trace_info(), exec_info_.prev_record_lsn_, exec_info_.redo_lsns_,
        exec_info_.incremental_participants_, cluster_version_, exec_info_.xid_, exec_info_.commit_parts_, epoch_);

    if (OB_SUCC(ret)) {
      if (exec_info_.is_dup_tx_) {
        if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->add_commiting_dup_trx(trans_id_))) {
          TRANS_LOG(WARN, "add committing dup table trx failed", K(ret), KPC(this));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
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
        } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
          TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
          return_log_cb_(log_cb);
          log_cb = NULL;
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb))) {
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
  ObRedoLogSubmitHelper helper;
  if (OB_FAIL(submit_redo_if_parallel_logging_())) {
  } else if (OB_FAIL(init_log_block_(log_block))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
    TRANS_LOG(WARN, "submit multi source data failed", K(ret), K(*this));
  } else if (OB_FAIL(submit_redo_if_serial_logging_(log_block, has_redo, helper))) {
    TRANS_LOG(WARN, "submit redo log failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_pending_log_block_(log_block, helper, logservice::ObReplayBarrierType::NO_NEED_BARRIER))) {
    TRANS_LOG(WARN, "submit pending log failed", KR(ret), K(*this));
  } else if (exec_info_.redo_lsns_.count() > 0 && OB_FAIL(submit_record_log_())) {
    TRANS_LOG(WARN, "submit record log failed", KR(ret), K(*this));
  } else if (OB_FAIL(reuse_log_block_(log_block))) {
    TRANS_LOG(WARN, "reuse log block failed", KR(ret), K(*this));
  } else {
    ObTxActiveInfoLog active_info_log(exec_info_.scheduler_, exec_info_.trans_type_, session_id_,
                                      associated_session_id_,
                                      trace_info_.get_app_trace_id(),
                                      mt_ctx_.get_min_table_version(), can_elr_,
                                      addr_,                 //
                                      cur_query_start_time_, //
                                      false,                 // sub2pc
                                      exec_info_.is_dup_tx_,
                                      trans_expired_time_, epoch_, last_op_sn_, first_scn_,
                                      last_scn_, exec_info_.max_submitted_seq_no_,
                                      cluster_version_,
                                      exec_info_.xid_,
                                      exec_info_.serial_final_seq_no_);
    ObTxLogCb *log_cb = nullptr;
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
    } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb))) {
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
  ObRedoLogSubmitHelper helper;
  const int64_t replay_hint = trans_id_.get_id();
  using ReplayBarrier = logservice::ObReplayBarrierType;
  ReplayBarrier commit_info_log_barrier = ReplayBarrier::NO_NEED_BARRIER;
  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  }

  bool contain_commit_info = false;
  if (OB_SUCC(ret)) {
    if (!sub_state_.is_info_log_submitted()) {
      prev_lsn.reset();
      if (OB_FAIL(submit_redo_if_parallel_logging_())) {
      } else if (OB_FAIL(init_log_block_(log_block))) {
        TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
      } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
        TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
      } else if (OB_FAIL(submit_redo_commit_info_log_(log_block, has_redo, helper,
                                                      commit_info_log_barrier))) {
        TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
      } else {
        // do nothing
        contain_commit_info = true;
      }
      // init log_block for prepare log
    } else if (OB_FAIL(init_log_block_(log_block))) {
      TRANS_LOG(WARN, "init log block failed", K(ret));
    }
  }

  const ReplayBarrier compound_log_barrier =
    contain_commit_info ? commit_info_log_barrier : ReplayBarrier::NO_NEED_BARRIER;

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_ && !is_dup_table_redo_sync_completed_()) {
      if (OB_FAIL(submit_pending_log_block_(log_block, helper, compound_log_barrier))) {
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
    ObTxPrevLogType prev_log_type(ObTxPrevLogType::TypeEnum::COMMIT_INFO);

    if (OB_FAIL(get_prev_log_lsn_(log_block, prev_log_type, prev_lsn))) {
      TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
    }

    ObTxPrepareLog prepare_log(exec_info_.incremental_participants_, prev_lsn, prev_log_type);

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
        } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
          TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
          return_log_cb_(log_cb);
          log_cb = NULL;
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb, replay_hint,
                                                 compound_log_barrier))) {
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
          } else if (OB_FAIL(submit_log_block_out_(log_block, exec_info_.prepare_version_, log_cb))) {
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
    } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
      TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(submit_log_block_out_(log_block, exec_info_.prepare_version_, log_cb, replay_hint, compound_log_barrier))) {
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
  ObRedoLogSubmitHelper helper;
  const int64_t replay_hint = trans_id_.get_id();

  const bool local_tx = is_local_tx_();
  using LogBarrierType = logservice::ObReplayBarrierType;
  LogBarrierType commit_info_log_barrier =  LogBarrierType::NO_NEED_BARRIER;
  if (need_force_abort_() || is_force_abort_logging_()
      || get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit prepare log", K(ret));
  } else if (OB_FAIL(mds_cache_.reserve_final_notify_array(exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "reserve mds cache memory failed", KR(ret), K(*this));
  } else if (OB_FAIL(mds_cache_.generate_final_notify_array(exec_info_.multi_data_source_,
                                                             true /*need_merge_cache*/,
                                                             false /*allow_log_overflow*/))) {
    TRANS_LOG(WARN, "generate final notify array failed", K(ret), K(mds_cache_), KPC(this));
  } else {
    bool log_block_inited = false;
    int64_t suggested_buf_size = ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE;
    if (local_tx &&
        mds_cache_.get_final_notify_array().count() == 0 &&
        // 512B
        ((mt_ctx_.get_pending_log_size() < ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE / 4) ||
         // for corner case test
         IS_CORNER(10000))) {
      suggested_buf_size = ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE;
    }
    if (local_tx) {
      if (!sub_state_.is_info_log_submitted()) {
        prev_lsn.reset();
        if (OB_FAIL(submit_redo_if_parallel_logging_())) {
        } else if (OB_FAIL(init_log_block_(log_block, suggested_buf_size))) {
          TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
        } else if (FALSE_IT(log_block_inited = true)) {
        } else if (OB_FAIL(submit_multi_data_source_(log_block))) {
          TRANS_LOG(WARN, "submit multi source data failed", KR(ret), K(*this));
        } else if (OB_SUCC(submit_redo_commit_info_log_(log_block, has_redo, helper,
                                                        commit_info_log_barrier))) {
          // do nothing
        } else {
          TRANS_LOG(WARN, "submit redo commit state log failed", KR(ret), K(*this));
        }
      }
    }
    // init log_block for commit log
    if (OB_SUCC(ret) && !log_block_inited && OB_FAIL(init_log_block_(log_block, suggested_buf_size))) {
      TRANS_LOG(WARN, "init log block failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_ && !is_dup_table_redo_sync_completed_()) {
      if (OB_FAIL(submit_pending_log_block_(log_block, helper, commit_info_log_barrier))) {
        TRANS_LOG(WARN, "submit pending log block failed", K(ret));
      } else {
        ret = OB_EAGAIN;
        TRANS_LOG(INFO, "need wait redo sync finish for a dup table trx", K(ret), K(trans_id_),
                  K(ls_id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_local_tx_() &&
        (exec_info_.participants_.count() > 1 ||
         exec_info_.intermediate_participants_.count() > 0)) {
      exec_info_.trans_type_ = TransType::DIST_TRANS;
      exec_info_.upstream_ = ls_id_;
      if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
        exec_info_.trans_type_ = TransType::SP_TRANS;
        exec_info_.upstream_.reset();
        TRANS_LOG(WARN, "drive self 2pc phase failed", KPC(this));
      } else {
        ret = OB_EAGAIN;
        TRANS_LOG(INFO, "convert trans to dist trans if participants is more than one",
                  K(ret), KPC(this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    SCN log_commit_version;
    ObSEArray<uint64_t, 1> checksum_arr;
    ObTxPrevLogType prev_log_type;
    if (exec_info_.need_checksum_
        && replay_completeness_.is_complete()
        && OB_FAIL(mt_ctx_.calc_checksum_all(checksum_arr))) {
      TRANS_LOG(WARN, "calc checksum failed", K(ret));
    } else if (!local_tx) {
      log_commit_version = ctx_tx_data_.get_commit_version();
      prev_log_type.set_prepare();
      if (OB_FAIL(get_prev_log_lsn_(log_block, prev_log_type, prev_lsn))) {
        TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
      }
    } else {
      prev_log_type.set_commit_info();
      if (OB_FAIL(get_prev_log_lsn_(log_block, prev_log_type, prev_lsn))) {
        TRANS_LOG(WARN, "get prev log lsn failed", K(ret), K(*this));
      }
    }
    uint64_t collapsed_checksum = 0;
    uint8_t _checksum_sig[checksum_arr.count()];
    ObArrayHelper<uint8_t> checksum_sig(checksum_arr.count(), _checksum_sig);
    mt_ctx_.convert_checksum_for_commit_log(checksum_arr, collapsed_checksum, checksum_sig);
    ObTxCommitLog commit_log(log_commit_version,
                             collapsed_checksum,
                             checksum_sig,
                             exec_info_.incremental_participants_,
                             mds_cache_.get_final_notify_array(), exec_info_.trans_type_, prev_lsn,
                             coord_prepare_info_arr_, prev_log_type);
    ObTxLogCb *log_cb = NULL;
    bool redo_log_submitted = false;
    LogBarrierType commit_log_barrier_type =  LogBarrierType::NO_NEED_BARRIER;
    LogBarrierType compound_log_barrier_type = commit_info_log_barrier;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_start_scn_in_commit_log_(commit_log))) {
        TRANS_LOG(WARN, "set start scn in commit log failed", K(ret), K(commit_log));
      } else if (OB_FAIL(check_dli_batch_completed_(ObTxLogType::TX_COMMIT_LOG))) {
        TRANS_LOG(WARN, "check dli batch completed error", KR(ret), K(*this));
      } else if ((exec_info_.multi_data_source_.count() > 0 || mds_cache_.count() > 0)
                 && OB_FAIL(try_alloc_retain_ctx_func_())) {
        TRANS_LOG(WARN, "alloc retain ctx func for mds trans failed", K(ret), K(mds_cache_), KPC(this));
      } else if (OB_FAIL(decide_state_log_barrier_type_(ObTxLogType::TX_COMMIT_LOG, commit_log_barrier_type))) {
        TRANS_LOG(WARN, "decide commit log barrier type failed", K(ret), K(commit_log_barrier_type),
                  KPC(this));
      } else if (OB_FAIL(ObTxLogTypeChecker::decide_final_barrier_type(commit_log_barrier_type, compound_log_barrier_type))) {
        TRANS_LOG(ERROR, "decide compound log barrier type failed", K(ret), K(commit_log_barrier_type), K(compound_log_barrier_type), KPC(this));
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
        } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
          TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
          return_log_cb_(log_cb);
          log_cb = NULL;
        } else if (OB_FAIL(acquire_ctx_ref_())) {
          TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb, replay_hint,
                                                 commit_info_log_barrier))) {
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
          } else if (log_block.get_cb_arg_array().count() == 0) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
            return_log_cb_(log_cb);
            log_cb = NULL;
          } else if (OB_FAIL(acquire_ctx_ref_())) {
            TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
          } else if (OB_FAIL(submit_log_block_out_(log_block,
                                                   ctx_tx_data_.get_commit_version(),
                                                   log_cb,
                                                   replay_hint,
                                                   commit_log_barrier_type))) {
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
    } else if (log_block.get_cb_arg_array().count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(prepare_log_cb_(NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
      TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(submit_log_block_out_(log_block,
                                             ctx_tx_data_.get_commit_version(),
                                             log_cb,
                                             replay_hint,
                                             compound_log_barrier_type))) {
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
  set_upstream_state(ObTxState::ABORT);
  ObTxLogCb *log_cb = NULL;
  ObTxLogBlock log_block;
  const int64_t replay_hint = trans_id_.get_id();
  using LogBarrierType = logservice::ObReplayBarrierType;
  logservice::ObReplayBarrierType barrier = LogBarrierType::NO_NEED_BARRIER;

  if (OB_FAIL(check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG))) {
    TRANS_LOG(WARN, "check dli batch completed error", KR(ret), K(*this));
  } else if (OB_FAIL(mds_cache_.reserve_final_notify_array(exec_info_.multi_data_source_))) {
    TRANS_LOG(WARN, "reserve final notify array failed", K(ret), K(mds_cache_), KPC(this));
  } else if (OB_FAIL(mds_cache_.generate_final_notify_array(exec_info_.multi_data_source_,
                                                            true /*need_merge_cache*/,
                                                            false /*allow_log_overflow*/))) {
    TRANS_LOG(WARN, "gen abort mds array failed", K(ret));
  }

  ObTxAbortLog abort_log(mds_cache_.get_final_notify_array());

  if (OB_SUCC(ret)) {
    if ((exec_info_.multi_data_source_.count() > 0 || mds_cache_.count() > 0)) {
      if (OB_FAIL(try_alloc_retain_ctx_func_())) {
        TRANS_LOG(WARN, "alloc retain ctx func for mds trans failed", K(ret), K(mds_cache_), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(decide_state_log_barrier_type_(ObTxLogType::TX_ABORT_LOG, barrier))) {
        TRANS_LOG(WARN, "decide abort log barrier type failed", K(ret), K(barrier), KPC(this));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
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
  } else if (OB_FAIL(init_log_block_(log_block))) {
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
  } else if (OB_FAIL(ctx_tx_data_.reserve_tx_op_space(1))) {
    TRANS_LOG(WARN, "reserve tx_op space failed", KR(ret), KPC(this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb, replay_hint, barrier, 50 * 1000))) {
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
  const int64_t replay_hint = trans_id_.get_id();
  ObTxLogCb *log_cb = NULL;
  if (OB_FAIL(init_log_block_(log_block))) {
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
  } else if (OB_FAIL(submit_log_block_out_(log_block,
                 share::SCN::max(ctx_tx_data_.get_end_log_ts(), max_2pc_commit_scn_),
                 log_cb))) {
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
  const int64_t replay_hint = trans_id_.get_id();
  ObTxLogCb *log_cb = NULL;
  if (OB_FAIL(init_log_block_(log_block))) {
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
  } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb))) {
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

template <typename DLI_LOG>
int ObPartTransCtx::submit_direct_load_inc_log_(
    DLI_LOG &dli_log,
    const ObTxDirectLoadIncLog::DirectLoadIncLogType dli_log_type,
    const ObDDLIncLogBasic &batch_key,
    logservice::AppendCb *extra_cb,
    const logservice::ObReplayBarrierType replay_barrier,
    const int64_t replay_hint,
    share::SCN &scn,
    bool need_free_extra_cb)
{

  int ret = OB_SUCCESS;

  ObTxLogBlock log_block;

  ObTxDLIncLogBuf dli_buf;
  ObTxDirectLoadIncLog::ConstructArg construct_arg(dli_log_type, batch_key, dli_buf);

  ObTxDirectLoadIncLog::SubmitArg submit_arg;
  submit_arg.reset();
  submit_arg.replay_barrier_type_ = replay_barrier;
  submit_arg.replay_hint_ = replay_hint;
  submit_arg.log_cb_ = nullptr;
  submit_arg.extra_cb_ = extra_cb;
  submit_arg.need_free_extra_cb_ = need_free_extra_cb;
  submit_arg.base_scn_ = share::SCN::min_scn();
  submit_arg.suggested_buf_size_ = dli_log.get_serialize_size() + 200;

  ObTxCtxLogOperator<ObTxDirectLoadIncLog> dli_log_op(this, &log_block, &construct_arg, submit_arg);
  if (OB_FAIL(dli_buf.serialize_log_object(&dli_log))) {
    TRANS_LOG(WARN, "serialize direct load log failed", K(ret), K(dli_log), K(replay_hint),
              KPC(this));
  } else if (OB_FAIL(dli_log_op(ObTxLogOpType::SUBMIT))) {
    if (ret == OB_TX_NOLOGCB) {
      if (REACH_COUNT_PER_SEC(10) && REACH_TIME_INTERVAL(100 * 1000)) {
        TRANS_LOG(INFO, "no log cb with dli log", KR(ret), K(dli_log_type), K(batch_key),
                  KPC(busy_cbs_.get_first()));
      }
    } else {
      TRANS_LOG(WARN, "try to submit direct load inc log failed", K(ret), KPC(this));
    }
  } else {
    scn = dli_log_op.get_scn();
  }

  TRANS_LOG(DEBUG, "<ObTxDirectLoadIncLog> submit direct load inc log", K(ret), K(get_trans_id()),
            K(get_ls_id()), K(dli_log), K(dli_log_type), K(batch_key), K(replay_barrier),
            K(replay_hint), K(scn));
  return ret;
}

int ObPartTransCtx::submit_direct_load_inc_redo_log(storage::ObDDLRedoLog &ddl_redo_log,
                                                    logservice::AppendCb *extra_cb,
                                                    const int64_t replay_hint,
                                                    share::SCN &scn)
{
  common::ObTimeGuard timeguard("ObPartTransCtx::submit_direct_load_inc_redo_log", 1 * 1000 * 1000); // 1s
  ObDDLIncLogBasic inc_log_basic;
  return submit_direct_load_inc_log_(
      ddl_redo_log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_REDO, inc_log_basic, extra_cb,
      logservice::ObReplayBarrierType::NO_NEED_BARRIER, replay_hint, scn);
}

int ObPartTransCtx::submit_direct_load_inc_start_log(storage::ObDDLIncStartLog &ddl_start_log,
                                                     logservice::AppendCb *extra_cb,
                                                     share::SCN &scn)
{
  common::ObTimeGuard timeguard("ObPartTransCtx::submit_direct_load_inc_start_log", 1 * 1000 * 1000); // 1s
  ObDDLIncLogBasic inc_log_basic = ddl_start_log.get_log_basic();
  return submit_direct_load_inc_log_(
      ddl_start_log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START, inc_log_basic, extra_cb,
      logservice::ObReplayBarrierType::STRICT_BARRIER, get_trans_id().get_id(), scn);
}

int ObPartTransCtx::submit_direct_load_inc_commit_log(storage::ObDDLIncCommitLog &ddl_commit_log,
                                                      logservice::AppendCb *extra_cb,
                                                      share::SCN &scn,
                                                      bool need_free_extra_cb)
{
  common::ObTimeGuard timeguard("ObPartTransCtx::submit_direct_load_inc_commit_log", 1 * 1000 * 1000); // 1s
  ObDDLIncLogBasic inc_log_basic = ddl_commit_log.get_log_basic();
  return submit_direct_load_inc_log_(
      ddl_commit_log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END, inc_log_basic, extra_cb,
      logservice::ObReplayBarrierType::STRICT_BARRIER, get_trans_id().get_id(), scn, need_free_extra_cb);
}

int ObPartTransCtx::check_dli_batch_completed_(ObTxLogType submit_log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();

  if (exec_info_.dli_batch_set_.size() > 0) {
    int64_t need_ddl_end_count = 0;
    int64_t compensate_ddl_end_fail_cnt = 0;
    const int64_t MAX_COMPENSATE_FAIL_CNT = 100;

    ObTxDirectLoadBatchKeyArray unused_batch_key_array;
    for (ObDLIBatchSet::iterator iter = exec_info_.dli_batch_set_.begin();
         iter != exec_info_.dli_batch_set_.end(); iter++) {
      if (iter->first.need_compensate_ddl_end()) {
        TRANS_LOG(DEBUG, "<ObTxDirectLoadIncLog> need compensate ddl end log", K(ret), K(tmp_ret),
                  K(submit_log_type), K(trans_id_), K(ls_id_), K(iter->first));
        if (compensate_ddl_end_fail_cnt <= MAX_COMPENSATE_FAIL_CNT
            && (ObTxLogType::TX_ABORT_LOG == submit_log_type
                || ObTxLogType::TX_COMMIT_INFO_LOG == submit_log_type)) {
          ObDDLIncCommitLog inc_commit_log;
          ObDDLIncCommitClogCb *extra_cb = static_cast<ObDDLIncCommitClogCb *>(
              mtl_malloc(sizeof(ObDDLIncCommitClogCb), "TxExtraCb"));

          share::SCN submitted_scn;
          if (OB_ISNULL(extra_cb)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TRANS_LOG(WARN, "alloc memory failed", K(ret), K(ls_id_), K(trans_id_),
                      K(submit_log_type));
          } else {
            new (extra_cb) ObDDLIncCommitClogCb();
          }

          if (OB_TMP_FAIL(ret)) {
            // do nothing
          } else if (OB_TMP_FAIL(extra_cb->init(ls_id_, iter->first.get_batch_key()))) {
            TRANS_LOG(WARN, "init extra cb failed", K(ret), K(iter->first), K(trans_id_),
                      K(ls_id_));
            extra_cb->~ObDDLIncCommitClogCb();
            mtl_free(extra_cb);
          } else if (OB_TMP_FAIL(inc_commit_log.init(iter->first.get_batch_key()))) {
            TRANS_LOG(WARN, "init inc commit log failed", K(ret), K(inc_commit_log), K(iter->first),
                      K(trans_id_), K(ls_id_));
            extra_cb->~ObDDLIncCommitClogCb();
            mtl_free(extra_cb);
          } else if (OB_TMP_FAIL(submit_direct_load_inc_commit_log(inc_commit_log, extra_cb,
                                                                   submitted_scn, true))) {
            if (tmp_ret != OB_TX_NOLOGCB) {
              TRANS_LOG(WARN, "submit direct load inc commit log failed", K(ret), K(inc_commit_log),
                        KPC(extra_cb), K(submitted_scn), K(trans_id_), K(ls_id_));
            }
            extra_cb->~ObDDLIncCommitClogCb();
            mtl_free(extra_cb);
          } else {
          }
        }

        if (OB_TMP_FAIL(tmp_ret)) {
          compensate_ddl_end_fail_cnt += 1;
        }

        if (iter->first.need_compensate_ddl_end()) {
          need_ddl_end_count += 1;
        }
      } else if (iter->first.is_ddl_start_logging()) {
        need_ddl_end_count += 1;
        TRANS_LOG(INFO, "the DDL inc start is logging", K(ret), K(tmp_ret), K(iter->first),
                  K(trans_id_), K(ls_id_));
      } else if (iter->first.is_ddl_end_logging()) {
        TRANS_LOG(INFO, "the DDL inc end is logging", K(ret), K(tmp_ret), K(iter->first),
                  K(trans_id_), K(ls_id_));
      } else if (OB_TMP_FAIL(unused_batch_key_array.push_back(iter->first.get_batch_key()))) {
        TRANS_LOG(WARN, "push back unused batch key failed", K(ret), K(tmp_ret), K(submit_log_type),
                  K(trans_id_), K(ls_id_), K(iter->first));
      } else {
        TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> Try to remove unused batch info", K(ret),
                  K(tmp_ret), K(submit_log_type), K(trans_id_), K(ls_id_), K(iter->first),
                  K(unused_batch_key_array.count()));
      }
    }

    if (OB_TMP_FAIL(exec_info_.dli_batch_set_.remove_unlog_batch_info(unused_batch_key_array))) {
      TRANS_LOG(WARN, "remove unlog batch_info failed", K(ret), K(tmp_ret), K(submit_log_type),
                K(trans_id_), K(ls_id_), K(unused_batch_key_array));
    }

    if (OB_SUCC(ret) && need_ddl_end_count > 0) {
      if (ObTxLogTypeChecker::is_state_log(submit_log_type)
          && submit_log_type != ObTxLogType::TX_ABORT_LOG
          && (get_downstream_state() >= ObTxState::REDO_COMPLETE
              || submit_log_type != ObTxLogType::TX_CLEAR_LOG)) {
        ret = OB_TRANS_NEED_ROLLBACK;
        TRANS_LOG(ERROR,
                  "<ObTxDirectLoadIncLog> incompleted direct load inc batch info with state log",
                  K(ret), K(tmp_ret), K(submit_log_type), K(trans_id_), K(ls_id_),
                  K(need_ddl_end_count), K(ObTimeUtility::fast_current_time() - start_ts));
      } else {
        ret = OB_TRANS_CANNOT_BE_KILLED;
        TRANS_LOG(WARN,
                  "<ObTxDirectLoadIncLog> The trx can not be finished because of a incompleted "
                  "direct load inc batch info",
                  K(ret), K(tmp_ret), K(submit_log_type), K(trans_id_), K(ls_id_),
                  K(need_ddl_end_count), K(ObTimeUtility::fast_current_time() - start_ts));
      }
    }
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
  if (OB_FAIL(init_log_block_(log_block))) {
    TRANS_LOG(WARN, "init log block failed", KR(ret), K(*this));
  }
  while (OB_SUCC(ret) && big_segment_info_.segment_buf_.is_active()) {
    const char *submit_buf = nullptr;
    int64_t submit_buf_len = 0;
    if (OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
      if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
        TRANS_LOG(WARN, "get log cb failed", KR(ret), K(*this));
      }
    } else if (OB_FAIL(log_cb->copy(*big_segment_info_.submit_log_cb_template_))) {
      TRANS_LOG(WARN, "log cb copy failed", KR(ret), K(*this));
    } else if (OB_FALSE_IT(ret = (log_block.acquire_segment_log_buf(source_log_type, &big_segment_info_.segment_buf_)))) {
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
    } else if (OB_FAIL(submit_log_block_out_(log_block, big_segment_info_.submit_base_scn_, log_cb))) {
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

inline
int ObPartTransCtx::submit_log_block_out_(ObTxLogBlock &log_block,
                                          const share::SCN &base_scn,
                                          ObTxLogCb *&log_cb,
                                          const int64_t replay_hint,
                                          const logservice::ObReplayBarrierType barrier,
                                          const int64_t retry_timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_2pc_state_log = false;
  if (OB_UNLIKELY(is_2pc_blocking())) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "tx submit log failed because of 2pc blocking", K(ret), KPC(this));
    // It is safe to merge the intermediate_participants because we will block
    // the persistent state machine with is_2pc_blocking. The detailed design
    // can be found in the implementation of the merge_intermediate_participants.
  } else if (is_contain_stat_log(log_block.get_cb_arg_array()) && FALSE_IT(is_2pc_state_log = true)) {
  } else if (is_2pc_state_log && OB_FAIL(merge_intermediate_participants())) {
    TRANS_LOG(WARN, "fail to merge intermediate participants", K(ret), KPC(this));
  } else {
    const int64_t replay_hint_v = replay_hint ?: trans_id_.get_id();
    log_block.get_header().set_log_entry_no(exec_info_.next_log_entry_no_);
    if (OB_FAIL(log_block.seal(replay_hint_v, barrier))) {
      TRANS_LOG(WARN, "seal log block fail", K(ret));
    } else if (OB_SUCC(ls_tx_ctx_mgr_->get_ls_log_adapter()
                       ->submit_log(log_block.get_buf(),
                                    log_block.get_size(),
                                    base_scn,
                                    log_cb,
                                    true,
                                    retry_timeout_us))) {
      busy_cbs_.add_last(log_cb);
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DELAY_TX_SUBMIT_LOG);

int ObPartTransCtx::submit_log_impl_(const ObTxLogType log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    if (big_segment_info_.segment_buf_.is_active()) {
      ret = OB_LOG_TOO_LARGE;
      TRANS_LOG(INFO, "can not submit any log before all big log submittted", K(ret), K(log_type),
                K(trans_id_), K(ls_id_), K(big_segment_info_));
    } else {
      switch (log_type) {
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
        if (get_upstream_state() < ObTxState::PREPARE) {
          if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
            TRANS_LOG(WARN, "drive 2pc prepare phase failed", K(ret), K(*this));
          } else if (OB_TMP_FAIL(ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_())) {
            TRANS_LOG(WARN, "retry to post preapre request failed", K(tmp_ret), KPC(this));
          }
        } else if (get_upstream_state() > ObTxState::PREPARE ||
                   get_downstream_state() >= ObTxState::PREPARE) {
          TRANS_LOG(INFO, "we need not submit prepare log after the prepare state", K(*this));
        }
        if (OB_SUCC(ret) &&
            mt_ctx_.is_prepared() &&
            get_upstream_state() == ObTxState::PREPARE &&
            get_downstream_state() < ObTxState::PREPARE &&
            !is_2pc_logging()) {
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
        } else if (ERRSIM_DELAY_TX_SUBMIT_LOG < int(-9999)) {
          // reject trans_id and tenant id to contorl trans sate
          ret = process_errsim_for_standby_read_(int(ERRSIM_DELAY_TX_SUBMIT_LOG),
                                                 OB_SWITCHING_TO_FOLLOWER_GRACEFULLY);
          TRANS_LOG(INFO, "reject delay tx submit log when submit commit log",
                    K(ret), K(ERRSIM_DELAY_TX_SUBMIT_LOG));
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
  if (OB_FAIL(ret)) {
    handle_submit_log_err_(log_type, ret);
  }
  return ret;
}

void ObPartTransCtx::handle_submit_log_err_(const ObTxLogType log_type, int &ret)
{
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

#define bitmap_is_contain(X) (bitmap & (uint64_t)X)

//big row may use a unused log_block to invoke after_submit_log_
int ObPartTransCtx::after_submit_log_(ObTxLogBlock &log_block,
                                      ObTxLogCb *log_cb,
                                      ObRedoLogSubmitHelper *helper)
{
  int ret = OB_SUCCESS;
  uint64_t bitmap = 0;
  const ObTxCbArgArray &cb_arg_array = log_block.get_cb_arg_array();
  if (cb_arg_array.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(*this));
  } else if (OB_FAIL(log_cb->get_cb_arg_array().assign(cb_arg_array))) {
    TRANS_LOG(WARN, "assign cb arg array failed", K(ret));
  } else {
    for (int i = 0; i < cb_arg_array.count(); i++) {
      bitmap |= (uint64_t)cb_arg_array.at(i).get_log_type();
    }
    if (bitmap_is_contain(ObTxLogType::TX_REDO_LOG) ||
        bitmap_is_contain(ObTxLogType::TX_ROLLBACK_TO_LOG) ||
        bitmap_is_contain(ObTxLogType::TX_BIG_SEGMENT_LOG) ||
        bitmap_is_contain(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG)) {
      if (!bitmap_is_contain(ObTxLogType::TX_COMMIT_INFO_LOG)) {
        TRANS_LOG(TRACE, "redo_lsns.push", K(log_cb->get_lsn()));
        ret = exec_info_.redo_lsns_.push_back(log_cb->get_lsn());
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(update_rec_log_ts_(false/*for_replay*/, SCN()))) {
    TRANS_LOG(WARN, "update rec log ts failed", KR(ret), KPC(log_cb), K(*this));
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_REDO_LOG)) {
    if (OB_FAIL(log_cb->set_callbacks(helper->callbacks_))) {
      ob_abort();
    } else {
      exec_info_.max_submitted_seq_no_.inc_update(helper->max_seq_no_);
      helper->log_scn_ = log_cb->get_log_ts();
      if (helper->callback_redo_submitted_ && OB_FAIL(mt_ctx_.log_submitted(*helper))) {
        TRANS_LOG(ERROR, "fill to do log_submitted on redo log gen", K(ret), K(*this));
      }
    }
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_ROLLBACK_TO_LOG)) {
    // do nothing
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG)) {
    // do nothing
    log_cb->get_mds_range().range_submitted(mds_cache_);
  }
  if(OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_BIG_SEGMENT_LOG))
  {
    add_unsynced_segment_cb_(log_cb);
    if (big_segment_info_.segment_buf_.is_completed()) {
      TRANS_LOG(INFO, "reuse big_segment_info_",K(ret),K(big_segment_info_),KPC(log_cb));
      big_segment_info_.reuse();
    }
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_ACTIVE_INFO_LOG)) {
    // do nothing
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_COMMIT_INFO_LOG)) {
    sub_state_.set_info_log_submitted();
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_PREPARE_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
    exec_info_.prepare_version_ = SCN::max(log_cb->get_log_ts(), exec_info_.prepare_version_);
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_COMMIT_LOG)) {
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
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_ABORT_LOG)) {
    sub_state_.set_state_log_submitting();
    sub_state_.set_state_log_submitted();
  }
  if (OB_SUCC(ret) && bitmap_is_contain(ObTxLogType::TX_CLEAR_LOG)) {
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
    TRANS_LOG(INFO, "after submit log success", K(ret), K(trans_id_), K(ls_id_), K(exec_info_), K(*log_cb), KPC(this));
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
  reuse_log_block_(log_block);
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
    bool empty = false;
    { // try fast_path
      ObSpinLockGuard guard(log_cb_lock_);
      empty = free_cbs_.is_empty();
      if (!empty) {
        if (OB_ISNULL(log_cb = free_cbs_.remove_first())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected null log cb", KR(ret), K(*this));
        }
      }
    }
    if (empty) { // slowpath
      CtxLockGuard guard;
      if (!lock_.is_locked_by_self()) {
        get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
      }
      if (OB_FAIL(extend_log_cbs_(log_cb))) {
        if (OB_TX_NOLOGCB != ret) {
          TRANS_LOG(WARN, "extend log callback failed", K(ret));
        }
        // rewrite ret
        ret = OB_TX_NOLOGCB;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(log_cb)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected log callback", K(ret));
    } else {
      log_cb->reuse();
    }
  }
  return ret;
}

int ObPartTransCtx::return_redo_log_cb(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  if (log_cb == &final_log_cb_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "redo log cb should not be final_log_cb", K(ret));
  } else {
    ret = return_log_cb_(log_cb);
  }
  return ret;
}

int ObPartTransCtx::return_log_cb_(ObTxLogCb *log_cb, bool release_final_cb)
{
  int ret = OB_SUCCESS;
  if (NULL != log_cb) {
    if ((&final_log_cb_) != log_cb) {
      log_cb->reuse();
      ObSpinLockGuard guard(log_cb_lock_);
      free_cbs_.add_first(log_cb);
    } else if (release_final_cb) {
      log_cb->reuse();
    } else {
      // for final_log_cb_, it will be used only once, we don't reuse it
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
  } else
  {
    lsn.reset();
  }

  return ret;
}

int ObPartTransCtx::get_prev_log_lsn_(const ObTxLogBlock &log_block,
                                      ObTxPrevLogType &prev_log_type,
                                      palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  palf::LSN tmp_lsn;
  SCN tmp_log_ts;
  bool in_same_block = false;

  if (!prev_log_type.is_normal_log() || !log_block.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(INFO, "invalid arguments", K(ret), K(prev_log_type), K(log_block));
  } else if (is_contain(log_block.get_cb_arg_array(), prev_log_type.convert_to_tx_log_type())) {
    // invalid lsn
    lsn.reset();
    prev_log_type.set_self();
  } else if (OB_FAIL(get_max_submitting_log_info_(tmp_lsn, tmp_log_ts))) {
  } else if (tmp_lsn.is_valid()) {
    if (exec_info_.max_durable_lsn_.is_valid() && exec_info_.max_durable_lsn_ > tmp_lsn) {
      tmp_lsn = exec_info_.max_durable_lsn_;
    }
    lsn = tmp_lsn;
  } else {
    lsn = exec_info_.max_durable_lsn_;
  }

  if (OB_SUCC(ret) && !lsn.is_valid() && is_transfer_ctx(ctx_source_) /*is_transfer*/) {
    prev_log_type.set_tranfer_in();
  }

  if (!prev_log_type.is_valid() || (prev_log_type.is_normal_log() && !lsn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected prev lsn", K(ret), K(log_block), K(prev_log_type), K(lsn),
              KPC(this));
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
  participant_id = -1;

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
  need_replay = true;
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

  if (OB_SUCC(ret)) {
    if (need_replay && !create_ctx_scn_.is_valid()) {
      create_ctx_scn_ = timestamp;
    }

    if (need_replay) {
      update_rec_log_ts_(true/*for_replay*/, timestamp);
    }
  }

  return ret;
}

int ObPartTransCtx::push_replaying_log_ts(const SCN log_ts_ns, const int64_t log_entry_no)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (transfer_deleted_) {
    // just for check
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx ctx is transfer deleted", KR(ret), K(trans_id_));
  } else if (log_ts_ns < exec_info_.max_applying_log_ts_) {
    TRANS_LOG(WARN,
              "[Replay Tx] replay a log with a older ts than part_ctx state, it will be ignored",
              K(exec_info_.max_applying_log_ts_), K(log_ts_ns));
  } else if (log_ts_ns > exec_info_.max_applying_log_ts_) {
    exec_info_.max_applying_log_ts_ = log_ts_ns;
    exec_info_.max_applying_part_log_no_ = 0;
  }
  if (OB_SUCC(ret)) {
    if (!ctx_tx_data_.get_start_log_ts().is_valid()) {
      ctx_tx_data_.set_start_log_ts(log_ts_ns);
    }
    if (OB_UNLIKELY(replay_completeness_.is_unknown())) {
      const bool replay_continous = exec_info_.next_log_entry_no_ == log_entry_no;
      set_replay_completeness_(replay_continous, log_ts_ns);
    }
  }
  return ret;
}

int ObPartTransCtx::push_replayed_log_ts(const SCN log_ts_ns,
                                         const palf::LSN &offset,
                                         const int64_t log_entry_no)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (exec_info_.max_applied_log_ts_ < log_ts_ns) {
    exec_info_.max_applied_log_ts_ = log_ts_ns;
  }

  if (!exec_info_.max_durable_lsn_.is_valid() || offset > exec_info_.max_durable_lsn_) {
    exec_info_.max_durable_lsn_ = offset;
  }

  if (log_entry_no >= exec_info_.next_log_entry_no_) {
    // In ActiveInfoLog is replayed, its log_entry_no is the final
    // of last leader, set the next_log_entry_no for new leader
    exec_info_.next_log_entry_no_ = log_entry_no + 1;
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

  if (create_ctx_scn_ == commit_log_ts) {
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

void ObPartTransCtx::force_no_need_replay_checksum(const bool parallel_replay,
                                                   const share::SCN &log_ts)
{
  if (ATOMIC_LOAD(&exec_info_.need_checksum_)) {
    CtxLockGuard guard(lock_);
    force_no_need_replay_checksum_(parallel_replay, log_ts);
  }
}

void ObPartTransCtx::force_no_need_replay_checksum_(const bool parallel_replay,
                                                    const share::SCN &log_ts)
{
  if (ATOMIC_LOAD(&exec_info_.need_checksum_)) {
    TRANS_LOG(INFO, "set skip calc checksum", K_(trans_id), K_(ls_id), KP(this), K(parallel_replay), K(log_ts));
    if (parallel_replay) {
      update_rec_log_ts_(true, log_ts);
    }
    ATOMIC_STORE(&exec_info_.need_checksum_, false);
    mt_ctx_.set_skip_checksum_calc();
  }
}

void ObPartTransCtx::check_no_need_replay_checksum(const SCN &log_ts, const int index)
{
  // TODO(handora.qc): How to lock the tx_ctx

  // checksum_scn_ means all data's checksum has been calculated before the
  // log of checksum_scn_(not included). So if the data with this scn is
  // not replayed with checksum_scn_ <= scn, it means may exist some data
  // will never be replayed because the memtable will filter the data.

  if (ATOMIC_LOAD(&exec_info_.need_checksum_)) {
    bool need_skip = true;
    bool serial_replay = index == 0;
    // the serial replay or the parallel replay in tx-log queue
    if (serial_replay) {
      const share::SCN serial_final_scn = exec_info_.serial_final_scn_.atomic_load();
      serial_replay = !serial_final_scn.is_valid() || log_ts <= serial_final_scn;
      // the log is before serial final point, if either of checksum_scn larger than
      // the log_ts, the checksum must contains the log_ts
      if (serial_replay) {
        CtxLockGuard guard(lock_); // acquire lock for access array
        ARRAY_FOREACH_NORET(exec_info_.checksum_scn_, i) {
          if (exec_info_.checksum_scn_.at(i) > log_ts) {
            need_skip = false;
            break;
          }
        }
      }
    }
    // for parallel replay, check the corresponding list's checksum_scn
    if (!serial_replay) {
      CtxLockGuard guard(lock_); // acquire lock for access array
      if (exec_info_.checksum_scn_.count() <= index) {
        // the checksum is not exist
      } else if (exec_info_.checksum_scn_.at(index) > log_ts) {
        need_skip = false;
      }
    }
    if (need_skip) {
      CtxLockGuard guard(lock_); // acquire lock for display ctx
      force_no_need_replay_checksum_(index != 0, log_ts);
      TRANS_LOG(INFO, "skip checksum, because checksum calc not continous",
                K(serial_replay), K(index), K(log_ts), KPC(this));
    }
  }
}

/*
 * since 4.2.4, redo_lsns can not be maintained on follower with order
 * because the redo were replay parallelly, instead, redo_lsns only
 * maintained on leader and when switch to follower, it will persistent
 * redo_lsns with `RecordLog`
 */
int ObPartTransCtx::check_and_merge_redo_lsns_(const palf::LSN &offset)
{
  int ret = OB_SUCCESS;
  if (!is_support_parallel_replay_()) {
    int64_t cnt = exec_info_.redo_lsns_.count();
    // TODO merge without same offset
    if (cnt != 0 && exec_info_.redo_lsns_[cnt - 1] == offset) {
      TRANS_LOG(INFO, "repeated redo log offset", K(offset), K(exec_info_.redo_lsns_), K(trans_id_));
    } else if (OB_FAIL(exec_info_.redo_lsns_.push_back(offset))) {
      TRANS_LOG(WARN, "redo log offset push back error", K(ret), K(offset), K(trans_id_));
    }
  }
  return ret;
}

/*
 * replay redo in tx ctx
 *
 * since 4.2.4, support parallel replay redo, and the design principle is
 * seperate redo and other logs(named as Txn's Log), redo is belongs to
 * memtable (and locktable), and only Txn's Log will replay into Tx ctx
 * and affect the Tx ctx's state
 *
 * for compatible with old version, redo belongs to `tx_log_queue` will
 * still handle same as Txn's Log
 */
int ObPartTransCtx::replay_redo_in_ctx(const ObTxRedoLog &redo_log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
                                       const int64_t &part_log_no,
                                       const bool is_tx_log_queue,
                                       const bool serial_final,
                                       const ObTxSEQ &max_seq_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_redo_in_ctx", 10 * 1000);
  {
    CtxLockGuard guard(lock_);
    // before 4.2.4, cluster_version is in RedoLog, and
    // the cluster_version_ is initialized to CURRENT_CLUSTER_VERSION when ctx created
    // it should be correct with in redo
    // since 4.2.4, cluster_version is in LogBlockHeader, so the cluster_version is correct
    // when created, and cluster_version in RedoLog is 0
    if (redo_log.get_cluster_version() > 0) {
      ret = correct_cluster_version_(redo_log.get_cluster_version());
    }

    // if we need calc checksum, must don't recycle redo log ts before they were replayed
    // otherwise the checksum scn in TxCtx checkpoint will be lag behind recovery scn
    // and after the restart, the txn's checksum verify will be skipped
    //
    // example:
    //
    // Log sequence of Txn is : 1 -> 2 -> 3 -> 4
    // where 1, 4 in queue 0 (aka tx-log-queue), 2 in queue 2 and 3 in queue 3
    // because of parallel replaying, assume queue 0 replayed 4, queue 2 and 3 not
    // replayed 2, 3 yet, then in this moment, a checkpoint are issued, the checksum
    // calculate for queue 2 and queue 3 will missing data of log 2 and 3
    // after checkpoint, and 2, 3 replayed, the system will recycle logs 1-4,
    // after a restart, recovery from log queue after 4, and 2,3 will not be replayed
    // finally the checksum of queue 2,3 not include log sequence 2,3
    //
    // the cons of this choice is after restart, the log recycle position
    // will be more older, which cause do more times checkpoint of TxCtx
    //
    if (OB_SUCC(ret) && !is_tx_log_queue && exec_info_.need_checksum_) {
      update_rec_log_ts_(true, timestamp);
    }
    // if this is serial final redo log
    // change the logging to parallel logging
    if (OB_SUCC(ret) && serial_final) {
      if (!is_tx_log_queue) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "serial final redo must be in tx_log_queue", K(ret), KPC(this), K(timestamp));
#ifdef ENABLE_DEBUG_LOG
        usleep(50_ms);
        ob_abort();
#endif
      } else if (!exec_info_.serial_final_scn_.is_valid()) {
        ret = switch_to_parallel_logging_(timestamp, max_seq_no);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObTransStatistic::get_instance().add_redo_log_replay_count(tenant_id_, 1);
    ObTransStatistic::get_instance().add_redo_log_replay_time(tenant_id_, timeguard.get_diff());

    // for redo compound with Txn Log, go with old ways, this can also handle replay of old version
    if (is_tx_log_queue) {
      replay_redo_in_ctx_compat_(redo_log, offset, timestamp, part_log_no);
    }
  }

#ifndef NDEBUG
  TRANS_LOG(INFO, "[Replay Tx] Replay Redo in TxCtx", K(ret),
            K(is_tx_log_queue), K(timestamp), K(offset), KPC(this));
#endif
  return ret;
}

int ObPartTransCtx::replay_redo_in_ctx_compat_(const ObTxRedoLog &redo_log,
                                               const palf::LSN &offset,
                                               const SCN &timestamp,
                                               const int64_t &part_log_no)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::INVALID;
  bool need_replay = true;

  CtxLockGuard guard(lock_);

  if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
    TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", K(redo_log), K(timestamp), K(offset), K(*this));
    // no need to replay
  } else if (OB_FAIL((update_replaying_log_no_(timestamp, part_log_no)))) {
    TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp));
  } else {
    // TODO add clog_encrypt_info_
    UNUSED(set_upstream_state(ObTxState::INIT));
    if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(offset))) {
      TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, replay_redo, OB_ID(ret), ret,
                                          OB_Y(need_replay), OB_ID(offset), offset.val_,
                                          OB_ID(t), timestamp, OB_ID(ref), get_ref());
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[Replay Tx] Replay Redo in TxCtx Failed", K(ret), K(timestamp), K(offset),
              K(need_replay), K(redo_log), K(*this));
  }
  return ret;
}

//
// Replay RollbackToLog
// the RollbackToLog operate on memtable, its replay seperate two step
// Step1: add UndoAction to TxData
// Step2: rollback(remove) data on memtable
//
// for Step1, repeatedly replay is handle here
// for Step2, it must be executed even Step1 is should be skipped
//
// When `Branch Savepoint` used, RollbackToLog can be replayed parallelly
// in this situation, Step1 can not be handled efficiently, so repeatedly
// replay such RollbackToLog is possible, maybe use TxData to deduplicate
// is possible.
//
//
int ObPartTransCtx::replay_rollback_to(const ObTxRollbackToLog &log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
                                       const int64_t &part_log_no,
                                       const bool is_tx_log_queue,
                                       const bool pre_barrier)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_rollback_to", 10 * 1000);
  // int64_t start = ObTimeUtility::fast_current_time();
  CtxLockGuard guard(lock_);
  bool need_replay = true;
  ObTxSEQ from = log.get_from();
  ObTxSEQ to = log.get_to();
  if (OB_UNLIKELY(from.get_branch() != to.get_branch())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid savepoint", K(log));
  }
  //
  // the log is replay in txn log queue
  // for parallel replay, a global savepoint after the serial final log
  // must set the pre-barrier replay flag
  // some branch savepoint also need this, but we can't distinguish
  // hence only sanity check for global savepoint
  //
  else if (is_tx_log_queue) { // global savepoint or branch level savepoint in txn-log queue
    if (is_parallel_logging()             // has enter parallel logging
        && to.get_branch() == 0           // is a global savepoint
        && timestamp > exec_info_.serial_final_scn_  // it is after the serial final log
        && !pre_barrier) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "missing pre barrier flag for parallel replay", KR(ret), K(*this));
#ifdef ENABLE_DEBUG_LOG
      usleep(5000);
      ob_abort();
#endif
    } else if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
      TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
    } else if (!need_replay) {
      TRANS_LOG(INFO, "need not replay log", K(log), K(timestamp), K(offset), K(*this));
    } else if (OB_FAIL((update_replaying_log_no_(timestamp, part_log_no)))) {
      TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
    }
  } else { // branch level savepoint, parallel replayed
    if (exec_info_.need_checksum_ && !has_replay_serial_final_()) {
      if (OB_UNLIKELY(pre_barrier || replay_completeness_.is_incomplete())) {
        // sanity check, if current is pre-barrier, then
        // either serial final log must been replayed
        // or the txn must been marked not `need_checksum`
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "replay branch savepoint hit bug", K(ret), KP(this),
                  K(from), K(to), K(pre_barrier), K_(trans_id), K_(ls_id), K_(replay_completeness), K_(exec_info));
      } else if (replay_completeness_.is_complete()) {
        ret = OB_EAGAIN;
        if (TC_REACH_TIME_INTERVAL(1_s)) {
          TRANS_LOG(INFO, "branch savepoint should wait replay serial final because of calc checksum",
                    K(ret), K(from), K(to), K(timestamp), KP(this), K_(trans_id), K_(ls_id), K_(exec_info));
        }
      } else if (replay_completeness_.is_unknown()) {
        // try to fetch the replay position of replay-queue of txn-log
        // to determin whether previouse txn log has been replayed or won't be replayed
        // if so, can decide that the current txn was replayed from middle, and mark it
        // replay incomplete
        share::SCN min_unreplayed_scn;
        logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
        if (OB_ISNULL(log_service)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "tenant logservice is null", K(ret), K(timestamp), K_(trans_id));
        } else if (OB_FAIL(log_service->get_log_replay_service()->get_min_unreplayed_scn(ls_id_, min_unreplayed_scn))) {
          TRANS_LOG(WARN, "get min unreplayed scn fail", K(ret), K(timestamp), K_(trans_id));
        } else if (min_unreplayed_scn == timestamp) {
          // all previous log replayed
          // the txn must not replay from its first log, aka. incomplete-replay
          TRANS_LOG(INFO, "detect txn replayed from middle", K(ret), K(timestamp), K_(trans_id), K_(ls_id), K_(exec_info));
          set_replay_completeness_(false, timestamp);
          ret = OB_SUCCESS;
        } else if (min_unreplayed_scn > timestamp) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "incorrect min unreplayed scn", K(ret), K(timestamp), K(min_unreplayed_scn), K_(trans_id));
        } else {
          ret = OB_EAGAIN;
          if (TC_REACH_TIME_INTERVAL(1_s)) {
            TRANS_LOG(INFO, "branch savepoint should wait replay serial final because of calc checksum",
                      K(ret), K(from), K(to), K(timestamp), K(min_unreplayed_scn), KP(this), K_(trans_id), K_(ls_id), K_(exec_info));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "code should not go here", K(ret), K(timestamp), K_(trans_id), KPC(this));
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
    }
    if (OB_SUCC(ret) &&
        !ctx_tx_data_.get_start_log_ts().is_valid() &&
        OB_FAIL(ctx_tx_data_.set_start_log_ts(timestamp))) {
      // update start_log_ts for branch savepoint, because it may replayed before first log in txn queue
      TRANS_LOG(WARN, "set tx data start log ts fail", K(ret), K(timestamp), KPC(this));
    }
  }

  //
  // Step1, add Undo into TxData, both for parallel replay and serial replay
  //
  if (OB_SUCC(ret) && need_replay && OB_FAIL(rollback_to_savepoint_(log.get_from(), log.get_to(), timestamp))) {
    TRANS_LOG(WARN, "replay savepoint_rollback fail", K(ret), K(log), K(offset), K(timestamp),
              KPC(this));
  }

  // this is compatible code, since 4.2.4, redo_lsn not collect during replay
  if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(offset))) {
    TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
  }

  //
  // Step2, remove TxNode(s)
  //
  if (OB_SUCC(ret) && !need_replay) {
    if (OB_FAIL(mt_ctx_.rollback(log.get_to(), log.get_from(), timestamp))) {
      TRANS_LOG(WARN, "mt ctx rollback fail", K(ret), K(log), KPC(this));
    }
  }

  if (OB_FAIL(ret) && OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "[Replay Tx] Replay RollbackToLog in TxCtx Failed", K(timestamp), K(offset),
              K(ret), K(need_replay), K(log), KPC(this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay RollbackToLog in TxCtx", K(timestamp), K(offset), K(ret),
              K(need_replay), K(log), KPC(this));
#endif
  }

  if (OB_EAGAIN != ret) {
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
  }

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
  }  else if (log.get_cluster_version() > 0 &&
    OB_FAIL(correct_cluster_version_(log.get_cluster_version()))) {
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

    exec_info_.max_submitted_seq_no_.inc_update(log.get_max_submitted_seq_no());
    exec_info_.serial_final_seq_no_ = log.get_serial_final_seq_no();
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

int ObPartTransCtx::assign_commit_parts(const share::ObLSArray &log_participants,
                                        const ObTxCommitParts &log_commit_parts)
{
  int ret = OB_SUCCESS;

  if (log_participants.count() != log_commit_parts.count()) {
    // replay old version log, we need mock the commit parts
    for (int64_t i = 0; OB_SUCC(ret) && i < log_participants.count(); i++) {
      if (OB_FAIL(exec_info_.commit_parts_.push_back(ObTxExecPart(log_participants[i],
                                                                  -1, /*exec_epoch*/
                                                                  -1  /*transfer_epoch*/)))) {
        TRANS_LOG(WARN, "set commit parts error", K(ret), K(*this));
      }
    }

    if (OB_FAIL(ret)) {
      // reset on failure to ensure atomicity
      exec_info_.commit_parts_.reset();
    }
  } else {
    if (OB_FAIL(exec_info_.commit_parts_.assign(log_commit_parts))) {
      TRANS_LOG(WARN, "set commit parts error", K(ret), K(*this));
    }
  }

  return ret;
}

int ObPartTransCtx::replay_commit_info(const ObTxCommitInfoLog &commit_info_log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
                                       const int64_t &part_log_no,
                                       const bool pre_barrier)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_commit_info", 10 * 1000);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;
  CtxLockGuard guard(lock_);
  if (is_parallel_logging() && !pre_barrier) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "missing pre barrier flag for parallel replay", KR(ret), K(*this));
#ifdef ENABLE_DEBUG_LOG
    usleep(5000);
    ob_abort();
#endif
  } else if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
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
  } else if (OB_FAIL(assign_commit_parts(commit_info_log.get_participants(),
                                         commit_info_log.get_commit_parts()))) {
    TRANS_LOG(WARN, "set commit parts error", K(ret), K(commit_info_log), K(*this));
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

    can_elr_ = commit_info_log.is_elr();
    if (commit_info_log.get_cluster_version() > 0) {
      ret = correct_cluster_version_(commit_info_log.get_cluster_version());
    }
    sub_state_.set_info_log_submitted();
    epoch_ = commit_info_log.get_epoch();
    reset_redo_lsns_();
    ObTwoPhaseCommitLogType two_phase_log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_MAX;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_local_tx_()) {
      set_durable_state_(ObTxState::REDO_COMPLETE);
      set_upstream_state(ObTxState::REDO_COMPLETE);
    } else if (replay_completeness_.is_incomplete()) {
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
      if (OB_FAIL(dup_table_before_preapre_(
              timestamp, false /*after_redo_completed*/,
              ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
        TRANS_LOG(WARN, "set commit_info scn as before_prepare_version failed", K(ret), KPC(this));
      }

      // if (OB_TMP_FAIL(post_redo_sync_ts_response_(timestamp))) {
      //   TRANS_LOG(WARN, "post dup table redo sync response failed", K(ret), K(tmp_ret), K(timestamp),
      //             KPC(this));
      // }
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
    if (replay_completeness_.is_incomplete()) {
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
  CtxLockGuard guard(lock_);
  // const int64_t start = ObTimeUtility::fast_current_time();
  const SCN commit_version = commit_log.get_commit_version();
  bool need_replay = true;

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
          && replay_completeness_.is_incomplete()) {
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
        if (replay_completeness_.is_incomplete()) {
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
        (exec_info_.need_checksum_ && replay_completeness_.is_complete() ? commit_log.get_checksum() : 0);
    mt_ctx_.set_replay_compact_version(replay_compact_version);

    if (OB_FAIL(notify_table_lock_(timestamp,
                                   true,
                                   exec_info_.multi_data_source_,
                                   false /* not a force kill */))) {
      TRANS_LOG(WARN, "notify table lock failed", KR(ret), "context", *this);
    } else if (OB_FAIL(notify_data_source_(NotifyType::ON_COMMIT,
                                           timestamp,
                                           true,
                                           exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(commit_log));
    } else if (OB_FAIL(trans_replay_commit_(ctx_tx_data_.get_commit_version(),
                                            timestamp,
                                            cluster_version_,
                                            checksum))) {
      TRANS_LOG(WARN, "trans replay commit failed", KR(ret), K(commit_log), KPC(this));
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
  CtxLockGuard guard(lock_);
  // const int64_t start = ObTimeUtility::fast_current_time();
  bool need_replay = true;
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
      if (replay_completeness_.is_incomplete()) {
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
          && replay_completeness_.is_incomplete()) {
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
        if (replay_completeness_.is_incomplete()) {
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
    if (OB_FAIL(mds_cache_.generate_final_notify_array(
            exec_info_.multi_data_source_, true /*need_merge_cache*/, true /*allow_log_overflow*/))) {
      TRANS_LOG(WARN, "gen total mds array failed", K(ret));
    } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END, timestamp, true,
                                           exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "notify data source for TX_END failed", KR(ret), K(*this));
    } else if (OB_FAIL(trans_replay_abort_(timestamp))) {
      TRANS_LOG(WARN, "transaction replay end error", KR(ret), "context", *this);
    } else if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
    } else if (OB_FAIL(notify_data_source_(NotifyType::ON_ABORT, timestamp, true,
                                           mds_cache_.get_final_notify_array()))) {
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
    if (replay_completeness_.is_incomplete()) {
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
  } else if (OB_FAIL(replay_mds_to_tx_table_(increamental_array, timestamp))) {
    TRANS_LOG(WARN, "insert mds_op to tx_table failed", K(ret));
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
        mds_cache_.free_mds_node(node.data_, node.get_register_no());
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
  } else if (OB_UNLIKELY(replay_completeness_.is_incomplete()) && (get_retain_cause() == RetainCause::UNKOWN)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "incomplete replayed ctx should not switch to leader", K(ret), KPC(this));
    print_trace_log_();
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_ls_log_adapter()->get_append_mode_initial_scn(
                 append_mode_initial_scn))) {
    /* We can not ensure whether there are some redo logs after the append_mode_initial_scn.
     * All running trans must be be killed by the append_mode_initial_scn.*/
    TRANS_LOG(WARN, "get append mode initial scn from the palf failed", K(ret), KPC(this));
  } else if (OB_FAIL(state_helper.switch_state(TxCtxOps::TAKEOVER))) {
    TRANS_LOG(WARN, "switch role state error", KR(ret), K(*this));
  } else {
    const bool contain_mds_table_lock = is_contain_mds_type_(ObTxDataSourceType::TABLE_LOCK);
    const bool contain_mds_transfer_out = is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT)
                           || is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT_PREPARE)
                           || is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT_V2);
    const bool need_kill_tx = contain_mds_table_lock || contain_mds_transfer_out;
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
                      K(contain_mds_table_lock), K(contain_mds_transfer_out), K(need_kill_tx),
                      K(kill_by_append_mode_initial_scn), K(append_mode_initial_scn), KPC(this));
            if (OB_FAIL(do_local_tx_end_(TxEndAction::ABORT_TX))) {
              //Temporary fix:
              //The transaction cannot be killed temporarily, waiting for handle_timeout to retry abort.
              //Do not block the rest of the transactions from taking over.
              if (OB_TRANS_CANNOT_BE_KILLED == ret) {
                TRANS_LOG(
                    INFO,
                    "The transaction cannot be killed temporarily, waiting for handle_timeout to retry abort.",
                    K(ret), KPC(this));
                ret = OB_SUCCESS;
              } else if (is_2pc_blocking()) {
                // when tx is blocked by transfer, we need wait handle_timeout to retry abort
                // not to block the rest of the transactions from taking over
                TRANS_LOG(INFO, "tx has blocked by transfer, waiting for handle_timeout to retry abort",
                    K(ret), KPC(this));
                ret = OB_SUCCESS;
              }
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
      exec_info_.is_empty_ctx_created_by_transfer_ = false;
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
int ObPartTransCtx::switch_to_follower_forcedly(ObTxCommitCallback *&cb_list_head)
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
        if (OB_FAIL(prepare_commit_cb_for_role_change_(OB_TRANS_KILLED, cb_list_head))) {
          TRANS_LOG(WARN, "prepare commit cb fail", K(ret), KPC(this));
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
      } else if (OB_FAIL(mt_ctx_.clean_unlog_callbacks())) {
        TRANS_LOG(WARN, "clear unlog callbacks", KR(ret), K(*this));
      }

      if (is_local_tx_() &&
          !is_logging_() &&
          !sub_state_.is_state_log_submitted()) {
        mt_ctx_.set_trans_version(SCN::max_scn());
        TRANS_LOG(INFO, "clear local tx trans version when switch to follower forcely", KP(this));
      }

      if (OB_SUCC(ret) && exec_info_.is_dup_tx_ && get_downstream_state() == ObTxState::REDO_COMPLETE
          && !sub_state_.is_state_log_submitted()) {
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(dup_table_before_preapre_(
                       exec_info_.max_applied_log_ts_, true /*after_redo_completed*/,
                       ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
          TRANS_LOG(WARN, "set commit_info scn as befre_prepare_version failed", K(ret), KPC(this));
        } else if (OB_FAIL(clear_dup_table_redo_sync_result_())) {
          TRANS_LOG(WARN, "clear redo sync result failed", K(ret));
        }
      }

      // special handle commit triggered by local call: coordinator colocate with scheduler
      // let scheduler retry commit with RPC if required
      if (need_callback_scheduler_()) {
        int commit_ret = OB_SUCCESS;
        // no CommitInfoLog has been submitted, txn must abort
        if (exec_info_.state_ == ObTxState::INIT && !sub_state_.is_info_log_submitted()) {
          commit_ret = OB_TRANS_KILLED;
        } else {
          // otherwise, txn either continue commit or abort, need retry to get final result
          commit_ret = OB_NOT_MASTER;
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(prepare_commit_cb_for_role_change_(commit_ret, cb_list_head))) {
          TRANS_LOG(WARN, "prepare commit cb fail", K(tmp_ret), KPC(this));
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        } else {
          TRANS_LOG(INFO, "switch to follower forcely, notify txn commit result to scheduler",
                    "commit_result", commit_ret, KPC(this));
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

int ObPartTransCtx::switch_to_follower_gracefully(ObTxCommitCallback *&cb_list_head)
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
        if (OB_FAIL(prepare_commit_cb_for_role_change_(OB_SWITCHING_TO_FOLLOWER_GRACEFULLY, cb_list_head))) {
          TRANS_LOG(WARN, "prepare commit cb fail", K(ret), KPC(this));
        } else {
          TRANS_LOG(INFO, "swtich to follower gracefully, notify scheduler retry", KPC(this));
        }
      }
    }
    timeguard.click();
    if (OB_SUCC(ret) && need_submit_log && !need_force_abort_()) {
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
      } else if (OB_FAIL(dup_table_before_preapre_(
                     exec_info_.max_applied_log_ts_, true /*after_redo_completed*/,
                     ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::REDO_COMPLETE_SCN))) {
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
    TRANS_LOG(WARN, "switch to follower gracefully failed", KR(ret), KPC(this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "switch to follower gracefully succeed", KPC(this));
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
  // after previous checkpoint.
  if (for_replay) {
    // follower may support parallel replay redo, so must do dec update
    if (!rec_log_ts_.is_valid()) {
      rec_log_ts_ = rec_log_ts;
    } else if (rec_log_ts_ > rec_log_ts){
      rec_log_ts_ = rec_log_ts;
    }
  } else {
    if (!rec_log_ts_.is_valid()) {
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
  exec_info_.exec_epoch_ = epoch_;
  // leave target_scn to MAX and the callee will choose the greatest
  // calculable scn, especially when parallel replay, the max scn of
  // a parallel replayed callback-list will be carefully choosen to
  // ensure checksum calculation was continous
  share::SCN target_scn = share::SCN::max_scn();
  if (OB_FAIL(ctx_tx_data_.get_tx_data(info.tx_data_guard_))) {
    TRANS_LOG(WARN, "get tx data failed", K(ret));
  } else if (exec_info_.need_checksum_ &&
             OB_FAIL(mt_ctx_.calc_checksum_before_scn(target_scn,
                 exec_info_.checksum_, exec_info_.checksum_scn_))) {
    TRANS_LOG(ERROR, "calc checksum before log ts failed", K(ret), KPC(this));
  } else if (OB_FAIL(exec_info_.generate_mds_buffer_ctx_array())) {
    TRANS_LOG(WARN, "fail to generate mds buffer ctx array", K(ret), KPC(this));
  } else if (OB_FAIL(info.exec_info_.assign(exec_info_))) {
    TRANS_LOG(WARN, "fail to assign exec_info", K(ret), KPC(this));
  } else {
    info.tx_id_ = trans_id_;
    info.ls_id_ = ls_id_;
    info.cluster_id_ = cluster_id_;
    if (cluster_version_accurate_) {
      info.cluster_version_ = cluster_version_;
    } else {
      info.cluster_version_ = 0;
    }
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

int ObPartTransCtx::gen_total_mds_array_(ObTxBufferNodeArray &mds_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mds_cache_.generate_final_notify_array(
          exec_info_.multi_data_source_, true /*need_merge_cache*/, true /*allow_log_overflow*/))) {
    TRANS_LOG(WARN, "generate final notify array failed", K(ret), KPC(this));
  } else if (OB_FAIL(mds_array.assign(mds_cache_.get_final_notify_array()))) {
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
      TRANS_LOG(DEBUG, "old mds type, no need process with buffer ctx",
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

  // void *ptr = nullptr;
  int64_t len = 0;

  if (OB_FAIL(tmp_buf_arr.reserve(additional_count))) {
    TRANS_LOG(WARN, "reserve array space failed", K(ret));
  } else if(OB_FAIL(incremental_array.reserve(additional_count))) {
    TRANS_LOG(WARN, "reserve incremental_array space failed", K(ret));
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
      ObString tmp_data;
      if (OB_FAIL(mds_cache_.alloc_mds_node(this, node.data_.ptr(), len, tmp_data, node.get_register_no()))) {
        TRANS_LOG(WARN, "alloc mds node from the mds_cache_ failed", K(ret), K(mds_cache_), KPC(this));
      } else {
      // if (OB_ISNULL(ptr = mtl_malloc(len, ""))) {
      //   ret = OB_ALLOCATE_MEMORY_FAILED;
      //   TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(*this), K(len));
      // } else {
        // MEMCPY(ptr, node.data_.ptr(), len);
        ObTxBufferNode new_node;
        // ObString data;
        // data.assign_ptr(reinterpret_cast<char *>(ptr), len);
        mds::BufferCtx *new_ctx = nullptr;
        if (OB_FAIL(process_with_buffer_ctx(node, new_ctx))) {
          mds_cache_.free_mds_node(tmp_data, node.get_register_no());
          // mtl_free(tmp_data.ptr());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService*)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "process_with_buffer_ctx failed", KR(ret), K(*this));
        } else if (OB_FAIL(new_node.init(node.get_data_source_type(), tmp_data, node.mds_base_scn_,
                                         node.seq_no_, new_ctx))) {
          mds_cache_.free_mds_node(tmp_data, node.get_register_no());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "init new node failed", KR(ret), K(*this));
        } else if (ObTxBufferNode::is_valid_register_no(node.get_register_no())
                   && OB_FAIL(new_node.set_mds_register_no(node.get_register_no()))) {
          mds_cache_.free_mds_node(tmp_data, node.get_register_no());
          // mtl_free(tmp_data.ptr());
          if (OB_NOT_NULL(new_ctx)) {
            MTL(mds::ObTenantMdsService *)->get_buffer_ctx_allocator().free(new_ctx);
            new_ctx = nullptr;
          }
          TRANS_LOG(WARN, "set mds register_no failed", KR(ret), K(*this));
        } else if (OB_FAIL(tmp_buf_arr.push_back(new_node))) {
          mds_cache_.free_mds_node(tmp_data, node.get_register_no());
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
        mds_cache_.free_mds_node(tmp_buf_arr[i].data_, tmp_buf_arr[i].get_register_no());
        tmp_buf_arr[i].buffer_ctx_node_.destroy_ctx();
      }
      tmp_buf_arr.reset();
    }
  }

  if (OB_FAIL(ret)) {

  } else if (need_replace) {

    for (int64_t i = 0; i < exec_info_.multi_data_source_.count(); ++i) {
      if (nullptr != exec_info_.multi_data_source_[i].data_.ptr()) {
        mds_cache_.free_mds_node(exec_info_.multi_data_source_[i].data_,
                                 exec_info_.multi_data_source_[i].get_register_no());
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
          mds_cache_.free_mds_node(tmp_buf_arr[i].data_, tmp_buf_arr[i].get_register_no());
          // mtl_free(tmp_buf_arr[i].data_.ptr());
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

int ObPartTransCtx::prepare_mds_tx_op_(const ObTxBufferNodeArray &mds_array,
                                       SCN op_scn,
                                       ObTenantTxDataOpAllocator &tx_op_allocator,
                                       ObTxOpArray &tx_op_array,
                                       bool is_replay)
{
  int ret = OB_SUCCESS;
  int64_t dest_max_register_no = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < mds_array.count(); i++) {
    const ObTxBufferNode &node = mds_array.at(i);
    ObTxBufferNodeWrapper *new_node_wrapper = nullptr;
    mds::BufferCtx *new_ctx = nullptr;
    ObTxOp tx_op;
    tx_op_allocator.reset_local_alloc_size();
    if (node.allow_to_use_mds_big_segment()) {
      // do nothing
    } else if (OB_ISNULL(new_node_wrapper = (ObTxBufferNodeWrapper*)(tx_op_allocator.alloc(sizeof(ObTxBufferNodeWrapper))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", KR(ret));
    } else if (FALSE_IT(new(new_node_wrapper) ObTxBufferNodeWrapper())) {
    } else if (!is_replay && OB_FAIL(new_node_wrapper->pre_alloc(trans_id_, node, tx_op_allocator))) {
      TRANS_LOG(WARN, "pre_alloc failed", KR(ret), KPC(this));
    } else if (is_replay && OB_FAIL(new_node_wrapper->assign(trans_id_, node, tx_op_allocator, false))) {
      TRANS_LOG(WARN, "assign failed", KR(ret), KPC(this));
    } else if (OB_FAIL(tx_op.init(ObTxOpCode::MDS_OP, op_scn, new_node_wrapper, tx_op_allocator.get_local_alloc_size()))) {
      TRANS_LOG(WARN, "init tx_op fail", KR(ret));
    } else if (OB_FAIL(tx_op_array.push_back(tx_op))) {
      TRANS_LOG(WARN, "push buffer_node to list fail", KR(ret));
    }
    // attention tx_op is not put into tx_op_array
    if (OB_FAIL(ret) && OB_NOT_NULL(new_node_wrapper)) {
      new_node_wrapper->~ObTxBufferNodeWrapper();
      tx_op_allocator.free(new_node_wrapper);
    }
  }
  TRANS_LOG(INFO, "prepare_mds_tx_op", K(ret), K(trans_id_), K(ls_id_), K(mds_array), K(tx_op_array), K(op_scn));
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

  // decide barrier for parallel logging
  if (OB_SUCC(ret)) {
    if (((state_log_type == ObTxLogType::TX_COMMIT_INFO_LOG)
         || (state_log_type == ObTxLogType::TX_ABORT_LOG))
      && OB_UNLIKELY(is_parallel_logging())) {
      using LogBarrierType = logservice::ObReplayBarrierType;
      switch(final_barrier_type) {
      case LogBarrierType::STRICT_BARRIER:
      case LogBarrierType::PRE_BARRIER:
        break;
      case LogBarrierType::NO_NEED_BARRIER:
        final_barrier_type = LogBarrierType::PRE_BARRIER;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected barrier type", K(final_barrier_type));
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
  if (OB_FAIL(init_log_block_(log_block))) {
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
  const int64_t replay_hint = trans_id_.get_id();
  ObTxLogCb *log_cb = nullptr;
  void *tmp_buf = nullptr;
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
      } else if (log_block.get_cb_arg_array().count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(log_block));
      } else if (OB_FAIL(ctx_tx_data_.reserve_tx_op_space(log_cb->get_mds_range().count()))) {
        TRANS_LOG(WARN, "reserve tx_op space failed", KR(ret), KPC(this));
      } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->alloc_tx_data(log_cb->get_tx_data_guard(), true, INT64_MAX))) {
        TRANS_LOG(WARN, "alloc tx_data failed", KR(ret), KPC(this));
      } else if (OB_ISNULL(tmp_buf = mtl_malloc(sizeof(ObTxOpArray), "ObTxOpArray"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memory failed", KR(ret), KPC(this));
      } else if (FALSE_IT(new (tmp_buf) ObTxOpArray())) {
      } else if (FALSE_IT(log_cb->get_tx_op_array() = (ObTxOpArray*)tmp_buf)) {
      } else if (OB_FAIL(prepare_mds_tx_op_(log_cb->get_mds_range().get_range_array(),
                                            SCN::min_scn(),
                                            *log_cb->get_tx_data_guard().tx_data()->op_allocator_,
                                            *log_cb->get_tx_op_array(),
                                            false))) {
          TRANS_LOG(WARN, "preapre tx_op failed", KR(ret), KPC(this));
      } else if (OB_FAIL(acquire_ctx_ref_())) {
        TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
        log_cb = nullptr;

      } else if ((mds_base_scn.is_valid() ? OB_FALSE_IT(mds_base_scn = share::SCN::scn_inc(mds_base_scn)) : OB_FALSE_IT(mds_base_scn.set_min()))) {
      } else if (OB_FAIL(submit_log_block_out_(log_block, mds_base_scn, log_cb, replay_hint, barrier_type))) {
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

  TRANS_LOG(DEBUG, "submit MDS redo", K(ret), K(trans_id_), KPC(log_cb), K(mds_cache_),
            K(exec_info_.multi_data_source_));

  return ret;
}

int ObPartTransCtx::prepare_mul_data_source_tx_end_(bool is_commit)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {

    if (is_commit && mds_cache_.count() > 0
        && OB_FAIL(submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG))) {
      TRANS_LOG(WARN, "submit multi data souce log failed", K(ret));
    } else if (OB_FAIL(mds_cache_.generate_final_notify_array(exec_info_.multi_data_source_,
                                                               true /*need_merge_cache*/,
                                                               true /*allow_log_overflo*/))) {
      TRANS_LOG(WARN, "copy total mds array failed", K(ret));
    } else if (OB_FAIL(notify_data_source_(NotifyType::TX_END, SCN(), false,
                                           mds_cache_.get_final_notify_array()))) {
      TRANS_LOG(WARN, "notify data source failed", KR(ret), K(*this));
    }
  }
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_DUP_TABLE_REDO_SYNC)
ERRSIM_POINT_DEF(EN_SUBMIT_TX_PREPARE_LOG)
ERRSIM_POINT_DEF(EN_NOTIFY_MDS)
#endif

int ObPartTransCtx::errism_dup_table_redo_sync_()
{

  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = EN_DUP_TABLE_REDO_SYNC;
#endif
  return ret;
}

int ObPartTransCtx::process_errsim_for_standby_read_(int err_code, int ret_code)
{
  // reject trans_id and tenant id to contorl trans sate
  int ret = OB_SUCCESS;

  int64_t rej_code = abs(err_code);
  int64_t rej_tenant_id = rej_code % 10000;
  int64_t rej_trans_id = (rej_code - rej_tenant_id) / 10000;
  if (trans_id_.get_id() == rej_trans_id && MTL_ID() == rej_tenant_id) {
    ret = ret_code;
    TRANS_LOG(INFO, "reject special code for control trans state", K(ret), K(err_code),
              K(rej_code), K(rej_tenant_id), K(rej_trans_id), K(trans_id_));
  }

  return ret;
}

OB_NOINLINE int ObPartTransCtx::errism_submit_prepare_log_()
{

  int ret = OB_SUCCESS;

  if (OB_NOT_MASTER == ERRSIM_DELAY_TX_SUBMIT_LOG && is_root()) {
    ret = ERRSIM_DELAY_TX_SUBMIT_LOG;
  } else if (OB_BLOCK_FROZEN == ERRSIM_DELAY_TX_SUBMIT_LOG && !is_root()) {
    ret = ERRSIM_DELAY_TX_SUBMIT_LOG;
  } else if (ERRSIM_DELAY_TX_SUBMIT_LOG < int(-9999)) {
    // reject trans_id and tenant id to contorl trans sate
    ret = process_errsim_for_standby_read_(int(ERRSIM_DELAY_TX_SUBMIT_LOG), OB_NOT_MASTER);
    TRANS_LOG(INFO, "reject delay tx submit log", K(ret), K(ERRSIM_DELAY_TX_SUBMIT_LOG));
  }

#ifdef ERRSIM
  ret = EN_SUBMIT_TX_PREPARE_LOG;
#endif

  return ret;
}

OB_NOINLINE int ObPartTransCtx::errsim_notify_mds_()
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = EN_NOTIFY_MDS;
#endif

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "errsim notify mds in test", K(ret));
  }

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

  if (OB_FAIL(errsim_notify_mds_())) {
    TRANS_LOG(WARN, "notify mds errsim", K(ret), K(ls_id_), K(trans_id_), K(notify_type), K(log_ts),
              K(for_replay), K(notify_array), K(is_force_kill));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_exiting_ && sub_state_.is_force_abort()) {
    // do nothing
  } else {
    ObMulSourceDataNotifyArg arg;
    arg.tx_id_ = trans_id_;
    arg.scn_ = log_ts;
    arg.trans_version_ = ctx_tx_data_.get_commit_version();
    arg.for_replay_ = for_replay;
    arg.notify_type_ = notify_type;
    arg.is_force_kill_ = is_force_kill;
    arg.is_incomplete_replay_ = replay_completeness_.is_incomplete();

    int64_t total_time = 0;

    if (OB_FAIL(
            ObMulSourceTxDataNotifier::notify(notify_array, notify_type, arg, this, total_time))) {
      TRANS_LOG(WARN, "notify data source failed", K(ret), K(arg));
    }
    if (notify_array.count() > 0) {
      TRANS_LOG(INFO, "notify MDS", K(ret), K(trans_id_), K(ls_id_),
                "notify_type", ObMultiDataSourcePrinter::to_str_notify_type(notify_type),
                K(log_ts), K(notify_array.count()), K(notify_array),
                K(total_time));
    }
  }
  return ret;
}

int ObPartTransCtx::register_multi_data_source(const ObTxDataSourceType data_source_type,
                                               const char *buf,
                                               const int64_t len,
                                               const bool try_lock,
                                               const ObTxSEQ seq_no,
                                               const ObRegisterMdsFlag &register_flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxBufferNode node;
  ObString data;
  // void *ptr = nullptr;
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
    } else if (OB_FAIL(mds_cache_.try_recover_max_register_no(exec_info_.multi_data_source_))) {
      TRANS_LOG(WARN, "recover max register no failed", K(ret), K(mds_cache_), KPC(this));
    } else if (OB_FAIL(mds_cache_.alloc_mds_node(this, buf, len, data))) {
      TRANS_LOG(WARN, "alloc mds node from the mds_cache_ failed", K(ret), K(mds_cache_), KPC(this));
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
      } else if (OB_FAIL(node.init(data_source_type, data, register_flag.mds_base_scn_, seq_no, buffer_ctx))) {
        TRANS_LOG(WARN, "init tx buffer node failed", KR(ret), K(data_source_type), K(*this));
      } else if (OB_FAIL(tmp_array.push_back(node))) {
        TRANS_LOG(WARN, "push back notify node  failed", KR(ret));
//#ifndef OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF
      } else if (tmp_array.get_serialize_size() > ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE
                 && !node.allow_to_use_mds_big_segment()) {
        ret = OB_LOG_TOO_LARGE;
        TRANS_LOG(WARN, "too large mds buf node", K(ret), K(tmp_array.get_serialize_size()));
//#endif
      } else if (OB_FAIL(mds_cache_.insert_mds_node(node))) {
        TRANS_LOG(WARN, "register multi source data failed", KR(ret), K(data_source_type),
                  K(*this));
      }

      if (OB_FAIL(ret)) {
        mds_cache_.free_mds_node(data, node.get_register_no());
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
        } else if (tmp_ret == OB_TX_NOLOGCB || tmp_ret == OB_EAGAIN) {
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

int ObPartTransCtx::dup_table_tx_redo_sync_(const bool need_retry_by_task)
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
  } else {
    if (is_dup_table_redo_sync_completed_()) {
      ret = OB_SUCCESS;
      redo_sync_finish = true;
      bool no_need_submit_log = false;
      if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
        TRANS_LOG(WARN, "do prepare failed after redo sync", K(tmp_ret), KPC(this));
      } else if (OB_TMP_FAIL(ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_())) {
        TRANS_LOG(WARN, "retry to post preapre request failed", K(tmp_ret), KPC(this));
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

        if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
          TRANS_LOG(WARN, "do prepare failed after redo sync", K(tmp_ret), KPC(this));
        } else if (OB_TMP_FAIL(ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_())) {
          TRANS_LOG(WARN, "retry to post preapre request failed", K(tmp_ret), KPC(this));
        }

      }
    } else {
      if (need_retry_by_task) {
        set_need_retry_redo_sync_by_task_();
      }
      ret = OB_EAGAIN;
      TRANS_LOG(INFO, "redo sync will retry", K(ret), K(redo_sync_finish), K(tmp_max_read_version),
                K(dup_table_follower_max_read_version_), KPC(this));
      if (OB_TMP_FAIL(restart_2pc_trans_timer_())) {
        TRANS_LOG(WARN, "set 2pc trans timer for dup table failed", K(ret), K(tmp_ret), KPC(this));
      }
      if (OB_TMP_FAIL(ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_())) {
        TRANS_LOG(WARN, "retry to post preapre request failed", K(tmp_ret), KPC(this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::submit_pending_log_block_(ObTxLogBlock &log_block,
                                              memtable::ObRedoLogSubmitHelper &helper,
                                              const logservice::ObReplayBarrierType &barrier)
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
    const int64_t replay_hint = trans_id_.get_id();
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
    } else if (OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
      TRANS_LOG(WARN, "resolve callbacks failed", K(ret), KPC(this));
      return_log_cb_(log_cb);
      log_cb = NULL;
    } else if (OB_FAIL(acquire_ctx_ref_())) {
      TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
    } else if (OB_FAIL(submit_log_block_out_(log_block, share::SCN::min_scn(), log_cb, replay_hint, barrier))) {
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
          || helper.callbacks_.count() > 0) {
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

int ObPartTransCtx::dup_table_before_preapre_(
    const share::SCN &before_prepare_version,
    const bool after_redo_completed,
    const ObDupTableBeforePrepareRequest::BeforePrepareScnSrc before_prepare_src)
{
  int ret = OB_SUCCESS;

  if (get_downstream_state() != ObTxState::REDO_COMPLETE
      || (!after_redo_completed && get_upstream_state() != ObTxState::REDO_COMPLETE)
      || (after_redo_completed && get_upstream_state() > ObTxState::PREPARE)
      || !exec_info_.is_dup_tx_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "unexpected dup trx state", K(ret), KPC(this));
  } else if (!before_prepare_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid before_prepare version", K(ret), K(before_prepare_version),
              K(after_redo_completed), KPC(this));
  } else if (mt_ctx_.get_trans_version().is_max() || !mt_ctx_.get_trans_version().is_valid()
             || mt_ctx_.get_trans_version() < before_prepare_version) {
    mt_ctx_.before_prepare(before_prepare_version);
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "set dup_table before prepare version successfully", K(ret),
              K(before_prepare_version), K(before_prepare_src), K(after_redo_completed),
              K(mt_ctx_.get_trans_version()), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::retry_dup_trx_before_prepare(
    const share::SCN &before_prepare_version,
    const ObDupTableBeforePrepareRequest::BeforePrepareScnSrc before_prepare_src)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (!is_follower_()) {
    ret = OB_NOT_FOLLOWER;
    TRANS_LOG(WARN, "leader need not handle a before_prepare retry request", K(ret),
              K(before_prepare_version), K(before_prepare_src), K(ls_id_), K(trans_id_));
  } else if (OB_FAIL(dup_table_before_preapre_(
                 before_prepare_version, false /*after_redo_completed*/, before_prepare_src))) {
    TRANS_LOG(WARN, "set dup table before_prepare_version failed", K(ret),
              K(before_prepare_version), K(before_prepare_src), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::dup_table_tx_redo_sync(const bool need_retry_by_task)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == lock_.try_lock()) {
    CtxLockGuard guard(lock_, false);

    if (!is_leader_()) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "redo sync need not execute on a follower", K(ret), KPC(this));
    } else if (is_exiting_) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "redo sync need not execute on a exiting tx_ctx", K(ret), KPC(this));
    } else if (get_downstream_state() >= ObTxState::PREPARE) {
      ret = OB_STATE_NOT_MATCH;
      TRANS_LOG(WARN, "redo sync need not execute on a prepared tx_ctx", K(ret), KPC(this));
    } else {
      if (OB_FAIL(dup_table_tx_redo_sync_(need_retry_by_task))) {
        TRANS_LOG(WARN, "execute dup table redo sync failed", K(ret), KPC(this));
      }

      if (OB_SUCC(ret)) {
        if (!is_dup_table_redo_sync_completed_()) {
          ret = OB_EAGAIN;
          // TRANS_LOG(INFO, "redo sync need retry", K(ret), KPC(this));
        } else {
          ret = OB_TRANS_HAS_DECIDED;
        }
      }
    }
  } else {
    TRANS_LOG(INFO, "The redo sync task can not acquire ctx lock. Waiting the next retry.", K(ret),
              K(get_trans_id()), K(get_ls_id()));
    ret = OB_EAGAIN;
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

int ObPartTransCtx::sub_prepare(const ObTxCommitParts &parts,
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

int ObPartTransCtx::supplement_tx_op_if_exist_(const bool for_replay, const SCN replay_scn)
{
  int ret = OB_SUCCESS;

  ObTxTable *tx_table = nullptr;
  ObTxDataGuard guard;
  ObTxDataGuard tmp_tx_data_guard;
  tmp_tx_data_guard.reset();
  ctx_tx_data_.get_tx_table(tx_table);

  if (for_replay && !replay_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "supplement tx_op", KR(ret), K(for_replay), K(replay_scn), KPC(this));
  } else if (OB_FAIL(ctx_tx_data_.get_tx_data(guard))) {
    TRANS_LOG(ERROR, "get tx data from ctx tx data failed", KR(ret));
  } else if (OB_FAIL(tx_table->alloc_tx_data(tmp_tx_data_guard))) {
    TRANS_LOG(WARN, "alloc tx_data failed", KR(ret), KPC(this));
  } else if (FALSE_IT(tmp_tx_data_guard.tx_data()->tx_id_ = trans_id_)) {
  } else if (OB_FAIL(tx_table->supplement_tx_op_if_exist(tmp_tx_data_guard.tx_data()))) {
    TRANS_LOG(WARN, "supplement tx_op ", KR(ret), K(ctx_tx_data_), KPC(this));
  } else if (OB_FAIL(ctx_tx_data_.recover_tx_data(tmp_tx_data_guard.tx_data()))) {
    TRANS_LOG(WARN, "replace tx data in ctx tx data failed.", KR(ret), K(ctx_tx_data_), KPC(this));
  } else if (for_replay && tmp_tx_data_guard.tx_data()->op_guard_.is_valid() &&
      OB_FAIL(recover_tx_ctx_from_tx_op_(tmp_tx_data_guard.tx_data()->op_guard_->get_tx_op_list(), replay_scn))) {
    TRANS_LOG(WARN, "recover tx_ctx from tx_op failed", KR(ret));
  }
  TRANS_LOG(INFO, "supplement_tx_op_if_exist_", KR(ret), K(trans_id_), K(ls_id_), K(ctx_tx_data_));
  return ret;
}

int ObPartTransCtx::recover_tx_ctx_from_tx_op_(ObTxOpVector &tx_op_list, const SCN replay_scn)
{
  TRANS_LOG(INFO, "recover tx_ctx from_tx_op begin", K(tx_op_list.get_count()), K(replay_scn), KPC(this));
  int ret = OB_SUCCESS;
  // filter tx_op for this tx_ctx life_cycle
  ObTxOpArray ctx_tx_op;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tx_op_list.get_count(); idx++) {
    ObTxOp &tx_op = *tx_op_list.at(idx);
    if (tx_op.get_op_scn() < replay_scn) {
      if (tx_op.get_op_code() == ObTxOpCode::ABORT_OP) {
        ctx_tx_op.reuse();
      } else if (OB_FAIL(ctx_tx_op.push_back(tx_op))) {
        TRANS_LOG(WARN, "push tx_op to array fail", KR(ret), KPC(this));
      }
    } else {
      if (tx_op.get_op_code() == ObTxOpCode::ABORT_OP) {
        break;
      } else if (OB_FAIL(ctx_tx_op.push_back(tx_op))) {
        TRANS_LOG(WARN, "push tx_op to array fail", KR(ret), KPC(this));
      }
    }
  }
  // recover tx_op to tx_ctx
  ObTxBufferNodeArray mds_array;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < ctx_tx_op.count(); idx++) {
    ObTxOp &tx_op = ctx_tx_op.at(idx);
    if (tx_op.get_op_code() == ObTxOpCode::MDS_OP) {
      ObTxBufferNodeWrapper &node_wrapper = *tx_op.get<ObTxBufferNodeWrapper>();
      const ObTxBufferNode &node = node_wrapper.get_node();
      if (OB_FAIL(mds_array.push_back(node))) {
        TRANS_LOG(WARN, "failed to push node to array", KR(ret), KPC(this));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "recover tx_op undefined", KR(ret), KPC(this));
    }
  }
  ObTxBufferNodeArray _unused_;
  if (FAILEDx(deep_copy_mds_array_(mds_array, _unused_))) {
    TRANS_LOG(WARN, "deep copy mds array failed", KR(ret), KPC(this));
  }
  int64_t mds_max_register_no = 0;
  if (mds_array.count() > 0) {
    mds_max_register_no = mds_array.at(mds_array.count() - 1).get_register_no();
  }
  int64_t ctx_max_register_no = 0;
  if (exec_info_.multi_data_source_.count() > 0) {
    ctx_max_register_no = exec_info_.multi_data_source_.at(exec_info_.multi_data_source_.count() - 1).get_register_no();
  }
  TRANS_LOG(INFO, "recover tx_ctx from tx_op finish", KR(ret), K(tx_op_list.get_count()), K(ctx_tx_op.count()),
      K(mds_array.count()), K(exec_info_.multi_data_source_.count()),
      K(mds_max_register_no), K(ctx_max_register_no),
      KPC(this));
  return ret;
}

int ObPartTransCtx::check_status()
{
  CtxLockGuard guard(lock_, CtxLockGuard::MODE::ACCESS);
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
inline int ObPartTransCtx::check_status_()
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
    if (is_trans_expired_()) {
      ret = OB_TRANS_TIMEOUT;
      TRANS_LOG(WARN, "tx has decided", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_KILLED;
      TRANS_LOG(WARN, "tx force aborted due to data incomplete", K(ret), KPC(this));
    }
  } else if (OB_UNLIKELY(is_follower_())) {
    ret =  OB_NOT_MASTER;
  } else if (OB_UNLIKELY(is_exiting_)) {
    if (OB_UNLIKELY(transfer_deleted_)) {
      ret = OB_NEED_RETRY;
      TRANS_LOG(WARN, "tx is transfer removing need retry", K(ret), KPC(this));
    } else {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "tx is exiting", K(ret), KPC(this));
    }
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
 * 3) alloc data_scn if not specified
 */
int ObPartTransCtx::start_access(const ObTxDesc &tx_desc, ObTxSEQ &data_scn, const int16_t branch)
{
  int ret = OB_SUCCESS;
  int pending_write = -1;
  const bool alloc = !data_scn.is_valid();
  int callback_list_idx = 0;
  {
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::ACCESS);
    if (OB_FAIL(check_status_())) {
    } else if (tx_desc.op_sn_ < last_op_sn_) {
      ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
      TRANS_LOG(WARN, "stale access operation", K(ret),
                K_(tx_desc.op_sn), K_(last_op_sn), KPC(this), K(tx_desc));
    } else {
      if (tx_desc.op_sn_ != last_op_sn_) {
        last_op_sn_ = tx_desc.op_sn_;
      }
      if (alloc) {
        data_scn = tx_desc.inc_and_get_tx_seq(branch);
      }
      last_scn_ = MAX(data_scn, last_scn_);
      if (!first_scn_.is_valid()) {
        first_scn_ = last_scn_;
      }
      pending_write = ATOMIC_AAF(&pending_write_, 1);
      // others must wait the first thread of parallel open the write epoch
      // hence this must be done in lock
      if (data_scn.support_branch() && pending_write == 1) {
        callback_list_idx = mt_ctx_.acquire_callback_list(true);
      }
    }
  }
  // other operations are allowed to out of lock
  if (OB_SUCC(ret)) {
    mt_ctx_.inc_ref();
    if (data_scn.support_branch()) {
      if (pending_write != 1) {
        callback_list_idx = mt_ctx_.acquire_callback_list(false);
      }
      // remember selected callback_list idx into seq_no
      if (data_scn.get_branch() == 0 && alloc && callback_list_idx != 0) {
        data_scn.set_branch(callback_list_idx);
      }
    } else { // OLD version < 4.2.4
      ret = OB_ERR_UNEXPECTED;
    }
  }
  last_request_ts_ = ObClockGenerator::getClock();
  TRANS_LOG(TRACE, "start_access", K(ret), K(data_scn.support_branch()), K(data_scn), KPC(this));
  common::ObTraceIdAdaptor trace_id;
  trace_id.set(ObCurTraceId::get());
  REC_TRANS_TRACE_EXT(tlog_, start_access,
                      OB_ID(ret), ret,
                      OB_ID(trace_id), trace_id,
                      OB_ID(opid), tx_desc.op_sn_,
                      OB_ID(data_seq), data_scn.cast_to_int(),
                      OB_ID(pending), pending_write,
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
  // to reduce lock contention, these operation is out of lock
  int pending_write = ATOMIC_SAF(&pending_write_, 1);
  TRANS_LOG(TRACE, "end_access", K(ret), KPC(this));
  mt_ctx_.dec_ref();
  mt_ctx_.revert_callback_list();
  REC_TRANS_TRACE_EXT(tlog_, end_access,
                      OB_ID(opid), last_op_sn_,
                      OB_ID(pending), pending_write,
                      OB_ID(ref), get_ref(),
                      OB_ID(tid), get_itid() + 1);
  return ret;
}

/*
 * rollback_to_savepoint - rollback to savepoint
 *
 * @op_sn       - operation sequence number, used to reject out of order msg
 * @from_scn    - the start position of rollback, inclusive
 *                generally not specified, and generated in callee
 * @to_scn      - the end position of rollback, exclusive
 * @seq_base    - the baseline of TxSEQ of current transaction
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
                                          ObTxSEQ from_scn,
                                          const ObTxSEQ to_scn,
                                          const int64_t seq_base,
                                          const int64_t request_id,
                                          ObIArray<ObTxLSEpochPair> &downstream_parts)
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
    TRANS_LOG(WARN, "rollback_to need retry because of logging", K(ret), K(trans_id_), K(ls_id_), K(busy_cbs_.get_size()));
  } else if (is_2pc_blocking()) {
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "rollback_to need retry because of 2pc blocking", K(trans_id_), K(ls_id_), KP(this), K(ret));
  } else if (op_sn < last_op_sn_) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else if (FALSE_IT(last_op_sn_ = op_sn)) {
  } else if ((to_scn.get_branch() == 0) && pending_write_ > 0) {
    // for branch savepoint rollback, pending_write !=0 almostly
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "has pending write, rollback blocked", K(ret), K(to_scn), K(pending_write_), KPC(this));
  } else if (last_scn_ <= to_scn) {
    TRANS_LOG(INFO, "rollback succeed trivially", K_(trans_id), K_(ls_id), K(op_sn), K(to_scn), K_(last_scn));
  } else if (!from_scn.is_valid() &&
             // generate from if not specified
             FALSE_IT(from_scn = to_scn.clone_with_seq(ObSequence::inc_and_get_max_seq_no(), seq_base))) {
  } else if (OB_FAIL(rollback_to_savepoint_(from_scn, to_scn, share::SCN::invalid_scn()))) {
    TRANS_LOG(WARN, "rollback_to_savepoint fail", K(ret), K(from_scn), K(to_scn), K(op_sn), KPC(this));
  } else if (to_scn.get_branch() == 0) {
    last_scn_ = to_scn;
  }

  if (OB_SUCC(ret)) {
    bool need_downstream = true;
    int64_t current_rollback_to_timestamp = ObTimeUtility::current_time();
    if (request_id != 0 && // come from rollback to request
        request_id == last_rollback_to_request_id_) { // the same rollback to with the last one
      if (last_transfer_in_timestamp_ != 0 &&
          last_rollback_to_timestamp_ != 0 &&
          // encounter transfer between two same rollback to
          last_transfer_in_timestamp_ > last_rollback_to_timestamp_) {
        need_downstream = true;
        TRANS_LOG(INFO, "transfer between rollback to happened", K(ret), K(request_id),
                  K(last_rollback_to_timestamp_), K(last_transfer_in_timestamp_),
                  K(last_rollback_to_request_id_), KPC(this));
      } else {
        need_downstream = false;
        TRANS_LOG(INFO, "no transfer between rollback to happened", K(ret), K(request_id),
                  K(last_rollback_to_timestamp_), K(last_transfer_in_timestamp_),
                  K(last_rollback_to_request_id_), KPC(this));
      }
    } else {
      need_downstream = true;
    }

    // must add downstream parts when return success
    for (int64_t idx = 0;
         OB_SUCC(ret) &&
           need_downstream &&
           idx < exec_info_.intermediate_participants_.count();
         idx++) {
      if (OB_FAIL(downstream_parts.push_back(
                    ObTxLSEpochPair(exec_info_.intermediate_participants_.at(idx).ls_id_,
                                    exec_info_.intermediate_participants_.at(idx).transfer_epoch_)))) {
        TRANS_LOG(WARN, "push parts to array failed", K(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      last_rollback_to_request_id_ = request_id;
      last_rollback_to_timestamp_ = current_rollback_to_timestamp;
    }
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
                                           const ObTxSEQ to_scn,
                                           const share::SCN replay_scn)
{
  int ret = OB_SUCCESS;

  // step 1: persistent 'UNDO' (if required)
  /*
   * Follower:
   *  1. add UndoAction into tx_ctx's tx_data
   *  2. insert tx-data into tx_data_table
   * Leader:
   *  1. submit 'RollbackToLog'
   *  2. add UndoAction into tx_ctx's tx_data
   *  3. insert tx-data into tx_data_table after log sync success
   */
  bool need_update_tx_data = false;
  ObTxDataGuard tmp_tx_data_guard;
  ObTxDataGuard update_tx_data_guard;
  tmp_tx_data_guard.reset();
  update_tx_data_guard.reset();
  if (is_follower_()) { /* Follower */
    ObUndoAction undo_action(from_scn, to_scn);
    // _NOTICE_ must load Undo(s) from TxDataTable before overwriten
    if (replay_completeness_.is_unknown() &&
        !ctx_tx_data_.has_recovered_from_tx_table() &&
        OB_FAIL(supplement_tx_op_if_exist_(true, replay_scn))) {
      TRANS_LOG(WARN, "load undos from tx table fail", K(ret), KPC(this));
    } else if (OB_FAIL(replay_undo_action_to_tx_table_(undo_action, replay_scn))) {
      TRANS_LOG(WARN, "insert to tx table failed", KR(ret), K(*this));
    }
  } else if (OB_UNLIKELY(exec_info_.max_submitted_seq_no_ > to_scn)) { /* Leader */
    ObTxDataGuard tx_data_guard;
    ObTxTable *tx_table = nullptr;
    ctx_tx_data_.get_tx_table(tx_table);
    ObUndoAction undo(from_scn, to_scn);
    if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
      TRANS_LOG(WARN, "get tx data failed", KR(ret));
    } else if (OB_FAIL(tx_data_guard.tx_data()->init_tx_op())) {
      TRANS_LOG(WARN, "init tx op failed", KR(ret));
    } else if (OB_FAIL(tx_data_guard.tx_data()->add_undo_action(ls_tx_ctx_mgr_->get_tx_table(),
                                                                undo))) {
      TRANS_LOG(WARN, "add undo action failed", KR(ret));
    } else if (OB_FAIL(submit_rollback_to_log_(from_scn, to_scn))) {
      TRANS_LOG(WARN, "submit undo redolog fail", K(ret), K(from_scn), K(to_scn), KPC(this));
    }
  }

  // step 2: remove TxNode(s) from memtable

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mt_ctx_.rollback(to_scn, from_scn, replay_scn))) {
      TRANS_LOG(WARN, "undo provisinal modifies fail",
                K(ret), K(from_scn), K(to_scn), KPC(this));
    }
  }

  return ret;
}

int ObPartTransCtx::submit_rollback_to_log_(const ObTxSEQ from_scn,
                                            const ObTxSEQ to_scn)
{
  int ret = OB_SUCCESS;
  ObTxLogBlock log_block;
  ObTxRollbackToLog log(from_scn, to_scn);
  ObTxLogCb *log_cb = NULL;
  int64_t replay_hint = trans_id_.get_id();
  ObUndoStatusNode *undo_node = NULL;
  logservice::ObReplayBarrierType barrier = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  if (to_scn.support_branch() && is_parallel_logging()) {
    const int16_t branch_id = to_scn.get_branch();
    if (branch_id != 0 && to_scn.get_seq() > exec_info_.serial_final_seq_no_.get_seq()) {
      replay_hint += mt_ctx_.get_tx_seq_replay_idx(to_scn);
    } else {
      // either this is a global savepoint or the savepoint is before serial final point
      // must wait the redo log after this savepoint replayed
      barrier = logservice::ObReplayBarrierType::PRE_BARRIER;
    }
  }
  if (OB_FAIL(init_log_block_(log_block))) {
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
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table()->alloc_tx_data(log_cb->get_tx_data_guard(), true, INT64_MAX))) {
    TRANS_LOG(WARN, "alloc_tx_data failed", KR(ret), KPC(this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (OB_ISNULL(undo_node = (ObUndoStatusNode*)MTL(ObSharedMemAllocMgr*)->tx_data_allocator().alloc(true, INT64_MAX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc_undo_status_node failed", KR(ret), KPC(this));
    return_log_cb_(log_cb);
    log_cb = NULL;
  } else if (FALSE_IT(new (undo_node) ObUndoStatusNode())) {
  } else if (FALSE_IT(log_cb->get_undo_node() = undo_node)) {
  } else if (OB_FAIL(submit_log_block_out_(log_block, SCN::min_scn(), log_cb, replay_hint, barrier))) {
    TRANS_LOG(WARN, "submit log fail", K(ret), K(log_block), KPC(this));
    return_log_cb_(log_cb);
  } else if (OB_FAIL(acquire_ctx_ref())) {
    // inc for log_cb calling back
    TRANS_LOG(ERROR, "inc TxCtx ref fail", K(ret), KPC(this));
  } else if (OB_FAIL(after_submit_log_(log_block, log_cb, NULL))) {
  } else {
    log_cb->set_undo_action(ObUndoAction(from_scn, to_scn));
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
  } else if (OB_UNLIKELY(is_2pc_blocking())) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "2pc blocking", KR(ret), KPC(this));
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

int ObPartTransCtx::handle_tx_keepalive_response(const int64_t status)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == lock_.try_lock()) {
    CtxLockGuard guard(lock_, false);
    ret = tx_keepalive_response_(status);
  }

  return ret;
}

int ObPartTransCtx::tx_keepalive_response_(const int64_t status)
{
  int ret = OB_SUCCESS;

  if (OB_SWITCHING_TO_FOLLOWER_GRACEFULLY == ERRSIM_DELAY_TX_SUBMIT_LOG) {
    return ret;
  } else if (ERRSIM_DELAY_TX_SUBMIT_LOG < int(-9999)) {
    // reject trans_id and tenant id to contorl trans sate
    ret = process_errsim_for_standby_read_(int(ERRSIM_DELAY_TX_SUBMIT_LOG),
                                           OB_SWITCHING_TO_FOLLOWER_GRACEFULLY);
    if (OB_FAIL(ret)) {
      return ret;
      TRANS_LOG(INFO, "reject delay tx submit log when response to keepalive",
                K(ret), K(ERRSIM_DELAY_TX_SUBMIT_LOG));
    }
  }

  if ((OB_TRANS_CTX_NOT_EXIST == status || OB_TRANS_ROLLBACKED == status ||
       OB_TRANS_KILLED == status || common::OB_TENANT_NOT_IN_SERVER == status) && can_be_recycled_()) {
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

  if (is_support_tx_op_()) {
    // do nothing
  } else if (OB_ISNULL(retain_ctx_func_ptr_)) {

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
  bool need_retain_ctx = !is_support_tx_op_();
  if (need_retain_ctx) {
    TRANS_LOG(INFO, "insert into retain_ctx", KPC(this), K(for_replay), K(log_ts));
  }

  ObTxRetainCtxMgr &retain_ctx_mgr = ls_tx_ctx_mgr_->get_retain_ctx_mgr();
  if (!need_retain_ctx) {
    // do nothing
  } else if (OB_ISNULL(ls_tx_ctx_mgr_) || RetainCause::UNKOWN == cause) {
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
    } else if (OB_FAIL(mds_retain_func_ptr->init(this, cause, log_ts))) {
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
      if (OB_SUCC(ret) && part_trans_action_ != ObPartTransAction::COMMIT) {
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
      // NOTE: clean unlog callbacks is requried:
      // if mvcc-row is too large and can't be serialized, freeze thread
      // will delay_abort the txn, if don't clean unlog_callbacks
      // the memtable's freeze will be blocked
      ret = mt_ctx_.clean_unlog_callbacks();
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
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(restart_2pc_trans_timer_())) {
      TRANS_LOG(WARN, "restart_2pc_trans_timer_ error", KR(ret), KR(tmp_ret), KPC(this));
      ret = OB_EAGAIN;
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObPartTransCtx::do_local_abort_tx_()
{
  int ret = OB_SUCCESS;

  TRANS_LOG(WARN, "do_local_abort_tx_", KR(ret), K(*this));

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
  // } else if (OB_FAIL(gen_total_mds_array_(tmp_array))) {
  //   TRANS_LOG(WARN, "gen total mds array failed", KR(ret), K(*this));
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

  if (OB_FAIL(tx_end_(false /*commit*/))) {
    TRANS_LOG(WARN, "trans end error", KR(ret), "context", *this);
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "local tx clear error", KR(ret), K(*this));
  } else if (OB_FAIL(mds_cache_.generate_final_notify_array(exec_info_.multi_data_source_,
                                                             true /*need_merge_cache*/,
                                                             true /*allow_log_overflow*/))) {
    TRANS_LOG(WARN, "gen total mds array failed", KR(ret), K(*this));
  } else if (OB_FAIL(notify_data_source_(NotifyType::ON_ABORT, ctx_tx_data_.get_end_log_ts(), false,
                                         mds_cache_.get_final_notify_array()))) {
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
                                      SCN &trans_version)
{
  int ret = OB_ERR_SHARED_LOCK_CONFLICT;
  int tmp_ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_local_tx_()) {
    // check local trans prepare_version and commit_version
    // TODO(siyu):: check standby_read in inner_lock_for_read
    SCN prepare_version = mt_ctx_.get_trans_version();
    int32_t tx_data_state = ctx_tx_data_.get_state();
    SCN commit_version = ctx_tx_data_.get_commit_version();

    switch (tx_data_state)
    {
      case ObTxData::RUNNING:
      case ObTxData::ELR_COMMIT:
        if (prepare_version.is_valid()) {
          if (prepare_version > snapshot) {
            can_read = false;
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_SHARED_LOCK_CONFLICT;
          }
        } else {
          can_read = false;
          ret = OB_SUCCESS;
        }
        trans_version.set_min();
        break;
      case ObTxData::COMMIT:
        if (commit_version.is_valid()) {
          if (commit_version > snapshot) {
            can_read = false;
          } else {
            can_read = true;
          }
          trans_version = commit_version;
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "tx data commit state with invalid commit version", KPC(this));
        }
        break;
      case ObTxData::ABORT:
        can_read = false;
        trans_version.set_min();
        ret = OB_SUCCESS;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "invalid tx data state", KPC(this));
    }
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
                ret = OB_SUCCESS;
              }
            }
            TRANS_LOG(INFO, "check for standby for unknown", K(ret), K(snapshot), K(can_read), K(trans_version), K(tmp_state_info), K(readable_scn));
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
            ret = OB_SUCCESS;
          }
          break;
        }
        case ObTxState::PREPARE: {
          if (tmp_state_info.version_ > snapshot) {
            can_read = false;
            trans_version.set_min();
            ret = OB_SUCCESS;
          } else {
            version = MAX(version, tmp_state_info.version_);
          }
          break;
        }
        case ObTxState::ABORT: {
          can_read = false;
          trans_version.set_min();
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
      ret = OB_SUCCESS;
    }
    if (count == 0 || (OB_ERR_SHARED_LOCK_CONFLICT == ret && min_snapshot <= snapshot)) {
      if (ask_state_info_interval_.reach()) {
        if (OB_SUCCESS != (tmp_ret = build_and_post_ask_state_msg_(snapshot, ls_id_, addr_))) {
          TRANS_LOG(WARN, "ask state from coord fail", K(ret), K(snapshot), KPC(this));
        }
      }
    }
  }
  TRANS_LOG(INFO, "check for standby", K(ret), K(snapshot), K(can_read), K(trans_version), KPC(this));
  return ret;
}

int ObPartTransCtx::build_and_post_ask_state_msg_(const SCN &snapshot,
                                                  const share::ObLSID &ori_ls_id, const ObAddr &ori_addr)
{
  int ret = OB_SUCCESS;
  if (is_root()) {
    if (!exec_info_.participants_.empty()) {
      build_and_post_collect_state_msg_(snapshot);
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
    msg.ori_ls_id_ = ori_ls_id;
    msg.ori_addr_ = ori_addr;
    if (OB_FAIL(rpc_->post_msg(msg.receiver_, msg))) {
      TRANS_LOG(WARN, "post ask state msg fail", K(ret), K(msg), KPC(this));
      if (OB_LS_NOT_EXIST == ret) {
        ObStandbyCheckInfo tmp_check_info; // construct invalid check info
        ret = check_ls_state_(snapshot, msg.receiver_, tmp_check_info);
      }
    }
  }
  TRANS_LOG(INFO, "build and post ask state msg", K(ret), K(snapshot), KPC(this));
  return ret;
}

int ObPartTransCtx::check_ls_state_(const SCN &snapshot, const ObLSID &ls_id, const ObStandbyCheckInfo &check_info)
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
    state_info.check_info_ = check_info;
    if (OB_FAIL(update_state_info_array_(state_info))) {
      TRANS_LOG(WARN, "update state info array failed when ls not exsit", K(state_info), KPC(this));
    }
  } else {
    ret = OB_TRANS_CTX_NOT_EXIST;
  }
  TRANS_LOG(INFO, "check ls state for standby read", K(ret), K(snapshot), K(ls_id), K(check_info));
  return ret;
}

int ObPartTransCtx::handle_trans_ask_state(const ObAskStateMsg &req, ObAskStateRespMsg &resp)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  const share::SCN snapshot = req.snapshot_;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_root()) {
    if (exec_info_.participants_.empty()) {
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
      build_and_post_collect_state_msg_(snapshot);
      if (OB_FAIL(resp.state_info_array_.assign(state_info_array_))) {
        TRANS_LOG(WARN, "build ObAskStateRespMsg fail", K(ret), K(snapshot), KPC(this));
      }
    }
  } else {
    build_and_post_ask_state_msg_(snapshot, req.ori_ls_id_, req.ori_addr_);
  }
  TRANS_LOG(INFO, "handle ask state msg", K(ret), K(req), K(resp));
  return ret;
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
    msg.check_info_ = state_info_array_.at(i).check_info_;
    if (OB_FAIL(rpc_->post_msg(msg.receiver_, msg))) {
      TRANS_LOG(WARN, "post collect state msg fail", K(ret), K(msg), KPC(this));
      if (OB_LS_NOT_EXIST == ret) {
        ret = check_ls_state_(snapshot, msg.receiver_, msg.check_info_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    // receive a larger ask state req
    if (snapshot > lastest_snapshot_) {
      lastest_snapshot_ = snapshot;
    }
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
    ARRAY_FOREACH(exec_info_.commit_parts_, i) {
      state_info.check_info_.check_info_ori_ls_id_ = ls_id_;
      state_info.check_info_.check_part_ = exec_info_.commit_parts_.at(i);
      state_info.ls_id_ = state_info.check_info_.check_part_.ls_id_;
      if (OB_FAIL(state_info_array_.push_back(state_info))) {
        TRANS_LOG(WARN, "state info array push back fail", K(ret), K(state_info), KPC(this));
        break;
      }
    }
    // clean state info array to avoid check_for_standby induce wrong result
    if (OB_FAIL(ret)) {
      state_info_array_.reset();
    }
  }
  TRANS_LOG(INFO, "set state info array", K(ret), KPC(this));
  return ret;
}

int ObPartTransCtx::update_state_info_array_(const ObStateInfo& state_info)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;

  EqualToStateInfoFunctor<ObStateInfo> fn(state_info);
  int64_t search_index = search(state_info_array_, fn);
  if (search_index <= -1) {
    if (OB_FAIL(state_info_array_.push_back(state_info))) {
      TRANS_LOG(WARN, "push new state info array failed", K(ret), K(state_info));
    }
  } else {
    if (state_info_array_.at(search_index).need_update(state_info)) {
      state_info_array_.at(search_index) = state_info;
    }
  }

  TRANS_LOG(INFO, "update state info", K(ret), K(search_index), K(state_info), K(state_info_array_));
  return ret;
}
// push back new transfer parts with invalid state
int ObPartTransCtx::update_state_info_array_with_transfer_parts_(const ObTxCommitParts &parts,
                                                                 const ObLSID &ls_id) // collect resp sender id
{
  int ret = OB_SUCCESS;
  bool is_contain = false;

  ARRAY_FOREACH(parts, i) {
    // conver exec_part to check_info
    ObStandbyCheckInfo tmp_info;
    tmp_info.check_part_ = parts.at(i);
    tmp_info.check_info_ori_ls_id_ = ls_id;
    EqualToTransferPartFunctor<ObStateInfo> fn(tmp_info);

    int64_t search_index = search(state_info_array_, fn);

    if (search_index <= -1) {
      ObStateInfo tmp_state;
      tmp_state.ls_id_ = tmp_info.check_part_.ls_id_; // set with transfer in id
      tmp_state.check_info_ = tmp_info;               // set check info
      if (OB_FAIL(state_info_array_.push_back(tmp_state))) {
        TRANS_LOG(WARN, "push back into state info array failed", K(ret),
                  K(state_info_array_), KPC(this));
      }
    }
  }

  TRANS_LOG(WARN, "update transfer parts in state info array", K(ret),
            K(state_info_array_), K(parts), K(ls_id));
  return ret;
}

int ObPartTransCtx::handle_trans_ask_state_resp(const ObAskStateRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    // force assign all state info from msg
    // to avoid a read node post rpc failed and construct an state_info with invalid check_info,
    // we should clean it when receive ask_state_resp
    state_info_array_.reuse();
    if (OB_FAIL(state_info_array_.assign(msg.state_info_array_))) {
      state_info_array_.reuse();
      TRANS_LOG(INFO, "assign state info array failed", K(ret), K(msg), KPC(this));
    }
  }
  TRANS_LOG(INFO, "handle trans ask state resp", K(ret), K(msg), KPC(this));
  return ret;
}

int ObPartTransCtx::handle_trans_collect_state(ObCollectStateRespMsg &resp,
                                               const ObCollectStateMsg &req)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  SCN snapshot = req.snapshot_;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    ObStateInfo &state_info = resp.state_info_;
    const ObStandbyCheckInfo &check_info = req.check_info_;
    state_info.check_info_ = check_info;
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
          // info check
          if (OB_FAIL(resp.transfer_parts_.assign(exec_info_.commit_parts_))) {
            TRANS_LOG(WARN, "assign transfer part in resp failed", K(ret), K(state_info), K(req), K(resp));
          } else if (!is_exec_complete_without_lock(check_info.check_info_ori_ls_id_,
                                                    check_info.check_part_.exec_epoch_,
                                                    check_info.check_part_.transfer_epoch_)) {
            // exec_epoch invalid and transfer_epoch invalid can not go to this
            if (check_info.check_part_.exec_epoch_ > 0) {
              state_info.state_ = ObTxState::ABORT;
            } else if (check_info.check_part_.transfer_epoch_ > 0) {
              // transfer epoch check failed, get readable scn
              state_info.state_ = ObTxState::UNKNOWN;
              if (OB_FAIL(get_ls_replica_readable_scn_(state_info.ls_id_, state_info.version_))) {
                TRANS_LOG(WARN, "get replica readable scn failed", K(ret), K(state_info), K(req), K(resp));
              }
            } else {
              TRANS_LOG(ERROR, "receive invalid check info but not pass complete check", K(ret),
                        K(state_info), K(req), K(resp));
            }
          }
          need_loop = false;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
      }
    } while(need_loop);
  }
  TRANS_LOG(INFO, "handle trans collect state", K(ret), K(req), K(resp), KPC(this));
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
  } else if (OB_FAIL(update_state_info_array_with_transfer_parts_(msg.transfer_parts_, msg.sender_))) {
    TRANS_LOG(WARN, "insert transfer parts into state info array failed", K(ret), K(msg), KPC(this));
  } else if (OB_FAIL(update_state_info_array_(msg.state_info_))) {
    TRANS_LOG(WARN, "update state info failed", K(ret), K(msg), KPC(this));
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

int ObPartTransCtx::transfer_op_log_cb_(share::SCN op_scn, NotifyType op_type)
{
  int ret = OB_SUCCESS;
  if (!ctx_tx_data_.get_start_log_ts().is_valid()) {
    if (OB_FAIL(ctx_tx_data_.set_start_log_ts(op_scn))) {
      TRANS_LOG(WARN, "set start log ts failed", KR(ret), K(op_scn));
    }
  }
  if ((!ctx_tx_data_.get_end_log_ts().is_valid() ||
      ctx_tx_data_.get_end_log_ts() < op_scn) &&
      !ctx_tx_data_.is_read_only()) {
    if (OB_FAIL(ctx_tx_data_.set_end_log_ts(op_scn))) {
      TRANS_LOG(WARN, "set end log ts failed", KR(ret), K(op_scn));
    }
  }
  if (OB_SUCC(ret)) {
    if (!exec_info_.max_applying_log_ts_.is_valid() || op_scn > exec_info_.max_applying_log_ts_) {
      exec_info_.max_applying_log_ts_ = op_scn;
      exec_info_.max_applying_part_log_no_ = 0;
      exec_info_.max_applied_log_ts_ = op_scn;
      update_rec_log_ts_(true/*for_replay*/, op_scn);
    }
  }
  return ret;
}

int ObPartTransCtx::do_transfer_out_tx_op(const SCN data_end_scn,
                                          const SCN op_scn,
                                          const NotifyType op_type,
                                          bool is_replay,
                                          const ObLSID dest_ls_id,
                                          const int64_t transfer_epoch,
                                          bool &is_operated)
{
  int ret = OB_SUCCESS;
  SCN start_scn;
  is_operated = false;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartTransCtx not inited", KR(ret));
  } else if (FALSE_IT(start_scn = get_start_log_ts())) {
  } else if (!data_end_scn.is_valid() || (NotifyType::REGISTER_SUCC != op_type &&
                                          NotifyType::ON_ABORT != op_type &&
                                          !op_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", KR(ret), K(data_end_scn), K(op_scn), K(op_type));
  } else if (!start_scn.is_valid() || start_scn > data_end_scn) {
    // do nothing skip tx which start_scn > data_end_scn
  } else if (NotifyType::REGISTER_SUCC == op_type) {
    // blocking active tx which start_scn <= data_end_scn
    // when register modify memory state only
    sub_state_.set_transfer_blocking();
    is_operated = true;
  } else if (NotifyType::ON_REDO == op_type) {
    if (exec_info_.max_applying_log_ts_.is_valid() && exec_info_.max_applying_log_ts_ >= op_scn) {
      // do nothing
    } else if (FALSE_IT(sub_state_.set_transfer_blocking())) {
    } else if (FALSE_IT(exec_info_.is_transfer_blocking_ = true)) {
    } else if (OB_FAIL(transfer_op_log_cb_(op_scn, op_type))) {
      TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(op_scn));
    } else {
      is_operated = true;
    }
  } else if (NotifyType::ON_COMMIT == op_type) {
    if (exec_info_.max_applying_log_ts_.is_valid() && exec_info_.max_applying_log_ts_ >= op_scn) {
      // do nothing
    } else {
      // just for check
      if (!sub_state_.is_transfer_blocking()) {
        TRANS_LOG(ERROR, "tx should in transfer blocking state", KPC(this), K(op_scn), K(op_type));
      }
      // add downstream for uncommit tx
      // In the current implementation, we hope to definitively confirm that the
      // number of objects targeted by the following operations in the transfer is
      // in line with our expectations:
      // 1. Blocking the corresponding txns
      // 2. Adding downstream for the corresponding txns
      // 3. Relocating the corresponding txns from src ls
      // (We use OpX to represents the above operations)
      //
      // In order to ensure idempotence during replay and restart scenarios, we
      // need ensure that the effectted objects of the Op2 equals to the Op3, and
      // the number is smaller than the Op1. Otherwise:
      // 1. If the effectted objects of the Op2 is not equal to the Op3, the tree
      // styled 2pc will, either there are participants without the participant
      // list, leading to txn ctx timeouts and death, or there is the participant
      // list without participants, causing the txn to die because the ctx cannot
      // be found.
      // 2. If the effectted number of the Op2 and Op3 is bigger than the Op1, the
      // relocated txns will not be blocked and generate an inconsistency snapshot
      // of src ls.
      //
      // Overall, in otder to guarantee the above rules, we then choose to use the
      // decisive log_scn to decide on the same objects for the Op1, Op2 and Op3.
      //
      // NB: We need also notice that the nature of high-concurrency txns donot
      // allow we use the transfer_scn as the log_scn to decide on the Operations.
      // So we decide to use an seperate scn(max consequent scn) to meet the above
      // requirements. While the scn which decides the state of the relocation of
      // the tree styled 2pc still is transfer_scn.
      if (get_downstream_state() < ObTxState::COMMIT) {
        if (OB_FAIL(add_intermediate_participants(dest_ls_id, transfer_epoch))) {
          TRANS_LOG(WARN, "fail to add intermediate participants", K(ret), KPC(this));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(sub_state_.clear_transfer_blocking())) {
      } else if (FALSE_IT(exec_info_.is_transfer_blocking_ = false)) {
      } else if (OB_FAIL(transfer_op_log_cb_(op_scn, op_type))) {
        TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(op_scn));
      } else {
        is_operated = true;
      }
    }
  } else if (NotifyType::ON_ABORT == op_type) {
    if (!op_scn.is_valid()) {
      // leader on_register fail to clean
      sub_state_.clear_transfer_blocking();
      exec_info_.is_transfer_blocking_ = false;
    } else if (exec_info_.max_applying_log_ts_.is_valid() && exec_info_.max_applying_log_ts_ >= op_scn) {
      // replay filter
    } else {
      sub_state_.clear_transfer_blocking();
      exec_info_.is_transfer_blocking_ = false;
      if (OB_FAIL(transfer_op_log_cb_(op_scn, op_type))) {
        TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(op_scn));
      }
      is_operated = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "undefine transfer_out_tx_op", KR(ret), K(op_type), K(is_replay));
  }
  LOG_INFO("transfer out op", KR(ret), K(op_type), K(op_scn), KPC(this));
  return ret;
}

int ObPartTransCtx::wait_tx_write_end()
{
  int ret = OB_SUCCESS;
  // promise without flying write
  mt_ctx_.wait_write_end();
  return ret;
}

int ObPartTransCtx::collect_tx_ctx(const ObLSID dest_ls_id,
                                   const SCN data_end_scn,
                                   const ObIArray<ObTabletID> &tablet_list,
                                   ObTxCtxMoveArg &arg,
                                   bool &is_collected)
{
  int ret = OB_SUCCESS;
  SCN start_scn;
  is_collected = false;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    // we should ignore exiting participants
    TRANS_LOG(INFO, "collect_tx_ctx tx skip ctx exiting", K(trans_id_), K(ls_id_));
  } else if (FALSE_IT(start_scn = get_start_log_ts())) {
  } else if (!start_scn.is_valid() || start_scn > data_end_scn) {
    TRANS_LOG(INFO, "collect_tx_ctx tx skip for start_scn", K(trans_id_), K(ls_id_), K(start_scn.is_valid()), K(start_scn > data_end_scn));
  } else if (exec_info_.state_ >= ObTxState::COMMIT) {
    TRANS_LOG(INFO, "collect_tx_ctx tx skip ctx has commit", K(trans_id_), K(ls_id_), K(exec_info_.state_));
  } else if (!sub_state_.is_transfer_blocking()) {
    // just for check
    if (!is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT_V2)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tx has no transfer_blocking state unexpected", KR(ret), KPC(this), K(start_scn), K(data_end_scn));
    } else {
      TRANS_LOG(INFO, "collect_tx_ctx tx skip transfer self", K(trans_id_), K(ls_id_), K(start_scn), K(start_scn > data_end_scn),
          K(is_contain_mds_type_(ObTxDataSourceType::START_TRANSFER_OUT_V2)));
    }
  } else if (sub_state_.is_state_log_submitting()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx is driving when transfer move", KR(ret), KPC(this),
      K(sub_state_.is_state_log_submitting()), K(sub_state_.is_gts_waiting()));
  } else {
    // start_scn <= log_scn && transfer_blocking
    arg.tx_id_ = trans_id_;
    arg.tx_state_ = exec_info_.state_;
    // must differ with src epoch bacause of may transfer back
    arg.epoch_ = epoch_ | (ObTimeUtility::current_time_ns() & ~(0xFFFFUL << 48));
    arg.session_id_ = session_id_;
    arg.associated_session_id_ = associated_session_id_;
    arg.trans_version_ = mt_ctx_.get_trans_version();
    arg.prepare_version_ = exec_info_.prepare_version_;
    arg.commit_version_ = get_commit_version();
    arg.cluster_id_ = cluster_id_;
    arg.cluster_version_ = cluster_version_;
    arg.scheduler_ = exec_info_.scheduler_;
    arg.tx_expired_time_ = trans_expired_time_;
    arg.last_seq_no_ = last_scn_;
    arg.max_submitted_seq_no_ = exec_info_.max_submitted_seq_no_;
    arg.tx_start_scn_ = get_start_log_ts();
    arg.tx_end_scn_ = get_tx_end_log_ts();
    arg.is_sub2pc_ = exec_info_.is_sub2pc_;
    arg.happened_before_ = false;
    for (int64_t idx = 0; idx < exec_info_.commit_parts_.count(); idx++) {
      if (exec_info_.commit_parts_.at(idx).ls_id_ == dest_ls_id) {
        if (exec_info_.commit_parts_.at(idx).transfer_epoch_ > 0) {
          arg.happened_before_ = true;
        }
        break;
      }
    }
    if (!arg.happened_before_) {
      for (int64_t idx = 0; idx < exec_info_.intermediate_participants_.count(); idx++) {
        if (exec_info_.intermediate_participants_.at(idx).ls_id_ == dest_ls_id) {
          if (exec_info_.intermediate_participants_.at(idx).transfer_epoch_ > 0) {
            arg.happened_before_ = true;
          }
          break;
        }
      }
    }
    // move table lock
    if (FAILEDx(mt_ctx_.get_table_lock_for_transfer(arg.table_lock_info_, tablet_list))) {
      TRANS_LOG(WARN, "get table lock info failed", KR(ret));
    }
    if (OB_SUCC(ret)) {
      is_collected = true;
      TRANS_LOG(INFO, "collect_tx_ctx", KR(ret), KP(this), K(trans_id_), K(ls_id_), K(arg));
    }
  }
  return ret;
}

// For the transfer, the src txn may transfer into the dst txn that has already
// aborted, and the txn may already release the lock which causes two alive txn
// on the same row
int ObPartTransCtx::check_is_aborted_in_tx_data_(const ObTransID tx_id,
                                                 bool &is_aborted)
{
  int ret = OB_SUCCESS;
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard guard;
  int64_t state;
  share::SCN trans_version;
  share::SCN recycled_scn;
  ctx_tx_data_.get_tx_table(tx_table);

  if (OB_FAIL(tx_table->get_tx_table_guard(guard))) {
    TRANS_LOG(WARN, "fail to get tx table guard", K(ret));
  } else if (!guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx table is null", K(ret));
  } else if (OB_FAIL(guard.try_get_tx_state(tx_id,
                                            state,
                                            trans_version,
                                            recycled_scn))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      is_aborted = false;
      ret = OB_SUCCESS;
    } else if (OB_REPLICA_NOT_READABLE == ret) {
      is_aborted = false;
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get tx state from tx data failed", K(ret), KPC(this));
    }
  } else if (ObTxData::ABORT == state) {
    is_aborted = true;
    TRANS_LOG(INFO, "check is aborted in tx data", K(tx_id), K(state), KPC(this));
  } else {
    is_aborted = false;
    TRANS_LOG(INFO, "check is not aborted in tx data", K(tx_id), K(state), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::load_tx_op_if_exist_()
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = NULL;
  ObTxTableGuard tx_table_guard;
  if (OB_FAIL(ctx_tx_data_.get_tx_data_ptr(tx_data))) {
    TRANS_LOG(WARN, "get_tx_data failed", KR(ret), KPC(this));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx_data is null", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_table_guard(tx_table_guard))) {
    TRANS_LOG(WARN, "get_tx_table failed", KR(ret), KPC(this));
  } else if (OB_FAIL(tx_table_guard.load_tx_op(trans_id_, *tx_data))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "load_tx_op failed", KR(ret), KPC(this));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// NB: This function can report a retryable error because the outer while loop
// will ignore the error and continuously retry until it succeeds within the
// callback function.
int ObPartTransCtx::move_tx_op(const ObTransferMoveTxParam &move_tx_param,
                               const ObTxCtxMoveArg &arg,
                               const bool is_new_created)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (sub_state_.is_state_log_submitting()) {
    // retry if has stat log cb
    ret = OB_NEED_RETRY;
    TRANS_LOG(WARN, "has state log submitting need retry", KR(ret), K(trans_id_), K(sub_state_));
  } else if (NotifyType::REGISTER_SUCC == move_tx_param.op_type_) {
    if (exec_info_.state_ >= ObTxState::ABORT) {
      // this ctx may be recycled soon
      // a. RetainCtx recycle
      // b. get_tx_ctx and abort/clear log callback concurrent
      if (ctx_tx_data_.get_state() != ObTxData::ABORT) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "tx trans state unexpected", KR(ret), KPC(this));
      } else if (exec_info_.multi_data_source_.empty()) {
        // ctx will be deleted soon
        ret = OB_NEED_RETRY;
        TRANS_LOG(WARN, "tx ctx has end need retry", KR(ret), KPC(this));
      } else {
        // ctx enter retain
        // TODO when ctx entery retain, it will be recycled delay
        // leader and follower will see different state
        ret = OB_OP_NOT_ALLOW;
        TRANS_LOG(WARN, "tx ctx has end", KR(ret), KPC(this));
      }
    } else if (epoch_ != arg.epoch_ // ctx created by itself
               && exec_info_.next_log_entry_no_ == 0 // no log submitted
               && get_redo_log_no_() == 0 // no log submitted
               && busy_cbs_.is_empty()) { // no log submitting
      // promise tx log before move log
      if (exec_info_.state_ == ObTxState::INIT) {
        // promise redo log before move log
        bool submitted = false;
        ObTxRedoSubmitter submitter(*this, mt_ctx_);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(submitter.serial_submit(false)) && submitter.get_submitted_cnt() <= 0) {
          ret = tmp_ret;
          TRANS_LOG(WARN, "submit redo failed", K(ret), KPC(this));
        } else {
          sub_state_.set_transfer_blocking();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "tx state is advance but no tx log", KR(ret), KPC(this));
      }
    } else {
      sub_state_.set_transfer_blocking();
    }
  } else if (NotifyType::ON_REDO == move_tx_param.op_type_) {
    if (exec_info_.max_applying_log_ts_.is_valid() && exec_info_.max_applying_log_ts_ >=move_tx_param.op_scn_) {
      // do nothing
    } else {
      if (is_new_created && is_follower_()) {
        exec_info_.is_empty_ctx_created_by_transfer_ = true;
        TRANS_LOG(INFO, "empty tx ctx created by transfer", KPC(this));
      }

      sub_state_.set_transfer_blocking();
      exec_info_.is_transfer_blocking_ = true;
      if (OB_FAIL(transfer_op_log_cb_(move_tx_param.op_scn_, move_tx_param.op_type_))) {
        TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(move_tx_param));
      }
    }
  } else if (NotifyType::ON_COMMIT == move_tx_param.op_type_) {
    bool is_aborted = false;
    if (exec_info_.max_applying_log_ts_.is_valid() && exec_info_.max_applying_log_ts_ >= move_tx_param.op_scn_) {
      // do nothing
    } else if (epoch_ == arg.epoch_ && // created by myself
               OB_FAIL(check_is_aborted_in_tx_data_(trans_id_, is_aborted))) {
      TRANS_LOG(WARN, "check is aborted in tx data failed", K(ret), KPC(this));
    } else {
      if (is_new_created && is_follower_()) {
        exec_info_.is_empty_ctx_created_by_transfer_ = true;
        TRANS_LOG(INFO, "empty tx ctx created by transfer", KPC(this));
      }

      // if ctx.epoch_ equals arg.epoch_ ctx is created by transfer
      // if ctx.epoch_ not equals arg.epoch_ ctx has exist we just merge
      if (epoch_ == arg.epoch_) {
        trans_expired_time_ = arg.tx_expired_time_;
      }
      exec_info_.max_submitted_seq_no_.inc_update(arg.max_submitted_seq_no_);
      if (arg.last_seq_no_ > last_scn_) {
        last_scn_.atomic_store(arg.last_seq_no_);
      }

      // start scn in dest ctx is not valid while start scn in previous dest
      // ctx(has been released) or src ctx is valid, so we need change it
      if ((!ctx_tx_data_.get_start_log_ts().is_valid() &&
           (arg.tx_start_scn_.is_valid()))
          ||
          // start scn in dest ctx is valid and start scn in src ctx is smaller
          // than it,, so we need change it
          (ctx_tx_data_.get_start_log_ts().is_valid() &&
           arg.tx_start_scn_.is_valid() &&
           arg.tx_start_scn_ < ctx_tx_data_.get_start_log_ts())) {
        if (!ctx_tx_data_.is_read_only()) {
          // for merging txn where the start_scn is smaller or refers to a
          // previously existing txn, we need to replace it with the smallest
          // start_scn to ensure the proper recycling mechanism is in place.
          // Otherwise the upper trans version will be calculated incorrectly
          ctx_tx_data_.set_start_log_ts(arg.tx_start_scn_);
        }
      }

      if (is_aborted) {
        // If the dest txn already aborted before transfer, the dest txn may
        // already release its lock and new txn may write new data onto it which
        // will cause two alive tx node on one row at the same time if transfer
        // into an alive txn. So we need set these transferred txn as aborted
        ctx_tx_data_.set_state(ObTxData::ABORT);
      }

      if (!arg.happened_before_) {
        bool epoch_exist = false;
        for (int64_t idx = 0; idx < exec_info_.transfer_parts_.count(); idx++) {
          if (exec_info_.transfer_parts_.at(idx).ls_id_ == move_tx_param.src_ls_id_ &&
              exec_info_.transfer_parts_.at(idx).transfer_epoch_ == move_tx_param.transfer_epoch_) {
            epoch_exist = true;
            break;
          }
        }
        if (!epoch_exist) {
          if (OB_FAIL(exec_info_.transfer_parts_.push_back(ObTxExecPart(move_tx_param.src_ls_id_, -1, move_tx_param.transfer_epoch_)))) {
            TRANS_LOG(WARN, "epochs push failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (exec_info_.state_ < ObTxState::COMMIT
        && OB_FAIL(mt_ctx_.recover_from_table_lock_durable_info(arg.table_lock_info_, true))) {
        TRANS_LOG(WARN, "recover table lock failed", KR(ret), K(arg));
      } else {
        // NB: we should increment the next_log_entry_no for the transfer,
        // otherwise if the tx_ctx is created by the transfer, it will take the
        // next log as the first log_entry_no and if it just replay from the it
        // during recover(replay from the middle), it will fail to identify the
        // fact that it is replaying from the middle and fail to pass the check.
        exec_info_.next_log_entry_no_++;
        sub_state_.clear_transfer_blocking();
        exec_info_.is_transfer_blocking_ = false;
        if (OB_FAIL(transfer_op_log_cb_(move_tx_param.op_scn_, move_tx_param.op_type_))) {
          TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(move_tx_param));
        } else {
          last_transfer_in_timestamp_ = ObTimeUtility::current_time();
        }
      }
    }
    // tx_ctx and tx_data checkpoint independent
    // dest_ls may recycle tx_data, need promote tx_data end_scn after transfer in tablet make it bigger than transfer_scn
    // log sequence move_tx --> transfer_in --> commit
    // so when recycle tx_data on dest_ls, we can see transfer in tablet, not to recycle tx_data which end_scn > transfer_scn
    if (OB_SUCC(ret) && exec_info_.state_ >= ObTxState::COMMIT) {
      if (OB_FAIL(update_tx_data_start_and_end_scn_(arg.tx_start_scn_,
                                                    move_tx_param.op_scn_,
                                                    move_tx_param.transfer_scn_))) {
        TRANS_LOG(WARN, "update tx data failed", KR(ret), KPC(this));
      }
    }
  } else if (NotifyType::ON_ABORT == move_tx_param.op_type_) {
    bool need_del = false;
    // del if no query write and replay
    if (epoch_ == arg.epoch_
        && // for leader, we will remove those who never takes effects
        (!first_scn_.is_valid()
         && pending_write_ == 0)) {
      need_del = true;
    }

    if (!move_tx_param.op_scn_.is_valid()) {
      // leader register fail to clean
      sub_state_.clear_transfer_blocking();
      exec_info_.is_transfer_blocking_ = false;
    } else if (exec_info_.max_applying_log_ts_.is_valid() &&
               exec_info_.max_applying_log_ts_ >= move_tx_param.op_scn_) {
      // replay filter
    } else {
      sub_state_.clear_transfer_blocking();
      exec_info_.is_transfer_blocking_ = false;
      if (OB_FAIL(transfer_op_log_cb_(move_tx_param.op_scn_, move_tx_param.op_type_))) {
        TRANS_LOG(WARN, "transfer op loc_cb failed", KR(ret), K(move_tx_param));
      }
    }
    if (need_del) {
      // stop ctx write
      transfer_deleted_ = true;
      // remove tx ctx from map
      set_exiting_();
      // here we need replay abort_log to delete tx ctx
      // so we need record rec_scn for this log
      if (move_tx_param.op_scn_.is_valid()) {
        ls_tx_ctx_mgr_->update_aggre_log_ts_wo_lock(move_tx_param.op_scn_);
      }
      TRANS_LOG(INFO, "move_tx_op delete ctx", K(trans_id_), K(ls_id_), K(move_tx_param), KP(this));
    }
  }
  TRANS_LOG(INFO, "move_tx_op", KR(ret), K(arg.epoch_), K(move_tx_param), K(epoch_), KPC(this));
  return ret;
}

void ObPartTransCtx::print_first_mvcc_callback_()
{
  mt_ctx_.print_first_mvcc_callback();
}

bool ObPartTransCtx::is_exec_complete(ObLSID ls_id, int64_t epoch, int64_t transfer_epoch)
{
  bool is_complete = true;
  // if no transfer epoch just compare epoch without ctx lock
  if (transfer_epoch <= 0) {
    if (epoch > 0 && epoch != epoch_) {
      is_complete = false;
    }
  } else {
    CtxLockGuard guard(lock_);
    is_complete = is_exec_complete_without_lock(ls_id, epoch, transfer_epoch);
  }
  return is_complete;
}

bool ObPartTransCtx::is_exec_complete_without_lock(ObLSID ls_id,
                                                   int64_t epoch,
                                                   int64_t transfer_epoch)
{
  bool is_complete = true;

  // TODO(handora.qc): fix compatibilty of epoch

  // Case1: the execution epoch is not equal
  if (epoch > 0 && epoch != epoch_) {
    is_complete = false;
  // Case2: the transfer epoch is not equal
  } else if (transfer_epoch > 0) {
    is_complete = false;
    for (int64_t idx = 0; idx < exec_info_.transfer_parts_.count(); idx++) {
      if (ls_id == exec_info_.transfer_parts_.at(idx).ls_id_ &&
          transfer_epoch == exec_info_.transfer_parts_.at(idx).transfer_epoch_) {
        is_complete = true;
        break;
      }
    }
  }

  if (!is_complete) {
    TRANS_LOG_RET(WARN, OB_SUCCESS, "ctx exec not complete", K(trans_id_), K(epoch_),
              K(exec_info_.transfer_parts_), K(ls_id), K(epoch), K(transfer_epoch));
  }

  return is_complete;
}

int ObPartTransCtx::update_tx_data_start_and_end_scn_(const SCN start_scn,
                                                      const SCN end_scn,
                                                      const SCN transfer_scn)
{
  int ret = OB_SUCCESS;
  ObTxTable *tx_table = NULL;
  ctx_tx_data_.get_tx_table(tx_table);
  ObTxDataGuard tx_data_guard;
  ObTxDataGuard tmp_tx_data_guard;
  if (OB_ISNULL(tx_table)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx table is null", KR(ret), KPC(this));
  } else if (OB_FAIL(ctx_tx_data_.get_tx_data(tx_data_guard))) {
    TRANS_LOG(WARN, "get tx_data failed", KR(ret));
  } else if (OB_FAIL(tx_table->alloc_tx_data(tmp_tx_data_guard))) {
    TRANS_LOG(WARN, "copy tx data failed", KR(ret), KPC(this));
  } else {
    ObTxData *new_tx_data = tmp_tx_data_guard.tx_data();
    *new_tx_data = *tx_data_guard.tx_data();
    if (start_scn.is_valid()) {
      share::SCN current_start_scn = get_start_log_ts();
      if (current_start_scn.is_valid()) {
        new_tx_data->start_scn_.atomic_store(MIN(start_scn, current_start_scn));
      } else {
        new_tx_data->start_scn_.atomic_store(start_scn);
      }
    }
    new_tx_data->end_scn_.atomic_store(end_scn);
    if (OB_FAIL(tx_table->insert(new_tx_data))) {
      TRANS_LOG(WARN, "insert tx data failed", KR(ret), KPC(this));
    }
  }
  return ret;
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

int ObPartTransCtx::recover_ls_transfer_status_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  share::SCN op_scn;
  bool need_recover = false;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_, ls_handle, ObLSGetMod::TRANS_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(ls_id_));
  } else {
     // recover mds type transfer_dest_prepare and move_tx_ctx
     // when MDS replay from middle and not end we need recover this mds operation
     /*                    replay form middle
      *                            |
      *                            |
      *                            |
      *           redo            \|/          redo                    commit
      * [transfer_dest_prepare]  ------->  [move_tx_ctx] ---------->  [commit]
      *           SCN:100                    SCN:150                   SCN:200
      */
    const share::SCN start_replay_scn = ls_handle.get_ls()->get_clog_checkpoint_scn();
    for (int64_t i = 0; OB_SUCC(ret) && i < exec_info_.multi_data_source_.count(); i++) {
      bool contain_transfer_mds = false;
      ObTxDataSourceType mds_type = exec_info_.multi_data_source_[i].get_data_source_type();
      if (mds_type == ObTxDataSourceType::TRANSFER_DEST_PREPARE) {
        const ObTransferDestPrepareTxCtx &user_ctx = static_cast<const ObTransferDestPrepareTxCtx&>(*exec_info_.multi_data_source_[i].get_buffer_ctx_node().get_ctx());
        op_scn = user_ctx.get_op_scn();
        contain_transfer_mds = true;
      } else if (mds_type == ObTxDataSourceType::TRANSFER_MOVE_TX_CTX) {
        const ObTransferMoveTxCtx &user_ctx = static_cast<const ObTransferMoveTxCtx&>(*exec_info_.multi_data_source_[i].get_buffer_ctx_node().get_ctx());
        op_scn = user_ctx.get_op_scn();
        contain_transfer_mds = true;
      }
      if (OB_FAIL(ret)) {
      } else if (!contain_transfer_mds) {
        // skip
      } else if (start_replay_scn < op_scn) {
        // skip replay will do this mds operation
      } else if (exec_info_.state_ >= ObTxState::COMMIT && ctx_tx_data_.get_end_log_ts().is_valid() && start_replay_scn > ctx_tx_data_.get_end_log_ts()) {
        // skip not replay this mds operation
      } else if (OB_FAIL(ls_handle.get_ls()->get_transfer_status().update_status(trans_id_, 0, op_scn,
              transaction::NotifyType::ON_REDO, mds_type))) {
        LOG_WARN("update transfer status failed", KR(ret), K(trans_id_), K(op_scn), K(mds_type));
      } else {
        FLOG_INFO("recover ls transfer status", KR(ret), K(trans_id_), K(op_scn), K(mds_type), KPC(this));
      }
    }
  }
  return ret;
}

int ObPartTransCtx::submit_redo_log_out(ObTxLogBlock &log_block,
                                        ObTxLogCb *&log_cb,
                                        ObRedoLogSubmitHelper &helper,
                                        const int64_t replay_hint,
                                        const bool has_hold_ctx_lock,
                                        share::SCN &submitted_scn)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("submit_redo_log_out_");
  CtxLockGuard ctx_lock;
  if (!has_hold_ctx_lock) {
    get_ctx_guard(ctx_lock, CtxLockGuard::MODE::CTX);
  }
  bool with_ref = false;
  bool alloc_cb = OB_ISNULL(log_cb);
  submitted_scn.reset();
  if (alloc_cb && OB_FAIL(prepare_log_cb_(!NEED_FINAL_CB, log_cb))) {
    TRANS_LOG(WARN, "get log_cb fail", K(ret), KPC(this));
  } else if (alloc_cb && OB_FAIL(log_cb->reserve_callbacks(helper.callbacks_.count()))) {
    TRANS_LOG(WARN, "log cb reserve callbacks space fail", K(ret));
  } else if (OB_FAIL(exec_info_.redo_lsns_.reserve(exec_info_.redo_lsns_.count() + 1))) {
    TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(*this));
  } else if (FALSE_IT(with_ref = true)) {
  } else if (FALSE_IT(time_guard.click("before_submit_log_block"))) {
  } else if (OB_FAIL(submit_log_block_out_(log_block, share::SCN::min_scn(), log_cb, replay_hint))) {
    TRANS_LOG(WARN, "submit log to clog adapter failed", KR(ret), K(*this));
  } else {
    time_guard.click("submit_out_to_palf");
    submitted_scn = log_cb->get_log_ts();
    ret = after_submit_log_(log_block, log_cb, &helper);
    time_guard.click("after_submit");
    log_cb = NULL;    // moved
    with_ref = false; // moved
  }
  if (log_cb) {
    return_log_cb_(log_cb);
    log_cb = NULL;
  }
  if (with_ref) {
    release_ctx_ref_();
  }
  TRANS_LOG(DEBUG, "after submit out redo", K(ret), K(time_guard));
  return ret;
}

bool ObPartTransCtx::is_parallel_logging() const
{
  return exec_info_.serial_final_scn_.is_valid();
}

inline bool ObPartTransCtx::has_replay_serial_final_() const
{
  return exec_info_.serial_final_scn_.is_valid() &&
    exec_info_.max_applied_log_ts_ >= exec_info_.serial_final_scn_;
}

int ObPartTransCtx::set_replay_incomplete(const share::SCN log_ts) {
  CtxLockGuard guard(lock_);
  return set_replay_completeness_(false, log_ts);
}

int ObPartTransCtx::set_replay_completeness_(const bool complete, const SCN replay_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(replay_completeness_.is_unknown())) {
    if (!complete && !ctx_tx_data_.has_recovered_from_tx_table()) {
      if (OB_FAIL(supplement_tx_op_if_exist_(true, replay_scn))) {
        TRANS_LOG(WARN, "load Undo(s) from tx-table fail", K(ret), KPC(this));
      } else {
        TRANS_LOG(INFO, "replay from middle, load Undo(s) from tx-table succuess",
                  K(ret), K_(ls_id), K_(trans_id));
      }
    }
    if (OB_SUCC(ret)) {
      replay_completeness_.set(complete);
      if (!complete) {
        force_no_need_replay_checksum_(false, share::SCN::invalid_scn());
        TRANS_LOG(INFO, "incomplete replay, set skip checksum", K_(trans_id), K_(ls_id));
      }
    }
  }
  return ret;
}

inline bool ObPartTransCtx::is_support_parallel_replay_() const
{
  return cluster_version_accurate_ && cluster_version_ >= MOCK_CLUSTER_VERSION_4_2_4_0;
}

inline bool ObPartTransCtx::is_support_tx_op_() const
{
  return cluster_version_accurate_ && cluster_version_ >= CLUSTER_VERSION_4_3_2_0;
}

inline int ObPartTransCtx::switch_to_parallel_logging_(const share::SCN serial_final_scn,
                                                       const ObTxSEQ max_seq_no)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!max_seq_no.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "max seq_no of serial final log is invalid",
              K(ret), K(serial_final_scn), K(max_seq_no), KPC(this));
    print_trace_log_();
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  if (OB_SUCC(ret)) {
    // when start replaying serial final redo log or submitted serial final redo log
    // switch the Tx's logging mode to parallel logging
    // this include mark serial final scn point in exec_info_
    // and notify callback_mgr to remember the serial_final_scn
    // which used to for check whether the callback-list has replayed continuously
    // or all of it's serial logs has been synced continously
    // if reach these condition, the checksum calculations of callback-list can continues
    // into the parallel logged part
    exec_info_.serial_final_scn_.atomic_set(serial_final_scn);
    // remember the max of seq_no of redos currently submitted
    // if an rollback to savepoint before this point, which means
    // replay of this rollback-savepoint-log must pre-berrier to
    // wait serial replay parts finished
    exec_info_.serial_final_seq_no_ = max_seq_no;
    mt_ctx_.set_parallel_logging(serial_final_scn, max_seq_no);
  }
  return ret;
}

inline void ObPartTransCtx::recovery_parallel_logging_()
{
  mt_ctx_.set_parallel_logging(exec_info_.serial_final_scn_, exec_info_.serial_final_seq_no_);
  if (exec_info_.max_applied_log_ts_ >= exec_info_.serial_final_scn_) {
    // the serial final log has been synced or replayed
    // notify callback_mgr serial part is finished
    // by fake an replay success call
    mt_ctx_.replay_end(true, 0, exec_info_.serial_final_scn_);
  }
}

inline int ObPartTransCtx::correct_cluster_version_(uint64_t cluster_version_in_log)
{
  int ret = OB_SUCCESS;
  if (cluster_version_in_log > 0 && cluster_version_ != cluster_version_in_log) {
    if (cluster_version_accurate_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cluster_version is different with TxCtx", K(ret), KPC(this));
    } else {
      cluster_version_ = cluster_version_in_log;
      cluster_version_accurate_ = true;
    }
  }
  return ret;
}

int ObPartTransCtx::get_stat_for_virtual_table(share::ObLSArray &participants, int &busy_cbs_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(lock_.try_rdlock_ctx())) {
    participants.assign(exec_info_.participants_);
    busy_cbs_cnt = busy_cbs_.get_size();
    lock_.unlock_ctx();
  }
  return ret;
}

int ObPartTransCtx::check_need_transfer(
    const SCN data_end_scn,
    ObIArray<ObTabletID> &tablet_list,
    bool &need_transfer)
{
  int ret = OB_SUCCESS;
  need_transfer = true;
  SCN start_scn;
  const int64_t LOCK_OP_CHECK_LIMIT = 100;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartTransCtx not inited", KR(ret));
  } else if (!data_end_scn.is_valid() || tablet_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", KR(ret), K(data_end_scn), K(tablet_list));
  } else if (FALSE_IT(start_scn = get_start_log_ts())) {
  } else if (!start_scn.is_valid() || start_scn > data_end_scn) {
    // filter
    need_transfer = false;
  } else if (exec_info_.state_ >= ObTxState::COMMIT) {
    // filter
    need_transfer = false;
  } else if (mt_ctx_.get_lock_mem_ctx().get_lock_op_count() > LOCK_OP_CHECK_LIMIT) {
    // too many lock_op just transfer
  } else {
    bool contain = false;
    for (int64_t idx =0; OB_SUCC(ret) && !contain && idx < tablet_list.count(); idx++) {
      if (OB_FAIL(mt_ctx_.get_lock_mem_ctx().check_contain_tablet(tablet_list.at(idx), contain))) {
        TRANS_LOG(WARN, "check lock_ctx contain tablet fail", KR(ret), K(tablet_list.at(idx)), K(trans_id_), K(ls_id_));
      }
    }
    if (OB_SUCC(ret) && !contain) {
      need_transfer = false;
    }
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
