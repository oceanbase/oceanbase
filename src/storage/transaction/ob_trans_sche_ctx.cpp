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

#include "ob_trans_sche_ctx.h"
#include "ob_trans_service.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_ts_mgr.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/resolver/ob_stmt_type.h"
#include "storage/ob_partition_service.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_trans_coord_ctx.h"
#include "lib/atomic/ob_atomic.h"
#include "ob_xa_rpc.h"

namespace oceanbase {

using namespace common;
using namespace storage;
using namespace sql;
using namespace sql::stmt;
using namespace obrpc;
using namespace share;

namespace transaction {
int ObScheTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service, const bool can_elr)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObScheTransCtx inited twice", K(self), K(trans_id));
    ret = OB_INIT_TWICE;
  } else if (!is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_expired_time <= 0 || !self.is_valid() ||
             OB_ISNULL(ctx_mgr) || !trans_param.is_valid() || cluster_version <= 0 || OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(trans_expired_time),
        K(self),
        KP(ctx_mgr),
        K(trans_param),
        K(cluster_version),
        KP(trans_service));
    ret = OB_INVALID_ARGUMENT;
    //} else if (OB_FAIL(last_stmt_participants_.reserve(OB_ARRAY_RESERVE_NUM))) {
    //  TRANS_LOG(WARN, "reserve memory error", KR(ret));
  } else if (OB_FAIL(ObDistTransCtx::init(tenant_id,
                 trans_id,
                 trans_expired_time,
                 self,
                 ctx_mgr,
                 trans_param,
                 cluster_version,
                 trans_service))) {
    TRANS_LOG(WARN, "ObDistTransCtx inited error", KR(ret), K(self), K(trans_id));
  } else {
    (void)app_trace_id_str_.assign_buffer(buffer_, OB_MAX_TRACE_ID_BUFFER_SIZE);
    is_inited_ = true;
    // 1. set to is_rollback_ to true and reset when caller call end_trans
    is_rollback_ = true;
    // is_readonly_ = true;
    state_.set_state(State::UNKNOWN);
    is_tenant_active_ = true;
    commit_times_ = 0;
    can_elr_ = can_elr;
    xa_rpc_ = trans_service->get_xa_rpc();
    if (trans_param.is_autocommit()) {
      set_stc_by_now_();
    }
    xa_branch_count_ = 0;
    // xa_ref_count_ = 0;
    // in_stmt_ = 0;
  }

  return ret;
}

bool ObScheTransCtx::is_inited() const
{
  return ATOMIC_LOAD(&is_inited_);
}

void ObScheTransCtx::destroy()
{
  if (is_inited_) {
    // call back to SQL-Layer when destroy sche-ctx,
    // this is required when observer exiting but transaction
    // can't promise transited to terminated state
    if (OB_UNLIKELY(NULL != trans_desc_)) {
      trans_desc_->~ObTransDesc();
      ob_free(trans_desc_);
      trans_desc_ = NULL;
    }
    if (OB_UNLIKELY(NULL != xa_branch_info_)) {
      xa_branch_info_->~ObSEArray();
      ob_free(xa_branch_info_);
      xa_branch_info_ = NULL;
    }
    end_trans_callback_(OB_TRANS_UNKNOWN);
    REC_TRANS_TRACE_EXT(tlog_, destroy, OB_ID(uref), get_uref());
    ObDistTransCtx::destroy();
    is_inited_ = false;
  }
}

void ObScheTransCtx::reset()
{
  // destroy();
  ObDistTransCtx::reset();
  // schectx, must be set after ObDistTransCtx::reset
  magic_number_ = SCHE_CTX_MAGIC_NUM;
  is_inited_ = false;
  is_rollback_ = false;
  sql_no_ = 0;
  cur_stmt_snapshot_version_ = 0;
  state_.set_state(State::UNKNOWN);
  last_stmt_participants_.reset();
  cur_stmt_unreachable_partitions_.reset();
  stmt_rollback_participants_.reset();
  last_stmt_participants_pla_.reset();
  trans_location_cache_.reset();
  participants_pla_.reset();
  gc_participants_.reset();
  start_stmt_cond_.reset();
  xa_end_trans_cond_.reset();
  xa_sync_status_cond_.reset();
  end_stmt_cond_.reset();
  msg_mask_set_.reset();
  stmt_expired_time_ = 0;
  cluster_id_ = 0;
  commit_times_ = -1;
  is_tenant_active_ = false;
  snapshot_gene_type_ = ObTransSnapshotGeneType::UNKNOWN;
  buffer_[0] = '\0';
  app_trace_id_str_.reset();
  is_gts_waiting_ = false;
  rq_received_ts_.reset();
  gts_request_ts_ = 0;
  can_elr_ = false;
  multi_tenant_stmt_ = false;
  savepoint_sql_no_ = -1;
  stmt_rollback_info_.reset();
  last_commit_timestamp_ = 0;
  is_xa_end_trans_ = false;
  is_xa_readonly_ = false;
  xa_trans_state_ = ObXATransState::NON_EXISTING;
  is_xa_one_phase_ = false;
  trans_desc_ = NULL;
  tmp_trans_desc_ = NULL;
  xa_rpc_ = NULL;
  stmt_rollback_req_timeout_ = GCONF._ob_trans_rpc_timeout / 3;
  xa_start_cond_.reset();
  xa_branch_count_ = 0;
  xa_ref_count_ = 0;
  lock_grant_ = 0;
  is_tightly_coupled_ = true;
  lock_xid_.reset();
  if (OB_NOT_NULL(xa_branch_info_)) {
    xa_branch_info_->reset();
  }
  is_terminated_ = false;
}

int ObScheTransCtx::handle_message(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    // TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (OB_TRANS_COMMIT_RESPONSE == msg_type || OB_TRANS_ABORT_RESPONSE == msg_type) {
    const int64_t commit_times = msg.get_commit_times();
    if (OB_TRANS_ABORT_RESPONSE == msg_type && commit_times > 0 && commit_times != commit_times_) {
      TRANS_LOG(WARN, "discard abort response", "context", *this, K(msg));
    } else {
      if (OB_FAIL(handle_trans_response_(
              msg_type, msg.get_status(), msg.get_sender(), msg.get_sender_addr(), msg.get_need_wait_interval_us()))) {
        TRANS_LOG(WARN, "handle transaction response error", K(ret), "context", *this, K(msg));
      } else if (OB_SUCCESS != msg.get_status()) {
        TRANS_LOG(WARN, "handle transaction response", "context", *this, K(msg));
      } else {
        // do nothing
      }
      (void)unregister_timeout_task_();
      set_exiting_();
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  } else if (OB_TRANS_XA_PREPARE_RESPONSE == msg_type) {
    if (OB_FAIL(handle_xa_trans_response_(msg_type, msg.get_status()))) {
      TRANS_LOG(WARN, "handle xa transaction response error", K(ret), "context", *this, K(msg));
    } else if (OB_SUCCESS != msg.get_status()) {
      TRANS_LOG(WARN, "handle xa transaction response", "context", *this, K(msg));
    } else {
      // do nothing
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  } else if (OB_TRANS_XA_ROLLBACK_RESPONSE == msg_type) {
    if (OB_FAIL(handle_xa_trans_response_(msg_type, msg.get_status()))) {
      TRANS_LOG(WARN, "handle xa transaction response error", K(ret), "context", *this, K(msg));
    } else if (OB_TRANS_ROLLBACKED != msg.get_status()) {
      TRANS_LOG(WARN, "handle xa transaction response", "context", *this, K(msg));
    } else {
      // do nothing
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  } else if (OB_TRANS_START_STMT_RESPONSE == msg_type || OB_TRANS_STMT_ROLLBACK_RESPONSE == msg_type ||
             OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE == msg_type) {
    if (request_id_ != msg.get_request_id()) {
      TRANS_LOG(WARN, "transaction message is the last round, discard it", "context", *this, K(msg));
    } else if (OB_FAIL(handle_stmt_response_(msg))) {
      TRANS_LOG(WARN, "handle stmt response error", KR(ret), "context", *this, K(msg));
    } else {
      // do nothing
    }
    // REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  } else if (OB_TRANS_CLEAR_RESPONSE == msg_type) {
    if (request_id_ != msg.get_request_id()) {
      TRANS_LOG(WARN, "transaction message is the last round, discard it", "context", *this, K(msg));
    } else if (OB_FAIL(
                   handle_trans_response_(msg_type, msg.get_status(), msg.get_sender(), msg.get_sender_addr(), 0))) {
      TRANS_LOG(WARN, "handle transaction response error", KR(ret), "context", *this, K(msg));
    } else {
      // do nothing
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  } else {
    TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg), K(msg_type));
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  }

  return ret;
}

int ObScheTransCtx::drive_()
{
  bool need_rollback = false;
  return drive_(need_rollback);
}

int ObScheTransCtx::drive_(bool& need_callback)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_.state_);

  if (is_xa_local_trans() && is_xa_end_trans_ && !is_xa_one_phase_) {
    TRANS_LOG(INFO, "drive xa end trans", "context", *this);
    if (OB_FAIL(xa_end_trans_(is_rollback_))) {
      if (need_retry_end_trans(ret)) {
        // rewrite ret
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "end trans error", K(ret));
      }
    }
  } else if (OB_FAIL(end_trans_(is_rollback_, need_callback))) {
    if (need_retry_end_trans(ret)) {
      // rewrite ret
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "end trans error", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(state_helper.switch_state(Ops::END_TRANS))) {
      TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
    }
  }

  return ret;
}

int ObScheTransCtx::inactive(ObEndTransCallbackArray& cb_array)
{
  int ret = OB_SUCCESS;
  ObEndTransCallbackItem cb_item;
  int cb_param = OB_TRANS_UNKNOWN;

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    is_tenant_active_ = false;
    if (NULL != end_trans_cb_.get_cb()) {
      cb_item.cb_ = end_trans_cb_.get_cb();
      cb_item.retcode_ = cb_param;
      end_trans_cb_.reset();
      ret = cb_array.push_back(cb_item);
    }
  }

  return ret;
}

int ObScheTransCtx::kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
{
  UNUSED(cb_array);
  UNUSED(arg);
  int ret = OB_SUCCESS;
  ObEndTransCallbackItem cb_item;
  int cb_param = OB_TRANS_UNKNOWN;

  common::ObTimeGuard timeguard("sche_kill", 10 * 1000);
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    need_print_trace_log_ = true;
    (void)unregister_timeout_task_();
    set_exiting_();
    // notify sql thread blocked on start_stmt/end_stmt call,
    // in order to fastly quit observer
    start_stmt_cond_.notify(OB_TRANS_KILLED);
    end_stmt_cond_.notify(OB_TRANS_KILLED);
    xa_end_trans_cond_.notify(OB_TRANS_KILLED);
    if (NULL != end_trans_cb_.get_cb()) {
      cb_item.cb_ = end_trans_cb_.get_cb();
      cb_item.retcode_ = cb_param;
      end_trans_cb_.reset();
      ret = cb_array.push_back(cb_item);
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      kill,
      OB_ID(ret),
      ret,
      OB_ID(arg1),
      arg.graceful_,
      OB_ID(used),
      timeguard.get_diff(),
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObScheTransCtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();

  common::ObTimeGuard timeguard("sche_handle_timeout", 10 * 1000);
  if (OB_SUCC(lock_.lock(5000000 /*5 seconds*/))) {
    CtxLockGuard guard(lock_, false);

    TRANS_LOG(DEBUG, "handle scheduler transaction timeout", "context", *this);
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObScheTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (!timeout_task_.is_registered()) {
      // timer task is canceled, do nothing
    } else if (is_gts_waiting_) {
      // do nothing
    } else if (!is_xa_local_trans()) {
      (void)unregister_timeout_task_();
      timeout_task_.set_running(true);
      if (now >= (trans_expired_time_ + OB_TRANS_WARN_USE_TIME)) {
        TRANS_LOG(ERROR, "transaction use too much time", "context", *this);
      }
      need_refresh_location_ = true;
      if (now >= trans_expired_time_) {
        end_trans_callback_(is_rollback_, OB_TRANS_TIMEOUT);
        ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id_, 1);
        // rollback
        if (!has_decided_()) {
          is_rollback_ = true;
          if (OB_FAIL(drive_())) {
            TRANS_LOG(WARN, "drive scheduler error", KR(ret), "context", *this);
          }
        }
        set_exiting_();
      } else {
        int64_t left_time = OB_INVALID_TIMESTAMP;
        if (has_decided_()) {
          if (OB_FAIL(drive_())) {
            TRANS_LOG(WARN, "drive scheduler error", K(ret), "context", *this);
          } else if (0 == commit_times_) {
            left_time = ObServerConfig::get_instance().trx_2pc_retry_interval;
          } else {
            left_time = MAX_TRANS_2PC_TIMEOUT_US;
          }
        } else {
          left_time = trans_expired_time_ - ObClockGenerator::getClock();
        }
        if (OB_SUCC(ret) && OB_FAIL(register_timeout_task_(left_time))) {
          TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
        }
        if (OB_FAIL(ret)) {
          end_trans_callback_(is_rollback_, OB_TRANS_UNKNOWN);
          set_exiting_();
        }
      }
      need_refresh_location_ = false;
      if (stmt_expired_time_ > 0 && now >= stmt_expired_time_) {
        end_trans_callback_(is_rollback_, OB_TRANS_STMT_TIMEOUT);
      }
      timeout_task_.set_running(false);
    } else {
      (void)unregister_timeout_task_();
      timeout_task_.set_running(true);
      need_refresh_location_ = true;
      TRANS_LOG(INFO, "xa trans handle timeout", K(*this));

      if (now >= trans_expired_time_ + OB_TRANS_WARN_USE_TIME) {
        TRANS_LOG(ERROR, "xa transaction use too much time", "context", *this);
      }
      int64_t left_time = OB_INVALID_TIMESTAMP;
      if (commit_times_ == 0) {
        left_time = ObServerConfig::get_instance().trx_2pc_retry_interval;
      } else {
        left_time = MAX_TRANS_2PC_TIMEOUT_US;
      }
      if (has_decided_()) {
        if (!is_xa_one_phase_ && !is_xa_end_trans_) {
          // xa prepare
          if (now >= trans_expired_time_ && commit_times_ > 5) {
            TRANS_LOG(INFO, "xa prepare timeout", K(*this));
            end_trans_callback_(is_rollback_, OB_TRANS_XA_RMFAIL);
            set_exiting_();
          } else {
            int64_t state = ObXATransState::UNKNOWN;
            int64_t end_flag = ObXAFlag::TMNOFLAGS;
            if (OB_FAIL(trans_service_->query_xa_state_and_flag(tenant_id_, xid_, state, end_flag))) {
              TRANS_LOG(WARN, "query xa state failed", K(ret), K(*this));
            }
            if (OB_SUCC(ret) && ObXATransState::ROLLBACKED == state) {
              TRANS_LOG(INFO, "xa trans has rollbcked", K_(trans_id), K_(xid));
              end_trans_callback_(is_rollback_, OB_TRANS_XA_RBROLLBACK);
              set_exiting_();
            } else if (OB_SUCC(ret) && ObXATransState::PREPARED == state) {
              TRANS_LOG(INFO, "xa trans has prepared", K_(trans_id), K_(xid));
              end_trans_callback_(is_rollback_, OB_SUCCESS);
              set_exiting_();
            } else {
              if (OB_FAIL(drive_())) {
                TRANS_LOG(WARN, "drive scheduler error", K(ret), "context", *this);
              } else {
                TRANS_LOG(INFO, "drive xa prepare success", K_(trans_id), K_(xid));
              }
              if (OB_FAIL(register_timeout_task_(left_time))) {
                TRANS_LOG(WARN, "register timeout task failed", K(ret), K(*this));
              }
            }
          }
        } else if (!is_xa_one_phase_ && is_xa_end_trans_) {
          // xa commit/rollback
          int64_t state = ObXATransState::UNKNOWN;
          int64_t end_flag = ObXAFlag::TMNOFLAGS;
          int tmp_ret = OB_SUCCESS;
          ;
          if (OB_FAIL(trans_service_->query_xa_state_and_flag(tenant_id_, xid_, state, end_flag))) {
            TRANS_LOG(WARN, "query xa state failed", K(ret), K(*this));
          }
          if (OB_SUCC(ret) && (ObXATransState::ROLLBACKED == state || ObXATransState::COMMITTED == state)) {
            if ((is_rollback_ && ObXATransState::COMMITTED == state) ||
                (!is_rollback_ && ObXATransState::ROLLBACKED == state)) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(ERROR, "unexpected error", K(ret), K(state), K(*this));
            } else {
              TRANS_LOG(INFO, "xa trans has ended", K_(trans_id), K_(xid));
            }
            if (!is_tightly_coupled_) {
              // loosely
              if (OB_SUCCESS != (tmp_ret = trans_service_->delete_xa_record(tenant_id_, xid_))) {
                TRANS_LOG(WARN, "delete xa reocord failed", K(ret), K(*this));
              }
            } else {
              // tightly
              // is_terminated_ = true;
              set_terminated_();
              if (OB_SUCCESS != (tmp_ret = trans_service_->delete_xa_all_tightly_branch(tenant_id_, xid_))) {
                TRANS_LOG(WARN, "delete all tightly branch from inner table failed", K(tmp_ret), K(*this));
              }
            }
            xa_end_trans_cond_.notify(ret);
            set_exiting_();
          } else {
            if (OB_FAIL(drive_())) {
              TRANS_LOG(WARN, "drive scheduler error", K(ret), "context", *this);
            } else {
              TRANS_LOG(INFO, "drive xa commit/rollback success", K_(trans_id), K_(xid));
            }
            if (OB_FAIL(register_timeout_task_(left_time))) {
              TRANS_LOG(WARN, "register timeout task failed", K(ret), K(*this));
            }
          }
        } else {
          // one phase
          if (now >= trans_expired_time_ && commit_times_ > 5) {
            TRANS_LOG(INFO, "one phase xa commit/rollback timeout", K(*this));
            xa_end_trans_cond_.notify(OB_TRANS_XA_RMFAIL);
            set_exiting_();
          } else {
            if (OB_FAIL(drive_())) {
              TRANS_LOG(WARN, "drive scheduler error", K(ret), "context", *this);
            }
            if (OB_FAIL(register_timeout_task_(left_time))) {
              TRANS_LOG(WARN, "register timeout task failed", K(ret), K(*this));
            }
          }
        }
      } else {
        TRANS_LOG(INFO, "xa trans timeout, going to rollback", K(*this));
        is_rollback_ = true;
        end_trans_callback_(is_rollback_, OB_TRANS_XA_RBROLLBACK);
        if (OB_FAIL(drive_())) {
          TRANS_LOG(WARN, "drive xa scheduler error", K(ret), "context", *this);
        }
        if (!is_tightly_coupled_) {
          // loosely
          if (OB_FAIL(trans_service_->delete_xa_record(tenant_id_, xid_))) {
            TRANS_LOG(WARN, "delete xa reocord failed", K(ret), K(*this));
          }
        } else {
          // tightly
          // is_terminated_ = true;
          set_terminated_();
          if (OB_FAIL(trans_service_->delete_xa_all_tightly_branch(tenant_id_, xid_))) {
            TRANS_LOG(WARN, "delete all tightly branch from inner table failed", K(ret), K(*this));
          }
        }
        set_exiting_();
      }
      need_refresh_location_ = false;
      timeout_task_.set_running(false);
    }

    REC_TRANS_TRACE_EXT(tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(uref), get_uref());
  } else {
    TRANS_LOG(WARN, "failed to acquire lock in specified time", K_(trans_id));
    unregister_timeout_task_();
    register_timeout_task_(delay);
  }

  return ret;
}

int ObScheTransCtx::start_trans(const ObTransDesc& trans_desc)
{
  UNUSED(trans_desc);
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_.state_);

  trans_start_time_ = ObClockGenerator::getRealClock();
  const int64_t left_time = trans_expired_time_ - trans_start_time_;

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(left_time <= 0)) {
    TRANS_LOG(WARN, "transaction is timeout", "context", *this, K(left_time));
    ret = OB_TRANS_TIMEOUT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(left_time <= 0)) {
    TRANS_LOG(WARN, "transaction is timeout", K_(trans_expired_time), "context", *this);
    ret = OB_TRANS_TIMEOUT;
  } else if (OB_FAIL(register_timeout_task_(left_time + trans_id_.hash() % 1000000))) {
    TRANS_LOG(WARN, "register timeout task error", KR(ret), "context", *this, K(left_time));
  } else if (OB_FAIL(state_helper.switch_state(Ops::START_TRANS))) {
    TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
    if (OB_NOT_INIT != ret) {
      set_exiting_();
      (void)unregister_timeout_task_();
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, start_trans, OB_ID(ret), ret, OB_ID(uref), get_uref());

  return ret;
}

void ObScheTransCtx::check_inner_table_commit_()
{
  // check whether these three table and other tables modified in a transaction
  // __all_core_table __all_root_table __all_tenant_meta_table
  int32_t mask = 0x0;
  for (int64_t i = 0; i < participants_.count(); i++) {
    if (OB_ALL_CORE_TABLE_TID == extract_pure_id(participants_.at(i).get_table_id())) {
      mask |= 0x1;
    } else if (OB_ALL_ROOT_TABLE_TID == extract_pure_id(participants_.at(i).get_table_id())) {
      mask |= 0x2;
    } else if (OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(participants_.at(i).get_table_id())) {
      mask |= 0x4;
    } else {
      mask |= 0x8;
    }
    if (mask > 0x8 || mask == 0x3 || mask == 0x5 || mask == 0x6 || mask == 0x7) {
      TRANS_LOG(ERROR, "attention, transaction contains __all_core_table and other table", K(*this));
      break;
    }
  }
}

int ObScheTransCtx::kill_trans(const ObTransDesc& trans_desc)
{
  TRANS_LOG(DEBUG, "sche_ctx kill_trans", K(trans_desc));
  int ret = OB_SUCCESS;
  int64_t trans_expired_time = trans_desc.get_trans_expired_time();
  ObNullEndTransCallback cb;
  MonotonicTs commit_time(INT64_MAX);
  if (OB_FAIL(end_trans(trans_desc, true, &cb, trans_expired_time, INT64_MAX, commit_time))) {
    TRANS_LOG(WARN, "kill trans failed", K(ret), K(trans_desc));
  }
  return ret;
}

int ObScheTransCtx::end_trans(const ObTransDesc& trans_desc, const bool is_rollback, sql::ObIEndTransCallback* cb,
    const int64_t trans_expired_time, const int64_t stmt_expired_time, const MonotonicTs commit_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_callback = false;

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_ISNULL(cb) || OB_UNLIKELY(trans_expired_time <= 0) || OB_UNLIKELY(stmt_expired_time <= 0) ||
             OB_UNLIKELY(!commit_time.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", KP(cb), K(trans_expired_time), K(stmt_expired_time), K(commit_time));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_xa_local_trans()) {
    if (has_decided_()) {
      if (is_xa_one_phase_) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "xa trans has entered into one phase", K(ret), "context", *this);
      } else if (ObXATransState::PREPARED == xa_trans_state_) {
        TRANS_LOG(INFO, "xa trans already in prepared state", K(*this));
        ret = OB_TRANS_HAS_DECIDED;
      } else {
        TRANS_LOG(INFO, "xa trans preparing", K(*this));
        ret = OB_TRANS_XA_RETRY;
      }
    } /* else if (ObXATransState::ACTIVE == xa_trans_state_) {
       int64_t state = ObXATransState::NON_EXISTING;
       int64_t end_flag = ObXAEndFlag::TMNOFLAGS;
       if (OB_FAIL(trans_service_->query_xa_state_and_flag(tenant_id_, xid_, state, end_flag))) {
         TRANS_LOG(WARN, "fail to query xa trans from inner table", K(ret), "context", *this);
       } else if (ObXATransState::IDLE != state) {
         ret = OB_TRANS_XA_PROTO;
         TRANS_LOG(WARN, "invalid xa trans one phase request", K(ret), K(state), "context", *this);
       } else {
         xa_trans_state_ = ObXATransState::IDLE;
       }
     }*/
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(end_trans_cb_.init(tenant_id_, cb))) {
      TRANS_LOG(ERROR, "end trans callback init error", KR(ret));
      // if start_stmt success, and then session killed,
      // participants of the newly started stmt must been rollbacked also
    } else if (OB_FAIL(stmt_rollback_info_.assign(trans_desc.get_stmt_rollback_info()))) {
      TRANS_LOG(WARN, "stmt rollback info assign failed", K(ret));
    } else if (State::START_STMT == state_.get_state() && is_rollback) {
      if (trans_param_.is_bounded_staleness_read()) {
        if (OB_FAIL(merge_participants_pla_())) {
          TRANS_LOG(WARN, "merge participants pla error", KR(ret), K(is_rollback), "context", *this);
        }
      } else {
        if (OB_FAIL(merge_participants_(last_stmt_participants_))) {
          TRANS_LOG(WARN, "merge participants error", KR(ret), K(is_rollback), "context", *this);
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_app_trace_info_(trans_desc.get_trace_info().get_app_trace_info());
      set_stc_(commit_time);
      //(void)check_inner_table_commit_();
      if (OB_FAIL(end_trans_(is_rollback, trans_expired_time, stmt_expired_time, need_callback))) {
        TRANS_LOG(WARN, "sche end trans error", KR(ret), K(*this));
      }
    }
    // release ctx which no need to commit
    if (!trans_param_.is_bounded_staleness_read()) {
      if (OB_SUCCESS != (tmp_ret = submit_trans_request_(OB_TRANS_DISCARD_REQUEST))) {
        TRANS_LOG(WARN, "submit discard request error", "ret", tmp_ret, K_(participants_pla), "context", *this);
      }
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "end transaction error", KR(ret), "context", *this);
      need_print_trace_log_ = true;
      end_trans_cb_.reset();
      if (OB_NOT_INIT != ret) {
        (void)unregister_timeout_task_();
        set_exiting_();
      }
    } else {
      if (need_callback) {
        end_trans_callback_(is_rollback, OB_SUCCESS);
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, end_trans, OB_ID(ret), ret, OB_ID(is_rollback), is_rollback, OB_ID(uref), get_uref());

  return ret;
}

/*
 * mark sche_ctx in start_stmt state
 * this function used when sp_trans stmt rollback fail
 * and create sche_ctx to rollback stmt (which call end_stmt)
 */
int ObScheTransCtx::mark_stmt_started()
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_.state_);
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (State::END_TRANS == state_.get_state()) {
    TRANS_LOG(WARN, "transaction terminated", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (State::START_TRANS != state_.get_state()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "transaction not in start_trans state", KR(ret), K_(state), "context", *this);
  } else if (OB_FAIL(state_helper.switch_state(Ops::START_STMT))) {
    TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
  }
  return ret;
}

/*
 * Attention:
 * In ObTransService::start_stmt, we always call scheduler::start_stmt
 * AND scheduler::wait_start_stmt *unconditionally*
 * So make sure we notify start_stmt_cond_ in those cases that do not need wait
 * Case 1: all participants in this stmt has rpc create ctx before
 * Case 2: this stmt is LOCAL/REMOTE plan
 * See comment "no need wait, notify cond now" in this function
 */
int ObScheTransCtx::start_stmt(
    const ObPartitionLeaderArray& partition_leader_arr, const ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_.state_);
  ObPhyPlanType phy_plan_type = trans_desc.get_cur_stmt_desc().phy_plan_type_;
  const ObPartitionArray& participants = partition_leader_arr.get_partitions();
  int64_t trans_receive_sql_us = ObTimeUtility::current_time();
  const stmt::StmtType stmt_type = trans_desc.get_cur_stmt_desc().stmt_type_;

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid() || OB_UNLIKELY(!stmt_desc.is_valid()))) {
    TRANS_LOG(WARN, "invaild argument", K(participants), K(trans_desc), K(stmt_desc));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_dup_table_trans_ = trans_desc.is_dup_table_trans();
    snapshot_gene_type_ = trans_desc.get_snapshot_gene_type();
    cluster_id_ = trans_desc.get_cluster_id();
    stmt_expired_time_ = trans_desc.get_cur_stmt_expired_time();
    // multi-tenant stmt need consult snapshot
    multi_tenant_stmt_ = partition_leader_arr.count() > 0 ? !partition_leader_arr.is_single_tenant() : false;
    start_stmt_cond_.reset();

    if (OB_FAIL(start_new_sql_(trans_desc.get_sql_no()))) {
      TRANS_LOG(WARN, "start new sql error", KR(ret), K(trans_desc));
    } else if (trans_param_.is_bounded_staleness_read()) {
      if (participants.count() <= 0) {
        TRANS_LOG(WARN, "unexpected participant count", K(trans_desc), K(participants));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(handle_start_bounded_staleness_stmt_(partition_leader_arr, trans_desc))) {
        TRANS_LOG(WARN, "handle start bounded staleness stmt error", KR(ret), K(trans_desc), K(stmt_desc));
      } else if (OB_FAIL(state_helper.switch_state(Ops::START_STMT))) {
        TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
      } else {
        // do nothing
      }
    } else if (OB_FAIL(merge_partition_leader_info_(partition_leader_arr))) {
      TRANS_LOG(WARN, "merge partition leader info error", KR(ret), "context", *this, K(partition_leader_arr));
    } else {
      if (OB_FAIL(last_stmt_participants_.assign(participants))) {
        TRANS_LOG(WARN, "assign participant error", KR(ret), "context", *this, K(participants));
      } else {
        if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type_ ||
            ObTransSnapshotGeneType::NOTHING == snapshot_gene_type_) {
          TRANS_LOG(
              DEBUG, "need not consult snapshot version", K(snapshot_gene_type_), K(trans_desc), "context", *this);
          start_stmt_cond_.notify(OB_SUCCESS);
        } else if (OB_UNLIKELY(ObTransSnapshotGeneType::CONSULT != snapshot_gene_type_)) {
          ret = OB_NOT_SUPPORTED;
          TRANS_LOG(
              WARN, "unknown snapshot generate type", KR(ret), K(snapshot_gene_type_), K(trans_desc), "context", *this);
        } else {
          msg_mask_set_.reset();
          if (OB_FAIL(msg_mask_set_.init(participants))) {
            TRANS_LOG(WARN, "message mask set init error", KR(ret), "context", *this, K(participants));
          } else if (OB_FAIL(submit_stmt_request_(participants, OB_TRANS_START_STMT_REQUEST, trans_desc))) {
            TRANS_LOG(WARN, "submit start stmt request error", KR(ret), "context", *this, K(participants));
          } else {
            // do nothing
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(state_helper.switch_state(Ops::START_STMT))) {
            TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      need_print_trace_log_ = true;
      if (OB_NOT_INIT != ret && OB_NOT_MASTER != ret) {
        set_exiting_();
        (void)unregister_timeout_task_();
      }
    }

    if (OB_NOT_NULL(trans_audit_record_)) {
      const int64_t proxy_receive_sql_us = 0;  // stmt_desc.get_proxy_receive_us();
      const int64_t server_receive_sql_us = stmt_desc.cur_query_start_time_;
      (void)trans_audit_record_->set_start_stmt_info(sql_no_,
          stmt_desc.phy_plan_type_,
          stmt_desc.trace_id_adaptor_,
          proxy_receive_sql_us,
          server_receive_sql_us,
          trans_receive_sql_us);
    }
  }

  REC_TRANS_TRACE_EXT(tlog_,
      start_stmt,
      OB_ID(ret),
      ret,
      OB_ID(tenant_id),
      tenant_id_,
      OB_ID(sql_no),
      sql_no_,
      OB_ID(phy_plan_type),
      phy_plan_type,
      OB_ID(stmt_type),
      stmt_type,
      OB_ID(sql_id),
      stmt_desc.get_sql_id(),
      OB_ID(trace_id),
      stmt_desc.trace_id_adaptor_,
      OB_ID(start_time),
      stmt_desc.cur_query_start_time_,
      OB_ID(uref),
      get_uref());
  TRANS_LOG(DEBUG, "start stmt", KR(ret), K(trans_id_), K(stmt_desc));

  return ret;
}

int ObScheTransCtx::get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts)
{
  UNUSED(receive_gts_ts);
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  const int64_t left_time = max(trans_expired_time_ - ObClockGenerator::getRealClock(), trans_2pc_timeout_);
  {
    CtxLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObScheTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      need_revert_ctx = true;
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (OB_UNLIKELY(!is_gts_waiting_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "transaction is not waiting gts", KR(ret), "context", *this);
    } else {
      if (EXECUTE_COUNT_PER_SEC(5)) {
        TRANS_LOG(INFO, "get gts callback", K_(stc), K(srr), K(gts), "context", *this);
      }
      if (srr < rq_received_ts_) {
        ret = OB_EAGAIN;
      } else {
        cur_stmt_snapshot_version_ = gts;
        is_gts_waiting_ = false;
        need_revert_ctx = true;
        (void)unregister_timeout_task_();
        // register timeout task until transaction timeout
        if (OB_FAIL(register_timeout_task_(left_time))) {
          TRANS_LOG(WARN, "register timeout handler error", KR(ret));
        }
        start_stmt_cond_.notify(OB_SUCCESS);
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, get_gts_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts), OB_ID(uref), get_uref());
  }
  if (need_revert_ctx) {
    (void)partition_mgr_->revert_trans_ctx_(this);
  }
  return ret;
}

int ObScheTransCtx::handle_start_bounded_staleness_stmt_(
    const ObPartitionLeaderArray& pla, const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(merge_stmt_participants_pla_(pla))) {
    TRANS_LOG(WARN, "merge partition location info error", KR(ret), "context", *this, K(pla));
  } else {
    if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type_ ||
        ObTransSnapshotGeneType::NOTHING == snapshot_gene_type_) {
      TRANS_LOG(DEBUG, "need not consult snapshot version", K(snapshot_gene_type_), K(trans_desc), "context", *this);
      start_stmt_cond_.notify(OB_SUCCESS);
    } else if (ObTransSnapshotGeneType::CONSULT == snapshot_gene_type_) {
      msg_mask_set_.reset();
      if (OB_FAIL(msg_mask_set_.init(pla))) {
        TRANS_LOG(WARN, "message mask set init error", KR(ret), "context", *this, K(pla));
      } else if (OB_FAIL(submit_bounded_staleness_request_(
                     pla, OB_TRANS_START_STMT_REQUEST, &const_cast<ObTransDesc&>(trans_desc)))) {
        TRANS_LOG(WARN, "submit bounded staleness start stmt request error", KR(ret), "context", *this, K(pla));
      } else {
        // do nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unknown snapshot generate type", K(snapshot_gene_type_), K(trans_desc), "context", *this);
    }
  }

  return ret;
}

// return value: OB_SUCCESS; OB_COND_TIMEOUT; other error
int ObScheTransCtx::wait_start_stmt(ObTransDesc& trans_desc, const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int result = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(expired_time <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t max_left_waittime = expired_time - ObClockGenerator::getRealClock();
    const int64_t rpc_timeout_us = GCONF._ob_trans_rpc_timeout + 100 * 1000;
    const int64_t waittime = ((max_left_waittime > rpc_timeout_us) ? rpc_timeout_us : max_left_waittime);
    if (OB_UNLIKELY(waittime <= 0)) {
      ret = OB_TRANS_STMT_TIMEOUT;
    } else if (OB_FAIL(start_stmt_cond_.wait(waittime, result)) || OB_FAIL(result)) {
      if (OB_TIMEOUT == ret) {
        if (ObTimeUtility::current_time() < expired_time) {
          ret = OB_TRANS_STMT_NEED_RETRY;
        } else {
          ret = OB_TRANS_STMT_TIMEOUT;
        }
      }
      ObPartitionArray noresponse_participants;
      ObPartitionLeaderArray noresponse_participants_pla;
      if (trans_param_.is_bounded_staleness_read()) {
        if (msg_mask_set_.is_inited() &&
            OB_SUCCESS != (tmp_ret = msg_mask_set_.get_not_mask(noresponse_participants_pla))) {
          TRANS_LOG(WARN, "get no response participants error", "ret", tmp_ret);
        }
      } else {
        if (msg_mask_set_.is_inited() &&
            OB_SUCCESS != (tmp_ret = msg_mask_set_.get_not_mask(noresponse_participants))) {
          TRANS_LOG(WARN, "get no response participants error", "ret", tmp_ret);
        }
      }
      TRANS_LOG(WARN,
          "wait start stmt error",
          KR(ret),
          "context",
          *this,
          K(waittime),
          K(result),
          K(noresponse_participants),
          K(noresponse_participants_pla),
          K_(cur_stmt_unreachable_partitions));
    } else {
      bool need_generate_snapshot = true;
      if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type_ ||
          ObTransSnapshotGeneType::NOTHING == snapshot_gene_type_) {
        need_generate_snapshot = false;
      } else if (ObTransSnapshotGeneType::CONSULT == snapshot_gene_type_) {
        need_generate_snapshot = true;
      } else {
        TRANS_LOG(ERROR, "invalid snapshot gene type, unexpected error", "context", *this);
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCC(ret) && need_generate_snapshot) {
        if (cur_stmt_snapshot_version_ < 0) {
          TRANS_LOG(ERROR,
              "unexpected snapshot version",
              K(snapshot_gene_type_),
              K_(cur_stmt_snapshot_version),
              "context",
              *this);
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(trans_desc.set_snapshot_version(cur_stmt_snapshot_version_))) {
          TRANS_LOG(WARN, "set snapshot version error", KR(ret), "context", *this);
        } else {
          // do nothing
        }
      }
    }
    multi_tenant_stmt_ = false;
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObScheTransCtx::start_savepoint_rollback(
    const ObTransDesc& trans_desc, const int64_t sql_no, const ObPartitionArray& rollback_partitions)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid())) {
    TRANS_LOG(WARN, "invaild argument", K(trans_desc), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    end_stmt_cond_.reset();
    msg_mask_set_.reset();
    savepoint_sql_no_ = sql_no;
    sql_no_ = trans_desc.get_sql_no();
    if (rollback_partitions.count() <= 0) {
      end_stmt_cond_.notify(OB_SUCCESS);
    } else if (OB_FAIL(msg_mask_set_.init(rollback_partitions))) {
      TRANS_LOG(WARN, "message mask set init error", KR(ret), "context", *this, K(rollback_partitions));
    } else if (OB_FAIL(submit_stmt_request_(rollback_partitions, OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST, trans_desc))) {
      TRANS_LOG(WARN, "submit savepoint rollback request error", KR(ret), K(trans_desc));
    } else {
      // do nothing
    }
  }

  need_print_trace_log_ = true;
  REC_TRANS_TRACE_EXT(tlog_,
      start_savepoint_rollback,
      OB_ID(ret),
      ret,
      OB_ID(rollback_to),
      savepoint_sql_no_,
      OB_ID(rollback_from),
      sql_no_,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObScheTransCtx::wait_savepoint_rollback(const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(expired_time <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t max_left_waittime = expired_time - ObClockGenerator::getRealClock();
    const int64_t rpc_timeout_us = ObTransRpcResult::OB_TRANS_RPC_TIMEOUT_THRESHOLD + 100 * 1000;
    const int64_t waittime = ((max_left_waittime > rpc_timeout_us) ? rpc_timeout_us : max_left_waittime);
    if (waittime <= 0) {
      ret = OB_TRANS_STMT_TIMEOUT;
    } else if (OB_FAIL(end_stmt_cond_.wait(waittime, result)) || OB_FAIL(result)) {
      if (ObTimeUtility::current_time() < expired_time && OB_TIMEOUT == ret) {
        ret = OB_TRANS_NEED_ROLLBACK;
      }
      TRANS_LOG(WARN, "wait savepoint rollback error", K(ret), K(*this), K(waittime), K(result));
    } else {
      // do nothing
    }
  }

  REC_TRANS_TRACE_EXT(tlog_, wait_savepoint_rollback, Y(ret), OB_ID(uref), get_uref());
  return ret;
}

int ObScheTransCtx::end_stmt(const bool is_rollback, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_.state_);

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid())) {
    TRANS_LOG(WARN, "invaild argument", K(trans_desc), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    end_stmt_cond_.reset();
    savepoint_sql_no_ = trans_desc.get_stmt_min_sql_no();
    sql_no_ = trans_desc.get_sql_no();
    // Attention Please:
    // if is_rollback=false, maybe need rpc end_task as well
    // Currently, participants do trivial work in end_task_ when is_rollback=false
    const ObPhyPlanType phy_plan_type = trans_desc.get_cur_stmt_desc().phy_plan_type_;
    const bool is_bounded_staleness_read = trans_param_.is_bounded_staleness_read();
    if (OB_PHY_PLAN_DISTRIBUTED == phy_plan_type || OB_PHY_PLAN_UNCERTAIN == phy_plan_type ||
        OB_PHY_PLAN_REMOTE == phy_plan_type || OB_PHY_PLAN_LOCAL == phy_plan_type) {
      if (!is_rollback) {
        end_stmt_cond_.notify(OB_SUCCESS);
      } else {
        msg_mask_set_.reset();
        if (is_bounded_staleness_read) {
          if (OB_FAIL(msg_mask_set_.init(last_stmt_participants_pla_))) {
            TRANS_LOG(WARN, "message mask set init error", KR(ret), "context", *this);
          } else if (0 == last_stmt_participants_pla_.count()) {
            end_stmt_cond_.notify(OB_SUCCESS);
          } else if (OB_FAIL(submit_bounded_staleness_request_(
                         last_stmt_participants_pla_, OB_TRANS_STMT_ROLLBACK_REQUEST))) {
            TRANS_LOG(WARN, "submit bounded staleness stmt rollback request error", KR(ret), K(trans_desc));
          } else {
            // do nothing
          }
        } else {
          const ObPartitionArray& tmp_array = trans_desc.get_stmt_participants();
          if (OB_FAIL(stmt_rollback_participants_.assign(tmp_array))) {
            TRANS_LOG(WARN, "assign stmt rollback participants failed", K(ret));
          } else if (OB_FAIL(msg_mask_set_.init(tmp_array))) {
            TRANS_LOG(WARN, "message mask set init error", K(ret), "context", *this, K(tmp_array));
          } else if (0 == tmp_array.count()) {
            end_stmt_cond_.notify(OB_SUCCESS);
          } else if (OB_FAIL(submit_stmt_request_(tmp_array, OB_TRANS_STMT_ROLLBACK_REQUEST, trans_desc))) {
            TRANS_LOG(WARN, "submit stmt rollback request error", KR(ret), K(trans_desc));
          } else {
            // do nothing
          }
        }
      }
    } else {
      TRANS_LOG(ERROR, "unknown phy plan type", K(phy_plan_type));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(state_helper.switch_state(Ops::END_STMT))) {
      TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
    }
  }
  if (OB_FAIL(ret) || is_rollback) {
    need_print_trace_log_ = true;
  }
  REC_TRANS_TRACE_EXT(tlog_,
      end_stmt,
      OB_ID(ret),
      ret,
      OB_ID(is_rollback),
      is_rollback,
      OB_ID(rollback_to),
      savepoint_sql_no_,
      OB_ID(rollback_from),
      sql_no_,
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObScheTransCtx::wait_end_stmt(const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int result = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(expired_time <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    stmt_rollback_req_timeout_ = GCONF._ob_trans_rpc_timeout / 3;
    bool need_retry_rpc = true;
    int64_t retry_count = 0;
    while (OB_SUCC(ret) && need_retry_rpc) {
      // 1s->2s->3s->3s...
      stmt_rollback_req_timeout_ = (1 + retry_count) * stmt_rollback_req_timeout_;
      const int64_t trans_rpc_timeout = GCONF._ob_trans_rpc_timeout;
      const int64_t rpc_timeout_us = std::min(trans_rpc_timeout, stmt_rollback_req_timeout_) + 200 * 1000;
      int64_t max_left_waittime = expired_time - ObTimeUtility::current_time();
      int64_t waittime = ((max_left_waittime > rpc_timeout_us) ? rpc_timeout_us : max_left_waittime);
      if (waittime <= 0) {
        // generate request id, so rolback stmt will not retry
        CtxLockGuard guard(lock_);
        generate_request_id_();
        ret = OB_TRANS_STMT_TIMEOUT;
        need_retry_rpc = false;
      } else if (OB_FAIL(end_stmt_cond_.wait(waittime, result)) || OB_FAIL(result)) {
        if (ObTimeUtility::current_time() < expired_time && OB_TIMEOUT == ret) {
          CtxLockGuard guard(lock_);
          need_retry_rpc = true;
          retry_count++;
          // rewrite ret
          ret = OB_SUCCESS;
          ObPartitionArray noresponse_participants;
          if (OB_UNLIKELY(!msg_mask_set_.is_inited())) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "msg mask set not init", K(ret), K(*this));
          } else if (OB_FAIL(msg_mask_set_.get_not_mask(noresponse_participants))) {
            TRANS_LOG(WARN, "get not mask error", K(ret), K(*this));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < noresponse_participants.count(); i++) {
              if (OB_FAIL(add_retry_stmt_rollback_task_(noresponse_participants.at(i),
                      OB_TRANS_STMT_ROLLBACK_REQUEST,
                      request_id_,
                      ObTimeUtility::current_time() + stmt_rollback_req_timeout_))) {
                TRANS_LOG(WARN,
                    "add retry stmt rollback task error",
                    K(ret),
                    K(waittime),
                    K(retry_count),
                    K(noresponse_participants.at(i)),
                    K(*this));
              } else {
                TRANS_LOG(INFO,
                    "add retry stmt rollback task succ",
                    K(ret),
                    K(waittime),
                    K(noresponse_participants.at(i)),
                    K(*this),
                    K(retry_count));
              }
            }
          }
        } else {
          CtxLockGuard guard(lock_);
          generate_request_id_();
          need_retry_rpc = false;
          ObPartitionArray noresponse_participants;
          ObPartitionLeaderArray noresponse_participants_pla;
          if (trans_param_.is_bounded_staleness_read()) {
            if (msg_mask_set_.is_inited() &&
                OB_SUCCESS != (tmp_ret = msg_mask_set_.get_not_mask(noresponse_participants_pla))) {
              TRANS_LOG(WARN, "get no response participants error", "ret", tmp_ret);
            }
          } else {
            if (msg_mask_set_.is_inited() &&
                OB_SUCCESS != (tmp_ret = msg_mask_set_.get_not_mask(noresponse_participants))) {
              TRANS_LOG(WARN, "get no response participants error", "ret", tmp_ret);
            }
          }
          TRANS_LOG(WARN,
              "wait end stmt error",
              K(ret),
              "context",
              *this,
              K(waittime),
              K(result),
              K(retry_count),
              K(noresponse_participants),
              K(noresponse_participants_pla),
              K(need_retry_rpc));
        }
      } else {
        CtxLockGuard guard(lock_);
        generate_request_id_();
        need_retry_rpc = false;
      }
    }  // while
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  if (OB_NOT_NULL(trans_audit_record_)) {
    const int64_t trans_execute_sql_us = ObTimeUtility::current_time();
    const int64_t lock_for_read_retry_count = -1;  // UNUSED
    (void)trans_audit_record_->set_end_stmt_info(sql_no_, trans_execute_sql_us, lock_for_read_retry_count);
  }

  return ret;
}

int ObScheTransCtx::handle_trans_msg_callback(const ObPartitionKey& partition, const int64_t msg_type, const int status,
    const ObAddr& sender_addr, const int64_t sql_no, const int64_t request_id)
{
  int ret = OB_SUCCESS;
  UNUSED(request_id);

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_TRANS_START_STMT_REQUEST == msg_type) {
    if (sql_no != sql_no_) {
      TRANS_LOG(WARN,
          "current msg not the latest round, discard it",
          K(partition),
          K(sender_addr),
          K(status),
          K(sql_no),
          "context",
          *this);
    }
    if (trans_param_.is_bounded_staleness_read()) {
      if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(partition))) {
        TRANS_LOG(WARN, "unreachable partition push error", KR(ret), K(msg_type), K(partition), K(status));
      } else {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(INFO,
              "unreachable partition push success",
              K(msg_type),
              K(partition),
              K(status),
              K_(cur_stmt_unreachable_partitions),
              "context",
              *this);
        }
      }
      start_stmt_cond_.notify(status);
    } else if (OB_TENANT_NOT_IN_SERVER == status) {
      start_stmt_cond_.notify(status);
    } else {
      // do nothing
    }
  } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
    // stmt executed on A then rollback on B, can not presume rollback success:
    // 1. if server addr has cached in scheduler context, the rollback is succeed
    // 2. otherwise, transaction must be rollbacked
    if (trans_param_.is_bounded_staleness_read()) {
      if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(partition))) {
        TRANS_LOG(WARN, "unreachable partition push error", KR(ret), K(msg_type), K(partition), K(status));
      } else {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(INFO,
              "unreachable partition push success",
              K(msg_type),
              K(partition),
              K(status),
              K_(cur_stmt_unreachable_partitions),
              "context",
              *this);
        }
      }
    }
    if (sql_no != sql_no_) {
      TRANS_LOG(WARN,
          "current msg not the latest round, discard it",
          K(partition),
          K(sender_addr),
          K(status),
          K(sql_no),
          "context",
          *this);
    } else if (OB_TENANT_NOT_IN_SERVER == status) {
      ObAddr addr;
      // verify the partition in scheduler's trans_location cache
      (void)get_trans_location_(partition, addr);
      if (addr == sender_addr) {
        (void)msg_mask_set_.mask(partition);
        // case 1
        if (msg_mask_set_.is_all_mask()) {
          TRANS_LOG(INFO, "scheduler receive all stmt rollback response", "context", *this);
          end_stmt_cond_.notify(OB_SUCCESS);
        }
      } else {
        // case 2
        start_stmt_cond_.notify(status);
      }
    } else if (OB_TRANS_RPC_TIMEOUT == status) {
      // do nothing
    } else {
      end_stmt_cond_.notify(status);
    }
  } else if (OB_TRANS_CLEAR_REQUEST == msg_type) {
    if (OB_TRANS_RPC_TIMEOUT == status) {
      // if transaction is readonly and participants no reponse, its ok
      // to presume transaction terminated
      if (trans_param_.is_bounded_staleness_read() || is_readonly_) {
        end_trans_callback_(is_rollback_, OB_TRANS_ROLLBACKED);
        (void)unregister_timeout_task_();
        set_exiting_();
      }
    }
  } else {
    TRANS_LOG(
        WARN, "handle transaction message callback error", K_(self), K_(trans_id), K(msg_type), K(status), K(sql_no));
  }

  return ret;
}

int ObScheTransCtx::get_cur_stmt_unreachable_partitions(ObPartitionArray& partitions) const
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (cur_stmt_unreachable_partitions_.count() > 0 &&
             OB_FAIL(partitions.assign(cur_stmt_unreachable_partitions_))) {
    TRANS_LOG(WARN,
        "current stmt unreachable partitions assign error",
        KR(ret),
        K_(cur_stmt_unreachable_partitions),
        "context",
        *this);
  } else {
    // do nothing
  }

  return ret;
}

int ObScheTransCtx::merge_participants(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    ret = merge_participants_(participants);
  }

  return ret;
}

int ObScheTransCtx::merge_gc_participants(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx is not initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    ret = merge_gc_participants_(participants);
  }

  return ret;
}

int ObScheTransCtx::merge_participants_(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); i++) {
    const ObPartitionKey& pkey = participants.at(i);
    if (!is_contain(participants_, pkey)) {
      ret = participants_.push_back(pkey);
    }
  }
  TRANS_LOG(DEBUG, "merge participants", KR(ret), K_(participants), K_(trans_id));

  return ret;
}

int ObScheTransCtx::merge_gc_participants_(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); i++) {
    const ObPartitionKey& pkey = participants.at(i);
    if (!is_contain(gc_participants_, pkey)) {
      ret = gc_participants_.push_back(pkey);
    }
  }

  return ret;
}

void ObScheTransCtx::clear_stmt_participants()
{
  CtxLockGuard guard(lock_);
  last_stmt_participants_.reset();
  cur_stmt_unreachable_partitions_.reset();
}

int ObScheTransCtx::merge_participants_pla(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
      bool exist = false;
      for (int64_t j = 0; !exist && j < participants_pla_.count(); ++j) {
        if (pla.get_partitions().at(i) == participants_pla_.get_partitions().at(j) &&
            pla.get_leaders().at(i) == participants_pla_.get_leaders().at(j)) {
          exist = true;
        }
      }
      if (!exist && OB_FAIL(participants_pla_.push(pla.get_partitions().at(i), pla.get_leaders().at(i)))) {
        TRANS_LOG(
            WARN, "push back error", KR(ret), "partition", pla.get_partitions().at(i), "addr", pla.get_leaders().at(i));
      }
    }
  }

  return ret;
}

int ObScheTransCtx::merge_participants_pla()
{
  CtxLockGuard guard(lock_);
  return merge_participants_pla_();
}

int ObScheTransCtx::merge_participants_pla_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < last_stmt_participants_pla_.count(); ++i) {
      bool exist = false;
      for (int64_t j = 0; !exist && j < participants_pla_.count(); ++j) {
        if (last_stmt_participants_pla_.get_partitions().at(i) == participants_pla_.get_partitions().at(j) &&
            last_stmt_participants_pla_.get_leaders().at(i) == participants_pla_.get_leaders().at(j)) {
          exist = true;
        }
      }
      if (!exist && OB_FAIL(participants_pla_.push(last_stmt_participants_pla_.get_partitions().at(i),
                        last_stmt_participants_pla_.get_leaders().at(i)))) {
        TRANS_LOG(WARN, "push back error", KR(ret), "partition", last_stmt_participants_pla_.get_partitions().at(i));
      }
    }
  }

  return ret;
}

void ObScheTransCtx::clear_stmt_participants_pla()
{
  CtxLockGuard guard(lock_);
  last_stmt_participants_pla_.reset();
  cur_stmt_unreachable_partitions_.reset();
}

int ObScheTransCtx::merge_partition_leader_info(const ObPartitionLeaderArray& arr)
{
  CtxLockGuard guard(lock_);
  return merge_partition_leader_info_(arr);
}

int ObScheTransCtx::merge_partition_leader_info_(const ObPartitionLeaderArray& arr)
{
  int ret = OB_SUCCESS;
  const ObPartitionArray& partitions = arr.get_partitions();
  const ObAddrArray& addrs = arr.get_leaders();

  int64_t j = 0;
  for (int64_t i = 0; i < arr.count() && OB_SUCCESS == ret; ++i) {
    if (arr.get_types().at(i) == ObPartitionType::NORMAL_PARTITION ||
        arr.get_types().at(i) == ObPartitionType::DUPLICATE_LEADER_PARTITION) {
      for (j = 0; j < trans_location_cache_.count() && OB_SUCCESS == ret; ++j) {
        if (partitions[i] == (trans_location_cache_.at(j)).get_partition()) {
          // update the old leader info
          if (OB_FAIL((trans_location_cache_.at(j)).set_addr(addrs[i]))) {
            TRANS_LOG(WARN, "set partition leader error", KR(ret), "context", *this, "addr", addrs[i]);
          }
          break;
        }
      }
      if (OB_SUCC(ret) && j == trans_location_cache_.count()) {
        ObPartitionLeaderInfo partition_leader_info(partitions[i], addrs[i]);
        if (OB_FAIL(trans_location_cache_.push_back(partition_leader_info))) {
          TRANS_LOG(WARN, "partition leader info push error", KR(ret), "context", *this);
        }
      }
    }
  }  // for

  return ret;
}

// save participants with its location
int ObScheTransCtx::merge_stmt_participants_pla_(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  const ObPartitionArray& partitions = pla.get_partitions();
  const ObAddrArray& addrs = pla.get_leaders();

  last_stmt_participants_pla_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
    if (OB_FAIL(last_stmt_participants_pla_.push(partitions.at(i), addrs.at(i)))) {
      TRANS_LOG(WARN, "last stmt participnats location info push error", KR(ret), K(pla), "context", *this);
    }
  }

  return ret;
}

int ObScheTransCtx::submit_stmt_rollback_request(const ObPartitionKey& receiver, const int64_t msg_type,
    const int64_t request_id, const int64_t sql_no, const int64_t msg_timeout)
{
  int ret = OB_SUCCESS;
  const common::ObVersion tmp_version;
  CtxLockGuard guard(lock_);

  need_refresh_location_ = true;
  if ((request_id_ == request_id) && (sql_no_ == sql_no) &&
      OB_FAIL(submit_request_(receiver, msg_type, false, tmp_version, msg_timeout))) {
    TRANS_LOG(WARN, "submit request fail", K(ret), K(receiver), "context", *this);
  } else if (request_id_ != request_id || sql_no_ != sql_no) {
    TRANS_LOG(INFO, "wait end stmt timeout, quit retry", K_(trans_id));
  } else {
    // do nothing
  }
  need_refresh_location_ = false;

  return ret;
}

int ObScheTransCtx::submit_request_(const ObPartitionKey& receiver, const int64_t msg_type, const bool need_create_ctx,
    const bool is_not_create_ctx_participant, const int64_t msg_timeout)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;
  const bool nonblock = true;
  bool partition_exist = false;

  if (OB_TRANS_START_STMT_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            scheduler_,
            trans_param_,
            addr_,
            sql_no_,
            cluster_id_,
            request_id_,
            stmt_expired_time_,
            need_create_ctx,
            cluster_version_,
            ObTransSnapshotGeneType::CONSULT,
            session_id_,
            proxy_session_id_,
            app_trace_id_str_,
            trans_location_cache_,
            can_elr_,
            is_not_create_ctx_participant))) {
      TRANS_LOG(WARN, "transaction start stmt request init error", KR(ret));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            scheduler_,
            trans_param_,
            addr_,
            sql_no_,
            savepoint_sql_no_,
            cluster_id_,
            request_id_,
            stmt_expired_time_,
            need_create_ctx,
            cluster_version_,
            ObTransSnapshotGeneType::CONSULT,
            session_id_,
            proxy_session_id_,
            app_trace_id_str_,
            trans_location_cache_,
            can_elr_,
            false,
            stmt_rollback_participants_))) {
      TRANS_LOG(WARN, "transaction stmt rollback request init error", KR(ret));
    } else if (OB_FAIL(msg.set_msg_timeout(msg_timeout))) {
      TRANS_LOG(WARN, "set message start timestamp error", K(ret), K(*this));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_COMMIT_REQUEST == msg_type || OB_TRANS_ABORT_REQUEST == msg_type ||
             OB_TRANS_XA_COMMIT_REQUEST == msg_type || OB_TRANS_XA_ROLLBACK_REQUEST == msg_type ||
             OB_TRANS_XA_PREPARE_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            trans_location_cache_,
            commit_times_,
            stc_,
            is_dup_table_trans_,
            stmt_rollback_info_,
            trace_info_.get_app_trace_info(),
            xid_))) {
      TRANS_LOG(WARN, "transaction commit or abort message init error", K(ret));
    }
  } else if (OB_TRANS_CLEAR_REQUEST == msg_type) {
    int status = OB_TRANS_ROLLBACKED;
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            scheduler_,
            participants_,
            trans_param_,
            addr_,
            request_id_,
            status))) {
      TRANS_LOG(WARN, "transaction clear request message init error", KR(ret));
    }
  } else if (OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            trans_param_,
            addr_,
            savepoint_sql_no_,
            sql_no_,
            request_id_,
            trans_location_cache_,
            cluster_version_))) {
      TRANS_LOG(WARN, "transaction savepoint rollback request init error", KR(ret), K(msg));
    }
  } else if (OB_TRANS_DISCARD_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            SCHE_PARTITION_ID,
            receiver,
            scheduler_,
            gc_participants_,
            trans_param_,
            addr_,
            ObClockGenerator::getClock(),
            OB_TRANS_ROLLBACKED))) {
      TRANS_LOG(WARN, "transaction discard request message init error", "ret", ret);
    }
  } else {
    TRANS_LOG(ERROR, "invalid message type", K(msg_type), "context", *this);
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  }

#ifdef ERRSIM
  if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
    const int64_t random = ObRandom::rand(1, 100);
    if (random < 80) {
      //[random fail]
      ret = E(EventTable::EN_TRANS_START_TASK_ERROR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "[random fail] location not exit", KR(ret), K_(trans_id));
      }
    }
  }
#endif

  if (OB_SUCC(ret)) {
    ret = post_trans_msg_(receiver, msg, msg_type, nonblock, partition_exist);
    if (!partition_exist) {
      TRANS_LOG(WARN, "partition not exist", K(ret), K(receiver), K(*this));
      if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type) {
        end_stmt_cond_.notify(OB_PARTITION_NOT_EXIST);
      } else if (OB_TRANS_CLEAR_REQUEST == msg_type) {
        msg_mask_set_.mask(receiver);
      }
    }
  }

  return ret;
}

int ObScheTransCtx::submit_bounded_staleness_request_(
    const ObPartitionLeaderArray& pla, const int64_t msg_type, ObTransDesc* trans_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr location;

  generate_request_id_();
  for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
    ObTransMsg msg;
    const ObPartitionKey& receiver = pla.get_partitions().at(i);
    location = pla.get_leaders().at(i);
    if (OB_TRANS_START_STMT_REQUEST == msg_type) {
      const bool need_create_ctx = true;
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              SCHE_PARTITION_ID,
              receiver,
              scheduler_,
              trans_param_,
              addr_,
              sql_no_,
              cluster_id_,
              request_id_,
              stmt_expired_time_,
              need_create_ctx,
              cluster_version_,
              trans_desc->get_snapshot_gene_type(),
              session_id_,
              proxy_session_id_,
              app_trace_id_str_,
              trans_location_cache_,
              false,
              trans_desc->is_not_create_ctx_participant(receiver)))) {
        TRANS_LOG(WARN, "transaction start consistent stmt request init error", KR(ret));
      }
    } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              SCHE_PARTITION_ID,
              receiver,
              trans_param_,
              addr_,
              sql_no_,
              savepoint_sql_no_,
              request_id_,
              trans_location_cache_,
              cluster_version_))) {
        TRANS_LOG(WARN, "transaction bounded staleness stmt rollback request init error", KR(ret));
      }
    } else {
      TRANS_LOG(ERROR, "invalid message type", K(msg_type), "context", *this);
      ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    }
    if (OB_SUCC(ret) && OB_FAIL(post_trans_msg_(receiver.get_tenant_id(), location, msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction msg error", "ret", tmp_ret, "context", *this, K(msg_type));
    }
  }

  return ret;
}

int ObScheTransCtx::submit_stmt_request_(
    const ObPartitionArray& participants, const int64_t msg_type, const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionArray* pptr = const_cast<ObPartitionArray*>(&participants);
  generate_request_id_();
  for (ObPartitionArray::iterator it = pptr->begin(); OB_SUCC(ret) && it != pptr->end(); it++) {
    const bool need_create_ctx = !trans_desc.has_create_ctx(*it);
    const bool is_not_create_ctx_participant = trans_desc.is_not_create_ctx_participant(*it);
    if (OB_SUCCESS != (tmp_ret = submit_request_(*it,
                           msg_type,
                           need_create_ctx,
                           is_not_create_ctx_participant,
                           ObTimeUtility::current_time() + stmt_rollback_req_timeout_))) {
      TRANS_LOG(WARN, "submit request error", "ret", tmp_ret, "context", *this, K(msg_type));
      if (OB_PARTITION_NOT_EXIST != tmp_ret && OB_LOCATION_NOT_EXIST != tmp_ret &&
          OB_LOCATION_LEADER_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        break;
      } else if (OB_LOCATION_NOT_EXIST == tmp_ret || OB_LOCATION_LEADER_NOT_EXIST == tmp_ret) {
        if (OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type || OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
          if (OB_FAIL(add_retry_stmt_rollback_task_(
                  *it, msg_type, request_id_, ObTimeUtility::current_time() + stmt_rollback_req_timeout_))) {
            TRANS_LOG(WARN, "add retry stmt rollback task fail", K(ret), K(msg_type), K(trans_desc));
          } else {
            TRANS_LOG(INFO, "add retry stmt rollback task succ", K(ret), K(msg_type), K_(trans_id));
          }
        } else if (OB_TRANS_START_STMT_REQUEST == msg_type) {
          (void)start_stmt_cond_.notify(OB_TRANS_STMT_NEED_RETRY);
          break;
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
  }  // for

  return ret;
}

int ObScheTransCtx::add_retry_stmt_rollback_task_(
    const ObPartitionKey& partition, const int64_t msg_type, const int64_t request_id, const int64_t task_timeout)
{
  int ret = OB_SUCCESS;

  TransRpcTask* task = NULL;
  if (OB_UNLIKELY(NULL == (task = TransRpcTaskFactory::alloc()))) {
    TRANS_LOG(ERROR, "alloc trans rpc task error", KP(task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (OB_FAIL(task->init(partition,
            trans_id_,
            msg_type,
            request_id,
            ObTransRetryTaskType::RETRY_STMT_ROLLBACK_TASK,
            sql_no_,
            task_timeout))) {
      TRANS_LOG(WARN, "task init error", K(ret), K(partition), K_(trans_id), K(msg_type), K(request_id));
    } else if (OB_FAIL(task->set_retry_interval_us(
                   RETRY_STMT_ROLLBACK_TASK_INTERVAL_US, RETRY_STMT_ROLLBACK_TASK_INTERVAL_US))) {
      TRANS_LOG(WARN, "task set retry interval us error", KR(ret), K(*task));
    } else if (OB_FAIL(trans_service_->push(task))) {
      TRANS_LOG(WARN, "transaction service push task error", KR(ret), "rpc_task", *task);
    } else {
      // do nothing
    }
    if (OB_FAIL(ret)) {
      TransRpcTaskFactory::release(task);
    }
  }

  return ret;
}

bool ObScheTransCtx::can_local_trans_opt_(const int status)
{
  bool bool_ret =
      (OB_SUCCESS == status && trans_location_cache_.count() > 0 && is_single_leader(trans_location_cache_) &&
          trans_location_cache_.at(0).get_addr() == addr_ && 0 == commit_times_ && !GCONF.enable_one_phase_commit &&
          !is_dup_table_trans_ && !is_xa_local_trans());
  if (bool_ret) {
    if (trans_location_cache_.count() < participants_.count()) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

int ObScheTransCtx::handle_local_trans_commit_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool alloc = true;
  ObTransCtx* ctx = NULL;
  if (OB_FAIL(trans_service_->get_coord_trans_ctx_mgr().get_trans_ctx(
          coordinator_, trans_id_, for_replay_, is_readonly_, alloc, ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST != tmp_ret) {
      TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), "context", *this);
    }
  } else {
    ObCoordTransCtx* coord_ctx = static_cast<ObCoordTransCtx*>(ctx);
    if (alloc) {
      if (OB_FAIL(coord_ctx->init(tenant_id_,
              trans_id_,
              trans_expired_time_,
              coordinator_,
              &(trans_service_->get_coord_trans_ctx_mgr()),
              trans_param_,
              cluster_version_,
              trans_service_,
              commit_times_))) {
        TRANS_LOG(WARN, "coordinator transaction context init error", K(ret), K(*this));
        coord_ctx = NULL;
      } else if (OB_FAIL(coord_ctx->construct_local_context(scheduler_, coordinator_, participants_))) {
        TRANS_LOG(WARN, "coordinator construct local transaction context error", K(ret), K(*this));
        coord_ctx = NULL;
      } else if (OB_FAIL(coord_ctx->handle_local_commit(is_dup_table_trans_, commit_times_))) {
        TRANS_LOG(WARN, "coordinator handle local commit error", K(ret), K(*this));
      } else {
        // do nothing
      }
    } else {
      if (OB_FAIL(coord_ctx->handle_local_commit(is_dup_table_trans_, commit_times_))) {
        TRANS_LOG(WARN, "coordinator handle local commit error", K(ret), K(*this));
      }
    }
    trans_service_->get_coord_trans_ctx_mgr().revert_trans_ctx(ctx);
  }
  return ret;
}

int ObScheTransCtx::submit_trans_request_(const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_TRANS_CLEAR_REQUEST == msg_type) {
    commit_times_++;
    generate_request_id_();
    ObPartitionArray participants;
    if (OB_FAIL(msg_mask_set_.get_not_mask(participants))) {
      TRANS_LOG(WARN, "get not mask participants error", KR(ret), "context", *this);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
        if (OB_SUCCESS != (tmp_ret = submit_request_(participants.at(i), msg_type))) {
          TRANS_LOG(WARN, "submit request error", "ret", tmp_ret, "context", *this, K(msg_type));
          if (OB_PARTITION_NOT_EXIST != tmp_ret && OB_LOCATION_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
            break;
          }
        }
      }
    }
  } else if (OB_TRANS_DISCARD_REQUEST == msg_type) {
    const int status = OB_TRANS_ROLLBACKED;
    for (int64_t i = 0; i < gc_participants_.count(); ++i) {
      ObTransMsg msg;
      const ObPartitionKey& receiver = gc_participants_.at(i);
      if (OB_SUCCESS != (tmp_ret = submit_request_(receiver, msg_type))) {
        TRANS_LOG(WARN, "post transaction msg error", "ret", tmp_ret, "context", *this, K(msg_type));
      } else {
        // do nothing
      }
    }
  } else if (OB_TRANS_COMMIT_REQUEST == msg_type && can_local_trans_opt_(OB_SUCCESS)) {
    ++commit_times_;
    if (OB_FAIL(handle_local_trans_commit_())) {
      TRANS_LOG(WARN, "handle local trans commit error", K(ret), K(*this));
    }
    // inner interface call fail, fallback to RPC
    if (OB_FAIL(ret)) {
      if (OB_FAIL(submit_request_(coordinator_, msg_type))) {
        TRANS_LOG(WARN, "submit request error", K(ret), "context", *this, K(msg_type));
        --commit_times_;
      }
    }
  } else {
    ++commit_times_;
    if (OB_FAIL(submit_request_(coordinator_, msg_type))) {
      TRANS_LOG(WARN, "submit request error", KR(ret), "context", *this, K(msg_type));
      --commit_times_;
    }
  }

  return ret;
}

int ObScheTransCtx::handle_stmt_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

  const int64_t msg_type = msg.get_msg_type();
  switch (msg_type) {
    case OB_TRANS_STMT_ROLLBACK_RESPONSE: {
      if (OB_FAIL(handle_stmt_rollback_response_(msg))) {
        TRANS_LOG(WARN, "handle stmt rollback response error", KR(ret), "context", *this, K(msg));
      }
      break;
    }
    case OB_TRANS_START_STMT_RESPONSE: {
      if (OB_FAIL(handle_start_stmt_response_(msg))) {
        TRANS_LOG(WARN, "handle start stmt response error", KR(ret), "context", *this, K(msg));
      }
      break;
    }
    case OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE: {
      if (OB_FAIL(handle_savepoint_rollback_response_(msg))) {
        TRANS_LOG(WARN, "handle savepoint rollback response error", KR(ret), K(*this));
      }
      break;
    }
    default: {
      TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(msg), "context", *this);
      ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    }
  }

  return ret;
}

int ObScheTransCtx::handle_start_stmt_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();

  if (OB_TRANS_START_STMT_RESPONSE != msg_type) {
    TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(msg), "context", *this);
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  } else if (msg.get_sql_no() < sql_no_) {
    // handle rpc start stmt response with old sql number
    TRANS_LOG(WARN, "receive old rpc stmt rollback response msg", K_(sql_no), K(msg));
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else {
    const ObPartitionKey& sender = msg.get_sender();
    const int status = msg.get_status();
    if (OB_SUCCESS != status) {
      // some participant rpc start stmt error
      // notify cond and return *immediately*
      if (!trans_param_.is_bounded_staleness_read()) {
        // do nothing
      } else {
        if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(sender))) {
          TRANS_LOG(WARN, "unreachable partition push error", K(ret), K(msg_type), K(sender), K(*this));
        }
      }
      start_stmt_cond_.notify(status);
    } else {
      bool can_notify_sql = true;
      if (trans_param_.is_bounded_staleness_read()) {
        if (msg_mask_set_.is_mask(sender, msg.get_sender_addr())) {
          // unexpect to receive such a dup msg
          TRANS_LOG(WARN, "receive duplicate message", "context", *this, K(msg));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(msg_mask_set_.mask(sender, msg.get_sender_addr()))) {
          TRANS_LOG(WARN, "mask set do mask error", K(ret), "context", *this, K(msg));
        } else if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type_) {
          // do nothing
        } else if (ObTransSnapshotGeneType::CONSULT == snapshot_gene_type_) {
          if (msg.get_snapshot_version() < 0) {
            // this replica unreadable, wakeup SQL to retry with other replica,
            if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(sender))) {
              TRANS_LOG(WARN, "unreachable partition push error", KR(ret), K(msg_type), K(msg));
            }
            start_stmt_cond_.notify(OB_REPLICA_NOT_READABLE);
            can_notify_sql = false;
          } else if (msg.get_snapshot_version() < cur_stmt_snapshot_version_) {
            cur_stmt_snapshot_version_ = msg.get_snapshot_version();
          } else {
            // do nothing
          }
        } else {
          TRANS_LOG(ERROR, "invalid snapshot generate type, unexpected error", K(msg), "context", *this);
          ret = OB_ERR_UNEXPECTED;
        }
      } else if (trans_param_.is_current_read()) {
        if (msg_mask_set_.is_mask(sender)) {
          // unexpect to receive such a dup msg
          TRANS_LOG(WARN, "receive duplicate message", "context", *this, K(msg));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(msg_mask_set_.mask(sender))) {
          TRANS_LOG(WARN, "mask set do mask error", KR(ret), "context", *this, K(msg));
        } else if (multi_tenant_stmt_) {
          if (msg.get_snapshot_version() < cur_stmt_snapshot_version_) {
            cur_stmt_snapshot_version_ = msg.get_snapshot_version();
          }
        } else {
          if (msg.get_snapshot_version() > cur_stmt_snapshot_version_) {
            cur_stmt_snapshot_version_ = msg.get_snapshot_version();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid transaction param", KR(ret), "context", *this);
      }
      if (OB_SUCC(ret)) {
        if (ObTimeUtility::current_time() - request_id_ > 1000 * 1000) {
          REC_TRANS_TRACE_EXT(tlog_,
              handle_slow_message,
              OB_ID(msg_type),
              msg_type,
              OB_ID(send_timestamp),
              msg.get_timestamp(),
              OB_ID(sender),
              sender,
              OB_ID(uref),
              get_uref());
        }
        if (msg_mask_set_.is_all_mask() && can_notify_sql) {
          start_stmt_cond_.notify(OB_SUCCESS);
          REC_TRANS_TRACE_EXT(tlog_, handle_all_message, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
        }
      }
    }
  }

  return ret;
}

int ObScheTransCtx::handle_savepoint_rollback_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();
  const int status = msg.get_status();
  const ObPartitionKey& sender = msg.get_sender();

  if (OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE != msg_type) {
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    TRANS_LOG(ERROR, "invalid message type", KR(ret), K(msg_type), K(msg), K(*this));
  } else if (OB_SUCCESS != status) {
    end_stmt_cond_.notify(status);
  } else {
    if (msg_mask_set_.is_mask(sender)) {
      // unexpect to receive such a dup msg
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "receive duplicate message", KR(ret), K(*this), K(msg));
    } else if (OB_FAIL(msg_mask_set_.mask(sender))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mask set do mask error", KR(ret), K(*this));
    } else if (msg_mask_set_.is_all_mask()) {
      end_stmt_cond_.notify(OB_SUCCESS);
      REC_TRANS_TRACE_EXT(tlog_, handle_all_message, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObScheTransCtx::get_trans_location_(const ObPartitionKey& partition, ObAddr& server)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < trans_location_cache_.count(); ++index) {
      if (partition == trans_location_cache_[index].get_partition()) {
        server = trans_location_cache_[index].get_addr();
        if (!server.is_valid()) {
          TRANS_LOG(WARN, "invalid server", K(server));
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
    }
  }

  return ret;
}

int ObScheTransCtx::handle_stmt_rollback_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();

  if (OB_TRANS_STMT_ROLLBACK_RESPONSE != msg_type) {
    TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(msg), "context", *this);
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  } else if (msg.get_sql_no() < sql_no_) {
    // handle rpc stmt rollback response with old sql number
    TRANS_LOG(WARN, "receive old rpc stmt rollback response msg", K_(sql_no), K(msg));
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else {
    const ObPartitionKey& sender = msg.get_sender();
    const int status = msg.get_status();
    if (OB_SUCCESS != status) {
      // some participant rpc stmt rollback error
      // notify cond and return *immediately*
      end_stmt_cond_.notify(status);
      // REC_TRANS_TRACE_EXT(tlog_, "receive stmt rollback response", K(sender), K(status), OB_ID(uref), get_uref());
    } else {
      if (trans_param_.is_bounded_staleness_read()) {
        if (msg_mask_set_.is_mask(sender, msg.get_sender_addr())) {
          // unexpect to receive such a dup msg
          TRANS_LOG(WARN, "receive duplicate message", "context", *this, K(msg));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(msg_mask_set_.mask(sender, msg.get_sender_addr()))) {
          TRANS_LOG(WARN, "mask set do mask error", KR(ret));
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        if (msg_mask_set_.is_mask(sender)) {
          // unexpect to receive such a dup msg
          TRANS_LOG(WARN, "receive duplicate message", "context", *this, K(msg));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(msg_mask_set_.mask(sender))) {
          TRANS_LOG(WARN, "mask set do mask error", KR(ret));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        if (ObTimeUtility::current_time() - request_id_ >= 1000 * 1000) {
          REC_TRANS_TRACE_EXT(tlog_,
              handle_slow_message,
              OB_ID(msg_type),
              msg_type,
              OB_ID(send_timestamp),
              msg.get_timestamp(),
              OB_ID(sender),
              sender,
              OB_ID(uref),
              get_uref());
        }
        if (msg_mask_set_.is_all_mask()) {
          end_stmt_cond_.notify(OB_SUCCESS);
          REC_TRANS_TRACE_EXT(tlog_, handle_all_message, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
        }
      }
    }
  }

  return ret;
}

int ObScheTransCtx::handle_err_response(const int64_t msg_type, const ObPartitionKey& partition,
    const ObAddr& sender_addr, const int status, const int64_t sql_no, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(DEBUG, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type) || !partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), K(partition), K(status), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_TRANS_START_STMT_REQUEST == msg_type) {
      if (sql_no != sql_no_) {
        TRANS_LOG(WARN,
            "current error msg not the latest round, discard it",
            K(partition),
            K(sender_addr),
            K(status),
            K(sql_no),
            "context",
            *this);
      } else if (OB_PARTITION_NOT_EXIST == status || OB_NOT_MASTER == status || OB_PARTITION_IS_STOPPED == status ||
                 OB_PARTITION_IS_BLOCKED == status) {
        if (trans_param_.is_bounded_staleness_read()) {
          if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(partition))) {
            TRANS_LOG(WARN,
                "unreachable partition push error",
                KR(ret),
                K(msg_type),
                K(partition),
                K(sender_addr),
                "context",
                *this);
          }
        } else {
          // do nothing
        }
        start_stmt_cond_.notify(status);
      } else {
        // do nothing
      }
    } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type) {
      if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type && sql_no != sql_no_) {
        TRANS_LOG(WARN,
            "current error msg not the latest round, discard it",
            K(partition),
            K(sender_addr),
            K(status),
            K(sql_no),
            "context",
            *this);
      } else if (OB_PARTITION_NOT_EXIST == status || OB_NOT_MASTER == status || OB_PARTITION_IS_STOPPED == status ||
                 OB_PARTITION_IS_BLOCKED == status || OB_TRANS_STMT_NEED_RETRY == status) {
        if (trans_param_.is_bounded_staleness_read()) {
          if (OB_FAIL(cur_stmt_unreachable_partitions_.push_back(partition))) {
            TRANS_LOG(WARN,
                "unreachable partition push error",
                KR(ret),
                K(msg_type),
                K(partition),
                K(sender_addr),
                "context",
                *this);
          }
          if (msg_mask_set_.is_mask(partition)) {
            TRANS_LOG(DEBUG, "scheduler received this message before", K(partition), K(sender_addr), "context", *this);
          } else {
            (void)msg_mask_set_.mask(partition);
            if (msg_mask_set_.is_all_mask()) {
              TRANS_LOG(INFO, "scheduler receive all stmt rollback response", "context", *this);
              end_stmt_cond_.notify(OB_SUCCESS);
            }
          }
        } else {
          if (OB_NOT_MASTER != status && OB_TRANS_STMT_NEED_RETRY != status) {
            end_stmt_cond_.notify(OB_TRANS_CTX_NOT_EXIST);
            // NOT_MASTER error-code retryable, add async task
          } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              TRANS_LOG(INFO, "stmt rollback need retry succ", KR(ret), K(partition), K(msg_type), K_(trans_id));
            }
          } else {
            if (OB_FAIL(add_retry_stmt_rollback_task_(partition, msg_type, request_id, INT64_MAX / 2))) {
              TRANS_LOG(
                  WARN, "add retry stmt rollback task fail", KR(ret), K(partition), K(msg_type), "context", *this);
            } else {
              if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                TRANS_LOG(INFO, "add retry stmt rollback task succ", KR(ret), K(partition), K(msg_type), K_(trans_id));
              }
            }
          }
        }
      } else {
        // do nothing
      }
    } else if (OB_TRANS_COMMIT_REQUEST == msg_type || OB_TRANS_ABORT_REQUEST == msg_type) {
      // don't retry for commit/abort request, it's driven by timeout timer
      if (1 == commit_times_) {
        commit_times_ = 0;
        trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
        if (timeout_task_.is_registered()) {
          (void)unregister_timeout_task_();
          register_timeout_task_(trans_2pc_timeout_);
        }
      }
    } else if (OB_TRANS_CLEAR_REQUEST == msg_type) {
      if (trans_param_.is_bounded_staleness_read() &&
          (OB_PARTITION_NOT_EXIST == status || OB_PARTITION_IS_STOPPED == status ||
              OB_PARTITION_IS_BLOCKED == status)) {
        if (!sender_addr.is_valid()) {
          TRANS_LOG(WARN, "invalid sender addr", K(partition), K(sender_addr), "context", *this);
          ret = OB_INVALID_ARGUMENT;
        } else if (msg_mask_set_.is_mask(partition, sender_addr)) {
          TRANS_LOG(DEBUG, "scheduler received this message before", K(partition), K(sender_addr), "context", *this);
        } else {
          (void)msg_mask_set_.mask(partition, sender_addr);
          if (msg_mask_set_.is_all_mask()) {
            TRANS_LOG(DEBUG, "scheduler receive all commit response", "context", *this);
            (void)unregister_timeout_task_();
            set_exiting_();
          }
        }
      }
    } else {
      TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(partition), K(status), "context", *this);
      ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    }
  }

  return ret;
}

int ObScheTransCtx::handle_trans_response(const int64_t msg_type, const int status, const ObPartitionKey& sender,
    const ObAddr& sender_addr, const int64_t need_wait_interval_us)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_FAIL(handle_trans_response_(msg_type, status, sender, sender_addr, need_wait_interval_us))) {
    TRANS_LOG(WARN, "handle local trans response error", KR(ret), K(msg_type), K(status), K(sender), "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  (void)unregister_timeout_task_();
  set_exiting_();

  return ret;
}

int ObScheTransCtx::handle_trans_response_(const int64_t msg_type, const int status, const ObPartitionKey& sender,
    const ObAddr& sender_addr, const int64_t need_wait_interval_us)
{
  int ret = OB_SUCCESS;

  switch (msg_type) {
    case OB_TRANS_COMMIT_RESPONSE:
      set_trans_need_wait_wrap_(MonotonicTs::current_time(), need_wait_interval_us);
      TRANS_STAT_COMMIT_TRANS_TIME(tenant_id_);
      TRANS_STAT_TOTAL_USED_TIME(tenant_id_, trans_start_time_);
      TRANS_STAT_COMMIT_TRANS_INC(tenant_id_);
      if (is_xa_local_trans() && OB_FAIL(handle_xa_trans_response_(msg_type, status))) {
        TRANS_LOG(WARN, "handle xa commit response error", K(ret));
      } else if (!is_readonly_) {
        end_trans_callback_(is_rollback_, status);
      }
      break;
    case OB_TRANS_ABORT_RESPONSE: {
      need_print_trace_log_ = true;
      TRANS_STAT_ABORT_TRANS_TIME(tenant_id_);
      TRANS_STAT_TOTAL_USED_TIME(tenant_id_, trans_start_time_);
      TRANS_STAT_ABORT_TRANS_INC(tenant_id_);
      // flush redo log fail on xa prepare phase
      if (is_xa_local_trans()) {
        if (!is_readonly_) {
          if (is_xa_one_phase_) {
            xa_end_trans_cond_.notify(OB_TRANS_XA_RBROLLBACK);
          } else {
            end_trans_callback_(is_rollback_, OB_TRANS_XA_RBROLLBACK);
          }
        }
      } else if (!is_readonly_) {
        end_trans_callback_(is_rollback_, status);
      }
      break;
    }
    case OB_TRANS_CLEAR_RESPONSE: {
      if (trans_param_.is_bounded_staleness_read()) {
        if (msg_mask_set_.is_mask(sender, sender_addr)) {
          TRANS_LOG(DEBUG, "scheduler received this message before", "partition", sender);
        } else {
          (void)msg_mask_set_.mask(sender, sender_addr);
        }
      } else {
        if (msg_mask_set_.is_mask(sender)) {
          TRANS_LOG(DEBUG, "scheduler received this message before", "partition", sender);
        } else {
          (void)msg_mask_set_.mask(sender);
        }
      }
      if (OB_SUCC(ret) && msg_mask_set_.is_all_mask()) {
        TRANS_LOG(DEBUG, "scheduler receive all commit response", "context", *this);
        (void)unregister_timeout_task_();
        set_exiting_();
        TRANS_STAT_TOTAL_USED_TIME(tenant_id_, trans_start_time_);
        TRANS_STAT_COMMIT_ABORT_TRANS_TIME(is_rollback_, tenant_id_);
        TRANS_STAT_COMMIT_ABORT_TRANS_INC(is_rollback_, tenant_id_);
      }

      break;
    }
    default: {
      TRANS_LOG(ERROR, "invalid message type", K(msg_type), "context", *this);
      ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    }
  }

  return ret;
}

#define DELETE_XA_TRANS_SQL "delete from %s where xid = '%.*s'"
#define INSERT_XA_TRANS_SQL \
  "\
  insert into %s (xid, trans_id, table_id, partition_id, partition_cnt, is_readonly, state) \
  values ('%.*s', '%s', %ld, %ld, %d, %d, 0)\
"
int ObScheTransCtx::handle_xa_trans_response_(const int64_t msg_type, int status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObTransMsgTypeChecker::is_valid_msg_type(msg_type))) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), K(status));
    ret = OB_INVALID_ARGUMENT;
  } else {
    TRANS_LOG(INFO, "receive xa trans response", K(msg_type), K(status), K_(trans_id), K_(xid));
    switch (msg_type) {
      case OB_TRANS_XA_PREPARE_RESPONSE:
        if (OB_SUCCESS != status) {
          // defensive code
          TRANS_LOG(WARN, "[XA] xa prepare failed", K(ret), K(status), "context", *this);
          is_rollback_ = true;
          is_xa_end_trans_ = true;
          register_timeout_task_(trans_2pc_timeout_);
          status = OB_TRANS_NEED_ROLLBACK;
          end_trans_callback_(is_rollback_, status);
        } else if (ObXATransState::PREPARED == xa_trans_state_) {
          // do nothing
        } else {
          if (is_xa_readonly_) {
            int64_t affected_rows = 0;
            if (OB_FAIL(trans_service_->update_coordinator_and_state(
                    tenant_id_, xid_, coordinator_, ObXATransState::PREPARED, is_xa_readonly_, affected_rows))) {
              TRANS_LOG(WARN, "fail to update inner table for read-only xa trans", K(ret));
            }
          }
          xa_trans_state_ = ObXATransState::PREPARED;
          TRANS_LOG(INFO, "handle XA prepare success", K(status), "context", *this);
          end_trans_callback_(is_rollback_, status);
          (void)unregister_timeout_task_();
          set_exiting_();
        }
        break;
      case OB_TRANS_COMMIT_RESPONSE: {
        // del/update inner table transaction state info
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != status) {
          TRANS_LOG(WARN, "[XA] xa commit failed", K(ret), K(status), K(msg_type), "context", *this);
          ret = OB_ERR_UNEXPECTED;
        } else if (!is_xa_one_phase_) {
          if (OB_SUCCESS != (tmp_ret = trans_service_->delete_xa_branch(tenant_id_, xid_, is_tightly_coupled_))) {
            TRANS_LOG(WARN, "delete XA inner table record failed", K(tmp_ret), K(*this), K(msg_type));
          }
        } else {
          // do nothing
        }
        xa_end_trans_cond_.notify(ret);
        (void)unregister_timeout_task_();
        set_exiting_();
        break;
      }
      case OB_TRANS_XA_ROLLBACK_RESPONSE: {
        if (ObXATransState::PREPARING == xa_trans_state_) {
          // ignore this message and retry
          TRANS_LOG(WARN, "original scheduler should not receive this response", K(msg_type), K(*this));
        } else {
          // del transaction info in inner table
          if (OB_TRANS_ROLLBACKED != status) {
            TRANS_LOG(WARN, "[XA] xa rollback failed", K(ret), K(status), K(msg_type), "context", *this);
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(trans_service_->delete_xa_branch(tenant_id_, xid_, is_tightly_coupled_))) {
            TRANS_LOG(WARN, "delete XA inner table record failed", K(ret), K(*this), K(msg_type));
          }
          xa_end_trans_cond_.notify(ret);
          (void)unregister_timeout_task_();
          set_exiting_();
        }
        break;
      }
      default: {
        TRANS_LOG(ERROR, "invalid message type", K(msg_type), "context", *this);
        ret = OB_TRANS_INVALID_MESSAGE_TYPE;
      }
    }
  }
  return ret;
}

int ObScheTransCtx::end_trans_(const bool is_rollback, bool& need_callback)
{
  int ret = OB_SUCCESS;
  const bool is_bounded_staleness_read = trans_param_.is_bounded_staleness_read();

  if (OB_UNLIKELY(!is_tenant_active_)) {
    TRANS_LOG(WARN, "tenant is inactive in current server", "context", *this);
    ret = OB_TENANT_NOT_IN_SERVER;
  } else if ((!is_bounded_staleness_read && participants_.count() == 0) ||
             (is_bounded_staleness_read && participants_pla_.count() == 0)) {
    is_xa_readonly_ = is_xa_local_trans() ? true : false;
    if (is_xa_local_trans() && !is_rollback && !is_xa_one_phase_) {
      // XA readonly transaction prepare
      if (OB_FAIL(handle_xa_trans_response_(OB_TRANS_XA_PREPARE_RESPONSE, OB_SUCCESS))) {
        TRANS_LOG(WARN, "handle xa readonly prepare failed", "context", *this, K(is_rollback));
      } else {
        (void)unregister_timeout_task_();
        TRANS_LOG(INFO, "handle xa readonly prepare success", "context", *this, K(is_rollback));
      }
    } else {
      // participants set is empty
      TRANS_STAT_TOTAL_USED_TIME(tenant_id_, trans_start_time_);
      TRANS_STAT_COMMIT_ABORT_TRANS_INC(is_rollback, tenant_id_);
      set_exiting_();
      (void)unregister_timeout_task_();
      need_callback = true;
    }
  } else {
    if (is_rollback || is_readonly_) {
      need_callback = true;
    }
    // select coordinator to first participant
    if (!is_bounded_staleness_read) {
      coordinator_ = participants_[0];
      TRANS_LOG(DEBUG, "coordinator", K_(coordinator), "context", *this);
    }
    const int64_t cur_ts = ObClockGenerator::getClock();
    commit_start_time_ = cur_ts;
    if (!is_rollback && !is_readonly_) {
      if (cur_ts - last_commit_timestamp_ < GCONF._trx_commit_retry_interval) {
        TRANS_LOG(INFO, "do not reach commit retry interval", K(ret), "context", *this, K_(last_commit_timestamp));
      } else {
        if (is_xa_local_trans() && !is_xa_one_phase_) {
          if (OB_FAIL(submit_trans_request_(OB_TRANS_XA_PREPARE_REQUEST))) {
            TRANS_LOG(WARN, "submit xa prepare request error", K(ret), "context", *this, K_(participants));
          }
        } else {
          if (OB_FAIL(submit_trans_request_(OB_TRANS_COMMIT_REQUEST))) {
            TRANS_LOG(WARN, "submit commit request error", K(ret), "context", *this, K_(participants));
          }
        }
        if (OB_SUCC(ret)) {
          last_commit_timestamp_ = cur_ts;
        }
        REC_TRANS_TRACE_EXT(tlog_, submit_commit, OB_ID(ret), ret, OB_ID(uref), get_uref());
      }
    } else {
      if (trans_param_.is_bounded_staleness_read() || is_readonly_) {
        (void)unregister_timeout_task_();
        set_exiting_();
      } else {
        if (commit_times_ <= 0) {
          msg_mask_set_.reset();
          if (OB_FAIL(msg_mask_set_.init(participants_))) {
            TRANS_LOG(WARN, "message mask set init error", KR(ret), "context", *this, K_(participants));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(submit_trans_request_(OB_TRANS_CLEAR_REQUEST))) {
          TRANS_LOG(WARN, "submit clear request error", KR(ret), "context", *this, K_(participants));
        }
      }
      REC_TRANS_TRACE_EXT(tlog_, submit_abort, OB_ID(ret), ret, OB_ID(uref), get_uref());
    }
  }  // participants count > 0

  return ret;
}

int ObScheTransCtx::start_new_sql_(const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sql_no < sql_no_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "sql no unexpected", KR(ret), K(sql_no), K(sql_no_), K(*this));
  } else {
    sql_no_ = sql_no;
    if (trans_param_.is_bounded_staleness_read()) {
      cur_stmt_snapshot_version_ = INT64_MAX;
    } else if (trans_param_.is_current_read()) {
      if (multi_tenant_stmt_) {
        cur_stmt_snapshot_version_ = INT64_MAX;
      } else {
        cur_stmt_snapshot_version_ = 0;
      }
    }
  }

  return ret;
}

int ObScheTransCtx::end_trans_(
    const bool is_rollback, const int64_t trans_expired_time, const int64_t stmt_expired_time, bool& need_callback)
{
  int ret = OB_SUCCESS;
  UNUSED(trans_expired_time);
  const int64_t pkey_count =
      trans_param_.is_bounded_staleness_read() ? participants_pla_.count() : participants_.count();

  // stmt_expired_time as a point in the future
  // to notify SQL client transaction timeout
  stmt_expired_time_ = stmt_expired_time;
  is_rollback_ = is_rollback;
  (void)unregister_timeout_task_();
  if (OB_FAIL(drive_(need_callback))) {
    TRANS_LOG(WARN, "drive scheduler error", KR(ret), "context", *this);
  } else if (!is_exiting_) {
    if (is_rollback || is_readonly_) {
      // retry interval < 1s
      trans_2pc_timeout_ =
          min((pkey_count / 100 + 1) * ObServerConfig::get_instance().trx_2pc_retry_interval, MAX_TRANS_2PC_TIMEOUT_US);
    } else if (0 == commit_times_) {
      // has not send commit to coordinator yet
      trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
    } else {
      // let scheduler retry after coordinator has retry
      // trans_2pc_timeout_ = min(trans_2pc_timeout_ + ObServerConfig::get_instance().trx_2pc_retry_interval,
      //   MAX_TRANS_2PC_TIMEOUT_US);
      trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
    }
    if (is_xa_readonly_) {
      // do nothing
    } else if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
      TRANS_LOG(WARN, "register timeout handler error", KR(ret));
    }
  } else {
    // do nothing
  }

  return ret;
}

//              START_TRANS  START_STMT  END_STMT  END_TRANS
// UNKNOWN      START_TRANS  N           N         N
// START_TRANS  N            START_STMT  N         END_TRANS
// START_STMT   N            START_STMT  END_STMT  END_TRANS
// END_STMT     N            START_STMT  N         END_TRANS
// END_TRANS    N            N           N         END_TRANS
int ObScheTransCtx::StateHelper::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::INVALID;
  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {{State::START_TRANS, N, N, N},
      {N, State::START_STMT, N, State::END_TRANS},
      {N, State::START_STMT, State::END_STMT, State::END_TRANS},
      {N, State::START_STMT, N, State::END_TRANS},
      {N, N, N, State::END_TRANS}};

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    TRANS_LOG(WARN, "invalid argument", K(op));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    TRANS_LOG(WARN, "ScheTransCtx current state is invalid", K_(state), K(op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t new_state = STATE_MAP[state_][op];
    if (!State::is_valid(new_state)) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      last_state_ = state_;
      state_ = new_state;
      is_switching_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(DEBUG, "switch state success", K_(last_state), K_(state), K(op));
  } else {
    TRANS_LOG(ERROR, "switch state error", KR(ret), K_(state), K(op));
  }

  return ret;
}

void ObScheTransCtx::StateHelper::restore_state()
{
  if (is_switching_) {
    is_switching_ = false;
    state_ = last_state_;
  }
}

int ObScheTransCtx::set_sql_no(const int64_t sql_no)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(sql_no < 0)) {
    TRANS_LOG(WARN, "invaild argument", "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_no_ = sql_no;
  }

  return ret;
}

int ObScheTransCtx::xa_end_trans(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObScheTransCtx not inited", K(ret));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "transaction is exiting", K(ret), "context", *this);
  } else {
    (void)unregister_timeout_task_();
    is_rollback_ = is_rollback;
    is_xa_end_trans_ = true;
    if (OB_FAIL(drive_())) {
      TRANS_LOG(WARN, "xa end trans error", K(ret), "context", *this);
    } else if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
      TRANS_LOG(WARN, "register timeout handler error", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  TRANS_LOG(INFO, "xa end trans", K(ret), K(is_rollback), K_(is_xa_readonly));
  REC_TRANS_TRACE_EXT(tlog_, xa_end_trans, OB_ID(ret), ret, OB_ID(is_rollback), is_rollback, OB_ID(uref), get_uref());
  return ret;
}

int ObScheTransCtx::wait_xa_end_trans()
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    const int64_t waittime = (INT64_MAX / 2) - ObTimeUtility::fast_current_time();
    if (OB_FAIL(xa_end_trans_cond_.wait(waittime, result)) || OB_FAIL(result)) {
      TRANS_LOG(WARN, "wait xa end trans error", K(ret), "context", *this, K(waittime), K(result));
    }
  }
  return ret;
}

int ObScheTransCtx::set_xa_trans_state(const int32_t state, const bool need_check /*true*/)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (!ObXATransState::is_valid(state)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid xa state", K(ret), K(state));
  } else if (need_check && !ObXATransState::can_convert(xa_trans_state_, state)) {
    ret = OB_TRANS_XA_PROTO;
    // LOG_USER_ERROR(OB_TRANS_XA_RMFAIL, ObXATransState::to_string(xa_trans_state_));
    TRANS_LOG(WARN, "invalid state", K_(xid), K_(xa_trans_state), K(state));
  } else {
    xa_trans_state_ = state;
  }
  return ret;
}

int ObScheTransCtx::get_xa_trans_state(int32_t& state) const
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    state = xa_trans_state_;
  }
  return ret;
}

int ObScheTransCtx::xa_end_trans_(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  if (commit_times_ > 0) {
    need_refresh_location_ = true;
  }
  if (is_xa_readonly_) {
    const int64_t msg_type = is_rollback ? OB_TRANS_XA_ROLLBACK_RESPONSE : OB_TRANS_COMMIT_RESPONSE;
    if (OB_FAIL(handle_xa_trans_response_(msg_type, OB_SUCCESS))) {
      TRANS_LOG(WARN, "readonly xa end trans error", K(ret), K(msg_type), "context", *this);
    }
  } else if (is_rollback) {
    if (OB_FAIL(submit_trans_request_(OB_TRANS_XA_ROLLBACK_REQUEST))) {
      TRANS_LOG(WARN, "submit xa rollback request error", K(ret), "context", *this);
    } else {
      TRANS_LOG(INFO, "submit xa rollback request success", "context", *this);
    }
  } else {
    if (OB_FAIL(submit_trans_request_(OB_TRANS_XA_COMMIT_REQUEST))) {
      TRANS_LOG(WARN, "submit xa commit request error", K(ret), "context", *this);
    } else {
      TRANS_LOG(INFO, "submit xa commit request success", "context", *this);
    }
  }
  need_refresh_location_ = false;
  return ret;
}

int ObScheTransCtx::set_trans_app_trace_id_str_(const ObString& app_trace_id_str)
{
  int ret = OB_SUCCESS;
  const int64_t app_trace_id_str_len = app_trace_id_str.length();
  if (OB_UNLIKELY(app_trace_id_str_len < 0) || OB_UNLIKELY(app_trace_id_str_len > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
    TRANS_LOG(WARN, "invalid argument", K(app_trace_id_str), "context", *this);
    ret = OB_INVALID_ARGUMENT;
    // when sp_trans convert to dist_trans, set app_trace_id_str first, and set sql_no second
  } else if (0 == app_trace_id_str_.length()) {
    (void)app_trace_id_str_.write(app_trace_id_str.ptr(), app_trace_id_str.length());
  } else if (app_trace_id_str_.length() != app_trace_id_str_len) {
    TRANS_LOG(WARN, "different trace id string", K(app_trace_id_str), "context", *this, K(lbt()));
  } else {
    // do nothing
  }
  return ret;
}

int ObScheTransCtx::xa_one_phase_end_trans_(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  bool need_callback = false;
  (void)unregister_timeout_task_();
  is_rollback_ = is_rollback;
  is_xa_end_trans_ = true;
  is_xa_one_phase_ = true;
  if (commit_times_ > 0) {
    need_refresh_location_ = true;
  }
  if (OB_FAIL(drive_(need_callback))) {
    TRANS_LOG(WARN, "drive scheduler error", K(ret), "context", *this);
  } else if (!is_exiting_) {
    // xa readonly transaction has call set_exiting, timeout task must not been registered
    if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
      TRANS_LOG(WARN, "register timeout handler error", K(ret));
    }
  } else {
    TRANS_LOG(INFO,
        "one phase xa transaction is ready to exit",
        K_(xid),
        K_(is_rollback),
        K_(is_xa_readonly),
        K(need_callback));
  }
  if (need_callback) {
    int tmp_ret = OB_SUCCESS;
    if (!is_tightly_coupled_) {
      // loosely
      if (OB_SUCCESS != (tmp_ret = trans_service_->delete_xa_branch(tenant_id_, xid_, is_tightly_coupled_))) {
        TRANS_LOG(WARN, "delete XA inner table record failed", K(tmp_ret), K(*this));
      }
    } else {
      // tightly
      if (OB_SUCCESS != (tmp_ret = trans_service_->delete_xa_all_tightly_branch(tenant_id_, xid_))) {
        TRANS_LOG(WARN, "delete all tightly branch from inner table failed", K(tmp_ret), K(*this));
      }
    }
    xa_end_trans_cond_.notify(ret);
  }
  need_refresh_location_ = false;
  return ret;
}

int ObScheTransCtx::xa_rollback_session_terminate()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObScheTransCtx not inited", K(ret));
  } else if (is_terminated_) {
    TRANS_LOG(INFO, "transaction is terminating", K(ret), "context", *this);
  } else if (ObXATransState::ACTIVE != xa_trans_state_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected state", K(ret), K(xa_trans_state_), K(*this));
  } else if (has_decided_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected, xa trans already decided", K(ret), K(*this));
  } else if (OB_FAIL(xa_rollback_session_terminate_())) {
    TRANS_LOG(WARN, "terminate xa trans failed", K(ret), K(*this));
  }
  TRANS_LOG(INFO, "rollback xa trans when session terminate", K(ret), K(*this));
  return ret;
}

int ObScheTransCtx::xa_rollback_session_terminate_()
{
  int ret = OB_SUCCESS;
  const bool is_rollback = true;
  if (!is_tightly_coupled_) {
    // loosely
    if (OB_FAIL(xa_one_phase_end_trans_(is_rollback))) {
      TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(*this));
    }
  } else {
    // tightly
    if (OB_FAIL(xa_one_phase_end_trans_(is_rollback))) {
      TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(*this));
    }
    // is_terminated_ = true;
    set_terminated_();
  }
  return ret;
}

int ObScheTransCtx::xa_one_phase_end_trans(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  TRANS_LOG(INFO, "xa one phase commit", K(is_rollback));
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (is_tightly_coupled_ && !is_rollback && 1 < get_unprepared_branch_count_()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(
        WARN, "not support one phase commit due to multiple unprepared branches", K(ret), K(is_rollback), K(*this));
  } else if (has_decided_()) {
    if (!is_xa_one_phase_) {
      // already in two-phase-commit
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "xa trans has entered into two phase", K(is_rollback), "context", *this);
    } else if ((is_rollback_ && !is_rollback) || (!is_rollback_ && is_rollback)) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "invalid xa trans one phase request", K(is_rollback), "context", *this);
    } else {
      // one-phase proxy end_trans request timeout
      // if scheduler in end_trans phase, need to retry and wait ctx released
      ret = OB_TRANS_XA_RETRY;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(xa_one_phase_end_trans_(is_rollback))) {
      TRANS_LOG(WARN, "xa one phase end trans failed", K(ret), K(is_rollback), K(*this));
    }
  }
  if (is_rollback && is_tightly_coupled_) {
    // set this for one-phase rollback in tightly coupe mode
    // is_terminated_ = true;
    set_terminated_();
  }
  return ret;
}

int ObScheTransCtx::pre_xa_prepare(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (xid.empty() || xid_.get_gtrid_str() != xid.get_gtrid_str()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(xid_));
  } else if (NULL == trans_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret));
  } else if (is_exiting_) {
    if (is_tightly_coupled_) {
      // tightly
      ret = OB_TRANS_XA_BRANCH_FAIL;
      TRANS_LOG(WARN, "xa trans is terminating", K(ret), K(*this));
    } else {
      // loosely
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
    }
  } else if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(WARN, "xa trans is terminating", K(ret), K(*this));
  } else if (0 > xa_branch_count_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xa branch count", K(ret), K(*this));
  } else if (is_xa_one_phase_) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa trans has entered into one phase", K(ret), K(*this));
  } else if (has_decided_() && xid_.all_equal_to(xid)) {
    // remove relative logic in end_trans
    if (ObXATransState::PREPARED == xa_trans_state_) {
      ret = OB_TRANS_HAS_DECIDED;
      TRANS_LOG(INFO, "xa trans already in prepared state", K(ret), K(xid), K(*this));
    } else {
      ret = OB_TRANS_XA_RETRY;
      TRANS_LOG(INFO, "xa trans preparing", K(ret), K(xid), K(*this));
    }
  } else {
    // for the sake of simplicity, state of last branch executed xa_preapre is preparing
    int64_t target_index = -1;
    int64_t prepared_branch_count = 0;
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo& info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        target_index = i;
        if (ObXATransState::IDLE == info.state_) {
          info.state_ = ObXATransState::PREPARING;
        }
      }
      if (ObXATransState::PREPARED == info.state_) {
        ++prepared_branch_count;
      }
    }
    if (0 > target_index || xa_branch_info_->count() <= target_index) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "branch info not found in array", K(ret), K(xid), K(*this));
    } else {
      ObXABranchInfo& target_info = xa_branch_info_->at(target_index);
      if (ObXATransState::PREPARING != target_info.state_ && ObXATransState::PREPARED != target_info.state_) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "unexpected xa branch state", K(ret), K(xid), K(target_info.state_), K(*this));
      } else if (is_tightly_coupled_) {
        if (prepared_branch_count < 0 || xa_branch_info_->count() < prepared_branch_count) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected prepared branch count", K(ret), K(xid), K(prepared_branch_count), K(*this));
        } else if (xa_branch_info_->count() <= prepared_branch_count + 1) {
          if (ObXATransState::PREPARING == target_info.state_) {
            xa_trans_state_ = ObXATransState::PREPARING;
            if (OB_FAIL(set_xid_(xid))) {
              TRANS_LOG(WARN, "set xid error", K(ret), K(xid), K(xid_));
            }
          } else {
            // return RDONLY for branch in preapred state
            ret = OB_TRANS_XA_RDONLY;
            TRANS_LOG(INFO, "xa branch has been prepared", K(ret), K(xid), K(*this));
          }
        } else {
          if (ObXATransState::PREPARED == target_info.state_) {
            ret = OB_TRANS_XA_RDONLY;
            TRANS_LOG(INFO, "xa branch has been prepared", K(ret), K(xid), K(*this));
          } else {
            int64_t affected_rows = 0;
            if (OB_FAIL(trans_service_->delete_xa_record_state(tenant_id_, xid, ObXATransState::IDLE, affected_rows))) {
              TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
            } else if (1 > affected_rows) {
              ret = OB_TRANS_XA_NOTA;
              TRANS_LOG(WARN, "invalid xid", K(ret), K(xid), K_(trans_id));
            } else {
              target_info.state_ = ObXATransState::PREPARED;
              xa_trans_state_ = ObXATransState::PREPARING;
              ret = OB_TRANS_XA_RDONLY;
              TRANS_LOG(INFO,
                  "xa rdonly for tightly-coupled branch",
                  K(ret),
                  K(xid),
                  K_(xa_branch_count),
                  K(prepared_branch_count));
            }
          }
        }
        TRANS_LOG(INFO,
            "pre xa prepare for tightly coupled branch",
            K(ret),
            K(xid),
            K(prepared_branch_count),
            K_(xa_branch_count),
            K(*this));
      } else {
        // loosely coupled xa trans branch
        TRANS_LOG(INFO, "pre xa prepare for loosely coupled branch", K(ret), K(xid), K_(xa_branch_count), K(*this));
      }
    }
  }
  return ret;
}

int ObScheTransCtx::kill_query_session(const int status)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    start_stmt_cond_.notify(status);
    end_stmt_cond_.notify(status);
    xa_end_trans_cond_.notify(status);
  }

  return ret;
}

int ObScheTransCtx::set_trans_app_trace_id_str(const ObString& app_trace_id_str)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    ret = set_trans_app_trace_id_str_(app_trace_id_str);
  }
  return ret;
}

int ObScheTransCtx::wait_xa_sync_status(const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  if (0 > expired_time) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(expired_time));
  } else {
    // xa_sync_status_cond_.reset();
    // TRANS_LOG(INFO, "enter wait_xa_sync_status", K(expired_time), K(this));
    if (OB_FAIL(xa_sync_status_cond_.wait(expired_time, result)) || OB_FAIL(result)) {
      TRANS_LOG(WARN, "wait xa sync status failed", K(ret), "context", *this, K(expired_time), K(result));
    }
  }
  TRANS_LOG(INFO, "wait_xa_sync_status completed", K(ret), K(this));
  return ret;
}

int ObScheTransCtx::xa_sync_status(const ObAddr& sender, const bool is_stmt_pull)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (NULL == trans_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret));
  } else if (has_decided_()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa trans has entered into commit phase", K(ret), "context", *this);
  } else {
    ObXASyncStatusRPCResponse response;
    if (OB_FAIL(response.init(*trans_desc_, is_stmt_pull))) {
      TRANS_LOG(WARN, "init sync status response failed", K(ret));
    } else if (OB_FAIL(xa_rpc_->xa_sync_status_response(tenant_id_, sender, response, NULL))) {
      TRANS_LOG(WARN, "xa sync status response failed", K(ret));
    } else {
      xa_trans_state_ = ObXATransState::ACTIVE;
    }
  }
  TRANS_LOG(INFO, "xa sync status", K(ret), K(trans_id_), K(sender), K_(xa_trans_state), K(*trans_desc_), K(this));
  return ret;
}

int ObScheTransCtx::xa_sync_status_response(const ObTransDesc& trans_desc, const bool is_stmt_pull)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (NULL == tmp_trans_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret));
  } else if (is_stmt_pull) {
    (void)tmp_trans_desc_->set_max_sql_no(trans_desc.get_sql_no());
    tmp_trans_desc_->set_stmt_min_sql_no(trans_desc.get_stmt_min_sql_no());
    sql_no_ = trans_desc.get_sql_no();
    tmp_trans_desc_->get_trans_param() = trans_desc.get_trans_param();
    if (OB_FAIL(tmp_trans_desc_->merge_participants(trans_desc.get_participants()))) {
      TRANS_LOG(WARN, "merge participants failed", K(ret));
    } else if (OB_FAIL(tmp_trans_desc_->merge_participants_pla(trans_desc.get_participants_pla()))) {
      TRANS_LOG(WARN, "merge participants pla failed", K(ret));
    } else if (OB_FAIL(
                   tmp_trans_desc_->set_trans_consistency_type(trans_desc.get_trans_param().get_consistency_type()))) {
      TRANS_LOG(WARN, "set consistency type failed", K(ret), K(trans_desc));
    }
    // cluster_id_ = trans_desc.get_cluster_id();
    // snapshot_gene_type_ = trans_desc.get_snapshot_gene_type();
  } else {
    uint32_t session_id = tmp_trans_desc_->get_session_id();
    uint64_t proxy_session_id = tmp_trans_desc_->get_proxy_session_id();
    if (OB_FAIL(tmp_trans_desc_->trans_deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "deep copy trans desc failed", K(ret));
    } else if (OB_FAIL(
                   tmp_trans_desc_->set_trans_consistency_type(trans_desc.get_trans_param().get_consistency_type()))) {
      TRANS_LOG(WARN, "set consistency type failed", K(ret), K(trans_desc));
    } else {
      if (OB_FAIL(participants_pla_.assign(trans_desc.get_participants_pla()))) {
        TRANS_LOG(WARN, "assign participants pla failed", K(ret));
      } else if (OB_FAIL(stmt_rollback_info_.assign(trans_desc.get_stmt_rollback_info()))) {
        TRANS_LOG(WARN, "assign stmt rollback info failed", K(ret));
      } else if (OB_FAIL(set_trans_app_trace_id_str_(trans_desc.get_trans_app_trace_id_str()))) {
        TRANS_LOG(WARN, "set trans app trace id failed", K(ret));
      } else if (OB_FAIL(tmp_trans_desc_->set_session_id(session_id))) {
        TRANS_LOG(WARN, "set session id error", K(ret), K(session_id));
      } else if (OB_FAIL(tmp_trans_desc_->set_proxy_session_id(proxy_session_id))) {
        TRANS_LOG(WARN, "set proxy session id error", K(ret), K(proxy_session_id));
      } else {
        sql_no_ = trans_desc.get_sql_no();
        cluster_id_ = trans_desc.get_cluster_id();
        snapshot_gene_type_ = trans_desc.get_snapshot_gene_type();
      }
    }
  }
  TRANS_LOG(INFO,
      "xa sync status response",
      K(ret),
      K(trans_id_),
      K(xid_),
      K(trans_desc),
      K(is_stmt_pull),
      KP(tmp_trans_desc_),
      K(this));
  // usleep(1000);
  xa_sync_status_cond_.notify(ret);
  // TRANS_LOG(INFO, "xa sync status response 2", K(ret), K(trans_id_), K(xid_), K(trans_desc), K(is_stmt_pull),
  // KP(tmp_trans_desc_), K(this));
  return ret;
}

int ObScheTransCtx::xa_merge_status(const ObTransDesc& trans_desc, const bool is_stmt_push)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!trans_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc));
  } else {
    ret = xa_merge_status_(trans_desc, is_stmt_push);
  }
  return ret;
}

int ObScheTransCtx::xa_merge_status_(const ObTransDesc& trans_desc, const bool is_stmt_push)
{
  int ret = OB_SUCCESS;

  if (NULL == trans_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret));
  } else if (has_decided_()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa trans has entered into commit phase", K(ret), "context", *this);
  } else if (is_tightly_coupled_ && !is_stmt_push) {
    // xa_trans_state_ = ObXATransState::IDLE;
  } else {
    ObXATransID origin_xid = trans_desc_->get_xid();
    uint32_t session_id = trans_desc_->get_session_id();
    uint64_t proxy_session_id = trans_desc_->get_proxy_session_id();
    if (OB_FAIL(trans_desc_->trans_deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "deep copy trans desc failed", K(ret));
    } else if (OB_FAIL(merge_participants_(trans_desc_->get_participants()))) {
      TRANS_LOG(WARN, "merge participants failed", K(ret), K_(participants), K(trans_desc_->get_participants()));
    } else {
      trans_desc_->set_xid(origin_xid);
      if (OB_FAIL(trans_desc_->set_session_id(session_id))) {
        TRANS_LOG(WARN, "set session id error", K(ret), K(session_id), K(*this));
      } else if (OB_FAIL(trans_desc_->set_proxy_session_id(proxy_session_id))) {
        TRANS_LOG(WARN, "set proxy session id error", K(ret), K(proxy_session_id), K(*this));
      }
    }
  }

  TRANS_LOG(INFO,
      "xa merge status",
      K(ret),
      K(trans_id_),
      K(xid_),
      K_(xa_trans_state),
      K(trans_desc),
      K(is_stmt_push),
      K(this));
  return ret;
}

int ObScheTransCtx::save_trans_desc(const ObTransDesc& trans_desc)
{
  CtxLockGuard guard(lock_);
  return save_trans_desc_(trans_desc);
}

int ObScheTransCtx::save_trans_desc_(const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  uint32_t session_id = trans_desc.get_session_id();
  uint64_t proxy_session_id = trans_desc.get_proxy_session_id();

  if (NULL == trans_desc_) {
    void* ptr = NULL;
    if (OB_UNLIKELY(NULL == (ptr = ob_malloc(sizeof(ObTransDesc), "ObScheTransCtx")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      trans_desc_ = new (ptr) ObTransDesc;
    }
  } else {
    session_id = trans_desc_->get_session_id();
    proxy_session_id = trans_desc_->get_proxy_session_id();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans_desc_->trans_deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "deep copy failed", K(ret));
      trans_desc_->~ObTransDesc();
      ob_free(trans_desc_);
      trans_desc_ = NULL;
    } else if (OB_FAIL(trans_desc_->set_session_id(session_id))) {
      TRANS_LOG(WARN, "set session id error", K(ret), K(session_id), K(*this));
    } else if (OB_FAIL(trans_desc_->set_proxy_session_id(proxy_session_id))) {
      TRANS_LOG(WARN, "set proxy session id error", K(ret), K(proxy_session_id), K(*this));
    }
  }
  TRANS_LOG(INFO, "save trans desc", K(trans_desc), K(this));
  return ret;
}

int ObScheTransCtx::trans_deep_copy(ObTransDesc& trans_desc) const
{
  int ret = OB_SUCCESS;
  uint32_t session_id = trans_desc.get_session_id();
  uint64_t proxy_session_id = trans_desc.get_proxy_session_id();
  CtxLockGuard guard(lock_);
  if (OB_ISNULL(trans_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans_desc_ is null", K(ret), K(*this));
  } else if (OB_FAIL(trans_desc.trans_deep_copy(*trans_desc_))) {
    TRANS_LOG(WARN, "trans deep copy failed", K(ret), K(*trans_desc_), K(trans_desc));
  } else if (OB_FAIL(trans_desc_->set_session_id(session_id))) {
    TRANS_LOG(WARN, "set session id error", K(ret), K(session_id), K(*this));
  } else if (OB_FAIL(trans_desc_->set_proxy_session_id(proxy_session_id))) {
    TRANS_LOG(WARN, "set proxy session id error", K(ret), K(proxy_session_id), K(*this));
  }
  return ret;
}

int ObXABranchInfo::init(const ObXATransID& xid, const int64_t state, const int64_t timeout_seconds,
    const int64_t abs_expired_time, const common::ObAddr& addr, const int64_t unrespond_msg_cnt,
    const int64_t last_hb_ts)
{
  int ret = OB_SUCCESS;
  if (!xid.is_valid() || !ObXATransState::is_valid(state) || timeout_seconds < 0 || !addr.is_valid() ||
      unrespond_msg_cnt < 0 || last_hb_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(
        WARN, "invalid argument", K(xid), K(state), K(timeout_seconds), K(addr), K(unrespond_msg_cnt), K(last_hb_ts));
  } else {
    xid_ = xid;
    state_ = state;
    timeout_seconds_ = timeout_seconds;
    abs_expired_time_ = abs_expired_time;
    addr_ = addr;
    unrespond_msg_cnt_ = unrespond_msg_cnt;
    last_hb_ts_ = last_hb_ts;
  }
  return ret;
}

int ObScheTransCtx::init_xa_branch_info_()
{
  int ret = OB_SUCCESS;

  if (NULL == xa_branch_info_) {
    void* ptr = NULL;
    if (NULL == (ptr = ob_malloc(sizeof(ObXABranchInfoArray), "XABranchInfo"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", K(ret), K(*this));
    } else {
      xa_branch_info_ = new (ptr) ObXABranchInfoArray();
    }
  }
  return ret;
}

int ObScheTransCtx::update_xa_branch_info(
    const ObXATransID& xid, const int64_t to_state, const ObAddr& addr, const int64_t timeout_seconds)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (!xid.is_valid() || !ObXATransState::is_valid(to_state) ||
      (ObXATransState::ACTIVE == to_state && !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(xid), K(timeout_seconds), K(to_state), K(addr), K(*this));
  } else if (OB_ISNULL(xa_branch_info_) && OB_FAIL(init_xa_branch_info_())) {
    TRANS_LOG(WARN, "init xa branch timeout array failed", K(ret), K(xid), K(*this));
  } else {
    bool found = false;
    int64_t now = ObTimeUtil::current_time();
    for (int64_t i = 0; !found && i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo& info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        info.state_ = to_state;
        info.unrespond_msg_cnt_ = 0;
        info.last_hb_ts_ = now;
        if (ObXATransState::ACTIVE == to_state) {
          info.addr_ = addr;  // only possible on xa_start
        }
        if (ObXATransState::IDLE == to_state) {
          info.abs_expired_time_ = now + info.timeout_seconds_ * 1000000;
        } else {
          info.abs_expired_time_ = INT64_MAX;
        }
        found = true;
      }
    }
    if (!found) {
      if (ObXATransState::ACTIVE != to_state) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "branch info not found in array", K(ret), K(xid), K(*this));
      } else {
        ObXABranchInfo info;
        if (OB_FAIL(info.init(xid, to_state, timeout_seconds, INT64_MAX, addr, 0, ObTimeUtil::current_time()))) {
          TRANS_LOG(WARN, "branch info init failed", K(ret), K(*this));
        } else if (OB_FAIL(xa_branch_info_->push_back(info))) {
          TRANS_LOG(WARN, "push xa branch info failed", K(ret), K(info), K(*this));
        } else {
          TRANS_LOG(INFO, "add new branch info", K(info), K(*this), "lbt", lbt());
        }
      }
    }
  }
  return ret;
}

int ObScheTransCtx::register_xa_timeout_task()
{
  int ret = OB_SUCCESS;
  int64_t target = INT64_MAX;
  CtxLockGuard guard(lock_);

  if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), K(*this));
  } else {
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      const ObXABranchInfo& info = xa_branch_info_->at(i);
      if (ObXATransState::IDLE == info.state_ && info.abs_expired_time_ < target) {
        target = info.abs_expired_time_;
      }
    }
    if (target > trans_expired_time_) {
      target = trans_expired_time_;
    }
    target = target - ObTimeUtil::current_time();
    if (OB_LIKELY(target > 0)) {
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(target))) {
        TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(target), K(*this));
      }
    }
  }
  TRANS_LOG(INFO, "register xa timeout task", K(target), K(ret), K(*this));
  return ret;
}

int ObScheTransCtx::update_xa_branch_hb_info(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (OB_UNLIKELY(!xid.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(*this));
  } else {
    ret = update_xa_branch_hb_info_(xid);
  }
  return ret;
}

// called when receive stmt pull/push
int ObScheTransCtx::update_xa_branch_hb_info_(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;

  if (is_exiting_ || is_terminated_) {
    // do nothing
  } else if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), K(xid), K(*this));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo& info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        info.unrespond_msg_cnt_ = 0;
        info.last_hb_ts_ = ObTimeUtil::current_time();
        found = true;
      }
    }
    if (OB_UNLIKELY(!found)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "branch info not found in array", K(ret), K(xid), K(*this));
    }
  }
  return ret;
}

static const int64_t MAX_UNRESPOND_XA_HB_CNT = 3;
static const int64_t XA_HB_THRESHOLD = 3000000;  // 3S

int ObScheTransCtx::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!is_xa_local_trans()) {
  } else if (!is_tightly_coupled_) {
    // do nothing
  } else if (OB_ISNULL(xa_branch_info_)) {
    // mostly a temproray scheduler, do nothing
  } else {
    int64_t now = ObTimeUtil::current_time();
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo& info = xa_branch_info_->at(i);
      if (info.addr_ == GCTX.self_addr_) {
        // do nothing
      } else if (ObXATransState::ACTIVE != info.state_) {
        // do nothing
      } else if (info.unrespond_msg_cnt_ > MAX_UNRESPOND_XA_HB_CNT) {
        // TODO: direct sync call or submit a async task
        const bool is_rollback = true;
        if (OB_FAIL(xa_one_phase_end_trans_(is_rollback))) {
          TRANS_LOG(WARN, "rollback xa trans failed", KR(ret), K(*this));
        } else {
          // is_terminated_ = true;
          set_terminated_();
        }
        TRANS_LOG(INFO, "scheduler unrespond, rollbacked", K(ret), K(info), K(*this));
        break;
      } else if (now > info.last_hb_ts_ + XA_HB_THRESHOLD) {
        ObXAHbRequest req;
        if (OB_FAIL(req.init(trans_id_, info.xid_, GCTX.self_addr_))) {
          TRANS_LOG(WARN, "xa hb request init failed", KR(ret), K(info), K(*this));
        } else if (OB_FAIL(xa_rpc_->xa_hb_req(tenant_id_, info.addr_, req, NULL))) {
          TRANS_LOG(WARN, "post xa  hb req failed", KR(ret), K(info), K(*this));
        } else {
          info.unrespond_msg_cnt_++;
        }
        TRANS_LOG(INFO, "post hb req", K(ret), K(info), K(*this));
      }
    }
  }
  return ret;
}

int ObScheTransCtx::xa_hb_resp(const ObXATransID& xid, const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!xid.is_valid() || !addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(addr));
  } else {
    ObXAHbResponse resp;
    if (OB_FAIL(resp.init(trans_id_, xid, GCTX.self_addr_))) {
      TRANS_LOG(WARN, "xa hb resp init failed", KR(ret), K(xid), K(*this));
    } else if (OB_FAIL(xa_rpc_->xa_hb_resp(tenant_id_, addr, resp, NULL))) {
      TRANS_LOG(WARN, "post xa hb resp failed", KR(ret), K(xid), K(*this));
    }
  }
  TRANS_LOG(INFO, "xa hb resp", KR(ret), K(xid), K(*this));
  return ret;
}

int ObScheTransCtx::xa_try_local_lock(const ObXATransID& xid)
{
  return xa_try_global_lock(xid);
}

int ObScheTransCtx::xa_release_local_lock(const ObXATransID& xid)
{
  CtxLockGuard guard(lock_);
  return xa_release_global_lock_(xid);
}

int ObScheTransCtx::xa_try_global_lock(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_SUCCESS != (ret = xa_stmt_lock_.try_wrlock(common::ObLatchIds::XA_STMT_LOCK))) {
    if (OB_UNLIKELY(!lock_xid_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected lock xid", K(*this));
    } else if (OB_UNLIKELY(lock_xid_.all_equal_to(xid))) {
      TRANS_LOG(INFO, "xa global lock hold by self", K(xid), K(lock_xid_), K(lock_grant_));
      ret = OB_SUCCESS;
    } else {
      ret = OB_TRANS_STMT_NEED_RETRY;
      TRANS_LOG(INFO, "xa get global lock failed", K(*this), K(xid));
    }
  } else {
    lock_xid_ = xid;
    ++lock_grant_;
    TRANS_LOG(INFO, "xa grant global lock", K(xid), K(*this), K(lock_grant_));
  }
  return ret;
}

int ObScheTransCtx::xa_release_global_lock(const ObXATransID& xid)
{
  CtxLockGuard guard(lock_);
  return xa_release_global_lock_(xid);
}

int ObScheTransCtx::xa_release_global_lock_(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;

  if (!lock_xid_.all_equal_to(xid)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected error when release xa global lock", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_stmt_lock_.unlock())) {
    TRANS_LOG(WARN, "unexpected lock state", K(ret), K(xid), K(*this));
  } else {
    --lock_grant_;
    lock_xid_.reset();
    TRANS_LOG(INFO, "xa release global lock", K(xid), K(*this), K(lock_grant_));
  }

  return ret;
}

int ObScheTransCtx::add_xa_branch_count()
{
  CtxLockGuard guard(lock_);
  xa_branch_count_++;
  // TRANS_LOG(INFO, "xa_branch_count_", K(*this), "lbt", lbt());
  return OB_SUCCESS;
}

int64_t ObScheTransCtx::dec_and_get_xa_branch_count()
{
  CtxLockGuard guard(lock_);
  xa_branch_count_--;
  // TRANS_LOG(INFO, "xa_branch_count_", K(*this), "lbt", lbt());
  return xa_branch_count_;
}

int64_t ObScheTransCtx::add_and_get_xa_ref_count()
{
  CtxLockGuard guard(lock_);
  xa_ref_count_++;
  // TRANS_LOG(INFO, "xa_ref_count_", K(xa_ref_count_), K(*this), "lbt", lbt());
  return xa_ref_count_;
}

int64_t ObScheTransCtx::dec_and_get_xa_ref_count()
{
  CtxLockGuard guard(lock_);
  xa_ref_count_--;
  // TRANS_LOG(INFO, "xa_ref_count_", K(xa_ref_count_), K(*this), "lbt", lbt());
  return xa_ref_count_;
}

void ObScheTransCtx::set_tmp_trans_desc(ObTransDesc& trans_desc)
{
  tmp_trans_desc_ = &trans_desc;
  xa_sync_status_cond_.reset();
  // TRANS_LOG(INFO, "set_tmp_trans_desc", KP(this), KP(&trans_desc), "lbt", common::lbt());
}

int ObScheTransCtx::wait_xa_start_complete()
{
  int ret = OB_SUCCESS;
  const int64_t wait_time = 10000000;  // 10s
  int result = OB_SUCCESS;
  if (OB_FAIL(xa_start_cond_.wait(wait_time, result)) || OB_FAIL(result)) {
    TRANS_LOG(WARN, "wait xa start complete failed", K(ret), K(result));
  }
  return ret;
}

void ObScheTransCtx::notify_xa_start_complete(const int result)
{
  xa_start_cond_.notify(result);
}

int ObScheTransCtx::check_for_xa_execution(const bool is_new_branch, const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    if (is_tightly_coupled_) {
      // tightly
      ret = OB_TRANS_XA_BRANCH_FAIL;
      TRANS_LOG(WARN, "xa trans is terminating", K(ret), K(*this));
    } else {
      // loosely
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
    }
  } else if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(WARN, "xa trans is terminating", K(ret), K(*this));
  } else if (has_decided_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected, xa trans already decided", K(ret), K(*this));
  } else {
    // original scheduler need check preparing state
    if (is_tightly_coupled_) {
      if (is_new_branch) {
        // add new branch
        if (ObXATransState::PREPARING == xa_trans_state_) {
          ret = OB_TRANS_XA_PROTO;
          TRANS_LOG(WARN, "xa trans has entered into commit phase", K(ret), K(is_new_branch), K(xid), K(*this));
        }
      } else {
        if (ObXATransState::PREPARING == xa_trans_state_) {
          if (OB_FAIL(xa_rollback_session_terminate_())) {
            TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(is_new_branch), K(xid), K(*this));
          } else {
            ret = OB_TRANS_XA_BRANCH_FAIL;
            TRANS_LOG(WARN,
                "other branches have prepared, xa trans shouled be terminated",
                K(ret),
                K(is_new_branch),
                K(xid),
                K(*this));
          }
        }
      }
    } else {
      // no need to check preparing state in loosely coupe mode
      // do nothing
    }
  }
  return ret;
}

void ObScheTransCtx::set_terminated_()
{
  TRANS_LOG(INFO, "set terminated", K(is_terminated_), K(*this), "lbt", lbt());
  is_terminated_ = true;
}

int ObScheTransCtx::xa_end_stmt(
    const ObXATransID& xid, const ObTransDesc& trans_desc, const bool from_remote, const int64_t seq_no)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObScheTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (!xid.is_valid() || !trans_desc.is_valid() || seq_no < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(xid), K(trans_desc), K(seq_no), K(*this));
  } else if (!is_tightly_coupled_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xa trans coupled mode", K(ret), K(xid), K(*this));
  } else {
    if (from_remote) {
      if (sql_no_ >= seq_no) {
        // duplicate request
        TRANS_LOG(INFO, "recevice duplicate requtest", K(xid), K(*this));
      } else if (OB_FAIL(update_xa_branch_hb_info_(xid))) {
        TRANS_LOG(WARN, "update xa branch hb info failed", KR(ret), K(xid), K(*this));
      } else if (OB_FAIL(xa_merge_status_(trans_desc, true /*is_stmt_push*/))) {
        TRANS_LOG(WARN, "xa sync status failed", K(ret), K(xid), K(*this));
      } else if (OB_FAIL(xa_release_global_lock_(xid))) {
        TRANS_LOG(WARN, "xa release global lock failed", K(ret), K(xid), K(*this));
      } else {
        savepoint_sql_no_ = trans_desc.get_stmt_min_sql_no();
        sql_no_ = seq_no;
      }
    } else {
      if (OB_FAIL(save_trans_desc_(trans_desc))) {
        TRANS_LOG(WARN, "save trans desc failed", K(ret), K(xid), K(*this));
      } else if (OB_FAIL(xa_release_global_lock_(xid))) {
        TRANS_LOG(WARN, "xa release global lock failed", K(ret), K(xid), K(*this));
      } else {
        savepoint_sql_no_ = trans_desc.get_stmt_min_sql_no();
        sql_no_ = seq_no;
      }
    }
  }
  return ret;
}

int64_t ObScheTransCtx::get_unprepared_branch_count_()
{
  int unprepared_branch_count = 0;
  for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
    ObXABranchInfo& info = xa_branch_info_->at(i);
    if (ObXATransState::PREPARED != info.state_) {
      ++unprepared_branch_count;
    }
  }
  return unprepared_branch_count;
}

}  // namespace transaction
}  // namespace oceanbase
