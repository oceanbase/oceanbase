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

#include "lib/profile/ob_perf_event.h"
#include "ob_trans_coord_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_service.h"
#include "storage/ob_partition_service.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_log_entry_header.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_status.h"
#include "ob_trans_msg_type2.h"
#include "ob_trans_msg2.h"
#include "ob_trans_split_adapter.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace transaction {
int ObCoordTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service, const int64_t commit_times)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObCoordTransCtx inited twice");
    ret = OB_INIT_TWICE;
    // commit_times is 0 when coordinator is created by participant;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!trans_id.is_valid()) ||
             OB_UNLIKELY(!self.is_valid()) || OB_UNLIKELY(trans_expired_time <= 0) || OB_ISNULL(ctx_mgr) ||
             OB_UNLIKELY(!trans_param.is_valid()) || OB_UNLIKELY(cluster_version <= 0) || OB_ISNULL(trans_service) ||
             OB_UNLIKELY(0 > commit_times)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(self),
        KP(ctx_mgr),
        K(trans_param),
        K(cluster_version),
        KP(trans_service),
        K(commit_times));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(part_trans_ctx_mgr_ = &(trans_service->get_part_trans_ctx_mgr())) ||
             OB_ISNULL(trans_status_mgr_ = trans_service->get_trans_status_mgr())) {
    TRANS_LOG(WARN, "get partition service error", K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObDistTransCtx::init(tenant_id,
                 trans_id,
                 trans_expired_time,
                 self,
                 ctx_mgr,
                 trans_param,
                 cluster_version,
                 trans_service))) {
    TRANS_LOG(WARN, "ObDistTransCtx inited error", KR(ret));
  } else {
    is_inited_ = true;
    commit_times_ = commit_times;
  }

  return ret;
}

bool ObCoordTransCtx::is_inited() const
{
  return ATOMIC_LOAD(&is_inited_);
}

void ObCoordTransCtx::destroy()
{
  if (is_inited_) {
    REC_TRANS_TRACE_EXT(tlog_, destroy, OB_ID(uref), get_uref());
    clear_part_ctx_arr_();
    ObDistTransCtx::destroy();
    is_gts_waiting_ = false;
    is_inited_ = false;
  }
}

void ObCoordTransCtx::reset()
{
  // destroy();
  ObDistTransCtx::reset();
  // coordctx, must be set after ObDistTransCtx::reset
  magic_number_ = COORD_CTX_MAGIC_NUM;
  is_inited_ = false;
  already_response_ = false;
  msg_mask_set_.reset();
  partition_log_info_arr_.reset();
  trans_location_cache_.reset();
  prepare_unknown_count_ = 0;
  prepare_error_count_ = 0;
  global_trans_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  batch_commit_trans_ = false;
  enable_new_1pc_ = false;
  part_trans_ctx_mgr_ = NULL;
  need_collect_logid_before_prepare_ = false;
  gts_request_ts_ = 0;
  is_gts_waiting_ = false;
  commit_times_ = -1;
  have_prev_trans_ = false;
  trans_status_mgr_ = NULL;
  stmt_rollback_info_.reset();
  same_leader_partitions_mgr_.reset();
  unconfirmed_participants_.reset();
  participant_publish_version_array_.reset();
  split_info_arr_.reset();
  is_waiting_xa_commit_ = true;
  part_ctx_arr_.reset();
  clear_log_base_ts_ = OB_INVALID_TIMESTAMP;
}

void ObCoordTransCtx::clear_part_ctx_arr_()
{
  for (int64_t i = 0; i < part_ctx_arr_.count(); ++i) {
    if (NULL != part_ctx_arr_.at(i).ctx_) {
      (void)part_trans_ctx_mgr_->revert_trans_ctx(part_ctx_arr_.at(i).ctx_);
      part_ctx_arr_.at(i).ctx_ = NULL;
    }
  }
  part_ctx_arr_.reset();
}

int ObCoordTransCtx::construct_context_raw_(int64_t msg_type, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, int64_t status, int64_t trans_version,
    const PartitionLogInfoArray& partition_log_info_arr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(set_scheduler_(scheduler))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), "scheduler", scheduler);
  } else if (OB_FAIL(set_coordinator_(coordinator))) {
    TRANS_LOG(WARN, "set coordinator error", K(ret), "coordinator", coordinator);
  } else if (OB_FAIL(set_participants_(participants))) {
    TRANS_LOG(WARN, "set participants error", K(ret), "participants", participants);
  } else if (OB_FAIL(msg_mask_set_.init(participants))) {
    TRANS_LOG(WARN, "init message mask set error", K(ret), "participants", participants);
    // } else if (OB_FAIL(set_xid(msg.get_xid()))) {
    //   TRANS_LOG(WARN, "set xid error", K(ret), K_(xid));
  } else {
    if (OB_TRANS_STATE_UNKNOWN != status) {
      // cannot set coordinator status unknown when current participant response is unkonwn
      set_status_(status);
    }
    set_global_trans_version_(trans_version);
    switch (msg_type) {
      case OB_TRANS_COMMIT_REQUEST:
      case OB_TRANS_XA_PREPARE_REQUEST:
      case OB_TRANS_ABORT_REQUEST: {
        break;
      }
      case OB_TRANS_2PC_LOG_ID_RESPONSE:
      case OB_TRX_2PC_PRE_COMMIT_RESPONSE:
      case OB_TRANS_2PC_PRE_COMMIT_RESPONSE: {
        // DISCARD_MESSAGE(msg);
        ret = OB_EAGAIN;
        break;
      }
      case OB_TRX_2PC_PREPARE_RESPONSE:
      case OB_TRANS_2PC_PREPARE_RESPONSE: {
        // need recover all participants' log id and log timestamp
        if (!is_xa_local_trans() && partition_log_info_arr.count() > 0) {
          batch_commit_trans_ = true;
          if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
            TRANS_LOG(WARN, "partition logid array push back error", K(ret), K(msg_type));
          }
        }
        set_state_(Ob2PCState::PREPARE);
        break;
      }
      case OB_TRX_2PC_COMMIT_RESPONSE:
      case OB_TRANS_2PC_COMMIT_RESPONSE: {
        set_state_(Ob2PCState::COMMIT);
        // recover log id array
        if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
          TRANS_LOG(WARN, "partition logid array push back error", K(ret), K(msg_type));
        }
        break;
      }
      case OB_TRX_2PC_ABORT_RESPONSE:
      case OB_TRANS_2PC_ABORT_RESPONSE: {
        set_state_(Ob2PCState::ABORT);
        break;
      }
      case OB_TRX_2PC_CLEAR_RESPONSE:
      case OB_TRANS_2PC_CLEAR_RESPONSE: {
        set_state_(Ob2PCState::CLEAR);
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid message type", K(msg_type));
        ret = OB_TRANS_INVALID_LOG_TYPE;
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t usec_per_sec = 1000 * 1000;
    if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_ + trans_id_.hash() % usec_per_sec))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), K_(self), K_(trans_id));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "construct transaction context error", KR(ret), "context", *this);
    // coordinator context should exit when construct context fail in case of context leak
    set_exiting_();
  } else {
    TRANS_LOG(DEBUG, "construct transaction context success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_, cons_context, OB_ID(ret), ret, OB_ID(read_only), is_readonly_, OB_ID(uref), get_uref());

  return ret;
}

int ObCoordTransCtx::construct_context(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObCoordTransCtx not inited", K(ret));
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(set_xid_(msg.get_xid()))) {
    TRANS_LOG(WARN, "set xid error", K(ret), K_(xid));
  } else {
    ret = construct_context_raw_(msg.get_msg_type(),
        msg.get_scheduler(),
        msg.get_coordinator(),
        msg.get_participants(),
        msg.get_status(),
        msg.get_trans_version(),
        msg.get_partition_log_info_arr());
  }

  return ret;
}

int ObCoordTransCtx::construct_context(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTrxMsgBase* message = const_cast<ObTrxMsgBase*>(&msg);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObCoordTransCtx not inited", K(ret));
  } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type) {
    ObTrx2PCCommitResponse* res = static_cast<ObTrx2PCCommitResponse*>(message);
    ret = construct_context_raw_(msg_type,
        res->scheduler_,
        res->coordinator_,
        res->participants_,
        res->status_,
        res->trans_version_,
        res->partition_log_info_arr_);
  } else if (OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type) {
    ObTrx2PCPreCommitResponse* res = static_cast<ObTrx2PCPreCommitResponse*>(message);
    ret = construct_context_raw_(msg_type,
        res->scheduler_,
        res->coordinator_,
        res->participants_,
        res->status_,
        res->trans_version_,
        res->partition_log_info_arr_);
  } else if (OB_TRX_2PC_ABORT_RESPONSE == msg_type) {
    ObTrx2PCAbortResponse* res = static_cast<ObTrx2PCAbortResponse*>(message);
    int trans_version = -1;
    PartitionLogInfoArray partition_log_info_arr;
    ret = construct_context_raw_(msg_type,
        res->scheduler_,
        res->coordinator_,
        res->participants_,
        res->status_,
        trans_version,
        partition_log_info_arr);
  } else if (OB_TRX_2PC_CLEAR_RESPONSE == msg_type) {
    ObTrx2PCClearResponse* res = static_cast<ObTrx2PCClearResponse*>(message);
    int trans_version = -1;
    PartitionLogInfoArray partition_log_info_arr;
    update_clear_log_base_ts_(res->clear_log_base_ts_);
    ret = construct_context_raw_(msg_type,
        res->scheduler_,
        res->coordinator_,
        res->participants_,
        res->status_,
        trans_version,
        partition_log_info_arr);
  } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type) {
    ObTrx2PCPrepareResponse* res = static_cast<ObTrx2PCPrepareResponse*>(message);
    int trans_version = -1;
    if (OB_FAIL(set_xid_(res->xid_))) {
      TRANS_LOG(WARN, "set xid error", K(ret), K_(xid));
    } else {
      if (is_xa_local_trans() && res->is_xa_prepare_) {
        is_waiting_xa_commit_ = true;
      } else {
        is_waiting_xa_commit_ = false;
      }
      ret = construct_context_raw_(msg_type,
          res->scheduler_,
          res->coordinator_,
          res->participants_,
          res->status_,
          trans_version,
          res->partition_log_info_arr_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObCoordTransCtx::handle_batch_commit_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ClogBuf* clog_buf = NULL;

  if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_ISNULL(clog_buf = ClogBufFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(ERROR, "alloc clog buffer error", KR(ret));
  } else {
    clog::ObLogInfoArray log_info_arr;
    clog::ObISubmitLogCbArray cb_arr;
    static __thread int64_t log_header_size = 0;
    if (0 == log_header_size) {
      clog::ObLogEntryHeader log_header;
      log_header_size = log_header.get_serialize_size();
    }
    char* buf = clog_buf->get_buf();
    const int64_t size =
        clog_buf->get_size() - OB_TRANS_REDO_LOG_RESERVE_SIZE - participants_.count() * log_header_size;
    int64_t pos = 0;
    if (0 >= size) {
      ret = OB_BUF_NOT_ENOUGH;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
      ObTransCtx* ctx = NULL;
      bool alloc = false;
      if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(participants_.at(i),
              trans_id_,
              false, /*for_replay*/
              is_readonly_,
              trans_param_.is_bounded_staleness_read(),
              false, /*need_completed_dirty_txn*/
              alloc,
              ctx))) {
        TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), "context", *this);
      } else {
        clog::ObLogInfo log_info;
        clog::ObISubmitLogCb* cb = NULL;
        ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (OB_FAIL(part_ctx->generate_redo_prepare_log_info(buf,
                size,
                pos,
                request_id_,
                partition_log_info_arr_,
                global_trans_version_,
                have_prev_trans_,
                log_info,
                cb))) {
          TRANS_LOG(WARN, "generate redo prepare log info error", KR(ret), "context", *this);
        } else if (OB_FAIL(log_info_arr.push_back(log_info))) {
          TRANS_LOG(WARN, "log info arr push back error", KR(ret), K(log_info), "context", *this);
        } else if (OB_FAIL(cb_arr.push_back(cb))) {
          TRANS_LOG(WARN, "cb array push back error", KR(ret), KP(cb), "context", *this);
        } else {
          // do nothing
        }
        (void)part_trans_ctx_mgr_->revert_trans_ctx(ctx);
      }
    }
    if (OB_SUCC(ret)) {
      ObIClogAdapter* clog_adapter = trans_service_->get_clog_adapter();
      if (OB_FAIL(clog_adapter->batch_submit_log(trans_id_, participants_, log_info_arr, cb_arr))) {
        TRANS_LOG(WARN,
            "batch_submit_log error",
            K(ret),
            K_(global_trans_version),
            K(log_info_arr),
            K(cb_arr),
            "context",
            *this);
      } else {
        msg_mask_set_.clear_set();
      }
    }
    if (OB_SUCC(ret)) {
      (void)batch_submit_log_over_(true);
      (void)ObTransStatistic::get_instance().add_batch_commit_trans_count(self_.get_tenant_id(), 1);
    } else {
      (void)batch_submit_log_over_(false);
      batch_commit_trans_ = false;
      msg_mask_set_.clear_set();
      partition_log_info_arr_.reset();
      ret = OB_SUCCESS;
      drive_();
    }
  }
  if (NULL != clog_buf) {
    ClogBufFactory::release(clog_buf);
    clog_buf = NULL;
  }

  return ret;
}

int ObCoordTransCtx::batch_submit_log_over_(const bool submit_log_succ)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    for (int64_t i = 0; i < participants_.count(); ++i) {
      ObTransCtx* ctx = NULL;
      bool alloc = false;
      // ignore retcode
      if (OB_SUCCESS != (tmp_ret = part_trans_ctx_mgr_->get_trans_ctx(participants_.at(i),
                             trans_id_,
                             false, /*for_replay*/
                             is_readonly_,
                             trans_param_.is_bounded_staleness_read(),
                             false, /*need_completed_dirty_txn*/
                             alloc,
                             ctx))) {
        TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), "context", *this);
      } else {
        ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (OB_SUCCESS != (tmp_ret = part_ctx->batch_submit_log_over(submit_log_succ, global_trans_version_))) {
          TRANS_LOG(WARN, "batch commit over error", K(tmp_ret), "context", *this);
        }
        (void)part_trans_ctx_mgr_->revert_trans_ctx(ctx);
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::handle_message(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t last_request_id = request_id_;

  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else {
    const int64_t msg_type = msg.get_msg_type();
    switch (msg_type) {
      case OB_TRANS_COMMIT_REQUEST: {
        commit_times_ = max(commit_times_, msg.get_commit_times());
        if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (msg.get_sender_addr() == addr_ && msg.get_stc() > MonotonicTs(0)) {
          set_stc_(msg.get_stc());
        } else {
          set_stc_by_now_();
        }
        if (is_xa_local_trans()) {
          // xa one phase commit
          xid_.reset();
          is_waiting_xa_commit_ = false;
        }
        if (OB_FAIL(set_app_trace_info_(msg.get_app_trace_info()))) {
          TRANS_LOG(WARN, "set app trace info error", K(ret), K(msg), K(*this));
        } else if (OB_FAIL(handle_trans_request_(msg.get_trans_location_cache(),
                       msg.get_stmt_rollback_info(),
                       msg.is_dup_table_trans(),
                       msg.get_commit_times(),
                       false))) {
          TRANS_LOG(WARN, "handle commit request error", KR(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_ABORT_REQUEST: {
        commit_times_ = max(commit_times_, msg.get_commit_times());
        if (is_xa_local_trans()) {
          // xa one phase rollback
          xid_.reset();
          is_waiting_xa_commit_ = false;
        }
        if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (OB_FAIL(handle_trans_request_(msg.get_trans_location_cache(),
                       msg.get_stmt_rollback_info(),
                       msg.is_dup_table_trans(),
                       msg.get_commit_times(),
                       true))) {
          TRANS_LOG(WARN, "handle abort request error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_XA_PREPARE_REQUEST: {
        commit_times_ = max(commit_times_, msg.get_commit_times());
        if (OB_FAIL(handle_xa_trans_prepare_request_(msg))) {
          TRANS_LOG(WARN, "handle xa trans prepare request error", K(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_XA_ROLLBACK_REQUEST:
      case OB_TRANS_XA_COMMIT_REQUEST: {
        const bool is_rollback = OB_TRANS_XA_ROLLBACK_REQUEST == msg_type;
        if (OB_FAIL(handle_xa_trans_request_(msg, is_rollback))) {
          TRANS_LOG(WARN, "handle xa trans request error", K(ret), "context", *this, K(msg), K(is_rollback));
        } else {
          TRANS_LOG(INFO, "handle xa trans request success", K_(trans_id), K_(xid));
        }
        break;
      }
      case OB_TRANS_2PC_LOG_ID_RESPONSE: {
        if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
          // do not set retcode
        } else if (OB_FAIL(handle_2pc_log_id_response_(msg))) {
          TRANS_LOG(WARN, "handle 2pc log id response error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_PREPARE_RESPONSE: {
        set_trans_need_wait_wrap_(MonotonicTs::current_time(), msg.get_need_wait_interval_us());
        if (OB_FAIL(set_app_trace_info_(msg.get_app_trace_info()))) {
          TRANS_LOG(WARN, "set app trace info error", K(ret), K(msg), K(*this));
        } else if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (msg.is_xa_prepare() && !is_waiting_xa_commit_) {
          // do nothing
        } else if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
          // do not set retcode
        } else if (OB_FAIL(handle_2pc_prepare_response_(msg))) {
          TRANS_LOG(WARN, "handle 2pc prepare response error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_COMMIT_RESPONSE: {
        if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
          // do not set retcode
        } else if (OB_FAIL(handle_2pc_commit_response_(msg))) {
          TRANS_LOG(WARN, "handle 2pc commit response error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_PRE_COMMIT_RESPONSE: {
        if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
        } else if (OB_FAIL(handle_2pc_pre_commit_response_(msg))) {
          TRANS_LOG(WARN, "handle 2pc pre commit response fail", K(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_ABORT_RESPONSE: {
        if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
          // do not set retcode
        } else if (OB_FAIL(handle_2pc_abort_response_(msg))) {
          TRANS_LOG(WARN, "handle 2pc abort response error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE:
      case OB_TRANS_2PC_CLEAR_RESPONSE: {
        if (need_collect_logid_before_prepare_) {
          // do nothing
        } else if (request_id_ != msg.get_request_id()) {
          TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(msg));
        } else if (OB_FAIL(handle_2pc_clear_response_raw_(msg.get_sender(), &msg.get_batch_same_leader_partitions()))) {
          TRANS_LOG(WARN, "handle 2pc clear response error", K(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      default: {
        TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg_type), K(msg));
        ret = OB_TRANS_INVALID_MESSAGE_TYPE;
        break;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_,
        handle_message,
        OB_ID(ret),
        ret,
        OB_ID(id1),
        last_request_id,
        OB_ID(id2),
        msg.get_request_id(),
        OB_ID(msg_type),
        msg_type,
        OB_ID(sender),
        msg.get_sender(),
        OB_ID(uref),
        get_uref());
  }

  return ret;
}

int ObCoordTransCtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();
  ObPartitionArray arr;
  ObPartitionArray new_participants;

  common::ObTimeGuard timeguard("coord_handle_timeout", 10 * 1000);
  if (OB_SUCC(lock_.lock(5000000 /*5 seconds*/))) {
    CtxLockGuard guard(lock_, false);
    timeguard.click();

    TRANS_LOG(DEBUG, "handle coordinator transaction timeout", "context", *this);
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObCoordTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (!timeout_task_.is_registered()) {
      // timer task is canceled, do nothing
    } else {
      (void)unregister_timeout_task_();
      update_trans_2pc_timeout_();
      timeout_task_.set_running(true);
      if (!split_info_arr_.empty()) {
        if (OB_SUCCESS != (tmp_ret = ObTransSplitAdapter::update_participant_list(
                               this, participants_, msg_mask_set_, split_info_arr_, new_participants))) {
          TRANS_LOG(WARN, "failed to update part list", KR(tmp_ret), K(split_info_arr_));
        } else if (!new_participants.empty()) {
          if (EXECUTE_COUNT_PER_SEC(1)) {
            TRANS_LOG(INFO, "retry 2pc protocol", "new_participants", new_participants, "context", *this);
          }
        }
      }
      // only retry participants that don't response ack
      if (OB_SUCCESS != (tmp_ret = msg_mask_set_.get_not_mask(arr))) {
        TRANS_LOG(WARN, "get not mask partitions error", "ret", tmp_ret);
      } else {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(INFO, "retry 2pc protocol", "unresponse_participants", arr, "context", *this);
        }
      }
      if (now >= (trans_expired_time_ + OB_TRANS_WARN_USE_TIME)) {
        TRANS_LOG(ERROR, "transaction use too much time", "unresponse_participants", arr, "context", *this);
      }
      need_refresh_location_ = true;
      if (OB_FAIL(drive_())) {
        TRANS_LOG(WARN, "handle coordinator timeout fail", KR(ret), "context", *this);
      }
      need_refresh_location_ = false;
      timeout_task_.set_running(false);
    }

    REC_TRANS_TRACE_EXT(
        tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(total_count), arr.count(), OB_ID(uref), get_uref());
  } else {
    TRANS_LOG(WARN, "failed to acquire lock in specified time", K_(trans_id));
    unregister_timeout_task_();
    register_timeout_task_(delay);
  }
  return ret;
}

int ObCoordTransCtx::kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
{
  UNUSED(cb_array);
  int ret = OB_SUCCESS;

  common::ObTimeGuard timeguard("coord_kill", 10 * 1000);
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_pre_preparing_()) {
    ret = OB_EAGAIN;
  } else {
    need_print_trace_log_ = true;
    (void)unregister_timeout_task_();
    set_exiting_();
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

int ObCoordTransCtx::leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array)
{
  UNUSED(cb_array);
  int ret = OB_SUCCESS;

  need_print_trace_log_ = true;
  common::ObTimeGuard timeguard("coord_leader_revoke", 10 * 1000);
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    (void)unregister_timeout_task_();
    set_exiting_();
  }
  REC_TRANS_TRACE_EXT(tlog_,
      leader_revoke,
      OB_ID(ret),
      ret,
      OB_ID(arg1),
      first_check,
      OB_ID(arg2),
      need_release,
      OB_ID(used),
      timeguard.get_diff(),
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObCoordTransCtx::drive_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionArray partitions;
  const int64_t state = get_state_();
  bool need_register_timeout_task = true;

  if (is_gts_waiting_) {
    TRANS_LOG(INFO, "is waiting gts, not drive now", "context", *this);
  } else if (OB_FAIL(msg_mask_set_.get_not_mask(partitions))) {
    TRANS_LOG(WARN, "get partition not tag error", KR(ret));
  } else {
    switch (state) {
      case Ob2PCState::INIT: {
        TRANS_LOG(ERROR, "protocol error", K(state), "context", *this);
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      case Ob2PCState::PRE_PREPARE: {
        if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_PRE_PREPARE_REQUEST))) {
          TRANS_LOG(WARN, "post 2pc pre request error", KR(ret), K(partitions));
        }
        break;
      }
      case Ob2PCState::PREPARE: {
        if (is_xa_local_trans() && msg_mask_set_.is_all_mask() && true == is_waiting_xa_commit_) {
          TRANS_LOG(INFO, "drive xa prepare", K(*this));
          if (OB_FAIL(xa_drive_after_prepare_())) {
            TRANS_LOG(WARN, "drive xa prepare failed", K(ret), K(*this));
          } else {
            // xa preapre over and register timeout task when xa start to commit;
            need_register_timeout_task = false;
            TRANS_LOG(INFO, "drive xa prepare success", K_(trans_id), K_(xid));
          }
        } else if (need_collect_logid_before_prepare_) {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_LOG_ID_REQUEST))) {
            TRANS_LOG(WARN, "post 2pc log id request error", KR(ret), K(partitions));
          }
        } else {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_PREPARE_REQUEST))) {
            TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
          }
        }
        break;
      }
      case Ob2PCState::COMMIT: {
        if (is_xa_local_trans() && msg_mask_set_.is_all_mask()) {
          if (OB_FAIL(xa_drive_after_commit_())) {
            TRANS_LOG(WARN, "xa update inner table commit failed", KR(ret), K(*this));
          }
        } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
          TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
        }
        break;
      }
      case Ob2PCState::PRE_COMMIT: {
        if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_PRE_COMMIT_REQUEST))) {
          TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
        }
        break;
      }
      case Ob2PCState::ABORT: {
        if (is_xa_local_trans() && msg_mask_set_.is_all_mask()) {
          if (OB_FAIL(xa_drive_after_rollback_())) {
            TRANS_LOG(WARN, "xa update inner table rollback failed", KR(ret), K(*this));
          }
        } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_ABORT_REQUEST))) {
          TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
        }
        break;
      }
      case Ob2PCState::CLEAR: {
        if (batch_commit_trans_ && OB_SUCCESS == status_) {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
            TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
          }
        } else {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_CLEAR_REQUEST))) {
            TRANS_LOG(WARN, "post 2pc request error", KR(ret), K(partitions));
          }
        }
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(state));
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }
  if (partitions.count() > 100) {
    update_trans_2pc_timeout_();
  }

  if (need_register_timeout_task) {
    (void)unregister_timeout_task_();
    if (OB_SUCCESS != (tmp_ret = register_timeout_task_(trans_2pc_timeout_))) {
      TRANS_LOG(WARN, "register timeout handler error", "ret", tmp_ret);
      ret = tmp_ret;
    }
  }

  return ret;
}

int ObCoordTransCtx::switch_state_(const int64_t state)
{
  int ret = OB_SUCCESS;
  // const int64_t old_state = state_;
  if (Ob2PCState::CLEAR != state) {
    trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
  } else {
    trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(state_.get_state() == state)) {
    TRANS_LOG(WARN, "state not changed", "cur_state", state_, "new_state", state);
    ret = OB_TRANS_STATE_NOT_CHANGE;
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timeout handler error", KR(ret));
  } else if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
    TRANS_LOG(WARN, "register timeout handler error", KR(ret));
  } else {
    set_state_(state);
    msg_mask_set_.clear_set();
  }
  //_FILL_TRACE_BUF(*tlog_, "ret=%d, %ld->%ld", ret, old_state, state, OB_ID(uref), get_uref());

  return ret;
}

int ObCoordTransCtx::check_and_response_scheduler_(const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  int64_t publish_version = 0;
  ObITsMgr* ts_mgr = get_ts_mgr_();

#ifdef ERRSIM
  int err_switch = E(EventTable::EN_XA_COMMIT_ABORT_RESP_LOST) OB_SUCCESS;
  if (is_xa_local_trans() && (OB_SUCCESS != err_switch)) {
    already_response_ = true;
  }
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type)) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (!already_response_) {
    if (OB_ISNULL(ts_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts mgr is null", KR(ret));
    } else if (OB_FAIL(ts_mgr->get_publish_version(self_.get_tenant_id(), publish_version))) {
      TRANS_LOG(WARN, "get publish version error", KR(ret), "context", *this);
    } else if (OB_TRANS_COMMIT_RESPONSE == msg_type && global_trans_version_ > publish_version) {
      TRANS_LOG(ERROR,
          "unexpected global transaction version",
          K_(global_trans_version),
          K(publish_version),
          "context",
          *this);
      need_print_trace_log_ = true;
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(post_trans_response_(scheduler_, msg_type))) {
      TRANS_LOG(WARN, "post transaction response error", KR(ret), "context", *this);
    } else {
      already_response_ = true;
    }

    REC_TRANS_TRACE_EXT(tlog_,
        response_scheduler,
        OB_ID(ret),
        ret,
        OB_ID(scheduler),
        scheduler_,
        OB_ID(version),
        global_trans_version_,
        OB_ID(publish_version),
        publish_version,
        OB_ID(msg_type),
        msg_type,
        OB_ID(uref),
        get_uref());
  } else {
    // do nothing
  }

  return ret;
}

int ObCoordTransCtx::update_global_trans_version_(const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(trans_version <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(trans_version), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (trans_version != global_trans_version_) {
      if (ObTransVersion::INVALID_TRANS_VERSION == global_trans_version_ || global_trans_version_ < trans_version) {
        global_trans_version_ = trans_version;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, update_trans_version, OB_ID(trans_version), trans_version, OB_ID(uref), get_uref());
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_log_id_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObPartitionKey& partition = msg.get_sender();
  const int status = msg.get_status();
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT:
    case Ob2PCState::PRE_PREPARE: {
      DISCARD_MESSAGE(msg);
      break;
    }
    case Ob2PCState::PREPARE: {
      if (msg_mask_set_.is_mask(partition)) {
        TRANS_LOG(DEBUG, "coordinator received this message before", K(partition));
      } else {
        (void)msg_mask_set_.mask(partition);
        if (OB_SUCCESS == status) {
          if (participants_.count() == msg.get_partition_log_info_arr().count()) {
            batch_commit_trans_ = true;
            if (OB_FAIL(partition_log_info_arr_.assign(msg.get_partition_log_info_arr()))) {
              TRANS_LOG(WARN,
                  "preapre log id info array assign error",
                  KR(ret),
                  "context",
                  *this,
                  "prepare_log_info_arr",
                  msg.get_partition_log_info_arr());
            }
          } else {
            if (OB_INVALID_ID != msg.get_prepare_log_id() && OB_INVALID_TIMESTAMP != msg.get_prepare_log_timestamp()) {
              if (OB_FAIL(collect_partition_log_info_(
                      partition, msg.get_prepare_log_id(), msg.get_prepare_log_timestamp()))) {
                TRANS_LOG(WARN, "coordinator record prepare log id and timestamp error", KR(ret));
              }
            }
          }
        } else if (OB_TRANS_STATE_UNKNOWN == status) {
          ++prepare_unknown_count_;
        } else {
          ++prepare_error_count_;
        }
        if (msg_mask_set_.is_all_mask()) {
          TRANS_LOG(INFO,
              "collect all participants' logid and timestamp success",
              K_(partition_log_info_arr),
              "context",
              *this);
          if (all_prepare_unknown_()) {
            // do nothing
          }
          msg_mask_set_.clear_set();
          prepare_error_count_ = 0;
          prepare_unknown_count_ = 0;
          need_collect_logid_before_prepare_ = false;
          if (!batch_commit_trans_) {
            partition_log_info_arr_.reset();
          }
          if (OB_SUCCESS != (tmp_ret = drive_())) {
            TRANS_LOG(WARN, "drive 2pc error", K(tmp_ret), K(msg), "context", *this);
            ret = tmp_ret;
          }
        }
      }
      break;
    }
    case Ob2PCState::PRE_COMMIT:
    // go through
    case Ob2PCState::COMMIT:
    // go through
    case Ob2PCState::ABORT:
    // go through
    case Ob2PCState::CLEAR: {
      DISCARD_MESSAGE(msg);
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state), "context", *this);
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_pre_prepare_response_(
    const ObPartitionKey& partition, const int status, const int64_t prepare_log_id, const int64_t prepare_log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT: {
      TRANS_LOG(WARN, "discard this message", "sender", partition, K(status), "context", *this);
      break;
    }
    case Ob2PCState::PRE_PREPARE: {
      if (msg_mask_set_.is_mask(partition)) {
        TRANS_LOG(DEBUG, "coordinator received this message before", K(partition));
      } else {
        (void)msg_mask_set_.mask(partition);
        if (OB_SUCCESS == status) {
          if (OB_FAIL(collect_partition_log_info_(partition, prepare_log_id, prepare_log_ts))) {
            TRANS_LOG(WARN,
                "coordinator record prepare log id and timestamp error",
                KR(ret),
                K(partition),
                K(prepare_log_id),
                K(prepare_log_ts));
            // caculate commit version
          } else if (OB_FAIL(update_global_trans_version_(prepare_log_ts))) {
            TRANS_LOG(WARN,
                "update global transaction version error",
                KR(ret),
                K(partition),
                K(prepare_log_id),
                K(prepare_log_ts));
          } else {
            // do nothing
          }
        } else if (OB_TRANS_STATE_UNKNOWN == status) {
          ++prepare_unknown_count_;
        } else {
          ++prepare_error_count_;
        }
        if (msg_mask_set_.is_all_mask()) {
          if (OB_FAIL(switch_state_(Ob2PCState::PREPARE))) {
            TRANS_LOG(WARN,
                "switch state error",
                KR(ret),
                K(partition),
                K(status),
                K(prepare_log_id),
                K(prepare_log_ts),
                "context",
                *this);
          } else if (prepare_error_count_ > 0 || prepare_unknown_count_ > 0) {
            (void)batch_submit_log_over_(false);
            msg_mask_set_.clear_set();
            prepare_error_count_ = 0;
            prepare_unknown_count_ = 0;
            partition_log_info_arr_.reset();
            batch_commit_trans_ = false;
            // start to 2pc
            drive_();
          } else if (OB_FAIL(handle_batch_commit_())) {
            TRANS_LOG(WARN, "handle batch commit error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        }
      }
      break;
    }  // PREPARE
    case Ob2PCState::PREPARE:
    // go through
    case Ob2PCState::PRE_COMMIT:
    // go through
    case Ob2PCState::COMMIT:
    // go through
    case Ob2PCState::ABORT:
    // go through
    case Ob2PCState::CLEAR: {
      TRANS_LOG(WARN, "discard this message", "sender", partition, K(status), "context", *this);
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state), "context", *this);
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::collect_partition_log_info_(
    const ObPartitionKey& partition, const uint64_t log_id, const int64_t log_timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(log_timestamp <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(log_id), K(log_timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool hit = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_log_info_arr_.count(); ++i) {
      if (partition == partition_log_info_arr_.at(i).get_partition()) {
        if (log_id != partition_log_info_arr_.at(i).get_log_id() ||
            log_timestamp != partition_log_info_arr_.at(i).get_log_timestamp()) {
          TRANS_LOG(ERROR,
              "unexpected log id and log timestamp",
              K(partition),
              K(log_id),
              K(log_timestamp),
              "context",
              *this);
          ret = OB_ERR_UNEXPECTED;
        } else {
          hit = true;
        }
      }
    }
    if (OB_SUCC(ret) && !hit) {
      ObPartitionLogInfo partition_log_info(partition, log_id, log_timestamp);
      if (OB_FAIL(partition_log_info_arr_.push_back(partition_log_info))) {
        TRANS_LOG(WARN, "partition log info arr push back error", KR(ret), K(partition_log_info));
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_prepare_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  // when the publish version is set to -1, then the coord is required to send the precommit to all participants.
  int64_t publish_version = -1;
  ObTransSplitInfo place_holder;
  ret = handle_2pc_prepare_response_raw_(msg.get_msg_type(),
      msg.get_sender(),
      msg.get_status(),
      msg.get_state(),
      msg.get_trans_version(),
      msg.get_partition_log_info_arr(),
      msg.get_prepare_log_id(),
      msg.get_prepare_log_timestamp(),
      publish_version,
      place_holder);
  return ret;
}

int ObCoordTransCtx::handle_2pc_prepare_response_raw_(const int64_t msg_type, const ObPartitionKey& partition,
    const int32_t status, const int64_t partition_state, const int64_t trans_version,
    const PartitionLogInfoArray& partition_log_info_arr, const int64_t prepare_log_id,
    const int64_t prepare_log_timestamp, const int64_t publish_version, const ObTransSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = get_ts_mgr_();
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT:
    case Ob2PCState::PRE_PREPARE: {
      // DISCARD_MESSAGE(msg);
      break;
    }
    case Ob2PCState::PREPARE: {
      bool need_response = false;
      bool commit = false;
      bool is_participant_commit = false;
      bool need_xa_commit = false;
      if (msg_mask_set_.is_mask(partition)) {
        TRANS_LOG(DEBUG, "coordinator received this message before", K(partition));
      } else if (is_xa_local_trans() && is_waiting_xa_commit_) {
        // send preapre request to all participants when state is switched from xa prepare to xa commit;
        TRANS_LOG(INFO, "[XA] learn xa transaction has been in xa commit from participant", K(partition));
        is_waiting_xa_commit_ = false;
        need_response = true;
        commit = true;
        need_xa_commit = true;
      } else if (Ob2PCState::COMMIT == partition_state) {
        if (OB_FAIL(update_global_trans_version_(trans_version))) {
          TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
        } else {
          // coordiantor is in prepare state and some participants is in commit state,
          // record all participants prepare log id and timestamp
          if (is_single_partition_()) {
            // do nothing
          } else if (partition_log_info_arr.count() <= 0) {
            TRANS_LOG(ERROR, "partition log info count can not less 0", "context", *this);
            ret = OB_ERR_UNEXPECTED;
          } else {
            partition_log_info_arr_.reset();
            if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
              TRANS_LOG(
                  WARN, "preapre log id info array assign error", KR(ret), "context", *this, K(partition_log_info_arr));
            }
          }
          if (OB_SUCC(ret)) {
            (void)msg_mask_set_.mask(partition);
            set_status_(status);
            need_response = true;
            commit = true;
            is_participant_commit = true;
          } else {
            // need reset when assign fail
            msg_mask_set_.reset();
            if (OB_SUCCESS != (tmp_ret = msg_mask_set_.init(participants_))) {
              TRANS_LOG(WARN, "msg mask set init error", "ret", tmp_ret, "context", *this);
              ret = tmp_ret;
            }
          }
        }
      } else if (Ob2PCState::ABORT == partition_state) {
        (void)msg_mask_set_.mask(partition);
        set_status_(status);
        need_response = true;
        commit = false;
      } else if (split_info.is_valid() && OB_FAIL(split_info_arr_.push_back(split_info))) {
        TRANS_LOG(WARN, "failed to store split info", KR(ret), K(split_info));
      } else {
        (void)add_participant_publish_version_(partition, publish_version);
        if (OB_SUCCESS == status) {
          // it is batch commit transaction when msg carries all participants logid and timestamp
          if (partition_log_info_arr.count() == participants_.count()) {
            if (!batch_commit_trans_) {
              TRANS_LOG(INFO, "set batch commit status success", "context", *this);
              batch_commit_trans_ = true;
            }
          }
          if (OB_FAIL(update_global_trans_version_(trans_version))) {
            TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
            // record the prepare log id of partition
          } else if (OB_FAIL(collect_partition_log_info_(partition, prepare_log_id, prepare_log_timestamp))) {
            TRANS_LOG(WARN, "coordinator record prepare log id and timestamp error", KR(ret));
            // do not need rollback global_trans_version
          } else {
            // do nothing
          }
        } else if (OB_TRANS_STATE_UNKNOWN == status) {
          if (OB_FAIL(update_global_trans_version_(ObClockGenerator::getClock() / 2))) {
            TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
          } else {
            ++prepare_unknown_count_;
          }
        } else {
          if (OB_FAIL(update_global_trans_version_(ObClockGenerator::getClock()))) {
            TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
          } else {
            ++prepare_error_count_;
            if (!is_xa_local_trans() || (is_xa_local_trans() && OB_TRANS_ROLLBACKED != status)) {
              TRANS_LOG(WARN, "prepare error", "context", *this);
            }
          }
        }
        if (OB_SUCC(ret)) {
          (void)msg_mask_set_.mask(partition);
        }
        bool need_update_part_list = !split_info_arr_.empty();
        bool update_part_list_succ = true;
        if (msg_mask_set_.is_all_mask() && need_update_part_list) {
          ObPartitionArray new_participants;
          if (OB_FAIL(ObTransSplitAdapter::update_participant_list(
                  this, participants_, msg_mask_set_, split_info_arr_, new_participants))) {
            TRANS_LOG(WARN, "failed to modify part list", KR(ret), K(new_participants));
            update_part_list_succ = false;
          } else if (!new_participants.empty() &&
                     OB_FAIL(post_2pc_request_(new_participants, OB_TRX_2PC_PREPARE_REQUEST))) {
            TRANS_LOG(WARN, "failed to notify new participants", KR(ret), K(new_participants));
          }
        }
        if (msg_mask_set_.is_all_mask() && (!need_update_part_list || update_part_list_succ)) {
          if (prepare_error_count_ > 0 || prepare_unknown_count_ > 0) {
            set_status_(OB_TRANS_ROLLBACKED);
            need_response = true;
            commit = false;
          } else {
            set_status_(OB_SUCCESS);
            need_response = true;
            commit = true;
          }
        }
      }
      if (OB_SUCCESS == ret && need_response) {
        if (commit) {
          bool need_wait = false;
          if (need_xa_commit) {
            msg_mask_set_.clear_set();
            if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_PREPARE_REQUEST))) {
              TRANS_LOG(WARN, "post 2pc request error", KR(ret), K_(participants));
            }
            (void)unregister_timeout_task_();
            if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
              TRANS_LOG(WARN, "register timeout handler error", KR(ret));
            }
          } else if (is_participant_commit) {
            if (OB_FAIL(switch_state_(Ob2PCState::COMMIT))) {
              TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
            } else {
              if (OB_SUCC(ret)) {
                if (batch_commit_trans_) {
                  if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
                    TRANS_LOG(WARN, "switch state error", KR(ret), K(*this));
                  } else if (OB_SUCCESS !=
                             (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
                    TRANS_LOG(WARN, "submit 2pc commit clear request error", "ret", tmp_ret, K(participants_));
                    ret = tmp_ret;
                  } else {
                    // do nothing
                  }
                } else {
                  // ignore error, post 2pc request continue
                  if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
                    TRANS_LOG(WARN, "submit 2pc commit request error", "ret", tmp_ret);
                    ret = tmp_ret;
                  }
                }
              }
            }
          } else if (OB_ISNULL(ts_mgr)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "ts mgr is null", KR(ret));
            // no need to wait gts
          } else if (OB_FAIL(ts_mgr->wait_gts_elapse(self_.get_tenant_id(), global_trans_version_, this, need_wait))) {
            TRANS_LOG(WARN, "wait gts elapse fail", K(ret), K(*this));
          } else if (need_wait) {
            gts_request_ts_ = ObTimeUtility::current_time();
            is_gts_waiting_ = true;
            // inc participant ref
            if (OB_FAIL(partition_mgr_->get_trans_ctx_(trans_id_))) {
              TRANS_LOG(WARN, "get trans ctx error", KR(ret), "context", *this);
            }
          } else {
            if (batch_commit_trans_) {
              if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
                TRANS_LOG(WARN, "switch to pre_commit fail", KR(ret));
              } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
                TRANS_LOG(WARN, "post pre-commit request to participants fail", KR(ret), K(participants_));
              } else {
                // do nothing
              }
            } else if (enable_new_1pc_) {
              if (OB_FAIL(switch_state_(Ob2PCState::COMMIT))) {
                TRANS_LOG(WARN, "switch to pre_commit fail", KR(ret));
              } else if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
              } else if (OB_FAIL(handle_2pc_local_commit_request_())) {
                TRANS_LOG(WARN, "handle 2pc local commit request error", K(ret), K(*this));
                if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
                  ret = tmp_ret;
                  TRANS_LOG(WARN, "post pre-commit request to participants fail", KR(ret), K(participants_));
                }
              } else {
                // do nothing
              }
            } else {
              DEBUG_SYNC(BEFORE_DIST_COMMIT);
#ifdef ERRSIM
              (void)DEBUG_SYNC_slow_txn_during_2pc_commit_phase_for_physical_backup_1055_();
#endif
              if (OB_FAIL(switch_state_(Ob2PCState::PRE_COMMIT))) {
                TRANS_LOG(WARN, "switch to pre_commit fail", KR(ret), "context", *this);
              } else {
                ObPartitionArray negative_participants;
                if ((participants_.count() != unconfirmed_participants_.count()) ||
                    (participants_.count() != participant_publish_version_array_.count())) {
                  if (OB_FAIL(negative_participants.assign(participants_))) {
                    TRANS_LOG(WARN, "assign participants error", KR(ret), K_(participants), K(negative_participants));
                  } else {
                    // do nothing
                  }
                } else {
                  for (int64_t i = 0; i < unconfirmed_participants_.count(); i++) {
                    if (global_trans_version_ > participant_publish_version_array_.at(i)) {
                      if (OB_SUCCESS != (tmp_ret = negative_participants.push_back(unconfirmed_participants_.at(i)))) {
                        TRANS_LOG(WARN,
                            "push participant error",
                            K(tmp_ret),
                            K(negative_participants),
                            K(unconfirmed_participants_.at(i)));
                      }
                    } else {
                      (void)msg_mask_set_.mask(unconfirmed_participants_.at(i));
                    }
                  }
                }
                if (OB_SUCC(ret)) {
                  if (negative_participants.count() != 0) {
                    if (OB_FAIL(post_2pc_request_(negative_participants, OB_TRANS_2PC_PRE_COMMIT_REQUEST))) {
                      TRANS_LOG(WARN,
                          "post pre-commit request to participants fail",
                          KR(ret),
                          K(negative_participants),
                          K_(unconfirmed_participants),
                          K_(participant_publish_version_array));
                    }
                  } else if (msg_mask_set_.is_all_mask()) {
                    if (OB_FAIL(switch_state_(Ob2PCState::COMMIT))) {
                      TRANS_LOG(WARN, "switch to commit fail", K(ret), "context", *this);
                    } else if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
                      TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
                    } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
                      TRANS_LOG(WARN, "post commit request to participants fail", KR(ret), K(participants_));
                    } else {
                      // do nothing
                    }
                  }
                }
              }
            }
          }
        } else {
          // all unknown response scenario :
          // if coordinator context is created for the first time, then response scheduler OB_TRANS_KILLED,
          // otherwise response OB_TRANS_UNKNOWN;
          if (all_prepare_unknown_()) {
            if (1 == commit_times_) {
              ObTransStatus trans_status;
              if (OB_SUCCESS != (tmp_ret = trans_status_mgr_->get_status(trans_id_, trans_status))) {
                if (OB_ENTRY_NOT_EXIST != tmp_ret) {
                  TRANS_LOG(WARN, "get trans status failed", K(tmp_ret));
                }
                set_status_(OB_TRANS_KILLED);
              } else {
                set_status_(trans_status.get_status());
                TRANS_LOG(INFO, "get trans status success", K(trans_status));
              }
            } else {
              ObTransStatus trans_status;
              if (OB_SUCCESS != (tmp_ret = trans_status_mgr_->get_status(trans_id_, trans_status))) {
                if (OB_ENTRY_NOT_EXIST != tmp_ret) {
                  TRANS_LOG(WARN, "get trans status failed", K(tmp_ret));
                }
                set_status_(OB_TRANS_UNKNOWN);
              } else {
                set_status_(trans_status.get_status());
                TRANS_LOG(INFO, "get trans status success", K(trans_status));
              }
              need_print_trace_log_ = true;
            }
            if (OB_SUCCESS == get_status_()) {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            } else {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            }
            (void)unregister_timeout_task_();
            set_exiting_();
          } else {
            if (OB_FAIL(switch_state_(Ob2PCState::ABORT))) {
              TRANS_LOG(WARN, "switch state error", KR(ret), "context", *this);
            } else if (is_xa_local_trans()) {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_XA_ROLLBACK_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            } else {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_ABORT_REQUEST))) {
              TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), "context", *this);
            }
          }
        }
      }
      break;
    }  // PREPARE
    case Ob2PCState::PRE_COMMIT: {
      ObPartitionArray partitions;
      // protocol error: current state is commit, it should not receive prepare no message
      if (OB_SUCCESS != status && OB_TRANS_STATE_UNKNOWN != status) {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else if (OB_TRANS_STATE_UNKNOWN == status) {
        TRANS_LOG(WARN,
            "commit state received a prepare unknow response, discard msg",
            K(state),
            K(status),
            "context",
            *this);
      } else if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
        // submit commit request again, even though coordinator had submitted it before
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_PRE_COMMIT_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc pre-commit request error", KR(ret), K(partition));
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::COMMIT: {
      ObPartitionArray partitions;
      // protocol error: current state is commit, it should not receive prepare no message
      if (OB_SUCCESS != status && OB_TRANS_STATE_UNKNOWN != status) {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else if (OB_TRANS_STATE_UNKNOWN == status) {
        TRANS_LOG(WARN,
            "commit state received a prepare unknow response, discard msg",
            K(state),
            K(status),
            "context",
            *this);
      } else if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
        // submit commit request again, even though coordinator had submitted it before
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc commit request error", KR(ret), K(partition));
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::ABORT: {
      ObPartitionArray partitions;
      // submit abort request again, when coordinator had submitted abort request before
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_ABORT_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), K(partition));
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      ObPartitionArray partitions;
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
      } else if (OB_SUCCESS != get_status_()) {
        if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_ABORT_REQUEST))) {
          TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), K(partition));
        }
      } else {
        if (batch_commit_trans_) {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
            TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), K(partition));
          }
        } else {
          if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
            TRANS_LOG(WARN, "submit 2pc commit request error", KR(ret), K(partition));
          }
        }
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state), "context", *this);
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_prepare_redo_response_raw_(
    const int64_t msg_type, const ObPartitionKey& partition, const int32_t status, const int64_t partition_state)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT:
    case Ob2PCState::PRE_PREPARE: {
      // DISCARD_MESSAGE(msg);
      break;
    }
    case Ob2PCState::PREPARE: {
      bool need_response = false;
      bool is_xa_prepared = false;
      bool is_participant_commit = false;
      if (msg_mask_set_.is_mask(partition) || !is_waiting_xa_commit_) {
        TRANS_LOG(DEBUG, "coordinator received this message before", K(partition));
      } else if (Ob2PCState::COMMIT == partition_state) {
        // protocol error: the participant state in xa prepare response should not be committed
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else if (Ob2PCState::PREPARE == partition_state) {
        // handle_2pc_prepare_response_raw_
      } else if (Ob2PCState::ABORT == partition_state) {
        (void)msg_mask_set_.mask(partition);
        set_status_(status);
        need_response = true;
        is_xa_prepared = false;
      } else {
        (void)msg_mask_set_.mask(partition);
        if (OB_SUCCESS == status) {
          // no need to update global trans version when xa prepare
          // no need to collect log id and timestamp
        } else if (OB_TRANS_STATE_UNKNOWN == status) {
          if (OB_FAIL(update_global_trans_version_(ObClockGenerator::getClock() / 2))) {
            TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
          } else {
            ++prepare_unknown_count_;
          }
        } else {
          if (OB_FAIL(update_global_trans_version_(ObClockGenerator::getClock()))) {
            TRANS_LOG(WARN, "update global transaction version error", KR(ret), "context", *this);
          } else {
            ++prepare_error_count_;
            TRANS_LOG(WARN, "prepare error", "context", *this);
          }
        }
        if (msg_mask_set_.is_all_mask()) {
          if (prepare_error_count_ > 0 || prepare_unknown_count_ > 0) {
            set_status_(OB_TRANS_ROLLBACKED);
            need_response = true;
            is_xa_prepared = false;
          } else {
            set_status_(OB_SUCCESS);
            need_response = true;
            is_xa_prepared = true;
          }
        }
#ifdef ERRSIM
        int err_switch = E(EventTable::EN_XA_PREPARE_ERROR) OB_SUCCESS;
        if (OB_SUCCESS != err_switch) {
          prepare_error_count_ = 1;
          is_xa_prepared = false;
          TRANS_LOG(INFO, "ERRSIM, xa prepare error", K(*this));
        }
#endif
      }
      if (OB_SUCCESS == ret && need_response) {
        if (is_xa_prepared) {
          if (OB_FAIL(xa_drive_after_prepare_())) {
            TRANS_LOG(WARN, "update xa inner table prepare failed", K(ret), K(*this));
          }
        } else {
          if (all_prepare_unknown_()) {
            if (1 == commit_times_) {
              ObTransStatus trans_status;
              if (OB_SUCCESS != (tmp_ret = trans_status_mgr_->get_status(trans_id_, trans_status))) {
                if (OB_ENTRY_NOT_EXIST != tmp_ret) {
                  TRANS_LOG(WARN, "get trans status failed", K(tmp_ret));
                }
                set_status_(OB_TRANS_KILLED);
              } else {
                set_status_(trans_status.get_status());
                TRANS_LOG(INFO, "get trans status success", K(trans_status));
              }
            } else {
              ObTransStatus trans_status;
              if (OB_SUCCESS != (tmp_ret = trans_status_mgr_->get_status(trans_id_, trans_status))) {
                if (OB_ENTRY_NOT_EXIST != tmp_ret) {
                  TRANS_LOG(WARN, "get trans status failed", K(tmp_ret));
                }
                set_status_(OB_TRANS_UNKNOWN);
              } else {
                set_status_(trans_status.get_status());
                TRANS_LOG(INFO, "get trans status success", K(trans_status));
              }
              need_print_trace_log_ = true;
            }
            if (OB_SUCCESS == get_status_()) {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_XA_PREPARE_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            } else {
              if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
                TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
              }
            }
            (void)unregister_timeout_task_();
            set_exiting_();
          } else {
            is_waiting_xa_commit_ = false;
            msg_mask_set_.clear_set();
            if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
              TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
            } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_PREPARE_REQUEST))) {
              TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), "context", *this);
            } else {
              // do nothing
            }
            (void)unregister_timeout_task_();
            if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
              TRANS_LOG(WARN, "register timeout handler error", KR(ret));
            }
          }
        }
      }
      break;
    }  // PREPARE
    case Ob2PCState::PRE_COMMIT: {
      ObPartitionArray partitions;
      // protocol error: current state is commit, it should not receive prepare no message
      if (OB_SUCCESS != status && OB_TRANS_STATE_UNKNOWN != status) {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else if (OB_TRANS_STATE_UNKNOWN == status) {
        TRANS_LOG(WARN,
            "commit state received a prepare unknow response, discard msg",
            K(state),
            K(status),
            "context",
            *this);
      } else {
        // discard this msg
        TRANS_LOG(
            INFO, "[XA] commit state receive xa prepare response, discard msg", K(state), K(status), "context", *this);
      }
      break;
    }
    case Ob2PCState::COMMIT: {
      ObPartitionArray partitions;
      // protocol error: current state is commit, it should not receive prepare no message
      if (OB_SUCCESS != status && OB_TRANS_STATE_UNKNOWN != status) {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else if (OB_TRANS_STATE_UNKNOWN == status) {
        TRANS_LOG(WARN,
            "commit state received a prepare unknow response, discard msg",
            K(state),
            K(status),
            "context",
            *this);
      } else if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
        // submit commit request again, even though coordinator had submitted it before
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc commit request error", KR(ret), K(partition));
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::ABORT: {
      ObPartitionArray partitions;
      // submit abort request again, when coordinator had submitted abort request before
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_ABORT_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), K(partition));
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      ObPartitionArray partitions;
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
      } else if (OB_SUCCESS != get_status_()) {
        if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_ABORT_REQUEST))) {
          TRANS_LOG(WARN, "submit 2pc abort request error", KR(ret), K(partition));
        }
      } else {
        if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
          TRANS_LOG(WARN, "submit 2pc commit request error", KR(ret), K(partition));
        }
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state), "context", *this);
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_batch_commit_succ()
{
  int ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = NULL;
  bool need_wait = false;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ts_mgr = get_ts_mgr_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KP(ts_mgr));
  } else if (OB_FAIL(ts_mgr->wait_gts_elapse(self_.get_tenant_id(), global_trans_version_, this, need_wait))) {
    TRANS_LOG(WARN, "wait gts elapse fail", KR(ret), "context", *this);
  } else if (need_wait) {
    gts_request_ts_ = ObTimeUtility::current_time();
    is_gts_waiting_ = true;
    // inc partiticpant ref
    if (OB_FAIL(partition_mgr_->get_trans_ctx_(trans_id_))) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), "context", *this);
    }
  } else {
    if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
      TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
    } else if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
      TRANS_LOG(WARN, "switch to clear fail", K(ret), "context", *this);
    } else {
      (void)unregister_timeout_task_();
      set_exiting_();
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      handle_batch_commit_succ,
      Y(ret),
      OB_ID(id1),
      need_wait,
      OB_ID(publish_version),
      global_trans_version_,
      Y_(request_id),
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObCoordTransCtx::handle_2pc_commit_response_raw_(
    const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT:
      // go through
    case Ob2PCState::PRE_PREPARE:
      // go through
    case Ob2PCState::PREPARE:
      // go through
    case Ob2PCState::PRE_COMMIT: {
      // DISCARD_MESSAGE(msg);
      break;
    }
    case Ob2PCState::COMMIT: {
      if (batch_same_leader_partitions.count() > 0) {
        (void)msg_mask_set_.multi_mask(batch_same_leader_partitions);
      } else {
        (void)msg_mask_set_.mask(partition);
      }
      TRANS_LOG(DEBUG, "handle 2pc commit response succ", K(partition), K(*this));
      if (msg_mask_set_.is_all_mask()) {
        if (enable_new_1pc_) {
          if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
            TRANS_LOG(WARN, "switch state error", K(ret), K(msg_type));
          } else if (OB_FAIL(handle_2pc_local_clear_request_())) {
            TRANS_LOG(WARN, "handle 2pc local clear request error", K(ret), K(*this));
            if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_CLEAR_REQUEST))) {
              TRANS_LOG(WARN, "submit 2pc clear request error", "ret", tmp_ret, "context", *this);
              ret = tmp_ret;
            }
          }
        } else {
          TRANS_LOG(DEBUG, "coordinator receive all commit response", "context", *this);
          if (is_xa_local_trans()) {
            int64_t affected_rows = 0;
            if (OB_FAIL(trans_service_->update_xa_state(tenant_id_,
                    ObXATransState::COMMITTED,
                    xid_,
                    false, /*not one phase*/
                    affected_rows))) {
              TRANS_LOG(WARN, "update xa trans state failed", K(ret), K(*this));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
            TRANS_LOG(WARN, "switch state error", K(ret), K(msg_type));
          } else {
#ifdef ERRSIM
            int err_switch = E(EventTable::EN_XA_COMMIT_ABORT_RESP_LOST) OB_SUCCESS;
            ;
            if (OB_SUCCESS != err_switch) {
              // do nothing
            } else
#endif
                if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
              TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
            }
            // ignore error, post 2pc request continue
            if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_CLEAR_REQUEST))) {
              TRANS_LOG(WARN, "submit 2pc clear request error", "ret", tmp_ret, "context", *this);
              ret = tmp_ret;
            }
          }
        }
      }
      break;
    }
    case Ob2PCState::ABORT: {
      if (prepare_unknown_count_ > 0) {
        // DISCARD_MESSAGE(msg);
      } else {
        // do nothing
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      ObPartitionArray partitions;
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back error", KR(ret), K(partition));
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_CLEAR_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc clear request error", K(ret), "context", *this, K(msg_type));
      } else {
        // do nothing
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", "context", *this, K(state), K(msg_type));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_commit_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  ret = handle_2pc_commit_response_raw_(msg.get_sender(), msg.get_batch_same_leader_partitions(), msg.get_msg_type());
  return ret;
}

int ObCoordTransCtx::handle_2pc_abort_response_raw_(
    const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::INIT:
      // go through
    case Ob2PCState::PRE_PREPARE:
      // go through
    case Ob2PCState::PREPARE: {
      // DISCARD_MESSAGE(msg);
      break;
    }
    // coordinator should not receive abort response when it had submitted commit request
    case Ob2PCState::PRE_COMMIT:
      // go through
    case Ob2PCState::COMMIT: {
      protocol_error(state, msg_type);
      ret = OB_TRANS_PROTOCOL_ERROR;
      break;
    }
    // coordinator store abort response to message tag and
    // submit clear request after collecting all abort response
    case Ob2PCState::ABORT: {
      if (batch_same_leader_partitions.count() > 0) {
        (void)msg_mask_set_.multi_mask(batch_same_leader_partitions);
      } else {
        (void)msg_mask_set_.mask(partition);
      }
      if (msg_mask_set_.is_all_mask()) {
        TRANS_LOG(DEBUG, "coordinator receive all abort response", "context", *this);
        int64_t affected_rows = 0;
        if (is_xa_local_trans() && OB_FAIL(trans_service_->update_xa_state(tenant_id_,
                                       ObXATransState::ROLLBACKED,
                                       xid_,
                                       false, /*not one phase*/
                                       affected_rows))) {
          TRANS_LOG(WARN, "update xa trans state failed", K(ret), K(*this));
        } else {
          if (all_prepare_unknown_()) {
            set_status_(OB_TRANS_UNKNOWN);
          }
          if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
            TRANS_LOG(WARN, "switch state error", K(ret), K(msg_type));
          } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_CLEAR_REQUEST))) {
            TRANS_LOG(WARN, "submit 2pc clear request error", K(ret), "context", *this);
          } else {
            // do nothing
          }
        }
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      ObPartitionArray partitions;
      if (OB_FAIL(partitions.push_back(partition))) {
        TRANS_LOG(WARN, "push back error", KR(ret), K(partition));
      } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_CLEAR_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc clear request error", KR(ret), "context", *this);
      } else {
        // do nothing
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state), K(msg_type), "context", *this);
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_abort_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  ret = handle_2pc_abort_response_raw_(msg.get_sender(), msg.get_batch_same_leader_partitions(), msg.get_msg_type());
  return ret;
}

int ObCoordTransCtx::handle_2pc_clear_response_raw_(
    const ObPartitionKey& partition, const ObPartitionArray* batch_partitions)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();

  switch (state) {
    case Ob2PCState::INIT:
      // go through
    case Ob2PCState::PRE_PREPARE:
    // go through
    case Ob2PCState::PREPARE:
    // go through:
    case Ob2PCState::PRE_COMMIT:
    // go through
    case Ob2PCState::COMMIT:
    // go through
    case Ob2PCState::ABORT: {
      TRANS_LOG(WARN, "discard this msg", K(partition), "context", *this);
      break;
    }
    case Ob2PCState::CLEAR: {
      if (batch_partitions != NULL && batch_partitions->count() > 0) {
        (void)msg_mask_set_.multi_mask(*batch_partitions);
      } else {
        (void)msg_mask_set_.mask(partition);
      }
      if (msg_mask_set_.is_all_mask()) {
        (void)unregister_timeout_task_();
        if (batch_commit_trans_ && !already_response_) {
          if (OB_SUCCESS != status_) {
            TRANS_LOG(ERROR, "unexpected status", K(partition), "context", *this);
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        }
        // release ref and release participant context memory
        clear_part_ctx_arr_();
        set_exiting_();
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "unknown 2pc state", K(state));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_pre_commit_response_raw_(
    const ObPartitionKey& partition, const int64_t commit_version, const ObPartitionArray& batch_same_leader_partitions)
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  const int64_t state = get_state_();

  if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(commit_version <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(commit_version), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (state) {
      case Ob2PCState::INIT:
      // go through
      case Ob2PCState::PRE_PREPARE:
      // go through
      case Ob2PCState::PREPARE:
      // go through
      case Ob2PCState::CLEAR: {
        TRANS_LOG(WARN, "discard this msg", K(partition), K(commit_version), "context", *this);
        break;
      }
      case Ob2PCState::PRE_COMMIT: {
        if (batch_same_leader_partitions.count() > 0) {
          (void)msg_mask_set_.multi_mask(batch_same_leader_partitions);
        } else {
          (void)msg_mask_set_.mask(partition);
        }
        if (msg_mask_set_.is_all_mask()) {
          // cannot ignore retcode
          if (!is_dup_table_trans_ &&
              OB_SUCCESS != (tmp_ret = check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "check and response scheduler error", KR(ret), "context", *this);
          }
          if (batch_commit_trans_) {
            if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
              TRANS_LOG(WARN, "switch state error", KR(ret), K(partition), K(commit_version), "context", *this);
            } else if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
              TRANS_LOG(WARN, "submit 2pc commit clear request error", "ret", tmp_ret);
              ret = tmp_ret;
            } else {
              // do nothing
            }
          } else if (OB_FAIL(switch_state_(Ob2PCState::COMMIT))) {
            TRANS_LOG(WARN, "switch state error", KR(ret), K(partition), K(commit_version), "context", *this);
          } else if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
            TRANS_LOG(WARN, "submit 2pc commit request error", "ret", tmp_ret);
            ret = tmp_ret;
          } else {
            // do nothing
          }
        }  // is_all_mask
        break;
      }
      case Ob2PCState::COMMIT: {
        ObPartitionArray partitions;
        if (commit_version != global_trans_version_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected commit version", KR(ret), K(partition), K(commit_version), "context", *this);
        } else if (OB_FAIL(partitions.push_back(partition))) {
          TRANS_LOG(WARN, "push back partition error", KR(ret), K(partition));
        } else if (OB_FAIL(post_2pc_request_(partitions, OB_TRANS_2PC_COMMIT_REQUEST))) {
          TRANS_LOG(WARN, "submit 2pc commit request error", KR(ret), K(partition));
        } else {
          // do nothing
        }
        break;
      }
      case Ob2PCState::ABORT: {
        if (prepare_unknown_count_ > 0) {
          TRANS_LOG(WARN, "discard this msg", K(partition), K(commit_version), "context", *this);
        } else {
          // do nothing
        }
        break;
      }
      default: {
        TRANS_LOG(WARN, "unknown 2pc state", K(state), K(partition), K(commit_version), "context", *this);
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_pre_commit_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& partition = msg.get_sender();
  const int64_t commit_version = msg.get_trans_version();

  if (OB_FAIL(handle_2pc_pre_commit_response_raw_(partition, commit_version, msg.get_batch_same_leader_partitions()))) {
    TRANS_LOG(WARN, "handle 2pc pre commit response raw error");
  }

  return ret;
}

////////////////local transaction optimization//////////////////////
int ObCoordTransCtx::construct_local_context(
    const ObAddr& scheduler, const ObPartitionKey& coordinator, const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  // principle :
  // scheduler call coordinator method with lock timeout
  // coordinator call scheduler with spinlock
  if (OB_FAIL(lock_.lock(100 /*100 us*/))) {
    TRANS_LOG(WARN, "handle local commit request error", K(ret), K(scheduler), K(coordinator), K(participants));
  } else {
    CtxLockGuard guard(lock_, false);
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObCoordTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(set_scheduler_(scheduler))) {
      TRANS_LOG(WARN, "set scheduler error", K(ret), "scheduler", scheduler);
    } else if (OB_FAIL(set_coordinator_(coordinator))) {
      TRANS_LOG(WARN, "set coordinator error", K(ret), "coordinator", coordinator);
    } else if (OB_FAIL(set_participants_(participants))) {
      TRANS_LOG(WARN, "set participants error", K(ret), "participants", participants);
    } else if (OB_FAIL(msg_mask_set_.init(participants))) {
      TRANS_LOG(WARN, "init message mask set error", K(ret), "participants", participants);
    } else {
      const int64_t usec_per_sec = 1000 * 1000;
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_ + trans_id_.hash() % usec_per_sec))) {
        TRANS_LOG(WARN, "register timeout task error", KR(ret), K_(self), K_(trans_id));
      }
    }
    if (OB_FAIL(ret)) {
      set_exiting_();
    }
    REC_TRANS_TRACE_EXT(tlog_, cons_context, OB_ID(ret), ret, OB_ID(read_only), is_readonly_);
  }

  return ret;
}

int ObCoordTransCtx::handle_local_commit(const bool is_dup_table_trans, const int64_t commit_times)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_.lock(100 /*100 us*/))) {
    TRANS_LOG(WARN, "handle local commit request error", K(ret), K(is_dup_table_trans), K(commit_times));
  } else {
    CtxLockGuard guard(lock_, false);

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObCoordTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(is_exiting_)) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (has_decided_()) {
      TRANS_LOG(DEBUG, "2pc has already been decided", "context", *this);
    } else {
      set_status_(OB_SUCCESS);
      set_state_(Ob2PCState::PREPARE);
      is_dup_table_trans_ = is_dup_table_trans;
      if (partition_mgr_->is_clog_aggregation_enabled()) {
        enable_new_1pc_ = true;
      }
      if (!enable_new_1pc_) {
        drive_();
      } else if (OB_FAIL(handle_2pc_local_prepare_action_())) {
        TRANS_LOG(WARN, "handle 2pc local prepare action error", K(ret), "contex", *this);
        enable_new_1pc_ = false;
        drive_();
      } else {
        // do nothing
      }
    }
    if (OB_FAIL(ret)) {
      need_print_trace_log_ = true;
      REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret);
    }
  }

  return ret;
}

int ObCoordTransCtx::handle_trans_request_(const ObTransLocationCache& trans_location,
    const ObStmtRollbackInfo& stmt_rollback_info, const bool is_dup_table_trans, const int64_t commit_times,
    const bool is_rollback)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = get_ts_mgr_();

  if (has_decided_()) {
    TRANS_LOG(DEBUG, "2pc has already been decided", "context", *this);
    // ignore it, do not set ret
  } else {
    const bool is_single_addr = is_single_leader(trans_location);
    ;
    // init partition_leader_arr_ when scheduler send msg to coordinator for the first time
    // for retry commit request msg, no need to update partition_leader_arr(11178446)
    if ((1 == commit_times) && (is_single_addr || trans_location.count() < 16)) {
      if (OB_FAIL(trans_location_cache_.assign(trans_location))) {
        TRANS_LOG(WARN, "transaction location cache assign error", K(ret), K(trans_location), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt_rollback_info_.assign(stmt_rollback_info))) {
        TRANS_LOG(WARN, "stmt rollback info assign failed", K(ret), K(stmt_rollback_info), K(*this));
      }
    }
    if (OB_FAIL(ret)) {
      set_status_(ret);
      // continue
    } else {
      const int status = (is_rollback ? OB_TRANS_ROLLBACKED : OB_SUCCESS);
      set_status_(status);
    }
    if (!is_readonly_) {
      set_state_(Ob2PCState::PREPARE);
    } else {
      set_state_(Ob2PCState::CLEAR);
    }
    if (1 < commit_times) {
      // coordinator context is created by participants
      if (!batch_commit_trans_ && !is_xa_local_trans()) {
        need_collect_logid_before_prepare_ = true;
      }
    }
    is_dup_table_trans_ = is_dup_table_trans;
    if (OB_SUCC(ret) && is_single_addr && can_batch_commit_()) {
      set_state_(Ob2PCState::PRE_PREPARE);
      batch_commit_trans_ = true;
    }
    if (!batch_commit_trans_) {
      if (OB_SUCCESS != (tmp_ret = drive_())) {
        TRANS_LOG(WARN, "2pc drive error", K(tmp_ret), K(is_rollback), K(*this));
        ret = tmp_ret;
      }
    } else {
      int64_t gts = 0;
      MonotonicTs receive_gts_ts;
      const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
      const MonotonicTs stc_ahead = (stc_ - MonotonicTs(GET_GTS_AHEAD_INTERVAL));
      if (OB_ISNULL(ts_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts mgr is null", KR(ret));
      } else if (OB_FAIL(ts_mgr->get_local_trans_version(
                     coordinator_.get_tenant_id(), stc_ahead, this, gts, receive_gts_ts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "coordinator get gts failed", KR(ret), K_(tenant_id), "context", *this);
          // ignore retcode
        } else if (OB_FAIL(partition_mgr_->get_trans_ctx_(trans_id_))) {
          TRANS_LOG(ERROR, "get trans ctx error", KR(ret), "context", *this);
        } else {
          is_gts_waiting_ = true;
          REC_TRANS_TRACE_EXT(tlog_, wait_get_gts, OB_ID(id), stc_.mts_, OB_ID(uref), get_uref());
        }
      } else {
        set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
        if (gts <= 0) {
          TRANS_LOG(WARN, "invalid gts, unexpected error", K(gts), "context", *this);
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(handle_2pc_pre_prepare_action_(gts))) {
          TRANS_LOG(WARN, "handle 2pc pre prepare action error", K(ret), "context", *this);
          (void)switch_state_(Ob2PCState::PREPARE);
          (void)batch_submit_log_over_(false);
          msg_mask_set_.clear_set();
          prepare_error_count_ = 0;
          prepare_unknown_count_ = 0;
          partition_log_info_arr_.reset();
          batch_commit_trans_ = false;
          drive_();
        } else {
          // do nothing
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(uref), get_uref());
  }

  return ret;
}

// batch commit transaction scenario :
// 1) all participants leader in local server
// 2) single participants
bool ObCoordTransCtx::can_batch_commit_()
{
  bool bool_ret = (OB_SUCCESS == status_ && GCONF.enable_one_phase_commit && 1 == commit_times_ &&
                   !is_dup_table_trans_ && !is_xa_local_trans());
  if (bool_ret) {
    if (trans_location_cache_.count() < participants_.count()) {
      bool_ret = false;
    }
  }
  if (bool_ret) {
    int ret = OB_SUCCESS;
    int64_t total_size = 0;
    if (OB_FAIL(get_trans_mem_total_size_(total_size))) {
      TRANS_LOG(WARN, "get trans mem total size failed", K(ret));
      // ignore ret
    } else {
      bool_ret = (total_size < MAX_ONE_PC_TRANS_SIZE);
    }
  }

  return bool_ret;
}

int ObCoordTransCtx::generate_trans_msg_(
    const int64_t msg_type, const ObPartitionKey& receiver, ObTransMsgUnion& msg_union)
{
  int ret = OB_SUCCESS;
  ObTransMsg& msg = msg_union.data_.trans_msg;

  if (OB_TRANS_2PC_PRE_PREPARE_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            get_status_(),
            request_id_,
            stc_))) {
      TRANS_LOG(WARN, "transacion 2pc pre prepare request init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_LOG_ID_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(
            tenant_id_, trans_id_, msg_type, trans_expired_time_, self_, receiver, trans_param_, addr_, request_id_))) {
      TRANS_LOG(WARN, "transacion 2pc log id request init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_PREPARE_REQUEST == msg_type) {
    bool is_xa_prepare = false;
    if (is_xa_local_trans() && is_waiting_xa_commit_) {
      is_xa_prepare = true;
    }
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            get_status_(),
            request_id_,
            stc_,
            partition_log_info_arr_,
            stmt_rollback_info_,
            trace_info_.get_app_trace_info(),
            xid_,
            is_xa_prepare))) {
      TRANS_LOG(WARN, "transacion 2pc prepare request init error", K(ret));
    }
  } else if (OB_TRANS_2PC_COMMIT_REQUEST == msg_type || OB_TRANS_2PC_PRE_COMMIT_REQUEST == msg_type) {
    // status must be OB_SUCCESS when transaction will commit;
    set_status_(OB_SUCCESS);
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            get_global_trans_version_(),
            addr_,
            request_id_,
            partition_log_info_arr_,
            get_status_()))) {
      TRANS_LOG(WARN, "transacion 2pc commit/pre-commit request init error", KR(ret), K(msg_type));
    }

    DEBUG_SYNC(BEFORE_DIST_COMMIT);
  } else if (OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg_type) {
    // status must be OB_SUCCESS when transaction will commit;
    set_status_(OB_SUCCESS);
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            get_global_trans_version_(),
            addr_,
            request_id_,
            partition_log_info_arr_,
            get_status_()))) {
      TRANS_LOG(WARN, "transacion 2pc commit clear request init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_ABORT_REQUEST == msg_type) {
    // status must not be OB_SUCCESS when transaction will commit;
    set_status_(OB_TRANS_ROLLBACKED);
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            request_id_,
            get_status_()))) {
      TRANS_LOG(WARN, "transacion 2pc abort request init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_CLEAR_REQUEST == msg_type) {
    if (OB_FAIL(msg.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            request_id_,
            get_status_()))) {
      TRANS_LOG(WARN, "transacion 2pc clear request init error", KR(ret));
    }
  } else if (OB_TRX_2PC_CLEAR_REQUEST == msg_type) {
    ObTrx2PCClearRequest& req = msg_union.data_.trx_2pc_clear_req;
    if (OB_FAIL(req.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            request_id_,
            get_status_(),
            clear_log_base_ts_))) {
      TRANS_LOG(WARN, "ObTrx2PCClearRequest init error", K(ret));
    }
  } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type) {
    ObTrx2PCAbortRequest& req = msg_union.data_.trx_2pc_abort_req;
    if (OB_FAIL(req.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            request_id_,
            get_status_()))) {
      TRANS_LOG(WARN, "ObTrx2PCAbortRequest init error", K(ret));
    }
  } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type) {
    ObTrx2PCCommitRequest& req = msg_union.data_.trx_2pc_commit_req;
    if (OB_FAIL(req.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            get_global_trans_version_(),
            addr_,
            request_id_,
            partition_log_info_arr_,
            get_status_()))) {
      TRANS_LOG(WARN, "ObTrx2PCCommitRequest init error", K(ret));
    }
  } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type) {
    ObTrx2PCPrepareRequest& req = msg_union.data_.trx_2pc_prepare_req;
    bool is_xa_prepare = false;
    if (is_xa_local_trans() && is_waiting_xa_commit_) {
      is_xa_prepare = true;
    }
    if (OB_FAIL(req.init(tenant_id_,
            trans_id_,
            msg_type,
            trans_expired_time_,
            self_,
            receiver,
            scheduler_,
            coordinator_,
            participants_,
            trans_param_,
            addr_,
            get_status_(),
            request_id_,
            stc_,
            partition_log_info_arr_,
            stmt_rollback_info_,
            trace_info_.get_app_trace_info(),
            xid_,
            is_xa_prepare))) {
      TRANS_LOG(WARN, "ObTrx2PCPrepareRequest init error", K(ret));
    } else if (!split_info_arr_.empty()) {
      ObTransSplitInfo split_info;
      if (OB_FAIL(ObTransSplitAdapter::find_corresponding_split_info(receiver, split_info_arr_, split_info))) {
        TRANS_LOG(WARN, "failed to find split info", KR(ret), K(split_info_arr_));
      } else if (split_info.is_valid() && OB_FAIL(req.set_split_info(split_info))) {
        TRANS_LOG(WARN, "failed to set split info in prepare request", KR(ret), K(split_info));
      }
    }
  } else {
    TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg_type));
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  }

  return ret;
}

int ObCoordTransCtx::handle_xa_trans_request_(const ObTransMsg& msg, const bool is_rollback)
{
  int ret = OB_SUCCESS;
  already_response_ = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!is_xa_local_trans()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xa trans request", K(msg), "context", *this);
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), "scheduler", msg.get_scheduler());
  } else {
    TRANS_LOG(INFO, "receive xa trans request", K(msg), "context", *this);
    const int64_t state = get_state_();
    switch (state) {
      case Ob2PCState::INIT:
        // go through
      case Ob2PCState::PRE_PREPARE:
        // go through
      case Ob2PCState::PREPARE: {
        DISCARD_MESSAGE(msg);
        break;
      }
      case Ob2PCState::PRE_COMMIT: {
        if (is_waiting_xa_commit_) {
          // receive xa commit/abort request
          is_waiting_xa_commit_ = false;
          if (is_rollback) {
            set_status_(OB_TRANS_ROLLBACKED);
          }
          if (OB_FAIL(switch_state_(Ob2PCState::PREPARE))) {
            TRANS_LOG(WARN, "switch state error", K(ret), K(msg));
          } else if (OB_FAIL(drive_())) {
            TRANS_LOG(WARN, "drive 2pc error", K(ret), K(msg), "context", *this);
          } else {
            // do nothing
            // TRANS_LOG(INFO, "receive xa commit/rollback request", K(msg), "context", *this);
          }
        } else {
          // repeatly receive xa commit/rollback request, do nothing
        }
        break;
      }
      case Ob2PCState::COMMIT:
        // go through
      case Ob2PCState::ABORT:
        // go through
      case Ob2PCState::CLEAR: {
        DISCARD_MESSAGE(msg);
        break;
      }
      default: {
        TRANS_LOG(WARN, "unknown 2pc state", K(state), K(msg), "context", *this);
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_local_prepare_action_()
{
  int ret = OB_SUCCESS;
  int status = OB_SUCCESS;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  generate_request_id_();
  const int64_t ctx_cache_cnt = part_ctx_arr_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
    ctx = NULL;
    if (ctx_cache_cnt > 0) {
      // the elements in part_ctx_arr_ must equal to participants_
      if (ctx_cache_cnt != participants_.count() || NULL == (ctx = part_ctx_arr_.at(i).ctx_) ||
          participants_.at(i) != ctx->get_partition()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected participant", "pkey", participants_.at(i), K(*ctx));
      }
    } else if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(
                   participants_.at(i), trans_id_, false, is_readonly_, false, false, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(*this));
    } else if (OB_FAIL(part_ctx_arr_.push_back(CtxInfo(ctx)))) {
      TRANS_LOG(WARN, "part ctx arr push back error", K(ret), K(*this));
      part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && NULL != ctx) {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(part_ctx->handle_2pc_local_prepare_request(request_id_,
              scheduler_,
              self_,
              participants_,
              trace_info_.get_app_trace_info(),
              stc_,
              status,
              false /*is_xa_prepare_*/))) {
        TRANS_LOG(WARN, "handle 2pc pre prepare request error", K(ret), "context", *this);
      }
    }
  }
  if (OB_FAIL(ret)) {
    clear_part_ctx_arr_();
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_local_prepare_response(const int64_t msg_type, const ObPartitionKey& partition,
    const int32_t status, const int64_t partition_state, const int64_t trans_version,
    const PartitionLogInfoArray& partition_log_info_arr, const int64_t prepare_log_id,
    const int64_t prepare_log_timestamp, const int64_t publish_version, const int64_t request_id,
    const int64_t need_wait_interval_us, const ObTransSplitInfo& split_info, const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;
  UNUSED(is_xa_prepare);
  // principle :
  // scheduler call coordinator method with lock timeout
  // coordinator call scheduler with spinlock
  if (OB_FAIL(lock_.lock(100 /*100 us*/))) {
    TRANS_LOG(WARN, "handle 2pc prepare response trylock error", K(ret), K(*this));
  } else {
    CtxLockGuard guard(lock_, false);
    // leader revoke
    if (is_exiting_) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "coordinator context is exiting", K(ret), K(partition), K(*this));
    } else {
      set_trans_need_wait_wrap_(MonotonicTs::current_time(), need_wait_interval_us);
      if (request_id_ != request_id) {
        TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*this));
      } else if (need_collect_logid_before_prepare_) {
        // do nothing
      } else if (OB_FAIL(handle_2pc_prepare_response_raw_(msg_type,
                     partition,
                     status,
                     partition_state,
                     trans_version,
                     partition_log_info_arr,
                     prepare_log_id,
                     prepare_log_timestamp,
                     publish_version,
                     split_info))) {
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_local_commit_request_()
{
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  const int64_t ctx_cache_cnt = part_ctx_arr_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
    ctx = NULL;
    if (ctx_cache_cnt > 0) {
      // the elements in part_ctx_arr_ must equal to participants_
      if (ctx_cache_cnt != participants_.count() || NULL == (ctx = part_ctx_arr_.at(i).ctx_) ||
          participants_.at(i) != ctx->get_partition()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected participant", "pkey", participants_.at(i), K(*ctx));
      }
    } else if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(
                   participants_.at(i), trans_id_, false, is_readonly_, false, false, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(*this));
    } else if (OB_FAIL(part_ctx_arr_.push_back(CtxInfo(ctx)))) {
      TRANS_LOG(WARN, "part ctx arr push back error", K(ret), K(*this));
      part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && NULL != ctx) {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(part_ctx->handle_2pc_local_commit_request(
              OB_TRANS_2PC_COMMIT_REQUEST, get_global_trans_version_(), partition_log_info_arr_))) {
        TRANS_LOG(WARN, "handle 2pc local commit request error", K(ret), "context", *this);
      }
    }
  }
  if (OB_FAIL(ret)) {
    clear_part_ctx_arr_();
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_local_commit_response(
    const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  // principle :
  // scheduler call coordinator method with lock timeout
  // coordinator call scheduler with spinlock
  if (OB_FAIL(lock_.lock(10000 /*100 us*/))) {
    TRANS_LOG(WARN, "handle local commit response error", K(ret), K(partition), K(*this));
  } else {
    CtxLockGuard guard(lock_, false);
    // leader revoke
    if (is_exiting_) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "coordinator context is exiting", K(ret), K(partition), K(*this));
    } else {
      ret = handle_2pc_commit_response_raw_(partition, batch_same_leader_partitions, msg_type);
    }
  }
  return ret;
}

int ObCoordTransCtx::handle_2pc_local_clear_request_()
{
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  const int64_t ctx_cache_cnt = part_ctx_arr_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
    ctx = NULL;
    if (ctx_cache_cnt > 0) {
      // the elements in part_ctx_arr_ must equal to participants_
      if (ctx_cache_cnt != participants_.count() || NULL == (ctx = part_ctx_arr_.at(i).ctx_) ||
          participants_.at(i) != ctx->get_partition()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected participant", "pkey", participants_.at(i), K(*ctx));
      }
    } else if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(
                   participants_.at(i), trans_id_, false, is_readonly_, false, false, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(*this));
    } else if (OB_FAIL(part_ctx_arr_.push_back(CtxInfo(ctx)))) {
      TRANS_LOG(WARN, "part ctx arr push back error", K(ret), K(*this));
      part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && NULL != ctx) {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(part_ctx->handle_2pc_local_clear_request())) {
        TRANS_LOG(WARN, "handle 2pc local commit request error", K(ret), "context", *this);
      }
    }
  }
  // for performance :
  // 1) coordinator can release self when all participants clear log submit successfully;
  // 2) all participants no need to callback coordinator context
  //    and release self immediatly when clog in majority state;
  if (OB_SUCC(ret)) {
    clear_part_ctx_arr_();
    (void)unregister_timeout_task_();
    set_exiting_();
  } else {
    clear_part_ctx_arr_();
  }

  return ret;
}

int ObCoordTransCtx::handle_xa_trans_prepare_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (has_decided_()) {
    TRANS_LOG(DEBUG, "2pc has already been decided", "context", *this);
    if (Ob2PCState::PRE_COMMIT == get_state_()) {
      already_response_ = false;
      if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_XA_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "check and response scheduler error", "context", *this, K(ret));
      }
    } else if (Ob2PCState::ABORT == get_state_()) {
      already_response_ = false;
      if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
        TRANS_LOG(WARN, "check and response scheduler error", "context", *this, K(ret));
      }
    }
  } else {
    // init partition_leader_arr_ when scheduler send msg to coordinator for the first time
    // for retry commit request msg, no need to update partition_leader_arr(11178446)
    if ((1 == msg.get_commit_times()) && (msg.get_trans_location_cache().count() < 16)) {
      if (OB_FAIL(trans_location_cache_.assign(msg.get_trans_location_cache()))) {
        TRANS_LOG(WARN, "transaction location cache assign error", K(ret), K(msg));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt_rollback_info_.assign(msg.get_stmt_rollback_info()))) {
        TRANS_LOG(WARN, "stmt rollback info assign failed", K(ret), K(msg));
      }
    }
    if (OB_FAIL(ret)) {
      set_status_(ret);
      // continue
    } else {
      set_status_(OB_SUCCESS);
    }
    if (!is_readonly_) {
      set_state_(Ob2PCState::PREPARE);
    } else {
      set_state_(Ob2PCState::CLEAR);
    }
    is_dup_table_trans_ = msg.is_dup_table_trans();
    if (OB_SUCCESS != (tmp_ret = drive_())) {
      TRANS_LOG(WARN, "2pc drive error", K(tmp_ret), K(msg));
      ret = tmp_ret;
    } else {
      TRANS_LOG(DEBUG, "2pc drive for xa prepare success");
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(uref), get_uref());
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_local_clear_response(const ObPartitionKey& partition)
{
  CtxLockGuard guard(lock_);
  return handle_2pc_clear_response_raw_(partition, NULL);
}

int ObCoordTransCtx::handle_2pc_pre_prepare_action_(const int64_t prepare_version)
{
  int ret = OB_SUCCESS;
  const int64_t tmp_msg_type = OB_TRANS_2PC_PRE_PREPARE_RESPONSE;
  int status = OB_SUCCESS;
  int64_t prepare_log_id = OB_INVALID_ID;
  int64_t prepare_log_ts = OB_INVALID_TIMESTAMP;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  bool have_prev_trans = false;
  generate_request_id_();

  for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
    if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(participants_.at(i),
            trans_id_,
            false, /*for_replay*/
            is_readonly_,
            false, /*is_bounded_staleness_read*/
            false, /*need_completed_dirty_txn*/
            alloc,
            ctx))) {
      TRANS_LOG(WARN, "get transaction context error", KR(ret), "context", *this);
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(part_ctx->handle_2pc_pre_prepare_request(prepare_version,
              request_id_,
              scheduler_,
              self_,
              participants_,
              trace_info_.get_app_trace_info(),
              status,
              prepare_log_id,
              prepare_log_ts,
              have_prev_trans))) {
        TRANS_LOG(
            WARN, "handle 2pc pre prepare request error", KR(ret), K(prepare_version), "context", *this, K(*part_ctx));
      } else if (have_prev_trans) {
        have_prev_trans_ = have_prev_trans;
      } else {
        // do nothing
      }
      part_trans_ctx_mgr_->revert_trans_ctx(ctx);
      REC_TRANS_TRACE_EXT(tlog_,
          handle_message,
          OB_ID(ret),
          ret,
          OB_ID(msg_type),
          tmp_msg_type,
          OB_ID(sender),
          participants_.at(i),
          OB_ID(uref),
          get_uref());

      // after revert_trans_ctx
      if (OB_SUCC(ret) &&
          OB_FAIL(handle_2pc_pre_prepare_response_(participants_.at(i), status, prepare_log_id, prepare_log_ts))) {
        TRANS_LOG(WARN, "handle 2pc pre prepare response error", KR(ret), "context", *this);
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::get_trans_mem_total_size_(int64_t& total_size) const
{
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  int64_t size = 0;

  total_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < participants_.count(); ++i) {
    if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(participants_.at(i),
            trans_id_,
            false, /*for_replay*/
            is_readonly_,
            false, /*is_bounded_staleness_read*/
            false, /*need_completed_dirty_txn*/
            alloc,
            ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), "context", *this);
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(part_ctx->get_trans_mem_total_size(size))) {
        TRANS_LOG(WARN, "get trans mem total size failed", K(ret));
      } else {
        total_size += size;
      }
      part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObCoordTransCtx::post_batch_commit_request_(
    const int64_t msg_type, const ObPartitionKey& pkey, const int64_t key_index)
{
  UNUSED(key_index);
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = NULL;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  int64_t tmp_msg_type = OB_TRANS_MSG_UNKNOWN;

  if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(pkey,
          trans_id_,
          false, /*for_replay*/
          is_readonly_,
          false, /*is_bounded_staleness_read*/
          false, /*need_completed_dirty_txn*/
          alloc,
          ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", K(ret), "context", *this);
  } else {
    part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg_type) {
      if (OB_FAIL(part_ctx->handle_2pc_commit_clear_request(get_global_trans_version_()))) {
        TRANS_LOG(WARN, "handle 2pc commit clear request error", K(ret), "context", *this);
      } else if (OB_FAIL(handle_2pc_clear_response_raw_(pkey))) {
        TRANS_LOG(WARN, "handle 2pc clear response error", K(ret), "context", *this);
      } else {
        tmp_msg_type = OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE;
        TRANS_LOG(DEBUG, "handle 2pc clear response success", KR(ret), K(pkey), "context", *this);
      }
    }
    part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    REC_TRANS_TRACE_EXT(tlog_,
        handle_message,
        OB_ID(ret),
        ret,
        OB_ID(msg_type),
        tmp_msg_type,
        OB_ID(sender),
        pkey,
        OB_ID(uref),
        get_uref());
  }

  return ret;
}

int ObCoordTransCtx::batch_post_2pc_request_(const ObPartitionArray& partitions, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  bool partition_exist = false;
  ObTransMsgUnion msg_union(msg_type);

  // construct trans msg
  if (OB_FAIL(generate_trans_msg_(msg_type, partitions.at(0), msg_union))) {
    TRANS_LOG(WARN, "generate transaction msg error", K(ret), K(msg_type), "context", *this);
  }
  if (!same_leader_partitions_mgr_.is_ready()) {
    for (int64_t i = 0; i < partitions.count() && OB_SUCC(ret); i++) {
      ObAddr leader;
      if (OB_FAIL(get_trans_location_leader_(partitions.at(i), true, leader, partition_exist))) {
        TRANS_LOG(WARN, "get partition leader error", K(ret), K(partitions.at(i)), K(*this));
        if (!partition_exist) {
          msg_union.set_receiver(partitions.at(i));
          (void)handle_partition_not_exist_(partitions.at(i), msg_union, msg_type);
        }
      } else if (OB_FAIL(same_leader_partitions_mgr_.push(leader, partitions.at(i)))) {
        TRANS_LOG(WARN, "push error", K(ret), K(partitions), K(*this));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      same_leader_partitions_mgr_.set_ready();
    }
  }

  // send batch rpc
  if (OB_SUCC(ret)) {
    ObTransMsg& msg = msg_union.data_.trans_msg;
    for (int64_t i = 0; OB_SUCC(ret) && i < same_leader_partitions_mgr_.count(); i++) {
      const ObPartitionArray& tmp_arr = same_leader_partitions_mgr_.at(i).get_partition_arr();
      const ObAddr& addr = same_leader_partitions_mgr_.at(i).get_leader();
      uint64_t tenant_id = 0;
      if (is_inner_table(tmp_arr.at(0).get_table_id())) {
        tenant_id = OB_SYS_TENANT_ID;
      } else {
        tenant_id = tmp_arr.at(0).get_tenant_id();
      }
      msg_union.set_receiver(tmp_arr.at(0));
      if (tmp_arr.count() > 1) {
        if (OB_FAIL(msg_union.set_batch_same_leader_partitions(tmp_arr, 0))) {
          TRANS_LOG(WARN, "set batch same leader partitions error", K(ret), K(*this));
        }
      } else {
        msg_union.reset_batch_same_leader_partitions();
      }
      if (OB_SUCC(ret)) {
        if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
          if (OB_FAIL(post_trans_msg_(tenant_id, addr, msg_union.data_.trx_msg_base, msg_type))) {
            TRANS_LOG(WARN, "post trans msg error", K(ret), K(msg_union.data_.trx_msg_base));
          }
        } else if (OB_FAIL(post_trans_msg_(tenant_id, addr, msg_union.data_.trans_msg, msg_type))) {
          TRANS_LOG(WARN, "post trans msg error", K(ret), K(msg_union.data_.trans_msg));
        }
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::post_2pc_request_(const ObPartitionArray& partitions, const int64_t raw_msg_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionArray* pptr = const_cast<ObPartitionArray*>(&partitions);
  const bool nonblock = true;
  bool partition_exist = false;
  ObAddr dst_server;
  bool need_rpc = true;
  // switch old msg type to new msg type
  int64_t msg_type = msg_type_switch_(raw_msg_type);

  if (OB_UNLIKELY(partitions.count() <= 0) ||
      (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type) && !ObTransMsgType2Checker::is_valid_msg_type(msg_type))) {
    TRANS_LOG(WARN, "invalid argument", K(partitions), K(msg_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (!need_refresh_location_ && partitions.count() > SAME_LEADER_PARTITION_BATCH_RPC_THRESHOLD &&
             cluster_version_after_2230_() && Ob2PCState::PRE_COMMIT != state_.get_state() && !enable_new_1pc_ &&
             !(batch_commit_trans_ && OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg_type)) {
    generate_request_id_();
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = batch_post_2pc_request_(partitions, msg_type)))) {
      TRANS_LOG(WARN, "batch post 2pc request error", K(tmp_ret), K(partitions), K(msg_type), K(*this));
    }
  } else {
    if (same_leader_partitions_mgr_.is_ready()) {
      same_leader_partitions_mgr_.reset();
    }
    generate_request_id_();
    ObTransMsgUnion msg_union(msg_type);
    bool need_construct_msg = true;
    int64_t i = -1;
    for (ObPartitionArray::iterator it = pptr->begin(); OB_SUCC(ret) && it != pptr->end(); it++) {
      i++;
      need_rpc = true;
      // no overwrite retcode
      if (batch_commit_trans_ && OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg_type) {
        if (OB_SUCCESS != (tmp_ret = post_batch_commit_request_(msg_type, *it, i))) {
          TRANS_LOG(WARN, "post batch commit request error", K(tmp_ret), K(msg_type), "context", *this);
        } else {
          need_rpc = false;
        }
      }
      if (need_rpc) {
        if (need_construct_msg) {
          if (OB_FAIL(generate_trans_msg_(msg_type, *it, msg_union))) {
            TRANS_LOG(WARN, "generate transaction msg error", K(ret), K(msg_type), "context", *this);
          } else {
            need_construct_msg = false;
          }
        } else {
          msg_union.set_receiver(*it);
        }
        if (OB_SUCC(ret)) {
          // no overwrite retcode
          if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
            if (OB_UNLIKELY(
                    OB_SUCCESS != (tmp_ret = post_trans_msg_(
                                       *it, msg_union.data_.trx_msg_base, msg_type, nonblock, partition_exist)))) {
              TRANS_LOG(WARN, "post trans msg error", K(tmp_ret), K(*it), K(msg_union.data_.trx_msg_base));
              if (!partition_exist) {
                (void)handle_partition_not_exist_(*it, msg_union, msg_type);
              }
              if (OB_PARTITION_NOT_EXIST != tmp_ret && OB_LOCATION_LEADER_NOT_EXIST != tmp_ret &&
                  OB_LOCATION_NOT_EXIST != tmp_ret) {
                ret = tmp_ret;
                break;
              }
            } else {
              TRANS_LOG(DEBUG, "post trans msg succ", K(*it), K(msg_union.data_.trx_msg_base), K(*this));
            }
          } else if (OB_SUCCESS !=
                     (tmp_ret = post_trans_msg_(*it, msg_union.data_.trans_msg, msg_type, nonblock, partition_exist))) {
            const ObTransMsg& msg = msg_union.data_.trans_msg;
            if (!partition_exist) {
              TRANS_LOG(WARN,
                  "post transaction message error",
                  "ret",
                  tmp_ret,
                  K(partition_exist),
                  "receiver",
                  msg.get_receiver(),
                  "msg_type",
                  msg.get_msg_type(),
                  "request_id",
                  msg.get_request_id());
              (void)handle_partition_not_exist_(*it, msg_union, msg_type);
            } else if (EXECUTE_COUNT_PER_SEC(1)) {
              TRANS_LOG(WARN,
                  "post transaction message error",
                  "ret",
                  tmp_ret,
                  "receiver",
                  msg.get_receiver(),
                  "msg_type",
                  msg.get_msg_type(),
                  "request_id",
                  msg.get_request_id());
            }
            if (OB_PARTITION_NOT_EXIST != tmp_ret && OB_LOCATION_LEADER_NOT_EXIST != tmp_ret &&
                OB_LOCATION_NOT_EXIST != tmp_ret) {
              ret = tmp_ret;
              break;
            }
          } else {
            // do nothing
          }
        }
      }
    }
  }

  return ret;
}

int ObCoordTransCtx::post_trans_response_(const ObAddr& server, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransMsg msg;

  if (OB_UNLIKELY(!server.is_valid()) || OB_UNLIKELY(!ObTransMsgTypeChecker::is_valid_msg_type(msg_type))) {
    TRANS_LOG(WARN, "invalid argument", K(server), K(msg_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if ((batch_commit_trans_ || enable_new_1pc_) && trans_id_.get_server() == addr_ && !is_xa_local_trans()) {
    ObScheTransCtxMgr& sche_ctx_mgr = trans_service_->get_sche_trans_ctx_mgr();
    ObTransCtx* ctx = NULL;
    bool alloc = false;
    if (OB_SUCCESS !=
        (tmp_ret = sche_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id_, false, is_readonly_, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), "context", *this);
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(ERROR, "transaction context is null", K_(trans_id), "context", *this);
    } else {
      ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
      if (OB_SUCCESS != (tmp_ret = sche_ctx->handle_trans_response(
                             msg_type, get_status_(), self_, addr_, get_remaining_wait_interval_us_()))) {
        TRANS_LOG(WARN, "handle local transaction response error", K(tmp_ret), "context", *this);
      }
      (void)sche_ctx_mgr.revert_trans_ctx(ctx);
    }
  } else if (OB_FAIL(msg.init(tenant_id_,
                 trans_id_,
                 msg_type,
                 trans_expired_time_,
                 self_,
                 SCHE_PARTITION_ID,
                 trans_param_,
                 addr_,
                 get_status_(),
                 commit_times_,
                 get_remaining_wait_interval_us_()))) {
    TRANS_LOG(WARN, "message init error", KR(ret), K(server), K(msg_type));
  } else if (OB_FAIL(post_trans_msg_(tenant_id_, server, msg, msg_type))) {
    TRANS_LOG(WARN, "post transaction message error", KR(ret), K(server), K(msg));
  } else {
    // do nothing
  }

  return ret;
}

int ObCoordTransCtx::handle_partition_not_exist_(
    const ObPartitionKey& pkey, const ObTransMsgUnion& msg_union, const int64_t type)
{
  int ret = OB_SUCCESS;
  ObTransMsgUnion ret_msg_union(type);
  ObTransMsg& ret_msg = ret_msg_union.data_.trans_msg;
  if (ObTransMsgType2Checker::is_valid_msg_type(type)) {
    if (OB_FAIL(trans_service_->get_trans_msg_handler().orphan_msg_handle(msg_union.data_.trx_msg_base, type))) {
      TRANS_LOG(WARN, "orphan msg handle error", K(ret), K(type), K(msg_union.data_.trx_msg_base));
    }
  } else if (OB_FAIL(trans_service_->handle_coordinator_orphan_msg(msg_union.data_.trans_msg, ret_msg))) {
    TRANS_LOG(WARN, "handle coordinator orphan msg failed", K(ret), K(pkey), K(msg_union.data_.trans_msg), K(type));
  } else {
    LocalTask* task = NULL;
    if (NULL == (task = LocalTaskFactory::alloc(type))) {
      TRANS_LOG(WARN, "alloc local task error");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      task->set_msg(ret_msg);
      task_list_.add_last(task);
    }
  }
  if (OB_SUCCESS == ret) {
    TRANS_LOG(INFO, "handle partition not exist success", K(pkey), K(type), K(*this));
  } else {
    TRANS_LOG(WARN, "handle partition not exist failed", K(ret), K(pkey), K(type), K(*this));
  }
  return ret;
}

bool ObCoordTransCtx::is_pre_preparing_() const
{
  return (state_.get_state() == Ob2PCState::PRE_PREPARE);
}

int ObCoordTransCtx::get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts)
{
  int ret = OB_SUCCESS;

  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObCoordTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      need_revert_ctx = true;
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (OB_UNLIKELY(!is_gts_waiting_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transaction is not waiting gts", KR(ret), "context", *this);
      ob_abort();
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "get gts callback", K(srr), K(gts), "context", *this);
      }
      if (srr < stc_) {
        ret = OB_EAGAIN;
      } else {
        set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
        is_gts_waiting_ = false;
        if (OB_FAIL(handle_2pc_pre_prepare_action_(gts))) {
          TRANS_LOG(WARN, "handle 2pc pre prepare action error", K(ret), K(srr), K(gts), "context", *this);
          ret = OB_SUCCESS;
          (void)switch_state_(Ob2PCState::PREPARE);
          (void)batch_submit_log_over_(false);
          msg_mask_set_.clear_set();
          prepare_error_count_ = 0;
          prepare_unknown_count_ = 0;
          partition_log_info_arr_.reset();
          batch_commit_trans_ = false;
          drive_();
        }
        need_revert_ctx = true;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, get_gts_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts), OB_ID(uref), get_uref());
  }

  if (need_revert_ctx) {
    (void)partition_mgr_->revert_trans_ctx(this);
  }

  return ret;
}
int ObCoordTransCtx::gts_elapse_callback(const MonotonicTs srr, const int64_t gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  bool need_revert_ctx = false;
  {
    CtxLockGuard guard(lock_);
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
    } else if (OB_UNLIKELY(!is_gts_waiting_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transaction is not waiting gts", KR(ret), "context", *this);
      ob_abort();
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "gts elapse callback", K(srr), K(gts), "context", *this);
      }
      if (global_trans_version_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "global_trans_version <= 0", KR(ret), K(srr), K(gts), "context", *this);
      } else if (global_trans_version_ > gts) {
        ret = OB_EAGAIN;
      } else {
        if (enable_new_1pc_) {
          if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
          } else if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
            TRANS_LOG(WARN, "switch to pre_commit fail", K(ret), "context", *this);
          } else if (OB_FAIL(handle_2pc_local_commit_request_())) {
            TRANS_LOG(WARN, "handle 2pc local commit request error", K(ret), K(*this));
            if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
              ret = tmp_ret;
              TRANS_LOG(WARN, "post pre-commit request to participants fail", KR(ret), K(participants_));
            }
          } else {
            // do nothing
          }
        } else if (batch_commit_trans_) {
          if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
          } else if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
            TRANS_LOG(WARN, "switch to pre_commit fail", K(ret), "context", *this);
          } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST))) {
            TRANS_LOG(WARN, "post pre-commit request to participants fail", KR(ret), K(participants_));
          } else {
            // do nothing
          }
        } else {
          if (OB_FAIL(switch_state_(Ob2PCState::PRE_COMMIT))) {
            TRANS_LOG(WARN, "switch to pre_commit fail", KR(ret), "context", *this);
          } else {
            int tmp_ret = OB_SUCCESS;
            ObPartitionArray negative_participants;
            if ((participants_.count() != unconfirmed_participants_.count()) ||
                (participants_.count() != participant_publish_version_array_.count())) {
              if (OB_FAIL(negative_participants.assign(participants_))) {
                TRANS_LOG(WARN, "assign participants error", K(ret), K_(participants), K(negative_participants));
              } else {
                // do nothing
              }
            } else {
              for (int64_t i = 0; i < unconfirmed_participants_.count(); i++) {
                if (global_trans_version_ > participant_publish_version_array_.at(i)) {
                  if (OB_SUCCESS != (tmp_ret = negative_participants.push_back(unconfirmed_participants_.at(i)))) {
                    TRANS_LOG(WARN,
                        "push participant error",
                        K(tmp_ret),
                        K(negative_participants),
                        K(unconfirmed_participants_.at(i)));
                  }
                } else {
                  (void)msg_mask_set_.mask(unconfirmed_participants_.at(i));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (negative_participants.count() != 0) {
                if (OB_FAIL(post_2pc_request_(negative_participants, OB_TRANS_2PC_PRE_COMMIT_REQUEST))) {
                  TRANS_LOG(WARN,
                      "post pre-commit request to participants fail",
                      K(ret),
                      K(negative_participants),
                      K_(unconfirmed_participants),
                      K_(participant_publish_version_array));
                }
              } else if (msg_mask_set_.is_all_mask()) {
                if (OB_FAIL(switch_state_(Ob2PCState::COMMIT))) {
                  TRANS_LOG(WARN, "switch to commit fail", K(ret), "context", *this);
                } else if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
                  TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
                } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_COMMIT_REQUEST))) {
                  TRANS_LOG(WARN, "post commit request to participants fail", K(ret), K(participants_));
                } else {
                  // do nothing
                }
              }
            }
          }
        }
        is_gts_waiting_ = false;
        need_revert_ctx = true;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, gts_elapse_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts), OB_ID(uref), get_uref());
  }

  if (need_revert_ctx) {
    (void)partition_mgr_->revert_trans_ctx(this);
  }

  return ret;
}

int ObCoordTransCtx::handle_2pc_response(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTrxMsgBase* message = const_cast<ObTrxMsgBase*>(&msg);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_TRX_2PC_CLEAR_RESPONSE == msg_type) {
    ObTrx2PCClearResponse* res = static_cast<ObTrx2PCClearResponse*>(message);
    if (need_collect_logid_before_prepare_) {
      // do nothing
    } else if (request_id_ != res->request_id_) {
      TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*res));
      // do not set retcode
    } else if (OB_FAIL(handle_2pc_clear_response_raw_(res->sender_, &res->batch_same_leader_partitions_))) {
      TRANS_LOG(WARN, "handle response failed", K(ret), K(msg_type), K(*this));
    } else {
      TRANS_LOG(DEBUG, "handle response succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_ABORT_RESPONSE == msg_type) {
    ObTrx2PCAbortResponse* res = static_cast<ObTrx2PCAbortResponse*>(message);
    if (need_collect_logid_before_prepare_) {
      // do nothing
    } else if (request_id_ != res->request_id_) {
      TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*res));
      // do not set retcode
    } else if (OB_FAIL(
                   handle_2pc_abort_response_raw_(res->sender_, res->batch_same_leader_partitions_, res->msg_type_))) {
      TRANS_LOG(WARN, "handle response failed", K(ret), K(msg_type), K(*this));
    } else {
      TRANS_LOG(DEBUG, "handle response succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type) {
    ObTrx2PCCommitResponse* res = static_cast<ObTrx2PCCommitResponse*>(message);
    if (need_collect_logid_before_prepare_) {
      // do nothing
    } else if (request_id_ != res->request_id_) {
      TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*res));
      // do not set retcode
    } else if (res->commit_log_ts_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(*res));
    } else {
      update_clear_log_base_ts_(res->commit_log_ts_);
      if (OB_FAIL(handle_2pc_commit_response_raw_(res->sender_, res->batch_same_leader_partitions_, res->msg_type_))) {
        TRANS_LOG(WARN, "handle response failed", K(ret), K(msg_type), K(*this));
      } else {
        TRANS_LOG(DEBUG, "handle response succ", K(msg_type), K(*this));
      }
    }
  } else if (OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type) {
    ObTrx2PCPreCommitResponse* res = static_cast<ObTrx2PCPreCommitResponse*>(message);
    if (request_id_ != res->request_id_) {
      TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*res));
      // do not set retcode
    } else if (OB_FAIL(handle_2pc_pre_commit_response_raw_(
                   res->sender_, res->trans_version_, res->batch_same_leader_partitions_))) {
      TRANS_LOG(WARN, "handle response failed", K(ret), K(msg_type), K(*this));
    } else {
      TRANS_LOG(DEBUG, "handle response succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type) {
    ObTrx2PCPrepareResponse* res = static_cast<ObTrx2PCPrepareResponse*>(message);
    set_trans_need_wait_wrap_(MonotonicTs::current_time(), res->need_wait_interval_us_);
    // bool is_xa_prepare = false;
    if (need_collect_logid_before_prepare_) {
      // do nothing
    } else if (is_xa_local_trans() && res->is_xa_prepare_ && !is_waiting_xa_commit_) {
      // do nothing
    } else if (request_id_ != res->request_id_) {
      TRANS_LOG(DEBUG, "transaction message not latest, discard message", K_(request_id), K(*res));
      // do not set retcode
    } else {
      if (is_xa_local_trans() && res->is_xa_prepare_) {
        if (OB_FAIL(handle_2pc_prepare_redo_response_raw_(res->msg_type_, res->sender_, res->status_, res->state_))) {
          TRANS_LOG(WARN, "handle 2pc prepare redo response error", K(ret), K(msg_type), K(*this));
        }
      } else {
        if (OB_FAIL(handle_2pc_prepare_response_raw_(res->msg_type_,
                res->sender_,
                res->status_,
                res->state_,
                res->trans_version_,
                res->partition_log_info_arr_,
                res->prepare_log_id_,
                res->prepare_log_timestamp_,
                res->publish_version_,
                res->split_info_))) {
          TRANS_LOG(WARN, "handle 2pc prepare response error", K(ret), K(msg_type), K(*this));
        }
      }
    }
    if (OB_SUCC(ret)) {
      TRANS_LOG(DEBUG, "handle response succ", K(msg_type), K(*this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected msg", K(ret), K(msg_type), K(*this));
  }

  REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());

  return ret;
}

int ObCoordTransCtx::add_participant_publish_version_(const ObPartitionKey& partition, const int64_t publish_version)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition));
  } else {
    if (OB_FAIL(unconfirmed_participants_.push_back(partition))) {
      TRANS_LOG(WARN, "push partition error", KR(ret), K(partition));
    } else if (OB_FAIL(participant_publish_version_array_.push_back(publish_version))) {
      TRANS_LOG(WARN, "push participant publish version error", KR(ret), K(partition), K(publish_version));
      unconfirmed_participants_.pop_back();
    } else {
      // do nothing
    }
  }

  return ret;
}
void ObCoordTransCtx::update_clear_log_base_ts_(const int64_t log_ts)
{
  if (log_ts > clear_log_base_ts_) {
    clear_log_base_ts_ = log_ts;
  }
}

int ObCoordTransCtx::xa_drive_after_prepare_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  bool already_changed = false;

#ifdef ERRSIM
  ret = E(EventTable::EN_XA_UPDATE_COORD_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "ERRSIM, update coordinator and state failed", K(ret), K(*this));
  } else
#endif
      if (OB_FAIL(trans_service_->update_coordinator_and_state(
              tenant_id_, xid_, self_, ObXATransState::PREPARED, false /*not xa read only*/, affected_rows))) {
    TRANS_LOG(WARN, "update coordinator and state failed", K(ret), K(*this));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    // xa not exist
    int64_t state = ObXATransState::UNKNOWN;
    int64_t end_flag;
    if (OB_FAIL(trans_service_->query_xa_state_and_flag(tenant_id_, xid_, state, end_flag))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query xa record failed", K(ret), K(*this));
      }
    }
    if (OB_SUCCESS == ret) {
      // maybe coordinator crash
      already_changed = true;
    } else if (OB_ITER_END == ret) {
      set_status_(OB_TRANS_ROLLBACKED);
      is_waiting_xa_commit_ = false;
      msg_mask_set_.clear_set();
      // ignore retcode
      if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_ABORT_RESPONSE))) {
        TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
      }
      if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_PREPARE_REQUEST))) {
        TRANS_LOG(WARN, "submit 2pc abort request error", K(ret), "context", *this);
      }
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
        TRANS_LOG(WARN, "register timeout handler error", K(ret));
      }
    }
  }
  if (affected_rows > 0 || already_changed) {
    if (OB_FAIL(switch_state_(Ob2PCState::PRE_COMMIT))) {
      TRANS_LOG(WARN, "switch to pre_commit fail", K(ret), "context", *this);
    } else {
      TRANS_LOG(INFO, "[XA] reponse scheduler xa prepapre", "context", *this);
      (void)unregister_timeout_task_();
#ifdef ERRSIM
      int err_switch = E(EventTable::EN_XA_PREPARE_RESP_LOST) OB_SUCCESS;
      if (OB_SUCCESS != err_switch) {
        TRANS_LOG(INFO, "ERRSIM, xa prepare response lost", K(*this));
        already_response_ = true;
      } else
#endif
          if (OB_SUCCESS != (tmp_ret = check_and_response_scheduler_(OB_TRANS_XA_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "check and response scheduler error", "context", *this, K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObCoordTransCtx::xa_drive_after_commit_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(trans_service_->update_xa_state(tenant_id_,
          ObXATransState::COMMITTED,
          xid_,
          false, /*not one phase*/
          affected_rows))) {
    TRANS_LOG(WARN, "update xa trans state failed", K(ret), K(*this));
  } else if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
    TRANS_LOG(WARN, "switch state error", K(ret), K(*this));
  } else {
    // already_response_ = false;
    if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_COMMIT_RESPONSE))) {
      TRANS_LOG(WARN, "check and response scheduler error", K(ret), "context", *this);
    }
    // ignore error, post 2pc request continue
    if (OB_SUCCESS != (tmp_ret = post_2pc_request_(participants_, OB_TRANS_2PC_CLEAR_REQUEST))) {
      TRANS_LOG(WARN, "submit 2pc clear request error", "ret", tmp_ret, "context", *this);
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObCoordTransCtx::xa_drive_after_rollback_()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(trans_service_->update_xa_state(tenant_id_,
          ObXATransState::ROLLBACKED,
          xid_,
          false, /*not one phase*/
          affected_rows))) {
    TRANS_LOG(WARN, "update xa trans state failed", K(ret), K(*this));
  } else {
    // already_response_ = false;
    // if (OB_FAIL(check_and_response_scheduler_(OB_TRANS_XA_ROLLBACK_RESPONSE))) {
    //   TRANS_LOG(WARN, "response scheduler xa rollback failed", K(ret), K(*this));
    // }
    if (all_prepare_unknown_()) {
      set_status_(OB_TRANS_UNKNOWN);
    }
    if (OB_FAIL(switch_state_(Ob2PCState::CLEAR))) {
      TRANS_LOG(WARN, "switch state error", K(ret), K(*this));
    } else if (OB_FAIL(post_2pc_request_(participants_, OB_TRANS_2PC_CLEAR_REQUEST))) {
      TRANS_LOG(WARN, "submit 2pc clear request error", K(ret), "context", *this);
    } else {
      // do nothing
    }
  }
  return ret;
}

void ObCoordTransCtx::DEBUG_SYNC_slow_txn_during_2pc_commit_phase_for_physical_backup_1055_()
{
  ObPartitionKey mock_key;
  mock_key.init(1100611139453782, 0, 0);

  if (mock_key == self_) {
    DEBUG_SYNC(SLOW_TXN_DURING_2PC_COMMIT_PHASE_FOR_PHYSICAL_BACKUP_1055);
  }
}

}  // namespace transaction
}  // namespace oceanbase
