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

#include "ob_xa_rpc.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "storage/ob_partition_service.h"
//#include "sql/ob_end_trans_callback.h"
#include "sql/ob_sql_trans_control.h"

namespace oceanbase {

using namespace transaction;
using namespace common;

namespace obrpc {
OB_SERIALIZE_MEMBER(ObXAPrepareRPCRequest, trans_id_, xid_, stmt_timeout_);
OB_SERIALIZE_MEMBER(ObXAEndTransRPCRequest, trans_id_, xid_, is_rollback_, is_terminated_);
OB_SERIALIZE_MEMBER(ObXASyncStatusRPCRequest, trans_id_, xid_, sender_, is_new_branch_, is_stmt_pull_,
    is_tightly_coupled_, pull_trans_desc_, timeout_seconds_);
OB_SERIALIZE_MEMBER(ObXASyncStatusRPCResponse, trans_desc_, is_stmt_pull_);
OB_SERIALIZE_MEMBER(ObXAMergeStatusRPCRequest, trans_desc_, is_stmt_push_, is_tightly_coupled_, xid_, seq_no_);
OB_SERIALIZE_MEMBER(ObXAHbRequest, trans_id_, xid_, sender_);

int ObXAPrepareRPCRequest::init(const ObTransID& trans_id, const ObXATransID& xid, const int64_t stmt_timeout)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(xid));
  } else {
    trans_id_ = trans_id;
    xid_ = xid;
    stmt_timeout_ = stmt_timeout;
  }
  return ret;
}

int ObXAEndTransRPCRequest::init(
    const ObTransID& trans_id, const ObXATransID& xid, const bool is_rollback, const bool is_terminated /*false*/)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(xid), K(is_rollback));
  } else {
    trans_id_ = trans_id;
    xid_ = xid;
    is_rollback_ = is_rollback;
    is_terminated_ = is_terminated;
  }
  return ret;
}

int ObXASyncStatusRPCRequest::init(const ObTransID& trans_id, const ObXATransID& xid, const ObAddr& sender,
    const bool is_new_branch, const bool is_stmt_pull, const bool is_tightly_coupled, const int64_t timeout_seconds)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid() || !sender.is_valid() || 0 > timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(xid), K(sender), K(timeout_seconds));
  } else {
    trans_id_ = trans_id;
    xid_ = xid;
    sender_ = sender;
    is_new_branch_ = is_new_branch;
    is_stmt_pull_ = is_stmt_pull;
    is_tightly_coupled_ = is_tightly_coupled;
    pull_trans_desc_ = true;  // false by default
    timeout_seconds_ = timeout_seconds;
  }
  return ret;
}

int ObXASyncStatusRPCResponse::init(const ObTransDesc& trans_desc, const bool is_stmt_pull)
{
  int ret = OB_SUCCESS;
  // TRANS_LOG(INFO, "", K(trans_desc));
  if (!trans_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_desc));
  } else if (OB_FAIL(trans_desc_.trans_deep_copy(trans_desc))) {
    TRANS_LOG(WARN, "deep copy trans desc failed", KR(ret));
  } else {
    is_stmt_pull_ = is_stmt_pull;
  }
  return ret;
}

int ObXAMergeStatusRPCRequest::init(const ObTransDesc& trans_desc, const bool is_stmt_push,
    const bool is_tightly_coupled, const ObXATransID& xid, const int64_t seq_no)
{
  int ret = OB_SUCCESS;
  if (!trans_desc.is_valid() || !xid.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_desc));
  } else if (OB_FAIL(trans_desc_.trans_deep_copy(trans_desc))) {
    TRANS_LOG(WARN, "deep copy trans desc failed", KR(ret));
  } else {
    is_stmt_push_ = is_stmt_push;
    is_tightly_coupled_ = is_tightly_coupled;
    xid_ = xid;
    seq_no_ = seq_no;
  }
  return ret;
}

int ObXAHbRequest::init(const ObTransID& trans_id, const ObXATransID& xid, const ObAddr& sender)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid() || !xid.is_valid() || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(xid), K(sender));
  } else {
    trans_id_ = trans_id;
    xid_ = xid;
    sender_ = sender;
  }
  return ret;
}

int ObXAPrepareP::process()
{
  // TODO, get sche_ctx and trans_desc according to trans id
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  bool alloc = false;
  const bool is_readonly = false;
  const bool for_replay = false;
  const ObTransID& trans_id = arg_.get_trans_id();
  const ObXATransID xid = arg_.get_xid();
  ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = trans_service->get_sche_trans_ctx_mgr();

  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(xid), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
  } else {
    sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    // TODO, verify timeout
    int64_t stmt_expired_time = ObClockGenerator::getClock() + arg_.get_stmt_timeout();
    if (OB_FAIL(trans_service->local_xa_prepare(xid, stmt_expired_time, sche_ctx))) {
      TRANS_LOG(WARN, "xa prepare failed", K(ret), K(xid));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXAEndTransP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  const ObXATransID xid = arg_.get_xid();
  const bool is_rollback = arg_.get_is_rollback();
  const ObTransID trans_id = arg_.get_trans_id();
  const bool is_terminated = arg_.is_terminated();
  ObScheTransCtxMgr* sche_trans_ctx_mgr = NULL;
  ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();
  if (OB_ISNULL(trans_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", K(ret));
  } else if (OB_ISNULL(sche_trans_ctx_mgr = &trans_service->get_sche_trans_ctx_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObScheTransCtxMgr is NULL", K(ret));
  } else if (OB_FAIL(
                 sche_trans_ctx_mgr->get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id), K(xid));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
  } else {
    ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    uint64_t tenant_id = sche_ctx->get_tenant_id();
    if (!sche_ctx->is_xa_tightly_coupled()) {
      // loosely
      if (sche_ctx->get_xid() != xid) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid sche xid", K(ret), K(xid), K(sche_ctx->get_xid()));
      } else if (is_terminated) {
        // terminate request
        TRANS_LOG(INFO, "handle terminate request", K(xid), K(trans_id), K(is_rollback), K(is_terminated));
        sche_ctx->set_exiting();
      } else {
        // one phase commit/rollback request
        if (OB_FAIL(sche_ctx->xa_one_phase_end_trans(is_rollback))) {
          TRANS_LOG(WARN, "sche_ctx one phase commit/rollback failed", KR(ret), K(xid), K(trans_id));
        } else if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
          if (OB_TRANS_XA_RBROLLBACK == ret) {
            if (OB_FAIL(trans_service->update_xa_state(
                    tenant_id, ObXATransState::ROLLBACKED, xid, true /*one_phase*/, affected_rows))) {
              TRANS_LOG(WARN, "update xa state failed", K(ret), K(xid), K(trans_id), K(affected_rows));
            }
          }
          TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_id));
        } else {
          // need to update inner table for one phase commit
          if (!is_rollback) {
            if (OB_SUCCESS != (tmp_ret = trans_service->update_xa_state(
                                   tenant_id, ObXATransState::COMMITTED, xid, true /*one_phase*/, affected_rows))) {
              TRANS_LOG(WARN, "update xa state failed", K(tmp_ret), K(xid), K(trans_id), K(affected_rows));
            }
          }
        }
      }
    } else {
      // tightly
      if (sche_ctx->get_xid().get_gtrid_str() != xid.get_gtrid_str()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid sche xid", K(ret), K(xid), K(sche_ctx->get_xid()));
      } else if (sche_ctx->is_terminated() && is_rollback) {
        // OB_SUCCESS
        TRANS_LOG(INFO, "transaction has terminated, may be rollback", K(xid), K(trans_id), K(is_rollback));
      } else {
        int64_t now = ObTimeUtility::current_time();
        int64_t expired_time = now + 10000000;  // 10s
        while (OB_SUCCESS != (ret = sche_ctx->xa_try_global_lock(xid))) {
          usleep(100);
          if (ObTimeUtility::current_time() > expired_time) {
            TRANS_LOG(INFO, "no need to wait, force to terminate", K(xid), K(trans_id));
            break;
          }
        }
        if (is_rollback) {
          // one phase rollback or terminate
          if (OB_FAIL(sche_ctx->xa_rollback_session_terminate())) {
            TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(xid), K(trans_id), K(is_rollback));
          } else {
            TRANS_LOG(INFO, "rollback xa trans success", K(xid), K(trans_id), K(is_rollback));
          }
        } else {
          if (OB_FAIL(sche_ctx->xa_one_phase_end_trans(is_rollback))) {
            TRANS_LOG(WARN, "sche_ctx one phase commit/rollback failed", KR(ret), K(xid), K(trans_id));
          } else if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
            if (OB_TRANS_XA_RBROLLBACK == ret) {
              if (OB_FAIL(trans_service->update_xa_state(
                      tenant_id, ObXATransState::ROLLBACKED, xid, true /*one_phase*/, affected_rows))) {
                TRANS_LOG(WARN, "update xa state failed", K(ret), K(xid), K(trans_id), K(affected_rows));
              }
            }
            TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_id));
          } else {
            // need to update inner table for one phase commit
            if (!is_rollback) {
              if (OB_SUCCESS != (tmp_ret = trans_service->update_xa_state(
                                     tenant_id, ObXATransState::COMMITTED, xid, true /*one_phase*/, affected_rows))) {
                TRANS_LOG(WARN, "update xa state failed", K(tmp_ret), K(xid), K(trans_id), K(affected_rows));
              }
            }
          }
        }
      }
    }
    (void)sche_trans_ctx_mgr->revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXASyncStatusP::process()
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  const ObXATransID& xid = arg_.get_xid();
  const ObTransID& trans_id = arg_.get_trans_id();
  // TODO, this variable is used to check, remove this in future
  const bool is_tightly_coupled = arg_.get_is_tightly_coupled();
  const bool is_stmt_pull = arg_.get_is_stmt_pull();
  const bool is_new_branch = arg_.get_is_new_branch();
  const ObAddr& sender = arg_.get_sender();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = global_ctx_.par_ser_->get_trans_service()->get_sche_trans_ctx_mgr();

  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id), K(xid));
  } else if (OB_ISNULL(ctx) || (NULL == (sche_ctx = static_cast<ObScheTransCtx*>(ctx)))) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
  } else if (is_tightly_coupled != sche_ctx->is_xa_tightly_coupled()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected tight couple flag", K(is_tightly_coupled), K(xid), K(trans_id));
  } else if (is_tightly_coupled) {
    if (OB_FAIL(sche_ctx->check_for_xa_execution(is_new_branch, xid))) {
      if (OB_TRANS_XA_BRANCH_FAIL == ret) {
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(trans_id));
      } else {
        TRANS_LOG(WARN, "unexpected original scheduler for xa execution", K(ret), K(xid), K(trans_id));
      }
    } else if (is_stmt_pull) {
      if (OB_FAIL(sche_ctx->update_xa_branch_hb_info(xid))) {
        TRANS_LOG(WARN, "update xa branch hb info failed", KR(ret), K(xid), K(trans_id));
      } else if (OB_SUCCESS != (ret = sche_ctx->xa_try_global_lock(xid))) {
        TRANS_LOG(INFO, "xa get global lock failed", K(ret), K(arg_));
      } else if (OB_FAIL(sche_ctx->xa_sync_status(sender, is_stmt_pull))) {
        TRANS_LOG(WARN, "xa sync status failed", K(ret), K(xid), K(trans_id));
      }
    } else {
      const int64_t timeout_seconds = arg_.get_timeout_seconds();
      if (OB_FAIL(sche_ctx->update_xa_branch_info(xid, ObXATransState::ACTIVE, sender, timeout_seconds))) {
        TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(arg_), K(xid), K(trans_id));
      } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
        TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(trans_id));
      } else if (is_new_branch && OB_FAIL(sche_ctx->add_xa_branch_count())) {
        TRANS_LOG(WARN, "add xa branch count failed", K(ret), K(xid), K(trans_id));
      }
      if (OB_SUCC(ret) && arg_.get_pull_trans_desc()) {
        if (OB_FAIL(sche_ctx->wait_xa_start_complete())) {
          TRANS_LOG(WARN, "wait xa start complete failed", K(ret), K(xid), K(trans_id));
        } else if (OB_FAIL(sche_ctx->xa_sync_status(sender, is_stmt_pull))) {
          TRANS_LOG(WARN, "xa sync status failed", K(ret), K(xid), K(trans_id));
        }
      }
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  } else {
    const int64_t timeout_seconds = arg_.get_timeout_seconds();
    if (OB_FAIL(sche_ctx->update_xa_branch_info(xid, ObXATransState::ACTIVE, sender, timeout_seconds))) {
      TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(arg_), K(xid), K(trans_id));
    } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
      TRANS_LOG(WARN, "register xa trans timeout task failed", K(ret), K(xid), K(trans_id));
    } else if (OB_FAIL(sche_ctx->xa_sync_status(sender, false))) {
      TRANS_LOG(WARN, "xa sync status failed", K(ret), K(xid), K(trans_id));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXASyncStatusResponseP::process()
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  const ObTransDesc& trans_desc = arg_.get_trans_desc();
  const ObTransID trans_id = trans_desc.get_trans_id();
  const bool is_stmt_pull = arg_.get_is_stmt_pull();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = global_ctx_.par_ser_->get_trans_service()->get_sche_trans_ctx_mgr();
  // TRANS_LOG(INFO, "", K(trans_desc));
  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
  } else {
    ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_FAIL(sche_ctx->xa_sync_status_response(trans_desc, is_stmt_pull))) {
      TRANS_LOG(WARN, "xa sync status failed", K(ret), K(trans_desc));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXAMergeStatusP::process()
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  const ObTransDesc& trans_desc = arg_.get_trans_desc();
  const ObTransID trans_id = trans_desc.get_trans_id();
  const ObXATransID& xid = arg_.get_xid();
  const bool is_stmt_push = arg_.get_is_stmt_push();
  const bool is_tightly_coupled = arg_.get_is_tightly_coupled();
  const int64_t seq_no = arg_.get_seq_no();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = global_ctx_.par_ser_->get_trans_service()->get_sche_trans_ctx_mgr();
  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
  } else {
    ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_ISNULL(sche_ctx->get_trans_desc())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans desc is null", K(ret), K(xid), K(trans_id));
    } else if (OB_FAIL(sche_ctx->check_for_xa_execution(false /*is_new_branch*/, xid))) {
      if (OB_TRANS_XA_BRANCH_FAIL == ret) {
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(trans_id));
      } else {
        TRANS_LOG(WARN, "unexpected original scheduler for xa execution", K(ret), K(xid), K(trans_id));
      }
    } else if (is_tightly_coupled) {
      // tightly coupled
      if (is_stmt_push) {
        // TODO, the failure of updating branch hb info may not impact the next operations
        if (OB_FAIL(sche_ctx->xa_end_stmt(xid, trans_desc, true, seq_no))) {
          TRANS_LOG(WARN, "xa end stmt failed", K(ret), K(xid), K(trans_desc));
        } else {
          TRANS_LOG(INFO, "xa end stmt success", K(xid), K(trans_desc));
        }
      } else {
        const int64_t timeout_seconds = trans_desc.get_xa_end_timeout_seconds();
        ObAddr fake_addr;
        if (OB_FAIL(sche_ctx->update_xa_branch_info(xid, ObXATransState::IDLE, fake_addr, timeout_seconds))) {
          TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_id));
        } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
          TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(trans_desc), K(xid), K(trans_id));
        }
      }
    } else {
      // loosely coupled
      const int64_t timeout_seconds = trans_desc.get_xa_end_timeout_seconds();
      ObAddr fake_addr;
      if (OB_FAIL(sche_ctx->update_xa_branch_info(xid, ObXATransState::IDLE, fake_addr, timeout_seconds))) {
        TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_id));
      } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
        TRANS_LOG(WARN, "register xa end timeout task failed", K(ret), K(xid), K(trans_id));
      } else if (OB_FAIL(sche_ctx->xa_merge_status(trans_desc, is_stmt_push))) {
        TRANS_LOG(WARN, "xa sync status failed", K(ret), K(trans_desc));
      }
    }
    if (OB_NOT_NULL(sche_ctx->get_trans_desc())) {
      TRANS_LOG(INFO, "xa merge status", K(sche_ctx->get_trans_desc()));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXAHbReqP::process()
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  const ObTransID& trans_id = arg_.get_trans_id();
  const ObXATransID& xid = arg_.get_xid();
  const ObAddr& sender = arg_.get_sender();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = global_ctx_.par_ser_->get_trans_service()->get_sche_trans_ctx_mgr();

  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get trans ctx failed", KR(ret), K(trans_id), K(xid));
  } else {
    // response rpc
    sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_FAIL(sche_ctx->xa_hb_resp(xid, sender))) {
      TRANS_LOG(WARN, "scheduler xa hb resp failed", KR(ret), K(trans_id), K(xid));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

int ObXAHbRespP::process()
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  const ObTransID& trans_id = arg_.get_trans_id();
  const ObXATransID& xid = arg_.get_xid();
  ObScheTransCtxMgr& sche_trans_ctx_mgr = global_ctx_.par_ser_->get_trans_service()->get_sche_trans_ctx_mgr();

  if (OB_FAIL(sche_trans_ctx_mgr.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get trans ctx failed", KR(ret), K(trans_id), K(xid));
  } else {
    sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_FAIL(sche_ctx->update_xa_branch_hb_info(xid))) {
      TRANS_LOG(WARN, "update xa hb info failed", KR(ret), K(trans_id), K(xid));
    }
    (void)sche_trans_ctx_mgr.revert_trans_ctx(ctx);
  }
  result_ = Int64(ret);
  return ret;
}

}  // namespace obrpc

using namespace obrpc;

namespace transaction {

int ObXARpc::init(ObXARpcProxy* proxy, const common::ObAddr& self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObXAEndTransRPC init twice", KR(ret));
  } else if (OB_ISNULL(proxy) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(proxy), K(self));
  } else {
    rpc_proxy_ = proxy;
    self_ = self;
    is_inited_ = true;
  }
  return ret;
}

int ObXARpc::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa rpc already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "xa rpc start success");
  }
  return ret;
}

int ObXARpc::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa rpc not inited", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "xa rpc stop success");
  }
  return ret;
}

int ObXARpc::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa rpc is running", KR(ret));
  } else {
    TRANS_LOG(INFO, "xa rpc wait success");
  }
  return ret;
}

void ObXARpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG(WARN, "gts request rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG(WARN, "gts request rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_.reset();
    TRANS_LOG(INFO, "gts request rpc destroy");
  }
}

int ObXARpc::xa_prepare(const uint64_t tenant_id, const common::ObAddr& server, const ObXAPrepareRPCRequest& req,
    ObXARPCCB<OB_XA_PREPARE>& cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } /* else if (server == self_) {
     // TODO
   } */
  else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_prepare(req, &cb))) {
    TRANS_LOG(WARN, "post xa prepare rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_end_trans(const uint64_t tenant_id, const common::ObAddr& server, const ObXAEndTransRPCRequest& req,
    ObXARPCCB<OB_XA_END_TRANS>& cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } /* else if (server == self_) {
     // TODO
   } */
  else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_end_trans(req, &cb))) {
    TRANS_LOG(WARN, "post xa end_trans rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_sync_status(const uint64_t tenant_id, const ObAddr& server, const ObXASyncStatusRPCRequest& req,
    ObXARPCCB<OB_XA_SYNC_STATUS>* cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } /* else if (server == self_) {
     ret = OB_ERR_UNEXPECTED;
     TRANS_LOG(WARN, "sync local xa status", KR(ret), K(tenant_id), K(req));
   } */
  else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_sync_status(req, cb))) {
    TRANS_LOG(WARN, "post xa sync status rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_sync_status_response(const uint64_t tenant_id, const ObAddr& server,
    const ObXASyncStatusRPCResponse& resp, ObXARPCCB<OB_XA_SYNC_STATUS_RESPONSE>* cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(resp));
  } /* else if (server == self_) {
     ret = OB_ERR_UNEXPECTED;
     TRANS_LOG(WARN, "sync local xa status", KR(ret), K(tenant_id), K(resp));
   } */
  else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_sync_status_response(resp, cb))) {
    TRANS_LOG(WARN, "post xa sync status rpc response failed", KR(ret), K(tenant_id), K(server), K(resp));
  }
  return ret;
}

int ObXARpc::xa_merge_status(const uint64_t tenant_id, const ObAddr& server, const ObXAMergeStatusRPCRequest& req,
    ObXARPCCB<OB_XA_MERGE_STATUS>* cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } /* else if (server == self_) {
     ret = OB_ERR_UNEXPECTED;
     TRANS_LOG(WARN, "sync local xa status", KR(ret), K(tenant_id), K(req));
   } */
  else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_merge_status(req, cb))) {
    TRANS_LOG(WARN, "post xa sync status rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_hb_req(
    const uint64_t tenant_id, const ObAddr& server, const ObXAHbRequest& req, ObXARPCCB<OB_XA_HB_REQ>* cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (server == self_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa hb to self", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_hb_req(req, cb))) {
    TRANS_LOG(WARN, "post xa hb req failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_hb_resp(
    const uint64_t tenant_id, const ObAddr& server, const ObXAHbResponse& resp, ObXARPCCB<OB_XA_HB_RESP>* cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXAPrepareRpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXAPrepareRpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(resp));
  } else if (server == self_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa hb to self", KR(ret), K(tenant_id), K(server), K(resp));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).timeout(OB_XA_RPC_TIMEOUT).xa_hb_resp(resp, cb))) {
    TRANS_LOG(WARN, "post xa hb req failed", KR(ret), K(tenant_id), K(server), K(resp));
  }
  return ret;
}

}  // namespace transaction

}  // namespace oceanbase
