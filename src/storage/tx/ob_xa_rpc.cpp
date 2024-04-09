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
#include "ob_trans_ctx_mgr.h"
//#include "sql/ob_end_trans_callback.h"
#include "sql/ob_sql_trans_control.h"
#include "ob_xa_service.h"
#include "ob_xa_ctx.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

using namespace transaction;
using namespace common;

namespace obrpc
{
OB_SERIALIZE_MEMBER(ObXAPrepareRPCRequest,
                    tx_id_,
                    xid_,
                    timeout_us_);
OB_SERIALIZE_MEMBER(ObXAStartRPCRequest,
                    tx_id_,
                    xid_,
                    sender_,
                    is_new_branch_,
                    is_tightly_coupled_,
                    is_first_branch_,
                    timeout_seconds_,
                    flags_);
OB_SERIALIZE_MEMBER(ObXAStartRPCResponse,
                    tx_id_,
                    tx_info_,
                    is_first_branch_);
OB_SERIALIZE_MEMBER(ObXAEndRPCRequest,
                    tx_id_,
                    stmt_info_,
                    xid_,
                    is_tightly_coupled_,
                    seq_no_,
                    end_flag_);
OB_SERIALIZE_MEMBER(ObXAStartStmtRPCRequest,
                    tx_id_,
                    xid_,
                    sender_,
                    request_id_);
OB_SERIALIZE_MEMBER(ObXAStartStmtRPCResponse,
                    tx_id_,
                    stmt_info_,
                    response_id_);
OB_SERIALIZE_MEMBER(ObXAEndStmtRPCRequest,
                    tx_id_,
                    stmt_info_,
                    xid_,
                    seq_no_);
OB_SERIALIZE_MEMBER(ObXACommitRPCRequest,
                    trans_id_,
                    xid_,
                    timeout_us_,
                    request_id_);
OB_SERIALIZE_MEMBER(ObXARollbackRPCRequest,
                    tx_id_,
                    xid_,
                    timeout_us_,
                    request_id_);
OB_SERIALIZE_MEMBER(ObXATerminateRPCRequest,
                    tx_id_,
                    xid_,
                    timeout_us_);
OB_SERIALIZE_MEMBER(ObXAHbRequest,
                    tx_id_,
                    xid_,
                    sender_);
OB_SERIALIZE_MEMBER(ObXACommitRpcResult, status_, has_tx_level_temp_table_);

int ObXAPrepareRPCRequest::init(const ObTransID &tx_id,
                                const ObXATransID &xid,
                                const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    timeout_us_ = timeout_us;
  }
  return ret;
}

int ObXACommitRPCRequest::init(const ObTransID &trans_id,
                               const ObXATransID &xid,
                               const int64_t timeout_us,
                               const int64_t request_id)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(xid));
  } else {
    trans_id_ = trans_id;
    xid_ = xid;
    timeout_us_ = timeout_us;
    request_id_ = request_id;
  }
  return ret;
}

int ObXAHbRequest::init(const ObTransID &tx_id, const ObXATransID &xid, const ObAddr &sender)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !xid.is_valid() || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(sender));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    sender_ = sender;
  }
  return ret;
}

int ObXAPrepareP::process()
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = arg_.get_xid();
  const ObTransID &tx_id = arg_.get_tx_id();
  const int64_t timeout_us = arg_.get_timeout_us();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->local_xa_prepare(xid, tx_id, timeout_us))) {
    TRANS_LOG(WARN, "local xa prepare failed", K(ret), K(arg_));
  } else {
    // do nothing
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAStartRPCRequest::init(const ObTransID &tx_id,
                              const ObXATransID &xid,
                              const ObAddr &sender,
                              const bool is_new_branch,
                              const bool is_tightly_coupled,
                              const int64_t timeout_seconds,
                              const int64_t flags,
                              const bool is_first_branch)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() ||
      !sender.is_valid() ||
      0 > timeout_seconds ||
      (!ObXAFlag::is_valid(flags, ObXAReqType::XA_START))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(sender),
        K(timeout_seconds), K(flags));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    sender_ = sender;
    is_new_branch_ = is_new_branch;
    is_tightly_coupled_ = is_tightly_coupled;
    is_first_branch_ = is_first_branch;  // false by default
    timeout_seconds_ = timeout_seconds;
    flags_ = flags;
  }
  return ret;
}

bool ObXAStartRPCRequest::is_valid() const
{
  return tx_id_.is_valid() &&
         timeout_seconds_ > 0 &&
         (ObXAFlag::is_valid(flags_, ObXAReqType::XA_START));
}

int ObXAStartP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &trans_id = arg_.get_tx_id();
  const ObXATransID &xid = arg_.get_xid();
  const bool is_tightly_coupled = arg_.is_tightly_coupled();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(trans_id), K(xid));
  } else if (OB_FAIL(xa_service->get_xa_ctx(trans_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed",K(ret), K(trans_id), K(xid));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(trans_id));
  } else {
    if (xa_ctx->is_tightly_coupled() != is_tightly_coupled) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected tight couple flag", K(is_tightly_coupled), K(xid), K(trans_id));
    } else if (arg_.is_first_branch() && OB_FAIL(xa_ctx->wait_xa_start_complete())) {
      TRANS_LOG(WARN, "wait xa start complete failed", K(ret));
    } else if (OB_FAIL(xa_ctx->process_xa_start(arg_))) {
      TRANS_LOG(WARN, "xa ctx remote pull failed", K(ret), K(trans_id), K(xid));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAStartRPCResponse::init(const ObTransID &tx_id,
                               ObTxDesc &tx_desc,
                               const bool is_first_branch)
{
  int ret = OB_SUCCESS;
  if (!tx_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret));
  } else if (OB_FAIL(MTL(ObTransService *)->get_tx_info(tx_desc, tx_info_))) {
    TRANS_LOG(WARN, "get tx info failed", KR(ret));
  } else {
    tx_id_ = tx_id;
    is_first_branch_ = is_first_branch;
  }
  return ret;
}

int ObXAStartResponseP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = arg_.get_tx_id();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K_(arg));
  } else if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K_(arg));
  } else {
    if (OB_FAIL(xa_ctx->process_xa_start_response(arg_))) {
      TRANS_LOG(WARN, "xa ctx sync status response failed", K(ret), K(arg_));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAEndRPCRequest::init(const ObTransID &tx_id,
                            ObTxDesc &tx_desc,
                            const ObXATransID &xid,
                            const bool is_tightly_coupled,
                            const int64_t seq_no,
                            const int64_t end_flag)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() ||
      !tx_desc.is_valid() ||
      !xid.is_valid() ||
      (!ObXAFlag::is_valid(end_flag, ObXAReqType::XA_END))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(end_flag));
  } else if (OB_FAIL(MTL(ObTransService *)->get_tx_stmt_info(tx_desc, stmt_info_))) {
    TRANS_LOG(WARN, "get tx stmt info failed", KR(ret));
  // } else if (OB_FAIL(trans_desc_.trans_deep_copy(trans_desc))) {
  //   TRANS_LOG(WARN, "deep copy trans desc failed", KR(ret));
  } else {
    tx_id_ = tx_id;
    is_tightly_coupled_ = is_tightly_coupled;
    xid_ = xid;
    seq_no_ = seq_no;
    end_flag_ = end_flag;
  }
  return ret;
}

bool ObXAEndRPCRequest::is_valid() const
{
  return stmt_info_.is_valid() &&
         xid_.is_valid() &&
         (ObXAFlag::is_valid(end_flag_, ObXAReqType::XA_END));
}

int ObXAEndP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = arg_.get_tx_id();
  const bool is_tightly_coupled = arg_.is_tightly_coupled();
  ObXACtx *xa_ctx = NULL;
  ObXAService *xa_service = MTL(ObXAService *);
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K_(arg));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K_(arg));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (xa_ctx->is_tightly_coupled() != is_tightly_coupled) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected tight coupled flag", K(arg_));
    } else if (OB_FAIL(xa_ctx->process_xa_end(arg_))) {
      TRANS_LOG(WARN, "xa ctx remote push failed", K(ret), K(arg_));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXACommitP::process()
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = arg_.get_xid();
  const ObTransID &tx_id = arg_.get_trans_id();
  const int64_t timeout_us = arg_.get_timeout_us();
  const int64_t request_id = arg_.get_id();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;
  bool has_tx_level_temp_table = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (OB_FAIL(xa_ctx->one_phase_end_trans(xid, false/*is_rollback*/, timeout_us, request_id))) {
      if (OB_TRANS_COMMITED != ret) {
         TRANS_LOG(WARN, "one phase xa commit failed", K(ret), K(xid));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(xa_ctx->wait_one_phase_end_trans(false/*is_rollback*/, timeout_us))) {
      TRANS_LOG(WARN, "fail to wait one phase xa end trans", K(ret), K(xid));
    }
    has_tx_level_temp_table = xa_ctx->has_tx_level_temp_table();
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_.init(ret, has_tx_level_temp_table);

  return ret;
}

int ObXAStartStmtRPCRequest::init(const ObTransID &tx_id,
                                  const ObXATransID &xid,
                                  const ObAddr &sender,
                                  const int64_t request_id)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !xid.is_valid() || !sender.is_valid() || 0 > request_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(sender), K(request_id));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    sender_ = sender;
    request_id_ = request_id;
  }
  return ret;
}

bool ObXAStartStmtRPCRequest::is_valid() const
{
  return tx_id_.is_valid() && xid_.is_valid() && sender_.is_valid();
}

int ObXAStartStmtP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = arg_.get_tx_id();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K_(arg));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed",K(ret), K_(arg));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K_(arg));
  } else {
    if (OB_FAIL(xa_ctx->process_start_stmt(arg_))) {
      TRANS_LOG(WARN, "fail to process start stmt", K(ret), K_(arg));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAStartStmtRPCResponse::init(const ObTransID &tx_id,
                                   ObTxDesc &tx_desc,
                                   const int64_t response_id)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !tx_desc.is_valid() || 0 > response_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(response_id));
  } else if (OB_FAIL(MTL(ObTransService *)->get_tx_stmt_info(tx_desc, stmt_info_))) {
    TRANS_LOG(WARN, "get tx stmt info failed", KR(ret));
  } else {
    tx_id_ = tx_id;
    response_id_ = response_id;
  }
  return ret;
}

int ObXAStartStmtResponseP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = arg_.get_tx_id();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (OB_FAIL(xa_ctx->process_start_stmt_response(arg_))) {
      TRANS_LOG(WARN, "fail to process start stmt response", K(ret), K(arg_));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAEndStmtRPCRequest::init(const ObTransID &tx_id,
                                ObTxDesc &tx_desc,
                                const ObXATransID &xid,
                                const int64_t seq_no)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !tx_desc.is_valid() || !xid.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid));
  } else if (OB_FAIL(MTL(ObTransService *)->get_tx_stmt_info(tx_desc, stmt_info_))) {
    TRANS_LOG(WARN, "get tx stmt info failed", KR(ret));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    seq_no_ = seq_no;
  }
  return ret;
}

bool ObXAEndStmtRPCRequest::is_valid() const
{
  return stmt_info_.is_valid() && xid_.is_valid();
}

int ObXAEndStmtP::process()
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = arg_.get_tx_id();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "fail to get xa ctx", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (OB_FAIL(xa_ctx->process_end_stmt(arg_))) {
      TRANS_LOG(WARN, "fail to process end stmt", K(ret), K(arg_));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXARollbackRPCRequest::init(const ObTransID &tx_id,
                                 const ObXATransID &xid,
                                 const int64_t timeout_us,
                                 const int64_t request_id)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !xid.is_valid() || 0 > timeout_us || 0 > request_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(timeout_us), K(request_id));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    timeout_us_ = timeout_us;
    request_id_ = request_id;
  }
  return ret;
}

int ObXARollbackP::process()
{
  int ret = OB_SUCCESS;
  ObXAService *xa_service = MTL(ObXAService *);

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K_(arg));
  } else if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else {
    const ObXATransID &xid = arg_.get_xid();
    const ObTransID &tx_id = arg_.get_tx_id();
    const int64_t timeout_us = arg_.get_timeout_us();
    const int64_t request_id = arg_.get_id();
    if (OB_FAIL(xa_service->xa_rollback_local(xid, tx_id, timeout_us, request_id))) {
      TRANS_LOG(WARN, "fail to do xa rollback local", K(ret), K_(arg));
    }
  }

  result_ = Int64(ret);

  return ret;
}

int ObXATerminateRPCRequest::init(const ObTransID &tx_id,
                                 const ObXATransID &xid,
                                 const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid() || !xid.is_valid() || 0 > timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_id), K(xid), K(timeout_us));
  } else {
    tx_id_ = tx_id;
    xid_ = xid;
    timeout_us_ = timeout_us;
  }
  return ret;
}

int ObXATerminateP::process()
{
  int ret = OB_SUCCESS;
  ObXAService *xa_service = MTL(ObXAService *);
  const ObXATransID &xid = arg_.get_xid();
  const ObTransID &tx_id = arg_.get_tx_id();
  const int64_t timeout_us = arg_.get_timeout_us();
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K_(arg));
  } else if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "fail to get xa ctx", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (OB_FAIL(xa_ctx->process_terminate(xid))) {
      TRANS_LOG(WARN, "process terminate failed", K(ret), K(xid), K(tx_id));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAHbReqP::process()
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = arg_.get_xid();
  const ObTransID &tx_id = arg_.get_tx_id();
  const ObAddr &sender = arg_.get_sender();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    // if ctx exists, response success
    if (OB_FAIL(xa_ctx->response_for_heartbeat(xid, sender))) {
      TRANS_LOG(WARN, "fail to response for heartbeat", K(ret), K(tx_id),
          K(xid), K(sender));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

int ObXAHbRespP::process()
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = arg_.get_xid();
  const ObTransID &tx_id = arg_.get_tx_id();
  const ObAddr &sender = arg_.get_sender();
  ObXAService *xa_service = MTL(ObXAService *);
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;

  if (OB_ISNULL(xa_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(arg_));
  } else if (OB_FAIL(xa_service->get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(arg_));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(arg_));
  } else {
    if (OB_FAIL(xa_ctx->update_xa_branch_for_heartbeat(xid))) {
      TRANS_LOG(WARN, "fail to response for heartbeat", K(ret), K(tx_id),
          K(xid), K(sender));
    }
    xa_service->revert_xa_ctx(xa_ctx);
  }

  result_ = Int64(ret);

  return ret;
}

template <ObRpcPacketCode PC>
void ObXARPCCB<PC>::statistics()
{
  int ret = OB_SUCCESS;
  if (is_valid_tenant_id(tenant_id_) && 0 < start_ts_) {
    MTL_SWITCH(tenant_id_) {
      ObXAService *xa_service = MTL(ObXAService *);
      const int64_t used_time_us = ObTimeUtility::current_time() - start_ts_;
      if (NULL != xa_service && 0 < used_time_us) {
        xa_service->get_statistics().inc_xa_inner_rpc_total_count();
        xa_service->get_statistics().add_xa_inner_rpc_total_used_time(used_time_us);
        if (10000 <= used_time_us) {
          xa_service->get_statistics().inc_xa_inner_rpc_ten_ms_total_count();
        }
        if (20000 <= used_time_us) {
          xa_service->get_statistics().inc_xa_inner_rpc_twenty_ms_total_count();
        }
      }
    } // MTL_SWITCH
  }
}

void ObXACommitRPCCB::statistics()
{
  int ret = OB_SUCCESS;
  ObXAService *xa_service = MTL(ObXAService *);
  if (is_valid_tenant_id(tenant_id_) && 0 < start_ts_) {
    MTL_SWITCH(tenant_id_) {
      ObXAService *xa_service = MTL(ObXAService *);
      const int64_t used_time_us = ObTimeUtility::current_time() - start_ts_;
      if (NULL != xa_service && 0 < used_time_us) {
        xa_service->get_statistics().inc_xa_inner_rpc_total_count();
        xa_service->get_statistics().add_xa_inner_rpc_total_used_time(used_time_us);
        if (10000 <= used_time_us) {
          xa_service->get_statistics().inc_xa_inner_rpc_ten_ms_total_count();
        }
        if (20000 <= used_time_us) {
          xa_service->get_statistics().inc_xa_inner_rpc_twenty_ms_total_count();
        }
      }
    }
  }
}
}//obrpc

using namespace obrpc;

namespace transaction
{

int ObXARpc::init(ObXARpcProxy *proxy, const common::ObAddr &self)
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
        TRANS_LOG_RET(WARN, tmp_ret, "gts request rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gts request rpc wait error", K(tmp_ret));
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

int ObXARpc::xa_prepare(const uint64_t tenant_id,
           const common::ObAddr &server,
           const ObXAPrepareRPCRequest &req,
           ObXARPCCB<OB_XA_PREPARE> &cb)
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
  }/* else if (server == self_) {
    // TODO
  } */else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_prepare(req, &cb))) {
    TRANS_LOG(WARN, "post xa prepare rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_start(const uint64_t tenant_id,
                      const common::ObAddr &server,
                      const ObXAStartRPCRequest &req,
                      ObXARPCCB<OB_XA_START_REQ> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_start_req(req, cb))) {
    TRANS_LOG(WARN, "post xa start rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_start_response(const uint64_t tenant_id,
                               const common::ObAddr &server,
                               const ObXAStartRPCResponse &resp,
                               ObXARPCCB<OB_XA_START_RESP> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(resp));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_start_resp(resp, cb))) {
    TRANS_LOG(WARN, "post xa start rpc response failed", KR(ret), K(tenant_id), K(server), K(resp));
  }
  return ret;
}

int ObXARpc::xa_end(const uint64_t tenant_id,
                    const common::ObAddr &server,
                    const ObXAEndRPCRequest &req,
                    ObXARPCCB<OB_XA_END_REQ> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_end_req(req, cb))) {
    TRANS_LOG(WARN, "post xa end rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_start_stmt(const uint64_t tenant_id,
                           const common::ObAddr &server,
                           const ObXAStartStmtRPCRequest &req,
                           ObXARPCCB<OB_XA_START_STMT_REQ> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_start_stmt_req(req, cb))) {
    TRANS_LOG(WARN, "post xa end rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_start_stmt_response(const uint64_t tenant_id,
                                    const common::ObAddr &server,
                                    const ObXAStartStmtRPCResponse &req,
                                    ObXARPCCB<OB_XA_START_STMT_RESP> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_start_stmt_resp(req, cb))) {
    TRANS_LOG(WARN, "post xa end rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_end_stmt(const uint64_t tenant_id,
                         const common::ObAddr &server,
                         const ObXAEndStmtRPCRequest &req,
                         ObXARPCCB<OB_XA_END_STMT_REQ> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_end_stmt_req(req, cb))) {
    TRANS_LOG(WARN, "post xa end stmt rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_commit(const uint64_t tenant_id,
                       const common::ObAddr &server,
                       const ObXACommitRPCRequest &req,
                       ObXACommitRPCCB &cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXARpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObXARpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  }/* else if (server == self_) {
    // TODO
  } */else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_commit(req, &cb))) {
    TRANS_LOG(WARN, "post xa commit rpc request failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_rollback(const uint64_t tenant_id,
                         const common::ObAddr &server,
                         const ObXARollbackRPCRequest &req,
                         ObXARPCCB<OB_XA_ROLLBACK> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "xa rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_rollback(req, cb))) {
    TRANS_LOG(WARN, "fail to post xa rollback rpc request", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_terminate(const uint64_t tenant_id,
                         const common::ObAddr &server,
                         const ObXATerminateRPCRequest &req,
                         ObXARPCCB<OB_XA_TERMINATE> *cb)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "xa rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(req));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_terminate(req, cb))) {
    TRANS_LOG(WARN, "fail to post xa terminate rpc request", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_hb_req(const uint64_t tenant_id, const ObAddr &server,
    const ObXAHbRequest &req, ObXARPCCB<OB_XA_HB_REQ> *cb)
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
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_hb_req(req, cb))) {
    TRANS_LOG(WARN, "post xa hb req failed", KR(ret), K(tenant_id), K(server), K(req));
  }
  return ret;
}

int ObXARpc::xa_hb_resp(const uint64_t tenant_id, const ObAddr &server,
    const ObXAHbResponse &resp, ObXARPCCB<OB_XA_HB_RESP> *cb)
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
  } else if (OB_FAIL(rpc_proxy_->to(server)
                                .by(tenant_id)
                                .timeout(OB_XA_RPC_TIMEOUT)
                                .xa_hb_resp(resp, cb))) {
    TRANS_LOG(WARN, "post xa hb req failed", KR(ret), K(tenant_id), K(server), K(resp));
  }
  return ret;
}

}//transaction

}//oceanbase
