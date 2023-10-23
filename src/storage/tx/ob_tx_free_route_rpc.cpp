/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_trans_rpc.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "ob_trans_service.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase {
namespace obrpc {

/*
 * check txn is alive on the session
 * skip the situation session was killed or un-exist
 */
int ObTxFreeRouteCheckAliveP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObTransID sess_tx_id;
  {
    sql::ObSQLSessionInfo *session = NULL;
    sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, arg_.tx_sess_id_);
    if (OB_FAIL(guard.get_session(session))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SESSION_NOT_FOUND;
      } else {
        TRANS_LOG(WARN, "get session fail", K(ret), K_(arg_.tx_sess_id));
      }
    } else if (OB_FAIL(session->get_query_lock().trylock())) {
      TRANS_LOG(WARN, "try session query_lock failed", K(ret), K_(arg_.tx_sess_id));
    } else {
      if (session->get_session_state() == sql::ObSQLSessionState::SESSION_KILLED) {
        ret = OB_SESSION_KILLED;
      } else {
        sess_tx_id = session->get_tx_id();
        ret = sess_tx_id == arg_.tx_id_ ? OB_SUCCESS : OB_TRANS_CTX_NOT_EXIST;
      }
      session->get_query_lock().unlock();
    }
  }
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "can not get trans service", KR(ret));
  } else if (OB_FAIL(txs->tx_free_route_handle_check_alive(arg_, ret))) {
    TRANS_LOG(WARN, "[tx free route] handle check alive fail", K(ret), K(arg_));
  }
  return ret;
}

int ObTxFreeRouteCheckAliveRespP::process()
{
  // if ret is OB_SESSION_NOT_FOUND || OB_SESSION_KILLED
  //    kill_session
  // if ret is OB_TRANS_CTX_NOT_EXIST
  // 1. get session by session_id
  // 2. lock session
  // 3. check session's txn id equals to this tx
  // 4. call : reset_session_tx_state to release txn
  // 5. unlock session
  // 6. revert session
  int ret = OB_SUCCESS;
  if (OB_SESSION_NOT_FOUND == arg_.ret_ || OB_SESSION_KILLED == arg_.ret_) {
    TRANS_LOG(INFO, "[txn free route] txn start session has quit, close current session", "respMsg", arg_);
    ret = kill_session_();
  } else if (OB_TRANS_CTX_NOT_EXIST == arg_.ret_) {
    TRANS_LOG(INFO, "[txn free route] txn has quit on start node, release tx on current node", "respMsg", arg_);
    ret = release_session_tx_();
  } else if (OB_SUCCESS != arg_.ret_) {
    TRANS_LOG(WARN, "[txn free route] check txn alive fail", "respMsg", arg_);
  }
  return ret;
}

int ObTxFreeRouteCheckAliveRespP::kill_session_()
{
  int ret = OB_SUCCESS;
  sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, arg_.req_sess_id_);
  sql::ObSQLSessionInfo *session = NULL;
  if (OB_SUCC(guard.get_session(session))) {
    if (session->get_is_in_retry()) {
      // TODO: only kill session when it is idle
    } else if (OB_FAIL(GCTX.session_mgr_->kill_session(*session))) {
      TRANS_LOG(WARN, "kill session fail", K(ret));
    }
  }
  return ret;
}

int ObTxFreeRouteCheckAliveRespP::release_session_tx_()
{
  int ret = OB_SUCCESS;
  sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, arg_.req_sess_id_);
  sql::ObSQLSessionInfo *session = NULL;
  if (OB_FAIL(guard.get_session(session))) {
  } else if (session->get_is_in_retry()) {
    // skip quit txn if session in Query Retry
  } else if (OB_FAIL(session->try_lock_query())) {
  } else if (OB_FAIL(session->try_lock_thread_data())) {
    session->unlock_query();
  } else {
    transaction::ObTxnFreeRouteCtx &ctx = session->get_txn_free_route_ctx();
    transaction::ObTxDesc *&tx_desc = session->get_tx_desc();
    if (ctx.get_local_version() != arg_.request_id_) {
      TRANS_LOG(INFO, "skip handle checkAliveResp, staled", K(arg_), K(ctx.get_local_version()));
    } else if (OB_NOT_NULL(tx_desc) && tx_desc->get_tx_id() == arg_.tx_id_) {
      // mark idle release, if an Query has release query_lock but not send txn state Packet yet,
      // it can sens the txn was released in plan (not a surprise)
      ctx.set_idle_released();
      // for XA txn temporary session on the XA orig node, the TxDesc is shared
      // between XA'Ctx and the temporary session, so release ref is required
      if (tx_desc->is_xa_trans() && tx_desc->get_addr() == GCONF.self_addr_) {
        MTL_SWITCH(session->get_effective_tenant_id()) {
          ret = MTL(transaction::ObTransService*)->release_tx_ref(*tx_desc);
        }
        tx_desc = NULL;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql::ObSqlTransControl::reset_session_tx_state(session, false))) {
        TRANS_LOG(WARN, "[txn free route] release session tx failed", K(ret), KPC(session));
      } else if (OB_NOT_NULL(tx_desc)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "txn not released", KR(ret));
      } else {
        TRANS_LOG(INFO, "[txn free route] release tx success");
      }
    }
    session->unlock_query();
    session->unlock_thread_data();
  }
  return ret;
}

int ObTxFreeRoutePushStateP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObTxFreeRoutePushState &tx_state = arg_;
  transaction::ObTxFreeRoutePushStateResp &resp = result_;
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "fail to get trans service", K(ret));
  } else if (OB_FAIL(txs->tx_free_route_handle_push_state(tx_state))) {
    TRANS_LOG(WARN, "[tx free route] handle push state fail", K(ret), K(tx_state));
  } else {
    TRANS_LOG(INFO, "[tx free route] handle push state success", K(tx_state));
  }
  resp.ret_ = ret;
  return ret;
}

template<ObRpcPacketCode PC>
int ObTxFreeRouteRPCCB<PC>::process()
{
 TRANS_LOG(TRACE, "txn free route rpc callback");
  return OB_SUCCESS;
}
template<ObRpcPacketCode PC>
void ObTxFreeRouteRPCCB<PC>::on_timeout()
{
  TRANS_LOG_RET(WARN, OB_TIMEOUT, "txn free route rpc timeout");
}

template int ObTxFreeRouteRPCCB<OB_TX_FREE_ROUTE_CHECK_ALIVE>::process();
template void ObTxFreeRouteRPCCB<OB_TX_FREE_ROUTE_CHECK_ALIVE>::on_timeout();
template int ObTxFreeRouteRPCCB<OB_TX_FREE_ROUTE_CHECK_ALIVE_RESP>::process();
template void ObTxFreeRouteRPCCB<OB_TX_FREE_ROUTE_CHECK_ALIVE_RESP>::on_timeout();

}
}
