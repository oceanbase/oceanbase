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

int ObTxFreeRouteCheckAliveP::process()
{
  int ret = OB_SUCCESS;
  // get txn by tx_id
  // if txn not exist return with OB_TRANS_CTX_NOT_EXIST
  auto txs = MTL(transaction::ObTransService*);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "can not get trans service", KR(ret));
  } else if (OB_FAIL(txs->tx_free_route_handle_check_alive(arg_))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "[tx free route] handle check alive fail", K(ret), K(arg_));
    }
  }
  return ret;
}

int ObTxFreeRouteCheckAliveRespP::process()
{
  // if ret is OB_TRANS_CTX_NOT_EXIST
  // 1. get session by session_id
  // 2. lock session
  // 3. check session's txn id equals to this tx
  // 4. call : reset_session_tx_state to release txn
  // 5. unlock session
  // 6. revert session
  int ret = OB_SUCCESS;
  if (arg_.ret_ == OB_TRANS_CTX_NOT_EXIST) {
    TRANS_LOG(INFO, "[txn free route] txn has quit on start node, release tx on current node", "respMsg", arg_);
    auto session_id = arg_.session_id_;
    sql::ObSQLSessionInfo *session = NULL;
    auto session_mgr = GCTX.session_mgr_;
    if (OB_FAIL(session_mgr->get_session(session_id, session))) {
      TRANS_LOG(WARN, "get session fail", K(ret), K(session_id));
    } else if (session->get_is_in_retry()) {
      // skip quit txn if session in Query Retry
    } else {
      {
        sql::ObSQLSessionInfo::LockGuard query_lock_guard(session->get_query_lock());
        sql::ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
        auto &ctx = session->get_txn_free_route_ctx();
        auto &tx_desc = session->get_tx_desc();
        if (ctx.version() != arg_.request_id_) {
          TRANS_LOG(INFO, "skip handle checkAliveResp, staled", K(arg_), K(ctx.version()));
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
      }
      session_mgr->revert_session(session);
    }
  } else if (OB_SUCCESS != arg_.ret_) {
    TRANS_LOG(WARN, "[txn free route] check txn alive fail", "respMsg", arg_);
  }
  return ret;
}

int ObTxFreeRoutePushStateP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObTxFreeRoutePushState &tx_state = arg_;
  transaction::ObTxFreeRoutePushStateResp &resp = result_;
  auto txs = MTL(transaction::ObTransService*);
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
