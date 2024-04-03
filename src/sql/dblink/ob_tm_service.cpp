// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "sql/dblink/ob_tm_service.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"
#include "pl/sys_package/ob_dbms_xa.h"
#include "storage/tx/ob_xa_service.h"

#define USING_LOG_PREFIX SQL

namespace oceanbase
{
using namespace transaction;
using namespace common;
using namespace common::sqlclient;
using namespace share;

namespace sql
{

// execute xa start for new dblink connection
// 1. if no trans in current session, start a dblink trans in local
//    and execute xa start for target dblink connection
// 2. if plain trans, promote local trans to dblink trans and execute
//    xa start for target dblink connection
// 3. if dblink trans, execute xa start for target dblink connection (if necessary)
// 4. if xa trans, return error
// @param[in] exec_ctx
// @param[in] dblink_type
// @param[in] dblink_conn
// @param[out] tx_id
int ObTMService::tm_rm_start(ObExecContext &exec_ctx,
                             const DblinkDriverProto dblink_type,
                             ObISQLConnection *dblink_conn,
                             ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  bool need_promote = false;
  bool need_start = false;
  int64_t tx_timeout = 0;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *&tx_desc = my_session->get_tx_desc();
  ObXAService *xa_service = MTL(ObXAService*);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const int64_t cluster_id = GCONF.cluster_id;
  ObXATransID xid;

  // step 1, check the trans in current session
  if (NULL == xa_service || NULL == my_session || NULL == plan_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KP(xa_service), KP(my_session));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (OB_FAIL(my_session->get_tx_timeout(tx_timeout))) {
    LOG_ERROR("fail to get trans timeout ts", K(ret));
  } else if (my_session->get_in_transaction()) {
    if (NULL == tx_desc) {
      // unexpected
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans descriptor", K(ret));
    } else if (tx_desc->is_tx_timeout()) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("dblink trans is timeout", K(ret), K(tx_desc));
    } else {
      ObGlobalTxType type = tx_desc->get_global_tx_type(my_session->get_xid());
      if (ObGlobalTxType::XA_TRANS == type) {
        // if xa trans, return error
        // TODO, ORA-24777
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not use dblink in xa trans", K(ret));
      } else if (ObGlobalTxType::PLAIN == type) {
        // if plain trans, need promote
        need_promote = true;
      } else {
        // if dblink trans, do nothing
      }
    }
  } else {
    // no trans in current session, need start a dblink trans
    need_start = true;
  }

  // step 2, promote or start trans in current session
  if (OB_SUCCESS != ret) {
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    const int64_t timeout_seconds = my_session->get_xa_end_timeout_seconds();
    //if (need_start || need_promote) {
    //  if (OB_FAIL(ObXAService::generate_xid(xid))) {
    //    LOG_WARN("fail to generate xid", K(ret), K(xid));
    //  } else {
    //    // do nothing
    //  }
    //}
    if (need_start) {
      ObTxParam &tx_param = plan_ctx->get_trans_param();
      tx_param.isolation_ = my_session->get_tx_isolation();
      tx_param.cluster_id_ = cluster_id;
      tx_param.timeout_us_ = tx_timeout;
      tx_param.lock_timeout_us_ = my_session->get_trx_lock_timeout();
      if (OB_FAIL(xa_service->xa_start_for_tm(0, timeout_seconds, my_session->get_sessid(),
              tx_param, tx_desc, xid, my_session->get_data_version()))) {
        LOG_WARN("xa start for dblink failed", K(ret), K(tx_param));
        // TODO, reset
        my_session->reset_first_need_txn_stmt_type();
        my_session->get_trans_result().reset();
        my_session->reset_tx_variable();
        exec_ctx.set_need_disconnect(false);
      } else if (NULL == tx_desc || !tx_desc->is_valid() || !xid.is_valid() || xid.empty()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected xid or tx desc", K(ret), K(xid), KPC(tx_desc));
      } else {
        my_session->associate_xa(xid);
      }
    } else if (need_promote) {
      if (OB_FAIL(xa_service->xa_start_for_tm_promotion(0, timeout_seconds, tx_desc, xid))) {
        LOG_WARN("xa start for dblink promotion failed", K(ret), K(xid));
      } else {
        my_session->associate_xa(xid);
      }
    }
  }

  // step 3, xa start for dblink connection
  ObXATransID remote_xid;
  if (OB_SUCCESS != ret) {
  } else if (OB_FAIL(xa_service->xa_start_for_dblink_client(dblink_type,
          dblink_conn, tx_desc, remote_xid))) {
    LOG_WARN("fail to execute xa start for dblink client", K(ret), K(xid), K(remote_xid));
  } else {
    tx_id = tx_desc->tid();
  }
  my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
  // for statistics
  if (need_start || need_promote) {
    DBLINK_STAT_ADD_TRANS_COUNT();
    if (need_promote) {
      DBLINK_STAT_ADD_TRANS_PROMOTION_COUNT();
    }
    if (OB_SUCCESS != ret) {
      DBLINK_STAT_ADD_TRANS_FAIL_COUNT();
    }
  }
  LOG_INFO("tm rm start", K(ret), K(tx_id), K(remote_xid), K(need_start), K(need_promote));

  // if fail, the trans should be rolled back by client

  return ret;
}

// execute two phase commit for each participant (i.e., dblink connection)
// 1. do xa end for each participant first
// 2. do xa preapre for each participant
// 3. if all participants are prepared successfully, do xa commit for each participant;
//    otherwise, do xa rollback for each
// @param[in] exec_ctx
// @param[out] tx_id
int ObTMService::tm_commit(ObExecContext &exec_ctx,
                           ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *&tx_desc = my_session->get_tx_desc();
  ObXAService *xa_service = MTL(ObXAService*);
  const int64_t start_ts = ObTimeUtility::current_time();

  if (NULL == xa_service || NULL == my_session || NULL == tx_desc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KP(xa_service), KP(my_session), KP(tx_desc));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    tx_id = tx_desc->tid();
    my_session->get_raw_audit_record().trans_id_ = tx_id;
    {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
      if (OB_FAIL(xa_service->commit_for_dblink_trans(tx_desc))) {
        LOG_WARN("fail to commit for dblink trans", K(ret));
      } else {
        // do nothing
      }
    }
    // TODO, if fail, kill trans forcely and reset session
    // reset
    my_session->reset_first_need_txn_stmt_type();
    my_session->get_trans_result().reset();
    my_session->reset_tx_variable();
    my_session->disassociate_xa();
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  DBLINK_STAT_ADD_TRANS_COMMIT_COUNT();
  DBLINK_STAT_ADD_TRANS_COMMIT_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    DBLINK_STAT_ADD_TRANS_COMMIT_FAIL_COUNT();
  }
  LOG_INFO("tm commit for dblink trans", K(tx_id), K(used_time_us));
  return ret;
}

// execute rollback for each participant
// 1. do xa end for each participant first
// 2. do xa rollback for each participant
// @param[in] exec_ctx
// @param[out] tx_id
int ObTMService::tm_rollback(ObExecContext &exec_ctx,
                             ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *&tx_desc = my_session->get_tx_desc();
  ObXAService *xa_service = MTL(ObXAService*);
  const int64_t start_ts = ObTimeUtility::current_time();

  if (NULL == xa_service || NULL == my_session || NULL == tx_desc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KP(xa_service), KP(my_session), KP(tx_desc));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    tx_id = tx_desc->tid();
    my_session->get_raw_audit_record().trans_id_ = tx_id;
    if (OB_FAIL(xa_service->rollback_for_dblink_trans(tx_desc))) {
      LOG_WARN("fail to rollback for dblink trans", K(ret));
    } else {
      // do nothing
    }
    // TODO, if fail, kill trans forcely and reset session
    // reset
    my_session->reset_first_need_txn_stmt_type();
    my_session->get_trans_result().reset();
    my_session->reset_tx_variable();
    my_session->disassociate_xa();
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  DBLINK_STAT_ADD_TRANS_ROLLBACK_COUNT();
  DBLINK_STAT_ADD_TRANS_ROLLBACK_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    DBLINK_STAT_ADD_TRANS_ROLLBACK_FAIL_COUNT();
  }
  LOG_INFO("tm rollback for dblink trans", K(tx_id), K(used_time_us));
  return ret;
}

int ObTMService::recover_tx_for_callback(const ObTransID &tx_id,
                                         ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *&tx_desc = my_session->get_tx_desc();
  ObXAService *xa_service = MTL(ObXAService*);
  if (!tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tx_id));
  } else if (NULL == xa_service || NULL == my_session) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KP(xa_service), KP(my_session));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (my_session->get_in_transaction()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session", K(ret), K(tx_id), K(tx_desc->tid()));
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    if (OB_FAIL(xa_service->recover_tx_for_dblink_callback(tx_id, tx_desc))) {
      LOG_WARN("fail to recover tx for dblink callback", K(ret), K(tx_id));
    } else if (NULL == tx_desc || tx_desc->get_xid().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans desc", K(ret), K(tx_id));
    } else {
      my_session->associate_xa(tx_desc->get_xid());
    }
  }
  DBLINK_STAT_ADD_TRANS_CALLBACK_COUNT();
  LOG_INFO("recover tx for dblink callback", K(ret), K(tx_id));
  return ret;
}

int ObTMService::revert_tx_for_callback(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *&tx_desc = my_session->get_tx_desc();
  ObXAService *xa_service = MTL(ObXAService*);
  ObTransID tx_id;
  if (NULL == xa_service || NULL == my_session || NULL == tx_desc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KP(xa_service), KP(my_session), KP(tx_desc));
  } else if (!my_session->get_in_transaction()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session", K(ret), K(tx_desc->tid()));
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    tx_id = tx_desc->tid();
    if (OB_FAIL(xa_service->revert_tx_for_dblink_callback(tx_desc))) {
      LOG_WARN("fail to recover tx for dblink callback", K(ret), K(tx_id));
    } else {
      // do nothing
    }
    my_session->reset_first_need_txn_stmt_type();
    my_session->get_trans_result().reset();
    my_session->reset_tx_variable();
    my_session->disassociate_xa();
  }
  LOG_INFO("revert tx for dblink callback", K(ret), K(tx_id));
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
