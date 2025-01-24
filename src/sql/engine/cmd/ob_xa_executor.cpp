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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_xa_executor.h"
#include "storage/tx/ob_xa_service.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace sql
{
int ObXaExecutorUtil::build_tx_param(ObSQLSessionInfo *session, transaction::ObTxParam &param)
{
  int ret = OB_SUCCESS;
  int64_t org_cluster_id = OB_INVALID_ORG_CLUSTER_ID;
  if (OB_FAIL(get_org_cluster_id(session, org_cluster_id))) {
    LOG_WARN("get original cluster id failed", K(ret));
  } else {
    int64_t tx_timeout_us = 0;
    bool is_read_only = session->get_tx_read_only();
    session->get_tx_timeout(tx_timeout_us);
    param.timeout_us_ = tx_timeout_us;
    param.lock_timeout_us_ = session->get_trx_lock_timeout();
    param.access_mode_ = is_read_only ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW;
    param.isolation_ = session->get_tx_isolation();
    param.cluster_id_ = org_cluster_id;
  }
  return ret;
}

int64_t ObXaExecutorUtil::get_query_timeout(ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout = 10L * 1000 * 1000; // 10 seconds by default
  if (NULL != session) {
    if (OB_FAIL(session->get_query_timeout(query_timeout))) {
      LOG_WARN("get query timeout failed", K(ret), K(query_timeout));
      query_timeout = 10L * 1000 * 1000;
    }
  }
  return query_timeout;
}

// for mysql xa start
int ObXaStartExecutor::execute(ObExecContext &ctx, ObXaStartStmt &stmt)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(xa_start);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
  } else if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected in oracle mode", K(ret));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid failed", K(ret), K(stmt));
  } else if (my_session->get_in_transaction()) {
    ObTxDesc *&tx_desc = my_session->get_tx_desc();
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret), K(xid), K(tx_desc->tid()),
        K(tx_desc->get_xid()));
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    const int64_t flags = stmt.get_flags();
    transaction::ObTxParam &tx_param = plan_ctx->get_trans_param();
    ObTxDesc *&tx_desc = my_session->get_tx_desc();
    if (OB_FAIL(ObXaExecutorUtil::build_tx_param(my_session, tx_param))) {
      LOG_WARN("build tx param failed", K(ret));
    } else if (OB_FAIL(MTL(transaction::ObXAService*)->xa_start_for_mysql(xid,
            flags, my_session->get_sessid(), tx_param, tx_desc))) {
      LOG_WARN("mysql xa start failed", K(ret), K(tx_param));
      my_session->get_trans_result().reset();
      my_session->reset_tx_variable();
      ctx.set_need_disconnect(false);
    } else {
      // associate xa with session
      my_session->associate_xa(xid);
      my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
      FLT_SET_TAG(trans_id, my_session->get_tx_id().get_id());
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_START_TOTAL_COUNT();
  XA_STAT_ADD_XA_START_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_START_FAIL_COUNT();
  }
  LOG_INFO("mysql xa start", K(ret), K(xid), K(tx_id));
  return ret;
}

// for mysql xa end
int ObXaEndExecutor::execute(ObExecContext &ctx, ObXaEndStmt &stmt)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(xa_end);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected in oracle mode", K(ret));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (!my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_WARN("not in transaction", K(ret));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid failed", K(ret), K(stmt));
  } else if (!xid.all_equal_to(my_session->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    LOG_WARN("unknown xid", K(ret), K(xid), K(my_session->get_xid()));
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    ObTxDesc *&tx_desc = my_session->get_tx_desc();
    FLT_SET_TAG(trans_id, my_session->get_tx_id().get_id());
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_end_for_mysql(xid, tx_desc))) {
      LOG_WARN("mysql xa end failed", K(ret), K(xid));
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_END_TOTAL_COUNT();
  XA_STAT_ADD_XA_END_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_END_FAIL_COUNT();
  }
  LOG_INFO("mysql xa end", K(ret), K(xid));
  return ret;
}

// for mysql xa prepare
int ObXaPrepareExecutor::execute(ObExecContext &ctx, ObXaPrepareStmt &stmt)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(xa_prepare);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected in oracle mode", K(ret));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (!my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_WARN("not in transaction", K(ret));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid failed", K(ret), K(stmt));
  } else if (!xid.all_equal_to(my_session->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    LOG_WARN("unknown xid", K(ret), K(xid), K(my_session->get_xid()));
  } else {
    // in two cases, xa trans need exit
    // case one, xa prepare succeeds
    // case two, xa trans need rollback
    const int64_t timeout_us = ObXaExecutorUtil::get_query_timeout(my_session);
    bool need_exit = false;
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    ObTxDesc *&tx_desc = my_session->get_tx_desc();
    my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
    FLT_SET_TAG(trans_id, my_session->get_tx_id().get_id());
    if (0 >= timeout_us) {
      ret = OB_TRANS_STMT_TIMEOUT;
      LOG_WARN("xa stmt timeout", K(ret), K(xid), K(timeout_us));
    } else if (OB_FAIL(MTL(transaction::ObXAService*)->xa_prepare_for_mysql(xid, timeout_us,
            tx_desc, need_exit))) {
      LOG_WARN("mysql xa prepare failed", K(ret), K(xid));
    }
    if (need_exit) {
      my_session->get_trans_result().reset();
      my_session->reset_tx_variable();
      my_session->disassociate_xa();
      ctx.set_need_disconnect(false);
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_PREPARE_TOTAL_COUNT();
  XA_STAT_ADD_XA_PREPARE_TOTAL_USED_TIME(used_time_us);
  if (OB_SUCCESS != ret && OB_TRANS_XA_RDONLY != ret) {
    XA_STAT_ADD_XA_PREPARE_FAIL_COUNT();
  }
  LOG_INFO("mysql xa prepare", K(ret), K(xid));
  return ret;
}

// for mysql xa commit
int ObXaCommitExecutor::execute(ObExecContext &ctx, ObXaCommitStmt &stmt)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(xa_commit);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected in oracle mode", K(ret));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid failed", K(ret), K(stmt));
  } else {
    const int64_t timeout_us = ObXaExecutorUtil::get_query_timeout(my_session);
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    const int64_t flags = stmt.get_flags();
    if (0 >= timeout_us) {
      ret = OB_TRANS_STMT_TIMEOUT;
      LOG_WARN("xa stmt timeout", K(ret), K(xid), K(timeout_us));
    } else if (ObXAFlag::is_tmonephase(flags)) {
      bool need_exit = false;
      // one phase xa commit
      if (!my_session->get_in_transaction()) {
        ret = OB_TRANS_XA_RMFAIL;
        LOG_WARN("no xa trans for one phase xa commit", K(ret), K(xid));
      } else if (my_session->get_xid().empty()) {
        ret = OB_TRANS_XA_NOTA;
        LOG_WARN("unknown xid", K(ret), K(xid));
      } else if (!xid.all_equal_to(my_session->get_xid())) {
        ret = OB_TRANS_XA_RMFAIL;
        LOG_WARN("unexpected xid", K(ret), K(xid), K(my_session->get_xid()));
      } else {
        ObTxDesc *&tx_desc = my_session->get_tx_desc();
        my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
        FLT_SET_TAG(trans_id, my_session->get_tx_id().get_id());
        if (OB_FAIL(MTL(transaction::ObXAService*)->xa_commit_onephase_for_mysql(xid,
                timeout_us, tx_desc, need_exit))) {
          LOG_WARN("mysql xa commit failed", K(ret), K(xid));
        }
        if (need_exit) {
          my_session->get_trans_result().reset();
          my_session->reset_tx_variable();
          my_session->disassociate_xa();
          ctx.set_need_disconnect(false);
        }
      }
    } else if (ObXAFlag::is_tmnoflags_for_mysql(flags)) {
      // two phase xa commit
      const bool is_rollback = false;
      if (my_session->get_in_transaction()) {
        if (my_session->get_xid().empty()) {
          ret = OB_TRANS_XA_NOTA;
          LOG_WARN("unknown xid", K(ret), K(xid));
        } else {
          ret = OB_TRANS_XA_RMFAIL;
          LOG_WARN("can not be executed in this state", K(ret), K(xid), K(my_session->get_xid()));
        }
      } else if (OB_FAIL(MTL(transaction::ObXAService*)->xa_second_phase_twophase_for_mysql(xid,
              timeout_us, is_rollback, tx_id))) {
        LOG_WARN("mysql xa commit failed", K(ret), K(xid));
      }
      my_session->get_raw_audit_record().trans_id_ = tx_id;
      FLT_SET_TAG(trans_id, tx_id.get_id());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flags for mysql xa commit", K(ret), K(xid));
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_COMMIT_TOTAL_COUNT();
  XA_STAT_ADD_XA_COMMIT_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_COMMIT_FAIL_COUNT();
  }
  LOG_INFO("mysql xa commit", K(ret), K(xid));
  return ret;
}

// for mysql xa rollback
int ObXaRollbackExecutor::execute(ObExecContext &ctx, ObXaRollBackStmt &stmt)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(xa_rollback);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected in oracle mode", K(ret));
  } else if (!my_session->is_inner() && my_session->is_txn_free_route_temp()) {
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
    LOG_WARN("not support tx free route for dblink trans");
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid failed", K(ret), K(stmt));
  } else {
    bool need_exit = false;
    const int64_t timeout_us = ObXaExecutorUtil::get_query_timeout(my_session);
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    if (0 >= timeout_us) {
      ret = OB_TRANS_STMT_TIMEOUT;
      LOG_WARN("xa stmt timeout", K(ret), K(xid), K(timeout_us));
    } else if (!my_session->get_in_transaction()) {
      // try two phase xa rollback
      const bool is_rollback = true;
      if (OB_FAIL(MTL(transaction::ObXAService*)->xa_second_phase_twophase_for_mysql(xid,
              timeout_us, is_rollback, tx_id))) {
        LOG_WARN("mysql xa rollback failed", K(ret), K(xid));
      }
      my_session->get_raw_audit_record().trans_id_ = tx_id;
      FLT_SET_TAG(trans_id, tx_id.get_id());
    } else {
      // try one phase xa rollback
      ObTxDesc *&tx_desc = my_session->get_tx_desc();
      my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
      FLT_SET_TAG(trans_id, my_session->get_tx_id().get_id());
      if (my_session->get_xid().empty()) {
        ret = OB_TRANS_XA_NOTA;
        LOG_WARN("unknown xid", K(ret), K(xid));
      } else if (!xid.all_equal_to(my_session->get_xid())) {
        ret = OB_TRANS_XA_RMFAIL;
        LOG_WARN("unexpected xid", K(ret), K(xid), K(my_session->get_xid()));
      } else if (OB_FAIL(MTL(transaction::ObXAService*)->xa_rollback_onephase_for_mysql(xid,
              timeout_us, tx_desc, need_exit))) {
        LOG_WARN("mysql xa rollback failed", K(ret), K(xid));
      }
      if (need_exit) {
        my_session->get_trans_result().reset();
        my_session->reset_tx_variable();
        my_session->disassociate_xa();
        ctx.set_need_disconnect(false);
      }
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_ROLLBACK_TOTAL_COUNT();
  XA_STAT_ADD_XA_ROLLBACK_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_ROLLBACK_FAIL_COUNT();
  }
  LOG_INFO("mysql xa rollback", K(ret), K(xid));
  return ret;
}

// for oracle xa start
int ObPlXaStartExecutor::execute(ObExecContext &ctx, ObXaStartStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObXATransID xid;
  int64_t org_cluster_id = OB_INVALID_ORG_CLUSTER_ID;
  int64_t tx_timeout = 0;
  uint64_t tenant_id = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);

  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
    ret = OB_TRANS_XA_RMFAIL;
  } else if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (my_session->get_in_transaction()) {
    auto tx_desc = my_session->get_tx_desc();
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret), K(xid), K(tx_desc->tid()),
        K(tx_desc->get_xid()));
  } else if (true == my_session->is_for_trigger_package()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support xa start in trigger", K(ret), K(xid));
  } else if (OB_FAIL(ObXaExecutorUtil::get_org_cluster_id(my_session, org_cluster_id))) {
  } else if (OB_FAIL(my_session->get_tx_timeout(tx_timeout))) {
    LOG_ERROR("fail to get trans timeout ts", K(ret));
  } else if (FALSE_IT(tenant_id = my_session->get_effective_tenant_id())) {
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    const int64_t flags = stmt.get_flags();
    transaction::ObTxParam &tx_param = plan_ctx->get_trans_param();
    const bool is_readonly = ObXAFlag::contain_tmreadonly(flags);
    const bool is_serializable = ObXAFlag::contain_tmserializable(flags);
    if (is_readonly && !my_session->get_tx_read_only()) {
      tx_param.access_mode_ = ObTxAccessMode::RD_ONLY;
    } else {
      tx_param.access_mode_ = my_session->get_tx_read_only() ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW;
    }
    if (is_serializable && !my_session->is_isolation_serializable()) {
      tx_param.isolation_ = ObTxIsolationLevel::SERIAL;
    } else {
      tx_param.isolation_ = my_session->get_tx_isolation();
    }
    tx_param.cluster_id_ = org_cluster_id;
    tx_param.timeout_us_ = tx_timeout;

    ObTxDesc *tx_desc = my_session->get_tx_desc();
    {
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_tx_desc() = NULL;
    }
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_start(xid,
                                                         flags,
                                                         my_session->get_xa_end_timeout_seconds(),
                                                         my_session->get_sessid(),
                                                         tx_param,
                                                         tx_desc,
                                                         my_session->get_data_version()))) {
      LOG_WARN("xa start failed", K(ret), K(tx_param));
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_tx_desc() = tx_desc;
      my_session->reset_tx_variable();
      my_session->set_early_lock_release(false);
      ctx.set_need_disconnect(false);
    } else {
      // associate xa with session
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_tx_desc() = tx_desc;
      my_session->associate_xa(xid);
      my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_START_TOTAL_COUNT();
  XA_STAT_ADD_XA_START_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_START_FAIL_COUNT();
  }
  LOG_INFO("xa start", K(ret), K(stmt), K(xid), K(used_time_us));
  return ret;
}

int ObXaExecutorUtil::get_org_cluster_id(ObSQLSessionInfo *session, int64_t &org_cluster_id) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(session->get_ob_org_cluster_id(org_cluster_id))) {
    LOG_WARN("fail to get ob_org_cluster_id", K(ret));
  } else if (OB_INVALID_ORG_CLUSTER_ID == org_cluster_id ||
             OB_INVALID_CLUSTER_ID == org_cluster_id) {
    org_cluster_id = ObServerConfig::get_instance().cluster_id;
    // 如果没设置ob_org_cluster_id（0为非法值，认为没有设置），则设为当前集群的cluster_id。
    // 如果配置项中没设置cluster_id，则ObServerConfig::get_instance().cluster_id会拿到默认值-1。
    // 配置项中没设置cluster_id的话observer是起不来的，因此这里org_cluster_id不会为-1。
    // 保险起见，这里判断org_cluster_id为0或者-1都将其设为ObServerConfig::get_instance().cluster_id。
    if (org_cluster_id < OB_MIN_CLUSTER_ID
        || org_cluster_id > OB_MAX_CLUSTER_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("org_cluster_id is set to cluster_id, but it is out of range",
                K(ret), K(org_cluster_id), K(OB_MIN_CLUSTER_ID), K(OB_MAX_CLUSTER_ID));
    }
  }
  return ret;
}

int ObPlXaEndExecutor::execute(ObExecContext &ctx, ObXaEndStmt &stmt)
{
  int ret = OB_SUCCESS;
  //int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObXATransID xid;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);

  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (!my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_PROTO;
    LOG_WARN("not in a trans", K(ret));
  } else if (true == my_session->is_for_trigger_package()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support xa end in trigger", K(ret), K(xid));
  } else if (my_session->get_xid().empty()) {
    ret = OB_TRANS_XA_PROTO;
    LOG_WARN("not in xa trans", K(ret));
  } else if (!xid.all_equal_to(my_session->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    TRANS_LOG(WARN, "xid not match", K(ret), K(xid));
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    int64_t flags = stmt.get_flags();
    flags = my_session->has_tx_level_temp_table() ? (flags | ObXAFlag::OBTEMPTABLE) : flags;
    ObTxDesc *tx_desc = my_session->get_tx_desc();
    {
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
      my_session->get_tx_desc() = NULL;
    }
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_end(xid, flags,
          tx_desc))) {
      LOG_WARN("xa end failed", K(ret), K(xid));
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_tx_desc() = tx_desc;
      // if branch fail is returned, clean trans in session
      if (OB_TRANS_XA_BRANCH_FAIL == ret) {
        my_session->reset_tx_variable();
        my_session->disassociate_xa();
        ctx.set_need_disconnect(false);
      }
    } else {
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      my_session->get_tx_desc() = tx_desc;
      my_session->reset_tx_variable();
      my_session->disassociate_xa();
      ctx.set_need_disconnect(false);
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_END_TOTAL_COUNT();
  XA_STAT_ADD_XA_END_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_END_FAIL_COUNT();
  }
  LOG_DEBUG("xa end", K(ret), K(stmt), K(xid), K(used_time_us));
  return ret;
}

int ObPlXaPrepareExecutor::execute(ObExecContext &ctx, ObXaPrepareStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);

  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMERR;
    LOG_WARN("already start trans", K(ret));
  } else {
    int64_t timeout_seconds = my_session->get_xa_end_timeout_seconds();
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_prepare(xid, timeout_seconds, tx_id))) {
      if (OB_TRANS_XA_RDONLY != ret) {
        LOG_WARN("xa prepare failed", K(ret), K(stmt));
      }
      // TODO: 如果是OB_TRANS_XA_RMFAIL错误那么由用户决定是否回滚
    }
    my_session->get_raw_audit_record().trans_id_ = tx_id;
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_PREPARE_TOTAL_COUNT();
  XA_STAT_ADD_XA_PREPARE_TOTAL_USED_TIME(used_time_us);
  if (OB_SUCCESS != ret && OB_TRANS_XA_RDONLY != ret) {
    XA_STAT_ADD_XA_PREPARE_FAIL_COUNT();
  }
  LOG_INFO("xa prepare", K(ret), K(xid), K(tx_id), K(used_time_us));
  return ret;
}

int ObPlXaEndTransExecutor::execute(ObExecContext &ctx, ObXaCommitStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else {
    ret = execute_(stmt.get_gtrid_string(),
                   stmt.get_bqual_string(),
                   stmt.get_format_id(),
                   false,
                   stmt.get_flags(),
                   ctx);
  }
  return ret;
}

int ObPlXaEndTransExecutor::execute(ObExecContext &ctx, ObXaRollBackStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else {
    ret = execute_(stmt.get_gtrid_string(),
                   stmt.get_bqual_string(),
                   stmt.get_format_id(),
                   true,
                   stmt.get_flags(),
                   ctx);
  }
  return ret;
}

int ObPlXaEndTransExecutor::execute_(const ObString &gtrid_str,
                                     const ObString &bqual_str,
                                     const int64_t format_id,
                                     const bool is_rollback,
                                     const int64_t flags,
                                     ObExecContext &ctx)
{
  UNUSEDx(gtrid_str, bqual_str, format_id, is_rollback, flags, ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "XA protocol end trans interface");
  // ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  // ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  // ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  // ObTxDesc &trans_desc = *my_session->get_tx_desc();
  // storage::ObPartitionService *ps = nullptr;
  // ObXATransID xid;

  // if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)
  //     || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
  // } else if (OB_FAIL(xid.set(gtrid_str,
  //                            bqual_str,
  //                            format_id))) {
  //   LOG_WARN("set xid error", K(ret), K(gtrid_str), K(bqual_str), K(format_id));
  // } else if (my_session->get_in_transaction()) {
  //   ret = OB_TRANS_XA_RMFAIL;
  //   LOG_WARN("already start trans", K(ret));
  // } else {
  //   transaction::ObTxParam &tx_param = plan_ctx->get_trans_param();
  //   if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, false))) {
  //     LOG_WARN("explicit start trans failed", K(ret), K(tx_param));
  //   } else if (OB_FAIL(ps->xa_end_trans(xid, is_rollback, flags, trans_desc))) {
  //     LOG_WARN("fail to execute xa commit/rollback", K(xid), K(is_rollback));
  //   } else {
  //     LOG_DEBUG("succeed to execute xa commit/rollback", K(xid), K(is_rollback));
  //   }
  //   my_session->reset_tx_variable();
  //   ctx.set_need_disconnect(false);
  // }
  // LOG_DEBUG("xa end_trans execute", K(gtrid_str), K(bqual_str), K(format_id), K(is_rollback),
  //     K(flags));
  return ret;
}

int ObPlXaCommitExecutor::execute(ObExecContext &ctx, ObXaCommitStmt &stmt)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTxDesc *tx_desc = my_session->get_tx_desc();
  int64_t xa_timeout_seconds = my_session->get_xa_end_timeout_seconds();
  ObXATransID xid;
  ObTransID tx_id;
  bool has_tx_level_temp_table = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt), K(xid));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_WARN("already start trans", K(ret), K(tx_desc->tid()));
  } else {
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_commit(xid, stmt.get_flags(),
            xa_timeout_seconds, has_tx_level_temp_table, tx_id))) {
      LOG_WARN("xa commit failed", K(ret), K(xid));
    }
    my_session->get_raw_audit_record().trans_id_ = tx_id;
  }
  if (has_tx_level_temp_table) {
    int temp_ret = my_session->drop_temp_tables(false, true/*is_xa_trans*/);
    if (OB_SUCCESS != temp_ret) {
      LOG_WARN("trx level temporary table clean failed", KR(temp_ret));
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_COMMIT_TOTAL_COUNT();
  XA_STAT_ADD_XA_COMMIT_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_COMMIT_FAIL_COUNT();
  }
  LOG_INFO("xa commit", K(ret), K(stmt), K(xid), K(tx_id), K(used_time_us));
  return ret;
}

int ObPlXaRollbackExecutor::execute(ObExecContext &ctx, ObXaRollBackStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTxDesc *tx_desc = my_session->get_tx_desc();
  int64_t xa_timeout_seconds = my_session->get_xa_end_timeout_seconds();
  ObXATransID xid;
  ObTransID tx_id;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAStmtGuard xa_stmt_guard(start_ts);
  if (!stmt.is_valid_oracle_xid()) {
    ret = OB_TRANS_XA_INVAL;
    LOG_WARN("invalid xid for oracle mode", K(ret), K(stmt));
  } else if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(),
                             stmt.get_bqual_string(),
                             stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt), K(xid));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_WARN("already start trans", K(ret), K(tx_desc->tid()));
  } else {
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_rollback(xid, xa_timeout_seconds, tx_id))) {
      LOG_WARN("xa rollback failed", K(ret), K(xid));
    }
    my_session->get_raw_audit_record().trans_id_ = tx_id;
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  XA_STAT_ADD_XA_ROLLBACK_TOTAL_COUNT();
  XA_STAT_ADD_XA_ROLLBACK_TOTAL_USED_TIME(used_time_us);
  if (OB_FAIL(ret)) {
    XA_STAT_ADD_XA_ROLLBACK_FAIL_COUNT();
  }
  LOG_INFO("xa rollback", K(ret), K(stmt), K(xid), K(tx_id), K(used_time_us));
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
