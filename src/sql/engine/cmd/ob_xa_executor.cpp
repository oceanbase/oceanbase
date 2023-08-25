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
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "sql/resolver/xa/ob_xa_stmt.h"
#include "sql/ob_sql_trans_control.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "storage/tx/ob_xa_service.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace sql
{

int ObXaStartExecutor::execute(ObExecContext &ctx, ObXaStartStmt &stmt)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "XA protocol start interface");
  UNUSED(ctx);
  UNUSED(stmt);
  // 暂时禁掉mysql模式下的xa调用
  return ret;
}

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
  } else if (OB_FAIL(get_org_cluster_id_(my_session, org_cluster_id))) {
  } else if (OB_FAIL(my_session->get_tx_timeout(tx_timeout))) {
    LOG_ERROR("fail to get trans timeout ts", K(ret));
  } else if (FALSE_IT(tenant_id = my_session->get_effective_tenant_id())) {
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
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

    ObTxDesc *&tx_desc = my_session->get_tx_desc();
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_start(xid,
                                                         flags,
                                                         my_session->get_xa_end_timeout_seconds(),
                                                         my_session->get_sessid(),
                                                         tx_param,
                                                         tx_desc))) {
      LOG_WARN("xa start failed", K(ret), K(tx_param));
      my_session->reset_tx_variable();
      my_session->set_early_lock_release(false);
      ctx.set_need_disconnect(false);
    } else {
      // associate xa with session
      my_session->associate_xa(xid);
      my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
    }
  }
  LOG_INFO("xa start execute", K(stmt));
  return ret;
}

int ObPlXaStartExecutor::get_org_cluster_id_(ObSQLSessionInfo *session, int64_t &org_cluster_id) {
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

int ObXaEndExecutor::execute(ObExecContext &ctx, ObXaEndStmt &stmt)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "XA protocol end interface");
  UNUSED(ctx);
  UNUSED(stmt);
  // 暂时禁掉mysql模式下的xa调用
  return ret;
  /*
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  //storage::ObPartitionService *ps = nullptr;

  // if (OB_ISNULL(my_session) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  // mysql调用flags置为TMSUCCESS
  } else if (OB_FAIL(ps->xa_end(stmt.get_xa_string(),
                                transaction::ObXAEndFlag::TMSUCCESS,
                                my_session->get_trans_desc()))) {
    LOG_WARN("xa end failed", K(ret), K(stmt.get_xa_string()));
    // 如果是OB_TRANS_XA_RMFAIL错误那么由用户决定是否回滚
    if (OB_TRANS_XA_RMFAIL != ret
        && OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
      ret = tmp_ret;
      LOG_WARN("explicit end trans failed", K(ret));
    }
  } else {
    my_session->reset_tx_variable();
    ctx.set_need_disconnect(false);
    my_session->get_trans_desc().get_standalone_stmt_desc().reset();
  }
  LOG_DEBUG("xa end execute", K(stmt.get_xa_string()));
  return ret;
  */
}

int ObPlXaEndExecutor::execute(ObExecContext &ctx, ObXaEndStmt &stmt)
{
  int ret = OB_SUCCESS;
  //int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObXATransID xid;

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
  } else if (my_session->get_xid().empty()) {
    ret = OB_TRANS_XA_PROTO;
    LOG_WARN("not in xa trans", K(ret));
  } else if (!xid.all_equal_to(my_session->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    TRANS_LOG(WARN, "xid not match", K(ret), K(xid));
  } else {
    ObSQLSessionInfo::LockGuard session_query_guard(my_session->get_query_lock());
    ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
    int64_t flags = stmt.get_flags();
    flags = my_session->has_tx_level_temp_table() ? (flags | ObXAFlag::OBTEMPTABLE) : flags;
    my_session->get_raw_audit_record().trans_id_ = my_session->get_tx_id();
    if (OB_FAIL(MTL(transaction::ObXAService*)->xa_end(xid, flags,
          my_session->get_tx_desc()))) {
      LOG_WARN("xa end failed", K(ret), K(xid));
      // if branch fail is returned, clean trans in session
      if (OB_TRANS_XA_BRANCH_FAIL == ret) {
        my_session->reset_tx_variable();
        my_session->disassociate_xa();
        ctx.set_need_disconnect(false);
      }
    } else {
      my_session->reset_tx_variable();
      my_session->disassociate_xa();
      ctx.set_need_disconnect(false);
    }
  }
  LOG_DEBUG("xa end execute", K(stmt));
  return ret;
}

int ObXaPrepareExecutor::execute(ObExecContext &ctx, ObXaPrepareStmt &stmt)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "XA protocol prepare interface");
  UNUSED(ctx);
  UNUSED(stmt);
  // 暂时禁掉mysql模式下的xa调用
  return ret;
  /*
  int ret = OB_SUCCESS;
  //int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  //storage::ObPartitionService *ps = nullptr;
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)
      // || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session), K(plan_ctx), K(ps));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret));
  } else {
    const uint64_t tenant_id = my_session->get_effective_tenant_id();
    int64_t timeout = plan_ctx->get_trans_timeout_timestamp();
    transaction::ObStartTransParam &start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);
    if (OB_FAIL(ps->xa_prepare(stmt.get_xa_string(), tenant_id, timeout))) {
      LOG_WARN("xa prepare failed", K(ret), K(stmt.get_xa_string()));
      // 如果是OB_TRANS_XA_RMFAIL错误那么由用户决定是否回滚
      // if (OB_TRANS_XA_RMFAIL != ret
      //    && OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
      //  ret = tmp_ret;
      //  LOG_WARN("explicit end trans failed", K(ret));
      // }
    }
  }

  LOG_DEBUG("xa prepare execute", K(stmt.get_xa_string()));
  return ret;
  */
}

int ObPlXaPrepareExecutor::execute(ObExecContext &ctx, ObXaPrepareStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObXATransID xid;
  ObTransID tx_id;

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

  LOG_INFO("xa prepare execute", K(ret), K(stmt), K(xid), K(tx_id));
  return ret;
}

int ObXaEndTransExecutor::execute_(const ObString &xid,
    const bool is_rollback, ObExecContext &ctx)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "XA protocol end trans interface");
  UNUSED(xid);
  UNUSED(is_rollback);
  UNUSED(ctx);
  // 暂时禁掉mysql模式下的xa调用
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
  LOG_INFO("xa commit", K(ret), K(stmt), K(xid), K(tx_id));
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
  LOG_INFO("xa rollback", K(ret), K(stmt), K(xid), K(tx_id));
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
