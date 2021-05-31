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
#include "storage/transaction/ob_trans_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace sql {

static inline void init_start_trans_param(
    ObSQLSessionInfo* session, ObTaskExecutorCtx& task_exec_ctx, transaction::ObStartTransParam& param)
{
  param.set_access_mode(transaction::ObTransAccessMode::READ_WRITE);
  param.set_type(session->get_trans_type());
  param.set_isolation(session->get_tx_isolation());
  param.set_cluster_version(task_exec_ctx.get_min_cluster_version());
  param.set_inner_trans(session->is_inner());
}

int ObXaStartExecutor::execute(ObExecContext& ctx, ObXaStartStmt& stmt)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(stmt);
  return ret;
  /*
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  storage::ObPartitionService *ps = nullptr;
  int32_t xa_trans_state = ObXATransState::NON_EXISTING;
  ObTransDesc &trans_desc = my_session->get_trans_desc();

  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)
      || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
  } else if (OB_FAIL(ps->get_xa_trans_state(xa_trans_state, trans_desc))) {
    LOG_WARN("get xa trans state failed", K(ret));
  } else if (ObXATransState::NON_EXISTING != xa_trans_state) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_USER_ERROR(OB_TRANS_XA_RMFAIL, ObXATransState::to_string(xa_trans_state));
    LOG_WARN("invalid xa state", K(ret), K(xa_trans_state));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret));
  } else {
    transaction::ObStartTransParam &start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);
    if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, start_trans_param))) {
      LOG_WARN("explicit start trans failed", K(ret), K(start_trans_param));
    } else if (OB_FAIL(ps->xa_start(stmt.get_xa_string(),
                                    stmt.get_gtrid_string().length(),
                                    stmt.get_bqual_string().length(),
                                    stmt.get_format_id(),
                                    stmt.get_flags(),
                                    trans_desc))) {
      LOG_WARN("xa start failed", K(ret), K(start_trans_param));
      if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
        ret = tmp_ret;
        LOG_WARN("explicit end trans failed", K(ret));
      }
    } else {
      // do nothing
    }
  }
  LOG_DEBUG("xa start execute", K(stmt.get_xa_string()));
  return ret;
  */
}

int ObPlXaStartExecutor::execute(ObExecContext& ctx, ObXaStartStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  storage::ObPartitionService* ps = nullptr;
  ObTransDesc& trans_desc = my_session->get_trans_desc();
  ObXATransID xid;

  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
    ret = OB_TRANS_XA_RMFAIL;
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(), stmt.get_bqual_string(), stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret), K(stmt.get_xa_string()));
  } else {
    transaction::ObStartTransParam& start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);

    if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, start_trans_param))) {
      LOG_WARN("explicit start trans failed", K(ret), K(start_trans_param));
    } else if (OB_FAIL(ps->xa_start(xid, stmt.get_flags(), my_session->get_xa_end_timeout_seconds(), trans_desc))) {
      LOG_WARN("xa start failed", K(ret), K(start_trans_param));
      // if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
      //  ret = tmp_ret;
      //  LOG_WARN("explicit end trans failed", K(ret));
      //}
      my_session->reset_first_stmt_type();
      my_session->reset_tx_variable();
      my_session->set_early_lock_release(false);
      ctx.set_need_disconnect(false);
      my_session->get_trans_desc().get_standalone_stmt_desc().reset();
    } else {
      // do nothing
    }
  }
  LOG_DEBUG("xa start execute", K(stmt));
  return ret;
}

int ObXaEndExecutor::execute(ObExecContext& ctx, ObXaEndStmt& stmt)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(stmt);
  return ret;
  /*
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  storage::ObPartitionService *ps = nullptr;

  if (OB_ISNULL(my_session) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (OB_FAIL(ps->xa_end(stmt.get_xa_string(),
                                transaction::ObXAEndFlag::TMSUCCESS,
                                my_session->get_trans_desc()))) {
    LOG_WARN("xa end failed", K(ret), K(stmt.get_xa_string()));
    if (OB_TRANS_XA_RMFAIL != ret
        && OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
      ret = tmp_ret;
      LOG_WARN("explicit end trans failed", K(ret));
    }
  } else {
    my_session->reset_first_stmt_type();
    my_session->reset_tx_variable();
    my_session->set_early_lock_release(false);
    ctx.set_need_disconnect(false);
    my_session->get_trans_desc().get_standalone_stmt_desc().reset();
  }
  LOG_DEBUG("xa end execute", K(stmt.get_xa_string()));
  return ret;
  */
}

int ObPlXaEndExecutor::execute(ObExecContext& ctx, ObXaEndStmt& stmt)
{
  int ret = OB_SUCCESS;
  // int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  storage::ObPartitionService* ps = nullptr;
  ObXATransID xid;

  if (OB_ISNULL(my_session) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(), stmt.get_bqual_string(), stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (!my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_PROTO;
    LOG_WARN("not in a trans", K(ret));
  } else if (OB_FAIL(ps->xa_end(xid, stmt.get_flags(), my_session->get_trans_desc()))) {
    LOG_WARN("xa end failed", K(ret), K(stmt.get_xa_string()));
    // if (OB_TRANS_XA_RMFAIL != ret
    //     && OB_SUCCESS != (tmp_ret = ObSqlTransControl::explicit_end_trans(ctx, true))) {
    //   ret = tmp_ret;
    //   LOG_WARN("explicit end trans failed", K(ret));
    // }
  } else {
    my_session->reset_first_stmt_type();
    my_session->reset_tx_variable();
    my_session->set_early_lock_release(false);
    ctx.set_need_disconnect(false);
    my_session->get_trans_desc().get_standalone_stmt_desc().reset();
  }
  LOG_DEBUG("xa end execute", K(stmt));
  return ret;
}

int ObXaPrepareExecutor::execute(ObExecContext& ctx, ObXaPrepareStmt& stmt)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(stmt);
  return ret;
  /*
  int ret = OB_SUCCESS;
  //int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  storage::ObPartitionService *ps = nullptr;
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)
      || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
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

int ObPlXaPrepareExecutor::execute(ObExecContext& ctx, ObXaPrepareStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  storage::ObPartitionService* ps = nullptr;
  ObXATransID xid;
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(my_session), K(plan_ctx), K(ps));
  } else if (OB_FAIL(xid.set(stmt.get_gtrid_string(), stmt.get_bqual_string(), stmt.get_format_id()))) {
    LOG_WARN("set xid error", K(ret), K(stmt));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMERR;
    LOG_WARN("already start trans", K(ret));
  } else {
    const uint64_t tenant_id = my_session->get_effective_tenant_id();
    int64_t timeout = plan_ctx->get_trans_timeout_timestamp();
    transaction::ObStartTransParam& start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);
    if (OB_FAIL(ps->xa_prepare(xid, tenant_id, timeout))) {
      if (OB_TRANS_XA_RDONLY != ret) {
        LOG_WARN("xa prepare failed", K(ret), K(stmt));
      }
    }
  }

  LOG_DEBUG("xa prepare execute", K(stmt));
  return ret;
}

int ObXaEndTransExecutor::execute_(const ObString& xid, const bool is_rollback, ObExecContext& ctx)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(xid);
  UNUSED(is_rollback);
  UNUSED(ctx);
  return ret;
  /*
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObTransDesc &trans_desc = my_session->get_trans_desc();
  storage::ObPartitionService *ps = nullptr;

  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)
      || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_OUTSIDE;
    LOG_WARN("already start trans", K(ret));
  } else {
    transaction::ObStartTransParam &start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);
    if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, start_trans_param))) {
      LOG_WARN("explicit start trans failed", K(ret), K(start_trans_param));
    } else if (OB_FAIL(ps->xa_end_trans(xid, is_rollback, 0, trans_desc))) {
      LOG_WARN("fail to execute xa commit/rollback", K(xid), K(is_rollback));
    } else {
      LOG_DEBUG("succeed to execute xa commit/rollback", K(xid), K(is_rollback));
    }
    my_session->reset_first_stmt_type();
    my_session->reset_tx_variable();
    my_session->set_early_lock_release(false);
    ctx.set_need_disconnect(false);
    my_session->get_trans_desc().get_standalone_stmt_desc().reset();
  }
  LOG_DEBUG("xa end_trans execute", K(xid), K(is_rollback));
  return ret;
  */
}

int ObPlXaEndTransExecutor::execute_(const ObString& gtrid_str, const ObString& bqual_str, const int64_t format_id,
    const bool is_rollback, const int64_t flags, ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObTransDesc& trans_desc = my_session->get_trans_desc();
  storage::ObPartitionService* ps = nullptr;
  ObXATransID xid;

  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session));
  } else if (OB_FAIL(xid.set(gtrid_str, bqual_str, format_id))) {
    LOG_WARN("set xid error", K(ret), K(gtrid_str), K(bqual_str), K(format_id));
  } else if (my_session->get_in_transaction()) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_WARN("already start trans", K(ret));
  } else {
    transaction::ObStartTransParam& start_trans_param = plan_ctx->get_start_trans_param();
    init_start_trans_param(my_session, task_exec_ctx, start_trans_param);
    if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, start_trans_param))) {
      LOG_WARN("explicit start trans failed", K(ret), K(start_trans_param));
    } else if (OB_FAIL(ps->xa_end_trans(xid, is_rollback, flags, trans_desc))) {
      LOG_WARN("fail to execute xa commit/rollback", K(xid), K(is_rollback));
    } else {
      LOG_DEBUG("succeed to execute xa commit/rollback", K(xid), K(is_rollback));
    }
    my_session->reset_first_stmt_type();
    my_session->reset_tx_variable();
    my_session->set_early_lock_release(false);
    ctx.set_need_disconnect(false);
    my_session->get_trans_desc().get_standalone_stmt_desc().reset();
  }
  LOG_DEBUG("xa end_trans execute", K(gtrid_str), K(bqual_str), K(format_id), K(is_rollback), K(flags));
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
