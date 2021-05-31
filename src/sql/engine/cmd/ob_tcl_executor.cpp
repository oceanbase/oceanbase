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
#include "sql/engine/cmd/ob_tcl_executor.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/resolver/tcl/ob_end_trans_stmt.h"
#include "sql/resolver/tcl/ob_start_trans_stmt.h"
#include "sql/resolver/tcl/ob_savepoint_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

int ObEndTransExecutor::execute(ObExecContext& ctx, ObEndTransStmt& stmt)
{
  return end_trans(ctx, stmt);
}

int ObEndTransExecutor::end_trans(ObExecContext& ctx, ObEndTransStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret));
  } else if (my_session->is_in_transaction() && my_session->get_trans_desc().is_xa_local_trans()) {
    transaction::ObXATransID xid = my_session->get_trans_desc().get_xid();
    if (stmt.get_is_rollback()) {
      // Rollback can be executed in the xa transaction,
      // the role is to roll back all modifications, but does not end the xa transaction
    } else {
      // commit is prohibited in xa transaction
      ret = OB_TRANS_XA_ERR_COMMIT;
      LOG_WARN("COMMIT is not allowed in a xa trans", K(ret), K(xid));
    }
    ctx.set_need_disconnect(false);
  } else if (OB_FAIL(ObSqlTransControl::explicit_end_trans(ctx, stmt.get_is_rollback()))) {
    LOG_WARN("fail end trans", K(ret));
  }
  return ret;
}

int ObStartTransExecutor::execute(ObExecContext& ctx, ObStartTransStmt& stmt)
{
  return start_trans(ctx, stmt);
}

int ObStartTransExecutor::start_trans(ObExecContext& ctx, ObStartTransStmt& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, stmt.get_read_only()))) {
    LOG_WARN("fail start trans", K(ret));
  }
  return ret;
}

int ObCreateSavePointExecutor::execute(ObExecContext& ctx, ObCreateSavePointStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (!session->is_in_transaction() && !session->get_local_autocommit() &&
             OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, session->get_tx_read_only()))) {
    LOG_WARN("fail start trans", K(ret));
  } else if (OB_FAIL(ObSqlTransControl::create_savepoint(ctx, stmt.get_sp_name()))) {
    LOG_WARN("fail create savepoint", K(ret), K(stmt.get_sp_name()));
  }
  return ret;
}

int ObRollbackSavePointExecutor::execute(ObExecContext& ctx, ObRollbackSavePointStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (!session->is_in_transaction()) {
    ret = OB_SAVEPOINT_NOT_EXIST;
    LOG_WARN("savepoint is not exist", K(ret), K(stmt.get_sp_name()));
  } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(ctx, stmt.get_sp_name()))) {
    LOG_WARN("fail rollback to savepoint", K(ret), K(stmt.get_sp_name()));
  }
  return ret;
}

int ObReleaseSavePointExecutor::execute(ObExecContext& ctx, ObReleaseSavePointStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (!session->is_in_transaction()) {
    ret = OB_SAVEPOINT_NOT_EXIST;
    LOG_WARN("savepoint is not exist", K(ret), K(stmt.get_sp_name()));
  } else if (OB_FAIL(ObSqlTransControl::release_savepoint(ctx, stmt.get_sp_name()))) {
    LOG_WARN("fail release savepoint", K(ret), K(stmt.get_sp_name()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
