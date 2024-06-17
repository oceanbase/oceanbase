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
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_dbms_xa.h"
#include "sql/dblink/ob_tm_service.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObEndTransExecutor::execute(ObExecContext &ctx, ObEndTransStmt &stmt)
{
  return end_trans(ctx, stmt);
}

int ObEndTransExecutor::end_trans(ObExecContext &ctx, ObEndTransStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret));
  } else if (my_session->is_in_transaction() &&
             my_session->associated_xa()) {
#ifdef OB_BUILD_ORACLE_PL
    transaction::ObTxDesc *tx_desc = my_session->get_tx_desc();
    const transaction::ObXATransID xid = my_session->get_xid();
    const transaction::ObGlobalTxType global_tx_type = tx_desc->get_global_tx_type(xid);
    if (transaction::ObGlobalTxType::XA_TRANS == global_tx_type) {
      if (stmt.get_is_rollback()) {
        // rollback can be executed in xa trans
        // NOTE that rollback does not finish the xa trans,
        // it only rollbacks all actions of the trans
        if (OB_FAIL(pl::ObDbmsXA::xa_rollback_origin_savepoint(ctx))) {
          LOG_WARN("rollback xa changes failed", K(ret), K(xid), K(global_tx_type));
        }
      } else {
        // commit is not allowed in xa trans
        ret = OB_TRANS_XA_ERR_COMMIT;
        LOG_WARN("COMMIT is not allowed in a xa trans", K(ret), K(xid), K(global_tx_type),
            KPC(tx_desc));
      }
    } else if (transaction::ObGlobalTxType::DBLINK_TRANS == global_tx_type) {
      transaction::ObTransID tx_id;
      if (stmt.get_is_rollback()) {
        if (OB_FAIL(ObTMService::tm_rollback(ctx, tx_id))) {
          LOG_WARN("fail to do rollback for dblink trans", K(ret), K(tx_id), K(xid),
              K(global_tx_type));
        }
        my_session->restore_auto_commit();
      } else {
        if (OB_FAIL(ObTMService::tm_commit(ctx, tx_id))) {
          LOG_WARN("fail to do commit for dblink trans", K(ret), K(tx_id), K(xid),
              K(global_tx_type));
        }
        my_session->restore_auto_commit();
      }
      const bool force_disconnect = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = my_session->get_dblink_context().clean_dblink_conn(force_disconnect)))) {
        LOG_WARN("dblink transaction failed to release dblink connections", K(tmp_ret), K(tx_id), K(xid));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected global trans type", K(ret), K(xid), K(global_tx_type), KPC(tx_desc));
    }
    ctx.set_need_disconnect(false);
#endif
  } else if (OB_FAIL(ObSqlTransControl::explicit_end_trans(ctx, stmt.get_is_rollback(), stmt.get_hint()))) {
    LOG_WARN("fail end trans", K(ret));
  }
  return ret;
}

int ObStartTransExecutor::execute(ObExecContext &ctx, ObStartTransStmt &stmt)
{
  return start_trans(ctx, stmt);
}

int ObStartTransExecutor::start_trans(ObExecContext &ctx, ObStartTransStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, stmt.get_read_only(), stmt.get_hint()))) {
    LOG_WARN("fail start trans", K(ret));
  }
  return ret;
}

int ObCreateSavePointExecutor::execute(ObExecContext &ctx,
                                       ObCreateSavePointStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  bool ac = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (OB_FAIL(ObSqlTransControl::create_savepoint(ctx, stmt.get_sp_name(), true))) {
    LOG_WARN("fail create savepoint", K(ret), K(stmt.get_sp_name()));
  } else if (!session->has_explicit_start_trans()) {
    if (OB_FAIL(session->get_autocommit(ac))) {
      LOG_WARN("session autocommit unknown, assume `True`", K(ret), KPC(session));
      ac = true;
    }
    // sync commit trans, it should fast because of tx is clean
    if (ac && OB_FAIL(ObSqlTransControl::implicit_end_trans(ctx, false, NULL))) {
      // overwrite ret
      LOG_WARN("auto commit transaction fail", K(ret), KPC(session));
    }
  }
  return ret;
}

int ObRollbackSavePointExecutor::execute(ObExecContext &ctx,
                                         ObRollbackSavePointStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(ctx, stmt.get_sp_name()))) {
    LOG_WARN("fail rollback to savepoint", K(ret), K(stmt.get_sp_name()));
  }
  return ret;
}

int ObReleaseSavePointExecutor::execute(ObExecContext &ctx,
                                        ObReleaseSavePointStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (OB_FAIL(ObSqlTransControl::release_savepoint(ctx, stmt.get_sp_name()))) {
    LOG_WARN("fail release savepoint", K(ret), K(stmt.get_sp_name()));
  }
  return ret;
}

}// ns sql
}// ns oceanbase
