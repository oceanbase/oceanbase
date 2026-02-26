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
#include "sql/engine/cmd/ob_catalog_executor.h"
#include "sql/resolver/ddl/ob_catalog_stmt.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObCatalogExecutor::execute(ObExecContext &ctx, ObCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString null_string;
  uint64_t catalog_id = 0;
  uint64_t drop_catalog_id = 0;
  ObSQLSessionInfo *session = NULL;
  ObObj obj;
  stmt.get_ddl_arg().ddl_stmt_str_ = stmt.get_query_ctx()->get_sql_stmt();
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->handle_catalog_ddl(stmt.get_ddl_arg()))) {
    LOG_WARN("handle catalog ddl error", K(ret));
  }
  if (OB_SUCC(ret) && OB_DDL_DROP_CATALOG == stmt.get_ddl_arg().ddl_type_) {
    drop_catalog_id = stmt.get_ddl_arg().schema_.get_catalog_id();
    obj.set_uint64(OB_INTERNAL_CATALOG_ID);
    if (OB_ISNULL(session = ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR__CURRENT_DEFAULT_CATALOG, catalog_id))) {
      LOG_WARN("failed to get current default catalog id", K(ret));
    } else if (catalog_id == drop_catalog_id && OB_INVALID_ID != drop_catalog_id) {
      // drop current default catalog, set catalog null
      if (OB_FAIL(session->update_sys_variable(share::SYS_VAR__CURRENT_DEFAULT_CATALOG, obj))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(ctx.get_my_session()->set_default_database(null_string))) {
        LOG_WARN("failed to set default database", K(ret));
      } else {
        ctx.get_my_session()->set_database_id(OB_INVALID_ID);
      }
    }
  }
  return ret;
}

int ObSetCatalogExecutor::execute(ObExecContext &ctx, ObCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObObj obj;
  ObString null_string;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FALSE_IT(obj.set_uint64(stmt.get_ddl_arg().schema_.get_catalog_id()))) {
    // do nothing
  } else if (OB_FAIL(session->update_sys_variable(share::SYS_VAR__CURRENT_DEFAULT_CATALOG, obj))) {
    LOG_WARN("failed to update sys var", K(ret));
  } else if (OB_FAIL(ctx.get_my_session()->set_default_database(null_string))) {
    LOG_WARN("failed to set default database", K(ret));
  } else {
    ctx.get_my_session()->set_database_id(OB_INVALID_ID);
  }
  return ret;
}

}
}
