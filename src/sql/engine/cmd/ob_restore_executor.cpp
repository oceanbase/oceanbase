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
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/engine/cmd/ob_restore_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "share/backup/ob_multi_backup_dest_util.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "rootserver/restore/ob_restore_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace rootserver;
namespace sql {

ObRestoreTenantExecutor::ObRestoreTenantExecutor()
{}

ObRestoreTenantExecutor::~ObRestoreTenantExecutor()
{}

int ObRestoreTenantExecutor::execute(ObExecContext& ctx, ObRestoreTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObRestoreTenantArg& restore_tenant_arg = stmt.get_rpc_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObRestoreTenantArg&>(restore_tenant_arg).sql_text_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->restore_tenant(restore_tenant_arg))) {
    LOG_WARN("rpc proxy restore tenant failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

ObPhysicalRestoreTenantExecutor::ObPhysicalRestoreTenantExecutor()
{}

ObPhysicalRestoreTenantExecutor::~ObPhysicalRestoreTenantExecutor()
{}

int ObPhysicalRestoreTenantExecutor::execute(ObExecContext& ctx, ObPhysicalRestoreTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  const obrpc::ObPhysicalRestoreTenantArg& restore_tenant_arg = stmt.get_rpc_arg();
  const bool is_preview = stmt.get_is_preview();
  ObString first_stmt;
  ObObj value;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObPhysicalRestoreTenantArg&>(restore_tenant_arg).sql_text_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (session_info->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
    if (OB_FAIL(session_info->get_user_variable_value(OB_RESTORE_SOURCE_NAME_SESSION_STR, value))) {
      LOG_WARN("failed to get user variable value", K(ret));
    } else {
      const_cast<obrpc::ObPhysicalRestoreTenantArg&>(restore_tenant_arg).multi_uri_ = value.get_char();
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    if (!is_preview) {
      if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
        ret = OB_NOT_INIT;
        LOG_WARN("get task executor context failed", K(ret));
      } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
        LOG_WARN("get common rpc proxy failed", K(ret));
      } else if (OB_ISNULL(common_rpc_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("common rpc proxy should not be null", K(ret));
      } else if (OB_FAIL(common_rpc_proxy->physical_restore_tenant(restore_tenant_arg))) {
        LOG_WARN("rpc proxy restore tenant failed", K(ret), "dst", common_rpc_proxy->get_server());
      }
      if (session_info->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
        tmp_ret = session_info->remove_user_variable(OB_RESTORE_SOURCE_NAME_SESSION_STR);
        if (tmp_ret != OB_SUCCESS) {
          tmp_ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("failed to remove user variable", KR(tmp_ret));
        }
      }
    } else {
      if (OB_FAIL(physical_restore_preview(ctx, stmt))) {
        LOG_WARN("failed to do physical restore preview", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTenantExecutor::physical_restore_preview(ObExecContext& ctx, ObPhysicalRestoreTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObArray<ObString> uri_list;
  ObArray<ObSimpleBackupSetPath> set_list;
  ObArray<ObSimpleBackupPiecePath> piece_list;
  ObSqlString set_tenant_id_sql;
  ObSqlString set_backup_dest_sql;
  ObSqlString set_timestamp_sql;
  ObSqlString set_cluster_name_sql;
  ObSqlString set_cluster_id_sql;
  sqlclient::ObISQLConnection* conn = NULL;
  observer::ObInnerSQLConnectionPool* pool = NULL;
  ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  const obrpc::ObPhysicalRestoreTenantArg& restore_tenant_arg = stmt.get_rpc_arg();

  int64_t affected_rows = 0;

  const int64_t fake_job_id = 1;
  ObPhysicalRestoreJob fake_job;

  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_) || OB_ISNULL(sql_proxy->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
  } else if (sqlclient::INNER_POOL != sql_proxy->get_pool()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool type must be inner", K(ret), "type", sql_proxy->get_pool()->get_type());
  } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool must not null", K(ret));
  } else if (OB_FAIL(ObRestoreUtil::fill_physical_restore_job(fake_job_id, restore_tenant_arg, fake_job))) {
    LOG_WARN("failed to fill physical restore job", KR(ret), K(fake_job_id), K(restore_tenant_arg));
  } else if (OB_FAIL(uri_list.push_back(restore_tenant_arg.uri_))) {
    LOG_WARN("failed to push back", KR(ret), K(restore_tenant_arg));
  } else if (OB_FAIL(ObMultiBackupDestUtil::get_backup_tenant_id(uri_list,
                 BACKUP_PATH_CLUSTER_LEVEL,
                 fake_job.backup_cluster_name_,
                 fake_job.cluster_id_,
                 restore_tenant_arg.backup_tenant_name_,
                 restore_tenant_arg.restore_timestamp_,
                 tenant_id))) {
    LOG_WARN("failed to get backup tenant id", KR(ret), K(uri_list), K(restore_tenant_arg));
  } else if (OB_FAIL(set_tenant_id_sql.assign_fmt(
                 "set @%s = '%lu'", OB_RESTORE_PREVIEW_TENANT_ID_SESSION_STR, tenant_id))) {
    LOG_WARN("failed to set tenant id", KR(ret), K(set_tenant_id_sql));
  } else if (OB_FAIL(set_backup_dest_sql.assign_fmt("set @%s = '%.*s'",
                 OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR,
                 restore_tenant_arg.uri_.length(),
                 restore_tenant_arg.uri_.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(set_backup_dest_sql));
  } else if (OB_FAIL(set_timestamp_sql.assign_fmt(
                 "set @%s = '%ld'", OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR, restore_tenant_arg.restore_timestamp_))) {
    LOG_WARN("failed to set timestamp", KR(ret), K(set_timestamp_sql));
  } else if (OB_FAIL(set_cluster_name_sql.assign_fmt("set @%s = '%s'",
                 OB_RESTORE_PREVIEW_BACKUP_CLUSTER_NAME_SESSION_STR,
                 fake_job.backup_cluster_name_))) {
    LOG_WARN("failed to set cluster name", KR(ret), K(set_cluster_name_sql));
  } else if (OB_FAIL(set_cluster_id_sql.assign_fmt(
                 "set @%s = '%ld'", OB_RESTORE_PREVIEW_BACKUP_CLUSTER_ID_SESSION_STR, fake_job.cluster_id_))) {
    LOG_WARN("failed to set cluster id", KR(ret), K(set_cluster_id_sql));
  } else if (OB_FAIL(pool->acquire(session_info, conn))) {
    LOG_WARN("failed to get conn", K(ret));
  } else if (OB_FAIL(conn->execute_write(
                 session_info->get_effective_tenant_id(), set_tenant_id_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set tenant id", K(ret), K(set_tenant_id_sql));
  } else if (OB_FAIL(conn->execute_write(
                 session_info->get_effective_tenant_id(), set_backup_dest_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set backup dest", K(ret), K(set_backup_dest_sql));
  } else if (OB_FAIL(conn->execute_write(
                 session_info->get_effective_tenant_id(), set_timestamp_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set restore timestamp", K(ret), K(set_timestamp_sql));
  } else if (OB_FAIL(conn->execute_write(
                 session_info->get_effective_tenant_id(), set_cluster_name_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set cluster name", K(ret), K(set_timestamp_sql));
  } else if (OB_FAIL(conn->execute_write(
                 session_info->get_effective_tenant_id(), set_cluster_id_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set cluster id", K(ret), K(set_timestamp_sql));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
