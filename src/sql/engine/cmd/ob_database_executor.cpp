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

#include "sql/engine/cmd/ob_database_executor.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/ddl/ob_alter_database_stmt.h"
#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/worker.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObCreateDatabaseExecutor::ObCreateDatabaseExecutor()
{
}

ObCreateDatabaseExecutor::~ObCreateDatabaseExecutor()
{
}

int ObCreateDatabaseExecutor::execute(ObExecContext &ctx, ObCreateDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateDatabaseArg &create_database_arg = stmt.get_create_database_arg();
  obrpc::ObCreateDatabaseArg &tmp_arg = const_cast<obrpc::ObCreateDatabaseArg&>(create_database_arg);
  ObString first_stmt;
  obrpc::UInt64 database_id(0);
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
   } else if (OB_ISNULL(common_rpc_proxy)
              || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx), K(common_rpc_proxy));
  } else {
    //为什么create database的协议需要返回database_id，暂时没有用上。
    if (OB_FAIL(common_rpc_proxy->create_database(create_database_arg, database_id))) {
      SQL_ENG_LOG(WARN, "rpc proxy create table failed", K(ret));
    } else {
      ctx.get_physical_plan_ctx()->set_affected_rows(1);
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "create database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", database_id,
      "schema_version", create_database_arg.database_schema_.get_schema_version());
  }
  SQL_ENG_LOG(INFO, "finish execute create database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

/////////////////////
ObUseDatabaseExecutor::ObUseDatabaseExecutor()
{
}

ObUseDatabaseExecutor::~ObUseDatabaseExecutor()
{
}

int ObUseDatabaseExecutor::execute(ObExecContext &ctx, ObUseDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "session is NULL");
  } else {
    const bool is_oracle_mode = lib::is_oracle_mode();
    const bool is_oceanbase_db = is_oceanbase_sys_database_id(stmt.get_db_id());
    if (session->is_tenant_changed() && !is_oceanbase_db) {
      ret = OB_OP_NOT_ALLOW;
      SQL_ENG_LOG(WARN, "tenant changed, access non oceanbase database not allowed", K(ret), K(stmt),
                  "login_tenant_id", session->get_login_tenant_id(),
                  "effective_tenant_id", session->get_effective_tenant_id());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant changed, access non oceanbase database");
    } else if (is_oracle_mode && is_oceanbase_db) {
      ret = OB_OP_NOT_ALLOW;
      SQL_ENG_LOG(WARN, "cannot access oceanbase database on tenant with oracle compatiblity", K(ret),
                  "effective_tenant_id", session->get_effective_tenant_id());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "access oceanbase database");
    } else {
      ObCollationType db_coll_type = ObCharset::collation_type(stmt.get_db_collation());
      if (OB_UNLIKELY(CS_TYPE_INVALID == db_coll_type)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(ERROR, "invalid collation", K(ret), K(stmt.get_db_name()), K(stmt.get_db_collation()));
      } else if (OB_FAIL(session->set_default_database(stmt.get_db_name(), db_coll_type))) {
        SQL_ENG_LOG(WARN, "fail to set default database", K(ret), K(stmt.get_db_name()), K(stmt.get_db_collation()), K(db_coll_type));
      } else {
        session->set_db_priv_set(stmt.get_db_priv_set());
        SQL_ENG_LOG(INFO, "use default database", "db", stmt.get_db_name());
        session->set_database_id(stmt.get_db_id());
      }
    }
  }
  return ret;
}

//////////////////
ObAlterDatabaseExecutor::ObAlterDatabaseExecutor()
{
}

ObAlterDatabaseExecutor::~ObAlterDatabaseExecutor()
{
}

int ObAlterDatabaseExecutor::execute(ObExecContext &ctx, ObAlterDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObAlterDatabaseArg &alter_database_arg = stmt.get_alter_database_arg();
  obrpc::ObAlterDatabaseArg &tmp_arg = const_cast<obrpc::ObAlterDatabaseArg&>(alter_database_arg);
  ObString first_stmt;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "session is NULL");
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->alter_database(alter_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy alter table failed", K(ret));
  } else if (! stmt.get_alter_option_set().has_member(obrpc::ObAlterDatabaseArg::COLLATION_TYPE)) {
    // do nothing
  } else if (0 == stmt.get_database_name().compare(session->get_database_name())) {
    const int64_t db_coll = static_cast<int64_t>(stmt.get_collation_type());
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_CHARACTER_SET_DATABASE, db_coll))) {
      SQL_ENG_LOG(WARN, "failed to update sys variable", K(ret));
    } else if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_COLLATION_DATABASE, db_coll))) {
      SQL_ENG_LOG(WARN, "failed to update sys variable", K(ret));
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "alter database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", alter_database_arg.database_schema_.get_database_id(),
      "schema_version", alter_database_arg.database_schema_.get_schema_version());
  }
    SQL_ENG_LOG(INFO, "finish execute alter database", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


//////////////////
ObDropDatabaseExecutor::ObDropDatabaseExecutor()
{
}

ObDropDatabaseExecutor::~ObDropDatabaseExecutor()
{
}

int ObDropDatabaseExecutor::execute(ObExecContext &ctx, ObDropDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropDatabaseArg &drop_database_arg = stmt.get_drop_database_arg();
  obrpc::ObDropDatabaseArg &tmp_arg = const_cast<obrpc::ObDropDatabaseArg&>(drop_database_arg);
  ObString first_stmt;
  uint64_t database_id = 0;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ctx), K(common_rpc_proxy));
  } else {
    obrpc::UInt64 affected_row(0);
    obrpc::ObDropDatabaseRes drop_database_res;
    const_cast<obrpc::ObDropDatabaseArg&>(drop_database_arg).compat_mode_ = ORACLE_MODE == ctx.get_my_session()->get_compatibility_mode()
        ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
    if (OB_FAIL(common_rpc_proxy->drop_database(drop_database_arg, drop_database_res))) {
      SQL_ENG_LOG(WARN, "rpc proxy drop table failed",
                  "timeout", THIS_WORKER.get_timeout_remain(), K(ret));
    } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx));
    } else {
      ObString null_string;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(ctx.get_my_session()->get_name_case_mode(case_mode))) {
        SQL_ENG_LOG(WARN, "fail to get name case mode from session", K(ret));
      } else if (ObCharset::case_mode_equal(case_mode,
                                            ctx.get_my_session()->get_database_name(),
                                            drop_database_arg.database_name_)) {
        ObCollationType server_coll_type = ObCharset::collation_type(stmt.get_server_collation());
        if (OB_UNLIKELY(CS_TYPE_INVALID == server_coll_type)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(ERROR, "invalid collation", K(ret), K(stmt.get_server_collation()));
        } else if (OB_FAIL(ctx.get_my_session()->set_default_database(null_string, server_coll_type))) {
          SQL_ENG_LOG(WARN, "fail to set default database", K(ret), K(stmt.get_server_collation()), K(server_coll_type));
        } else {
          ctx.get_my_session()->set_database_id(OB_INVALID_ID);
        }
      }
    }
    if (OB_SUCC(ret)) {
      ctx.get_physical_plan_ctx()->set_affected_rows(drop_database_res.affected_row_);
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "drop database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", database_id);
  }
  SQL_ENG_LOG(INFO, "finish execute drop database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObFlashBackDatabaseExecutor::execute(ObExecContext &ctx, ObFlashBackDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObFlashBackDatabaseArg &flashback_database_arg = stmt.get_flashback_database_arg();
  obrpc::ObFlashBackDatabaseArg &tmp_arg = const_cast<obrpc::ObFlashBackDatabaseArg&>(flashback_database_arg);
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->flashback_database(flashback_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy flashback database failed", K(ret));
  }

  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "flashback database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "origin_db_name", flashback_database_arg.origin_db_name_,
      "new_db_name", flashback_database_arg.new_db_name_);
  }
  SQL_ENG_LOG(INFO, "finish execute flashback database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObPurgeDatabaseExecutor::execute(ObExecContext &ctx, ObPurgeDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObPurgeDatabaseArg &purge_database_arg = stmt.get_purge_database_arg();
  obrpc::ObPurgeDatabaseArg &tmp_arg = const_cast<obrpc::ObPurgeDatabaseArg&>(purge_database_arg);
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->purge_database(purge_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy purge database failed", K(ret));
  }

  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "purge database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", purge_database_arg.db_name_);
  }
  SQL_ENG_LOG(INFO, "finish purge database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


}
}
