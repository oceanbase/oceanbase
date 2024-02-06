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
#include "ob_trigger_executor.h"
#include "sql/resolver/ddl/ob_trigger_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "pl/ob_pl_package.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "sql/resolver/ddl/ob_trigger_resolver.h"

namespace oceanbase
{
using namespace obrpc;
using namespace pl;
using namespace common;
using namespace share::schema;

namespace sql
{
int ObCreateTriggerExecutor::execute(ObExecContext &ctx, ObCreateTriggerStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObCreateTriggerArg &arg = stmt.get_trigger_arg();
  uint64_t tenant_id = arg.trigger_info_.get_tenant_id();
  ObString first_stmt;
  OZ (stmt.get_first_stmt(first_stmt));
  arg.ddl_stmt_str_ = first_stmt;
  OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
  OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
  OV (OB_NOT_NULL(common_rpc_proxy));
  OZ (common_rpc_proxy->create_trigger(arg), common_rpc_proxy->get_server());
  //这里需要刷新schema，否则可能获取不到最新的trigger_info
  OZ (ObSPIService::force_refresh_schema(tenant_id));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_task_exec_ctx().schema_service_));
  OZ (ctx.get_task_exec_ctx().schema_service_->
      get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(),
                              *ctx.get_sql_ctx()->schema_guard_));
  OZ (analyze_dependencies(*ctx.get_sql_ctx()->schema_guard_,
                           ctx.get_my_session(),
                           ctx.get_sql_proxy(),
                           ctx.get_allocator(),
                           arg));
  if (OB_SUCC(ret)) {
    arg.ddl_stmt_str_.reset();
    OZ (common_rpc_proxy->create_trigger(arg), common_rpc_proxy->get_server());
  }
  OZ (ctx.get_sql_ctx()->schema_guard_->reset());
  return ret;
}

int ObDropTriggerExecutor::execute(ObExecContext &ctx, ObDropTriggerStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObDropTriggerArg &arg = stmt.get_trigger_arg();
  ObString first_stmt;
  OZ (stmt.get_first_stmt(first_stmt));
  arg.ddl_stmt_str_ = first_stmt;
  OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
  OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
  OV (OB_NOT_NULL(common_rpc_proxy));
  OZ (common_rpc_proxy->drop_trigger(arg), common_rpc_proxy->get_server());
  return ret;
}

int ObAlterTriggerExecutor::execute(ObExecContext &ctx, ObAlterTriggerStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObAlterTriggerArg &arg = stmt.get_trigger_arg();
  ObString first_stmt;
  OZ (stmt.get_first_stmt(first_stmt));
  arg.ddl_stmt_str_ = first_stmt;
  OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
  OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
  OV (OB_NOT_NULL(common_rpc_proxy));
  if (OB_SUCC(ret) && !arg.is_alter_compile_) {
    OZ (common_rpc_proxy->alter_trigger(arg), common_rpc_proxy->get_server());
  }
  return ret;
}

int ObCreateTriggerExecutor::analyze_dependencies(ObSchemaGetterGuard &schema_guard,
                                                  ObSQLSessionInfo *session_info,
                                                  ObMySQLProxy *sql_proxy,
                                                  ObIAllocator &allocator,
                                                  ObCreateTriggerArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.trigger_info_.get_tenant_id();
  const ObString &trigger_name = arg.trigger_info_.get_trigger_name();\
  const ObString &db_name = arg.trigger_database_;
  const ObTriggerInfo *trigger_info = NULL;
  if (OB_FAIL(schema_guard.get_trigger_info(tenant_id, arg.trigger_info_.get_database_id(),
                                            trigger_name, trigger_info))) {
    LOG_WARN("failed to get trigger info", K(ret));
  } else if (NULL == trigger_info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trigger info is null", K(db_name), K(trigger_name), K(ret));
  } else {
    if (OB_FAIL(ObTriggerResolver::analyze_trigger(schema_guard, session_info, sql_proxy,
                                                   allocator, *trigger_info, db_name, arg.dependency_infos_, false))) {
      LOG_WARN("analyze trigger failed", K(trigger_info), K(db_name), K(ret));
    }
    if (OB_FAIL(ret) && ret != OB_ERR_UNEXPECTED) {
        LOG_USER_WARN(OB_ERR_TRIGGER_COMPILE_ERROR, "TRIGGER",
                      db_name.length(), db_name.ptr(),
                      trigger_name.length(), trigger_name.ptr());
        ObPL::insert_error_msg(ret);
        ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      arg.trigger_info_.deep_copy(*trigger_info);
      arg.error_info_.collect_error_info(&arg.trigger_info_);
      arg.in_second_stage_ = true;
      arg.with_replace_ = true;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
