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

int ObCompileTriggerInf::compile_trigger(sql::ObExecContext &ctx,
                                          uint64_t tenant_id,
                                          uint64_t db_id,
                                          const ObString &trigger_name,
                                          int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = nullptr;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (ctx.get_sql_ctx()->schema_guard_->get_trigger_info(tenant_id, db_id, trigger_name, trigger_info));
  CK (OB_NOT_NULL(trigger_info));
  CK (OB_NOT_NULL(ctx.get_pl_engine()));
  if (OB_SUCC(ret) && schema_version == trigger_info->get_schema_version()) {
    ObPLPackage *package_spec = nullptr;
    ObPLPackage *package_body = nullptr;
    pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(ctx.get_allocator(),
                                    *ctx.get_my_session(),
                                    *ctx.get_sql_ctx()->schema_guard_,
                                    package_guard,
                                    *ctx.get_sql_proxy(),
                                    false);

    OZ (package_guard.init());
    OZ (ctx.get_pl_engine()->get_package_manager().get_cached_package(resolve_ctx,
                                                                      trigger_info->get_package_spec_info().get_package_id(),
                                                                      package_spec,
                                                                      package_body));
    CK (OB_NOT_NULL(package_spec));
    CK (OB_NOT_NULL(package_body));
  }

  return ret;
}

int ObCreateTriggerExecutor::execute(ObExecContext &ctx, ObCreateTriggerStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObCreateTriggerArg &arg = stmt.get_trigger_arg();
  uint64_t tenant_id = arg.trigger_info_.get_tenant_id();
  bool has_error = false;
  ObString first_stmt;
  obrpc::ObCreateTriggerRes res;
  bool with_res = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_1_2);
  pl::ObPL *pl_engine = nullptr;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
  CK (OB_NOT_NULL(pl_engine = ctx.get_my_session()->get_pl_engine()));
  OZ (stmt.get_first_stmt(first_stmt));
  arg.ddl_stmt_str_ = first_stmt;
  OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
  OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
  OV (OB_NOT_NULL(common_rpc_proxy));
  if (with_res) {
    OZ (common_rpc_proxy->create_trigger_with_res(arg, res), common_rpc_proxy->get_server());
  } else {
    OZ (common_rpc_proxy->create_trigger(arg), common_rpc_proxy->get_server());
  }
  //这里需要刷新schema，否则可能获取不到最新的trigger_info
  OZ (ObSPIService::force_refresh_schema(tenant_id));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
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
  OX (has_error = ERROR_STATUS_HAS_ERROR == arg.error_info_.get_error_status());
  OZ (ctx.get_sql_ctx()->schema_guard_->reset());
  if (OB_SUCC(ret)) {
    arg.ddl_stmt_str_.reset();
    if (with_res) {
      arg.based_schema_object_infos_.reset();
      OZ (arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(arg.trigger_info_.get_base_object_id(),
                                                                           TABLE_SCHEMA,
                                                                           res.table_schema_version_)));
      OZ (arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(arg.trigger_info_.get_trigger_id(),
                                                                           TRIGGER_SCHEMA,
                                                                           res.trigger_schema_version_)));
      OZ (common_rpc_proxy->create_trigger_with_res(arg, res), common_rpc_proxy->get_server());
      if (OB_ERR_PARALLEL_DDL_CONFLICT == ret) {
        LOG_WARN("trigger or base table maybe changed by other session, ignore the error", K(ret), K(res));
        ret = OB_SUCCESS;
      }
    } else {
      OZ (common_rpc_proxy->create_trigger(arg), common_rpc_proxy->get_server());
    }
  }
  if (OB_SUCC(ret) && !has_error
      && ((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
           && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
          || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0)
      && tenant_config.is_valid() && tenant_config->plsql_v2_compatibility) {
    OZ (ObSPIService::force_refresh_schema(arg.trigger_info_.get_tenant_id(), res.trigger_schema_version_));
    OZ (ctx.get_task_exec_ctx().schema_service_->
          get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
    OZ (compile_trigger(ctx,
                        arg.trigger_info_.get_tenant_id(),
                        arg.trigger_info_.get_database_id(),
                        arg.trigger_info_.get_trigger_name(),
                        res.trigger_schema_version_));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to persistent trigger", K(ret));
      ret = OB_SUCCESS;
      if (NULL != ctx.get_my_session()) {
        ctx.get_my_session()->reset_warnings_buf();
      }
    }
  }
  if(arg.with_if_not_exist_ && ret == OB_ERR_TRIGGER_ALREADY_EXIST) {
    const ObString &trigger_name = arg.trigger_info_.get_trigger_name();
    LOG_WARN("trigger with if not exist grammar, ignore the error", K(ret), K(arg.with_if_not_exist_), K(trigger_name));
    LOG_USER_WARN(OB_ERR_TRIGGER_ALREADY_EXIST, trigger_name.length(), trigger_name.ptr());
    ret = OB_SUCCESS;
  }
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
  pl::ObPL *pl_engine = nullptr;
  CK (arg.trigger_infos_.count() > 0);
  CK (OB_NOT_NULL(pl_engine = ctx.get_my_session()->get_pl_engine()));
  OZ (stmt.get_first_stmt(first_stmt));
  if (OB_SUCC(ret)) {
    const ObTriggerInfo& trigger_info = arg.trigger_infos_.at(0);
    int64_t latest_schema_version = OB_INVALID_VERSION;
    arg.ddl_stmt_str_ = first_stmt;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
    OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
    OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
    OV (OB_NOT_NULL(common_rpc_proxy));
    if (OB_FAIL(ret)) {
    } else if (!arg.is_alter_compile_) {
      obrpc::ObRoutineDDLRes res;
      bool with_res = (GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
                       && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                      || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0;
      if (with_res) {
        OZ (common_rpc_proxy->alter_trigger_with_res(arg, res), common_rpc_proxy->get_server());
      } else {
        OZ (common_rpc_proxy->alter_trigger(arg), common_rpc_proxy->get_server());
      }
      if (OB_SUCC(ret) && with_res) {
        OZ (ObSPIService::force_refresh_schema(trigger_info.get_tenant_id(), res.store_routine_schema_version_));
        OX (latest_schema_version = res.store_routine_schema_version_);
      }
    } else {
      latest_schema_version = trigger_info.get_schema_version();
    }
    if (OB_SUCC(ret) &&
        tenant_config.is_valid() &&
        tenant_config->plsql_v2_compatibility) {
      OZ (ctx.get_task_exec_ctx().schema_service_->
          get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
      OZ (compile_trigger(ctx,
                          trigger_info.get_tenant_id(),
                          trigger_info.get_database_id(),
                          trigger_info.get_trigger_name(),
                          latest_schema_version));
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to persistent trigger", K(ret));
        common::ob_reset_tsi_warning_buffer();
        ret = OB_SUCCESS;
      }
    }
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
  const ObString &trigger_name = arg.trigger_info_.get_trigger_name();
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
