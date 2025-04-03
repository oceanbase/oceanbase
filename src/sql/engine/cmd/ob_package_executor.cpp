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
#include "ob_package_executor.h"
#include "sql/resolver/ddl/ob_create_package_stmt.h"
#include "sql/resolver/ddl/ob_alter_package_stmt.h"
#include "sql/resolver/ddl/ob_drop_package_stmt.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_compile_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share::schema;

int ObCreatePackageExecutor::execute(ObExecContext &ctx, ObCreatePackageStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 table_id;
  obrpc::ObCreatePackageArg &arg = stmt.get_create_package_arg();
  uint64_t tenant_id = arg.package_info_.get_tenant_id();
  bool has_error = ERROR_STATUS_HAS_ERROR == arg.error_info_.get_error_status();
  ObString &db_name = arg.db_name_;
  const ObString &package_name = arg.package_info_.get_package_name();
  share::schema::ObPackageType type = arg.package_info_.get_type();
  ObString first_stmt;
  obrpc::ObRoutineDDLRes res;
  bool with_res = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_3_0);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (!with_res && OB_FAIL(common_rpc_proxy->create_package(arg))) {
    LOG_WARN("rpc proxy create package failed", K(ret),
             "dst", common_rpc_proxy->get_server());
  } else if (with_res && OB_FAIL(common_rpc_proxy->create_package_with_res(arg, res))) {
    LOG_WARN("rpc proxy create package failed", K(ret),
             "dst", common_rpc_proxy->get_server());
  }
  if (OB_SUCC(ret)
      && !has_error
      && with_res
      && tenant_config.is_valid()
      && tenant_config->plsql_v2_compatibility) {
    OZ (ObSPIService::force_refresh_schema(tenant_id, res.store_routine_schema_version_));
    OZ (ctx.get_task_exec_ctx().schema_service_->
      get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
    OZ (pl::ObPLCompilerUtils::compile(ctx,
                                       tenant_id,
                                       db_name,
                                       package_name,
                                       pl::ObPLCompilerUtils::get_compile_type(type),
                                       res.store_routine_schema_version_));
  }
  return ret;
}

int ObAlterPackageExecutor::execute(ObExecContext &ctx, ObAlterPackageStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 table_id;
  obrpc::ObAlterPackageArg &arg = stmt.get_alter_package_arg();
  uint64_t tenant_id = arg.tenant_id_;
  bool has_error = ERROR_STATUS_HAS_ERROR == arg.error_info_.get_error_status();
  ObString &db_name = arg.db_name_;
  const ObString &package_name = arg.package_name_;
  share::schema::ObPackageType type = arg.package_type_;
  ObString first_stmt;
  obrpc::ObRoutineDDLRes res;
  bool with_res = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_3_0);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  // we need send rpc for alter package, because it must refresh package state after alter package
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (!with_res && OB_FAIL(common_rpc_proxy->alter_package(arg))) {
    LOG_WARN("rpc proxy drop procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
  } else if (with_res && OB_FAIL(common_rpc_proxy->alter_package_with_res(arg, res))) {
    LOG_WARN("rpc proxy drop procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  if (OB_SUCC(ret) && !has_error && with_res &&
      tenant_config.is_valid() &&
      tenant_config->plsql_v2_compatibility) {
    OZ (ObSPIService::force_refresh_schema(tenant_id, res.store_routine_schema_version_));
    OZ (ctx.get_task_exec_ctx().schema_service_->
      get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
    OZ (pl::ObPLCompilerUtils::compile(ctx,
                                       tenant_id,
                                       db_name,
                                       package_name,
                                       pl::ObPLCompilerUtils::get_compile_type(type),
                                       res.store_routine_schema_version_));
  }

  return ret;
}

int ObDropPackageExecutor::execute(ObExecContext &ctx, ObDropPackageStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 table_id;
  obrpc::ObDropPackageArg &arg = stmt.get_drop_package_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->drop_package(arg))) {
    LOG_WARN("rpc proxy drop package failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

}
}




