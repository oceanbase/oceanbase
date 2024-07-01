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
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "pl/ob_pl_resolver.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share::schema;

int ObCompilePackageInf::compile_package(sql::ObExecContext &ctx,
                                          uint64_t tenant_id,
                                          const ObString &db_name,
                                          schema::ObPackageType type,
                                          const ObString &package_name,
                                          int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObPackageInfo *package_info = nullptr;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (schema_checker.init(*ctx.get_sql_ctx()->schema_guard_, ctx.get_my_session()->get_sessid()));
  OZ (schema_checker.get_package_info(tenant_id, db_name, package_name, type, compatible_mode, package_info));
  CK (OB_NOT_NULL(package_info));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_pl_engine()));
  if (OB_SUCC(ret) && schema_version == package_info->get_schema_version()) {
    const ObPackageInfo *package_spec_info = NULL;
    const ObPackageInfo *package_body_info = NULL;
    pl::ObPLPackage *package_spec = nullptr;
    pl::ObPLPackage *package_body = nullptr;
    pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(ctx.get_allocator(),
                                    *ctx.get_my_session(),
                                    *ctx.get_sql_ctx()->schema_guard_,
                                    package_guard,
                                    *ctx.get_sql_proxy(),
                                    false);

    OZ (package_guard.init());
    OZ (ctx.get_pl_engine()->get_package_manager().get_package_schema_info(resolve_ctx.schema_guard_,
                                                                           package_info->get_package_id(),
                                                                           package_spec_info,
                                                                           package_body_info));
    // trigger compile package & add to disk & add to pl cache only has package body
    if (OB_SUCC(ret) && OB_NOT_NULL(package_body_info)) {
      OZ (ctx.get_pl_engine()->get_package_manager().get_cached_package(resolve_ctx, package_info->get_package_id(), package_spec, package_body));
      CK (OB_NOT_NULL(package_spec));
    }
  }

  return ret;
}

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
  bool with_res = ((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
                    && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                   || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0);
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
  if (OB_SUCC(ret) && !has_error && with_res &&
      tenant_config.is_valid() &&
      tenant_config->plsql_v2_compatibility) {
    OZ (ObSPIService::force_refresh_schema(tenant_id, res.store_routine_schema_version_));
    OZ (ctx.get_task_exec_ctx().schema_service_->
      get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
    OZ (compile_package(ctx, tenant_id, db_name, type, package_name, res.store_routine_schema_version_));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to persitent package", K(ret));
      common::ob_reset_tsi_warning_buffer();
      ret = OB_SUCCESS;
      if (NULL != ctx.get_my_session()) {
        ctx.get_my_session()->reset_warnings_buf();
      }
    }
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
  bool with_res = ((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
                    && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                   || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0);
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
    OZ (compile_package(ctx, tenant_id, db_name, type, package_name, res.store_routine_schema_version_));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to persitent package", K(ret));
      ret = OB_SUCCESS;
    }
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




