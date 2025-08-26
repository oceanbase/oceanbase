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

#define USING_LOG_PREFIX PL

#include "ob_dbms_java.h"

#include "pl/ob_pl.h"
#include "share/ob_cluster_version.h"
#include "sql/executor/ob_task_executor_ctx.h"


namespace oceanbase
{

namespace pl
{

int ObDBMSJava::loadjava_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;

  uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  uint64_t database_id = OB_INVALID_ID;

  ObString name;
  ObString content;
  ObString comment;

  ObSchemaGetterGuard *schema_guard = nullptr;
  bool is_exist = true;

  if (3 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tenant_config", K(ret));
  } else if (!tenant_config->ob_enable_java_udf) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ob_enable_java_udf is not enabled", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_0_0 || data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external resource is only supported after 4.4.0", K(ret));
  } else if (OB_FAIL(params.at(0).get_string(name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(content))) {
    LOG_WARN("failed to get content string", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(comment))) {
    LOG_WARN("failed to get comment string", K(ret), K(params.at(2)));
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_my_session()", K(ret));
  } else if (OB_INVALID_ID == (database_id = ctx.exec_ctx_->get_my_session()->get_database_id())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("no database selected", K(ret), K(database_id));
  } else if (name.empty()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_WARN("java jar name is empty", K(ret), K(params), K(name));
    ObString var_name = "JAR_NAME";
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, var_name.length(), var_name.ptr(), name.length(), name.ptr());
  } else if (content.empty()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_WARN("java jar content is empty", K(ret), K(params), K(content));
    ObString var_name = "CONTENT";
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, var_name.length(), var_name.ptr(), content.length(), content.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()->schema_guard_", K(ret));
  } else if (OB_FAIL(schema_guard->check_external_resource_exist(tenant_id, database_id, name, is_exist))) {
    LOG_WARN("failed to check_external_resource_exist", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (OB_UNLIKELY(is_exist)) {
    ret = OB_OBJECT_NAME_EXIST;
    LOG_WARN("external resource with same name already exists", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObCreateExternalResourceArg args(tenant_id, database_id, name, ObSimpleExternalResourceSchema::JAVA_JAR_TYPE, content, comment);
    ObCreateExternalResourceRes res;

    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_);
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;

    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (!args.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid create external resource arg", K(ret), K(args));
    } else if (OB_FAIL(common_rpc_proxy->create_external_resource(args, res))) {
      LOG_WARN("failed to create_external_resource", K(ret), K(args), K(res));
    }

    LOG_INFO("finished to create external resource", K(ret), K(args), K(res));
  }

  return ret;
}

int ObDBMSJava::dropjava_mysql(ObPLExecCtx & ctx, sql::ParamStore & params,
                               common::ObObj & result)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;

  uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  uint64_t database_id = OB_INVALID_ID;

  ObString name;

  ObSchemaGetterGuard *schema_guard = nullptr;
  bool is_exist = false;

  if (1 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tenant_config", K(ret));
  } else if (!tenant_config->ob_enable_java_udf) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ob_enable_java_udf is not enabled", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_0_0 || data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external resource is only supported after 4.4.0", K(ret));
  } else if (OB_FAIL(params.at(0).get_string(name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_my_session()", K(ret));
  } else if (OB_INVALID_ID == (database_id = ctx.exec_ctx_->get_my_session()->get_database_id())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("no database selected", K(ret), K(database_id));
  } else if (name.empty()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_WARN("java jar name is empty", K(ret), K(params), K(name));
    ObString var_name = "JAR_NAME";
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, var_name.length(), var_name.ptr(), name.length(), name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()->schema_guard_", K(ret));
  } else if (OB_FAIL(schema_guard->check_external_resource_exist(tenant_id, database_id, name, is_exist))) {
    LOG_WARN("failed to check_external_resource_exist", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("external resource not exist", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObDropExternalResourceArg args(tenant_id, database_id, name);
    ObDropExternalResourceRes res;

    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_);
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;

    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (!args.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid drop external resource arg", K(ret), K(args));
    } else if (OB_FAIL(common_rpc_proxy->drop_external_resource(args, res))) {
      LOG_WARN("failed to drop_external_resource", K(ret), K(args), K(res));
    }

    LOG_INFO("finished to external resource", K(ret), K(args), K(res));
  }

  return ret;
}

} // namespace pl
} // namespace oceanbase
