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
#include "pl/external_routine/ob_java_utils.h"
#include "share/ob_encryption_util.h"


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
    ObDropExternalResourceArg args(tenant_id, database_id, name,
                                   share::schema::ObSimpleExternalResourceSchema::ResourceType::JAVA_JAR_TYPE);
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

    LOG_INFO("finished to drop external resource", K(ret), K(args), K(res));
  }

  return ret;
}

#ifdef OB_BUILD_ORACLE_PL

int ObDBMSJava::prepare_ora_jar_info(ObPLExecCtx &ctx,
                                     const ObString &jar_binary,
                                     uint64_t &tenant_id,
                                     uint64_t &database_id,
                                     ObSchemaGetterGuard *&schema_guard,
                                     ObSqlString &jar_hash,
                                     ObIArray<std::pair<ObString, ObString>> &classes,
                                     JNIEnv *&env,
                                     jobject &buffer_handle)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;

  tenant_id = MTL_ID();
  database_id = OB_INVALID_ID;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  char hash_buf[SHA512_DIGEST_LENGTH] = {0};
  int64_t hash_buf_out_len = 0;

  if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_my_session()", K(ret));
  } else if (OB_INVALID_ID == (database_id = ctx.exec_ctx_->get_my_session()->get_database_id())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("no database selected", K(ret), K(database_id));
  } else if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tenant_config", K(ret));
  } else if (!tenant_config->ob_enable_java_udf) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ob_enable_java_udf is not enabled", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_2_1 || data_version < DATA_VERSION_4_4_2_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external resource is only supported after 4.4.2", K(ret));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  } else if (OB_FAIL(ObJavaUtils::trans_jar_to_classes(jar_binary, classes, *env, buffer_handle))) {
    LOG_WARN("failed to trans_jar_to_classes", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()->schema_guard_", K(ret));
  } else if (OB_FAIL(share::ObHashUtil::hash(share::OB_HASH_SH256,
                                             jar_binary.ptr(),
                                             jar_binary.length(),
                                             hash_buf,
                                             sizeof(hash_buf),
                                             hash_buf_out_len))) {
    LOG_WARN("failed to calculate hash", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hash_buf_out_len; ++i) {
      if (OB_FAIL(jar_hash.append_fmt("%02x", static_cast<unsigned char>(hash_buf[i])))) {
        LOG_WARN("failed to append hash byte", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObDBMSJava::ob_loadjar(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t database_id = OB_INVALID_ID;
  ObSchemaGetterGuard *schema_guard = nullptr;
  ObSqlString jar_hash;
  ObSEArray<std::pair<ObString, ObString>, 8> classes;
  JNIEnv *env = nullptr;
  jobject buffer_handle = nullptr;

  ObString jar_binary;
  ObString flags;

  if (2 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(params.at(0).get_string(jar_binary))) {
    LOG_WARN("failed to get jar binary string", K(ret), K(params));
  } else if (OB_FAIL(params.at(1).get_string(flags))) {
    LOG_WARN("failed to get flags string", K(ret), K(params));
  } else if (OB_FAIL(prepare_ora_jar_info(ctx,
                                          jar_binary,
                                          tenant_id,
                                          database_id,
                                          schema_guard,
                                          jar_hash,
                                          classes,
                                          env,
                                          buffer_handle))) {
    LOG_WARN("failed to prepare_ora_jar_info", K(ret));
  } else {
    ObSEArray<ObString, 8> class_names;
    bool is_force = flags.case_compare_equal("-F")
                      || flags.case_compare_equal("-force");
    bool jar_exist = false;

    if (OB_FAIL(schema_guard->check_external_resource_exist(tenant_id, database_id, jar_hash.string(), jar_exist))) {
      LOG_WARN("failed to check_external_resource_exist for jar_hash", K(ret), K(tenant_id), K(database_id), K(jar_hash));
    } else if (jar_exist && !is_force) {
      LOG_WARN("external resource with jar_hash already exists, skip loading", K(tenant_id), K(database_id), K(jar_hash), K(is_force));
    } else if (OB_FAIL(class_names.reserve(classes.count()))) {
      LOG_WARN("failed to reserve class names array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < classes.count(); ++i) {
        ObString class_name = classes[i].first;
        bool is_exist = false;

        if (!is_force) {
          if (OB_FAIL(schema_guard->check_external_resource_exist(tenant_id, database_id, class_name, is_exist))) {
            LOG_WARN("failed to check_external_resource_exist", K(ret), K(tenant_id), K(database_id), K(class_name));
          }
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (is_exist) {
          // do nothing, ignore existent Java class
        } else if (OB_FAIL(class_names.push_back(class_name))) {
          LOG_WARN("failed to push back class name", K(ret), K(i), K(classes), K(class_names));
        }
      }

      if (OB_SUCC(ret)) {
        ObOraUploadJarArg args(tenant_id, database_id, jar_hash.string(), jar_binary, is_force);
        ObOraUploadJarRes res;

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
        } else if (class_names.empty()) {
          LOG_INFO("no new class to upload", K(ret), K(classes), K(class_names));
        } else if (OB_FAIL(args.assign_class_names(class_names))) {
          LOG_WARN("failed to assign class names", K(ret), K(args), K(class_names));
        } else if (!args.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid upload jar arg", K(ret), K(args));
        } else if (OB_FAIL(common_rpc_proxy->ora_upload_jar_external_resource(args, res))) {
          LOG_WARN("failed to upload jar RPC", K(ret), K(args), K(res));
        }

        LOG_INFO("finished to upload jar", K(ret), K(args), K(res));
      }
    }
  }

  ObJavaUtils::delete_local_ref(buffer_handle, env);

  return ret;
}

int ObDBMSJava::ob_dropjar(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t database_id = OB_INVALID_ID;
  oceanbase::share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
  ObSqlString jar_hash;
  ObSEArray<std::pair<ObString, ObString>, 8> classes;
  JNIEnv *env = nullptr;
  jobject buffer_handle = nullptr;

  ObString jar_binary;

  if (1 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(params.at(0).get_string(jar_binary))) {
    LOG_WARN("failed to get jar binary string", K(ret), K(params));
  } else if (OB_FAIL(prepare_ora_jar_info(ctx,
                                          jar_binary,
                                          tenant_id,
                                          database_id,
                                          schema_guard,
                                          jar_hash,
                                          classes,
                                          env,
                                          buffer_handle))) {
    LOG_WARN("failed to prepare_ora_jar_info", K(ret));
  } else {
    ObSEArray<ObString, 8> class_names;
    const ObSimpleExternalResourceSchema *jar_schema = nullptr;

    if (OB_FAIL(schema_guard->get_external_resource_schema(tenant_id, database_id, jar_hash.string(), jar_schema))) {
      LOG_WARN("failed to get_external_resource_schema for jar_hash", K(ret), K(tenant_id), K(database_id), K(jar_hash));
    } else if (OB_ISNULL(jar_schema)) {
      // do nothing
    } else if (OB_FAIL(class_names.reserve(classes.count()))) {
      LOG_WARN("failed to reserve class names array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < classes.count(); ++i) {
        ObString class_name = classes[i].first;
        if (OB_FAIL(class_names.push_back(class_name))) {
          LOG_WARN("failed to push back class name", K(ret), K(i), K(classes), K(class_names));
        }
      }

      if (OB_SUCC(ret)) {
        ObOraDropJarArg args(tenant_id, jar_schema->get_resource_id());
        ObOraDropJarRes res;

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
        } else if (OB_FAIL(args.assign_class_names(class_names))) {
          LOG_WARN("failed to assign class names", K(ret), K(args), K(class_names));
        } else if (!args.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid drop jar arg", K(ret), K(args));
        } else if (OB_FAIL(common_rpc_proxy->ora_drop_jar_external_resource(args, res))) {
          LOG_WARN("failed to drop jar RPC", K(ret), K(args), K(res));
        }

        LOG_INFO("finished to drop jar external resource", K(ret), K(args), K(res));
      }
    }
  }

  ObJavaUtils::delete_local_ref(buffer_handle, env);

  return ret;
}

#endif // OB_BUILD_ORACLE_PL

int ObDBMSJava::grant_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ObString grantee_name;
  ObString permission_type;
  ObString permission_name;
  ObString permission_action;
  uint64_t key = OB_INVALID_ID;

  if (4 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(grantee_name))) {
    LOG_WARN("failed to get grantee name", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(permission_type))) {
    LOG_WARN("failed to get permission type", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(permission_name))) {
    LOG_WARN("failed to get permission name", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(permission_action))) {
    LOG_WARN("failed to get permission action", K(ret), K(params.at(3)));
  } else if (OB_FAIL(create_permission_impl(ctx,
                                            tenant_id,
                                            true,
                                            grantee_name,
                                            permission_type,
                                            permission_name,
                                            permission_action,
                                            key))) {
    LOG_WARN("failed to create java permission", K(ret));
  }

  return ret;
}

int ObDBMSJava::grant_permission_with_key(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ObString grantee_name;
  ObString permission_type;
  ObString permission_name;
  ObString permission_action;
  uint64_t key = OB_INVALID_ID;

  if (5 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(grantee_name))) {
    LOG_WARN("failed to get grantee name", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(permission_type))) {
    LOG_WARN("failed to get permission type", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(permission_name))) {
    LOG_WARN("failed to get permission name", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(permission_action))) {
    LOG_WARN("failed to get permission action", K(ret), K(params.at(3)));
  } else if (OB_FAIL(create_permission_impl(ctx,
                                            tenant_id,
                                            true,
                                            grantee_name,
                                            permission_type,
                                            permission_name,
                                            permission_action,
                                            key))) {
    LOG_WARN("failed to create java permission", K(ret));
  } else {
    params.at(4).set_uint64(key);
  }

  return ret;
}

int ObDBMSJava::restrict_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ObString grantee_name;
  ObString permission_type;
  ObString permission_name;
  ObString permission_action;
  uint64_t key = OB_INVALID_ID;

  if (4 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(grantee_name))) {
    LOG_WARN("failed to get grantee name", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(permission_type))) {
    LOG_WARN("failed to get permission type", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(permission_name))) {
    LOG_WARN("failed to get permission name", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(permission_action))) {
    LOG_WARN("failed to get permission action", K(ret), K(params.at(3)));
  } else if (OB_FAIL(create_permission_impl(ctx,
                                            tenant_id,
                                            false,
                                            grantee_name,
                                            permission_type,
                                            permission_name,
                                            permission_action,
                                            key))) {
    LOG_WARN("failed to create java permission", K(ret));
  }

  return ret;
}

int ObDBMSJava::restrict_permission_with_key(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ObString grantee_name;
  ObString permission_type;
  ObString permission_name;
  ObString permission_action;
  uint64_t key = OB_INVALID_ID;

  if (5 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(grantee_name))) {
    LOG_WARN("failed to get grantee name", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(permission_type))) {
    LOG_WARN("failed to get permission type", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(permission_name))) {
    LOG_WARN("failed to get permission name", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(permission_action))) {
    LOG_WARN("failed to get permission action", K(ret), K(params.at(3)));
  } else if (OB_FAIL(create_permission_impl(ctx,
                                            tenant_id,
                                            false,
                                            grantee_name,
                                            permission_type,
                                            permission_name,
                                            permission_action,
                                            key))) {
    LOG_WARN("failed to create java permission", K(ret));
  } else {
    params.at(4).set_uint64(key);
  }

  return ret;
}

int ObDBMSJava::revoke_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ObString grantee_name;
  ObString permission_type;
  ObString permission_name;
  ObString permission_action;

  ObSchemaGetterGuard *schema_guard = nullptr;
  uint64_t grantee_id = OB_INVALID_ID;
  ObString host_name(OB_DEFAULT_HOST_NAME);

  if (4 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(grantee_name))) {
    LOG_WARN("failed to get permission schema", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_string(permission_type))) {
    LOG_WARN("failed to get permission type", K(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_string(permission_name))) {
    LOG_WARN("failed to get permission name", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(permission_action))) {
    LOG_WARN("failed to get permission action", K(ret), K(params.at(3)));
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_user_id(tenant_id, grantee_name, host_name, grantee_id))) {
    LOG_WARN("failed to get user id", K(ret), K(tenant_id), K(grantee_name), K(host_name));
  } else {
    ObSEArray<const ObSimpleJavaPolicySchema *, 8> schemas;
    if (OB_FAIL(schema_guard->get_java_policy_schemas_of_grantee(tenant_id, grantee_id, schemas))) {
      LOG_WARN("failed to get java policy schemas in tenant", K(ret), K(tenant_id));
    } else {
      uint64_t type_schema_id = lib::is_oracle_mode() ? OB_ORA_SYS_DATABASE_ID : OB_SYS_DATABASE_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObSimpleJavaPolicySchema *schema = schemas.at(i);
        if (OB_ISNULL(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL java policy schema", K(ret), K(i));
        } else if (schema->get_type_schema() == type_schema_id
                    && 0 == schema->get_type_name().case_compare(permission_type)
                    && 0 == schema->get_name().case_compare(permission_name)
                    && 0 == schema->get_action().case_compare(permission_action)
                    && schema->is_enabled()) {
          if (OB_FAIL(modify_permission_impl(ctx,
                                            tenant_id,
                                            ObSimpleJavaPolicySchema::JavaPolicyStatus::DISABLED,
                                            schema->get_key()))) {
            LOG_WARN("failed to disable java permission", K(ret), K(i), KPC(schema));
          }
        }
      }
    }

    LOG_INFO("finished to revoke java permission", K(ret), K(tenant_id),
             K(grantee_name), K(permission_type), K(permission_name), K(permission_action));
  }

  return ret;
}

int ObDBMSJava::disable_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  uint64_t key = OB_INVALID_ID;

  if (1 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_java_policy_key(params.at(0), key))) {
    LOG_WARN("failed to get key", K(ret), K(params.at(0)));
  } else if (OB_FAIL(modify_permission_impl(ctx,
                                            tenant_id,
                                            ObSimpleJavaPolicySchema::JavaPolicyStatus::DISABLED,
                                            key))) {
    LOG_WARN("failed to disable java permission", K(ret));
  }

  return ret;
}

int ObDBMSJava::enable_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  uint64_t key = OB_INVALID_ID;

  if (1 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_java_policy_key(params.at(0), key))) {
    LOG_WARN("failed to get key", K(ret), K(params.at(0)));
  } else if (OB_FAIL(modify_permission_impl(ctx,
                                            tenant_id,
                                            ObSimpleJavaPolicySchema::JavaPolicyStatus::ENABLED,
                                            key))) {
    LOG_WARN("failed to enable java permission", K(ret));
  }

  return ret;
}

int ObDBMSJava::delete_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  uint64_t key = OB_INVALID_ID;

  ObSchemaGetterGuard *schema_guard = nullptr;
  const ObSimpleJavaPolicySchema *java_policy_schema = nullptr;

  if (1 != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params));
  } else if (OB_FAIL(check_java_policy_supported(ctx, tenant_id))) {
    LOG_WARN("java policy is not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_java_policy_key(params.at(0), key))) {
    LOG_WARN("failed to get key", K(ret), K(params.at(0)));
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_java_policy_schema(tenant_id, key, java_policy_schema))) {
    LOG_WARN("failed to get java policy schema", K(ret), K(tenant_id), K(key));
  } else if (OB_ISNULL(java_policy_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("java policy not found", K(ret), K(tenant_id), K(key));
  } else {
    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_);
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;

    obrpc::ObDropJavaPolicyArg args(tenant_id, key);
    obrpc::ObDropJavaPolicyRes res;

    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (!args.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected invalid drop java policy arg", K(ret), K(args));
    } else if (OB_FAIL(common_rpc_proxy->drop_java_policy(args, res))) {
      LOG_WARN("failed to drop java policy", K(ret), K(args), K(res));
    }

    LOG_INFO("finished to delete java permission", K(ret), K(args), K(res));
  }

  return ret;
}

int ObDBMSJava::check_java_policy_supported(ObPLExecCtx &ctx, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;
  tenant_id = OB_INVALID_TENANT_ID;

  if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_my_session()", K(ret));
  } else {
    ObSQLSessionInfo &session = *ctx.exec_ctx_->get_my_session();
    tenant_id = session.get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid tenant_config", K(ret));
    } else if (!tenant_config->ob_enable_java_udf) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ob_enable_java_udf is not enabled", K(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_2_1 || data_version < DATA_VERSION_4_4_2_1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("java policy is only supported after 4.4.2", K(ret));
    } else if ((lib::is_oracle_mode() && !session.is_oracle_sys_user())
               || (lib::is_mysql_mode() && !session.is_mysql_root_user())) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("only SYS or ROOT user can operate java policy", K(ret));
    }
  }

  return ret;
}

int ObDBMSJava::create_permission_impl(ObPLExecCtx &ctx,
                                       const uint64_t tenant_id,
                                       const bool is_grant,
                                       const ObString &grantee_name,
                                       const ObString &permission_type,
                                       const ObString &name,
                                       const ObString &action,
                                       uint64_t &key)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard *schema_guard = nullptr;
  uint64_t grantee_id = OB_INVALID_ID;
  uint64_t type_schema_id = OB_INVALID_ID;

  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;

  if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_my_session()", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema guard", K(ret));
  } else if (grantee_name.empty() || permission_type.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty argument", K(ret), K(grantee_name), K(permission_type));
  } else {
    ObString type_name = permission_type;
    ObString host_name(OB_DEFAULT_HOST_NAME);

    const ObSimpleJavaPolicySchema::JavaPolicyKind target_kind = is_grant ? ObSimpleJavaPolicySchema::JavaPolicyKind::GRANT
                                                                          : ObSimpleJavaPolicySchema::JavaPolicyKind::RESTRICT;
    const ObSimpleJavaPolicySchema *existing_schema = nullptr;

    // Parse permission_type: allow "SCHEMA:TYPE_NAME" or default schema "SYS"
    const char *ptr = permission_type.ptr();
    const int64_t len = permission_type.length();
    int64_t sep_pos = -1;

    for (int64_t i = 0; i < len; ++i) {
      if (':' == ptr[i]) {
        sep_pos = i;
        break;
      }
    }

    if (sep_pos >= 0) {
      type_name.assign_ptr(ptr + sep_pos + 1, static_cast<int32_t>(len - sep_pos - 1));

      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-system permission type is not supported", K(ret), K(type_name), K(permission_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-system permission type is");
    } else {
      type_schema_id = lib::is_oracle_mode() ? OB_ORA_SYS_DATABASE_ID : OB_SYS_DATABASE_ID;
    }

    if (OB_SUCC(ret)) {
      bool is_whitelisted = false;

      ObSQLSessionInfo &session = *ctx.exec_ctx_->get_my_session();

      if (OB_FAIL(check_permission_whitelist(type_name, name, action, is_whitelisted))) {
        LOG_WARN("failed to check permission whitelist", K(ret), K(type_name), K(name), K(action));
      } else if (!is_whitelisted) {
        if (OB_SYS_TENANT_ID != session.get_login_tenant_id()) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("permission is not whitelisted, only root@sys can grant non-whitelisted permission",
                   K(ret), K(type_name), K(name), K(action), K(session.get_login_tenant_id()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObSEArray<const ObSimpleJavaPolicySchema *, 8> schemas;
      if (OB_FAIL(schema_guard->get_user_id(tenant_id, grantee_name, host_name, grantee_id))) {
        LOG_WARN("failed to get grantee user id", K(ret), K(tenant_id), K(grantee_name));
      } else if (OB_FAIL(schema_guard->get_java_policy_schemas_of_grantee(tenant_id, grantee_id, schemas))) {
        LOG_WARN("failed to get java policy schemas of grantee", K(ret), K(tenant_id), K(grantee_id));
      } else {
        for (int64_t i = 0; i < schemas.count(); ++i) {
          const ObSimpleJavaPolicySchema *schema = schemas.at(i);
          if (OB_NOT_NULL(schema)
              && schema->get_kind() == target_kind
              && schema->get_grantee() == grantee_id
              && schema->get_type_schema() == type_schema_id
              && 0 == schema->get_type_name().case_compare(type_name)
              && 0 == schema->get_name().case_compare(name)
              && 0 == schema->get_action().case_compare(action)) {
            existing_schema = schema;
            break;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_NOT_NULL(existing_schema) && existing_schema->is_enabled()) {
      key = existing_schema->get_key();
      LOG_INFO("java permission already exists and is enabled, skip create",
               K(tenant_id), K(key), KPC(existing_schema));
    } else if (OB_NOT_NULL(existing_schema)) {
      key = existing_schema->get_key();
      if (OB_FAIL(modify_permission_impl(ctx,
                                         tenant_id,
                                         ObSimpleJavaPolicySchema::JavaPolicyStatus::ENABLED,
                                         key))) {
        LOG_WARN("failed to enable existing java permission", K(ret), K(key), KPC(existing_schema));
      } else {
        LOG_INFO("existing java permission was disabled, now enabled",
                 K(tenant_id), K(key), KPC(existing_schema));
      }
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else {
      obrpc::ObCreateJavaPolicyArg args(tenant_id,
                                        target_kind,
                                        grantee_id,
                                        type_schema_id,
                                        type_name,
                                        name,
                                        action,
                                        ObSimpleJavaPolicySchema::JavaPolicyStatus::ENABLED);
      obrpc::ObCreateJavaPolicyRes res;

      if (!args.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected invalid create java policy arg", K(ret), K(args));
      } else if (OB_FAIL(common_rpc_proxy->create_java_policy(args, res))) {
        LOG_WARN("failed to create java policy", K(ret), K(args), K(res));
      } else {
        key = res.key_;
      }

      LOG_INFO("finished to create java permission", K(ret), K(args), K(res));
    }
  }

  return ret;
}

int ObDBMSJava::modify_permission_impl(ObPLExecCtx &ctx,
                                       const uint64_t tenant_id,
                                       const ObSimpleJavaPolicySchema::JavaPolicyStatus status,
                                       const uint64_t key)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard *schema_guard = nullptr;
  const ObSimpleJavaPolicySchema *java_policy_schema = nullptr;
  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;

  if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ctx.exec_ctx_->get_sql_ctx()", K(ret));
  } else if (OB_ISNULL(schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_java_policy_schema(tenant_id, key, java_policy_schema))) {
    LOG_WARN("failed to get java policy schema", K(ret), K(tenant_id), K(key));
  } else if (OB_ISNULL(java_policy_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("java policy not found", K(ret), K(tenant_id), K(key));
  } else if (java_policy_schema->get_status() == status) {
    LOG_INFO("java policy status is already the same, skip modify", K(tenant_id), K(key), K(status));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else {
    obrpc::ObModifyJavaPolicyArg args(tenant_id, status, key);
    obrpc::ObModifyJavaPolicyRes res;

    if (!args.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected invalid modify java policy arg", K(ret), K(args));
    } else if (OB_FAIL(common_rpc_proxy->modify_java_policy(args, res))) {
      LOG_WARN("failed to modify java policy", K(ret), K(args), K(res));
    }

    LOG_INFO("finished to modify java permission", K(ret), K(args), K(res));
  }

  return ret;
}

int ObDBMSJava::get_java_policy_key(ObObjParam &param, uint64_t &key)
{
  int ret = OB_SUCCESS;

  if (param.is_unsigned_integer()) {
    key = param.get_uint64();
  } else if (param.is_signed_integer()) {
    key = static_cast<uint64_t>(param.get_int());
  } else if (param.is_number()) {
    const ObNumber &nmb = param.get_number();
    if (!nmb.is_valid_uint64(key)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  }

  return ret;
}

int ObDBMSJava::check_permission_whitelist(const ObString &permission_type,
                                           const ObString &permission_name,
                                           const ObString &permission_action,
                                           bool &is_whitelisted)
{
  int ret = OB_SUCCESS;

  is_whitelisted = false;

  JNIEnv *env = nullptr;
  jclass jar_utils = nullptr;
  jmethodID check_permission_whitelist_method = nullptr;

  ObArenaAllocator alloc(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_ARENA)));

  if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  }else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "com/oceanbase/internal/ObOraUtils", jar_utils))) {
    LOG_WARN("failed to get_cached_class com/oceanbase/internal/ObOraUtils", K(ret));
  } else if (FALSE_IT(check_permission_whitelist_method = env->GetStaticMethodID(jar_utils, "checkPermissionWhitelist", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Z"))) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
    LOG_WARN("failed to find checkPermissionWhitelist method", K(ret));
  } else if (OB_ISNULL(check_permission_whitelist_method)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL checkPermissionWhitelist method", K(ret));
  } else {
    jstring j_type = nullptr;
    jstring j_name = nullptr;
    jstring j_action = nullptr;
    jboolean is_whitelisted_result = JNI_FALSE;

    if (OB_FAIL(ObJavaUtils::ob_string_to_jstring(*env, alloc, permission_type, j_type))) {
      LOG_WARN("failed to convert permission type to jstring", K(ret), K(permission_type));
    } else if (OB_FAIL(ObJavaUtils::ob_string_to_jstring(*env, alloc, permission_name, j_name))) {
      LOG_WARN("failed to convert permission name to jstring", K(ret), K(permission_name));
    } else if (OB_FAIL(ObJavaUtils::ob_string_to_jstring(*env, alloc, permission_action, j_action))) {
      LOG_WARN("failed to convert permission action to jstring", K(ret), K(permission_action));
    } else if (FALSE_IT(is_whitelisted_result = env->CallStaticBooleanMethod(jar_utils, check_permission_whitelist_method, j_type, j_name, j_action))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to check call checkPermissionWhitelist method exception", K(ret), K(permission_type), K(permission_name), K(permission_action));
    } else {
      is_whitelisted = is_whitelisted_result == JNI_TRUE;
    }

    ObJavaUtils::delete_local_ref(j_action, env);
    ObJavaUtils::delete_local_ref(j_name, env);
    ObJavaUtils::delete_local_ref(j_type, env);
  }

  return ret;
}

}  // namespace pl
}  // namespace oceanbase
