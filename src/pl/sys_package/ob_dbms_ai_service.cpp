/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include "ob_dbms_ai_service.h"
#include "share/ai_service/ob_ai_service_executor.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_license_utils.h"
#include "src/pl/ob_pl.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "lib/container/ob_se_array.h"

using namespace oceanbase::share;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace pl
{

int ObDBMSAiService::check_ai_model_privilege_(ObPLExecCtx &ctx, ObPrivSet required_priv)
{
  int ret = OB_SUCCESS;
  bool has_priv = false;

  if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or session is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    share::schema::ObSchemaGetterGuard *schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", K(ret));
    } else {
      sql::ObAIServiceEndpointPrivUtil priv_util(*schema_guard);
      share::schema::ObSessionPrivInfo session_priv;
      if (OB_FAIL(schema_guard->get_session_priv_info(ctx.exec_ctx_->get_my_session()->get_priv_tenant_id(),
                                                    ctx.exec_ctx_->get_my_session()->get_priv_user_id(),
                                                    ctx.exec_ctx_->get_my_session()->get_database_name(),
                                                    session_priv))) {
        LOG_WARN("failed to get session priv info", K(ret));
      } else {
        switch (required_priv) {
          case OB_PRIV_CREATE_AI_MODEL:
            if (OB_FAIL(priv_util.check_create_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
              LOG_WARN("failed to check create ai model privilege", K(ret));
            }
            break;
          case OB_PRIV_ALTER_AI_MODEL:
            if (OB_FAIL(priv_util.check_alter_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
              LOG_WARN("failed to check alter ai model privilege", K(ret));
            }
            break;
          case OB_PRIV_DROP_AI_MODEL:
            if (OB_FAIL(priv_util.check_drop_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
              LOG_WARN("failed to check drop ai model privilege", K(ret));
            }
            break;
          case OB_PRIV_ACCESS_AI_MODEL:
            if (OB_FAIL(priv_util.check_access_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
              LOG_WARN("failed to check access ai model privilege", K(ret));
            }
            break;
          default:
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid privilege type", K(ret), K(required_priv));
            break;
        }

        if (OB_SUCC(ret) && !has_priv) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("no privilege for ai model operation", K(ret), K(required_priv));
        }
      }
    }
  }

  return ret;
}

int ObDBMSAiService::create_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString endpoint_name;
  ctx.set_is_sensitive(true);

  if (OB_FAIL(precheck_version_and_param_count_(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_CREATE_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("failed to check create ai model privilege", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "create ai model endpoint");
    } else {
      LOG_WARN("failed to check create ai model privilege", K(ret));
    }
  } else if (OB_FAIL(ObLicenseUtils::check_ai_allowed(static_cast<int64_t>(MTL_ID())))) {
    LOG_WARN("AI option is required for create ai model endpoint", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(params.at(0).get_string(endpoint_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (endpoint_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai service endpoint name is empty", K(ret), K(params));
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai service endpoint params is wrong", K(ret), K(params));
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else if (OB_FAIL(ObAiServiceExecutor::create_ai_model_endpoint(tmp_allocator, endpoint_name, *j_base))) {
      LOG_WARN("failed to insert ai service endpoint", K(ret), K(endpoint_name));
    }
  }

  LOG_DEBUG("finished to create ai service endpoint", K(ret), K(params));
  return ret;
}

int ObDBMSAiService::alter_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString endpoint_name;

  if (OB_FAIL(precheck_version_and_param_count_(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_ALTER_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_WARN("failed to check alter ai model privilege", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "alter ai model endpoint");
    } else {
      LOG_WARN("failed to check alter ai model privilege", K(ret));
    }
  } else if (OB_FAIL(params.at(0).get_string(endpoint_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (endpoint_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai service endpoint name is empty", K(ret), K(params), K(endpoint_name));
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai service endpoint params is wrong", K(ret), K(params));
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else {
    ObIJsonBase *j_base = nullptr;
    ObArenaAllocator tmp_allocator;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else if (OB_FAIL(ObAiServiceExecutor::alter_ai_model_endpoint(tmp_allocator, endpoint_name, *j_base))) {
      LOG_WARN("failed to alter ai service endpoint", K(ret), K(endpoint_name));
    }
  }

  LOG_DEBUG("finished to alter ai service endpoint", K(ret), K(params));
  return ret;
}

int ObDBMSAiService::drop_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString endpoint_name;

  if (OB_FAIL(precheck_version_and_param_count_(1, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_DROP_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_WARN("failed to check drop ai model privilege", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "drop ai model endpoint");
    } else {
      LOG_WARN("failed to check drop ai model privilege", K(ret));
    }
  } else if (OB_FAIL(params.at(0).get_string(endpoint_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (endpoint_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai service endpoint name is empty", K(ret), K(params), K(endpoint_name));
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_FAIL(ObAiServiceExecutor::drop_ai_model_endpoint(endpoint_name))) {
    LOG_WARN("failed to drop ai service endpoint", K(ret), K(endpoint_name));
  }

  LOG_DEBUG("finished to drop ai service endpoint", K(ret), K(endpoint_name));

  return ret;
}

int ObDBMSAiService::precheck_version_and_param_count_(int expect_param_count, sql::ParamStore &params)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t tenant_id = MTL_ID();
  if (expect_param_count != params.count()) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument", K(ret), K(params.count()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT_NUM);
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  }
  return ret;
}

int ObDBMSAiService::precheck_version_and_param_count_v2(int expect_param_count, sql::ParamStore &params)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t tenant_id = MTL_ID();
  if (expect_param_count != params.count()) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument", K(ret), K(params.count()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT_NUM);
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  }
  return ret;
}

int ObDBMSAiService::get_json_base_(ObArenaAllocator &allocator, sql::ParamStore &params, ObIJsonBase *&j_base)
{
  int ret = OB_SUCCESS;
  ObString j_str;
  ObJsonInType in_type = ObJsonInType::JSON_BIN;
  uint32_t parse_flag = 0; // mysql mode

  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, params.at(1), j_str))) {
    LOG_WARN("fail to read real string data", K(ret), K(params.at(1)));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, in_type, in_type, j_base, parse_flag))) {
    LOG_WARN("fail to get json base", K(ret), K(j_str));
  } else if (j_base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ai service endpoint params is not a json object", K(ret), K(params));
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("PARAMS"), "PARAMS", (int)strlen("JSON_OBJECT"), "JSON_OBJECT");
  }
  return ret;
}

int ObDBMSAiService::create_ai_model(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString model_name;
  uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObAiModelSchema *ai_model_schema = nullptr;

  if (OB_FAIL(precheck_version_and_param_count_(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_CREATE_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_WARN("failed to check create ai model privilege", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "create ai model");
    } else {
      LOG_WARN("failed to check create ai model privilege", K(ret));
    }
  } else if (OB_FAIL(ObLicenseUtils::check_ai_allowed(static_cast<int64_t>(tenant_id)))) {
    LOG_WARN("AI option is required for create ai model", KR(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_string(model_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (model_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai model name is empty", K(ret), K(params), K(model_name));
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai model params is null", K(ret), K(params));
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_ai_model_schema(tenant_id, model_name, ai_model_schema))) {
    LOG_WARN("failed to get ai model schema", K(ret), K(tenant_id), K(model_name));
  } else if (OB_NOT_NULL(ai_model_schema)) {
    ret = OB_AI_FUNC_MODEL_EXISTS;
    LOG_WARN("ai model already exists", K(ret), K(tenant_id), K(model_name));
    LOG_USER_ERROR(OB_AI_FUNC_MODEL_EXISTS, model_name.length(), model_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    ObAiServiceModelInfo model_info;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else if (OB_FAIL(model_info.parse_from_json_base(model_name, *j_base))) {
      LOG_WARN("failed to parse ai model info", K(ret), K(model_name));
    } else {
      ObCreateAiModelArg arg(tenant_id, model_info);
      arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
      } else if (OB_FAIL(arg.check_valid())) {
        LOG_WARN("invalid create ai model arg", K(ret), K(arg));
      } else if (OB_FAIL(common_rpc_proxy->create_ai_model(arg))) {
        LOG_WARN("failed to create ai model", K(ret), K(arg));
      }
    }

    LOG_DEBUG("finished to create ai model", K(ret), K(params), K(model_name));
  }
  return ret;
}

int ObDBMSAiService::drop_ai_model(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString model_name;
  uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObAiModelSchema *ai_model_schema = nullptr;

  if (OB_FAIL(precheck_version_and_param_count_(1, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_DROP_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_WARN("failed to check drop ai model privilege", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "drop ai model");
    } else {
      LOG_WARN("failed to check drop ai model privilege", K(ret));
    }
  } else if (OB_FAIL(params.at(0).get_string(model_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (model_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_WARN("ai model name is empty", K(ret), K(params), K(model_name));
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_ai_model_schema(tenant_id, model_name, ai_model_schema))) {
    LOG_WARN("failed to get ai model schema", K(ret), K(tenant_id), K(model_name));
  } else if (OB_ISNULL(ai_model_schema)) {
    ret = OB_AI_FUNC_MODEL_NOT_FOUND;
    LOG_WARN("ai model not exists", K(ret), K(tenant_id), K(model_name));
    LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, model_name.length(), model_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObDropAiModelArg arg(tenant_id, model_name);
    arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
    } else if (OB_FAIL(common_rpc_proxy->drop_ai_model(arg))) {
      LOG_WARN("failed to drop ai model", K(ret), K(arg));
    }

    LOG_INFO("finished to drop ai model", K(ret), K(params), K(model_name));
  }

  return ret;
}


int ObDBMSAiService::register_provider(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString provider_name;
  ctx.set_is_sensitive(true);

  if (OB_FAIL(precheck_version_and_param_count_v2(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_CREATE_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "register provider");
    }
  } else if (OB_FAIL(params.at(0).get_string(provider_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (provider_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or sql_ctx is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else {
      JsonObjectIterator iter = j_base->object_iterator();
      ObString protocol;
      ObString base_url;
      ObString access_key;
      ObString storage_access_key;
      bool has_protocol = false;
      bool has_base_url = false;
      while (OB_SUCC(ret) && !iter.end()) {
        ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("failed to get json element", KR(ret));
        } else if (0 == elem.first.case_compare("protocol")) {
          if (elem.second->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, elem.first.length(), elem.first.ptr(), (int)strlen("STRING"), "STRING");
          } else { protocol = ObString(elem.second->get_data_length(), elem.second->get_data()); has_protocol = true; }
        } else if (0 == elem.first.case_compare("base_url")) {
          if (elem.second->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, elem.first.length(), elem.first.ptr(), (int)strlen("STRING"), "STRING");
          } else { base_url = ObString(elem.second->get_data_length(), elem.second->get_data()); has_base_url = true; }
        } else if (0 == elem.first.case_compare("access_key")) {
          if (elem.second->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, elem.first.length(), elem.first.ptr(), (int)strlen("STRING"), "STRING");
          } else { access_key = ObString(elem.second->get_data_length(), elem.second->get_data()); }
        } else {
          ret = OB_AI_FUNC_PARAM_INVALID;
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr());
        }
        if (OB_SUCC(ret)) { iter.next(); }
      }
      if (OB_FAIL(ret)) {
      } else if (access_key.empty()) {
        ret = OB_AI_FUNC_PARAM_EMPTY;
        ObString var_name = "access_key";
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
      } else if (has_protocol && !share::is_supported_ai_provider_protocol(protocol)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported ai provider protocol", K(ret), K(protocol));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ai provider protocol");
      } else if (OB_FAIL(ObAiModelEndpointInfo::encrypt_access_key_to_storage_format(tmp_allocator, access_key, storage_access_key))) {
        LOG_WARN("failed to encrypt provider access key for storage", K(ret));
      } else {
        obrpc::ObRegisterProviderArg arg;
        arg.exec_tenant_id_ = MTL_ID();
        arg.provider_name_ = provider_name;
        arg.protocol_ = protocol;
        arg.base_url_ = base_url;
        arg.access_key_ = storage_access_key;
        arg.has_protocol_ = has_protocol;
        arg.has_base_url_ = has_base_url;
        // mask access_key from ddl_stmt_str_ to avoid leaking plaintext credential
        // into __all_ddl_operation and __all_ai_model_provider_history
        char masked_stmt_buf[OB_MAX_SQL_LENGTH] = {0};
        int64_t masked_pos = 0;
        ObTaskExecutorCtx *task_exec_ctx = nullptr;
        obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
        if (OB_FAIL(databuff_printf(masked_stmt_buf, sizeof(masked_stmt_buf), masked_pos,
            "call DBMS_AI_SERVICE.REGISTER_PROVIDER('%.*s', <params masked>)",
            provider_name.length(), provider_name.ptr()))) {
          LOG_WARN("failed to format masked ddl_stmt", K(ret), K(provider_name));
        } else if (FALSE_IT(arg.ddl_stmt_str_ = ObString(masked_pos, masked_stmt_buf))) {
        } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(*ctx.exec_ctx_))) {
          ret = OB_NOT_INIT;
          LOG_WARN("get task executor context failed", K(ret));
        } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
          LOG_WARN("get common rpc proxy failed", K(ret));
        } else if (OB_ISNULL(common_rpc_proxy)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("common rpc proxy should not be null", K(ret));
        } else if (OB_FAIL(arg.check_valid())) {
          LOG_WARN("invalid register provider arg", K(ret), K(arg));
        } else if (OB_FAIL(common_rpc_proxy->register_provider(arg))) {
          LOG_WARN("failed to register provider", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObDBMSAiService::unregister_provider(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString provider_name;

  if (OB_FAIL(precheck_version_and_param_count_v2(1, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_DROP_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "unregister provider");
    }
  } else if (OB_FAIL(params.at(0).get_string(provider_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (provider_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or sql_ctx is null", K(ret));
  } else {
    obrpc::ObUnregisterProviderArg arg;
    arg.exec_tenant_id_ = MTL_ID();
    arg.provider_name_ = provider_name;
    arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
    } else if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid unregister provider arg", K(ret), K(arg));
    } else if (OB_FAIL(common_rpc_proxy->unregister_provider(arg))) {
      LOG_WARN("failed to unregister provider", K(ret), K(arg));
    }
  }
  return ret;
}

int ObDBMSAiService::alter_model_profile(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString model_str;
  ctx.set_is_sensitive(true);

  if (OB_FAIL(precheck_version_and_param_count_v2(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_ALTER_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "alter model profile");
    }
  } else if (OB_FAIL(params.at(0).get_string(model_str))) {
    LOG_WARN("failed to get model string", K(ret), K(params.at(0)));
  } else if (model_str.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "model";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else if (OB_FAIL(ObAiServiceExecutor::alter_ai_model_profile(tmp_allocator, model_str, *j_base))) {
      LOG_WARN("failed to alter ai model profile", K(ret), K(model_str));
    }
  }
  return ret;
}

int ObDBMSAiService::drop_model_profile(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString model_str;

  if (OB_FAIL(precheck_version_and_param_count_v2(1, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_DROP_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "drop model profile");
    }
  } else if (OB_FAIL(params.at(0).get_string(model_str))) {
    LOG_WARN("failed to get model string", K(ret), K(params.at(0)));
  } else if (model_str.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "model";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_FAIL(ObAiServiceExecutor::drop_ai_model_profile(model_str))) {
    LOG_WARN("failed to drop ai model profile", K(ret), K(model_str));
  }
  return ret;
}

int ObDBMSAiService::validate_gateway_endpoint_json_(common::ObIAllocator &allocator,
                                                     common::ObIJsonBase &j_base,
                                                     common::ObString &endpoints_str,
                                                     common::ObString &circuit_breaker_str)
{
  int ret = OB_SUCCESS;
  bool has_endpoints = false;
  endpoints_str.reset();
  circuit_breaker_str.reset();

  ObIJsonBase *endpoints_obj = nullptr;
  ObIJsonBase *cb_obj = nullptr;

  JsonObjectIterator iter = j_base.object_iterator();
  while (OB_SUCC(ret) && !iter.end()) {
    ObJsonObjPair elem;
    if (OB_FAIL(iter.get_elem(elem))) {
      LOG_WARN("failed to get json element", KR(ret));
    } else if (0 == elem.first.case_compare("endpoints")) {
      has_endpoints = true;
      if (elem.second->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 9, "endpoints");
      } else if (0 == elem.second->element_count()) {
        ret = OB_AI_FUNC_PARAM_EMPTY;
        ObString var_name = "endpoints";
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
      } else {
        endpoints_obj = elem.second;
        uint64_t ep_count = endpoints_obj->element_count();
        common::ObSEArray<ObString, 16> seen_ep_names;
        const ObString ep_name_key(4, "name");
        for (uint64_t i = 0; OB_SUCC(ret) && i < ep_count; ++i) {
          ObIJsonBase *ep = nullptr;
          if (OB_FAIL(endpoints_obj->get_array_element(i, ep))) {
            LOG_WARN("failed to get array element", K(ret), K(i));
          } else if (OB_ISNULL(ep) || ep->json_type() != ObJsonNodeType::J_OBJECT) {
            ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 9, "endpoints");
          } else {
            bool has_model = false;
            JsonObjectIterator ep_iter = ep->object_iterator();
            while (OB_SUCC(ret) && !ep_iter.end()) {
              ObJsonObjPair ep_elem;
              if (OB_FAIL(ep_iter.get_elem(ep_elem))) {
                LOG_WARN("failed to get endpoint element", KR(ret));
              } else if (0 == ep_elem.first.case_compare("model")) {
                has_model = true;
                if (ep_elem.second->json_type() != ObJsonNodeType::J_STRING) {
                  ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
                } else {
                  ObString model_val(ep_elem.second->get_data_length(), ep_elem.second->get_data());
                  const char *slash_ptr = model_val.empty() ? nullptr
                      : static_cast<const char *>(MEMCHR(model_val.ptr(), '/', model_val.length()));
                  if (nullptr == slash_ptr
                      || slash_ptr == model_val.ptr()
                      || slash_ptr == model_val.ptr() + model_val.length() - 1) {
                    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
                  }
                }
              } else if (0 == ep_elem.first.case_compare("access_key")) {
                ret = OB_AI_FUNC_PARAM_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, 10, "access_key");
              } else if (0 == ep_elem.first.case_compare("name")) {
                if (ep_elem.second->json_type() != ObJsonNodeType::J_STRING) {
                  ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 13, "endpoint_name");
                }
              } else if (0 == ep_elem.first.case_compare("weight")) {
                if (ep_elem.second->json_type() != ObJsonNodeType::J_INT
                    && ep_elem.second->json_type() != ObJsonNodeType::J_UINT) {
                  ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 6, "weight");
                } else if (ep_elem.second->get_int() < 0) {
                  ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 6, "weight");
                }
              } else {
                ret = OB_AI_FUNC_PARAM_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, ep_elem.first.length(), ep_elem.first.ptr());
              }
              if (OB_SUCC(ret)) { ep_iter.next(); }
            }
            if (OB_SUCC(ret) && !has_model) {
              ret = OB_AI_FUNC_PARAM_EMPTY;
              ObString var_name = "model";
              LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
            }
            if (OB_SUCC(ret)) {
              ObIJsonBase *name_js = nullptr;
              if (OB_FAIL(ep->get_object_value(ep_name_key, name_js))) {
                if (OB_SEARCH_NOT_FOUND == ret) {
                  ret = OB_AI_FUNC_PARAM_EMPTY;
                  ObString var_name = "name";
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
                } else {
                  LOG_WARN("failed to get endpoint name", K(ret));
                }
              } else if (OB_ISNULL(name_js) || name_js->json_type() != ObJsonNodeType::J_STRING) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 13, "endpoint_name");
              } else {
                const ObString cur_ep_name(name_js->get_data_length(), name_js->get_data());
                if (cur_ep_name.empty()) {
                  ret = OB_AI_FUNC_PARAM_EMPTY;
                  ObString var_name = "name";
                  LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
                } else {
                  for (int64_t si = 0; OB_SUCC(ret) && si < seen_ep_names.count(); ++si) {
                    if (0 == seen_ep_names.at(si).case_compare(cur_ep_name)) {
                      ret = OB_AI_FUNC_MODEL_EXISTS;
                      LOG_USER_ERROR(OB_AI_FUNC_MODEL_EXISTS, cur_ep_name.length(), cur_ep_name.ptr());
                      break;
                    }
                  }
                  if (OB_SUCC(ret) && OB_FAIL(seen_ep_names.push_back(cur_ep_name))) {
                    LOG_WARN("failed to push endpoint name", K(ret));
                  }
                }
              }
            }
          }
        }
      }
    } else if (0 == elem.first.case_compare("circuit_breaker")) {
      if (elem.second->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 15, "circuit_breaker");
      } else {
        cb_obj = elem.second;
      }
    } else {
      ret = OB_AI_FUNC_PARAM_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr());
    }
    if (OB_SUCC(ret)) { iter.next(); }
  }

  if (OB_FAIL(ret)) {
  } else if (!has_endpoints) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "endpoints";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else {
    common::ObStringBuffer buf(&allocator);
    if (OB_FAIL(endpoints_obj->print(buf, false))) {
      LOG_WARN("failed to serialize endpoints", K(ret));
    } else {
      endpoints_str = ObString(static_cast<int32_t>(buf.length()), buf.ptr());
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(cb_obj)) {
      common::ObStringBuffer cb_buf(&allocator);
      if (OB_FAIL(cb_obj->print(cb_buf, false))) {
        LOG_WARN("failed to serialize circuit_breaker", K(ret));
      } else {
        circuit_breaker_str = ObString(static_cast<int32_t>(cb_buf.length()), cb_buf.ptr());
      }
    }
  }
  return ret;
}

int ObDBMSAiService::create_ai_gateway(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString gateway_name;
  ctx.set_is_sensitive(true);

  if (OB_FAIL(precheck_version_and_param_count_v2(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_CREATE_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "create ai gateway");
    }
  } else if (OB_FAIL(params.at(0).get_string(gateway_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (gateway_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or sql_ctx is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else {
      ObString endpoints_str;
      ObString circuit_breaker_str;
      if (OB_FAIL(validate_gateway_endpoint_json_(tmp_allocator, *j_base, endpoints_str, circuit_breaker_str))) {
        LOG_WARN("failed to validate gateway json", K(ret));
      } else {
        ObCreateAiGatewayArg arg(MTL_ID(), gateway_name);
        arg.endpoints_ = endpoints_str;
        arg.circuit_breaker_ = circuit_breaker_str;
        arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
        } else if (OB_FAIL(arg.check_valid())) {
          LOG_WARN("invalid create ai gateway arg", K(ret), K(arg));
        } else if (OB_FAIL(common_rpc_proxy->create_ai_gateway(arg))) {
          LOG_WARN("failed to create ai gateway", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObDBMSAiService::alter_ai_gateway(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString gateway_name;
  ctx.set_is_sensitive(true);

  if (OB_FAIL(precheck_version_and_param_count_v2(2, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_ALTER_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "alter ai gateway");
    }
  } else if (OB_FAIL(params.at(0).get_string(gateway_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (gateway_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (params.at(1).is_null()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "PARAMS";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or sql_ctx is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(get_json_base_(tmp_allocator, params, j_base))) {
      LOG_WARN("failed to get json base", K(ret), K(params));
    } else {
      ObString endpoints_str;
      ObString circuit_breaker_str;
      if (OB_FAIL(validate_gateway_endpoint_json_(tmp_allocator, *j_base, endpoints_str, circuit_breaker_str))) {
        LOG_WARN("failed to validate gateway json", K(ret));
      } else {
        ObAlterAiGatewayArg arg(MTL_ID(), gateway_name);
        arg.endpoints_ = endpoints_str;
        arg.circuit_breaker_ = circuit_breaker_str;
        arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
        } else if (OB_FAIL(arg.check_valid())) {
          LOG_WARN("invalid alter ai gateway arg", K(ret), K(arg));
        } else if (OB_FAIL(common_rpc_proxy->alter_ai_gateway(arg))) {
          LOG_WARN("failed to alter ai gateway", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObDBMSAiService::drop_ai_gateway(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString gateway_name;
  uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObAIGatewaySchema *gateway_schema = nullptr;

  if (OB_FAIL(precheck_version_and_param_count_v2(1, params))) {
    LOG_WARN("failed to pre check", K(ret));
  } else if (OB_FAIL(ObDBMSAiService::check_ai_model_privilege_(ctx, OB_PRIV_DROP_AI_MODEL))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "drop ai gateway");
    }
  } else if (OB_FAIL(params.at(0).get_string(gateway_name))) {
    LOG_WARN("failed to get name string", K(ret), K(params.at(0)));
  } else if (gateway_name.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    ObString var_name = "name";
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_ai_gateway_schema(tenant_id, gateway_name, gateway_schema))) {
    LOG_WARN("failed to get ai gateway schema", K(ret), K(tenant_id), K(gateway_name));
  } else if (OB_ISNULL(gateway_schema)) {
    ret = OB_AI_FUNC_MODEL_NOT_FOUND;
    LOG_WARN("ai gateway not exists", K(ret), K(tenant_id), K(gateway_name));
    LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, gateway_name.length(), gateway_name.ptr());
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx or sql_ctx is null", K(ret));
  } else {
    ObDropAiGatewayArg arg(tenant_id, gateway_name);
    arg.ddl_stmt_str_ = ctx.exec_ctx_->get_sql_ctx()->cur_sql_;
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
    } else if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid drop ai gateway arg", K(ret), K(arg));
    } else if (OB_FAIL(common_rpc_proxy->drop_ai_gateway(arg))) {
      LOG_WARN("failed to drop ai gateway", K(ret), K(arg));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
