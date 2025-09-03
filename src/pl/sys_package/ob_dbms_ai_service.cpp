/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_dbms_ai_service.h"
#include "share/ai_service/ob_ai_service_executor.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_rpc_struct.h"
#include "src/pl/ob_pl.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"

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

} // namespace pl
} // namespace oceanbase
