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
#include "ob_ai_func.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "observer/omt/ob_tenant_ai_service.h"

namespace oceanbase
{
namespace common
{

OB_SERIALIZE_MEMBER(ObAIFuncExprInfo, name_, type_, url_, api_key_, model_, provider_, parameters_, request_transform_fn_, response_transform_fn_);
int ObAIFuncExprInfo::deep_copy(common::ObIAllocator &allocator,
                                const ObExprOperatorType type,
                                ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *other = NULL;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc ai func expr info", K(ret), K(type));
  } else if (OB_ISNULL(other = static_cast<ObAIFuncExprInfo *>(copied_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ai func expr info ptr", K(ret));
  } else {
    OZ(ob_write_string(allocator, name_, other->name_));
    OZ(ob_write_string(allocator, url_, other->url_));
    OZ(ob_write_string(allocator, api_key_, other->api_key_));
    OZ(ob_write_string(allocator, model_, other->model_));
    OZ(ob_write_string(allocator, provider_, other->provider_));
    OZ(ob_write_string(allocator, parameters_, other->parameters_));
    OZ(ob_write_string(allocator, request_transform_fn_, other->request_transform_fn_));
    OZ(ob_write_string(allocator, response_transform_fn_, other->response_transform_fn_));
    other->type_ = type_;
  }
  return ret;
}

int ObAIFuncExprInfo::init(ObExecContext &exec_ctx, ObString &model_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_model_info(exec_ctx.get_allocator(), model_id))) {
    LOG_WARN("failed to get model info", K(ret));
  }
  return ret;
}

int ObAIFuncExprInfo::init(ObIAllocator &allocator, ObString &model_id)
{
  return get_model_info(allocator, model_id);
}

int ObAIFuncExprInfo::init_by_endpoint_info(common::ObIAllocator &allocator, share::ObAiModelEndpointInfo *endpoint_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(endpoint_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endpoint info is null", K(ret));
  } else {
    OZ(ob_write_string(allocator, endpoint_info->get_url(), url_));
    OZ(ob_write_string(allocator, endpoint_info->get_provider(), provider_));
    OZ(ob_write_string(allocator, endpoint_info->get_parameters(), parameters_));
    OZ(ob_write_string(allocator, endpoint_info->get_request_transform_fn(), request_transform_fn_));
    OZ(ob_write_string(allocator, endpoint_info->get_response_transform_fn(), response_transform_fn_));
    OZ(endpoint_info->get_unencrypted_access_key(allocator, api_key_));
    if (OB_SUCC(ret)) {
      ObString request_model_name = endpoint_info->get_request_model_name();
      if (request_model_name.length() > 0) {
        OZ(ob_write_string(allocator, request_model_name, model_));
      }
    }
  }
  return ret;
}

int ObAIFuncExprInfo::get_model_info(ObIAllocator &allocator, ObString &model_id)
{
  int ret = OB_SUCCESS;
  share::ObAiModelEndpointInfo *endpoint_info = nullptr;
  omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
  omt::ObAiServiceGuard ai_service_guard;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  const ObAiModelSchema *ai_model_schema = NULL;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(MTL_ID(), guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(guard.get_ai_model_schema(MTL_ID(), model_id, ai_model_schema))) {
    LOG_WARN("fail to get ai model schema", KR(ret), K(MTL_ID()), K(model_id));
  } else if (OB_ISNULL(ai_model_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ai model schema is null", KR(ret), K(MTL_ID()), K(model_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai model not found, please check if the model exists");
  } else {
    OZ(ob_write_string(allocator, ai_model_schema->get_name(), name_));
    OZ(ob_write_string(allocator, ai_model_schema->get_model_name(), model_));
    type_ = ai_model_schema->get_type();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ai_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai service is null", K(ret));
  } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
    LOG_WARN("failed to get ai service guard", K(ret));
  } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(name_, endpoint_info))) {
    LOG_WARN("failed to get model info", K(ret), K(model_id));
  } else if (OB_ISNULL(endpoint_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endpoint info is null", K(ret));
  } else if (OB_FAIL(init_by_endpoint_info(allocator, endpoint_info))) {
    LOG_WARN("failed to init by endpoint info", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase