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

OB_SERIALIZE_MEMBER(ObAIFuncExprInfo, name_, type_, model_);
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
    OZ(ob_write_string(allocator, model_, other->model_));
    other->type_ = type_;
  }
  return ret;
}

int ObAIFuncExprInfo::init(ObIAllocator &allocator, const ObString &model_id, share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObAiModelSchema *ai_model_schema = NULL;
  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model id is empty", KR(ret), K(model_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model id is empty");
  } else if (OB_FAIL(schema_guard.get_ai_model_schema(MTL_ID(), model_id, ai_model_schema))) {
    LOG_WARN("fail to get ai model schema", KR(ret), K(MTL_ID()), K(model_id));
  } else if (OB_ISNULL(ai_model_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ai model schema is null", KR(ret), K(MTL_ID()), K(model_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, ai model not found, please check if the model exists");
  } else {
    OZ(ob_write_string(allocator, ai_model_schema->get_name(), name_));
    OZ(ob_write_string(allocator, ai_model_schema->get_model_name(), model_));
    type_ = ai_model_schema->get_type();
  }
  return ret;
}

} // namespace common
} // namespace oceanbase