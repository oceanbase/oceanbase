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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_schema_getter_guard.h"

#include "ob_schema_mgr.h"

namespace oceanbase
{

using namespace common;
using namespace observer;

namespace share
{

namespace schema
{

int ObSchemaGetterGuard::get_ai_model_schema(const uint64_t tenant_id,
                                             const ObString &ai_model_name,
                                             const ObAiModelSchema *&ai_model_schema)
{
  int ret = OB_SUCCESS;

  const ObSchemaMgr *mgr = nullptr;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || ai_model_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(ai_model_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  } else if (OB_FAIL(mgr->get_ai_model_schema(tenant_id, ai_model_name, mode, ai_model_schema))){
    LOG_WARN("fail to get ai model schema", K(ret), K(tenant_id), K(ai_model_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_ai_model_schema(const uint64_t tenant_id,
                                             const uint64_t ai_model_id,
                                             const ObAiModelSchema *&ai_model_schema)
{
  int ret = OB_SUCCESS;

  const ObSchemaMgr *mgr = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || (OB_INVALID_ID == ai_model_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(ai_model_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_ai_model_schema(tenant_id, ai_model_id, ai_model_schema))){
    LOG_WARN("fail to get ai model schema", K(ret), K(tenant_id), K(ai_model_id));
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
