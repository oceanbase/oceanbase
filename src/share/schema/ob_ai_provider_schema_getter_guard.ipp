/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

int ObSchemaGetterGuard::get_ai_provider_schema(const uint64_t tenant_id,
                                                 const uint64_t provider_id,
                                                 const ObAIProviderSchema *&provider_schema)
{
  int ret = OB_SUCCESS;

  const ObSchemaMgr *mgr = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || (OB_INVALID_ID == provider_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(provider_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_ai_provider_schema(tenant_id, provider_id, provider_schema))){
    LOG_WARN("fail to get ai provider schema", K(ret), K(tenant_id), K(provider_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_ai_provider_schema(const uint64_t tenant_id,
                                                 const ObString &provider_name,
                                                 const ObAIProviderSchema *&provider_schema)
{
  int ret = OB_SUCCESS;

  const ObSchemaMgr *mgr = nullptr;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || provider_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(provider_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  } else if (OB_FAIL(mgr->get_ai_provider_schema(tenant_id, provider_name, mode, provider_schema))){
    LOG_WARN("fail to get ai provider schema", K(ret), K(tenant_id), K(provider_name));
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
