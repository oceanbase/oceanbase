/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_schema_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace share
{
namespace schema
{

int ObSchemaMgr::get_ai_model_schema(
  const uint64_t &tenant_id,
  const uint64_t &ai_model_id,
  const ObAiModelSchema *&ai_model_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_model_mgr_.get_ai_model_schema(ai_model_id, ai_model_schema);
  }

  return ret;
}

int ObSchemaMgr::add_ai_models(const common::ObIArray<ObAiModelSchema> &ai_model_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ai_model_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_ai_model(ai_model_schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaMgr::add_ai_model(const ObAiModelSchema &ai_model_schema)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (is_sys_tenant(tenant_id_)) {
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(ai_model_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), "tenant_id", ai_model_schema.get_tenant_id());
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }

  if (OB_SUCC(ret) && OB_FAIL(ai_model_mgr_.add_ai_model(ai_model_schema, mode))) {
    LOG_WARN("fail to add ai model", K(ret));
  }
  return ret;
}

int ObSchemaMgr::del_ai_model(const ObTenantAiModelId &tenant_ai_model_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ai_model_mgr_.del_ai_model(tenant_ai_model_id))) {
    LOG_WARN("fail to del ai model", K(ret));
  }
  return ret;
}

int ObSchemaMgr::get_ai_model_schema(
  const uint64_t &tenant_id,
  const ObString &ai_model_name,
  const common::ObNameCaseMode &case_mode,
  const ObAiModelSchema *&ai_model_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_model_mgr_.get_ai_model_schema(tenant_id, ai_model_name, case_mode, ai_model_schema);
  }

  return ret;
}

int ObSchemaMgr::add_ai_providers(const common::ObIArray<ObAIProviderSchema> &ai_provider_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ai_provider_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_ai_provider(ai_provider_schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaMgr::add_ai_provider(const ObAIProviderSchema &ai_provider_schema)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (is_sys_tenant(tenant_id_)) {
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(ai_provider_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), "tenant_id", ai_provider_schema.get_tenant_id());
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }

  if (OB_SUCC(ret) && OB_FAIL(ai_provider_mgr_.add_ai_provider(ai_provider_schema, mode))) {
    LOG_WARN("fail to add ai provider", K(ret));
  }
  return ret;
}

int ObSchemaMgr::del_ai_provider(const ObTenantAIProviderId &tenant_ai_provider_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ai_provider_mgr_.del_ai_provider(tenant_ai_provider_id))) {
    LOG_WARN("fail to del ai provider", K(ret));
  }
  return ret;
}

int ObSchemaMgr::add_ai_gateways(const common::ObIArray<ObAIGatewaySchema> &ai_gateway_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ai_gateway_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_ai_gateway(ai_gateway_schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaMgr::add_ai_gateway(const ObAIGatewaySchema &ai_gateway_schema)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (is_sys_tenant(tenant_id_)) {
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(ai_gateway_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), "tenant_id", ai_gateway_schema.get_tenant_id());
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }

  if (OB_SUCC(ret) && OB_FAIL(ai_gateway_mgr_.add_ai_gateway(ai_gateway_schema, mode))) {
    LOG_WARN("fail to add ai gateway", K(ret));
  }
  return ret;
}

int ObSchemaMgr::del_ai_gateway(const ObTenantAIGatewayId &tenant_ai_gateway_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ai_gateway_mgr_.del_ai_gateway(tenant_ai_gateway_id))) {
    LOG_WARN("fail to del ai gateway", K(ret));
  }
  return ret;
}

int ObSchemaMgr::get_ai_provider_schema(
    const uint64_t &tenant_id,
    const uint64_t &provider_id,
    const ObAIProviderSchema *&provider_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_provider_mgr_.get_ai_provider_schema(provider_id, provider_schema);
  }

  return ret;
}

int ObSchemaMgr::get_ai_provider_schema(
    const uint64_t &tenant_id,
    const ObString &provider_name,
    const common::ObNameCaseMode &case_mode,
    const ObAIProviderSchema *&provider_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_provider_mgr_.get_ai_provider_schema(tenant_id, provider_name, case_mode, provider_schema);
  }

  return ret;
}

int ObSchemaMgr::get_ai_gateway_schema(
    const uint64_t &tenant_id,
    const uint64_t &gateway_id,
    const ObAIGatewaySchema *&gateway_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_gateway_mgr_.get_ai_gateway_schema(gateway_id, gateway_schema);
  }

  return ret;
}

int ObSchemaMgr::get_ai_gateway_schema(
    const uint64_t &tenant_id,
    const ObString &gateway_name,
    const common::ObNameCaseMode &case_mode,
    const ObAIGatewaySchema *&gateway_schema) const
{
  int ret = OB_SUCCESS;

  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = ai_gateway_mgr_.get_ai_gateway_schema(tenant_id, gateway_name, case_mode, gateway_schema);
  }

  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
