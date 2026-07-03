/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

RETRIEVE_SCHEMA_FUNC_DEFINE(ai_model);
RETRIEVE_SCHEMA_FUNC_DEFINE(ai_provider);
RETRIEVE_SCHEMA_FUNC_DEFINE(ai_gateway);
template<typename T>
int ObSchemaRetrieveUtils::fill_ai_model_schema(const uint64_t tenant_id,
                                                T &result,
                                                ObAiModelSchema &schema,
                                                bool &is_deleted)
{
  int ret = OB_SUCCESS;

  schema.reset();

  schema.set_tenant_id(tenant_id);
  int64_t type = 0;

  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, model_id, schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);

  if (OB_SUCC(ret) && !is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "type", type, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, model_name, schema);
  }

  if (OB_SUCC(ret)) {
    schema.set_type(EndpointType::convert_type_from_int(type));
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_ai_provider_schema(const uint64_t tenant_id,
                                                  T &result,
                                                  ObAIProviderSchema &schema,
                                                  bool &is_deleted)
{
  int ret = OB_SUCCESS;

  schema.reset();

  schema.set_tenant_id(tenant_id);

  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, provider_id, schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);

  if (OB_SUCC(ret) && !is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, protocol, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, base_url, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, access_key, schema);
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_ai_gateway_schema(const uint64_t tenant_id,
                                                  T &result,
                                                  ObAIGatewaySchema &schema,
                                                  bool &is_deleted)
{
  int ret = OB_SUCCESS;

  schema.reset();

  schema.set_tenant_id(tenant_id);

  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, gateway_id, schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);

  if (OB_SUCC(ret) && !is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, endpoints, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, circuit_breaker, schema);
  }

  return ret;
}
