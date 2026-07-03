/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

int ObSchemaServiceSQLImpl::fetch_ai_models(ObISQLClient &sql_client,
                                            const ObRefreshSchemaStatus &schema_status,
                                            const int64_t schema_version,
                                            const uint64_t tenant_id,
                                            ObIArray<ObAiModelSchema> &schema_array,
                                            const SchemaKey *schema_keys,
                                            const int64_t schema_key_size)
{
  int ret = OB_SUCCESS;

  const uint64_t exec_tenant_id = fill_exec_tenant_id(schema_status);

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;

    if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id=0",
                               OB_ALL_AI_MODEL_HISTORY_TNAME))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND schema_version <= %ld", schema_version))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_NOT_NULL(schema_keys) && schema_key_size > 0) {
      if (OB_FAIL(sql.append(" AND model_id IN"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(SQL_APPEND_SCHEMA_ID(ai_model, schema_keys, schema_key_size, sql))) {
        LOG_WARN("failed to append ai_model id to sql", K(ret), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
      DEFINE_SQL_CLIENT_RETRY_WEAK_WITH_SNAPSHOT(sql_client, snapshot_timestamp);

      if (OB_FAIL(sql.append(" ORDER BY tenant_id DESC, model_id DESC, schema_version DESC"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result", K(ret), K(sql));
      } else if (OB_FAIL(ObSchemaRetrieveUtils::retrieve_ai_model_schema(tenant_id, *result, schema_array))) {
        LOG_WARN("failed to ObSchemaRetrieveUtils::retrieve_ai_model_schema", K(ret), K(tenant_id), K(sql));
      }
    }
  }

  return ret;
}

int ObSchemaServiceSQLImpl::fetch_ai_providers(ObISQLClient &sql_client,
                                               const ObRefreshSchemaStatus &schema_status,
                                               const int64_t schema_version,
                                               const uint64_t tenant_id,
                                               ObIArray<ObAIProviderSchema> &schema_array,
                                               const SchemaKey *schema_keys,
                                               const int64_t schema_key_size)
{
  int ret = OB_SUCCESS;

  const uint64_t exec_tenant_id = fill_exec_tenant_id(schema_status);

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;

    if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id=0",
                               OB_ALL_AI_MODEL_PROVIDER_HISTORY_TNAME))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND schema_version <= %ld", schema_version))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_NOT_NULL(schema_keys) && schema_key_size > 0) {
      if (OB_FAIL(sql.append(" AND provider_id IN"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(SQL_APPEND_SCHEMA_ID(ai_provider, schema_keys, schema_key_size, sql))) {
        LOG_WARN("failed to append ai_provider id to sql", K(ret), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
      DEFINE_SQL_CLIENT_RETRY_WEAK_WITH_SNAPSHOT(sql_client, snapshot_timestamp);

      if (OB_FAIL(sql.append(" ORDER BY tenant_id DESC, provider_id DESC, schema_version DESC"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result", K(ret), K(sql));
      } else if (OB_FAIL(ObSchemaRetrieveUtils::retrieve_ai_provider_schema(tenant_id, *result, schema_array))) {
        LOG_WARN("failed to retrieve_ai_provider_schema", K(ret), K(tenant_id), K(sql));
      }
    }
  }

  return ret;
}

int ObSchemaServiceSQLImpl::fetch_ai_gateways(ObISQLClient &sql_client,
                                              const ObRefreshSchemaStatus &schema_status,
                                              const int64_t schema_version,
                                              const uint64_t tenant_id,
                                              ObIArray<ObAIGatewaySchema> &schema_array,
                                              const SchemaKey *schema_keys,
                                              const int64_t schema_key_size)
{
  int ret = OB_SUCCESS;

  const uint64_t exec_tenant_id = fill_exec_tenant_id(schema_status);

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;

    if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id=0",
                               OB_ALL_AI_GATEWAY_HISTORY_TNAME))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND schema_version <= %ld", schema_version))) {
      LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
    } else if (OB_NOT_NULL(schema_keys) && schema_key_size > 0) {
      if (OB_FAIL(sql.append(" AND gateway_id IN"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(SQL_APPEND_SCHEMA_ID(ai_gateway, schema_keys, schema_key_size, sql))) {
        LOG_WARN("failed to append ai_gateway id to sql", K(ret), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
      DEFINE_SQL_CLIENT_RETRY_WEAK_WITH_SNAPSHOT(sql_client, snapshot_timestamp);

      if (OB_FAIL(sql.append(" ORDER BY tenant_id DESC, gateway_id DESC, schema_version DESC"))) {
        LOG_WARN("failed to append_fmt to sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result", K(ret), K(sql));
      } else if (OB_FAIL(ObSchemaRetrieveUtils::retrieve_ai_gateway_schema(tenant_id, *result, schema_array))) {
        LOG_WARN("failed to retrieve_ai_gateway_schema", K(ret), K(tenant_id), K(sql));
      }
    }
  }

  return ret;
}
