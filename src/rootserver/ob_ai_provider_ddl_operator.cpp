/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ai_provider_ddl_operator.h"
#include "share/schema/ob_ai_provider_sql_service.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace rootserver
{

int ObAIProviderDDLOperator::register_provider(ObAIProviderSchema &provider_schema,
                                               const ObAIProviderSchema *old_provider_schema,
                                               const ObString &ddl_stmt,
                                               common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = provider_schema.get_tenant_id();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_NOT_NULL(old_provider_schema)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("failed to gen new schema version", K(ret), K(provider_schema));
    } else {
      provider_schema.set_provider_id(old_provider_schema->get_provider_id());
      provider_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service_impl->get_ai_provider_sql_service().alter_ai_provider(
          provider_schema, *old_provider_schema, new_schema_version, ddl_stmt, trans))) {
        LOG_WARN("failed to alter ai provider", K(ret), K(provider_schema));
      }
    }
  } else {
    int64_t new_schema_version = OB_INVALID_VERSION;
    uint64_t new_provider_id = OB_INVALID_ID;
    if (OB_FAIL(schema_service_impl->fetch_new_ai_provider_id(tenant_id, new_provider_id))) {
      LOG_WARN("failed to fetch new ai provider id", K(ret), K(provider_schema));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("failed to gen new schema version", K(ret), K(provider_schema));
    } else {
      provider_schema.set_provider_id(new_provider_id);
      provider_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service_impl->get_ai_provider_sql_service().create_ai_provider(
          provider_schema, ddl_stmt, trans))) {
        LOG_WARN("failed to create ai provider", K(ret), K(provider_schema));
      }
    }
  }
  return ret;
}

int ObAIProviderDDLOperator::unregister_provider(const ObAIProviderSchema &provider_schema,
                                                 const ObString &ddl_stmt,
                                                 common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = provider_schema.get_tenant_id();
  bool is_referenced = false;
  if (OB_UNLIKELY(!provider_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(provider_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(check_gateway_reference(provider_schema, trans, is_referenced))) {
    LOG_WARN("failed to check gateway reference", K(ret), K(tenant_id));
  } else if (is_referenced) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("provider is referenced by gateway, cannot unregister",
             K(ret), K(provider_schema.get_name()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                   "the ai provider is referenced by a gateway, cannot unregister");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_impl->get_ai_provider_sql_service().drop_ai_provider(
      provider_schema, new_schema_version, ddl_stmt, trans))) {
    LOG_WARN("failed to drop ai provider", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAIProviderDDLOperator::check_gateway_reference(const ObAIProviderSchema &provider_schema,
                                                     common::ObMySQLTransaction &trans,
                                                     bool &is_referenced)
{
  int ret = OB_SUCCESS;
  is_referenced = false;
  ObSqlString sql;
  // __all_ai_gateway rows use tenant_id = 0 in PK (see ObAIGatewaySqlService), same as
  // __all_ai_model_provider; filter must match physical column, not exec tenant id.
  if (OB_FAIL(sql.assign_fmt(
      "SELECT count(*) as cnt FROM %s g, "
      "JSON_TABLE(CAST(g.endpoints AS JSON), '$[*]' COLUMNS ("
      "ep_model VARCHAR(512) PATH '$.model')) jt "
      "WHERE g.tenant_id = 0 AND SUBSTRING_INDEX(jt.ep_model, '/', 1) = ",
      share::OB_ALL_AI_GATEWAY_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(provider_schema.get_name(), sql))) {
    LOG_WARN("failed to append hex escaped provider name", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(trans.read(res, provider_schema.get_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to read gateway table", K(ret));
      } else if (OB_ISNULL(res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null result", K(ret));
      } else if (OB_FAIL(res.get_result()->next())) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        int64_t cnt = 0;
        if (OB_FAIL(res.get_result()->get_int("cnt", cnt))) {
          LOG_WARN("failed to get int", K(ret));
        } else {
          is_referenced = (cnt > 0);
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
}
