/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ai_gateway_ddl_operator.h"
#include "share/schema/ob_ai_gateway_sql_service.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace rootserver
{

int ObAIGatewayDDLOperator::register_gateway(ObAIGatewaySchema &gateway_schema,
                                              const ObAIGatewaySchema *old_gateway_schema,
                                              const ObString &ddl_stmt,
                                              common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = gateway_schema.get_tenant_id();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_NOT_NULL(old_gateway_schema)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("failed to gen new schema version", K(ret), K(gateway_schema));
    } else {
      gateway_schema.set_gateway_id(old_gateway_schema->get_gateway_id());
      gateway_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service_impl->get_ai_gateway_sql_service().alter_ai_gateway(
          gateway_schema, *old_gateway_schema, new_schema_version, ddl_stmt, trans))) {
        LOG_WARN("failed to alter ai gateway", K(ret), K(gateway_schema));
      }
    }
  } else {
    int64_t new_schema_version = OB_INVALID_VERSION;
    uint64_t new_gateway_id = OB_INVALID_ID;
    if (OB_FAIL(schema_service_impl->fetch_new_ai_gateway_id(tenant_id, new_gateway_id))) {
      LOG_WARN("failed to fetch new ai gateway id", K(ret), K(gateway_schema));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("failed to gen new schema version", K(ret), K(gateway_schema));
    } else {
      gateway_schema.set_gateway_id(new_gateway_id);
      gateway_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service_impl->get_ai_gateway_sql_service().create_ai_gateway(
          gateway_schema, ddl_stmt, trans))) {
        LOG_WARN("failed to create ai gateway", K(ret), K(gateway_schema));
      }
    }
  }
  return ret;
}

int ObAIGatewayDDLOperator::unregister_gateway(const ObAIGatewaySchema &gateway_schema,
                                                const ObString &ddl_stmt,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = gateway_schema.get_tenant_id();
  if (OB_UNLIKELY(!gateway_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gateway_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_impl->get_ai_gateway_sql_service().drop_ai_gateway(
      gateway_schema, new_schema_version, ddl_stmt, trans))) {
    LOG_WARN("failed to drop ai gateway", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAIGatewayDDLOperator::check_endpoint_provider_exists(const ObAIGatewaySchema &gateway_schema,
                                                            common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObIJsonBase *json_root = nullptr;
  const ObString endpoints_str = gateway_schema.get_endpoints();

  if (endpoints_str.empty()) {
    // nothing to check
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                       endpoints_str,
                                                       ObJsonInType::JSON_TREE,
                                                       ObJsonInType::JSON_TREE,
                                                       json_root))) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_WARN("failed to parse endpoints json", K(ret), K(endpoints_str));
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 9, "endpoints");
  } else if (OB_ISNULL(json_root) || json_root->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_WARN("endpoints must be a JSON array", K(ret));
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 9, "endpoints");
  } else {
    const uint64_t count = json_root->element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObIJsonBase *elem = nullptr;
      if (OB_FAIL(json_root->get_array_element(i, elem))) {
        LOG_WARN("failed to get array element", K(ret), K(i));
      } else if (OB_ISNULL(elem) || elem->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
        LOG_WARN("endpoint element must be a JSON object", K(ret), K(i));
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 9, "endpoints");
      } else {
        // validate model contains '/' and provider exists
        ObString model_full;
        {
          ObIJsonBase *model_value = nullptr;
          if (OB_FAIL(elem->get_object_value("model", model_value))) {
            ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
            LOG_WARN("endpoint missing model field", K(ret), K(i));
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 12, "model in endpoint");
          } else if (OB_ISNULL(model_value) || model_value->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
            LOG_WARN("model must be a string", K(ret));
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
          } else {
            model_full = ObString(model_value->get_data_length(), model_value->get_data());
            const char *slash_ptr = model_full.empty() ? nullptr
                : static_cast<const char *>(MEMCHR(model_full.ptr(), '/', model_full.length()));
            if (nullptr == slash_ptr
                || slash_ptr == model_full.ptr()
                || slash_ptr == model_full.ptr() + model_full.length() - 1) {
              ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
              LOG_WARN("model must be in 'provider/model' format with both parts non-empty", K(ret));
              LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
            }
          }
        }
        if (OB_SUCC(ret)) {
          // validate weight is non-negative integer
          {
            ObIJsonBase *weight_value = nullptr;
            if (OB_FAIL(elem->get_object_value("weight", weight_value))) {
              if (OB_SEARCH_NOT_FOUND == ret) {
                ret = OB_SUCCESS; // weight is optional
              } else {
                LOG_WARN("failed to get weight from json", K(ret));
              }
            } else if (OB_NOT_NULL(weight_value)) {
              if (weight_value->json_type() != ObJsonNodeType::J_INT
                  && weight_value->json_type() != ObJsonNodeType::J_UINT) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_WARN("weight must be a non-negative integer", K(ret));
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 6, "weight");
              } else if (weight_value->get_int() < 0) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_WARN("weight must be non-negative", K(ret));
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 6, "weight");
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          // extract provider name (part before '/') and check it exists
          const char *slash = static_cast<const char *>(
              MEMCHR(model_full.ptr(), '/', model_full.length()));
          ObString provider_name(static_cast<int32_t>(slash - model_full.ptr()), model_full.ptr());
          ObSqlString sql;
          if (provider_name.length() > 128) {
            ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
            LOG_WARN("provider name too long in model field", K(ret), K(provider_name));
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
          } else if (OB_FAIL(sql.assign_fmt(
              "SELECT count(*) as cnt FROM %s WHERE tenant_id = 0 AND name = ",
              share::OB_ALL_AI_MODEL_PROVIDER_TNAME))) {
            LOG_WARN("failed to assign sql", KR(ret));
          } else if (OB_FAIL(sql_append_hex_escape_str(provider_name, sql))) {
            LOG_WARN("failed to append provider_name", KR(ret), K(provider_name));
          } else {
            SMART_VAR(ObMySQLProxy::MySQLResult, res) {
              if (OB_FAIL(trans.read(res, gateway_schema.get_tenant_id(), sql.ptr()))) {
                LOG_WARN("failed to read provider table", K(ret));
              } else if (OB_ISNULL(res.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null result", K(ret));
              } else if (OB_FAIL(res.get_result()->next())) {
                LOG_WARN("failed to get next", K(ret));
              } else {
                int64_t cnt = 0;
                if (OB_FAIL(res.get_result()->get_int("cnt", cnt))) {
                  LOG_WARN("failed to get int", K(ret));
                } else if (0 == cnt) {
                  ret = OB_AI_FUNC_MODEL_NOT_FOUND;
                  LOG_WARN("provider referenced by endpoint not found",
                           K(ret), K(provider_name));
                  LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND,
                                 provider_name.length(), provider_name.ptr());
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
}
