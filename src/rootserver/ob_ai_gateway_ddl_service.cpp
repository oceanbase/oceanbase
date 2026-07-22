/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ai_gateway_ddl_service.h"
#include "rootserver/ob_ai_gateway_ddl_operator.h"
#include "share/schema/ob_ai_gateway_mgr.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace rootserver
{

int ObAIGatewayDDLService::create_gateway(const obrpc::ObCreateAiGatewayArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObAIGatewaySchema new_schema;
    new_schema.set_tenant_id(tenant_id);
    ObSchemaGetterGuard schema_guard;
    const ObAIGatewaySchema *old_gateway_schema = nullptr;
    if (OB_FAIL(new_schema.set_name(arg.gateway_name_))) {
      LOG_WARN("failed to set gateway name", K(ret), K(arg.gateway_name_));
    } else if (OB_FAIL(new_schema.set_endpoints(arg.endpoints_))) {
      LOG_WARN("failed to set endpoints", K(ret));
    } else if (!arg.circuit_breaker_.empty()
               && OB_FAIL(new_schema.set_circuit_breaker(arg.circuit_breaker_))) {
      LOG_WARN("failed to set circuit_breaker", K(ret));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_gateway_schema(tenant_id, arg.gateway_name_, old_gateway_schema))) {
      LOG_WARN("fail to get ai gateway schema", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else if (OB_NOT_NULL(old_gateway_schema)) {
      ret = OB_AI_FUNC_MODEL_EXISTS;
      LOG_USER_ERROR(OB_AI_FUNC_MODEL_EXISTS, arg.gateway_name_.length(), arg.gateway_name_.ptr());
      LOG_WARN("ai gateway already exists", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else if (arg.endpoints_.empty()) {
      ret = OB_AI_FUNC_PARAM_EMPTY;
      ObString var_name = "endpoints";
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
      LOG_WARN("endpoints is required for new gateway", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else {
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIGatewayDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.check_endpoint_provider_exists(new_schema, trans))) {
        LOG_WARN("failed to check endpoint provider exists", K(ret), K(new_schema));
      } else if (OB_FAIL(ddl_operator.register_gateway(new_schema, old_gateway_schema, arg.ddl_stmt_str_, trans))) {
        LOG_WARN("failed to register ai gateway", K(new_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAIGatewayDDLService::alter_gateway(const obrpc::ObAlterAiGatewayArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObAIGatewaySchema new_schema;
    new_schema.set_tenant_id(tenant_id);
    ObSchemaGetterGuard schema_guard;
    const ObAIGatewaySchema *old_gateway_schema = nullptr;
    if (OB_FAIL(new_schema.set_name(arg.gateway_name_))) {
      LOG_WARN("failed to set gateway name", K(ret), K(arg.gateway_name_));
    } else if (OB_FAIL(new_schema.set_endpoints(arg.endpoints_))) {
      LOG_WARN("failed to set endpoints", K(ret));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_gateway_schema(tenant_id, arg.gateway_name_, old_gateway_schema))) {
      LOG_WARN("fail to get ai gateway schema", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else if (OB_ISNULL(old_gateway_schema)) {
      ret = OB_AI_FUNC_MODEL_NOT_FOUND;
      LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, arg.gateway_name_.length(), arg.gateway_name_.ptr());
      LOG_WARN("ai gateway not found", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else if (arg.endpoints_.empty()) {
      ret = OB_AI_FUNC_PARAM_EMPTY;
      ObString var_name = "endpoints";
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
      LOG_WARN("endpoints is required for alter gateway", K(ret));
    }

    if (OB_SUCC(ret)) {
      // ALTER fully replaces endpoints; circuit_breaker is a field-level merge so an
      // ALTER that omits (or partially sets) it preserves the existing config.
      const ObString old_cb = old_gateway_schema->get_circuit_breaker();
      if (arg.circuit_breaker_.empty()) {
        if (OB_FAIL(new_schema.set_circuit_breaker(old_cb))) {
          LOG_WARN("failed to preserve circuit_breaker", K(ret));
        }
      } else if (old_cb.empty()) {
        if (OB_FAIL(new_schema.set_circuit_breaker(arg.circuit_breaker_))) {
          LOG_WARN("failed to set circuit_breaker", K(ret));
        }
      } else {
        ObArenaAllocator tmp_alloc("AiGwCbMerge");
        ObString merged_cb;
        if (OB_FAIL(share::merge_gateway_circuit_breaker_json(tmp_alloc, old_cb,
                                                              arg.circuit_breaker_, merged_cb))) {
          LOG_WARN("failed to merge circuit_breaker on alter", K(ret));
        } else if (OB_FAIL(new_schema.set_circuit_breaker(merged_cb))) {
          LOG_WARN("failed to set merged circuit_breaker", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIGatewayDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.check_endpoint_provider_exists(new_schema, trans))) {
        LOG_WARN("failed to check endpoint provider exists", K(ret), K(new_schema));
      } else if (OB_FAIL(ddl_operator.register_gateway(new_schema, old_gateway_schema, arg.ddl_stmt_str_, trans))) {
        LOG_WARN("failed to alter ai gateway", K(new_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAIGatewayDDLService::drop_gateway(const obrpc::ObDropAiGatewayArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObAIGatewaySchema *old_gateway_schema = nullptr;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_gateway_schema(tenant_id, arg.gateway_name_, old_gateway_schema))) {
      LOG_WARN("fail to get ai gateway schema", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else if (OB_ISNULL(old_gateway_schema)) {
      ret = OB_AI_FUNC_MODEL_NOT_FOUND;
      LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, arg.gateway_name_.length(), arg.gateway_name_.ptr());
      LOG_WARN("ai gateway not found", K(ret), K(tenant_id), K(arg.gateway_name_));
    } else {
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIGatewayDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.unregister_gateway(*old_gateway_schema, arg.ddl_stmt_str_, trans))) {
        LOG_WARN("failed to unregister ai gateway", KPC(old_gateway_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
