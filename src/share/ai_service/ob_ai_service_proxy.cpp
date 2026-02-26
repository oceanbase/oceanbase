/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/ai_service/ob_ai_service_proxy.h"
#include "share/ob_server_struct.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_max_id_fetcher.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "lib/utility/ob_utility.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::common::sqlclient;

int ObAiServiceProxy::insert_ai_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(user_tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(user_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", user_tenant_id))) {
    LOG_WARN("failed to add column", K(ret), K(user_tenant_id));
  } else if (OB_FAIL(sql.add_pk_column("endpoint_id", endpoint.endpoint_id_))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_pk_column("scope", ObHexEscapeSqlStr(endpoint.scope_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
    LOG_WARN("failed to add column", K(ret), K(new_endpoint_version));
  } else if (OB_FAIL(sql.add_column("endpoint_name", endpoint.name_))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("ai_model_name", ObHexEscapeSqlStr(endpoint.ai_model_name_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("url", ObHexEscapeSqlStr(endpoint.url_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("access_key", ObHexEscapeSqlStr(endpoint.access_key_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("request_model_name", ObHexEscapeSqlStr(endpoint.request_model_name_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("provider", ObHexEscapeSqlStr(endpoint.provider_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("parameters", ObHexEscapeSqlStr(endpoint.parameters_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ObHexEscapeSqlStr(endpoint.request_transform_fn_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ObHexEscapeSqlStr(endpoint.response_transform_fn_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(tenant_id), K(buffer));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
  }

  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    ret = OB_ENTRY_EXIST;
    LOG_USER_ERROR(OB_ENTRY_EXIST, "endpoint already exists");
    LOG_WARN("ai model endpoint already exists", KR(ret), K(tenant_id), K(endpoint));
  }
  LOG_DEBUG("insert ai model endpoint", K(tenant_id), K(new_endpoint_version), K(endpoint), K(buffer), KR(ret));
  return ret;
}

int ObAiServiceProxy::update_ai_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(user_tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(user_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", user_tenant_id))) {
    LOG_WARN("failed to add column", K(ret), K(user_tenant_id));
  } else if (OB_FAIL(sql.add_pk_column("endpoint_id", endpoint.endpoint_id_))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_pk_column("scope", ObHexEscapeSqlStr(endpoint.scope_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
    LOG_WARN("failed to add column", K(ret), K(new_endpoint_version));
  } else if (OB_FAIL(sql.add_column("endpoint_name", endpoint.name_))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("ai_model_name", ObHexEscapeSqlStr(endpoint.ai_model_name_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("url", ObHexEscapeSqlStr(endpoint.url_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("access_key", ObHexEscapeSqlStr(endpoint.access_key_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("request_model_name", ObHexEscapeSqlStr(endpoint.request_model_name_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("provider", ObHexEscapeSqlStr(endpoint.provider_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("parameters", ObHexEscapeSqlStr(endpoint.parameters_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ObHexEscapeSqlStr(endpoint.request_transform_fn_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ObHexEscapeSqlStr(endpoint.response_transform_fn_)))) {
    LOG_WARN("failed to add column", K(ret), K(endpoint));
  } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
    LOG_WARN("failed to splice_update_sql", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(buffer));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
  }
  LOG_DEBUG("update ai model endpoint", K(tenant_id), K(new_endpoint_version), K(endpoint), K(buffer), KR(ret));
  return ret;
}

int ObAiServiceProxy::select_ai_endpoint(const uint64_t tenant_id, ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
                                         const ObString &name, ObAiModelEndpointInfo &endpoint, bool for_update)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  endpoint.reset();
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(user_tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(user_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("failed to set timeout ctx", KR(ret), K(ctx), K(default_timeout));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME, user_tenant_id))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql), K(user_tenant_id));
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
    LOG_WARN("failed to append name", KR(ret), K(name));
  } else if (for_update && (OB_FAIL(sql.append_fmt(" FOR UPDATE")))) {
    LOG_WARN("failed to append for update", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
          LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_NOT_FOUND, name.length(), name.ptr());
          LOG_WARN("ai model endpoint not found", K(ret), K(name));
        } else {
          LOG_WARN("failed to get next result", KR(ret));
        }
      } else if (OB_FAIL(build_ai_endpoint_(allocator, *result, endpoint))) {
        LOG_WARN("failed to build ai endpoint", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObAiServiceProxy::select_ai_endpoint_by_ai_model_name(
  const uint64_t tenant_id, ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
  const ObString &ai_model_name, ObNameCaseMode name_case_mode, ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  endpoint.reset();
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(user_tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(user_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("failed to set timeout ctx", KR(ret), K(ctx), K(default_timeout));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu", OB_ALL_AI_MODEL_ENDPOINT_TNAME, user_tenant_id))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql), K(user_tenant_id));
  } else {
    if (OB_ORIGIN_AND_INSENSITIVE == name_case_mode || OB_LOWERCASE_AND_INSENSITIVE == name_case_mode) {
      if (OB_FAIL(sql.append(" AND ai_model_name = "))) {
        LOG_WARN("failed to append sql string", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(ai_model_name, sql))) {
        LOG_WARN("failed to append ai model name", KR(ret), K(ai_model_name));
      }
    } else if (OB_ORIGIN_AND_SENSITIVE == name_case_mode) {
      if (OB_FAIL(sql.append(" AND ai_model_name COLLATE utf8mb4_bin = "))) {
        LOG_WARN("failed to append sql string", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(ai_model_name, sql))) {
        LOG_WARN("failed to append ai model name", KR(ret), K(ai_model_name));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid name case mode", K(ret), K(name_case_mode));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid name case mode");
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
          LOG_WARN("ai model endpoint not found by ai model name", K(ret), K(ai_model_name));
        } else {
          LOG_WARN("failed to get next result", KR(ret));
        }
      } else if (OB_FAIL(build_ai_endpoint_(allocator, *result, endpoint))) {
        LOG_WARN("failed to build ai endpoint", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObAiServiceProxy::build_ai_endpoint_(ObArenaAllocator &allocator, ObMySQLResult &result, ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  endpoint.reset();
  ObString scope;
  ObString name;
  ObString ai_model_name;
  ObString url;
  ObString access_key;
  ObString provider;
  ObString request_model_name;
  ObString arguments;
  ObString request_transform_fn;
  ObString response_transform_fn;
  int64_t type = 0;
  EXTRACT_INT_FIELD_MYSQL(result, "endpoint_id", endpoint.endpoint_id_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "scope", scope);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "endpoint_name", name);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "ai_model_name", ai_model_name);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "url", url);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "access_key", access_key);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "provider", provider, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "request_model_name", request_model_name, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "parameters", arguments, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "request_transform_fn", request_transform_fn, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "response_transform_fn", response_transform_fn, true, false, ""/*wont be used*/);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, scope, endpoint.scope_))) {
    LOG_WARN("failed to deep copy scope", KR(ret), K(scope));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, name, endpoint.name_))) {
    LOG_WARN("failed to deep copy name", KR(ret), K(name));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, ai_model_name, endpoint.ai_model_name_))) {
    LOG_WARN("failed to deep copy ai_model_name", KR(ret), K(ai_model_name));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, url, endpoint.url_))) {
    LOG_WARN("failed to deep copy url", KR(ret), K(url));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, access_key, endpoint.access_key_))) {
    LOG_WARN("failed to deep copy access_key", KR(ret), K(access_key));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, provider, endpoint.provider_))) {
    LOG_WARN("failed to deep copy provider", KR(ret), K(provider));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, request_model_name, endpoint.request_model_name_))) {
    LOG_WARN("failed to deep copy request_model_name", KR(ret), K(request_model_name));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, arguments, endpoint.parameters_))) {
    LOG_WARN("failed to deep copy arguments", KR(ret), K(arguments));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, request_transform_fn, endpoint.request_transform_fn_))) {
    LOG_WARN("failed to deep copy request_transform_fn", KR(ret), K(request_transform_fn));
  } else if (OB_FAIL(deep_copy_ob_string(allocator, response_transform_fn, endpoint.response_transform_fn_))) {
    LOG_WARN("failed to deep copy response_transform_fn", KR(ret), K(response_transform_fn));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(endpoint.check_valid())) {
      LOG_WARN("select invalid ai endpoint", KR(ret), K(name), K(endpoint));
    }
  }

  return ret;
}

int ObAiServiceProxy::drop_ai_model_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const ObString &name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(user_tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(user_tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME, user_tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
    LOG_WARN("failed to append name", KR(ret), K(name));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
    LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_NOT_FOUND, name.length(), name.ptr());
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObAiServiceProxy::check_ai_endpoint_exists(const uint64_t tenant_id, ObArenaAllocator &allocator, ObISQLClient &sql_proxy, const ObString &name, bool &is_exists)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  int64_t count = 0;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("failed to set timeout ctx", KR(ret), K(ctx), K(default_timeout));
  } else if (OB_FAIL(sql.assign_fmt("SELECT count(*) FROM %s WHERE tenant_id = %lu AND endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME, user_tenant_id))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql), K(user_tenant_id));
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
    LOG_WARN("failed to append name", KR(ret), K(name));
  } else if (OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append for update", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      const int64_t idx = 0;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next result", KR(ret));
      } else if (OB_FAIL(result->get_int(idx, count))) {
        LOG_WARN("failed to get count", KR(ret));
      } else if (count > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count should be less or equal than one", KR(ret), K(count));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_exists = count == 1;
  }
  return ret;
}
