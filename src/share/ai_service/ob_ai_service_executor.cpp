/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ai_service/ob_ai_service_executor.h"
#include "share/ai_service/ob_ai_service_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "share/ob_max_id_fetcher.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "share/restore/ob_import_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::observer;
using namespace oceanbase::transaction::tablelock;

namespace oceanbase
{
namespace share
{
const int64_t ObAiServiceExecutor::SPECIAL_ENDPOINT_ID_FOR_VERSION = -1;
const int64_t ObAiServiceExecutor::INIT_ENDPOINT_VERSION = 0;
const char *ObAiServiceExecutor::SPECIAL_ENDPOINT_SCOPE_FOR_VERSION = "";

int ObAiServiceExecutor::create_ai_model_endpoint(common::ObArenaAllocator &allocator, const ObString &endpoint_name, const ObIJsonBase &create_jbase)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObAiModelEndpointInfo endpoint;
  uint64_t new_endpoint_id = OB_INVALID_ID;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  bool is_exists = false;
  ObAiModelEndpointInfo tmp_endpoint;
  if (OB_FAIL(endpoint.parse_from_json_base(allocator, endpoint_name, create_jbase))) {
    LOG_WARN("failed to parse ai service endpoint info", KR(ret), K(create_jbase));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(ObAiServiceProxy::check_ai_endpoint_exists(tenant_id, allocator, trans, endpoint_name, is_exists))) {
    LOG_WARN("failed to check ai endpoint exists", KR(ret), K(endpoint_name));
  } else if (is_exists) {
    ret = OB_AI_FUNC_ENDPOINT_EXISTS;
    LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_EXISTS, endpoint_name.length(), endpoint_name.ptr());
  } else {
    // check if the ai model endpoint has the same ai model name is already exists
    if (OB_FAIL(read_ai_endpoint_by_ai_model_name(allocator, endpoint.get_ai_model_name(), tmp_endpoint))) {
      if (ret == OB_AI_FUNC_ENDPOINT_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read ai endpoint by ai model name", KR(ret), K(endpoint.get_ai_model_name()));
      }
    } else {
      ret = OB_AI_FUNC_ENDPOINT_EXISTS;
      LOG_WARN("there is already an ai endpoint with the same ai model name", KR(ret), K(tmp_endpoint.get_name()), K(tmp_endpoint.get_ai_model_name()));
      FORWARD_USER_ERROR_MSG(OB_AI_FUNC_ENDPOINT_EXISTS, "The ai model endpoint '%.*s' has the same ai model name '%.*s'", tmp_endpoint.get_name().length(), tmp_endpoint.get_name().ptr(), tmp_endpoint.get_ai_model_name().length(), tmp_endpoint.get_ai_model_name().ptr());
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_new_ai_model_endpoint_id(tenant_id, new_endpoint_id))) {
    LOG_WARN("failed to fetch new ai model endpoint id", KR(ret), K(tenant_id));
  } else if (FALSE_IT(endpoint.set_endpoint_id(new_endpoint_id))) {
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, tenant_id, new_endpoint_version))) {
    LOG_WARN("failed to lock and fetch endpoint version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAiServiceProxy::insert_ai_endpoint(tenant_id, trans, new_endpoint_version, endpoint))) {
    LOG_WARN("failed to insert ai endpoint", KR(ret), K(endpoint));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAiServiceExecutor::alter_ai_model_endpoint(ObArenaAllocator &allocator, const ObString &name, const ObIJsonBase &alter_jbase)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo old_endpoint;
  ObAiModelEndpointInfo new_endpoint;
  ObMySQLTransaction trans;
  ObAiModelEndpointInfo tmp_endpoint;
  ObNameCaseMode name_case_mode;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  uint64_t user_tenant_id = MTL_ID(); // ai model name maybe case sensitive, so need user tenant id to get name case mode
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint(tenant_id, allocator, *GCTX.sql_proxy_, name, old_endpoint, true))) {
    LOG_WARN("failed to select ai endpoint", K(ret), K(name));
  } else if (OB_FAIL(construct_new_endpoint(allocator, old_endpoint, alter_jbase, new_endpoint))) {
    LOG_WARN("failed to construct new endpoint", KR(ret), K(old_endpoint), K(alter_jbase));
  } else if (OB_FAIL(new_endpoint.check_valid())) {
    LOG_WARN("invalid endpoint", KR(ret), K(new_endpoint));
  } else if (OB_FAIL(ObImportTableUtil::get_tenant_name_case_mode(user_tenant_id, name_case_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(user_tenant_id));
  } else if (ObCharset::case_mode_equal(name_case_mode, new_endpoint.get_ai_model_name(), old_endpoint.get_ai_model_name())) {
    // need check name case mode equal, if not change ai model name, just update the endpoint
    LOG_INFO("ai model name is the same, just update the endpoint", KR(ret), K(name), K(user_tenant_id), K(name_case_mode), K(new_endpoint), K(old_endpoint));
  } else {
    // if change ai model name, check if the ai model endpoint has the same ai model name is already exists
    // if not exists, continue
    if (OB_FAIL(read_ai_endpoint_by_ai_model_name(allocator, new_endpoint.get_ai_model_name(), tmp_endpoint))) {
      if (ret == OB_AI_FUNC_ENDPOINT_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read ai endpoint by ai model name", KR(ret), K(new_endpoint.get_ai_model_name()));
      }
    } else {
      ret = OB_AI_FUNC_ENDPOINT_EXISTS;
      LOG_WARN("there is already an ai endpoint with the same ai model name", KR(ret), K(tmp_endpoint.get_name()), K(tmp_endpoint.get_ai_model_name()));
      FORWARD_USER_ERROR_MSG(OB_AI_FUNC_ENDPOINT_EXISTS, "The ai model endpoint '%.*s' has the same ai model name '%.*s'", tmp_endpoint.get_name().length(), tmp_endpoint.get_name().ptr(), tmp_endpoint.get_ai_model_name().length(), tmp_endpoint.get_ai_model_name().ptr());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, tenant_id, new_endpoint_version))) {
    LOG_WARN("failed to lock and fetch endpoint version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAiServiceProxy::update_ai_endpoint(tenant_id, trans, new_endpoint_version, new_endpoint))) {
    LOG_WARN("failed to insert new ai endpoint", KR(ret), K(new_endpoint));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAiServiceExecutor::construct_new_endpoint(common::ObArenaAllocator &allocator,
                                                const ObAiModelEndpointInfo &old_endpoint,
                                                const ObIJsonBase &alter_jbase,
                                                ObAiModelEndpointInfo &new_endpoint)
{
  int ret = OB_SUCCESS;
  new_endpoint = old_endpoint;
  if (OB_FAIL(new_endpoint.merge_delta_endpoint(allocator, alter_jbase))) {
    LOG_WARN("failed to merge delta endpoint", KR(ret), K(new_endpoint), K(alter_jbase));
  }
  return ret;
}

int ObAiServiceExecutor::drop_ai_model_endpoint(const ObString &endpoint_name)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, tenant_id, new_endpoint_version))) {
    LOG_WARN("failed to lock and fetch endpoint version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAiServiceProxy::drop_ai_model_endpoint(tenant_id, trans, endpoint_name))) {
    LOG_WARN("failed to drop ai endpoint", KR(ret), K(endpoint_name));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAiServiceExecutor::read_ai_endpoint(ObArenaAllocator &allocator, const ObString &endpoint_name, ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint(tenant_id, allocator, *GCTX.sql_proxy_, endpoint_name, endpoint_info))) {
    LOG_WARN("failed to select ai endpoint", KR(ret), K(endpoint_name));
  }
  return ret;
}

int ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(ObArenaAllocator &allocator, const ObString &ai_model_name, ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  uint64_t meta_tenant_id = gen_meta_tenant_id(MTL_ID());
  ObNameCaseMode name_case_mode;
  if (OB_FAIL(ObImportTableUtil::get_tenant_name_case_mode(tenant_id, name_case_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID >= name_case_mode || OB_NAME_CASE_MAX <= name_case_mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid name case mode", K(ret), K(name_case_mode));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint_by_ai_model_name(meta_tenant_id, allocator, *GCTX.sql_proxy_, ai_model_name, name_case_mode, endpoint_info))) {
    LOG_WARN("failed to select ai endpoint by ai model name", KR(ret), K(ai_model_name));
  }
  return ret;
}

int ObAiServiceExecutor::fetch_new_ai_model_endpoint_id(const uint64_t tenant_id, uint64_t &new_ai_model_endpoint_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObMaxIdFetcher fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(fetcher.fetch_new_max_id(tenant_id, OB_MAX_USED_AI_MODEL_ENDPOINT_ID_TYPE, new_ai_model_endpoint_id, 0))) {
      LOG_WARN("failed to fetch new ai model endpoint id", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObAiServiceExecutor::lock_and_fetch_endpoint_version(ObMySQLTransaction &trans, const uint64_t tenant_id, int64_t &endpoint_version)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  observer::ObInnerSQLConnection *conn = NULL;
  ObSqlString sql;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  int64_t old_endpoint_version = OB_INVALID_VERSION;
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  bool need_insert = false;

  if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id,
                                                           OB_ALL_AI_MODEL_ENDPOINT_TID,
                                                           EXCLUSIVE,
                                                           timeout,
                                                           conn))) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT VERSION FROM %s WHERE tenant_id = %lu AND endpoint_id = %ld AND scope = '%s'",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME, user_tenant_id, SPECIAL_ENDPOINT_ID_FOR_VERSION, SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      int tmp_ret = OB_SUCCESS;
      const int64_t idx = 0;
      if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read sql", KR(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          need_insert = true;
          res.reset();
        } else {
          LOG_WARN("failed to get next result", K(sql), KR(ret));
        }
      } else if (OB_FAIL(result->get_int(idx, old_endpoint_version))) {
        LOG_WARN("failed to get version", K(sql), KR(ret));
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      } else {
        res.reset();
        new_endpoint_version = old_endpoint_version + 1;
      }
    }

    if (OB_SUCC(ret) && need_insert) {
      if (OB_FAIL(insert_special_endpoint_for_version(trans, tenant_id))) {
        LOG_WARN("failed to insert special endpoint for version", KR(ret), K(tenant_id));
      } else {
        new_endpoint_version = INIT_ENDPOINT_VERSION + 1;
      }
    }

    if (OB_SUCC(ret)) {
      ObDMLSqlSplicer sql;
      ObSqlString buffer;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.add_pk_column("tenant_id", user_tenant_id))) {
        LOG_WARN("failed to add column", K(ret), K(user_tenant_id));
      } else if (OB_FAIL(sql.add_pk_column("endpoint_id", SPECIAL_ENDPOINT_ID_FOR_VERSION))) {
        LOG_WARN("failed to add column", K(ret), K(SPECIAL_ENDPOINT_ID_FOR_VERSION));
      } else if (OB_FAIL(sql.add_pk_column("scope", SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
        LOG_WARN("failed to add column", K(ret), K(SPECIAL_ENDPOINT_SCOPE_FOR_VERSION));
      } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
        LOG_WARN("failed to add column", K(ret), K(new_endpoint_version));
      } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
        LOG_WARN("failed to splice_insert_sql", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, buffer.ptr(), affected_rows))) {
        LOG_WARN("failed to write sql", KR(ret), K(tenant_id), K(buffer));
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
      }
    }
  }

  if (OB_SUCC(ret)) {
    endpoint_version = new_endpoint_version;
  }
  return ret;
}

int ObAiServiceExecutor::insert_special_endpoint_for_version(ObMySQLTransaction &trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  uint64_t new_endpoint_version = OB_INVALID_VERSION;
  int64_t affected_rows = 0;

  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  if (OB_FAIL(sql.add_pk_column("tenant_id", user_tenant_id))) {
    LOG_WARN("failed to add column", K(ret), K(user_tenant_id));
  } else if (OB_FAIL(sql.add_pk_column("endpoint_id", SPECIAL_ENDPOINT_ID_FOR_VERSION))) {
    LOG_WARN("failed to add column", K(ret), K(SPECIAL_ENDPOINT_ID_FOR_VERSION));
  } else if (OB_FAIL(sql.add_pk_column("scope", SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
    LOG_WARN("failed to add column", K(ret), K(SPECIAL_ENDPOINT_SCOPE_FOR_VERSION));
  } else if (OB_FAIL(sql.add_column("version", INIT_ENDPOINT_VERSION))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("endpoint_name", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("ai_model_name", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("url", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("access_key", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("request_model_name", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("provider", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("parameters", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ""))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(tenant_id), K(buffer));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObAiServiceExecutor::alter_ai_model_profile(ObArenaAllocator &allocator, const ObString &model_str, const ObIJsonBase &alter_jbase)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  uint64_t user_tenant_id = MTL_ID();
  ObString provider_name;
  ObString model_name;
  ObSqlString model_config;
  ObSqlString run_config;
  bool has_model_config = false;
  bool has_run_config = false;

  // parse "provider/model" from model_str
  const char *slash = static_cast<const char *>(MEMCHR(model_str.ptr(), '/', model_str.length()));
  if (OB_ISNULL(slash) || slash == model_str.ptr() || slash == model_str.ptr() + model_str.length() - 1) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
    LOG_WARN("model string must be in provider/model format", K(ret), K(model_str));
  } else {
    provider_name.assign_ptr(model_str.ptr(), static_cast<int32_t>(slash - model_str.ptr()));
    model_name.assign_ptr(slash + 1, static_cast<int32_t>(model_str.length() - (slash - model_str.ptr()) - 1));
  }

  // parse JSON: model_config, run_config, reject access_key
  if (OB_SUCC(ret)) {
    JsonObjectIterator iter = alter_jbase.object_iterator();
    while (OB_SUCC(ret) && !iter.end()) {
      ObJsonObjPair elem;
      if (OB_FAIL(iter.get_elem(elem))) {
        LOG_WARN("failed to get json element", KR(ret));
      } else if (0 == elem.first.case_compare("model_config")) {
        has_model_config = true;
        if (elem.second->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("model_config"), "model_config", (int)strlen("JSON_OBJECT"), "JSON_OBJECT");
        } else {
          common::ObStringBuffer buf(&allocator);
          if (OB_FAIL(elem.second->print(buf, false))) {
            LOG_WARN("failed to serialize model_config", K(ret));
          } else {
            model_config.assign(buf.ptr());
          }
        }
      } else if (0 == elem.first.case_compare("run_config")) {
        has_run_config = true;
        if (elem.second->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("run_config"), "run_config", (int)strlen("JSON_OBJECT"), "JSON_OBJECT");
        } else {
          // validate run_config fields
          bool has_min_concurrency = false;
          bool has_max_concurrency = false;
          int64_t min_concurrency = 0;
          int64_t max_concurrency = 0;
          JsonObjectIterator opt_iter = elem.second->object_iterator();
          while (OB_SUCC(ret) && !opt_iter.end()) {
            ObJsonObjPair opt_elem;
            if (OB_FAIL(opt_iter.get_elem(opt_elem))) {
              LOG_WARN("failed to get run_config element", KR(ret));
            } else if (0 == opt_elem.first.case_compare("batch_size")) {
              if (opt_elem.second->json_type() != ObJsonNodeType::J_INT && opt_elem.second->json_type() != ObJsonNodeType::J_UINT) {
                ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("batch_size"), "batch_size", (int)strlen("INT"), "INT");
              } else if (opt_elem.second->get_int() <= 0) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, (int)strlen("batch_size"), "batch_size");
              }
            } else if (0 == opt_elem.first.case_compare("max_image_size")) {
              if (opt_elem.second->json_type() != ObJsonNodeType::J_INT && opt_elem.second->json_type() != ObJsonNodeType::J_UINT) {
                ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("max_image_size"), "max_image_size", (int)strlen("INT"), "INT");
              } else if (opt_elem.second->get_int() <= 0) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, (int)strlen("max_image_size"), "max_image_size");
              }
            } else if (0 == opt_elem.first.case_compare("min_concurrency")) {
              if (opt_elem.second->json_type() != ObJsonNodeType::J_INT && opt_elem.second->json_type() != ObJsonNodeType::J_UINT) {
                ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("min_concurrency"), "min_concurrency", (int)strlen("INT"), "INT");
              } else if (opt_elem.second->get_int() <= 0) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, (int)strlen("min_concurrency"), "min_concurrency");
              } else {
                has_min_concurrency = true;
                min_concurrency = opt_elem.second->get_int();
              }
            } else if (0 == opt_elem.first.case_compare("max_concurrency")) {
              if (opt_elem.second->json_type() != ObJsonNodeType::J_INT && opt_elem.second->json_type() != ObJsonNodeType::J_UINT) {
                ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_TYPE_INVALID, (int)strlen("max_concurrency"), "max_concurrency", (int)strlen("INT"), "INT");
              } else if (opt_elem.second->get_int() <= 0) {
                ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
                LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, (int)strlen("max_concurrency"), "max_concurrency");
              } else {
                has_max_concurrency = true;
                max_concurrency = opt_elem.second->get_int();
              }
            } else {
              ret = OB_AI_FUNC_PARAM_INVALID;
              LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, opt_elem.first.length(), opt_elem.first.ptr());
            }
            if (OB_SUCC(ret)) { opt_iter.next(); }
          }
          if (OB_SUCC(ret) && has_min_concurrency && has_max_concurrency && min_concurrency > max_concurrency) {
            ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
            LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, (int)strlen("min_concurrency"), "min_concurrency");
          }
          if (OB_SUCC(ret)) {
            common::ObStringBuffer buf(&allocator);
            if (OB_FAIL(elem.second->print(buf, false))) {
              LOG_WARN("failed to serialize run_config", K(ret));
            } else {
              run_config.assign(buf.ptr());
            }
          }
        }
      } else if (0 == elem.first.case_compare("access_key")) {
        ret = OB_AI_FUNC_PARAM_INVALID;
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, (int)strlen("access_key"), "access_key");
      } else {
        ret = OB_AI_FUNC_PARAM_INVALID;
        LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr());
      }
      if (OB_SUCC(ret)) { iter.next(); }
    }
  }

  // execute SQL: check provider → SELECT existing → INSERT or UPDATE
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, user_tenant_id))) {
      LOG_WARN("failed to start transaction", KR(ret));
    } else {
      // check provider exists via SQL (not schema_guard, which can be stale after DDL)
      ObSqlString check_sql;
      if (OB_FAIL(check_sql.assign_fmt(
          "SELECT count(*) as cnt FROM %s WHERE tenant_id = 0 AND name = ",
          OB_ALL_AI_MODEL_PROVIDER_TNAME))) {
        LOG_WARN("failed to assign check sql", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(provider_name, check_sql))) {
        LOG_WARN("failed to append provider_name", KR(ret), K(provider_name));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          int64_t cnt = 0;
          if (OB_FAIL(trans.read(res, user_tenant_id, check_sql.ptr()))) {
            LOG_WARN("failed to read provider table", K(ret));
          } else if (OB_ISNULL(res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null result", K(ret));
          } else if (OB_FAIL(res.get_result()->next())) {
            LOG_WARN("failed to get next", K(ret));
          } else if (OB_FAIL(res.get_result()->get_int("cnt", cnt))) {
            LOG_WARN("failed to get int", K(ret));
          } else if (cnt == 0) {
            ret = OB_AI_FUNC_MODEL_NOT_FOUND;
            LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, provider_name.length(), provider_name.ptr());
            LOG_WARN("provider not found", K(ret), K(provider_name));
          }
        }
      }

      // SELECT existing → INSERT or UPDATE
      bool exists = false;
      int64_t old_model_profile_id = OB_INVALID_ID;
      ObString old_model_config;
      ObString old_run_config;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObAiServiceProxy::select_ai_model_profile(user_tenant_id, allocator, trans,
          provider_name, model_name, old_model_profile_id, old_model_config, old_run_config, exists))) {
        LOG_WARN("failed to select ai model profile", K(ret), K(provider_name), K(model_name));
      } else if (exists) {
        // Empty JSON object {}: no field to merge; skip UPDATE. Otherwise a no-op UPDATE can
        // return affected_rows=0 while update_ai_model_profile requires exactly one row changed.
        if (has_model_config || has_run_config) {
          ObString update_model_config = has_model_config ? ObString(model_config.length(), model_config.ptr()) : old_model_config;
          ObString update_run_config = has_run_config ? ObString(run_config.length(), run_config.ptr()) : old_run_config;
          const bool config_unchanged = (0 == update_model_config.case_compare(old_model_config));
          const bool run_unchanged = (0 == update_run_config.case_compare(old_run_config));
          if (!config_unchanged || !run_unchanged) {
            if (OB_FAIL(ObAiServiceProxy::update_ai_model_profile(user_tenant_id, trans, old_model_profile_id,
                update_model_config, update_run_config))) {
              LOG_WARN("failed to update ai model profile", K(ret));
            }
          }
        }
      } else {
        // INSERT
        uint64_t new_model_profile_id = OB_INVALID_ID;
        ObMaxIdFetcher fetcher(*GCTX.sql_proxy_);
        if (OB_FAIL(fetcher.fetch_new_max_id(user_tenant_id, OB_MAX_USED_AI_MODEL_PROFILE_ID_TYPE, new_model_profile_id, 0))) {
          LOG_WARN("failed to fetch new ai model profile id", KR(ret), K(user_tenant_id));
        } else {
          ObString insert_model_config = has_model_config ? ObString(model_config.length(), model_config.ptr()) : ObString();
          ObString insert_run_config = has_run_config ? ObString(run_config.length(), run_config.ptr()) : ObString();
          if (OB_FAIL(ObAiServiceProxy::insert_ai_model_profile(user_tenant_id, trans, new_model_profile_id,
              provider_name, model_name, insert_model_config, insert_run_config))) {
            LOG_WARN("failed to insert ai model profile", K(ret));
          }
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", KR(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObAiServiceExecutor::drop_ai_model_profile(const ObString &model_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
  ObString provider_name;
  ObString model_name;

  const char *slash = static_cast<const char *>(MEMCHR(model_str.ptr(), '/', model_str.length()));
  if (OB_ISNULL(slash) || slash == model_str.ptr() || slash == model_str.ptr() + model_str.length() - 1) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, 5, "model");
    LOG_WARN("model string must be in provider/model format", K(ret), K(model_str));
  } else {
    provider_name.assign_ptr(model_str.ptr(), static_cast<int32_t>(slash - model_str.ptr()));
    model_name.assign_ptr(slash + 1, static_cast<int32_t>(model_str.length() - (slash - model_str.ptr()) - 1));
  }

  if (OB_SUCC(ret)) {
    uint64_t user_tenant_id = MTL_ID();
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, user_tenant_id))) {
      LOG_WARN("failed to start transaction", KR(ret));
    } else if (OB_FAIL(ObAiServiceProxy::delete_ai_model_profile(user_tenant_id, trans, provider_name, model_name))) {
      LOG_WARN("failed to delete ai model profile", K(ret), K(provider_name), K(model_name));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}


} // namespace share
} // namespace oceanbase
