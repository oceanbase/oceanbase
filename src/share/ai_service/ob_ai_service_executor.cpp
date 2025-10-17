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

#include "share/ai_service/ob_ai_service_executor.h"
#include "share/ai_service/ob_ai_service_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "share/ob_max_id_fetcher.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "share/restore/ob_import_util.h"

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
  if (OB_FAIL(endpoint.parse_from_json_base(allocator, endpoint_name, create_jbase))) {
    LOG_WARN("failed to parse ai service endpoint info", KR(ret), K(create_jbase));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(ObAiServiceProxy::check_ai_endpoint_exists(tenant_id, allocator, trans, endpoint_name, is_exists))) {
    LOG_WARN("failed to check ai endpoint exists", KR(ret), K(endpoint_name));
  } else if (is_exists) {
    ret = OB_AI_FUNC_ENDPOINT_EXISTS;
    LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_EXISTS, endpoint_name.length(), endpoint_name.ptr());
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
  uint64_t tenant_id = gen_meta_tenant_id(MTL_ID());
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


} // namespace share
} // namespace oceanbase
