/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ai_service/ob_ai_provider_importer.h"
#include "share/ai_service/ob_ai_builtin_provider.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_utility.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/ob_sql_context.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
using namespace share;
using namespace sql;
using namespace common;
namespace table
{

int ObAiProviderImporter::exec_op(ObModuleDataArg::ObInfoOpType op)
{
  int ret = OB_SUCCESS;
  switch (op) {
    case ObModuleDataArg::LOAD_INFO: {
      bool need_import = false;
      if (OB_FAIL(check_basic_info(need_import))) {
        LOG_WARN("fail to check basic info", K(ret));
      } else if (need_import && OB_FAIL(import_ai_provider_info())) {
        LOG_WARN("fail to import ai provider info", K(ret));
      }
      break;
    }
    case ObModuleDataArg::CHECK_INFO: {
      if (OB_FAIL(check_ai_provider_info())) {
        LOG_WARN("fail to check ai provider info", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unknown op type", K(ret), K(op));
      break;
    }
  }
  return ret;
}

int ObAiProviderImporter::check_basic_info(bool &need_import)
{
  int ret = OB_SUCCESS;
  need_import = true;
  if (OB_ISNULL(exec_ctx_.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  }
  return ret;
}

int ObAiProviderImporter::import_ai_provider_info()
{
  int ret = OB_SUCCESS;
  ObCommonSqlProxy *sql_proxy = exec_ctx_.get_sql_proxy();
  ObSQLSessionInfo *session = exec_ctx_.get_my_session();
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or session is null", K(ret));
  } else {
    ObSessionParam session_param;
    ObSQLMode sess_sql_mode = session->get_sql_mode();
    session_param.sql_mode_ = reinterpret_cast<int64_t *>(&sess_sql_mode);
    ObSqlString sql_str;
    ObMemAttr attr(MTL_ID(), "AiProvImpt");
    sql_str.set_attr(attr);
    int64_t affected_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < BUILTIN_PROVIDER_COUNT; ++i) {
      const ObAIBuiltinProvider &provider = BUILTIN_PROVIDERS[i];
      sql_str.reset();
      if (OB_FAIL(sql_str.assign_fmt(
              "CALL DBMS_AI_SERVICE.REGISTER_PROVIDER('%s', "
              "'{\"protocol\":\"%s\",\"base_url\":\"%s\",\"access_key\":\"sk-xxxx\"}')",
              provider.name_, provider.protocol_, provider.default_base_url_))) {
        LOG_WARN("failed to build register provider sql", K(ret));
      } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql_str.ptr(), affected_rows,
                                           ObCompatibilityMode::MYSQL_MODE, &session_param))) {
        LOG_WARN("failed to register builtin provider", K(ret), K(sql_str));
      } else {
        ++affected_rows_;
      }
    }
  }
  LOG_INFO("import ai provider info done", K(ret), K(tenant_id_), K(affected_rows_));
  return ret;
}

int ObAiProviderImporter::check_ai_provider_info()
{
  int ret = OB_SUCCESS;
  ObCommonSqlProxy *sql_proxy = exec_ctx_.get_sql_proxy();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObSqlString sql;
    ObMemAttr attr(MTL_ID(), "AiProvImpt");
    sql.set_attr(attr);
    if (OB_FAIL(sql.assign_fmt(
            "SELECT count(DISTINCT p.name) as cnt FROM %s p WHERE p.tenant_id = 0 AND p.name IN (",
            OB_ALL_AI_MODEL_PROVIDER_TNAME))) {
      LOG_WARN("failed to set sql", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < BUILTIN_PROVIDER_COUNT; ++i) {
      const ObString provider_name = ObString::make_string(BUILTIN_PROVIDERS[i].name_);
      if (i > 0 && OB_FAIL(sql.append(", "))) {
        LOG_WARN("failed to append separator", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(provider_name, sql))) {
        LOG_WARN("failed to append provider name", K(ret), K(provider_name));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(
            ") AND EXISTS (SELECT 1 FROM %s h WHERE h.tenant_id = 0 "
            "AND h.provider_id = p.provider_id AND h.is_deleted = 0)",
            OB_ALL_AI_MODEL_PROVIDER_HISTORY_TNAME))) {
      LOG_WARN("failed to append history check", K(ret));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("failed to read", K(ret));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get result", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get next", K(ret));
        } else {
          int64_t cnt = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
          if (cnt < BUILTIN_PROVIDER_COUNT) {
            ret = OB_PARTIAL_FAILED;
            LOG_WARN("ai provider data incomplete, need re-import",
                K(ret), K(tenant_id_), K(cnt), K(BUILTIN_PROVIDER_COUNT));
            LOG_USER_ERROR(OB_PARTIAL_FAILED, "ai provider data incomplete, please execute LOAD MODULE DATA AI_PROVIDER again");
          }
        }
      }
    }
  }
  return ret;
}

} // namespace table
} // namespace oceanbase
