/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_dbms_catalog_stats_preferences.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
namespace common
{

// Note: string values (db_name/table_name/pname/valchar/sname) are appended via
// sql_append_hex_escape_str to avoid special-character issues (single quotes, backslashes).
// These macros only keep the numeric/identifier parts; string values are appended by code.
#define FETCH_CATALOG_USER_PREFS_PREFIX                                                                  \
  "SELECT /*+ OPT_PARAM('USE_DEFAULT_OPT_STAT','TRUE') */ valchar FROM %s WHERE tenant_id = %lu and "\
  "catalog_id = %lu and db_name = "

#define UPDATE_CATALOG_USER_PREFS_PREFIX                                                                 \
  "REPLACE INTO %s(tenant_id, catalog_id, db_name, table_name, pname, valnum, valchar, chgtime) "   \
  "VALUES (%lu, %lu, "

#define DELETE_CATALOG_USER_PREFS_PREFIX                                                                 \
  "DELETE FROM %s WHERE tenant_id = %lu and catalog_id = %lu and db_name = "

#define FETCH_CATALOG_GLOBAL_PREFS_PREFIX                                                                \
  "SELECT /*+ OPT_PARAM('USE_DEFAULT_OPT_STAT','TRUE') */ spare4 FROM %s WHERE sname = upper("

#define UPDATE_CATALOG_GLOBAL_PREFS_PREFIX                                                               \
  "REPLACE INTO %s(sname, sval1, sval2, spare4) VALUES (upper("

#define DELETE_CATALOG_GLOBAL_PREFS_PREFIX                                                               \
  "DELETE FROM %s WHERE sname = upper("

int ObDbmsCatalogStatsPreferences::do_get_prefs(ObMySQLProxy *mysql_proxy,
                                                ObIAllocator &allocator,
                                                const uint64_t tenant_id,
                                                const ObSqlString &raw_sql,
                                                bool &get_result,
                                                ObObj &result)
{
  int ret = OB_SUCCESS;
  get_result = false;
  if (OB_ISNULL(mysql_proxy) || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), KP(mysql_proxy), K(raw_sql.empty()));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        bool is_first = true;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObObj tmp;
          int64_t idx = 0;
          if (OB_FAIL(client_result->get_obj(idx, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (is_first) {
            is_first = false;
            if (OB_FAIL(ob_write_obj(allocator, tmp, result))) {
              LOG_WARN("failed to write object", K(ret));
            } else {
              get_result = true;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("duplicate catalog prefs rows found", K(ret), K(raw_sql));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::get_prefs(ObMySQLProxy *mysql_proxy,
                                             ObIAllocator &allocator,
                                             const ObCatalogTableIdentity &table_identity,
                                             const ObString &opt_name,
                                             ObObj &result)
{
  int ret = OB_SUCCESS;
  ObSqlString get_catalog_user_sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(table_identity.tenant_id_)
                  || OB_INVALID_ID == table_identity.catalog_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog table identity", K(ret), K(table_identity));
  } else {
    uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(table_identity.tenant_id_);
    if (OB_FAIL(get_catalog_user_sql.append_fmt(FETCH_CATALOG_USER_PREFS_PREFIX,
                                                share::OB_ALL_OPTSTAT_CATALOG_USER_PREFS_TNAME,
                                                share::schema::ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, table_identity.tenant_id_),
                                                table_identity.catalog_id_))) {
      LOG_WARN("failed to append catalog prefs sql", K(ret), K(get_catalog_user_sql));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.db_name_, get_catalog_user_sql))) {
      LOG_WARN("failed to append db name", K(ret), K(table_identity.db_name_));
    } else if (OB_FAIL(get_catalog_user_sql.append(" and table_name = "))) {
      LOG_WARN("failed to append table name", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.tab_name_, get_catalog_user_sql))) {
      LOG_WARN("failed to append table name", K(ret), K(table_identity.tab_name_));
    } else if (OB_FAIL(get_catalog_user_sql.append(" and pname = upper("))) {
      LOG_WARN("failed to append pname", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, get_catalog_user_sql))) {
      LOG_WARN("failed to append opt name", K(ret), K(opt_name));
    } else if (OB_FAIL(get_catalog_user_sql.append(")"))) {
      LOG_WARN("failed to append close paren", K(ret));
    } else {
      bool got_result = false;
      if (OB_FAIL(do_get_prefs(mysql_proxy,
                               allocator,
                               table_identity.tenant_id_,
                               get_catalog_user_sql,
                               got_result,
                               result))) {
        LOG_WARN("failed to do get catalog user prefs", K(ret));
      } else if (got_result) {
      } else if (OB_FAIL(get_global_prefs(mysql_proxy,
                                          allocator,
                                          table_identity.tenant_id_,
                                          opt_name,
                                          result))) {
        LOG_WARN("failed to get catalog global prefs", K(ret), K(table_identity), K(opt_name));
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::set_prefs(ObExecContext &ctx,
                                             const ObCatalogTableIdentity &table_identity,
                                             const ObString &opt_name,
                                             const ObString &opt_value)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSqlString raw_sql;
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(mysql_proxy), KP(session));
  } else {
    uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(table_identity.tenant_id_);
    if (OB_FAIL(raw_sql.append_fmt(
                 UPDATE_CATALOG_USER_PREFS_PREFIX,
                 share::OB_ALL_OPTSTAT_CATALOG_USER_PREFS_TNAME,
                 share::schema::ObSchemaUtils::get_extract_tenant_id(
                     exec_tenant_id, table_identity.tenant_id_),
                 table_identity.catalog_id_))) {
      LOG_WARN("failed to append sql", K(ret), K(raw_sql));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.db_name_, raw_sql))) {
      LOG_WARN("failed to append db name", K(ret), K(table_identity.db_name_));
    } else if (OB_FAIL(raw_sql.append(", "))) {
      LOG_WARN("failed to append comma", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.tab_name_, raw_sql))) {
      LOG_WARN("failed to append table name", K(ret), K(table_identity.tab_name_));
    } else if (OB_FAIL(raw_sql.append(", upper("))) {
      LOG_WARN("failed to append upper", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, raw_sql))) {
      LOG_WARN("failed to append opt name", K(ret), K(opt_name));
    } else if (OB_FAIL(raw_sql.append("), NULL, "))) {
      LOG_WARN("failed to append separator", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(opt_value, raw_sql))) {
      LOG_WARN("failed to append opt value", K(ret), K(opt_value));
    } else if (OB_FAIL(raw_sql.append_fmt(", usec_to_time(%ld));", current_time))) {
      LOG_WARN("failed to append time", K(ret));
    } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                          raw_sql.ptr(),
                                          affected_rows))) {
      LOG_WARN("fail to exec sql", K(ret), K(raw_sql));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::delete_prefs(ObExecContext &ctx,
                                                const ObCatalogTableIdentity &table_identity,
                                                const ObString &opt_name)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSqlString raw_sql;
  ObSqlString condition_str;
  int64_t affected_rows = 0;
  uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(table_identity.tenant_id_);
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(mysql_proxy), KP(session));
  } else if (opt_name.empty()) {
    // delete all catalog table prefs
  } else if (OB_FAIL(condition_str.append(" and pname = upper("))) {
    LOG_WARN("failed to append condition prefix", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, condition_str))) {
    LOG_WARN("failed to append opt name", K(ret), K(opt_name));
  } else if (OB_FAIL(condition_str.append(")"))) {
    LOG_WARN("failed to append condition suffix", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(raw_sql.append_fmt(
                 DELETE_CATALOG_USER_PREFS_PREFIX,
                 share::OB_ALL_OPTSTAT_CATALOG_USER_PREFS_TNAME,
                 share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                     table_identity.tenant_id_),
                 table_identity.catalog_id_))) {
    LOG_WARN("failed to append delete sql", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.db_name_, raw_sql))) {
    LOG_WARN("failed to append db name", K(ret), K(table_identity.db_name_));
  } else if (OB_FAIL(raw_sql.append(" and table_name = "))) {
    LOG_WARN("failed to append table name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(table_identity.tab_name_, raw_sql))) {
    LOG_WARN("failed to append table name", K(ret), K(table_identity.tab_name_));
  } else if (!condition_str.empty() && OB_FAIL(raw_sql.append(condition_str.string()))) {
    LOG_WARN("failed to append condition", K(ret), K(condition_str));
  } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                        raw_sql.ptr(),
                                        affected_rows))) {
    LOG_WARN("failed to execute delete sql", K(ret), K(raw_sql));
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::get_global_prefs(ObMySQLProxy *mysql_proxy,
                                                    ObIAllocator &allocator,
                                                    const uint64_t tenant_id,
                                                    const ObString &opt_name,
                                                    ObObj &result)
{
  int ret = OB_SUCCESS;
  ObSqlString get_catalog_global_sql;
  bool got_result = false;
  if (OB_UNLIKELY(opt_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog global prefs name", K(ret), K(opt_name));
  } else if (OB_FAIL(get_catalog_global_sql.append_fmt(FETCH_CATALOG_GLOBAL_PREFS_PREFIX,
                                                       share::OB_ALL_OPTSTAT_CATALOG_GLOBAL_PREFS_TNAME))) {
    LOG_WARN("failed to append catalog global prefs sql", K(ret), K(get_catalog_global_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, get_catalog_global_sql))) {
    LOG_WARN("failed to append opt name", K(ret), K(opt_name));
  } else if (OB_FAIL(get_catalog_global_sql.append(")"))) {
    LOG_WARN("failed to append close paren", K(ret));
  } else if (OB_FAIL(do_get_prefs(mysql_proxy,
                                  allocator,
                                  tenant_id,
                                  get_catalog_global_sql,
                                  got_result,
                                  result))) {
    LOG_WARN("failed to get catalog global prefs", K(ret));
  } else if (!got_result) {
    result.set_null();
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::set_global_prefs(ObExecContext &ctx,
                                                    const ObString &opt_name,
                                                    const ObString &opt_value)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSqlString raw_sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(mysql_proxy), KP(session));
  } else if (OB_UNLIKELY(opt_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog global prefs name", K(ret), K(opt_name));
  } else if (OB_FAIL(raw_sql.append_fmt(UPDATE_CATALOG_GLOBAL_PREFS_PREFIX,
                                        share::OB_ALL_OPTSTAT_CATALOG_GLOBAL_PREFS_TNAME))) {
    LOG_WARN("failed to append sql", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, raw_sql))) {
    LOG_WARN("failed to append opt name", K(ret), K(opt_name));
  } else if (OB_FAIL(raw_sql.append("), NULL, CURRENT_TIMESTAMP, "))) {
    LOG_WARN("failed to append middle", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(opt_value, raw_sql))) {
    LOG_WARN("failed to append opt value", K(ret), K(opt_value));
  } else if (OB_FAIL(raw_sql.append(");"))) {
    LOG_WARN("failed to append tail", K(ret));
  } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                        raw_sql.ptr(),
                                        affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
  }
  return ret;
}

int ObDbmsCatalogStatsPreferences::delete_global_prefs(ObExecContext &ctx,
                                                       const ObString &opt_name)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSqlString raw_sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(mysql_proxy), KP(session));
  } else if (OB_UNLIKELY(opt_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog global prefs name", K(ret), K(opt_name));
  } else if (OB_FAIL(raw_sql.append_fmt(DELETE_CATALOG_GLOBAL_PREFS_PREFIX,
                                        share::OB_ALL_OPTSTAT_CATALOG_GLOBAL_PREFS_TNAME))) {
    LOG_WARN("failed to append delete sql", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(opt_name, raw_sql))) {
    LOG_WARN("failed to append opt name", K(ret), K(opt_name));
  } else if (OB_FAIL(raw_sql.append(")"))) {
    LOG_WARN("failed to append close paren", K(ret));
  } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                        raw_sql.ptr(),
                                        affected_rows))) {
    LOG_WARN("failed to execute delete sql", K(ret), K(raw_sql));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
