/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/ob_dbms_stats_preferences.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "sql/parser/ob_parser.h"

namespace oceanbase {
using namespace sql;
namespace common {

#define FETCH_GLOBAL_PREFS "SELECT /*+ OPT_PARAM(\'USE_DEFAULT_OPT_STAT\',\'TRUE\') */ spare4 FROM %s WHERE sname = upper('%.*s')"

#define FETCH_USER_PREFS "SELECT /*+ OPT_PARAM(\'USE_DEFAULT_OPT_STAT\',\'TRUE\') */ valchar FROM %s WHERE tenant_id = %lu and \
                          table_id = %lu and pname = upper('%.*s')"

#define UPDATE_GLOBAL_PREFS "UPDATE %s SET spare4 = upper('%.*s'), \
                             sval2 = usec_to_time('%ld') WHERE sname = upper('%.*s')"

#define UPDATE_USER_PREFS "REPLACE INTO %s(tenant_id,\
                                           table_id,\
                                           pname,\
                                           valnum,\
                                           valchar,\
                                           chgtime) VALUES"

#define DELETE_USER_PREFS "DELETE FROM %s WHERE table_id in %s %.*s"

#define INIT_GLOBAL_PREFS "REPLACE INTO %s(sname, sval1, sval2, spare4) VALUES %s;"

int ObDbmsStatsPreferences::reset_global_pref_defaults(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session));
  } else if (OB_FAIL(gen_init_global_prefs_sql(raw_sql, true))) {
    LOG_WARN("failed gen init global prefs sql", K(ret), K(raw_sql));
  } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                        raw_sql.ptr(),
                                        affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to reset global pref defaults", K(raw_sql));
  }
  return ret;
}

int ObDbmsStatsPreferences::get_prefs(ObExecContext &ctx,
                                      const ObTableStatParam &param,
                                      const ObString &opt_name,
                                      ObObj &result)
{
  int ret = OB_SUCCESS;
  ObSqlString get_user_sql;
  ObSqlString get_global_sql;
  bool is_user_prefs = (param.table_id_ != OB_INVALID_ID);
  if (OB_FAIL(get_global_sql.append_fmt(FETCH_GLOBAL_PREFS,
                                        share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
                                        opt_name.length(),
                                        opt_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(get_global_sql));
  } else if (is_user_prefs) {
    uint64_t tenant_id = param.tenant_id_;
    uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(get_user_sql.append_fmt(FETCH_USER_PREFS,
                                        share::OB_ALL_OPTSTAT_USER_PREFS_TNAME,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                        share::schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, param.table_id_),
                                        opt_name.length(),
                                        opt_name.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret), K(get_user_sql));
    }
  } else {/*do nothing*/}
  if (OB_SUCC(ret)) {
    bool got_result = false;
    if (is_user_prefs && OB_FAIL(do_get_prefs(ctx, param.allocator_, get_user_sql, got_result, result))) {
      LOG_WARN("failed to do get prefs", K(ret));
    } else if (got_result) {
      /*do nothing*/
    } else if OB_FAIL(do_get_prefs(ctx, param.allocator_, get_global_sql, got_result, result)) {
      LOG_WARN("failed to do get prefs", K(ret));
    } else if (got_result) {
      /*do nothing*/
    } else {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Invalid input values for pname", K(ret), K(opt_name));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
    }
    LOG_TRACE("Succeed to get prefs", K(ret), K(get_user_sql), K(get_global_sql), K(result));
  }
  return ret;
}

int ObDbmsStatsPreferences::set_prefs(ObExecContext &ctx,
                                      const ObIArray<uint64_t> &table_ids,
                                      const ObString &opt_name,
                                      const ObString &opt_value)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session));
  } else if (!table_ids.empty()) {//update user prefs
    if (OB_FAIL(raw_sql.append_fmt(UPDATE_USER_PREFS, share::OB_ALL_OPTSTAT_USER_PREFS_TNAME))) {
      LOG_WARN("failed to append", K(ret), K(raw_sql));
    } else {
      ObSqlString val_sql;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        val_sql.reset();
        if (OB_FAIL(get_user_prefs_sql(session->get_effective_tenant_id(),
                                       table_ids.at(i), opt_name, opt_value,
                                       current_time, val_sql))) {
          LOG_WARN("failed to get user prefs sql", K(ret), K(val_sql));
        } else if (OB_FAIL(raw_sql.append_fmt("(%s)%c", val_sql.ptr(),
                                                       (i == table_ids.count() - 1 ? ';' : ',')))) {
          LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                       raw_sql.ptr(),
                                       affected_rows))) {
          LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
        } else {
          LOG_TRACE("Succeed to set table prefs", K(raw_sql));
        }
      }
    }
  } else {//update global prefs
    if (OB_FAIL(raw_sql.append_fmt(UPDATE_GLOBAL_PREFS,
                                   share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
                                   opt_value.length(),
                                   opt_value.ptr(),
                                   current_time,
                                   opt_name.length(),
                                   opt_name.ptr()))) {
      LOG_WARN("failed to append", K(ret), K(raw_sql));
    } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                          raw_sql.ptr(),
                                          affected_rows))) {
      LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
    } else {
      LOG_TRACE("Succeed to set table prefs", K(raw_sql), K(affected_rows));
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::delete_user_prefs(ObExecContext &ctx,
                                              const ObIArray<uint64_t> &table_ids,
                                              const ObString &opt_name)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  int64_t affected_rows = 0;
  ObString dummy_str;
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session));
  } else if (!table_ids.empty()) {
    ObSqlString tbl_list_str;
    uint64_t tenant_id = session->get_effective_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_ids.at(i));
      char prefix = (i == 0 ? '(' : ' ');
      char suffix = (i == table_ids.count() - 1 ? ')' : ',');
      if (OB_FAIL(tbl_list_str.append_fmt("%c%lu%c", prefix, pure_table_id, suffix))) {
        LOG_WARN("failed to append sql", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      ObSqlString condition_str;
      if (opt_name.length() != 0 && OB_FAIL(condition_str.append_fmt("and pname = upper('%.*s')", opt_name.length(), opt_name.ptr()))) {
        LOG_WARN("fail to append sql", K(ret));
      } else if (OB_FAIL(raw_sql.append_fmt(DELETE_USER_PREFS,
                                            share::OB_ALL_OPTSTAT_USER_PREFS_TNAME,
                                            tbl_list_str.ptr(),
                                            condition_str.string().length(),
                                            condition_str.string().ptr()))) {
        LOG_WARN("failed to append", K(ret), K(raw_sql));
      } else if (OB_FAIL(mysql_proxy->write(tenant_id,
                                            raw_sql.ptr(),
                                            affected_rows))) {
        LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
      } else {
        LOG_TRACE("Succeed to delete user prefs", K(raw_sql), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::do_get_prefs(ObExecContext &ctx,
                                         ObIAllocator *allocator,
                                         const ObSqlString &raw_sql,
                                         bool &get_result,
                                         ObObj &result)
{
  int ret = OB_SUCCESS;
  get_result = false;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session) ||
      OB_ISNULL(allocator) || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session),
                                     K(allocator), K(raw_sql.empty()));
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
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
          if (!is_first) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(result), K(raw_sql));
          } else if (OB_FAIL(client_result->get_obj(idx, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(ob_write_obj(*allocator, tmp, result))) {
            LOG_WARN("failed to write object", K(ret));
          } else {
            is_first = false;
            get_result = true;
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
          LOG_TRACE("Succeed to get stats history info", K(result), K(raw_sql));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::get_user_prefs_sql(const uint64_t tenant_id,
                                               const uint64_t table_id,
                                               const ObString &opt_name,
                                               const ObString &opt_value,
                                               const int64_t current_time,
                                               ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("pname", opt_name)) ||
      OB_FAIL(dml_splicer.add_column("valnum", NULL)) ||
      OB_FAIL(dml_splicer.add_column("valchar", opt_value)) ||
      OB_FAIL(dml_splicer.add_time_column("last_analyzed", current_time))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObDbmsStatsPreferences::get_sys_default_stat_options(ObExecContext &ctx,
                                                         ObIArray<ObStatPrefs*> &stat_prefs,
                                                         ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString sname_list;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
  ObSEArray<ObStatPrefs*, 4> no_acquired_prefs;
  if (stat_prefs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(gen_sname_list_str(stat_prefs, sname_list))) {
    LOG_WARN("failed to gen sname list str", K(ret), K(sname_list));
  } else if (OB_FAIL(raw_sql.append_fmt("SELECT pname, valchar FROM %s WHERE"\
                                        " tenant_id = %lu and table_id = %lu and pname in %s",
                                        share::OB_ALL_OPTSTAT_USER_PREFS_TNAME,
                                        ext_tenant_id,
                                        pure_table_id,
                                        sname_list.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(do_get_sys_perfs(ctx, raw_sql, stat_prefs, param))) {
    LOG_WARN("failed to do get sys perfs", K(ret));
  } else if (OB_FAIL(get_no_acquired_prefs(stat_prefs, no_acquired_prefs))) {
    LOG_WARN("failed to get no acquired prefs", K(ret));
  } else if (no_acquired_prefs.empty()) {//have got all expected sys prefs from user prefs sys table
    LOG_TRACE("succeed to get sys default stat options", K(stat_prefs), K(param));
  } else {//try get sys prefs from global prefs sys table
    raw_sql.reset();
    sname_list.reset();
    if (OB_FAIL(gen_sname_list_str(no_acquired_prefs, sname_list))) {
      LOG_WARN("failed to gen sname list str", K(ret), K(sname_list));
    } else if (OB_FAIL(raw_sql.append_fmt("SELECT sname, spare4 FROM %s WHERE sname in %s",
                                          share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
                                          sname_list.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
    } else if (OB_FAIL(do_get_sys_perfs(ctx, raw_sql, no_acquired_prefs, param))) {
      LOG_WARN("failed to do get sys perfs", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::gen_init_global_prefs_sql(ObSqlString &raw_sql,
                                                      bool is_reset_prefs/*default false*/,
                                                      int64_t *expect_affected_rows/*default NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString value_str;
  int64_t total_rows = 0;
  const char *stats_retention = "STATS_RETENTION";
  const char *null_str = "NULL";
  const char *time_str = "CURRENT_TIMESTAMP";
  if (!is_reset_prefs) {//init histogram stats retention
    if (OB_FAIL(value_str.append_fmt("('%s', '%ld', %s, %s), ",
                                     stats_retention,
                                     OPT_DEFAULT_STATS_RETENTION,
                                     time_str,
                                     null_str))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init cascade
    ObCascadePrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init degree
    ObDegreePrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_NOT_NULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, %s), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            null_str))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init esimate_percent
    ObEstimatePercentPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init incremental
    ObIncrementalPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                        prefs.get_stat_pref_name(),
                                        null_str,
                                        time_str,
                                        prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init incremental_level
    ObIncrementalLevelPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init granularity
    ObGranularityPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init method_opt
    ObMethodOptPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init no_invalidate
    ObNoInvalidatePrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init options
    ObOptionsPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init stale_percent
    ObStalePercentPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'), ",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init approximate_ndv
    ObApproximateNdvPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s'),",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {//init estimate_block
    ObEstimateBlockPrefs prefs;
    if (OB_ISNULL(prefs.get_stat_pref_name()) || OB_ISNULL(prefs.get_stat_pref_default_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(prefs.get_stat_pref_name()),
                                       K(prefs.get_stat_pref_default_value()));
    } else if (OB_FAIL(value_str.append_fmt("('%s', %s, %s, '%s');",
                                            prefs.get_stat_pref_name(),
                                            null_str,
                                            time_str,
                                            prefs.get_stat_pref_default_value()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      ++ total_rows;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(raw_sql.append_fmt(INIT_GLOBAL_PREFS,
                                   share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
                                   value_str.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else {
      LOG_TRACE("Succeed gen init global prefs sql", K(raw_sql));
      if (expect_affected_rows != NULL) {
        *expect_affected_rows = total_rows;
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::gen_sname_list_str(ObIArray<ObStatPrefs*> &stat_prefs,
                                               ObSqlString &sname_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stat_prefs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(stat_prefs), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stat_prefs.count(); ++i) {
      if (OB_ISNULL(stat_prefs.at(i)) ||
          OB_ISNULL(stat_prefs.at(i)->get_stat_pref_name())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(stat_prefs.at(i)));
      } else {
        char prefix = (i == 0 ? '(' : ' ');
        char suffix = (i == stat_prefs.count() - 1 ? ')' : ',');
        if (OB_FAIL(sname_list.append_fmt("%c'%s'%c", prefix,
                                          stat_prefs.at(i)->get_stat_pref_name(), suffix))) {
          LOG_WARN("failed to append sql", K(ret), K(sname_list));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::do_get_sys_perfs(ObExecContext &ctx,
                                             const ObSqlString &raw_sql,
                                             ObIArray<ObStatPrefs*> &need_acquired_prefs,
                                             ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session) || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session), K(raw_sql.empty()));
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          if (OB_FAIL(decode_perfs_result(param.allocator_, *client_result,
                                          need_acquired_prefs, param))) {
            LOG_WARN("failed to decode perfs result", K(ret));
          } else {/*do nothing*/}
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
          LOG_TRACE("Succeed to do get sys perfs", K(raw_sql), K(need_acquired_prefs));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::decode_perfs_result(ObIAllocator *allocator,
                                                sqlclient::ObMySQLResult &client_result,
                                                ObIArray<ObStatPrefs*> &need_acquired_prefs,
                                                ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObObj name_obj;
  ObObj val_obj;
  int64_t name_id = 0;
  int64_t val_id = 1;
  ObString pname;
  ObString pvalue;
  if (OB_FAIL(client_result.get_obj(name_id, name_obj))) {
    LOG_WARN("failed to get object", K(ret));
  } else if (OB_FAIL(client_result.get_obj(val_id, val_obj))) {
    LOG_WARN("failed to get object", K(ret));
  } else {
    bool is_decoded = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_decoded && i < need_acquired_prefs.count(); ++i) {
      if (OB_ISNULL(need_acquired_prefs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(need_acquired_prefs.at(i)));
      } else if (need_acquired_prefs.at(i)->is_decoded()) {
        /*do nothing*/
      } else if (OB_FAIL(need_acquired_prefs.at(i)->decode_pref_result(allocator,
                                                                       name_obj,
                                                                       val_obj,
                                                                       param))) {
        LOG_WARN("failed to decode pref result", K(ret));
      } else {
        is_decoded = need_acquired_prefs.at(i)->is_decoded();
      }
    }
  }
  return ret;
}

int ObDbmsStatsPreferences::get_no_acquired_prefs(ObIArray<ObStatPrefs*> &stat_prefs,
                                                  ObIArray<ObStatPrefs*> &no_acquired_prefs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_prefs.count(); ++i) {
    if (OB_ISNULL(stat_prefs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(stat_prefs.at(i)));
    } else if (stat_prefs.at(i)->is_decoded()) {
      /*do nothing*/
    } else if (OB_FAIL(no_acquired_prefs.push_back(stat_prefs.at(i)))) {
      LOG_WARN("failed to decode pref result", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObStatPrefs::dump_pref_name_and_value(ObString &pref_name, ObString &pvalue)
{
  int ret = OB_SUCCESS;
  const char *str_name = get_stat_pref_name();
  if (OB_ISNULL(str_name) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(str_name), K(allocator_));
  } else {
    char *buf_name = NULL;
    int64_t buf_name_len = strlen(str_name);
    if (OB_ISNULL(buf_name = static_cast<char*>(allocator_->alloc(buf_name_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf_name), K(buf_name_len));
    } else {
      MEMCPY(buf_name, str_name, buf_name_len);
      pref_name.assign_ptr(buf_name, buf_name_len);
    }
    if (pvalue_.empty() && OB_NOT_NULL(get_stat_pref_default_value())) {
      const char *str_value = get_stat_pref_default_value();
      char *buf_value = NULL;
      int64_t buf_value_len = strlen(str_value);
      if (OB_ISNULL(buf_value = static_cast<char*>(allocator_->alloc(buf_value_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf_value), K(buf_value_len));
      } else {
        MEMCPY(buf_value, str_value, buf_value_len);
        pvalue.assign_ptr(buf_value, buf_value_len);
      }
    } else {
      pvalue = pvalue_;
    }
  }
  return ret;
}

int ObStatPrefs::decode_pref_result(ObIAllocator *allocator,
                                    const ObObj &name_obj,
                                    const ObObj &val_obj,
                                    ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObString name_str;
  ObString val_str;
  if (OB_FAIL(name_obj.get_string(name_str))) {
    LOG_WARN("failed to get string", K(ret));
  } else if (!val_obj.is_null() && (val_obj.get_string(val_str))) {
    LOG_WARN("failed to get string", K(ret));
  } else if (OB_ISNULL(get_stat_pref_name())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stat_pref_name()));
  } else if (0 == name_str.case_compare(get_stat_pref_name())) {
    pvalue_ = val_str;
    allocator_ = allocator;
    if (OB_FAIL(check_pref_value_validity(&param))) {
      LOG_WARN("failed to check pref value validity");
    } else {
      is_decoded_ = true;
    }
  } else {/*do nothing*/}
  return ret;
}

int ObCascadePrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (0 == pvalue_.case_compare("FALSE")) {
    if (param != NULL) {
      param->cascade_ = false;
    }
  } else if (pvalue_.empty() ||
             0 == pvalue_.case_compare("TRUE") ||
             0 == pvalue_.case_compare("DBMS_STATS.AUTO_CASCADE")) {
    if (param != NULL) {
      param->cascade_ = true;
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for CASCADE", K(ret), K(pvalue_), K(param));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal value for CASCADE : must be "\
                                         "{TRUE, FALSE, DBMS_STATS.AUTO_CASCADE}");
  }
  return ret;
}

int ObDegreePrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (!pvalue_.empty()) {
    ObObj src_obj;
    ObObj dest_obj;
    src_obj.set_string(ObVarcharType, pvalue_);
    ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
    ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
    number::ObNumber num_degree;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, src_obj, dest_obj))) {
      LOG_WARN("failed to type", K(ret), K(src_obj));
    } else if (OB_ISNULL(param)) {
    } else if (OB_FAIL(dest_obj.get_number(num_degree))) {
      LOG_WARN("failed to get degree", K(ret));
    } else if (OB_FAIL(num_degree.extract_valid_int64_with_trunc(param->degree_))) {
      LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_degree));
    } else {/*do noting*/}
    if (OB_FAIL(ret)) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Illegal degree", K(ret), K(pvalue_));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal degree");
    }
  } else if (param != NULL) {
    param->degree_ = 1;
  }
  return ret;
}

int ObEstimatePercentPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (!pvalue_.empty()) {
    if (0 == pvalue_.case_compare("DBMS_STATS.AUTO_SAMPLE_SIZE")) {
      /*do nothing*/
    } else {
      ObObj src_obj;
      ObObj dest_obj;
      src_obj.set_string(ObVarcharType, pvalue_);
      ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
      ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
      double dst_val = 0.0;
      if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, src_obj, dest_obj))) {
        LOG_WARN("failed to type", K(ret), K(src_obj));
      } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(dest_obj.get_number(), dst_val))) {
        LOG_WARN("failed to cast number to double", K(ret), K(src_obj));
      } else if (dst_val < 0.000001 || dst_val > 100.0) {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("Illegal value for estimate percent", K(ret), K(dst_val));
      } else if (param != NULL) {
        param->sample_info_.set_percent(dst_val);
      } else {/*do nothing*/}
      if (OB_FAIL(ret)) {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal sample percent: must be in the range [0.000001,100]");
      }
    }
  }
  return ret;
}

int ObGranularityPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  const char *default_value = get_stat_pref_default_value();
  if (pvalue_.empty()) {
    pvalue_.assign_ptr(default_value, strlen(default_value));
  }
  if (param != NULL) {//no need check
    char *buf = NULL;
    int64_t buf_len = pvalue_.length();
    if (OB_ISNULL(allocator_) ||
        OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(allocator_), K(buf), K(buf_len));
    } else {
      MEMCPY(buf, pvalue_.ptr(), buf_len);
      param->granularity_.assign_ptr(buf, buf_len);
    }
  } else {
    ObGranularityType dummy_type = ObGranularityType::GRANULARITY_INVALID;
    if (OB_FAIL(ObDbmsStatsUtils::parse_granularity(pvalue_, dummy_type))) {
      LOG_WARN("failed to parse granularity", K(ret), K(pvalue_));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObIncrementalPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (pvalue_.empty()) {
  } else if (0 == pvalue_.case_compare("TRUE")) {
    if (param != NULL) {
      //not implement
    }
  } else if (0 == pvalue_.case_compare("FALSE")) {
    if (param != NULL) {
      //not implement
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for INCREMENTAL", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal value for INCREMENTAL : must be {TRUE, FALSE}");
  }
  return ret;
}

int ObIncrementalLevelPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (pvalue_.empty()) {
  } else if (0 == pvalue_.case_compare("TABLE")) {
    if (param != NULL) {
      //not implement
    }
  } else if (0 == pvalue_.case_compare("PARTITION")) {
    if (param != NULL) {
      //not implement
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for INCREMENTAL_LEVEL", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal value for INCREMENTAL_LEVEL : must be {TABLE, PARTITION}");
  }
  return ret;
}

int ObMethodOptPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  const char *default_value = get_stat_pref_default_value();
  if (pvalue_.empty()) {
    pvalue_.assign_ptr(default_value, strlen(default_value));
  }
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info_), K(allocator_));
  } else if (param != NULL) {//no need check
    char *buf = NULL;
    int64_t buf_len = pvalue_.length();
    if (OB_ISNULL(allocator_) ||
        OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(allocator_), K(buf), K(buf_len));
    } else {
      MEMCPY(buf, pvalue_.ptr(), buf_len);
      param->method_opt_.assign_ptr(buf, buf_len);
    }
  } else if (is_global_prefs() && OB_FAIL(check_global_method_opt_prefs_value_validity(pvalue_))) {
    LOG_WARN("failed to check method opt value validity", K(ret), K(pvalue_));
  } else {
    ObParser parser(*allocator_,
                    session_info_->get_sql_mode(),
                    session_info_->get_charsets4parser());
    ParseMode parse_mode = DYNAMIC_SQL_MODE;
    ParseResult parse_result;
    if (OB_FAIL(parser.parse(pvalue_, parse_result, parse_mode))) {
      LOG_WARN("failed to parse result", K(ret), K(pvalue_));
    } else {/*do nothing*/}
    if (OB_FAIL(ret)) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Cannot parse for clause", K(ret), K(pvalue_));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Cannot parse for clause");
    }
  }
  return ret;
}

int ObNoInvalidatePrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (0 == pvalue_.case_compare("TRUE")) {
    if (param != NULL) {
      param->no_invalidate_ = true;
    }
  } else if (pvalue_.empty() ||
             0 == pvalue_.case_compare("FALSE") ||
             0 == pvalue_.case_compare("DBMS_STATS.AUTO_INVALIDATE")) {
    if (param != NULL) {
      param->no_invalidate_ = false;
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for CASCADE", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal value for CASCADE : must be \
                                          {TRUE, FALSE, DBMS_STATS.AUTO_INVALIDATE}");
  }
  return ret;
}

int ObOptionsPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (0 == pvalue_.case_compare("GATHER")) {
    if (param != NULL) {
      //not implement
    }
  } else if (pvalue_.empty() ||
             0 == pvalue_.case_compare("GATHER AUTO")) {
    if (param != NULL) {
      //not implement
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for option", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal option: must be GATHER | GATHER AUTO");
  }
  return ret;
}

int ObStalePercentPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (!pvalue_.empty()) {
    ObObj src_obj;
    ObObj dest_obj;
    src_obj.set_string(ObVarcharType, pvalue_);
    ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
    ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
    double dst_val = 0.0;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, src_obj, dest_obj))) {
      LOG_WARN("failed to type", K(ret), K(src_obj));
    } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(dest_obj.get_number(), dst_val))) {
      LOG_WARN("failed to cast number to double", K(ret), K(src_obj));
    } else if (dst_val < 0) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Illegal staleness percentage", K(ret), K(dst_val));
    } else if (param != NULL) {
      //do nothing
    } else {/*do nothing*/}
    if (OB_FAIL(ret)) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Illegal staleness percentage", K(ret), K(pvalue_));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal staleness percentage, must be a non-negative number");
    }
  }
  return ret;
}

int ObApproximateNdvPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (pvalue_.empty() ||
      0 == pvalue_.case_compare("TRUE")) {
    if (param != NULL) {
      param->need_approx_ndv_ = true;
    }
  } else if (0 == pvalue_.case_compare("FALSE")) {
    if (param != NULL) {
      param->need_approx_ndv_ = false;
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for APPROXIMATE_NDV", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Illegal value for APPROXIMATE_NDV: must be {TRUE, FALSE}");
  }
  return ret;
}

int ObEstimateBlockPrefs::check_pref_value_validity(ObTableStatParam *param/*default null*/)
{
  int ret = OB_SUCCESS;
  if (pvalue_.empty() ||
      0 == pvalue_.case_compare("TRUE")) {
    bool no_estimate_block = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
    if (param != NULL) {
      if (no_estimate_block) {
        param->need_estimate_block_ = false;
      } else {
        param->need_estimate_block_ = true;
      }
    }
  } else if (0 == pvalue_.case_compare("FALSE")) {
    if (param != NULL) {
      param->need_estimate_block_ = false;
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal value for ESTIMATE_BLOCK", K(ret), K(pvalue_));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Illegal value for ESTIMATE_BLOCK: must be {TRUE, FALSE}");
  }
  return ret;
}

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

//compatible oracle, global prefs/schema prefs just only can set "for all columns...."
int ObMethodOptPrefs::check_global_method_opt_prefs_value_validity(ObString &method_opt_val)
{
  int ret = OB_SUCCESS;
  const char *val_ptr = method_opt_val.ptr();
  const int32_t val_len = method_opt_val.length();
  const char *str1 = "for";
  const char *str2 = "all";
  int32_t i = 0;
  bool is_valid = false;
  while (i < val_len && ISSPACE(val_ptr[i])) { ++i; }
  if (i < val_len && 0 == strncasecmp(val_ptr + i, str1, strlen(str1))) {
    i = i + static_cast<int32_t>(strlen(str1));
    if (i < val_len && ISSPACE(val_ptr[i++])) {
      while (i < val_len && ISSPACE(val_ptr[i])) { ++i; }
      if (i < val_len && 0 == strncasecmp(val_ptr + i, str2, strlen(str2))) {
        i = i + static_cast<int32_t>(strlen(str2));
        if (i < val_len && ISSPACE(val_ptr[i])) {
          is_valid = true;
        }
      }
    }
  }
  if (!is_valid) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("method_opt should follow the syntax \"[FOR ALL [INDEXED|HIDDEN] COLUMNS [size_caluse]]\""
             " when gathering statistics on a group of tables", K(ret), K(method_opt_val));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "method_opt should follow the syntax \"[FOR ALL [INDEXED|HIDDEN]"
                          " COLUMNS [size_caluse]]\" when gathering statistics on a group of tables");
  }
  return ret;
}


} // namespace common
} // namespace oceanbase
