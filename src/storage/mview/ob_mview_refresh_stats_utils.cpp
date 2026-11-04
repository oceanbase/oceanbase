/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share::schema;

int ObMViewRefreshStatsUtils::execute_trans_write(common::ObMySQLProxy *sql_proxy,
                                                  uint64_t tenant_id,
                                                  const common::ObSqlString &sql,
                                                  int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObMySQLTransaction trans;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write", KR(ret), K(sql));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to end trans", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsUtils::purge_refresh_stats(common::ObMySQLProxy *sql_proxy,
                                                  uint64_t tenant_id,
                                                  int64_t retention_period,
                                                  int64_t &affected_rows,
                                                  bool enable_retention_reload)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;

  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy), K(tenant_id));
  } else {
    PurgeState state = {retention_period, 0 /*delete_rounds*/, 0 /*affected_rows*/, enable_retention_reload};
    const char *table_names[] = { share::OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME,
                                  share::OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME,
                                  share::OB_ALL_MVIEW_REFRESH_STATS_TNAME,
                                  share::OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME
                                };
    const int64_t table_count = sizeof(table_names) / sizeof(table_names[0]);
    for (int64_t table_index = 0; OB_SUCC(ret) && !state.is_stopped() && table_index < table_count; ++table_index) {
      if (OB_FAIL(purge_table(sql_proxy, tenant_id, table_names[table_index], state))) {
        LOG_WARN("fail to purge table", KR(ret), K(table_index), K(table_names[table_index]));
      }
    }
    affected_rows = state.affected_rows_;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::purge_table(common::ObMySQLProxy *sql_proxy,
                                          uint64_t tenant_id,
                                          const char *table_name,
                                          PurgeState &state)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  int64_t lower_id = 0;
  int64_t max_id = 0;
  // refresh_id ascending never moves gmt_create backwards, so a window that deleted less than
  // a batch means every later window is not expired either.
  for (int64_t window_deleted = PURGE_BATCH_COUNT;
       OB_SUCC(ret) && !state.is_stopped() && window_deleted >= PURGE_BATCH_COUNT;) {
    window_deleted = 0;
    if (OB_FAIL(fetch_refresh_id_range(sql_proxy, tenant_id, table_name, lower_id, max_id))) {
      LOG_WARN("fail to fetch refresh_id range", KR(ret), K(table_name), K(lower_id));
    } else if (max_id <= lower_id) {
      // empty window, window_deleted stays 0 and finishes this table
    } else if (OB_FAIL(delete_sql.assign_fmt("delete from %s where tenant_id = 0 and refresh_id > %ld "
                                             "and refresh_id <= %ld and gmt_create < date_sub(now(), interval %ld day)",
                                             table_name,
                                             lower_id,
                                             max_id,
                                             state.retention_period_))) {
      LOG_WARN("fail to assign delete sql", KR(ret), K(table_name));
    } else if (OB_FAIL(execute_trans_write(sql_proxy, tenant_id, delete_sql, window_deleted))) {
      LOG_WARN("fail to execute delete", KR(ret), K(delete_sql));
    } else if (OB_FAIL(record_delete(sql_proxy, tenant_id, window_deleted, state))) {
      LOG_WARN("fail to record delete", KR(ret), K(window_deleted));
    } else {
      lower_id = max_id;
    }
  }
  return ret;
}

int ObMViewRefreshStatsUtils::fetch_refresh_id_range(common::ObMySQLProxy *sql_proxy,
                                                     uint64_t tenant_id,
                                                     const char *table_name,
                                                     int64_t lower_id,
                                                     int64_t &max_id)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObSqlString sql;
  max_id = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("select max(refresh_id) as max_id from "
                               "(select refresh_id from %s where tenant_id = 0 and refresh_id > %ld "
                               "order by refresh_id limit %ld) batch",
                               table_name,
                               lower_id,
                               PURGE_BATCH_COUNT))) {
      LOG_WARN("fail to assign select sql", KR(ret), K(table_name));
    } else if (OB_FAIL(sql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute select", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret), K(sql));
    } else {
      // an empty window has NULL max, which is read as 0
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "max_id", max_id, int64_t, true, false, 0);
    }
  }
  return ret;
}

int ObMViewRefreshStatsUtils::record_delete(common::ObMySQLProxy *sql_proxy,
                                            uint64_t tenant_id,
                                            int64_t deleted_rows,
                                            PurgeState &state)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshStatsParams sys_defaults;
  state.affected_rows_ += deleted_rows;
  ++state.delete_rounds_;
  if (state.enable_retention_reload_ && 0 == state.delete_rounds_ % RETENTION_CHECK_INTERVAL) {
    if (OB_FAIL(ObMViewRefreshStatsParams::fetch_sys_defaults(*sql_proxy, tenant_id, sys_defaults))) {
      LOG_WARN("fail to fetch sys defaults for unified retention", KR(ret), K(tenant_id));
    } else if (state.retention_period_ != sys_defaults.get_retention_period()) {
      LOG_INFO("system retention period changed during purge",
               K(tenant_id),
               "old_retention_period",
               state.retention_period_,
               "new_retention_period",
               sys_defaults.get_retention_period());
      state.retention_period_ = sys_defaults.get_retention_period();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
