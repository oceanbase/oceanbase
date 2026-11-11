/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_refresh_report_fetcher.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"
#include "storage/mview/ob_mview_refresh_plan_format.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace sql;
using namespace share::schema;

// One __ALL_VIRTUAL_PROCESSLIST row: the session currently executing a
// statement of the observed refresh run, pinning its active plan location.
struct MViewLiveSessionRow
{
  MViewLiveSessionRow() : svr_ip_(), svr_port_(0), plan_id_(0), sql_id_() {}

  common::ObString svr_ip_;
  int64_t svr_port_;
  int64_t plan_id_;
  common::ObString sql_id_;
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(plan_id), K_(sql_id));
};

enum LiveSessionColumnIdx : int64_t
{
  LIVE_SESSION_COL_SVR_IP = 0,
  LIVE_SESSION_COL_SVR_PORT,
  LIVE_SESSION_COL_PLAN_ID,
  LIVE_SESSION_COL_SQL_ID,
};

static bool is_live_plan_sql_id_valid(const common::ObString &sql_id)
{
  return !sql_id.empty() && sql_id.length() <= common::OB_MAX_SQL_ID_LENGTH;
}

static bool is_pending_live_match(const MViewReportStmtData *stmt, const common::ObString &sql_id)
{
  return OB_NOT_NULL(stmt) && MViewLivePlanInfo::NOT_AVAILABLE == stmt->live_plan_info_.state_
         && 0 == stmt->sqlid_.case_compare(sql_id);
}

static int collect_distinct_mv_ids(const ObIArray<MViewReportMVData> &mv_array, ObIArray<int64_t> &mv_ids)
{
  int ret = OB_SUCCESS;
  int64_t last_id = common::OB_INVALID_ID;
  mv_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_array.count(); ++i) {
    if (mv_array.at(i).mview_id_ != last_id) {
      last_id = mv_array.at(i).mview_id_;
      if (OB_FAIL(mv_ids.push_back(last_id))) {
        LOG_WARN("fail to push mv_id", KR(ret));
      }
    }
  }
  return ret;
}

static int resolve_table_name(ObSchemaGetterGuard &schema_guard,
                              const uint64_t tenant_id,
                              const uint64_t table_id,
                              ObIAllocator &allocator,
                              ObString &table_name,
                              ObString &display_name)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObSimpleDatabaseSchema *db_schema = NULL;
  ObSqlString tmp_name;
  uint64_t db_id = 0;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
  } else if (OB_FAIL(ob_write_string(allocator, table_schema->get_table_name_str(), table_name))) {
    LOG_WARN("fail to copy table name", KR(ret), K(table_id));
  } else if (OB_FALSE_IT(db_id = table_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_id, db_schema))) {
    LOG_WARN("fail to get db schema", KR(ret), K(tenant_id), K(db_id));
  } else if (OB_ISNULL(db_schema)) {
  } else if (OB_FAIL(tmp_name.assign_fmt("%.*s.%.*s",
                                         db_schema->get_database_name_str().length(),
                                         db_schema->get_database_name_str().ptr(),
                                         table_name.length(),
                                         table_name.ptr()))) {
    LOG_WARN("fail to build full name", KR(ret), K(db_id), K(table_name));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_name.string(), display_name))) {
    LOG_WARN("fail to copy display name", KR(ret));
  }
  return ret;
}

static int prepare_report_names(ObExecContext &ctx,
                                const uint64_t target_tenant_id,
                                MViewReportContext &context)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObUserInfo *user_info = NULL;
  ObIAllocator *allocator = NULL;
  MViewReportData *data = NULL;
  ObSchemaGetterGuard tenant_guard;
  if (OB_ISNULL(sql_ctx = ctx.get_sql_ctx())
      || OB_ISNULL(session_info = ctx.get_my_session())
      || OB_ISNULL(allocator = context.allocator_)
      || OB_ISNULL(data = context.data_)
      || OB_ISNULL(data->run_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_ctx, session, allocator, data or run_data is null", KR(ret));
  } else if (OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (session_info->get_effective_tenant_id() == target_tenant_id) {
    schema_guard = sql_ctx->schema_guard_;
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(target_tenant_id, tenant_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(target_tenant_id));
  } else {
    schema_guard = &tenant_guard;
  }

  if (OB_FAIL(ret)) {
  } else if (0 != data->run_data_->mview_id_
             && OB_FAIL(resolve_table_name(*schema_guard,
                                           target_tenant_id,
                                           data->run_data_->mview_id_,
                                           *allocator,
                                           data->run_data_->mview_name_,
                                           data->run_data_->mview_display_name_))) {
    LOG_WARN("fail to resolve target mv name", KR(ret), K(target_tenant_id), K(data->run_data_->mview_id_));
  }

  if (OB_FAIL(ret)) {
  } else if (0 != data->run_data_->run_user_id_) {
    user_info = schema_guard->get_user_info(target_tenant_id,
                                            static_cast<uint64_t>(data->run_data_->run_user_id_));
    if (OB_ISNULL(user_info)) {
    } else if (OB_FAIL(ob_write_string(*allocator, user_info->get_user_name(), data->run_data_->run_owner_))) {
      LOG_WARN("fail to copy user name", KR(ret), K(data->run_data_->run_user_id_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (data->mv_array_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data->mv_array_.count(); ++i) {
      MViewReportMVData &mv = data->mv_array_.at(i);
      if (OB_FAIL(resolve_table_name(*schema_guard,
                                     target_tenant_id,
                                     mv.mview_id_,
                                     *allocator,
                                     mv.mv_name_,
                                     mv.mv_display_name_))) {
        LOG_WARN("fail to resolve mv name", KR(ret), K(target_tenant_id), K(mv.mview_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (data->change_array_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data->change_array_.count(); ++i) {
      MViewReportChangeData &ch = data->change_array_.at(i);
      if (OB_FAIL(resolve_table_name(*schema_guard,
                                     target_tenant_id,
                                     ch.detail_table_id_,
                                     *allocator,
                                     ch.tbl_name_,
                                     ch.tbl_display_name_))) {
        LOG_WARN("fail to resolve change table name", KR(ret), K(target_tenant_id), K(ch.detail_table_id_));
      }
    }
  }
  return ret;
}

static int find_final_mv_data(ObIArray<MViewReportMVData> &mv_array, int64_t mview_id,
                              MViewReportMVData *&mv_data)
{
  int ret = OB_SUCCESS;
  mv_data = NULL;
  for (int64_t i = mv_array.count() - 1; OB_ISNULL(mv_data) && i >= 0; --i) {
    if (mv_array.at(i).mview_id_ == mview_id) {
      mv_data = &mv_array.at(i);
    }
  }
  if (OB_ISNULL(mv_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv data not found in array", KR(ret), K(mview_id));
  }
  return ret;
}

static int fetch_prev_refresh_info(ObExecContext &ctx,
                                   uint64_t conn_tenant_id,
                                   uint64_t target_tenant_id,
                                   int64_t refresh_id,
                                   int64_t mview_id,
                                   const char *stats_table,
                                   const char *run_stats_table,
                                   bool same_tenant,
                                   ObIArray<MViewReportMVData> &mv_array)
{
  int ret = OB_SUCCESS;
  ObSqlString prev_sql;
  const char *hint = same_tenant ? "/*+ INDEX(s idx_mview_refresh_stats_mview_end_time) */ " : "";
  // Inner tables store data with extract tenant_id (0), while virtual tables
  // expose the real tenant_id.  Use 0 for direct inner-table queries, otherwise
  // use the caller-supplied target_tenant_id.
  const uint64_t query_tenant_id = same_tenant ? 0 : target_tenant_id;
  uint64_t prev_refresh_scn = 0;
  uint64_t data_target_scn = 0;
  MViewReportMVData *mv_data = NULL;
  if (OB_FAIL(prev_sql.assign_fmt(
          "SELECT %s"
          "s.refresh_scn, r.data_target_scn "
          "FROM %s s "
          "LEFT JOIN %s r "
          "  ON r.tenant_id = s.tenant_id AND r.refresh_id = s.refresh_id "
          "WHERE s.tenant_id = %lu AND s.mview_id = %ld "
          "  AND s.result = 0 AND s.refresh_scn IS NOT NULL "
          "  AND s.end_time < ("
          "    SELECT MIN(c.start_time) FROM %s c "
          "    WHERE c.tenant_id = %lu AND c.refresh_id = %ld AND c.mview_id = %ld"
          "  ) "
          "ORDER BY s.end_time DESC, s.refresh_id DESC, s.retry_id DESC LIMIT 1",
          hint,
          stats_table,
          run_stats_table,
          query_tenant_id,
          mview_id,
          stats_table,
          query_tenant_id,
          refresh_id,
          mview_id))) {
    LOG_WARN("fail to build prev scn sql", KR(ret), K(mview_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, prev_res)
    {
      sqlclient::ObMySQLResult *prev_result = NULL;
      if (OB_FAIL(ctx.get_sql_proxy()->read(prev_res, conn_tenant_id, prev_sql.ptr()))) {
        LOG_WARN("fail to execute prev scn query", KR(ret), K(mview_id));
      } else if (OB_ISNULL(prev_result = prev_res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null prev scn result", KR(ret));
      } else if (OB_FAIL(prev_result->next())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get prev scn result", KR(ret), K(mview_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*prev_result,
                                                    "refresh_scn",
                                                    prev_refresh_scn,
                                                    uint64_t,
                                                    false /*skip_null_error*/,
                                                    false /*skip_column_error*/,
                                                    0);
        EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*prev_result,
                                                    "data_target_scn",
                                                    data_target_scn,
                                                    uint64_t,
                                                    true /*skip_null_error*/,
                                                    false /*skip_column_error*/,
                                                    0);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(find_final_mv_data(mv_array, mview_id, mv_data))) {
      LOG_WARN("fail to find final mv data", KR(ret), K(mview_id));
    } else {
      mv_data->mv_refresh_start_scn_ = prev_refresh_scn;
      mv_data->base_table_start_scn_ = data_target_scn;
    }
  }
  return ret;
}

static int fetch_run_data(ObExecContext &ctx,
                          uint64_t conn_tenant_id,
                          uint64_t target_tenant_id,
                          int64_t refresh_id,
                          ObIAllocator &allocator,
                          MViewReportRunData &run_data)
{
  int ret = OB_SUCCESS;
  run_data.reset();
  run_data.refresh_id_ = refresh_id;
  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT s.run_user_id, s.method, s.parallelism, s.nested, "
                               "s.start_time, s.end_time, s.elapsed_time, "
                               "s.trace_id, s.mview_id, s.data_target_scn, s.result, s.error_message "
                               "FROM %s s "
                               "WHERE s.tenant_id = %lu AND s.refresh_id = %ld",
                               OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME,
                               target_tenant_id,
                               refresh_id))) {
      LOG_WARN("fail to build run_stats query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *sql_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute run_stats query", KR(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql result", KR(ret));
        } else if (OB_FAIL(sql_result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("refresh_id not found in run_stats", KR(ret), K(refresh_id));
            LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "refresh_id not found");
          }
        } else {
          ObString tmp_str;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "run_user_id",
                                                     run_data.run_user_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "method", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, run_data.method_))) {
            LOG_WARN("fail to deep copy method", KR(ret));
          }
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "parallelism",
                                                     run_data.parallelism_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(*sql_result, "nested", run_data.nested_);
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "start_time", run_data.start_time_);
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "end_time", run_data.end_time_);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "elapsed_time",
                                                     run_data.elapsed_time_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "trace_id", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, run_data.trace_id_))) {
            LOG_WARN("fail to deep copy trace_id", KR(ret));
          }
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "mview_id",
                                                     run_data.mview_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                      "data_target_scn",
                                                      run_data.data_target_scn_,
                                                      uint64_t,
                                                      true,
                                                      true,
                                                      0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "result",
                                                     run_data.result_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          tmp_str.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "error_message", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, run_data.error_message_))) {
            LOG_WARN("fail to deep copy error_message", KR(ret));
          }
          if (OB_SUCC(ret) && run_data.needs_live_elapsed()) {
            run_data.elapsed_time_ = ObTimeUtility::current_time() - run_data.start_time_;
          }
          if (OB_SUCC(ret)) {
            run_data.is_valid_ = true;
          }
        }
      }
    }
  }
  return ret;
}

static int fetch_mv_data(ObExecContext &ctx,
                         uint64_t conn_tenant_id,
                         uint64_t target_tenant_id,
                         int64_t refresh_id,
                         ObIAllocator &allocator,
                         ObIArray<MViewReportMVData> &mv_array)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  mv_array.reset();
  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT s.mview_id, s.retry_id, s.refresh_type, "
                               "s.start_time, s.end_time, s.elapsed_time, "
                               "s.initial_num_rows, s.final_num_rows, s.result, "
                               "s.refresh_scn AS mv_refresh_end_scn, "
                               "s.svr_ip, s.svr_port "
                               "FROM %s s "
                               "WHERE s.tenant_id = %lu AND s.refresh_id = %ld "
                               "ORDER BY s.mview_id, s.retry_id",
                               OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TNAME,
                               target_tenant_id,
                               refresh_id))) {
      LOG_WARN("fail to build mv_stats query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *sql_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute mv_stats query", KR(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql result", KR(ret));
        }
        const int64_t now = ObTimeUtility::current_time();
        while (OB_SUCC(ret) && OB_SUCC(sql_result->next())) {
          MViewReportMVData mv_data;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "mview_id",
                                                     mv_data.mview_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "retry_id",
                                                     mv_data.retry_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "refresh_type",
                                                     mv_data.refresh_type_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "start_time", mv_data.start_time_);
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "end_time", mv_data.end_time_);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "elapsed_time",
                                                     mv_data.elapsed_time_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "initial_num_rows",
                                                     mv_data.initial_num_rows_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "final_num_rows",
                                                     mv_data.final_num_rows_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "result", mv_data.result_, int64_t, true, true, 0);
          mv_data.topo_order_ = 0;
          EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                      "mv_refresh_end_scn",
                                                      mv_data.mv_refresh_end_scn_,
                                                      uint64_t,
                                                      true,
                                                      true,
                                                      0);
          tmp_str.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "svr_ip", tmp_str);
          if (OB_SUCC(ret) && !tmp_str.empty()
              && OB_FAIL(ob_write_string(allocator, tmp_str, mv_data.svr_ip_))) {
            LOG_WARN("fail to deep copy svr_ip", KR(ret));
          }
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "svr_port", mv_data.svr_port_, int64_t, true, true, 0);
          if (OB_SUCC(ret) && mv_data.needs_live_elapsed()) {
            mv_data.elapsed_time_ = now - mv_data.start_time_;
          }
          if (OB_SUCC(ret) && OB_FAIL(mv_array.push_back(mv_data))) {
            LOG_WARN("fail to push mv_data", KR(ret));
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

static int fetch_change_data(ObExecContext &ctx,
                             uint64_t conn_tenant_id,
                             uint64_t target_tenant_id,
                             int64_t refresh_id,
                             ObIArray<MViewReportChangeData> &change_array)
{
  int ret = OB_SUCCESS;
  change_array.reset();
  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT c.mview_id, c.retry_id, c.detail_table_id, "
                               "c.num_rows_ins, c.num_rows_upd, c.num_rows_del, c.num_rows "
                               "FROM %s c "
                               "WHERE c.tenant_id = %lu AND c.refresh_id = %ld "
                               "ORDER BY c.mview_id, c.retry_id, c.detail_table_id",
                               OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TNAME,
                               target_tenant_id,
                               refresh_id))) {
      LOG_WARN("fail to build change_stats query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *sql_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute change_stats query", KR(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql result", KR(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(sql_result->next())) {
          MViewReportChangeData change_data;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "mview_id",
                                                     change_data.mview_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "retry_id",
                                                     change_data.retry_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "detail_table_id",
                                                     change_data.detail_table_id_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_ins",
                                                     change_data.num_rows_ins_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_upd",
                                                     change_data.num_rows_upd_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_del",
                                                     change_data.num_rows_del_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows",
                                                     change_data.num_rows_,
                                                     int64_t,
                                                     true,
                                                     true,
                                                     0);
          if (OB_SUCC(ret) && OB_FAIL(change_array.push_back(change_data))) {
            LOG_WARN("fail to push change_data", KR(ret));
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

static int fetch_stmt_data(ObExecContext &ctx,
                           uint64_t conn_tenant_id,
                           uint64_t target_tenant_id,
                           int64_t refresh_id,
                           ObIAllocator &allocator,
                           ObIArray<MViewReportStmtData> &stmt_array)
{
  int ret = OB_SUCCESS;
  stmt_array.reset();
  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT mview_id, retry_id, step, "
                               "sqlid, start_time, execution_time, "
                               "execution_plan, result "
                               "FROM %s "
                               "WHERE tenant_id = %lu AND refresh_id = %ld "
                               "ORDER BY mview_id, retry_id, step",
                               OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TNAME,
                               target_tenant_id,
                               refresh_id))) {
      LOG_WARN("fail to build stmt_stats query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *sql_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute stmt_stats query", KR(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql result", KR(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(sql_result->next())) {
          MViewReportStmtData stmt_data;
          ObString tmp_str;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "mview_id", stmt_data.mview_id_, int64_t, true, true, 0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "retry_id", stmt_data.retry_id_, int64_t, true, true, 0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "step", stmt_data.step_, int64_t, true, true, 0);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "sqlid", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, stmt_data.sqlid_))) {
            LOG_WARN("fail to deep copy sqlid", KR(ret));
          }
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "start_time", stmt_data.start_time_);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "execution_time", stmt_data.execution_time_, int64_t, true, true, 0);
          tmp_str.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "execution_plan", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, stmt_data.execution_plan_))) {
            LOG_WARN("fail to deep copy execution_plan", KR(ret));
          }
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result, "result", stmt_data.result_, int64_t, true, true, 0);
          if (OB_SUCC(ret) && !stmt_data.execution_plan_.empty()) {
            int tmp_ret = parse_mview_plan_resources(allocator,
                                                     stmt_data.execution_plan_,
                                                     stmt_data.cpu_time_,
                                                     stmt_data.io_wait_time_,
                                                     stmt_data.disk_reads_,
                                                     stmt_data.memory_used_);
            if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
              LOG_WARN("fail to parse plan resources, skip plan", K(tmp_ret), K(stmt_data.step_));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(stmt_array.push_back(stmt_data))) {
            LOG_WARN("fail to push stmt_data", KR(ret));
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

static int collect_live_plan_candidates(MViewReportData &data,
                                        ObIArray<MViewReportStmtData *> &candidates)
{
  int ret = OB_SUCCESS;
  candidates.reset();
  for (int64_t mv_idx = 0; mv_idx < data.mv_array_.count(); ++mv_idx) {
    const MViewReportMVData &mv = data.mv_array_.at(mv_idx);
    const bool can_observe_live_plan = data.is_latest_mv_attempt(mv_idx)
                                       && mv.is_running()
                                       && !mv.is_complete_refresh();
    for (int64_t stmt_idx = 0; stmt_idx < data.stmt_array_.count(); ++stmt_idx) {
      MViewReportStmtData &stmt = data.stmt_array_.at(stmt_idx);
      if (!stmt.matches_mv_attempt(mv) || !stmt.is_running()) {
      } else if (can_observe_live_plan) {
        stmt.live_plan_info_.reset();
        stmt.live_plan_info_.state_ = MViewLivePlanInfo::NOT_AVAILABLE;
        stmt.plan_info_ = NULL;
        if (OB_UNLIKELY(!is_live_plan_sql_id_valid(stmt.sqlid_))) {
          stmt.live_plan_info_.state_ = MViewLivePlanInfo::ERROR;
          LOG_WARN("invalid live plan sql id", K(stmt.sqlid_));
        } else if (OB_SUCC(ret) && OB_FAIL(candidates.push_back(&stmt))) {
          LOG_WARN("fail to collect live plan candidate", KR(ret), K(stmt));
        }
      }
    }
  }
  return ret;
}

/**
 * Locate the sessions currently executing the candidate statements of the
 * observed refresh run, filtered by trace id and the candidate sql ids.
 * Each row pins the active plan of one running statement by
 * (svr_ip, svr_port, plan_id); sql_id attributes the session back to report
 * candidate steps.
 */
static int query_live_plan_sessions(sql::ObExecContext &ctx,
                                    const uint64_t tenant_id,
                                    const common::ObString &trace_id,
                                    const common::ObIArray<MViewReportStmtData *> &candidates,
                                    common::ObIAllocator &allocator,
                                    common::ObIArray<MViewLiveSessionRow> &sessions)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *mysql_result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT SVR_IP, SVR_PORT, PLAN_ID, SQL_ID "
                               "FROM OCEANBASE.__ALL_VIRTUAL_PROCESSLIST "
                               "WHERE TENANT_ID = %lu AND TRACE_ID = '%.*s' AND SQL_ID IN (",
                               tenant_id,
                               trace_id.length(),
                               trace_id.ptr()))) {
      LOG_WARN("fail to build live session query", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
      const common::ObString &sql_id = candidates.at(i)->sqlid_;
      if (OB_FAIL(sql.append_fmt(0 == i ? "'%.*s'" : ", '%.*s'", sql_id.length(), sql_id.ptr()))) {
        LOG_WARN("fail to append sql id filter", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append(")"))) {
      LOG_WARN("fail to close sql id filter", KR(ret));
    } else if (OB_FAIL(ctx.get_sql_proxy()->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute live session query", KR(ret), K(sql));
    } else if (OB_ISNULL(mysql_result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("live session query returned null result", KR(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
      MViewLiveSessionRow row;
      ObString tmp_str;
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*mysql_result,
                                                 LIVE_SESSION_COL_SVR_PORT,
                                                 row.svr_port_,
                                                 int64_t,
                                                 true,
                                                 false,
                                                 0);
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*mysql_result,
                                                 LIVE_SESSION_COL_PLAN_ID,
                                                 row.plan_id_,
                                                 int64_t,
                                                 true,
                                                 false,
                                                 0);
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*mysql_result, LIVE_SESSION_COL_SVR_IP, tmp_str);
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, row.svr_ip_))) {
        LOG_WARN("fail to copy live session svr ip", KR(ret));
      }
      tmp_str.reset();
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*mysql_result, LIVE_SESSION_COL_SQL_ID, tmp_str);
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, row.sql_id_))) {
        LOG_WARN("fail to copy live session sql id", KR(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (row.svr_ip_.empty() || row.plan_id_ <= 0 || !is_live_plan_sql_id_valid(row.sql_id_)) {
        LOG_WARN("invalid live session plan reference", K(row));
      } else if (OB_FAIL(sessions.push_back(row))) {
        LOG_WARN("fail to add live session row", KR(ret), K(row));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/**
 * Capture the complete active plan of one running statement (plan cache
 * operators LEFT JOIN realtime monitor aggregates) directly onto the
 * report allocator; the payload stays resident for report assembly
 * and is shared by candidates executing identical SQL.
 */
static int capture_live_session_plan(common::ObMySQLProxy &sql_proxy,
                                     const uint64_t tenant_id,
                                     const common::ObString &trace_id,
                                     const MViewLiveSessionRow &session,
                                     common::ObIAllocator &allocator,
                                     MViewStmtPlanInfo *&payload)
{
  int ret = OB_SUCCESS;
  MViewStmtPlanInfo *captured = NULL;
  payload = NULL;
  if (OB_ISNULL(captured = OB_NEWx(MViewStmtPlanInfo, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate live plan payload", KR(ret), K(session));
  } else if (OB_FAIL(capture_mview_live_plan(sql_proxy,
                                             tenant_id,
                                             trace_id,
                                             session.sql_id_,
                                             session.svr_ip_,
                                             session.svr_port_,
                                             static_cast<uint64_t>(session.plan_id_),
                                             allocator,
                                             *captured))) {
    LOG_WARN("fail to capture live plan rows", KR(ret), K(session));
  } else if (0 == captured->plan_infos_.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("active plan not found in plan cache", KR(ret), K(session));
  } else {
    payload = captured;
  }
  return ret;
}

// Capture each matching SQL once; successful payloads survive later observation failures.
static int observe_live_plans(ObExecContext &ctx,
                              const uint64_t tenant_id,
                              const ObString &trace_id,
                              ObIAllocator &allocator,
                              ObIArray<MViewReportStmtData *> &candidates)
{
  int ret = OB_SUCCESS;
  // Session rows are only referenced inside this function; keep them on a
  // temporary arena released at return instead of the report allocator.
  ObArenaAllocator session_allocator("MvLiveSess", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObArray<MViewLiveSessionRow> sessions(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                        ModulePageAllocator(session_allocator, "MvLiveSess"));
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(query_live_plan_sessions(ctx,
                                       tenant_id,
                                       trace_id,
                                       candidates,
                                       session_allocator,
                                       sessions))) {
    LOG_WARN("fail to query live plan sessions", KR(ret));
  } else if (sessions.empty()) {
    LOG_WARN("no active session for live plan", K(trace_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sessions.count(); ++i) {
    const MViewLiveSessionRow &session = sessions.at(i);
    bool has_pending_match = false;
    for (int64_t j = 0; !has_pending_match && j < candidates.count(); ++j) {
      has_pending_match = is_pending_live_match(candidates.at(j), session.sql_id_);
    }
    if (!has_pending_match) {
    } else if (OB_FAIL(ctx.check_status())) {
      LOG_WARN("live plan observation interrupted", KR(ret));
    } else {
      MViewStmtPlanInfo *payload = NULL;
      const int capture_ret = capture_live_session_plan(*ctx.get_sql_proxy(),
                                                        tenant_id,
                                                        trace_id,
                                                        session,
                                                        allocator,
                                                        payload);
      if (OB_SUCCESS != capture_ret) {
        LOG_WARN("fail to capture live session plan", KR(capture_ret), K(session));
      }
      for (int64_t j = 0; j < candidates.count(); ++j) {
        MViewReportStmtData *stmt = candidates.at(j);
        if (is_pending_live_match(stmt, session.sql_id_)) {
          stmt->plan_info_ = payload;
          // ERROR marks an attempted SQL so another session cannot retry it.
          stmt->live_plan_info_.state_ = OB_SUCCESS == capture_ret ? MViewLivePlanInfo::AVAILABLE : MViewLivePlanInfo::ERROR;
        }
      }
      if (OB_TIMEOUT == capture_ret || OB_ALLOCATE_MEMORY_FAILED == capture_ret) {
        ret = capture_ret;
      }
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < candidates.count(); ++i) {
    candidates.at(i)->live_plan_info_.observation_start_time_ = start_time;
    candidates.at(i)->live_plan_info_.observation_end_time_ = end_time;
  }
  return ret;
}

// Live plans are optional; only the report request's own status is fatal.
static int fetch_live_execution_plans(ObExecContext &ctx,
                                      uint64_t target_tenant_id,
                                      ObIAllocator &allocator,
                                      MViewReportData &data)
{
  int ret = OB_SUCCESS;
  int plan_ret = OB_SUCCESS;
  ObSEArray<MViewReportStmtData *, 16> candidates;
  const ObString &trace_id = data.run_data_->trace_id_;
  if (OB_FAIL(ctx.check_status())) {
    LOG_WARN("refresh report is no longer active", KR(ret));
  } else {
    if (OB_SUCCESS != (plan_ret = collect_live_plan_candidates(data, candidates))) {
      LOG_WARN("fail to collect live plan candidates, keep report", KR(plan_ret));
    } else if (candidates.empty()) {
    } else if (OB_UNLIKELY(trace_id.empty() || trace_id.length() > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
      LOG_WARN("invalid trace id for live plan", K(trace_id));
    } else if (OB_ISNULL(ctx.get_sql_proxy())) {
      LOG_WARN("sql proxy is null for live plan observation");
    } else if (OB_SUCCESS != (plan_ret = observe_live_plans(ctx, target_tenant_id, trace_id, allocator, candidates))) {
      LOG_WARN("fail to observe live plans, keep report", KR(plan_ret));
    }
    if (OB_FAIL(ctx.check_status())) {
      LOG_WARN("refresh report is no longer active after live plan observation", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshReportFetcher::fetch_all(ObExecContext &ctx,
                                           uint64_t conn_tenant_id,
                                           uint64_t target_tenant_id,
                                           int64_t refresh_id,
                                           MViewReportContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context.allocator_) || OB_ISNULL(context.data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context not initialized", KR(ret), KP(context.allocator_), KP(context.data_));
  } else {
    ObIAllocator &allocator = *context.allocator_;
    MViewReportData &data = *context.data_;
  if (OB_ISNULL(data.run_data_ = (MViewReportRunData *)allocator.alloc(sizeof(MViewReportRunData)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc run_data", KR(ret));
  } else {
    new(data.run_data_) MViewReportRunData();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_run_data(ctx, conn_tenant_id, target_tenant_id, refresh_id, allocator, *data.run_data_))) {
    LOG_WARN("fail to fetch run data", KR(ret), K(refresh_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_mv_data(ctx, conn_tenant_id, target_tenant_id, refresh_id, allocator, data.mv_array_))) {
    LOG_WARN("fail to fetch mv data", KR(ret), K(refresh_id));
  }
  if (OB_FAIL(ret)) {
  } else if (0 < data.mv_array_.count() && OB_FAIL(collect_distinct_mv_ids(data.mv_array_, data.mv_ids_))) {
    LOG_WARN("fail to collect distinct mv ids", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (0 < data.mv_ids_.count()) {
    const bool same_tenant = (conn_tenant_id == target_tenant_id);
    const char *stats_table = same_tenant ? OB_ALL_MVIEW_REFRESH_STATS_TNAME
                                          : OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TNAME;
    const char *run_stats_table = same_tenant ? OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME
                                              : OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME;
    ObSqlString in_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < data.mv_ids_.count(); ++i) {
      if (OB_FAIL(in_ids.append_fmt(0 == i ? "%ld" : ",%ld", data.mv_ids_.at(i)))) {
        LOG_WARN("fail to append mv_id", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < data.mv_ids_.count(); ++i) {
      int tmp_ret = fetch_prev_refresh_info(ctx, conn_tenant_id, target_tenant_id,
                                            refresh_id, data.mv_ids_.at(i),
                                            stats_table, run_stats_table, same_tenant,
                                            data.mv_array_);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("fail to fetch prev refresh info, skip mv", KR(tmp_ret), K(data.mv_ids_.at(i)));
      }
    }
    ObSqlString dep_sql;
    if (OB_FAIL(ret)) {
    } else if (1 < data.mv_ids_.count()
               && OB_FAIL(dep_sql.assign_fmt("SELECT d.mview_id, d.p_obj "
                                             "FROM %s d "
                                             "WHERE d.tenant_id = %lu "
                                             "  AND d.mview_id IN (%s) "
                                             "  AND d.p_obj IN (%s)",
                                             OB_ALL_VIRTUAL_MVIEW_DEP_TNAME,
                                             target_tenant_id,
                                             in_ids.ptr(),
                                             in_ids.ptr()))) {
      LOG_WARN("fail to build dep query", KR(ret));
    } else if (1 < data.mv_ids_.count()) {
      SMART_VAR(ObMySQLProxy::MySQLResult, dep_res)
      {
        sqlclient::ObMySQLResult *dep_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(dep_res, conn_tenant_id, dep_sql.ptr()))) {
          LOG_WARN("fail to execute dep query", KR(ret));
        } else if (OB_ISNULL(dep_result = dep_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null dep result", KR(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(dep_result->next())) {
          int64_t child_id = 0;
          int64_t parent_id = 0;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*dep_result, "mview_id", child_id, int64_t, true, true, 0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*dep_result, "p_obj", parent_id, int64_t, true, true, 0);
          if (OB_SUCC(ret) && OB_FAIL(data.dep_edges_.push_back(MViewDepEdge(child_id, parent_id)))) {
            LOG_WARN("fail to push dep edge", KR(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_change_data(ctx, conn_tenant_id, target_tenant_id, refresh_id, data.change_array_))) {
    LOG_WARN("fail to fetch change data", KR(ret), K(refresh_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_stmt_data(ctx, conn_tenant_id, target_tenant_id, refresh_id, allocator, data.stmt_array_))) {
    LOG_WARN("fail to fetch stmt data", KR(ret), K(refresh_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_report_names(ctx, target_tenant_id, context))) {
    LOG_WARN("fail to prepare report names", KR(ret), K(refresh_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_live_execution_plans(ctx,
                                                target_tenant_id,
                                                allocator,
                                                data))) {
    LOG_WARN("fail to fetch live report plan", KR(ret), K(refresh_id));
  }
  }  // end else (context valid)
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
