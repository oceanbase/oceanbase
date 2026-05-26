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

#include "storage/mview/cmd/ob_mview_refresh_report_fetcher.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_errno.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"
#include "storage/mview/ob_mview_refresh_plan_format.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace sql;

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
    if (OB_FAIL(sql.assign_fmt("SELECT s.run_user_id, s.method, s.push_deferred_rpc, s.refresh_after_errors, "
                               "s.purge_option, s.parallelism, s.heap_size, "
                               "s.atomic_refresh, s.nested, s.out_of_place, "
                               "s.number_of_failures, s.start_time, s.end_time, "
                               "s.elapsed_time, s.log_purge_time, s.complete_stats_avaliable, "
                               "s.trace_id, s.mview_id, s.data_target_scn "
                               "FROM %s s "
                               "WHERE s.tenant_id = %lu AND s.refresh_id = %ld AND s.elapsed_time > 0",
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
            LOG_WARN("refresh has not finished yet or refresh_id not found", KR(ret), K(refresh_id));
            LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "refresh has not finished yet, no report available");
          }
        } else {
          ObString tmp_str;
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "run_user_id", run_data.run_user_id_, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "method", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, run_data.method_))) {
            LOG_WARN("fail to deep copy method", KR(ret));
          }
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "push_deferred_rpc", run_data.push_deferred_rpc_);
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "refresh_after_errors", run_data.refresh_after_errors_);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "purge_option", run_data.purge_option_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "parallelism", run_data.parallelism_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "heap_size", run_data.heap_size_, int64_t);
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "atomic_refresh", run_data.atomic_refresh_);
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "nested", run_data.nested_);
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "out_of_place", run_data.out_of_place_);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "number_of_failures", run_data.number_of_failures_, int64_t);
          EXTRACT_TIMESTAMP_FIELD_MYSQL(*sql_result, "start_time", run_data.start_time_);
          EXTRACT_TIMESTAMP_FIELD_MYSQL(*sql_result, "end_time", run_data.end_time_);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "elapsed_time", run_data.elapsed_time_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "log_purge_time", run_data.log_purge_time_, int64_t);
          EXTRACT_BOOL_FIELD_MYSQL(*sql_result, "complete_stats_avaliable", run_data.complete_stats_available_);
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
                               "s.initial_num_rows, s.final_num_rows, s.num_steps, s.result, "
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
        while (OB_SUCC(ret) && OB_SUCC(sql_result->next())) {
          MViewReportMVData mv_data;
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "mview_id", mv_data.mview_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "retry_id", mv_data.retry_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "refresh_type", mv_data.refresh_type_, int64_t);
          EXTRACT_TIMESTAMP_FIELD_MYSQL(*sql_result, "start_time", mv_data.start_time_);
          EXTRACT_TIMESTAMP_FIELD_MYSQL(*sql_result, "end_time", mv_data.end_time_);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "elapsed_time", mv_data.elapsed_time_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "initial_num_rows", mv_data.initial_num_rows_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "final_num_rows", mv_data.final_num_rows_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "num_steps", mv_data.num_steps_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "result", mv_data.result_, int64_t);
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
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "mview_id", change_data.mview_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "retry_id", change_data.retry_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "detail_table_id", change_data.detail_table_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_ins",
                                                     change_data.num_rows_ins_,
                                                     int64_t,
                                                     true,
                                                     false,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_upd",
                                                     change_data.num_rows_upd_,
                                                     int64_t,
                                                     true,
                                                     false,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows_del",
                                                     change_data.num_rows_del_,
                                                     int64_t,
                                                     true,
                                                     false,
                                                     0);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*sql_result,
                                                     "num_rows",
                                                     change_data.num_rows_,
                                                     int64_t,
                                                     true,
                                                     false,
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
                               "sqlid, stmt, start_time, execution_time, "
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
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "mview_id", stmt_data.mview_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "retry_id", stmt_data.retry_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "step", stmt_data.step_, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "sqlid", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, stmt_data.sqlid_))) {
            LOG_WARN("fail to deep copy sqlid", KR(ret));
          }
          tmp_str.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL(*sql_result, "stmt", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, stmt_data.stmt_))) {
            LOG_WARN("fail to deep copy stmt", KR(ret));
          }
          EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*sql_result, "start_time", stmt_data.start_time_);
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "execution_time", stmt_data.execution_time_, int64_t);
          tmp_str.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*sql_result, "execution_plan", tmp_str);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_str, stmt_data.execution_plan_))) {
            LOG_WARN("fail to deep copy execution_plan", KR(ret));
          }
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "result", stmt_data.result_, int64_t);
          if (OB_SUCC(ret) && !stmt_data.execution_plan_.empty()) {
            int tmp_ret = aggregate_mview_plan_resources(allocator,
                                                         stmt_data.execution_plan_,
                                                         stmt_data.cpu_time_,
                                                         stmt_data.io_wait_time_,
                                                         stmt_data.disk_reads_,
                                                         stmt_data.memory_used_,
                                                         stmt_data.affected_rows_);
            if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
              LOG_WARN("fail to aggregate plan resources, skip plan", K(tmp_ret), K(stmt_data.step_));
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
  } else {
    ObSqlString base_scn_sql;
    if (OB_FAIL(base_scn_sql.assign_fmt("SELECT MAX(data_target_scn) AS base_scn FROM %s "
                                        "WHERE tenant_id = %lu AND refresh_id < %ld AND data_target_scn IS NOT NULL",
                                        OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME,
                                        target_tenant_id,
                                        refresh_id))) {
      LOG_WARN("fail to build base_scn sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, base_res)
      {
        sqlclient::ObMySQLResult *base_result = NULL;
        if (OB_FAIL(ctx.get_sql_proxy()->read(base_res, conn_tenant_id, base_scn_sql.ptr()))) {
          LOG_WARN("fail to execute base_scn query", KR(ret));
        } else if (OB_ISNULL(base_result = base_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null base_scn result", KR(ret));
        } else if (OB_FAIL(base_result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get base_scn result", KR(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          uint64_t base_scn = 0;
          EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*base_result, "base_scn", base_scn, uint64_t, true, true, 0);
          for (int64_t i = 0; i < data.mv_array_.count(); ++i) {
            data.mv_array_.at(i).base_table_start_scn_ = base_scn;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 < data.mv_array_.count() && OB_FAIL(collect_distinct_mv_ids(data.mv_array_, data.mv_ids_))) {
    LOG_WARN("fail to collect distinct mv ids", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (0 < data.mv_ids_.count()) {
    ObSqlString in_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < data.mv_ids_.count(); ++i) {
      if (OB_FAIL(in_ids.append_fmt(0 == i ? "%ld" : ",%ld", data.mv_ids_.at(i)))) {
        LOG_WARN("fail to append mv_id", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSqlString hist_scn_sql;
      if (OB_FAIL(hist_scn_sql.assign_fmt("SELECT mview_id, MAX(refresh_scn) AS hist_scn "
                                          "FROM %s "
                                          "WHERE tenant_id = %lu AND result = 0 "
                                          "  AND refresh_id < %ld "
                                          "  AND mview_id IN (%s) "
                                          "GROUP BY mview_id",
                                          OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TNAME,
                                          target_tenant_id,
                                          refresh_id,
                                          in_ids.ptr()))) {
        LOG_WARN("fail to build hist_scn sql", KR(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, hist_res)
        {
          sqlclient::ObMySQLResult *hist_result = NULL;
          if (OB_FAIL(ctx.get_sql_proxy()->read(hist_res, conn_tenant_id, hist_scn_sql.ptr()))) {
            LOG_WARN("fail to execute hist_scn query", KR(ret));
          } else if (OB_ISNULL(hist_result = hist_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null hist_scn result", KR(ret));
          } else {
            while (OB_SUCC(ret) && OB_SUCC(hist_result->next())) {
              int64_t result_mv_id = 0;
              uint64_t hist_scn = 0;
              EXTRACT_INT_FIELD_MYSQL(*hist_result, "mview_id", result_mv_id, int64_t);
              EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*hist_result, "hist_scn", hist_scn, uint64_t, true, true, 0);
              if (OB_SUCC(ret)) {
                for (int64_t i = 0; i < data.mv_array_.count(); ++i) {
                  if (data.mv_array_.at(i).mview_id_ == result_mv_id
                      && hist_scn > data.mv_array_.at(i).mv_refresh_start_scn_) {
                    data.mv_array_.at(i).mv_refresh_start_scn_ = hist_scn;
                  }
                }
              }
            }
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
          }
        }
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
          EXTRACT_INT_FIELD_MYSQL(*dep_result, "mview_id", child_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*dep_result, "p_obj", parent_id, int64_t);
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
  }  // end else (context valid)
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
