/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "storage/mview/ob_mview_refresh_plan_format.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_server_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "storage/mview/cmd/ob_mview_refresh_executor.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_transaction.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/scn.h"
#include "share/ob_table_range.h"
#include "storage/mview/ob_mview_refresh.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;

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

int ObMViewRefreshStatsUtils::write_run_start(common::ObMySQLProxy *sql_proxy,
                                              const ObMViewRefreshRunStatsParam &run_params)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;

  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(!run_params.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy), K(run_params));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(run_params.tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(run_params));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("refresh_id", run_params.refresh_id_)) ||
             OB_FAIL(dml.add_column("run_user_id", run_params.run_user_id_)) ||
             OB_FAIL(dml.add_column("method", ObHexEscapeSqlStr(run_params.method_))) ||
             OB_FAIL(dml.add_column("parallelism", run_params.parallelism_)) ||
             OB_FAIL(dml.add_column("nested", run_params.nested_)) ||
             OB_FAIL(dml.add_time_column("start_time", run_params.start_time_)) ||
             OB_FAIL(dml.add_column("trace_id", ObHexEscapeSqlStr(ObCurTraceId::get_trace_id_str())))) {
    LOG_WARN("fail to add columns", KR(ret), K(run_params));
  } else if (data_version >= DATA_VERSION_4_6_1_0 &&  // todo: adjust version control
             (OB_FAIL(dml.add_column("mview_id", run_params.mview_id_)) ||
              OB_FAIL(dml.add_uint64_column("data_target_scn", run_params.data_target_scn_)) ||
              OB_FAIL(dml.add_column("result", 1)) ||
              OB_FAIL(dml.add_column(true, "error_message")) ||
              OB_FAIL(dml.add_column(true, "rollback_seg")) ||
              OB_FAIL(dml.add_column(true, "push_deferred_rpc")) ||
              OB_FAIL(dml.add_column(true, "refresh_after_errors")) ||
              OB_FAIL(dml.add_column(true, "purge_option")) ||
              OB_FAIL(dml.add_column(true, "heap_size")) ||
              OB_FAIL(dml.add_column(true, "atomic_refresh")) ||
              OB_FAIL(dml.add_column(true, "out_of_place")) ||
              OB_FAIL(dml.add_column(true, "complete_stats_avaliable")) ||
              OB_FAIL(dml.add_column(true, "num_mvs_total")) ||
              OB_FAIL(dml.add_column("num_mvs_current", 0)) ||
              OB_FAIL(dml.add_column(true, "mviews")) ||
              OB_FAIL(dml.add_column(true, "base_tables")) ||
              OB_FAIL(dml.add_column(true, "log_purge_time")))) {
    LOG_WARN("fail to add columns introduced in 4.4.2.2", KR(ret), K(run_params));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_4_2_2 &&
                data_version < DATA_VERSION_4_5_0_0) ||
               data_version >= DATA_VERSION_4_6_1_0) &&
             (OB_FAIL(dml.add_column("rollback_seg", "")) ||
              OB_FAIL(dml.add_column("push_deferred_rpc", false)) ||
              OB_FAIL(dml.add_column("refresh_after_errors", false)) ||
              OB_FAIL(dml.add_column("purge_option", 0)) ||
              OB_FAIL(dml.add_column("heap_size", 0)) ||
              OB_FAIL(dml.add_column("atomic_refresh", false)) ||
              OB_FAIL(dml.add_column("out_of_place", false)) ||
              OB_FAIL(dml.add_column("complete_stats_avaliable", false)) ||
              OB_FAIL(dml.add_column("num_mvs_total", 0)) ||
              OB_FAIL(dml.add_column("num_mvs_current", 0)) ||
              OB_FAIL(dml.add_column("mviews", "")) ||
              OB_FAIL(dml.add_column("base_tables", "")) ||
              OB_FAIL(dml.add_column("log_purge_time", 0)))) {
    LOG_WARN("fail to add unused columns with default", KR(ret));
  // Updated later by write_run_end; insert with zeros.
  } else if (OB_FAIL(dml.add_column("number_of_failures", 0)) ||
             OB_FAIL(dml.add_time_column("end_time", 0)) ||
             OB_FAIL(dml.add_column("elapsed_time", 0))) {
    LOG_WARN("fail to add columns updated by write_run_end", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME, sql))) {
    LOG_WARN("fail to splice insert sql", KR(ret), K(run_params));
  } else if (OB_FAIL(execute_trans_write(sql_proxy,
                                         run_params.tenant_id_,
                                         sql,
                                         affected_rows))) {
    LOG_WARN("fail to insert run stats", KR(ret), K(run_params));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("write_run_start failed, ignore error", KR(ret), K(run_params));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_mv_start(sql::ObExecContext &ctx,
                                            ObMViewTransaction &trans,
                                            const ObMViewRefreshParam &refresh_param,
                                            const ObMVRefreshStatsCollectionLevel collection_level,
                                            const share::schema::ObMVRefreshType refresh_type,
                                            const int64_t start_time,
                                            const int64_t num_steps)
{
  int ret = OB_SUCCESS;
  int64_t initial_num_rows = 0;

  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObMViewRefreshHelper::get_table_row_num(
            trans, refresh_param.tenant_id_, refresh_param.mview_id_,
            SCN::invalid_scn(), refresh_param.parallel_,
            initial_num_rows))) {
      LOG_WARN("fail to get mview row num before refresh", KR(tmp_ret), K(refresh_param.mview_id_));
    }
  }

  if (OB_SUCC(ret)) {
    share::ObDMLSqlSplicer refresh_dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    uint64_t data_version = 0;
    if (OB_FAIL(refresh_dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(refresh_dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
        OB_FAIL(refresh_dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
        OB_FAIL(refresh_dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
        OB_FAIL(refresh_dml.add_column("refresh_type", static_cast<int64_t>(refresh_type))) ||
        OB_FAIL(refresh_dml.add_time_column("start_time", start_time)) ||
        OB_FAIL(refresh_dml.add_column("initial_num_rows", initial_num_rows)) ||
        OB_FAIL(refresh_dml.add_time_column("end_time", 0)) ||
        OB_FAIL(refresh_dml.add_column("elapsed_time", 0)) ||
        OB_FAIL(refresh_dml.add_column("final_num_rows", 0)) ||
        OB_FAIL(refresh_dml.add_column("result", 1)) ||
        OB_FAIL(refresh_dml.add_column("num_steps", num_steps))) {
      LOG_WARN("fail to add refresh stats columns", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(refresh_param.tenant_id_, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(refresh_param.tenant_id_));
    } else if ((data_version >= MOCK_DATA_VERSION_4_4_2_2 && data_version < DATA_VERSION_4_5_0_0)
               || data_version >= DATA_VERSION_4_6_1_0) {
      char svr_ip_buf[OB_IP_STR_BUFF] = {0};
      if (OB_UNLIKELY(!GCTX.self_addr().ip_to_string(svr_ip_buf, sizeof(svr_ip_buf)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get server ip", KR(ret));
      } else if (OB_FAIL(refresh_dml.add_column("svr_ip", ObHexEscapeSqlStr(ObString(svr_ip_buf))))) {
        LOG_WARN("fail to add svr_ip column", KR(ret));
      } else if (OB_FAIL(refresh_dml.add_column("svr_port", GCTX.self_addr().get_port()))) {
        LOG_WARN("fail to add svr_port column", KR(ret));
      } else if (OB_FAIL(refresh_dml.add_column(true, "log_purge_time"))) {
        LOG_WARN("fail to add log_purge_time column", KR(ret));
      } else if (OB_FAIL(refresh_dml.add_uint64_column("refresh_scn", 0))) {
        LOG_WARN("fail to add refresh_scn column", KR(ret));
      }
    } else if (OB_FAIL(refresh_dml.add_column("log_purge_time", 0))) {
      LOG_WARN("fail to add log_purge_time column with default", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(refresh_dml.splice_insert_sql(OB_ALL_MVIEW_REFRESH_STATS_TNAME, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret));
    } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(),
                                           refresh_param.tenant_id_,
                                           sql,
                                           affected_rows))) {
      LOG_WARN("fail to insert refresh stats", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_mv_start, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_stmt_batch(sql::ObExecContext &ctx,
                                               sql::ObSQLSessionInfo *session_info,
                                               const ObMViewRefreshParam &refresh_param,
                                               const ObIArray<ObString> &refresh_sqls)
{
  int ret = OB_SUCCESS;
  static const int UNEXECUTED_RESULT = 1;
  uint64_t data_version = 0;

  if (OB_ISNULL(session_info) || OB_ISNULL(ctx.get_sql_proxy()) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == refresh_param.tenant_id_ ||
                  OB_INVALID_ID == refresh_param.mview_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KR(ret), K(session_info), K(ctx.get_sql_proxy()), K(refresh_param));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(refresh_param.tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(refresh_param.tenant_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < refresh_sqls.count(); ++i) {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    char sql_id_buf[OB_MAX_SQL_ID_LENGTH + 1];
    sql_id_buf[0] = '\0';
    if (OB_FAIL(sql::ObSQLUtils::gen_sql_id_from_sql_string(*session_info,
                                                            refresh_sqls.at(i),
                                                            sql_id_buf,
                                                            sizeof(sql_id_buf)))) {
      LOG_WARN("fail to calc sql id from sql string", KR(ret), K(i));
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
        OB_FAIL(dml.add_pk_column("step", i + 1)) ||
        OB_FAIL(dml.add_column("stmt", ObHexEscapeSqlStr(refresh_sqls.at(i)))) ||
        OB_FAIL(dml.add_column("sqlid", ObHexEscapeSqlStr(ObString(sql_id_buf)))) ||
        OB_FAIL(dml.add_column("execution_time", 0)) ||
        OB_FAIL(dml.add_column("result", UNEXECUTED_RESULT))) {
      LOG_WARN("fail to add stmt stats columns", KR(ret), K(i));
    } else if (data_version >= DATA_VERSION_4_6_1_0 &&
               (OB_FAIL(dml.add_time_column("start_time", 0)) ||
                OB_FAIL(dml.add_column("parallelism", 0)))) {
      LOG_WARN("fail to add new columns for data version 4.6.1.0", KR(ret), K(i));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret), K(i));
    } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(),
                                            refresh_param.tenant_id_,
                                            sql,
                                            affected_rows))) {
      LOG_WARN("fail to insert stmt stats", KR(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_stmt_batch, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_detail_table_change(sql::ObExecContext &ctx,
                                                         const ObMViewRefreshParam &refresh_param,
                                                         const ObMViewDetailTableChangeStats &data)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_ISNULL(ctx.get_sql_proxy()) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == refresh_param.tenant_id_ ||
                  OB_INVALID_ID == refresh_param.mview_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KR(ret), K(refresh_param));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
             OB_FAIL(dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
             OB_FAIL(dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
             OB_FAIL(dml.add_pk_column("detail_table_id", data.detail_table_id_)) ||
             OB_FAIL(dml.add_column("num_rows_ins", data.num_rows_ins_)) ||
             OB_FAIL(dml.add_column("num_rows_upd", data.num_rows_upd_)) ||
             OB_FAIL(dml.add_column("num_rows_del", data.num_rows_del_)) ||
             OB_FAIL(dml.add_column("num_rows", data.num_rows_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME, sql))) {
    LOG_WARN("fail to splice insert sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(),
                                         refresh_param.tenant_id_,
                                         sql,
                                         affected_rows))) {
    LOG_WARN("fail to insert change stats", KR(ret));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows unexpected", KR(ret), K(affected_rows));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_detail_table_change, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_stmt_before_step(sql::ObExecContext &ctx,
                                                     const ObMViewRefreshParam &refresh_param,
                                                     int64_t step,
                                                     int64_t start_time)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;
  if (OB_ISNULL(ctx.get_sql_proxy()) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == refresh_param.tenant_id_ ||
                  OB_INVALID_ID == refresh_param.mview_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KR(ret), K(ctx.get_sql_proxy()), K(refresh_param));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(refresh_param.tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(refresh_param.tenant_id_));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
             OB_FAIL(dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
             OB_FAIL(dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
             OB_FAIL(dml.add_pk_column("step", step))) {
    LOG_WARN("fail to add update columns", KR(ret), K(step), K(start_time));
  } else if (data_version >= DATA_VERSION_4_6_1_0 &&
             OB_FAIL(dml.add_time_column("start_time", start_time))) {
    LOG_WARN("fail to add start_time column for data version 4.4.2.2", KR(ret), K(step), K(start_time));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME, sql))) {
    LOG_WARN("fail to splice update sql", KR(ret), K(step));
  } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(),
                                         refresh_param.tenant_id_,
                                         sql,
                                         affected_rows))) {
    LOG_WARN("fail to update stmt start time", KR(ret), K(step));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_stmt_before_step, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_stmt_after_step(sql::ObExecContext &ctx,
                                                    const ObMViewRefreshParam &refresh_param,
                                                    int64_t step,
                                                    int64_t execution_time,
                                                    int result,
                                                    const ObMViewStmtPlanCaptureInfo &capture_info,
                                                    int64_t actual_parallelism)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;
  if (OB_ISNULL(ctx.get_sql_proxy()) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == refresh_param.tenant_id_ ||
                  OB_INVALID_ID == refresh_param.mview_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KR(ret), K(ctx.get_sql_proxy()), K(refresh_param));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(refresh_param.tenant_id_, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(refresh_param.tenant_id_));
  }

  // Best-effort plan capture before DML build — failure does not affect ret
  ObSqlString plan_json;
  if (OB_SUCC(ret)) {
    if (capture_info.is_full_plan() && !capture_info.sql_id().empty()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(get_mview_stmt_execution_plan(ctx,
                                                     refresh_param.tenant_id_,
                                                     capture_info.sql_id(),
                                                     capture_info.trace_id(),
                                                     capture_info.svr_ip(),
                                                     capture_info.svr_port_,
                                                     capture_info.plan_id_,
                                                     capture_info.plan_hash_,
                                                     plan_json))) {
        LOG_WARN("fail to get stmt execution plan, ignore", KR(tmp_ret));
      }
    } else if (capture_info.is_hash_only()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(build_mview_plan_hash_json(ctx.get_allocator(),
                                                 capture_info.plan_hash_,
                                                 plan_json))) {
        LOG_WARN("fail to build hash-only plan json, ignore", KR(tmp_ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
             OB_FAIL(dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
             OB_FAIL(dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
             OB_FAIL(dml.add_pk_column("step", step)) ||
             OB_FAIL(dml.add_column("execution_time", execution_time)) ||
             OB_FAIL(dml.add_column("result", result)) ||
             (!plan_json.string().empty() &&
             OB_FAIL(dml.add_column("execution_plan", ObHexEscapeSqlStr(plan_json.string()))))) {
    LOG_WARN("fail to add update columns", KR(ret), K(step));
  } else if (data_version >= DATA_VERSION_4_6_1_0 &&
             OB_FAIL(dml.add_column("parallelism", actual_parallelism))) {
    LOG_WARN("fail to add parallelism column", KR(ret), K(step));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME, sql))) {
    LOG_WARN("fail to splice update sql", KR(ret), K(step));
  } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(),
                                         refresh_param.tenant_id_,
                                         sql,
                                         affected_rows))) {
    LOG_WARN("fail to update stmt stats", KR(ret), K(step));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_stmt_after_step, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_mv_end(sql::ObExecContext &ctx,
                                          ObMViewTransaction &trans,
                                          const ObMViewRefreshParam &refresh_param,
                                          const ObMVRefreshStatsCollectionLevel collection_level,
                                          const int64_t end_time,
                                          const int64_t elapsed_time,
                                          const int result,
                                          const share::ObScnRange &mview_refresh_scn_range)
{
  int ret = OB_SUCCESS;
  int64_t final_num_rows = 0;

  if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObMViewRefreshHelper::get_table_row_num(
            trans, refresh_param.tenant_id_, refresh_param.mview_id_,
            SCN::invalid_scn(), refresh_param.parallel_,
            final_num_rows))) {
      LOG_WARN("fail to get mview row num after refresh", KR(tmp_ret), K(refresh_param.mview_id_));
    }
  }

  if (OB_SUCC(ret)) {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    uint64_t data_version = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_param.refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", refresh_param.mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", refresh_param.retry_id_)) ||
        OB_FAIL(dml.add_time_column("end_time", end_time)) ||
        OB_FAIL(dml.add_column("elapsed_time", elapsed_time)) ||
        OB_FAIL(dml.add_column("final_num_rows", final_num_rows)) ||
        OB_FAIL(dml.add_column("result", result))) {
      LOG_WARN("fail to add update columns", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(refresh_param.tenant_id_, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(refresh_param.tenant_id_));
    } else if (mview_refresh_scn_range.is_valid()
               && ((data_version >= MOCK_DATA_VERSION_4_4_2_2 && data_version < DATA_VERSION_4_5_0_0)
                   || data_version >= DATA_VERSION_4_6_1_0)
               && OB_FAIL(dml.add_uint64_column("refresh_scn",
                                                mview_refresh_scn_range.end_scn_.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to add refresh_scn column", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_MVIEW_REFRESH_STATS_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret));
    } else if (OB_FAIL(execute_trans_write(ctx.get_sql_proxy(), refresh_param.tenant_id_, sql, affected_rows))) {
      LOG_WARN("fail to update refresh stats", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to run write_mv_end, ignore error", KR(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::write_run_end(common::ObMySQLProxy *sql_proxy,
                                            const uint64_t tenant_id,
                                            const int64_t refresh_id,
                                            const int64_t end_time,
                                            const int64_t log_purge_time,
                                            const int result,
                                            const common::ObString &error_message,
                                            const int64_t num_failures)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;

  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == refresh_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE %s SET end_time = usec_to_time(%ld),"
                 " elapsed_time = %ld - time_to_usec(start_time),"
                 " number_of_failures = number_of_failures + %ld",
                 OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME,
                 end_time, end_time, num_failures))) {
    LOG_WARN("fail to assign update sql", KR(ret));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_4_2_2 &&
               data_version < DATA_VERSION_4_5_0_0) ||
              data_version >= DATA_VERSION_4_6_1_0) &&
             OB_FAIL(sql.append_fmt(", log_purge_time = %ld", log_purge_time))) {
    LOG_WARN("fail to append log_purge_time", KR(ret));
  } else if (((data_version >= MOCK_DATA_VERSION_4_4_2_2 &&
               data_version < DATA_VERSION_4_5_0_0) ||
              data_version >= DATA_VERSION_4_6_1_0) &&
             OB_FAIL(sql.append_fmt(", result = %d", result))) {
    LOG_WARN("fail to append result column", KR(ret));
  } else if (((data_version >= MOCK_DATA_VERSION_4_4_2_2 &&
               data_version < DATA_VERSION_4_5_0_0) ||
              data_version >= DATA_VERSION_4_6_1_0) && !error_message.empty()
             && (OB_FAIL(sql.append(", error_message = ")) || OB_FAIL(sql_append_hex_escape_str(error_message, sql)))) {
    LOG_WARN("fail to append error_message column", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE tenant_id = 0 AND refresh_id = %ld", refresh_id))) {
    LOG_WARN("fail to append where clause", KR(ret));
  } else if (((data_version >= MOCK_DATA_VERSION_4_4_2_2 &&
               data_version < DATA_VERSION_4_5_0_0) ||
              data_version >= DATA_VERSION_4_6_1_0) &&
             OB_FAIL(sql.append(" AND result IN (0, 1)"))) {
    LOG_WARN("fail to append first-wins guard", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update run stats", KR(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("write_run_end failed, ignore error", KR(ret), K(tenant_id), K(refresh_id), K(end_time));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMViewRefreshStatsUtils::purge_refresh_stats(common::ObMySQLProxy *sql_proxy,
                                                    uint64_t tenant_id,
                                                    int64_t retention_period,
                                                    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;

  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy), K(tenant_id));
  } else {
    ObSqlString delete_sql;
    static const int64_t PURGE_BATCH_COUNT = 100000;
    const char *table_names[] = { OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME,
                                  OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME,
                                  OB_ALL_MVIEW_REFRESH_STATS_TNAME,
                                  OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME
                                };
    const int64_t table_count = sizeof(table_names) / sizeof(table_names[0]);
    for (int64_t table_index = 0; OB_SUCC(ret) && table_index < table_count; ++table_index) {
      if (OB_FAIL(delete_sql.assign_fmt(
              "delete from %s where tenant_id = 0 and gmt_modified < date_sub(now(), interval %ld day) limit %ld",
              table_names[table_index], retention_period, PURGE_BATCH_COUNT))) {
        LOG_WARN("fail to assign delete sql", KR(ret), K(table_index));
      } else {
        int64_t cur_affected_rows = 0;
        do {
          if (OB_FAIL(execute_trans_write(sql_proxy, tenant_id, delete_sql, cur_affected_rows))) {
            LOG_WARN("fail to execute delete", KR(ret), K(delete_sql));
          } else {
            affected_rows += cur_affected_rows;
          }
        } while (OB_SUCC(ret) && cur_affected_rows >= PURGE_BATCH_COUNT);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
