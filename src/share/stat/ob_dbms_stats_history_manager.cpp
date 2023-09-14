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
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_opt_stat_sql_service.h"

namespace oceanbase {
using namespace sql;
namespace common {

#define FETCH_TAB_STATS_HISTROY "SELECT table_id, partition_id, object_type, row_cnt row_count, \
                                 avg_row_len avg_row_size, macro_blk_cnt macro_block_num,\
                                 micro_blk_cnt micro_block_num, stattype_locked,last_analyzed FROM \
                                 %s T WHERE tenant_id = %lu and table_id = %ld and \
                                 partition_id in %s and savtime in (SELECT min(savtime) From \
                                 %s TF where TF.tenant_id = T.tenant_id \
                                 and TF.table_id = T.table_id and TF.partition_id = T.partition_id \
                                 and savtime >= usec_to_time('%ld'));"

#define FETCH_COL_STATS_HISTROY "SELECT table_id, partition_id, column_id, object_type stat_level,\
                                 distinct_cnt num_distinct, null_cnt num_null, b_max_value,\
                                 b_min_value, avg_len, distinct_cnt_synopsis, distinct_cnt_synopsis_size,\
                                 histogram_type, sample_size, bucket_cnt, density, last_analyzed\
                                 FROM %s T WHERE tenant_id = %lu and table_id = %ld \
                                 and partition_id in %s and savtime in (SELECT min(savtime) From \
                                 %s TF where TF.tenant_id = T.tenant_id \
                                 and TF.table_id = T.table_id and TF.partition_id = T.partition_id \
                                 and savtime >= usec_to_time('%ld'));"

#define FETCH_HISTOGRAM_HISTROY "SELECT endpoint_num, b_endpoint_value, endpoint_repeat_cnt\
                                 FROM %s T WHERE TENANT_ID=%lu AND\
                                 table_id=%ld and partition_id = %ld and column_id = %ld and \
                                 savtime in (SELECT min(savtime) From %s \
                                 TF where TF.tenant_id = T.tenant_id and TF.table_id = T.table_id \
                                 and TF.partition_id = T.partition_id and savtime >= \
                                 usec_to_time('%ld')) ORDER BY ENDPOINT_NUM;"

#define FETCH_STATS_HISTROY_RETENTION "SELECT sval1 retention FROM %s WHERE\
                                       sname = 'STATS_RETENTION';"

#define FETCH_STATS_HISTROY_AVAILABILITY "SELECT min(savtime) FROM %s;"

#define UPDATE_STATS_HISTROY_RETENTION "UPDATE %s SET sval1 = %ld, \
                                        sval2 = CURRENT_TIMESTAMP  where sname = 'STATS_RETENTION';"

#define DELETE_STAT_HISTORY "DELETE FROM %s %s;"


int ObDbmsStatsHistoryManager::get_history_stat_handles(ObExecContext &ctx,
                                                        const ObTableStatParam &param,
                                                        ObIArray<ObOptTableStatHandle> &history_tab_handles,
                                                        ObIArray<ObOptColumnStatHandle> &history_col_handles)
{
  int ret = OB_SUCCESS;
  int64_t retention_val = 0;
  if (param.is_index_stat_) {
    //do nothing
  } else if (OB_FAIL(get_stats_history_retention(ctx, retention_val))) {
    LOG_WARN("failed to get stats history retention", K(ret));
  } else if (retention_val == 0) {
    /*do nothing*/
  } else if (OB_FAIL(ObDbmsStatsUtils::get_current_opt_stats(param,
                                                             history_tab_handles,
                                                             history_col_handles))) {
    LOG_WARN("failed to get current opt stats", K(ret));
  } else if (OB_FAIL(set_col_stat_cs_type(param.column_params_, history_col_handles))) {
    LOG_WARN("failed to set col stat cs type", K(ret));
  } else {
    LOG_TRACE("Succeed to get history stat handles", K(param), K(history_tab_handles.count()),
                                                     K(history_col_handles.count()));
  }
  return ret;
}

int ObDbmsStatsHistoryManager::purge_stats(ObExecContext &ctx, const int64_t specify_time)
{
  int ret = OB_SUCCESS;
  ObObj retention;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSqlString time_str;
  ObSqlString gather_time_str;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session), K(ret));
  } else if (specify_time == -1) {
    /*Versions of statistics saved before this timestamp are purged.If NULL(-1), it uses the purging
      policy used by automatic purge. The automatic purge deletes all history older than the older
      of (current time - statistics history retention) and (time of recent analyze in the system - 1
      ). The statistics history retention value can be changed using ALTER_STATS_HISTORY_RETENTION
      Procedure.The default is 31 days. ---> Oracle rule, we compatible it*/
    int64_t retention_val = 0;
    if (OB_FAIL(get_stats_history_retention(ctx, retention_val))) {
      LOG_WARN("failed to get stats history retention", K(ret));
    } else if (OB_FAIL(time_str.append_fmt("WHERE savtime < "\
                                           "date_sub(CURRENT_TIMESTAMP, interval %ld day)",
                                           retention_val))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_FAIL(gather_time_str.append_fmt("WHERE start_time < "\
                                                  "date_sub(CURRENT_TIMESTAMP, interval %ld day)",
                                                   retention_val))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else {/*do nothing*/}
  } else if (specify_time == 0) {//delete all statistics history
    if (OB_FAIL(time_str.append(" "))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(gather_time_str.append(" "))) {
      LOG_WARN("failed to append", K(ret));
    } else {/*do nothing*/}
  } else if (OB_FAIL(time_str.append_fmt("WHERE savtime < usec_to_time(%ld)", specify_time))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(gather_time_str.append_fmt("WHERE start_time < usec_to_time(%ld)", specify_time))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {/*do nothing*/}
  if (OB_FAIL(ret)) {
  } else {
    int64_t affected_rows = 0;
    uint64_t tenant_id = session->get_effective_tenant_id();
    ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
    ObSqlString del_tab_history;
    ObSqlString del_col_history;
    ObSqlString del_hist_history;
    ObSqlString del_task_opt_stat_gather_history;
    ObSqlString del_table_opt_stat_gather_history;
    ObMySQLTransaction trans;
    ObMySQLTransaction trans1;
    if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(mysql_proxy));
    } else if (OB_FAIL(del_tab_history.append_fmt(DELETE_STAT_HISTORY,
                                                  share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                                  time_str.ptr()))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(del_tab_history));
    } else if (OB_FAIL(del_col_history.append_fmt(DELETE_STAT_HISTORY,
                                                  share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                                  time_str.ptr()))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(del_col_history));
    } else if (OB_FAIL(del_hist_history.append_fmt(DELETE_STAT_HISTORY,
                                                   share::OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
                                                   time_str.ptr()))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(del_hist_history));
    } else if (OB_FAIL(del_task_opt_stat_gather_history.append_fmt(DELETE_STAT_HISTORY,
                                                                   share::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
                                                                   gather_time_str.ptr()))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(del_hist_history));
    } else if (OB_FAIL(del_table_opt_stat_gather_history.append_fmt(DELETE_STAT_HISTORY,
                                                                    share::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
                                                                    gather_time_str.ptr()))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(del_hist_history));
    } else if (OB_FAIL(trans.start(mysql_proxy, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans1.start(mysql_proxy, gen_meta_tenant_id(tenant_id)))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, del_tab_history.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(del_tab_history), K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, del_col_history.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(del_col_history), K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, del_hist_history.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(del_hist_history), K(ret));
    } else if (OB_FAIL(trans1.write(gen_meta_tenant_id(tenant_id), del_task_opt_stat_gather_history.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(del_task_opt_stat_gather_history), K(ret));
    } else if (OB_FAIL(trans1.write(gen_meta_tenant_id(tenant_id), del_table_opt_stat_gather_history.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(del_table_opt_stat_gather_history), K(ret));
    } else if (OB_FAIL(remove_useless_column_stats(trans, tenant_id))) {
      LOG_WARN("failed to remove useless column stats", K(ret));
    } else {
      LOG_TRACE("Succeed to do execute sql", K(del_tab_history), K(del_col_history),
                                             K(del_hist_history), K(del_task_opt_stat_gather_history),
                                             K(del_table_opt_stat_gather_history));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true)) || OB_FAIL(trans1.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = trans1.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::alter_stats_history_retention(ObExecContext &ctx,
                                                             const int64_t new_retention)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  int64_t affected_rows = 0;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  ObSQLSessionInfo *session = ctx.get_my_session();
  uint64_t tenant_id = 0;
  int64_t tmp_new_retention = (new_retention == -1 ? MAX_HISTORY_RETENTION : new_retention);//compatible oracle
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session) ||
      OB_UNLIKELY(tmp_new_retention < 0 || tmp_new_retention > MAX_HISTORY_RETENTION)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session), K(tmp_new_retention));
  } else if (OB_FAIL(raw_sql.append_fmt(UPDATE_STATS_HISTROY_RETENTION,
                                        share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
                                        tmp_new_retention))) {
    LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(mysql_proxy->write(session->get_effective_tenant_id(),
                                        raw_sql.ptr(),
                                        affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else if (tmp_new_retention == 0) {
    //0: Old statistics are never saved. The automatic purge will delete all statistics history
    if (OB_FAIL(purge_stats(ctx, 0))) {
      LOG_WARN("failed to purge stats", K(ret));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsHistoryManager::get_stats_history_retention_and_availability(ObExecContext &ctx,
                                                                            bool fetch_history_retention,
                                                                            ObObj &result)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session));
  } else if (fetch_history_retention &&
             OB_FAIL(raw_sql.append_fmt(FETCH_STATS_HISTROY_RETENTION,
                                        share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME))) {
    LOG_WARN("failed to append", K(ret));
  } else if (!fetch_history_retention &&
             OB_FAIL(raw_sql.append_fmt(FETCH_STATS_HISTROY_AVAILABILITY,
                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME))) {
    LOG_WARN("failed to append", K(ret));
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
            LOG_WARN("get unexpected error", K(ret));
          } else if (OB_FAIL(client_result->get_obj(idx, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(ob_write_obj(ctx.get_allocator(), tmp, result))) {
            LOG_WARN("failed to write object", K(ret));
          } else {
            is_first = false;
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

int ObDbmsStatsHistoryManager::restore_table_stats(ObExecContext &ctx,
                                                   const ObTableStatParam &param,
                                                   const int64_t specify_time)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat *, 4> all_tstats;
  ObSEArray<ObOptColumnStat *, 4> all_cstats;
  ObSEArray<ObOptTableStatHandle, 4> history_tab_handles;
  ObSEArray<ObOptColumnStatHandle, 4> history_col_handles;
  //before restore, we need record history stats.
  if (OB_FAIL(get_history_stat_handles(ctx, param, history_tab_handles, history_col_handles))) {
    LOG_WARN("failed to get history stat handles", K(ret));
  } else if (OB_FAIL(fetch_table_stat_histrory(ctx, param, specify_time, all_tstats))) {
    LOG_WARN("failed to fetch table stat histrory", K(ret));
  } else if (OB_FAIL(fetch_column_stat_history(ctx, param, specify_time, all_cstats))) {
    LOG_WARN("failed to fetch column stat history", K(ret));
  } else if (all_tstats.empty() && all_cstats.empty()) {
    //do nothing
  } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_tstats, all_cstats))) {
    LOG_WARN("failed to split batch write", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::batch_write_history_stats(ctx, history_tab_handles,
                                                                 history_col_handles))) {
    LOG_WARN("failed to batch write history stats", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsHistoryManager::fetch_table_stat_histrory(ObExecContext &ctx,
                                                         const ObTableStatParam &param,
                                                         const int64_t specify_time,
                                                         ObIArray<ObOptTableStat*> &all_part_stats)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString raw_sql;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObSqlString partition_list;
  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy()) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(param));
  } else if (OB_FAIL(gen_partition_list(param, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_TAB_STATS_HISTROY,
                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                        share::schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, param.table_id_),
                                        partition_list.ptr(),
                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                        specify_time))) {
    LOG_WARN("failed to append", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, param.tenant_id_, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObOptTableStat *stat = NULL;
          if (OB_FAIL(fill_table_stat_history(*param.allocator_, *client_result, stat))) {
            LOG_WARN("failed to fill table stat", K(ret));
          } else if (OB_ISNULL(stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(stat));
          } else if (OB_FAIL(all_part_stats.push_back(stat))) {
            LOG_WARN("failed to push back table stats", K(ret));
          } else {
            stat->set_table_id(param.table_id_);
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
          LOG_TRACE("Succeed to get stats history info", K(raw_sql), K(all_part_stats));
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

int ObDbmsStatsHistoryManager::fill_table_stat_history(ObIAllocator &allocator,
                                                       common::sqlclient::ObMySQLResult &result,
                                                       ObOptTableStat *&stat)
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  ObObjMeta obj_type;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptTableStat)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else {
    stat = new (ptr) ObOptTableStat();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, *stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, object_type, *stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, row_count, *stat, int64_t);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_type("avg_row_size", obj_type))) {
        LOG_WARN("failed to get type", K(ret));
      } else if (OB_LIKELY(obj_type.is_double())) {
        EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, avg_row_size, *stat, double);
      } else {
        EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, avg_row_size, *stat, int64_t);
      }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, macro_block_num, *stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, micro_block_num, *stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, stattype_locked, *stat, int64_t);
    if (OB_SUCCESS != (ret = result.get_timestamp("last_analyzed", NULL, int_value))) {
      LOG_WARN("fail to get column in row. ", "column_name", "last_analyzed", K(ret));
    } else {
      stat->set_last_analyzed(static_cast<int64_t>(int_value));
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::fetch_column_stat_history(ObExecContext &ctx,
                                                         const ObTableStatParam &param,
                                                         const int64_t specify_time,
                                                         ObIArray<ObOptColumnStat*> &all_cstats)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString raw_sql;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObSqlString partition_list;
  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy()) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(param));
  } else if (OB_FAIL(gen_partition_list(param, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_COL_STATS_HISTROY,
                                        share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                        share::schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, param.table_id_),
                                        partition_list.ptr(),
                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                        specify_time))) {
    LOG_WARN("failed to append", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, param.tenant_id_, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObOptColumnStat *col_stat = NULL;
          if (OB_FAIL(fill_column_stat_history(*param.allocator_, *client_result, col_stat))) {
            LOG_WARN("failed to fill table stat", K(ret));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(col_stat));
          } else if (OB_FAIL(all_cstats.push_back(col_stat))) {
            LOG_WARN("failed to push back table stats", K(ret));
          } else {
            col_stat->set_table_id(param.table_id_);
            if (OB_FAIL(set_col_stat_cs_type(param.column_params_, col_stat))) {
              LOG_WARN("failed to set col stat cs type", K(ret));
            } else if (!col_stat->get_histogram().is_valid()) {
              // do nothing
            } else if (OB_FAIL(fetch_histogram_stat_histroy(ctx, *param.allocator_,
                                                            specify_time, *col_stat))) {
              LOG_WARN("fetch histogram statistics failed", K(ret));
            } else {/*do nothing*/}
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
          LOG_TRACE("Succeed to get stats history info", K(raw_sql), K(all_cstats));
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

int ObDbmsStatsHistoryManager::fill_column_stat_history(ObIAllocator &allocator,
                                                        common::sqlclient::ObMySQLResult &result,
                                                        ObOptColumnStat *&col_stat)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptColumnStat)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else {
    col_stat = new (ptr) ObOptColumnStat();
    int64_t llc_bitmap_size = 0;
    int64_t bucket_cnt = 0;
    ObHistType histogram_type = ObHistType::INVALID_TYPE;
    ObObjMeta obj_type;
    ObHistogram &hist = col_stat->get_histogram();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, *col_stat, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, *col_stat, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, stat_level, *col_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_distinct, *col_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_null, *col_stat, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "histogram_type", histogram_type, ObHistType);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_type("sample_size", obj_type))) {
        LOG_WARN("failed to get type", K(ret));
      } else if (OB_LIKELY(obj_type.is_integer_type())) {
        EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sample_size, hist, int64_t);
      } else {
        EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, sample_size, hist, int64_t);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_type("avg_len", obj_type))) {
        LOG_WARN("failed to get type", K(ret));
      } else if (OB_LIKELY(obj_type.is_double())) {
        EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, avg_len, *col_stat, int64_t);
      } else {
        EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, avg_len, *col_stat, int64_t);
      }
    }
    EXTRACT_INT_FIELD_MYSQL(result, "bucket_cnt", bucket_cnt, int64_t);
    EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, density, hist, double);
    EXTRACT_INT_FIELD_MYSQL(result, "distinct_cnt_synopsis_size", llc_bitmap_size, int64_t);
    if (OB_SUCC(ret)) {
      hist.set_type(histogram_type);
      if (hist.is_valid() && OB_FAIL(hist.prepare_allocate_buckets(allocator, bucket_cnt))) {
        LOG_WARN("failed to prepare allocate buckets", K(ret));
      }
    }
    ObString hex_str;
    common::ObObj obj;
    if (OB_SUCC(ret)) {
      int64_t int_value = 0;
      if (OB_FAIL(result.get_timestamp("last_analyzed", NULL, int_value))) {
        LOG_WARN("failed to get last analyzed field", K(ret));
      } else {
        col_stat->set_last_analyzed(int_value);
      }
    }
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_min_value", hex_str);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptStatSqlService::hex_str_to_obj(hex_str.ptr(), hex_str.length(),
                                                      allocator, obj))) {
        LOG_WARN("failed to convert hex str to obj", K(ret));
      } else {
        col_stat->set_min_value(obj);
      }
    }
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_max_value", hex_str);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptStatSqlService::hex_str_to_obj(hex_str.ptr(), hex_str.length(),
                                                      allocator, obj))) {
        LOG_WARN("failed to convert hex str to obj", K(ret));
      } else {
        col_stat->set_max_value(obj);
      }
    }
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "distinct_cnt_synopsis", hex_str);
    char *bitmap_buf = NULL;
    if (OB_SUCC(ret) && llc_bitmap_size > 0) {
      if (NULL == (bitmap_buf = static_cast<char*>(allocator.alloc(hex_str.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate memory for llc_bitmap failed.", K(hex_str.length()), K(ret));
      } else {
        common::str_to_hex(hex_str.ptr(), hex_str.length(), bitmap_buf, hex_str.length());
        // decompress llc bitmap;
        char *decomp_buf = NULL ;
        int64_t decomp_size = ObOptColumnStat::NUM_LLC_BUCKET;
        const int64_t bitmap_size = hex_str.length() / 2;
        if (OB_FAIL(ObOptStatSqlService::get_decompressed_llc_bitmap(allocator, bitmap_buf,
                                                                     bitmap_size, decomp_buf,
                                                                     decomp_size))) {
          COMMON_LOG(WARN, "decompress bitmap buffer failed.", K(ret));
        } else {
          col_stat->set_llc_bitmap(decomp_buf, decomp_size);
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::fetch_histogram_stat_histroy(ObExecContext &ctx,
                                                            ObIAllocator &allocator,
                                                            const int64_t specify_time,
                                                            ObOptColumnStat &col_stat)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSQLSessionInfo *session = NULL;
  ObSqlString raw_sql;
  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy()) || OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session));
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
    uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(raw_sql.append_fmt(FETCH_HISTOGRAM_HISTROY,
                                   share::OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
                                   share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                   share::schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, col_stat.get_table_id()),
                                   col_stat.get_partition_id(),
                                   col_stat.get_column_id(),
                                   share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                   specify_time))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
        sqlclient::ObMySQLResult *client_result = NULL;
        const bool did_retry_weak = false;
        ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy, did_retry_weak);
        if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
          LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
        } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to execute sql", K(ret));
        } else {
          while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
            if (OB_FAIL(fill_bucket_stat_histroy(allocator, *client_result, col_stat))) {
              LOG_WARN("fill bucket stat failed", K(ret));
            } else {/*do nothing*/}
          }
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get result", K(ret));
          } else {
            ret = OB_SUCCESS;
            LOG_TRACE("Succeed to get stats history info", K(raw_sql), K(col_stat));
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
  }
  return ret;
}

int ObDbmsStatsHistoryManager::fill_bucket_stat_histroy(ObIAllocator &allocator,
                                                        sqlclient::ObMySQLResult &result,
                                                        ObOptColumnStat &stat)
{
  int ret = OB_SUCCESS;
  ObHistBucket bkt;
  ObString str;
  EXTRACT_INT_FIELD_MYSQL(result, "endpoint_num", bkt.endpoint_num_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "endpoint_repeat_cnt", bkt.endpoint_repeat_count_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_endpoint_value", str);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptStatSqlService::hex_str_to_obj(str.ptr(), str.length(),
                                                    allocator, bkt.endpoint_value_))) {
      LOG_WARN("deserialize object value failed.", K(stat), K(ret));
    } else if (OB_FAIL(stat.get_histogram().add_bucket(bkt))) {
      LOG_WARN("failed to push back buckets", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsHistoryManager::get_stats_history_retention(ObExecContext &ctx,
                                                           int64_t &retention_val)
{
  int ret = OB_SUCCESS;
  ObObj retention;
  number::ObNumber num_retention;
  if (OB_FAIL(get_stats_history_retention_and_availability(ctx, true, retention))) {
    LOG_WARN("failed to get stats history retention and availability", K(ret));
  } else if (OB_FAIL(retention.get_number(num_retention))) {
    LOG_WARN("failed to get int", K(ret), K(retention));
  } else if (OB_FAIL(num_retention.extract_valid_int64_with_trunc(retention_val))) {
    LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_retention));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsHistoryManager::set_col_stat_cs_type(
    const ObIArray<ObColumnStatParam> &column_params,
    ObIArray<ObOptColumnStatHandle> &col_handles)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_handles.count(); i++) {
    const ObOptColumnStat *&col_stat = col_handles.at(i).stat_;
    if (OB_ISNULL(col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(col_stat));
    } else if (OB_FAIL(set_col_stat_cs_type(column_params,
                                            const_cast<ObOptColumnStat*&>(col_stat)))) {
      LOG_WARN("failed to set col stat cs type", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsHistoryManager::set_col_stat_cs_type(
    const ObIArray<ObColumnStatParam> &column_params,
    ObOptColumnStat *&col_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(col_stat));
  } else {
    bool find_it = false;
    common::ObCollationType cs_type = CS_TYPE_INVALID;
    for (int64_t i = 0; !find_it && i < column_params.count(); ++i) {
      if (col_stat->get_column_id() == column_params.at(i).column_id_) {
        find_it = true;
        cs_type = column_params.at(i).cs_type_;
      }
    }
    if (!find_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(find_it), K(cs_type));
    } else {
      col_stat->set_collation_type(cs_type);
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::gen_partition_list(const ObTableStatParam &param,
                                                  ObSqlString &partition_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> partition_ids;
  if (param.global_stat_param_.need_modify_) {
    if (OB_FAIL(partition_ids.push_back(param.global_part_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(partition_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret), K(param));
      }
    }
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret), K(param));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(partition_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(param), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      char prefix = (i == 0 ? '(' : ' ');
      char suffix = (i == partition_ids.count() - 1 ? ')' : ',');
      if (OB_FAIL(partition_list.append_fmt("%c%ld%c", prefix, partition_ids.at(i), suffix))) {
        LOG_WARN("failed to append sql", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

//here we remove useless column stats, because drop table won't delete column stats.
int ObDbmsStatsHistoryManager::remove_useless_column_stats(ObMySQLTransaction &trans,
                                                           uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_col_stat_sql;
  ObSqlString delete_hist_stat_sql;
  int64_t affected_rows1 = 0;
  int64_t affected_rows2 = 0;
  if (OB_FAIL(delete_col_stat_sql.append_fmt("DELETE FROM %s c WHERE (NOT EXISTS (SELECT 1 " \
                                          "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
                                          "AND t.table_id = c.table_id AND t.tenant_id = c.tenant_id AND db.database_name != '__recyclebin')) "\
                                          "AND table_id > %ld;",
                                          share::OB_ALL_COLUMN_STAT_TNAME,
                                          share::OB_ALL_TABLE_TNAME,
                                          share::OB_ALL_DATABASE_TNAME,
                                          OB_MAX_INNER_TABLE_ID))) {
  } else if (OB_FAIL(delete_hist_stat_sql.append_fmt("DELETE FROM %s hist WHERE (NOT EXISTS (SELECT 1 " \
                                                     "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
                                                     "AND t.table_id = hist.table_id AND t.tenant_id = hist.tenant_id AND db.database_name != '__recyclebin')) "\
                                                     "AND table_id > %ld;",
                                                     share::OB_ALL_HISTOGRAM_STAT_TNAME,
                                                     share::OB_ALL_TABLE_TNAME,
                                                     share::OB_ALL_DATABASE_TNAME,
                                                     OB_MAX_INNER_TABLE_ID))) {
  } else if (OB_FAIL(trans.write(tenant_id, delete_col_stat_sql.ptr(), affected_rows1))) {
    LOG_WARN("fail to exec sql", K(delete_col_stat_sql), K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, delete_hist_stat_sql.ptr(), affected_rows2))) {
    LOG_WARN("fail to exec sql", K(delete_hist_stat_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to do execute sql", K(delete_col_stat_sql), K(delete_hist_stat_sql), K(affected_rows1), K(affected_rows2));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
