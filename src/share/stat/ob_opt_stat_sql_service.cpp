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

#define USING_LOG_PREFIX COMMON
#include "ob_opt_stat_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"
#include "lib/charset/ob_charset.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"

#define ALL_HISTOGRAM_STAT_COLUMN_NAME "tenant_id, "     \
                                       "table_id, "      \
                                       "partition_id, "  \
                                       "column_id, "     \
                                       "endpoint_num, "    \
                                       "b_endpoint_value," \
                                       "endpoint_repeat_cnt"

#define ALL_COLUMN_STAT_COLUMN_NAME  "tenant_id, "     \
                                     "table_id, "      \
                                     "partition_id, "  \
                                     "column_id, "     \
                                     "object_type as stat_level, " \
                                     "distinct_cnt as num_distinct, "  \
                                     "null_cnt as num_null,"       \
                                     "b_max_value, "     \
                                     "b_min_value,"      \
                                     "avg_len,"          \
                                     "distinct_cnt_synopsis,"     \
                                     "distinct_cnt_synopsis_size," \
                                     "histogram_type," \
                                     "sample_size,"    \
                                     "bucket_cnt,"     \
                                     "density,"        \
                                     "last_analyzed"  \

#define INSERT_TABLE_STAT_SQL "REPLACE INTO __all_table_stat(tenant_id," \
                                                               "table_id," \
                                                               "partition_id," \
                                                               "index_type," \
                                                               "object_type," \
                                                               "last_analyzed," \
                                                               "sstable_row_cnt," \
                                                               "sstable_avg_row_len," \
                                                               "macro_blk_cnt," \
                                                               "micro_blk_cnt," \
                                                               "memtable_row_cnt," \
                                                               "memtable_avg_row_len," \
                                                               "row_cnt," \
                                                               "avg_row_len," \
                                                               "global_stats," \
                                                               "user_stats," \
                                                               "stattype_locked," \
                                                               "stale_stats," \
                                                               "spare1) VALUES " \

#define REPLACE_COL_STAT_SQL "REPLACE INTO __all_column_stat(tenant_id," \
                                                              "table_id," \
                                                              "partition_id," \
                                                              "column_id," \
                                                              "object_type," \
                                                              "last_analyzed," \
                                                              "distinct_cnt," \
                                                              "null_cnt," \
                                                              "max_value," \
                                                              "b_max_value," \
                                                              "min_value," \
                                                              "b_min_value," \
                                                              "avg_len," \
                                                              "distinct_cnt_synopsis," \
                                                              "distinct_cnt_synopsis_size," \
                                                              "sample_size,"\
                                                              "density,"\
                                                              "bucket_cnt," \
                                                              "histogram_type," \
                                                              "global_stats," \
                                                              "user_stats,"\
                                                              "spare1%s) VALUES "


#define INSERT_HISTOGRAM_STAT_SQL "INSERT INTO __all_histogram_stat(tenant_id," \
                                                                      "table_id," \
                                                                      "partition_id," \
                                                                      "column_id," \
                                                                      "object_type," \
                                                                      "endpoint_num," \
                                                                      "endpoint_normalized_value," \
                                                                      "endpoint_value," \
                                                                      "b_endpoint_value," \
                                                                      "endpoint_repeat_cnt) VALUES "

#define DELETE_HISTOGRAM_STAT_SQL "DELETE /*+%.*s*/  FROM __all_histogram_stat WHERE %.*s"
#define DELETE_COL_STAT_SQL "DELETE /*+%.*s*/ FROM __all_column_stat WHERE %.*s"
#define DELETE_TAB_STAT_SQL "DELETE /*+%.*s*/ FROM __all_table_stat WHERE %.*s"
#define UPDATE_HISTOGRAM_TYPE_SQL "UPDATE /*+%.*s*/ __all_column_stat SET histogram_type = 0, bucket_cnt = 0 WHERE %.*s"

// not used yet.
#define INSERT_ONLINE_TABLE_STAT_SQL "INSERT INTO oceanbase.__all_table_stat(tenant_id," \
                                                               "table_id," \
                                                               "partition_id," \
                                                               "index_type," \
                                                               "object_type," \
                                                               "last_analyzed," \
                                                               "sstable_row_cnt," \
                                                               "sstable_avg_row_len," \
                                                               "macro_blk_cnt," \
                                                               "micro_blk_cnt," \
                                                               "memtable_row_cnt," \
                                                               "memtable_avg_row_len," \
                                                               "row_cnt," \
                                                               "avg_row_len," \
                                                               "global_stats," \
                                                               "user_stats," \
                                                               "stattype_locked," \
                                                               "stale_stats) VALUES " \

#define INSERT_ONLINE_TABLE_STAT_DUPLICATE "ON DUPLICATE KEY UPDATE " \
                                           "index_type = index_type," \
                                           "object_type = object_type," \
                                           "last_analyzed = VALUES(last_analyzed)," \
                                           "sstable_row_cnt = VALUES(sstable_row_cnt)," \
                                           "sstable_avg_row_len = VALUES(sstable_avg_row_len)," \
                                           "macro_blk_cnt = VALUES(macro_blk_cnt)," \
                                           "micro_blk_cnt = VALUES(micro_blk_cnt)," \
                                           "memtable_row_cnt = VALUES(memtable_row_cnt)," \
                                           "memtable_avg_row_len = VALUES(memtable_avg_row_len)," \
                                           "row_cnt = row_cnt + VALUES(row_cnt)," \
                                           "avg_row_len = (avg_row_len*row_cnt + VALUES(avg_row_len)*VALUES(row_cnt)) / (row_cnt+ VALUES(row_cnt))," \
                                           "global_stats = VALUES(global_stats)," \
                                           "user_stats = VALUES(user_stats)," \
                                           "stattype_locked = VALUES(stattype_locked)," \
                                           "stale_stats = VALUES(stale_stats)"
//TODO DAISI, MICRO/MACRO/MEMTABLE/SSTABLE
//TODO DAISI, check lock.

#define INSERT_ONLINE_COL_STAT_SQL "INSERT INTO __all_column_stat(tenant_id," \
                                                                  "table_id," \
                                                                  "partition_id," \
                                                                  "column_id," \
                                                                  "object_type," \
                                                                  "last_analyzed," \
                                                                  "distinct_cnt," \
                                                                  "null_cnt," \
                                                                  "max_value," \
                                                                  "b_max_value," \
                                                                  "min_value," \
                                                                  "b_min_value," \
                                                                  "avg_len," \
                                                                  "distinct_cnt_synopsis," \
                                                                  "distinct_cnt_synopsis_size," \
                                                                  "sample_size,"\
                                                                  "density,"\
                                                                  "bucket_cnt," \
                                                                  "histogram_type," \
                                                                  "global_stats," \
                                                                  "user_stats) VALUES "

#define INSERT_ONLINE_COL_STAT_DUPLICATE "ON DUPLICATE KEY UPDATE " \
                                         "object_type = object_type," \
                                         "last_analyzed = VALUES(last_analyzed)," \
                                         "distinct_cnt = VALUES(distinct_cnt) + distinct_cnt," \
                                         "distinct_cnt_synopsis = VALUES(distinct_cnt_synopsis)," \
                                         "distinct_cnt_synopsis_size = VALUES(distinct_cnt_synopsis_size)," \
                                         "null_cnt = VALUES(null_cnt)," \
                                         "max_value = VALUES(max_value)," \
                                         "b_max_value = VALUES(b_max_value)," \
                                         "min_value = VALUES(min_value)," \
                                         "b_min_value = VALUES(b_min_value)," \
                                         "global_stats = VALUES(global_stats)," \
                                         "user_stats = VALUES(user_stats);"
// TODO DAISI, add a sys_func to merge NDV by llc.

#define INSERT_TASK_OPT_STAT_GATHER_SQL "INSERT INTO %s(tenant_id," \
                                                        "task_id," \
                                                        "type," \
                                                        "ret_code," \
                                                        "failed_count,"\
                                                        "table_count," \
                                                        "start_time," \
                                                        "end_time) VALUES (%s);"

#define INSERT_TABLE_OPT_STAT_GATHER_SQL "INSERT INTO %s(tenant_id," \
                                                         "task_id," \
                                                         "table_id," \
                                                         "ret_code," \
                                                         "start_time," \
                                                         "end_time," \
                                                         "memory_used," \
                                                         "stat_refresh_failed_list," \
                                                         "properties) VALUES (%s);"


#define ALL_HISTOGRAM_STAT_COLUMN_NAME "tenant_id, "     \
                                       "table_id, "      \
                                       "partition_id, "  \
                                       "column_id, "     \
                                       "endpoint_num, "    \
                                       "b_endpoint_value," \
                                       "endpoint_repeat_cnt"

#define FETCH_ALL_COLUMN_STAT_SQL   "SELECT col_stat.tenant_id as tenant_id, "     \
                                            "col_stat.table_id as table_id, "      \
                                            "col_stat.partition_id as partition_id, "  \
                                            "col_stat.column_id as column_id, "     \
                                            "col_stat.object_type as stat_level, " \
                                            "col_stat.distinct_cnt as num_distinct, "  \
                                            "col_stat.null_cnt as num_null,"       \
                                            "col_stat.b_max_value as b_max_value, "     \
                                            "col_stat.b_min_value as b_min_value,"      \
                                            "col_stat.avg_len as avg_len,"          \
                                            "col_stat.distinct_cnt_synopsis as distinct_cnt_synopsis,"     \
                                            "col_stat.distinct_cnt_synopsis_size as distinct_cnt_synopsis_size," \
                                            "col_stat.histogram_type as histogram_type," \
                                            "col_stat.sample_size as sample_size,"    \
                                            "col_stat.bucket_cnt as bucket_cnt,"     \
                                            "col_stat.density as density,"        \
                                            "col_stat.last_analyzed as last_analyzed,"\
                                            "col_stat.spare1 as compress_type,"\
                                            "hist_stat.endpoint_num as endpoint_num, "    \
                                            "hist_stat.b_endpoint_value as b_endpoint_value," \
                                            "hist_stat.endpoint_repeat_cnt as endpoint_repeat_cnt "\
                                            "FROM %s col_stat LEFT JOIN %s hist_stat "\
                                            "ON col_stat.tenant_id = hist_stat.tenant_id AND "\
                                            "   col_stat.table_id = hist_stat.table_id AND "\
                                            "   col_stat.partition_id = hist_stat.partition_id AND "\
                                            "   col_stat.column_id = hist_stat.column_id "\
                                            "WHERE %.*s "\
                                            "ORDER BY tenant_id, table_id, partition_id, column_id, endpoint_num;"

#define FETCH_ALL_COLUMN_STAT_SQL_COL   "SELECT col_stat.tenant_id as tenant_id, "     \
                                            "col_stat.table_id as table_id, "      \
                                            "col_stat.partition_id as partition_id, "  \
                                            "col_stat.column_id as column_id, "     \
                                            "col_stat.object_type as stat_level, " \
                                            "col_stat.distinct_cnt as num_distinct, "  \
                                            "col_stat.null_cnt as num_null,"       \
                                            "col_stat.b_max_value as b_max_value, "     \
                                            "col_stat.b_min_value as b_min_value,"      \
                                            "col_stat.avg_len as avg_len,"          \
                                            "col_stat.distinct_cnt_synopsis as distinct_cnt_synopsis,"     \
                                            "col_stat.distinct_cnt_synopsis_size as distinct_cnt_synopsis_size," \
                                            "col_stat.histogram_type as histogram_type," \
                                            "col_stat.sample_size as sample_size,"    \
                                            "col_stat.bucket_cnt as bucket_cnt,"     \
                                            "col_stat.density as density,"        \
                                            "col_stat.last_analyzed as last_analyzed,"\
                                            "col_stat.spare1 as compress_type,"\
                                            "col_stat.cg_macro_blk_cnt as cg_macro_blk_cnt,"\
                                            "col_stat.cg_micro_blk_cnt as cg_micro_blk_cnt,"\
                                            "hist_stat.endpoint_num as endpoint_num, "    \
                                            "hist_stat.b_endpoint_value as b_endpoint_value," \
                                            "hist_stat.endpoint_repeat_cnt as endpoint_repeat_cnt "\
                                            "FROM %s col_stat LEFT JOIN %s hist_stat "\
                                            "ON col_stat.tenant_id = hist_stat.tenant_id AND "\
                                            "   col_stat.table_id = hist_stat.table_id AND "\
                                            "   col_stat.partition_id = hist_stat.partition_id AND "\
                                            "   col_stat.column_id = hist_stat.column_id "\
                                            "WHERE %.*s "\
                                            "ORDER BY tenant_id, table_id, partition_id, column_id, endpoint_num;"

#define INSERT_SYSTEM_STAT_SQL "REPLACE INTO %s(tenant_id," \
                                                "last_analyzed," \
                                                "cpu_speed," \
                                                "disk_seq_read_speed," \
                                                "disk_rnd_read_speed," \
                                                "network_speed) VALUES "

#define DELETE_SYSTEM_STAT_SQL "DELETE FROM %s WHERE TENANT_ID=%ld"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace common
{

ObOptStatSqlService::ObOptStatSqlService()
    : inited_(false), mysql_proxy_(nullptr), mutex_(ObLatchIds::DEFAULT_MUTEX), config_(nullptr)
{
}

ObOptStatSqlService::~ObOptStatSqlService()
{
}

int ObOptStatSqlService::init(ObMySQLProxy *proxy, ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (NULL == proxy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client proxy is null", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sql service have already been initialized.", K(ret));
  } else {
    mysql_proxy_ = proxy;
    config_ = config;
    inited_ = true;
  }
  return ret;
}

int ObOptStatSqlService::fetch_table_stat(const uint64_t tenant_id,
                                          const ObOptTableStat::Key &key,
                                          ObIArray<ObOptTableStat> &all_part_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat stat;
  stat.set_table_id(key.get_table_id());
  ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql service has not been initialized.", K(ret));
    } else if (OB_FAIL(sql.append_fmt("SELECT partition_id, "
                                      "object_type, "
                                      "row_cnt as row_count, "
                                      "avg_row_len as avg_row_size, "
                                      "macro_blk_cnt as macro_block_num, "
                                      "micro_blk_cnt as micro_block_num, "
                                      "stattype_locked as stattype_locked,"
                                      "last_analyzed FROM %s ", share::OB_ALL_TABLE_STAT_TNAME))) {
      LOG_WARN("fail to append SQL stmt string.", K(sql), K(ret));
    } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld",
                                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_)))) {
      LOG_WARN("fail to append SQL where string.", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        } else if (all_part_stats.empty()) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(fill_table_stat(*result, stat))) {
        LOG_WARN("failed to fill table stat", K(ret));
      } else if (OB_FAIL(all_part_stats.push_back(stat))) {
        LOG_WARN("failed to push back table stats", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::batch_fetch_table_stats(sqlclient::ObISQLConnection *conn,
                                                 const uint64_t tenant_id,
                                                 const uint64_t table_id,
                                                 const ObIArray<int64_t> &part_ids,
                                                 ObIArray<ObOptTableStat*> &all_part_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      ObSqlString part_list;
      ObSqlString part_str;
      uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql service has not been initialized.", K(ret));
      } else if (OB_FAIL(sql.append_fmt("SELECT partition_id, "
                                        "object_type, "
                                        "row_cnt as row_count, "
                                        "avg_row_len as avg_row_size, "
                                        "macro_blk_cnt as macro_block_num, "
                                        "micro_blk_cnt as micro_block_num, "
                                        "stattype_locked as stattype_locked,"
                                        "last_analyzed FROM %s", share::OB_ALL_TABLE_STAT_TNAME))) {
        LOG_WARN("fail to append SQL stmt string.", K(sql), K(ret));
      } else if (OB_FAIL(generate_in_list(part_ids, part_list))) {
        LOG_WARN("failed to generate in list", K(ret));
      } else if (!part_list.empty() &&
                 OB_FAIL(part_str.append_fmt(" AND partition_id in %s", part_list.ptr()))) {
        LOG_WARN("fail to append partition string.", K(ret));
      } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld %s",
                                        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                                        !part_str.empty() ? part_str.ptr() : " "))) {
        LOG_WARN("fail to append SQL where string.", K(ret));
      } else if (OB_FAIL(conn->execute_read(exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      }
      while (OB_SUCC(ret)) {
        ObOptTableStat stat;
        stat.set_table_id(table_id);
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(fill_table_stat(*result, stat))) {
          LOG_WARN("failed to fill table stat", K(ret));
        } else {
          bool found_it = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < all_part_stats.count(); ++i) {
            if (OB_ISNULL(all_part_stats.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(ret));
            } else if (all_part_stats.at(i)->get_table_id() == stat.get_table_id() &&
                      all_part_stats.at(i)->get_partition_id() == stat.get_partition_id()) {
              found_it = true;
              *all_part_stats.at(i) = stat;
            }
          }
          if (OB_SUCC(ret) && !found_it) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(all_part_stats), K(stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::update_table_stat(const uint64_t tenant_id,
                                           sqlclient::ObISQLConnection *conn,
                                           const ObOptTableStat *table_stat,
                                           const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  ObSqlString table_stat_sql;
  ObSqlString tmp;
  int64_t current_time = ObTimeUtility::current_time();
  int64_t affected_rows = 0;
  if (OB_ISNULL(table_stat) || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat or conn is null", K(ret), K(table_stat), K(conn));
  } else if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(get_table_stat_sql(tenant_id, *table_stat, current_time, is_index_stat, tmp))) {
    LOG_WARN("failed to get table stat sql", K(ret));
  } else if (OB_FAIL(table_stat_sql.append_fmt("(%s);", tmp.ptr()))) {
    LOG_WARN("failed to append table stat sql", K(ret));
  } else if (OB_FAIL(conn->execute_write(tenant_id, table_stat_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatSqlService::update_table_stat(const uint64_t tenant_id,
                                           sqlclient::ObISQLConnection *conn,
                                           const common::ObIArray<ObOptTableStat *> &table_stats,
                                           const int64_t current_time,
                                           const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  ObSqlString table_stat_sql;
  ObSqlString tmp;
  int64_t affected_rows = 0;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is is null", K(ret), K(conn));
  } else if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    bool is_last = (i == table_stats.count() - 1);
    tmp.reset();
    if (OB_ISNULL(table_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table stat is null", K(ret));
    } else if (OB_FAIL(get_table_stat_sql(tenant_id, *table_stats.at(i), current_time, is_index_stat, tmp))) {
      LOG_WARN("failed to get table stat sql", K(ret));
    } else if (OB_FAIL(table_stat_sql.append_fmt("(%s)%c",tmp.ptr(), (is_last? ';' : ',')))) {
      LOG_WARN("failed to append table stat sql", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("sql string of table stat update", K(table_stat_sql));
    if (OB_FAIL(conn->execute_write(tenant_id, table_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to write", K(ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::update_column_stat(share::schema::ObSchemaGetterGuard *schema_guard,
                                            const uint64_t exec_tenant_id,
                                            ObIAllocator &allocator,
                                            sqlclient::ObISQLConnection *conn,
                                            const ObIArray<ObOptColumnStat*> &column_stats,
                                            const int64_t current_time,
                                            bool only_update_col_stat /*default false*/,
                                            const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString insert_histogram;
  ObSqlString delete_histogram;
  ObSqlString column_stats_sql;
  bool need_histogram = false;
  if (!inited_ || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(conn), K(inited_));
  } else if (OB_UNLIKELY(column_stats.empty()) || OB_ISNULL(column_stats.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column stats is empty", K(ret));
  // construct column stat sql
  } else if (OB_FAIL(construct_column_stat_sql(schema_guard,
                                               exec_tenant_id,
                                               allocator,
                                               column_stats,
                                               current_time,
                                               column_stats_sql,
                                               print_params))) {
    LOG_WARN("failed to construct column stat sql", K(ret));
  // construct histogram delete column
  } else if (!only_update_col_stat &&
             construct_delete_column_histogram_sql(exec_tenant_id, column_stats, delete_histogram)) {
    LOG_WARN("failed to construc delete column histogram sql", K(ret));
  // construct histogram insert sql
  } else if (!only_update_col_stat &&
             OB_FAIL(construct_histogram_insert_sql(schema_guard,
                                                    exec_tenant_id,
                                                    allocator,
                                                    column_stats,
                                                    current_time,
                                                    insert_histogram,
                                                    need_histogram,
                                                    print_params))) {
    LOG_WARN("failed to construct histogram insert sql", K(ret));
  } else if (!only_update_col_stat &&
             OB_FAIL(conn->execute_write(exec_tenant_id, delete_histogram.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(delete_histogram));
  } else if (OB_FAIL(need_histogram &&
              conn->execute_write(exec_tenant_id, insert_histogram.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(insert_histogram));
  } else if (OB_FAIL(conn->execute_write(exec_tenant_id, column_stats_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(column_stats_sql));
  }
  return ret;
}

int ObOptStatSqlService::construct_column_stat_sql(share::schema::ObSchemaGetterGuard *schema_guard,
                                                   const uint64_t tenant_id,
                                                   ObIAllocator &allocator,
                                                   const ObIArray<ObOptColumnStat*> &column_stats,
                                                   const int64_t current_time,
                                                   ObSqlString &column_stats_sql,
                                                   const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  ObObjMeta min_meta;
  ObObjMeta max_meta;
  uint64_t data_version = 0;
  if (OB_FAIL(get_column_stat_min_max_meta(schema_guard, tenant_id,
                                           share::OB_ALL_COLUMN_STAT_TID,
                                           min_meta,
                                           max_meta))) {
    LOG_WARN("failed to get column stat min max meta", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
    tmp.reset();
    if (OB_ISNULL(column_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stat is null", K(ret));
    } else if (i == 0 && OB_FAIL(column_stats_sql.append_fmt(REPLACE_COL_STAT_SQL,
                                                             data_version < DATA_VERSION_4_3_0_0 ? " " : ",cg_macro_blk_cnt, cg_micro_blk_cnt"))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(get_column_stat_sql(tenant_id, allocator,
                                           *column_stats.at(i), current_time,
                                           min_meta, max_meta, tmp, print_params))) {
      LOG_WARN("failed to get column stat", K(ret), K(*column_stats.at(i)));
    } else if (OB_FAIL(column_stats_sql.append_fmt("(%s)%s", tmp.ptr(),
                                                    (i == column_stats.count() - 1 ? ";" : ",")))) {
      LOG_WARN("failed to append sql", K(ret));
    } else {/*do nothing*/}
  }
  LOG_TRACE("Succeed to construct column stat sql", K(column_stats_sql));
  return ret;
}

int ObOptStatSqlService::construct_delete_column_histogram_sql(const uint64_t tenant_id,
                                                               const ObIArray<ObOptColumnStat*> &column_stats,
                                                               ObSqlString &delete_histogram_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString where_str;
  ObSqlString hint_str;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
    if (OB_ISNULL(column_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(column_stats.at(i)));
    } else if (where_str.append_fmt(" %s (%lu, %ld, %ld, %lu) %s",
                                     i != 0 ? "," : "(TENANT_ID, TABLE_ID, PARTITION_ID, COLUMN_ID) IN (",
                                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_stats.at(i)->get_table_id()),
                                     column_stats.at(i)->get_partition_id(),
                                     column_stats.at(i)->get_column_id(),
                                     i == column_stats.count() - 1 ? ")" : "")) {
      LOG_WARN("failed to append fmt", K(ret));
    }
  }
  if (OB_SUCC(ret) && !where_str.empty()) {
    if (OB_FAIL(hint_str.append("opt_param('enable_in_range_optimization','true')"))) {
       LOG_WARN("fail to append hint", K(ret));
    } else if (OB_FAIL(delete_histogram_sql.append_fmt(DELETE_HISTOGRAM_STAT_SQL,
                                                hint_str.string().length(),
                                                hint_str.string().ptr(),
                                                where_str.string().length(),
                                                where_str.string().ptr()))) {
      LOG_WARN("fail to append SQL where string.", K(ret));
    } else {
      LOG_TRACE("Succeed to construct delete column histogram sql", K(delete_histogram_sql));
    }
  }
  return ret;
}

int ObOptStatSqlService::construct_histogram_insert_sql(share::schema::ObSchemaGetterGuard *schema_guard,
                                                        const uint64_t tenant_id,
                                                        ObIAllocator &allocator,
                                                        const ObIArray<ObOptColumnStat*> &column_stats,
                                                        const int64_t current_time,
                                                        ObSqlString &insert_histogram_sql,
                                                        bool &need_histogram,
                                                        const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  need_histogram = false;
  ObObjMeta endpoint_meta;
  if (OB_FAIL(get_histogram_endpoint_meta(schema_guard, tenant_id,
                                          share::OB_ALL_HISTOGRAM_STAT_TID,
                                          endpoint_meta))) {
    LOG_WARN("failed to get histogram endpoint meta", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
    if (OB_ISNULL(column_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(column_stats.at(i)));
    } else {
      ObHistogram &hist = column_stats.at(i)->get_histogram();
      for (int64_t j = 0; OB_SUCC(ret) && hist.is_valid() && j < hist.get_bucket_size(); ++j) {
        tmp.reset();
        if (!need_histogram && OB_FAIL(insert_histogram_sql.append(INSERT_HISTOGRAM_STAT_SQL))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (OB_FAIL(get_histogram_stat_sql(tenant_id, *column_stats.at(i),
                                                  allocator, hist.get(j), endpoint_meta, tmp, print_params))) {
          LOG_WARN("failed to get histogram sql", K(ret));
        } else if (OB_FAIL(insert_histogram_sql.append_fmt("%s (%s)", (!need_histogram ? "" : ","), tmp.ptr()))) {
          LOG_WARN("failed to append sql", K(ret));
        } else {
          need_histogram = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_histogram) {
    if (OB_FAIL(insert_histogram_sql.append(";"))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      LOG_TRACE("Succeed to construct histogram insert sql", K(insert_histogram_sql));
    }
  }
  return ret;
}

int ObOptStatSqlService::delete_table_stat(const uint64_t exec_tenant_id,
                                           const uint64_t table_id,
                                           const ObIArray<int64_t> &part_ids,
                                           const bool cascade_column,
                                           int64_t degree,
                                           int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString delete_cstat;
  ObSqlString delete_tstat;
  ObSqlString delete_hist;
  ObSqlString in_list;
  ObSqlString where_str;
  ObSqlString hint_str;
  bool has_part = !part_ids.empty();
  int64_t tmp_affected_rows1 = 0;
  int64_t tmp_affected_rows2 = 0;
  affected_rows = 0;
  if (!inited_) {
     ret = OB_NOT_INIT;
     LOG_WARN("sql service not inited", K(ret));
  } else if (OB_FAIL(generate_in_list(part_ids, in_list))) {
    LOG_WARN("failed to generate in list", K(ret));
  } else if (degree > 1 &&
            OB_FAIL(hint_str.append_fmt(
              "ENABLE_PARALLEL_DML parallel(%ld)",
              degree
            ))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(where_str.append_fmt(
                      "tenant_id = %lu and table_id = %ld %s%s;",
                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                      has_part ? "AND partition_id in " : "",
                      has_part ? in_list.ptr() : ""))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_tstat.append_fmt(
                    DELETE_TAB_STAT_SQL,
                    hint_str.string().length(),
                    hint_str.string().ptr(),
                    where_str.string().length(),
                    where_str.string().ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (!cascade_column) {
    // do nothing
  } else if (OB_FAIL(delete_cstat.append_fmt(
                    DELETE_COL_STAT_SQL,
                    hint_str.string().length(),
                    hint_str.string().ptr(),
                    where_str.string().length(),
                    where_str.string().ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(hint_str.append("opt_param('enable_in_range_optimization','true')"))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_hist.append_fmt(
                    DELETE_HISTOGRAM_STAT_SQL,
                    hint_str.string().length(),
                    hint_str.string().ptr(),
                    where_str.string().length(),
                    where_str.string().ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.start(mysql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans.write(exec_tenant_id, delete_tstat.ptr(), tmp_affected_rows1))) {
      LOG_WARN("fail to exec sql", K(delete_tstat), K(ret));
    } else {
      affected_rows += tmp_affected_rows1;
      tmp_affected_rows1 = 0;
    }
    if (OB_SUCC(ret)) {
      if (!cascade_column) {
        // do nothing
      } else if (OB_FAIL(trans.write(exec_tenant_id, delete_cstat.ptr(), tmp_affected_rows1))) {
        LOG_WARN("failed to exec sql", K(delete_cstat), K(ret));
      } else if (OB_FAIL(trans.write(exec_tenant_id, delete_hist.ptr(), tmp_affected_rows2))) {
        LOG_WARN("failed to delete histogram", K(delete_hist), K(ret));
      } else {
        affected_rows += tmp_affected_rows1 + tmp_affected_rows2;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::delete_column_stat(const uint64_t exec_tenant_id,
                                            const uint64_t table_id,
                                            const ObIArray<uint64_t> &column_ids,
                                            const ObIArray<int64_t> &partition_ids,
                                            const bool only_histogram /*=false*/,
                                            const int64_t degree)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  ObSqlString write_cstat;
  ObSqlString delete_histogram;
  ObSqlString partition_list;
  ObSqlString column_list;
  ObSqlString hint_str;
  ObSqlString where_str;
  bool has_part = !partition_ids.empty();
  if (!inited_) {
     ret = OB_NOT_INIT;
     LOG_WARN("sql service not inited", K(ret));
  } else if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(column_ids.empty()));
  } else if (OB_FAIL(generate_in_list(partition_ids, partition_list))) {
    LOG_WARN("failed to generate in list", K(ret));
  } else if (OB_FAIL(generate_in_list(column_ids, column_list))) {
    LOG_WARN("failed to generate in list", K(ret));
  } else if (degree > 1 &&
            OB_FAIL(hint_str.append_fmt("ENABLE_PARALLEL_DML parallel(%ld)",degree))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(where_str.append_fmt(
                      "tenant_id = %lu and table_id = %ld and column_id in %s %s%s;",
                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                      column_list.ptr(),
                      has_part ? "AND partition_id in " : "",
                      has_part ? partition_list.ptr() : ""))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(hint_str.append_fmt("opt_param('enable_in_range_optimization','true')"))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_histogram.append_fmt(
                       DELETE_HISTOGRAM_STAT_SQL,
                       hint_str.string().length(),
                       hint_str.string().ptr(),
                       where_str.string().length(),
                       where_str.string().ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (FALSE_IT(hint_str.reuse())) {
  } else if (degree > 1 &&
            OB_FAIL(hint_str.append_fmt("ENABLE_PARALLEL_DML parallel(%ld)",degree))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(write_cstat.append_fmt(
                    (only_histogram ? UPDATE_HISTOGRAM_TYPE_SQL :  DELETE_COL_STAT_SQL),
                    hint_str.string().length(),
                    hint_str.string().ptr(),
                    where_str.string().length(),
                    where_str.string().ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("sql string of stat update", K(write_cstat), K(delete_histogram));
    if (OB_FAIL(trans.start(mysql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans.write(exec_tenant_id, delete_histogram.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(delete_histogram), K(ret));
    } else if (OB_FAIL(trans.write(exec_tenant_id, write_cstat.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(write_cstat), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::get_table_stat_sql(const uint64_t tenant_id,
                                            const ObOptTableStat &stat,
                                            const int64_t current_time,
                                            const bool is_index_stat,
                                            ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = stat.get_table_id();
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
      OB_FAIL(dml_splicer.add_column("index_type", is_index_stat)) ||
      OB_FAIL(dml_splicer.add_column("object_type", stat.get_object_type())) ||
      OB_FAIL(dml_splicer.add_time_column("last_analyzed", stat.get_last_analyzed() == 0 ?
                                                        current_time : stat.get_last_analyzed())) ||
      OB_FAIL(dml_splicer.add_column("sstable_row_count", stat.get_sstable_row_count())) ||
      OB_FAIL(dml_splicer.add_column("sstable_avg_row_len", -1)) ||
      OB_FAIL(dml_splicer.add_column("macro_blk_cnt", stat.get_macro_block_num())) ||
      OB_FAIL(dml_splicer.add_column("micro_blk_cnt", stat.get_micro_block_num())) ||
      OB_FAIL(dml_splicer.add_column("memtable_row_cnt", stat.get_memtable_row_count())) ||
      OB_FAIL(dml_splicer.add_column("memtable_avg_row_len", -1)) ||
      OB_FAIL(dml_splicer.add_column("row_cnt", stat.get_row_count())) ||
      OB_FAIL(dml_splicer.add_column("avg_row_len", stat.get_avg_row_size())) ||
      OB_FAIL(dml_splicer.add_column("global_stats", 0)) ||
      OB_FAIL(dml_splicer.add_column("user_stats", 0)) ||
      OB_FAIL(dml_splicer.add_column("stattype_locked", stat.get_stattype_locked())) ||
      OB_FAIL(dml_splicer.add_column("stale_stats", 0)) ||
      OB_FAIL(dml_splicer.add_column("spare1", stat.get_sample_size()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObOptStatSqlService::get_column_stat_sql(const uint64_t tenant_id,
                                             ObIAllocator &allocator,
                                             const ObOptColumnStat &stat,
                                             const int64_t current_time,
                                             ObObjMeta min_meta,
                                             ObObjMeta max_meta,
                                             ObSqlString &sql_string,
                                             const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  ObString min_str, b_min_str;
  ObString max_str, b_max_str;
  uint64_t table_id = stat.get_table_id();
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  char *llc_comp_buf = NULL;
  char *llc_hex_buf = NULL;
  int64_t llc_comp_size = 0;
  int64_t llc_hex_size = 0;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (OB_UNLIKELY((ObHistType::INVALID_TYPE != stat.get_histogram().get_type() &&
                          stat.get_histogram().get_bucket_cnt() == 0) ||
                         stat.get_num_distinct() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(stat));
  } else if (OB_FAIL(get_valid_obj_str(stat.get_min_value(), min_meta, allocator, min_str, print_params)) ||
             OB_FAIL(get_valid_obj_str(stat.get_max_value(), max_meta, allocator, max_str, print_params))) {
    LOG_WARN("failed to get valid obj str", K(stat.get_min_value()), K(stat.get_max_value()));
  } else if (OB_FAIL(get_obj_binary_hex_str(stat.get_min_value(), allocator, b_min_str)) ||
             OB_FAIL(get_obj_binary_hex_str(stat.get_max_value(), allocator, b_max_str))) {
    LOG_WARN("failed to convert obj to str", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (stat.get_llc_bitmap_size() <= 0) {
    // do nothing
  } else if (OB_FAIL(get_compressed_llc_bitmap(allocator,
                                               data_version < DATA_VERSION_4_2_2_0 ? bitmap_compress_lib_name[ObOptStatCompressType::ZLIB_COMPRESS] :
                                                                                     bitmap_compress_lib_name[ObOptStatCompressType::ZSTD_1_3_8_COMPRESS],
                                               stat.get_llc_bitmap(),
                                               stat.get_llc_bitmap_size(),
                                               llc_comp_buf,
                                               llc_comp_size))) {
    LOG_WARN("failed to get compressed llc bit map", K(ret));
  } else if (FALSE_IT(llc_hex_size = llc_comp_size * 2 + 2)){
    // 1 bytes are reprensented by 2 hex char (2 bytes)
    // 1 bytes for '\0', and 1 bytes just safe
  } else if (OB_ISNULL(llc_hex_buf = static_cast<char*>(allocator.alloc(llc_hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(llc_hex_buf), K(llc_hex_size));
  } else if (OB_FAIL(common::to_hex_cstr(llc_comp_buf, llc_comp_size, llc_hex_buf, llc_hex_size))) {
    LOG_WARN("failed to convert to hex cstr", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
        OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
        OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
        OB_FAIL(dml_splicer.add_pk_column("column_id", stat.get_column_id())) ||
        OB_FAIL(dml_splicer.add_column("object_type", stat.get_stat_level())) ||
        OB_FAIL(dml_splicer.add_time_column("last_analyzed", stat.get_last_analyzed() == 0 ?
                                                        current_time : stat.get_last_analyzed())) ||
        OB_FAIL(dml_splicer.add_column("distinct_cnt", stat.get_num_distinct())) ||
        OB_FAIL(dml_splicer.add_column("null_cnt", stat.get_num_null())) ||
        OB_FAIL(dml_splicer.add_column("max_value", ObHexEscapeSqlStr(max_str))) ||
        OB_FAIL(dml_splicer.add_column("b_max_value", b_max_str)) ||
        OB_FAIL(dml_splicer.add_column("min_value", ObHexEscapeSqlStr(min_str))) ||
        OB_FAIL(dml_splicer.add_column("b_min_value", b_min_str)) ||
        OB_FAIL(dml_splicer.add_column("avg_len", stat.get_avg_len())) ||
        OB_FAIL(dml_splicer.add_column("distinct_cnt_synopsis", llc_hex_buf == NULL ? "" : llc_hex_buf)) ||
        OB_FAIL(dml_splicer.add_column("distinct_cnt_synopsis_size", llc_comp_size * 2)) ||
        OB_FAIL(dml_splicer.add_column("sample_size", stat.get_histogram().get_sample_size())) ||
        OB_FAIL(dml_splicer.add_long_double_column("density", stat.get_histogram().get_density())) ||
        OB_FAIL(dml_splicer.add_column("bucket_cnt", stat.get_histogram().get_bucket_cnt())) ||
        OB_FAIL(dml_splicer.add_column("histogram_type", stat.get_histogram().get_type())) ||
        OB_FAIL(dml_splicer.add_column("global_stats", 0)) ||
        OB_FAIL(dml_splicer.add_column("user_stats", 0)) ||
        OB_FAIL(dml_splicer.add_column("spare1", data_version < DATA_VERSION_4_2_2_0 ? ObOptStatCompressType::ZLIB_COMPRESS :
                                                                                       ObOptStatCompressType::ZSTD_1_3_8_COMPRESS)) ||
        (data_version >= DATA_VERSION_4_3_0_0 &&
         OB_FAIL(dml_splicer.add_column("cg_macro_blk_cnt", stat.get_cg_macro_blk_cnt()))) ||
        (data_version >= DATA_VERSION_4_3_0_0 &&
         OB_FAIL(dml_splicer.add_column("cg_micro_blk_cnt", stat.get_cg_micro_blk_cnt())))) {
      LOG_WARN("failed to add dml splicer column", K(ret));
    } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
      LOG_WARN("failed to get sql string", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptStatSqlService::get_histogram_stat_sql(const uint64_t tenant_id,
                                                const ObOptColumnStat &stat,
                                                ObIAllocator &allocator,
                                                ObHistBucket &bucket,
                                                ObObjMeta endpoint_meta,
                                                ObSqlString &sql_string,
                                                const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  ObString endpoint_value;
  ObString b_endpoint_value;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = stat.get_table_id();
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(get_valid_obj_str(bucket.endpoint_value_,
                                endpoint_meta,
                                allocator,
                                endpoint_value,
                                print_params))) {
    LOG_WARN("failed to get valid obj str", K(ret));
  } else if (OB_FAIL(get_obj_binary_hex_str(bucket.endpoint_value_, allocator, b_endpoint_value))) {
    LOG_WARN("failed to convert obj to binary string", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
             OB_FAIL(dml_splicer.add_pk_column("column_id", stat.get_column_id())) ||
             OB_FAIL(dml_splicer.add_column("object_type", stat.get_stat_level())) ||
             OB_FAIL(dml_splicer.add_pk_column("endpoint_num", bucket.endpoint_num_)) ||
             OB_FAIL(dml_splicer.add_column("endpoint_normalized_value", -1)) ||
             OB_FAIL(dml_splicer.add_column("endpoint_value", ObHexEscapeSqlStr(endpoint_value))) ||
             OB_FAIL(dml_splicer.add_column("b_endpoint_value", b_endpoint_value)) ||
             OB_FAIL(dml_splicer.add_column("endpoint_repeat_cnt", bucket.endpoint_repeat_count_))) {
    LOG_WARN("failed to add dml splice values", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatSqlService::hex_str_to_obj(const char *buf,
                                        int64_t buf_len,
                                        ObIAllocator &allocator,
                                        ObObj &obj)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t ret_len = 0;
  char *resbuf = NULL;
  if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (NULL == (resbuf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot allocate memory for deserializing obj.", K(buf_len), K(ret));
  } else if (buf_len != (ret_len = common::str_to_hex(buf,
                                                      static_cast<int32_t>(buf_len),
                                                      resbuf,
                                                      static_cast<int32_t>(buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer str to hex failed", K(buf), K(buf_len), K(ret_len), K(ret));
  } else if (OB_FAIL(obj.deserialize(resbuf, ret_len, pos))) {
    LOG_WARN("deserialize obj failed.", K(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

int ObOptStatSqlService::get_obj_str(const ObObj &obj,
                                     ObIAllocator &allocator,
                                     ObString &out_str,
                                     const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (obj.is_string_type()) {
    ObObjPrintParams copy_print_params = print_params;
    copy_print_params.cs_type_ = obj.get_collation_type();
    if (OB_FAIL(obj.print_varchar_literal(buf, buf_len, pos, copy_print_params))) {
      LOG_WARN("failed to print sql literal", K(obj));
    } else { /*do nothing*/ }
  } else if (obj.is_valid_type()) {
    if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos, print_params))) {
      LOG_WARN("failed to print_sql_literal", K(obj));
    } else { /*do nothing*/ }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(obj.get_type()));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get buf", K(pos), K(buf_len), K(ret));
    } else {
      out_str.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObOptStatSqlService::get_obj_binary_hex_str(const ObObj &obj,
                                                ObIAllocator &allocator,
                                                ObString &out_str)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  char *hex_buf = NULL;
  const int64_t buf_len = obj.get_serialize_size();
  int64_t pos = 0;
  int64_t hex_pos = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(OB_MAX_PARTITION_EXPR_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(obj.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", K(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), K(ret));
  } else if (OB_FAIL(hex_print(buf, pos, hex_buf, OB_MAX_PARTITION_EXPR_LENGTH, hex_pos))) {
    LOG_WARN("failed to hex cstr", K(ret));
  } else {
    out_str.assign_ptr(hex_buf, static_cast<int32_t>(hex_pos));
  }
  return ret;
}

int ObOptStatSqlService::fill_table_stat(common::sqlclient::ObMySQLResult &result, ObOptTableStat &stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else {
    int64_t int_value = 0;
    ObObjMeta obj_type;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, object_type, stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, row_count, stat, int64_t);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_type("avg_row_size", obj_type))) {
        LOG_WARN("failed to get type", K(ret));
      } else if (OB_LIKELY(obj_type.is_double())) {
        EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, avg_row_size, stat, int64_t);
      } else {
        EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, avg_row_size, stat, int64_t);
      }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, macro_block_num, stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, micro_block_num, stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, stattype_locked, stat, int64_t);
    if (OB_SUCCESS != (ret = result.get_timestamp("last_analyzed", NULL, int_value))) {
      LOG_WARN("fail to get column in row. ", "column_name", "last_analyzed", K(ret));
    } else {
      stat.set_last_analyzed(static_cast<int64_t>(int_value));
      if (!stat.is_locked()) {
        stat.set_stat_expired_time(ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL);
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::fetch_column_stat(const uint64_t tenant_id,
                                           ObIAllocator &allocator,
                                           ObIArray<ObOptKeyColumnStat> &key_col_stats,
                                           bool is_accross_tenant_query,
                                           sqlclient::ObISQLConnection *conn/*default null*/)
{
  int ret = OB_SUCCESS;
  ObSqlString keys_list_str;
  hash::ObHashMap<ObOptKeyInfo, int64_t> key_index_map;
  if (key_col_stats.empty()) {
  } else if (OB_FAIL(generate_specified_keys_list_str_for_column(tenant_id, key_col_stats, keys_list_str))) {
    LOG_WARN("failed to generate specified keys list str for column", K(ret), K(key_col_stats));
  } else if (OB_FAIL(key_index_map.create(key_col_stats.count(), "OptKeyColStat", "OptColStatNode", !is_accross_tenant_query ? tenant_id : OB_SERVER_TENANT_ID))) {
    LOG_WARN("fail to create hash map", K(ret), K(key_col_stats.count()));
  } else if (OB_FAIL(generate_key_index_map(tenant_id, key_col_stats, key_index_map))) {
    LOG_WARN("failed to init key index map", K(ret));
  } else if (OB_UNLIKELY(key_col_stats.count() < 1) || OB_ISNULL(key_col_stats.at(0).key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(key_col_stats), K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      uint64_t data_version = 0;
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql service has not been initialized.", K(ret));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("fail to get tenant data version", KR(ret));
      } else if (data_version < DATA_VERSION_4_3_0_0 && OB_FAIL(sql.append_fmt(FETCH_ALL_COLUMN_STAT_SQL,
                                        share::OB_ALL_COLUMN_STAT_TNAME,
                                        share::OB_ALL_HISTOGRAM_STAT_TNAME,
                                        keys_list_str.string().length(),
                                        keys_list_str.string().ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(ret));
      } else if (data_version >= DATA_VERSION_4_3_0_0 && OB_FAIL(sql.append_fmt(FETCH_ALL_COLUMN_STAT_SQL_COL,
                                        share::OB_ALL_COLUMN_STAT_TNAME,
                                        share::OB_ALL_HISTOGRAM_STAT_TNAME,
                                        keys_list_str.string().length(),
                                        keys_list_str.string().ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(ret));
      } else if (conn != NULL &&
                 OB_FAIL(conn->execute_read(exec_tenant_id, sql.ptr(), res))) {
      LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (conn == NULL &&
                 OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed, ", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(fill_column_stat(allocator,
                                              *result,
                                              key_index_map,
                                              key_col_stats,
                                              data_version >= DATA_VERSION_4_3_0_0))) {
            LOG_WARN("read stat from result failed. ", K(ret));
          } else {/*do nothing*/}
        }
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::fill_column_stat(ObIAllocator &allocator,
                                          common::sqlclient::ObMySQLResult &result,
                                          hash::ObHashMap<ObOptKeyInfo, int64_t> &key_index_map,
                                          ObIArray<ObOptKeyColumnStat> &key_col_stats,
                                          bool need_cg_info)
{
  int ret = OB_SUCCESS;
  uint64_t pure_table_id = 0;
  int64_t partition_id = 0;
  uint64_t column_id = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "table_id", pure_table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "partition_id", partition_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "column_id", column_id, uint64_t);
    if (OB_SUCC(ret)) {
      ObOptKeyInfo dst_key_info(pure_table_id, partition_id, column_id);
      int64_t dst_idx = -1;
      if (OB_FAIL(key_index_map.get_refactored(dst_key_info, dst_idx))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          LOG_TRACE("the column stat doesn't process, have been get", K(dst_key_info));
        } else {
          LOG_WARN("failed to get refactored", K(ret), K(dst_key_info));
        }
      } else if (OB_UNLIKELY(dst_idx < 0 || dst_idx >= key_col_stats.count()) ||
                 OB_ISNULL(key_col_stats.at(dst_idx).stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(dst_idx), K(key_col_stats));
      } else {
        ObOptKeyColumnStat &dst_key_col_stat = key_col_stats.at(dst_idx);
        if (dst_key_col_stat.only_histogram_stat_) {
          ObHistBucket bkt;
          ObString str;
          EXTRACT_INT_FIELD_MYSQL(result, "endpoint_num", bkt.endpoint_num_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(result, "endpoint_repeat_cnt", bkt.endpoint_repeat_count_, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_endpoint_value", str);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(hex_str_to_obj(str.ptr(), str.length(), allocator, bkt.endpoint_value_))) {
              LOG_WARN("deserialize object value failed.", K(stat), K(ret));
            } else if (OB_FAIL(dst_key_col_stat.stat_->get_histogram().add_bucket(bkt))) {
              LOG_WARN("failed to push back buckets", K(ret));
            } else {/*do nothing*/}
          }
        } else {//column stat has been obtained, just skip
          int64_t llc_bitmap_size = 0;
          int64_t bucket_cnt = 0;
          ObHistType histogram_type = ObHistType::INVALID_TYPE;
          ObObjMeta obj_type;
          ObOptColumnStat *stat = dst_key_col_stat.stat_;
          ObHistogram &hist = stat->get_histogram();
          stat->set_table_id(dst_key_col_stat.key_->table_id_);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, *stat, uint64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, *stat, uint64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, stat_level, *stat, int64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_distinct, *stat, int64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_null, *stat, int64_t);
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
              EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, avg_len, *stat, int64_t);
            } else {
              EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, avg_len, *stat, int64_t);
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
              stat->set_last_analyzed(int_value);
            }
          }
          EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_min_value", hex_str);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(hex_str_to_obj(hex_str.ptr(), hex_str.length(), allocator, obj))) {
              LOG_WARN("failed to convert hex str to obj", K(ret));
            } else {
              stat->set_min_value(obj);
            }
          }
          EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_max_value", hex_str);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(hex_str_to_obj(hex_str.ptr(), hex_str.length(), allocator, obj))) {
              LOG_WARN("failed to convert hex str to obj", K(ret));
            } else {
              stat->set_max_value(obj);
            }
          }
          EXTRACT_VARCHAR_FIELD_MYSQL(result, "distinct_cnt_synopsis", hex_str);
          char *bitmap_buf = NULL;
          if (OB_SUCC(ret) && llc_bitmap_size > 0) {
            int64_t compress_type = ObOptStatCompressType::MAX_COMPRESS;
            EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "compress_type", compress_type, int64_t, true, false, ObOptStatCompressType::ZLIB_COMPRESS);
            if (OB_SUCC(ret)) {
              if (OB_UNLIKELY(compress_type < 0 || compress_type >= ObOptStatCompressType::MAX_COMPRESS)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected error", K(ret), K(compress_type));
              } else if (NULL == (bitmap_buf = static_cast<char*>(allocator.alloc(hex_str.length())))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("allocate memory for llc_bitmap failed.", K(hex_str.length()), K(ret));
              } else {
                common::str_to_hex(hex_str.ptr(), hex_str.length(), bitmap_buf, hex_str.length());
                // decompress llc bitmap;
                char *decomp_buf = NULL ;
                int64_t decomp_size = ObOptColumnStat::NUM_LLC_BUCKET;
                const int64_t bitmap_size = hex_str.length() / 2;
                if (OB_FAIL(get_decompressed_llc_bitmap(allocator, bitmap_compress_lib_name[compress_type], bitmap_buf,
                                                        bitmap_size, decomp_buf, decomp_size))) {
                  COMMON_LOG(WARN, "decompress bitmap buffer failed.", K(ret));
                } else {
                  stat->set_llc_bitmap(decomp_buf, decomp_size);
                }
              }
            }
          }
          if (OB_SUCC(ret) && hist.is_valid()) {
            ObHistBucket bkt;
            ObString str;
            EXTRACT_INT_FIELD_MYSQL(result, "endpoint_num", bkt.endpoint_num_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(result, "endpoint_repeat_cnt", bkt.endpoint_repeat_count_, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_endpoint_value", str);
            if (OB_SUCC(ret)) {
              if (OB_FAIL(hex_str_to_obj(str.ptr(), str.length(), allocator, bkt.endpoint_value_))) {
                LOG_WARN("deserialize object value failed.", K(stat), K(ret));
              } else if (OB_FAIL(hist.add_bucket(bkt))) {
                LOG_WARN("failed to push back buckets", K(ret));
              } else {
                dst_key_col_stat.only_histogram_stat_ = true;
              }
            }
          }
          if (OB_SUCC(ret) && need_cg_info) {
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, cg_macro_blk_cnt, *stat, int64_t, true, true, 0);
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, cg_micro_blk_cnt, *stat, int64_t, true, true, 0);
            //will be used in the future, not removed.
            // if (OB_SUCC(ret)) {
            //   if (OB_FAIL(result.get_type("cg_skip_rate", obj_type))) {
            //     LOG_WARN("failed to get type", K(ret));
            //   } else if (OB_LIKELY(obj_type.is_double())) {
            //     EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, cg_skip_rate, *stat, int64_t);
            //   } else {
            //     EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, cg_skip_rate, *stat, int64_t);
            //   }
            // }
          }
        }
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::get_compressed_llc_bitmap(ObIAllocator &allocator,
                                                   const char *bitmap_compress_name,
                                                   const char *bitmap_buf,
                                                   int64_t bitmap_size,
                                                   char *&comp_buf,
                                                   int64_t &comp_size)
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor  = NULL;
  int64_t max_comp_size = 0;
  if (NULL == bitmap_buf || bitmap_size <= 0 || bitmap_compress_name == NULL) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(bitmap_buf), K(bitmap_size), K(bitmap_compress_name), K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
      bitmap_compress_name, compressor))) {
    COMMON_LOG(WARN, "cannot create compressor, do not compress data.",
               K(bitmap_compress_name), K(ret));
  } else if (NULL == compressor) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "compressor is NULL, do not compress data.",
               K(bitmap_compress_name), K(ret));
  } else if (OB_FAIL(compressor->get_max_overflow_size(bitmap_size, max_comp_size))) {
    COMMON_LOG(WARN, "get max overflow size failed.",
               K(bitmap_compress_name), K(bitmap_size), K(ret));
  } else {
    max_comp_size += bitmap_size;
    if (NULL == (comp_buf = static_cast<char*>(allocator.alloc(max_comp_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "cannot allocate compressed buffer.",
                 K(max_comp_size), K(ret));
    } else if (OB_FAIL(compressor->compress(bitmap_buf,
                                            bitmap_size,
                                            comp_buf,
                                            max_comp_size,
                                            comp_size))) {
      COMMON_LOG(WARN, "compress llc bitmap failed.", K(ret));
    } else if (comp_size >= bitmap_size) {
      // compress is not work, just use original data.
      comp_buf = const_cast<char*>(bitmap_buf);
      comp_size = bitmap_size;
    }
  }
  return ret;
}

int ObOptStatSqlService::get_decompressed_llc_bitmap(ObIAllocator &allocator,
                                                     const char *bitmap_compress_name,
                                                     const char *comp_buf,
                                                     int64_t comp_size,
                                                     char *&bitmap_buf,
                                                     int64_t &bitmap_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_bitmap_size = ObOptColumnStat::NUM_LLC_BUCKET; // max size of uncompressed buffer.
  ObCompressor* compressor = NULL;

  if (OB_ISNULL(bitmap_compress_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(bitmap_compress_name));
  } else if (comp_size >= ObOptColumnStat::NUM_LLC_BUCKET) {
    // not compressed bitmap, use directly;
    bitmap_buf = const_cast<char*>(comp_buf);
    bitmap_size = comp_size;
  } else if (NULL == (bitmap_buf = static_cast<char*>(allocator.alloc(max_bitmap_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for uncompressed data failed.", K(max_bitmap_size), K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
      bitmap_compress_name, compressor)))  {
    LOG_WARN("cannot create compressor, do not uncompress data.",
               K(bitmap_compress_name), K(ret));
  } else if (NULL == compressor) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is NULL, do not compress data.",
             K(bitmap_compress_name), K(ret));
  } else if (OB_FAIL(compressor->decompress(comp_buf,
                                            comp_size,
                                            bitmap_buf,
                                            max_bitmap_size,
                                            bitmap_size))) {
    LOG_WARN("decompress bitmap buffer failed.",
               KP(comp_buf), K(comp_size), KP(bitmap_buf),
               K(max_bitmap_size), K(bitmap_size), K(ret));
  }
  return ret;
}

int ObOptStatSqlService::generate_in_list(const ObIArray<int64_t> &list, ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); i++) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == list.count() - 1 ? ')' : ',');
    if (OB_FAIL(sql_string.append_fmt("%c%ld%c", prefix, list.at(i), suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::generate_in_list(const ObIArray<uint64_t> &list, ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); i++) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == list.count() - 1 ? ')' : ',');
    if (OB_FAIL(sql_string.append_fmt("%c%ld%c", prefix, list.at(i), suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::get_valid_obj_str(const ObObj &src_obj,
                                           common::ObObjMeta dst_column_meta,
                                           ObIAllocator &allocator,
                                           ObString &dest_str,
                                           const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  if (src_obj.is_string_type()) {
    ObObj dst_obj;
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, dst_column_meta.get_collation_type());
    const char *incorrect_string = "-4258: Incorrect string value, can't show.";
    int64_t well_formed_len = 0;
    if (OB_FAIL(ObObjCaster::to_type(dst_column_meta.get_type(), cast_ctx, src_obj, dst_obj)) ||
        OB_FAIL(ObCharset::well_formed_len(dst_column_meta.get_collation_type(), dst_obj.get_string().ptr(),
                                           dst_obj.get_string().length(), well_formed_len))) {
      //for column which have invalid char ==> save obj binary to use, and obj value to
      //  save "-4258: Incorrect string value" to show this obj have invalid.
      if (OB_ERR_INCORRECT_STRING_VALUE == ret) {
        ret = OB_SUCCESS;
        dst_obj.set_string(dst_column_meta.get_type(), incorrect_string, static_cast<int32_t>(strlen(incorrect_string)));
        dst_obj.set_meta_type(dst_column_meta);
        LOG_TRACE("invalid string for charset", K(ret), K(src_obj), K(dst_column_meta));
      } else {
        LOG_WARN("failed to type", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(get_obj_str(dst_obj, allocator, dest_str, print_params))) {
      LOG_WARN("fail to get obj str", K(ret));
    } else {
      LOG_TRACE("succeed to get valid obj str", K(src_obj), K(dst_obj), K(dest_str));
    }
  } else if (OB_FAIL(get_obj_str(src_obj, allocator, dest_str, print_params))) {
    LOG_WARN("failed to get obj str", K(ret), K(src_obj));
  }
  return ret;
}

int ObOptStatSqlService::generate_specified_keys_list_str_for_column(const uint64_t tenant_id,
                                                                     ObIArray<ObOptKeyColumnStat> &key_col_stats,
                                                                     ObSqlString &keys_list_str)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  ObSqlString partition_list_str;
  ObSqlString column_list_str;
  hash::ObHashMap<int64_t, bool> partition_ids_map;
  hash::ObHashMap<uint64_t, bool> column_ids_map;
  if (OB_UNLIKELY(key_col_stats.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(key_col_stats));
  } else if (OB_FAIL(partition_ids_map.create(10000, "OptKeyColStat"))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(column_ids_map.create(10000, "OptKeyColStat"))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_col_stats.count(); ++i) {
      if (OB_ISNULL(key_col_stats.at(i).key_) || OB_UNLIKELY(!key_col_stats.at(i).key_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(key_col_stats.at(i).key_));
      } else if (i == 0) {
        table_id = key_col_stats.at(i).key_->table_id_;
      }
      if (OB_SUCC(ret)) {
        //expected the key from the same table.
        if (OB_UNLIKELY(table_id != key_col_stats.at(i).key_->table_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(table_id), KPC(key_col_stats.at(i).key_));
        } else {
          //process partition list
          bool tmp_var = false;
          if (OB_FAIL(partition_ids_map.get_refactored(key_col_stats.at(i).key_->partition_id_, tmp_var))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              if (OB_FAIL(partition_list_str.append_fmt("%s%ld", i == 0 ? "" : ",",
                                                         key_col_stats.at(i).key_->partition_id_))) {
                LOG_WARN("failed to append", K(ret));
              } else if (OB_FAIL(partition_ids_map.set_refactored(key_col_stats.at(i).key_->partition_id_, true))) {
                LOG_WARN("failed to set refactored", K(ret));
              } else {/*do nothing*/}
            } else {
              LOG_WARN("failed to get refactored", K(ret));
            }
          }
          //process column list
          if (OB_SUCC(ret)) {
            if (OB_FAIL(column_ids_map.get_refactored(key_col_stats.at(i).key_->column_id_, tmp_var))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                if (OB_FAIL(column_list_str.append_fmt("%s%lu", i == 0 ? "" : ",",
                                                        key_col_stats.at(i).key_->column_id_))) {
                  LOG_WARN("failed to append", K(ret));
                } else if (OB_FAIL(column_ids_map.set_refactored(key_col_stats.at(i).key_->column_id_, true))) {
                  LOG_WARN("failed to set refactored", K(ret));
                } else {/*do nothing*/}
              } else {
                LOG_WARN("failed to get refactored", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (OB_FAIL(keys_list_str.append_fmt(" (col_stat.TENANT_ID=%lu AND col_stat.TABLE_ID=%ld AND col_stat.PARTITION_ID IN (%.*s) AND col_stat.COLUMN_ID IN (%.*s))",
                                            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                                            partition_list_str.string().length(),
                                            partition_list_str.string().ptr(),
                                            column_list_str.string().length(),
                                            column_list_str.string().ptr()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else {
        LOG_TRACE("succeed to generate specified keys list str", K(key_col_stats), K(keys_list_str));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::generate_key_index_map(const uint64_t tenant_id,
                                                ObIArray<ObOptKeyColumnStat> &key_col_stats,
                                                hash::ObHashMap<ObOptKeyInfo, int64_t> &key_index_map)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < key_col_stats.count(); ++i) {
    if (OB_ISNULL(key_col_stats.at(i).key_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(key_col_stats.at(i).key_));
    } else {
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      const uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                          key_col_stats.at(i).key_->table_id_);
      ObOptKeyInfo key_info(pure_table_id,
                            key_col_stats.at(i).key_->partition_id_,
                            key_col_stats.at(i).key_->column_id_);
      if (OB_FAIL(key_index_map.set_refactored(key_info, i))) {
        LOG_WARN("fail to set refactored for hashmap", K(ret), K(key_info));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptStatSqlService::get_column_stat_min_max_meta(share::schema::ObSchemaGetterGuard *schema_guard,
                                                      const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      ObObjMeta &min_meta,
                                                      ObObjMeta &max_meta)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get index schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema));
  } else {
    bool found_min_col = false;
    bool found_max_col = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && (!found_min_col || !found_max_col) && i < table_schema->get_column_count();
         ++i) {
      const share::schema::ObColumnSchemaV2 *col = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret), K(col));
      } else if (0 == col->get_column_name_str().case_compare("min_value")) {
        min_meta = col->get_meta_type();
        found_min_col = true;
      } else if (0 == col->get_column_name_str().case_compare("max_value")) {
        max_meta = col->get_meta_type();
        found_max_col = true;
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && (!found_min_col || !found_max_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(found_min_col), K(found_max_col));
    }
  }
  return ret;
}

int ObOptStatSqlService::get_histogram_endpoint_meta(share::schema::ObSchemaGetterGuard *schema_guard,
                                                     const uint64_t tenant_id,
                                                     const uint64_t table_id,
                                                     ObObjMeta &endpoint_meta)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get index schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema));
  } else {
    bool found_it = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !found_it && i < table_schema->get_column_count();
         ++i) {
      const share::schema::ObColumnSchemaV2 *col = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret), K(col));
      } else if (0 == col->get_column_name_str().case_compare("endpoint_value")) {
        endpoint_meta = col->get_meta_type();
        found_it = true;
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && !found_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(found_it), K(table_id));
    }
  }
  return ret;
}

/*
*** NOTE: leave batch_update_col_stat_online and batch_update_tab_stat_online function for future use.
*** We use the split_write to update tab & col stats.
*** In the future, to support rollback, we may update tab & col stats in same connection.
int ObOptStatSqlService::batch_update_online_tab_stat(const uint64_t tenant_id,
                                                  const common::ObIArray<ObOptTableStat *> &table_stats,
                                                  const common::ObIArray<ObOptColumnStatHandle> &old_tab_handles)
{
  int ret = OB_SUCCESS;
  ObSqlString table_stat_sql;
  ObSqlString tmp;
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  // merge old and new table stats;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_tab_handles.count(); i++) {
    const ObOptTableStat *&old_tab_stat = old_tab_handles.at(i).stat_;
    ObOptTableStat *new_tab_stat = table_stats.at(i);
    if (old_tab_stat->get_column_id() != new_tab_stat->get_column_id()
        || old_tab_stat->get_partition_id() != new_tab_stat->get_partition_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stats do not match", K(ret));
    } else if (OB_FAIL(new_tab_stat->merge_table_stat(*new_tab_stat))) {
      //merge
      LOG_WARN("fail to merge new table stat with old table stat", K(ret));
    }
  }
  if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_SQL_ONLINE))) {
    LOG_WARN("failed to append sql", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    bool is_last = (i == table_stats.count() - 1);
    tmp.reset();
    if (OB_ISNULL(table_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table stat is null", K(ret));
    } else if (OB_FAIL(get_table_stat_sql(tenant_id, *table_stats.at(i), current_time, false, tmp))) {
      LOG_WARN("failed to get table stat sql", K(ret));
    } else if (OB_FAIL(table_stat_sql.append_fmt("(%s)%c",tmp.ptr(), (is_last ? ' ' : ',')))) {
      LOG_WARN("failed to append table stat sql", K(ret));
    } else {}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_ONLINE_DUPLICATE))) {
      LOG_WARN("failed to append table stat sql", K(ret));
    } else if (OB_FAIL(table_stat_sql.append(";"))) {
      LOG_WARN("failed to append table stat sql", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, table_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}
int ObOptStatSqlService::batch_update_online_col_state(const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   share::schema::ObSchemaGetterGuard *schema_guard,
                                                   const common::ObIArray<ObOptColumnStat *> &column_stats,
                                                   const common::ObIArray<ObOptColumnStatHandle> &old_col_handles)
{
  int ret = OB_SUCCESS;
  ObSqlString col_stat_sql;
  ObArenaAllocator allocator(ObModIds::OB_BUFFER);
  // step 1: get old column stat (here we use the history stat);)

  // step 2: merge old and new column stat
  for (int64_t i = 0; OB_SUCC(ret) && i < old_col_handles.count(); i++) {
    const ObOptColumnStat *&old_col_stat = old_col_handles.at(i).stat_;
    ObOptColumnStat *new_col_stat = column_stats.at(i);
    if (old_col_stat->get_column_id() != new_col_stat->get_column_id()
        || old_col_stat->get_partition_id() != new_col_stat->get_partition_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stats do not match", K(ret));
    } else if (OB_FAIL(new_col_stat->merge_column_stat(*new_col_stat))) {
      //merge
      LOG_WARN("fail to merge new column stat with old column stat", K(ret));
    }
  }
  // step 3: write column stat
  // generate column stat sql;
  if (OB_FAIL(ret)) {
  } else {
    ObObjMeta min_meta;
    ObObjMeta max_meta;
    int64_t current_time = ObTimeUtility::current_time();
    if (OB_FAIL(get_column_stat_min_max_meta(schema_guard, tenant_id,
                                            share::OB_ALL_COLUMN_STAT_TID,
                                            min_meta,
                                            max_meta))) {
      LOG_WARN("failed to get column stat min max meta", K(ret));
    } else {
      // HEAD SQL: INSERT INTO....
      if (OB_FAIL(col_stat_sql.append(INSERT_COL_STAT_SQL_ONLINE))) {
        LOG_WARN("fail to format str", K(ret));
      }
      LOG_WARN("get full insert column stat sql", K(col_stat_sql));
      ObSqlString tmp;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
        tmp.reset();
        if (OB_FAIL(get_column_stat_sql(tenant_id, allocator,
                                        *column_stats.at(i), current_time,
                                        min_meta, max_meta, tmp))) {
          LOG_WARN("fail to get column stat sql", K(ret));
        } else if (OB_FAIL(col_stat_sql.append_fmt("(%s)%s", tmp.ptr(),
                                                    (i == column_stats.count() - 1 ? " " : ",")))) {
          LOG_WARN("failed to append sql", K(ret));
        }
      }
      LOG_WARN("get full insert column stat sql", K(col_stat_sql));
      // TAIL SQL: ON DUPLICATE KEY
      if (OB_SUCC(ret) && OB_FAIL(col_stat_sql.append(INSERT_COL_STAT_ONLINE_DUPLICATE))) {
        LOG_WARN("fail to append insert", K(ret));
      }
      LOG_WARN("get full insert column stat sql", K(col_stat_sql));
    }
  }
  // insert
  if (OB_SUCC(ret)) {
    // TODO daisi: sending inner sql in same connection to suuport rollback.
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, col_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {
      LOG_TRACE("Success to write column stats", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}*/

int ObOptStatSqlService::fetch_table_rowcnt(const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            const ObIArray<ObTabletID> &all_tablet_ids,
                                            const ObIArray<share::ObLSID> &all_ls_ids,
                                            ObIArray<ObOptTableStat> &tstats)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString tablet_list_str;
  ObSqlString tablet_ls_list_str;
  uint64_t real_table_id = share::is_oracle_mapping_real_virtual_table(table_id) ?
                           ObSchemaUtils::get_real_table_mappings_tid(table_id) : table_id;
  if (OB_FAIL(gen_tablet_list_str(all_tablet_ids, all_ls_ids, tablet_list_str, tablet_ls_list_str))) {
    LOG_WARN("failed to gen tablet list str", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt("select /*+opt_param('enable_in_range_optimization','true') opt_param('use_default_opt_stat','true')*/"\
                                         "tablet_id, max(row_count) from "\
                                         "(select cast(tablet_id as unsigned) as tablet_id, cast(inserts - deletes as signed) as row_count "\
                                         "from %s where tenant_id = %lu and table_id = %lu and tablet_id in %s union all "\
                                         "select cast(tablet_id as unsigned) as tablet_id, cast(row_count as signed) as row_count from %s, "\
                                         "(select frozen_scn from %s order by frozen_scn desc limit 1) where "\
                                         "tenant_id = %lu and compaction_scn = frozen_scn and (tablet_id, ls_id) in %s) group by tablet_id;",
                                         share::OB_ALL_MONITOR_MODIFIED_TNAME,
                                         share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                         share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, real_table_id),
                                         tablet_list_str.ptr(),
                                         share::OB_ALL_TABLET_CHECKSUM_TNAME,
                                         share::OB_ALL_FREEZE_INFO_TNAME,
                                         share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id,tenant_id),
                                         tablet_ls_list_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        int64_t expired_time = ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL;
        while (OB_SUCC(ret)) {
          int64_t tablet_idx = 0;
          int64_t row_cnt_idx = 1;
          ObObj tablet_obj;
          ObObj row_cnt_obj;
          uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
          int64_t row_cnt = 0;
          if (OB_FAIL(client_result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(client_result->get_obj(tablet_idx, tablet_obj)) ||
                     OB_FAIL(client_result->get_obj(row_cnt_idx, row_cnt_obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tablet_obj.get_uint64(tablet_id)) ||
                     OB_FAIL(row_cnt_obj.get_int(row_cnt))) {
            LOG_WARN("failed to get int", K(ret), K(tablet_obj), K(row_cnt_obj));
          } else {
            ObOptTableStat tstat;
            tstat.set_table_id(table_id);
            tstat.set_tablet_id(tablet_id);
            tstat.set_row_count(row_cnt);
            tstat.set_stat_expired_time(expired_time);
            if (OB_FAIL(tstats.push_back(tstat))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
        LOG_TRACE("succeed to fetch table rowcnt", K(tstats), K(raw_sql));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::gen_tablet_list_str(const ObIArray<ObTabletID> &all_tablet_ids,
                                             const ObIArray<share::ObLSID> &all_ls_ids,
                                             ObSqlString &tablet_list_str,
                                             ObSqlString &tablet_ls_list_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(all_tablet_ids.empty() || all_tablet_ids.count() != all_ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(all_tablet_ids), K(all_ls_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tablet_ids.count(); ++i) {
      char prefix = i == 0 ? '(' : ' ';
      char suffix = i == all_tablet_ids.count() - 1 ? ')' : ',';
      if (OB_FAIL(tablet_list_str.append_fmt("%c%lu%c",
                                             prefix,
                                             all_tablet_ids.at(i).id(),
                                             suffix))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(tablet_ls_list_str.append_fmt("%c(%lu, %ld)%c",
                                                       prefix,
                                                       all_tablet_ids.at(i).id(),
                                                       all_ls_ids.at(i).id(),
                                                       suffix))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptStatSqlService::update_opt_stat_task_stat(const ObOptStatTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString value_str;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = gen_meta_tenant_id(task_info.tenant_id_);
  if (!is_valid_tenant_id(tenant_id)) {
    //do nothing
  } else if (OB_FAIL(get_gather_stat_task_value(task_info, value_str))) {
    LOG_WARN("failed to get gather stat values list", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(INSERT_TASK_OPT_STAT_GATHER_SQL,
                                        share::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
                                        value_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
  } else {
    ObMySQLTransaction trans;
    LOG_TRACE("sql string of update opt stat task stat", K(raw_sql));
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::update_opt_stat_gather_stat(const ObOptStatGatherStat &gather_stat)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString value_str;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = gen_meta_tenant_id(gather_stat.get_tenant_id());
  if (!is_valid_tenant_id(tenant_id)) {
    //do nothing
  } else if (OB_FAIL(get_gather_stat_value(gather_stat, value_str))) {
    LOG_WARN("failed to get gather stat value", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(INSERT_TABLE_OPT_STAT_GATHER_SQL,
                                        share::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
                                        value_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
  } else {
    ObMySQLTransaction trans;
    LOG_TRACE("sql string of update opt stat gather stat", K(raw_sql));
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::get_gather_stat_task_value(const ObOptStatTaskInfo &task_info,
                                                    ObSqlString &value_str)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t tenant_id = task_info.tenant_id_;
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("task_id", task_info.task_id_)) ||
      OB_FAIL(dml_splicer.add_column("type", task_info.type_)) ||
      OB_FAIL(dml_splicer.add_column("ret_code", task_info.ret_code_)) ||
      OB_FAIL(dml_splicer.add_column("failed_count", task_info.failed_count_)) ||
      OB_FAIL(dml_splicer.add_column("table_count", task_info.task_table_count_)) ||
      OB_FAIL(dml_splicer.add_time_column("start_time", task_info.task_start_time_)) ||
      OB_FAIL(dml_splicer.add_time_column("end_time", task_info.task_end_time_))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(value_str))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatSqlService::get_gather_stat_value(const ObOptStatGatherStat &gather_stat,
                                               ObSqlString &values_ptr)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t tenant_id = gather_stat.get_tenant_id();
  uint64_t table_id = gather_stat.get_table_id();
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("task_id", gather_stat.get_task_id())) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_column("ret_code", gather_stat.get_ret_code())) ||
      OB_FAIL(dml_splicer.add_time_column("start_time", gather_stat.get_start_time())) ||
      OB_FAIL(dml_splicer.add_time_column("end_time", gather_stat.get_end_time())) ||
      OB_FAIL(dml_splicer.add_column("memory_used", gather_stat.get_memory_used())) ||
      OB_FAIL(dml_splicer.add_column("stat_refresh_failed_list", gather_stat.get_stat_refresh_failed_list())) ||
      OB_FAIL(dml_splicer.add_column("properties", gather_stat.get_properties()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(values_ptr))) {
    LOG_WARN("failed to get sql string", K(ret));
  }
  return ret;
}


int ObOptStatSqlService::update_system_stats(const uint64_t tenant_id,
                                            const ObOptSystemStat *system_stat)
{
  int ret = OB_SUCCESS;
  ObSqlString system_stat_sql;
  ObSqlString tmp;
  int64_t current_time = ObTimeUtility::current_time();
  int64_t affected_rows = 0;
  if (OB_ISNULL(system_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat is null", K(ret), K(system_stat));
  } else if (OB_FAIL(system_stat_sql.append_fmt(INSERT_SYSTEM_STAT_SQL, OB_ALL_AUX_STAT_TNAME))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(get_system_stat_sql(tenant_id, *system_stat, current_time, tmp))) {
    LOG_WARN("failed to get table stat sql", K(ret));
  } else if (OB_FAIL(system_stat_sql.append_fmt("(%s);", tmp.ptr()))) {
    LOG_WARN("failed to append system stat sql", K(ret));
  } else {
    ObMySQLTransaction trans;
    LOG_TRACE("sql string of system stat update", K(system_stat_sql));
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, system_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::get_system_stat_sql(const uint64_t tenant_id,
                                            const ObOptSystemStat &stat,
                                            const int64_t current_time,
                                            ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_time_column("last_analyzed", stat.get_last_analyzed() == 0 ?
                                                        current_time : stat.get_last_analyzed())) ||
      OB_FAIL(dml_splicer.add_column("cpu_speed", stat.get_cpu_speed())) ||
      OB_FAIL(dml_splicer.add_column("disk_seq_read_speed", stat.get_disk_seq_read_speed())) ||
      OB_FAIL(dml_splicer.add_column("disk_rnd_read_speed", stat.get_disk_rnd_read_speed())) ||
      OB_FAIL(dml_splicer.add_column("network_speed", stat.get_network_speed()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatSqlService::fetch_system_stat(const uint64_t tenant_id,
                                          const ObOptSystemStat::Key &key,
                                          ObOptSystemStat &stat)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version >= DATA_VERSION_4_3_0_0) {
    ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
    int64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql service has not been initialized.", K(ret));
      } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s ", share::OB_ALL_AUX_STAT_TNAME))) {
        LOG_WARN("fail to append SQL stmt string.", K(sql), K(ret));
      } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld", ext_tenant_id))) {
        LOG_WARN("fail to append SQL where string.", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(fill_system_stat(*result, stat))) {
        LOG_WARN("failed to fill system stat", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::fill_system_stat(sqlclient::ObMySQLResult &result, ObOptSystemStat &stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, cpu_speed, stat, int64_t, true, true, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, disk_seq_read_speed, stat, int64_t, true, true, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, disk_rnd_read_speed, stat, int64_t, true, true, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, network_speed, stat, int64_t, true, true, 0);
  }
  return ret;
}

int ObOptStatSqlService::delete_system_stats(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString system_stat_sql;
  int64_t current_time = ObTimeUtility::current_time();
  int64_t affected_rows = 0;
  int64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  if (OB_FAIL(system_stat_sql.append_fmt(DELETE_SYSTEM_STAT_SQL, OB_ALL_AUX_STAT_TNAME, ext_tenant_id))) {
    LOG_WARN("failed to append sql", K(ret));
  } else {
    ObMySQLTransaction trans;
    LOG_TRACE("sql string of system stat delete", K(system_stat_sql));
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, system_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase

#undef ALL_HISTOGRAM_STAT_COLUMN_NAME
#undef ALL_COLUMN_STAT_COLUMN_NAME
#undef INSERT_TABLE_STAT_SQL
#undef REPLACE_COL_STAT_SQL
#undef INSERT_HISTOGRAM_STAT_SQL
#undef DELETE_HISTOGRAM_STAT_SQL
#undef DELETE_COL_STAT_SQL
#undef DELETE_TAB_STAT_SQL
#undef UPDATE_HISTOGRAM_TYPE_SQL
#undef INSERT_ONLINE_TABLE_STAT_SQL
#undef INSERT_ONLINE_TABLE_STAT_DUPLICATE
#undef INSERT_ONLINE_COL_STAT_SQL
#undef INSERT_ONLINE_COL_STAT_DUPLICATE
#undef INERT_SYSTEM_STAT_SQL
#undef DELETE_SYSTEM_STAT_SQL
