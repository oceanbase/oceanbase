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
                                 histogram_type, sample_size, bucket_cnt, density, last_analyzed, spare1 as compress_type %s\
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

#define DELETE_STAT_HISTORY "DELETE /*+QUERY_TIMEOUT(%ld)*/FROM %s %s LIMIT %ld;"

#define INSERT_TABLE_STAT_HISTORY "INSERT INTO %s(tenant_id,           \
                                                  table_id,            \
                                                  partition_id,        \
                                                  savtime,             \
                                                  object_type,         \
                                                  flags,               \
                                                  last_analyzed,       \
                                                  sstable_row_cnt,     \
                                                  sstable_avg_row_len, \
                                                  macro_blk_cnt,       \
                                                  micro_blk_cnt,       \
                                                  memtable_row_cnt,    \
                                                  memtable_avg_row_len,\
                                                  row_cnt,             \
                                                  avg_row_len,         \
                                                  index_type,          \
                                                  stattype_locked,     \
                                                  spare1) %s"

#define SELECT_TABLE_STAT                "SELECT tenant_id,           \
                                                  table_id,            \
                                                  partition_id,        \
                                                  usec_to_time(%ld),   \
                                                  object_type,         \
                                                  0,                   \
                                                  last_analyzed,       \
                                                  sstable_row_cnt,     \
                                                  sstable_avg_row_len, \
                                                  macro_blk_cnt,       \
                                                  micro_blk_cnt,       \
                                                  memtable_row_cnt,    \
                                                  memtable_avg_row_len,\
                                                  row_cnt,             \
                                                  avg_row_len,         \
                                                  index_type,          \
                                                  stattype_locked,     \
                                                  spare1               \
                                             FROM %s                   \
                                             WHERE tenant_id = %lu and table_id = %lu %s"

#define TABLE_STAT_MOCK_VALUE_PATTERN "(%lu, %lu, %ld, usec_to_time(%ld), 0, 0, 0, 0, -1, -1, 0, 0, -1, 0, 0, 0, 0, 0)"

#define INSERT_COLUMN_STAT_HISTORY "INSERT INTO %s(tenant_id,                 \
                                                   table_id,                  \
                                                   partition_id,              \
                                                   column_id,                 \
                                                   savtime,                   \
                                                   object_type,               \
                                                   flags,                     \
                                                   last_analyzed,             \
                                                   distinct_cnt,              \
                                                   null_cnt,                  \
                                                   max_value,                 \
                                                   b_max_value,               \
                                                   min_value,                 \
                                                   b_min_value,               \
                                                   avg_len,                   \
                                                   distinct_cnt_synopsis,     \
                                                   distinct_cnt_synopsis_size,\
                                                   sample_size,               \
                                                   density,                   \
                                                   bucket_cnt,                \
                                                   histogram_type,            \
                                                   spare1%s) %s"

#define SELECT_COLUMN_STAT               "SELECT   tenant_id,                 \
                                                   table_id,                  \
                                                   partition_id,              \
                                                   column_id,                 \
                                                   usec_to_time(%ld),         \
                                                   object_type,               \
                                                   0,                         \
                                                   last_analyzed,             \
                                                   distinct_cnt,              \
                                                   null_cnt,                  \
                                                   max_value,                 \
                                                   b_max_value,               \
                                                   min_value,                 \
                                                   b_min_value,               \
                                                   avg_len,                   \
                                                   distinct_cnt_synopsis,     \
                                                   distinct_cnt_synopsis_size,\
                                                   sample_size,               \
                                                   density,                   \
                                                   bucket_cnt,                \
                                                   histogram_type,            \
                                                   spare1%s                   \
                                             FROM %s                          \
                                             WHERE %s"

#define COLUMN_STAT_MOCK_VALUE_PATTERN "(%lu, %lu, %ld, %lu, usec_to_time(%ld), 0, 0, usec_to_time(%ld), 0, 0, \
                                         %s, '%.*s', %s, '%.*s', 0, '', 0, -1, 0.000000, 0, 0, NULL%s)"

#define INSERT_HISTOGRAM_STAT_HISTORY "INSERT INTO %s(tenant_id,                \
                                                      table_id,                 \
                                                      partition_id,             \
                                                      column_id,                \
                                                      endpoint_num,             \
                                                      savtime,                  \
                                                      object_type,              \
                                                      endpoint_normalized_value,\
                                                      endpoint_value,           \
                                                      b_endpoint_value,         \
                                                      endpoint_repeat_cnt)      \
                                              SELECT  tenant_id,                \
                                                      table_id,                 \
                                                      partition_id,             \
                                                      column_id,                \
                                                      endpoint_num,             \
                                                      usec_to_time(%ld),        \
                                                      object_type,              \
                                                      endpoint_normalized_value,\
                                                      endpoint_value,           \
                                                      b_endpoint_value,         \
                                                      endpoint_repeat_cnt       \
                                                FROM %s                         \
                                                WHERE %s"

#define CHECK_TABLE_STAT "SELECT partition_id FROM %s WHERE tenant_id = %lu \
                          and table_id = %lu %s"

#define CHECK_COLUMN_STAT "SELECT partition_id, column_id FROM %s WHERE tenant_id = %lu \
                           and table_id = %lu %s"

int ObDbmsStatsHistoryManager::backup_opt_stats(ObExecContext &ctx,
                                                ObMySQLTransaction &trans,
                                                const ObTableStatParam &param,
                                                int64_t saving_time,
                                                bool is_backup_for_gather/*default false*/)
{
  int ret = OB_SUCCESS;
  int64_t retention_val = 0;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<uint64_t, 4> column_ids;
  if (param.is_index_stat_) {
    //do nothing
  } else if (OB_FAIL(get_stats_history_retention(ctx, retention_val))) {
    LOG_WARN("failed to get stats history retention", K(ret));
  } else if (retention_val == 0) {
    /*do nothing*/
  } else if (OB_FAIL(ObDbmsStatsUtils::get_part_ids_and_column_ids(param, part_ids, column_ids, is_backup_for_gather))) {
    LOG_WARN("failed to get part ids and column ids", K(ret));
  } else if (OB_FAIL(backup_table_stats(ctx, trans, param, saving_time, part_ids))) {
    LOG_WARN("faile to backup table stats", K(ret));
  } else if (OB_FAIL(backup_column_stats(ctx, trans, param, saving_time, part_ids, column_ids))) {
    LOG_WARN("faile to backup column stats", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsHistoryManager::backup_table_stats(ObExecContext &ctx,
                                                  ObMySQLTransaction &trans,
                                                  const ObTableStatParam &param,
                                                  const int64_t saving_time,
                                                  ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> no_stat_part_ids;
  ObSEArray<int64_t, 4> have_stat_part_ids;
  bool is_specify_partition_gather = param.is_specify_partition_gather();
  if (part_ids.empty()) {
  } else if (OB_FAIL(calssify_table_stat_part_ids(ctx,
                                                  param.tenant_id_,
                                                  param.table_id_,
                                                  is_specify_partition_gather,
                                                  part_ids,
                                                  no_stat_part_ids,
                                                  have_stat_part_ids))) {
    LOG_WARN("failed to calssify table stat part ids", K(ret));
  } else if (OB_FAIL(backup_having_table_part_stats(trans,
                                                    param.tenant_id_,
                                                    param.table_id_,
                                                    (is_specify_partition_gather || have_stat_part_ids.count() != part_ids.count()),
                                                    have_stat_part_ids,
                                                    saving_time))) {
    LOG_WARN("failed to backup having table part stats", K(ret));
  } else if (OB_FAIL(backup_no_table_part_stats(trans, param.tenant_id_, param.table_id_, no_stat_part_ids, saving_time))) {
    LOG_WARN("failed to backup no table part stats", K(ret));
  }
  return ret;
}

int ObDbmsStatsHistoryManager::calssify_table_stat_part_ids(ObExecContext &ctx,
                                                            const uint64_t tenant_id,
                                                            const uint64_t table_id,
                                                            const bool is_specify_partition_gather,
                                                            const ObIArray<int64_t> &partition_ids,
                                                            ObIArray<int64_t> &no_stat_part_ids,
                                                            ObIArray<int64_t> &have_stat_part_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString partition_list;
  ObSqlString extra_where_str;
  ObSqlString raw_sql;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session) || OB_UNLIKELY(partition_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session), K(partition_ids));
  } else if (is_specify_partition_gather &&
             OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (is_specify_partition_gather &&
             OB_FAIL(extra_where_str.append_fmt(" and partition_id in %s", partition_list.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(CHECK_TABLE_STAT,
                                        share::OB_ALL_TABLE_STAT_TNAME,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                        is_specify_partition_gather ? extra_where_str.ptr() : " "))) {
    LOG_WARN("failed to append fmt", K(ret));
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
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObObj tmp;
          int64_t idx = 0;
          int64_t partition_id = 0;
          if (OB_FAIL(client_result->get_obj(idx, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(partition_id))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(have_stat_part_ids.push_back(partition_id))) {
            LOG_WARN("failed to write object", K(ret));
          } else {/*do nothing*/}
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
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
    if (OB_SUCC(ret)) {
      if (have_stat_part_ids.count() == partition_ids.count()) {
        //do nothing
      } else if (have_stat_part_ids.empty()) {
        if (OB_FAIL(no_stat_part_ids.assign(partition_ids))) {
          LOG_WARN("failed to assign", K(ret));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
          bool found_it = false;
          for (int64_t j = 0; !found_it && j < have_stat_part_ids.count(); ++j) {
            found_it = (partition_ids.at(i) == have_stat_part_ids.at(j));
          }
          if (!found_it) {
            if (OB_FAIL(no_stat_part_ids.push_back(partition_ids.at(i)))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
    }
  }
  LOG_TRACE("calssify table stat part ids", K(partition_ids), K(no_stat_part_ids), K(have_stat_part_ids));
  return ret;
}

int ObDbmsStatsHistoryManager::backup_having_table_part_stats(ObMySQLTransaction &trans,
                                                              const uint64_t tenant_id,
                                                              const uint64_t table_id,
                                                              const bool is_specify_partition_gather,
                                                              const ObIArray<int64_t> &partition_ids,
                                                              const int64_t saving_time)
{
  int ret = OB_SUCCESS;
  ObSqlString partition_list;
  ObSqlString extra_where_str;
  ObSqlString raw_sql;
  ObSqlString select_sql;
  int64_t affected_rows = 0;
  if (partition_ids.empty()) {
  } else if (is_specify_partition_gather &&
             OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (is_specify_partition_gather &&
             OB_FAIL(extra_where_str.append_fmt(" and partition_id in %s", partition_list.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(select_sql.append_fmt(SELECT_TABLE_STAT,
                                           saving_time,
                                           share::OB_ALL_TABLE_STAT_TNAME,
                                           share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                           share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                           is_specify_partition_gather ? extra_where_str.ptr() : " "))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(INSERT_TABLE_STAT_HISTORY,
                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                        select_sql.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else {
    LOG_TRACE("succeed to backup having table part stats", K(raw_sql), K(affected_rows));
  }
  return ret;
}

//mock the null stat info for no table part stats.
int ObDbmsStatsHistoryManager::backup_no_table_part_stats(ObMySQLTransaction &trans,
                                                          const uint64_t tenant_id,
                                                          const uint64_t table_id,
                                                          const ObIArray<int64_t> &partition_ids,
                                                          const int64_t saving_time)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  //write 2000 part stats every time.
  while (OB_SUCC(ret) && idx < partition_ids.count()) {
    ObSqlString values_list;
    if (OB_UNLIKELY(idx >= partition_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpectd error", K(ret), K(idx), K(partition_ids));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_NUM_OF_WRITE_STATS && idx < partition_ids.count(); ++i) {
        ObSqlString value;
        if (OB_FAIL(value.append_fmt(TABLE_STAT_MOCK_VALUE_PATTERN,
                                     share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                     share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                     partition_ids.at(idx++),
                                     saving_time))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else if (OB_FAIL(values_list.append_fmt("%s%s",
                                                  i == 0 ? "VALUES " : ", ",
                                                  value.ptr()))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObSqlString raw_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(raw_sql.append_fmt(INSERT_TABLE_STAT_HISTORY,
                                     share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                     values_list.ptr()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
      } else {
        LOG_TRACE("succeed to backup no table part stats", K(raw_sql), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::backup_column_stats(ObExecContext &ctx,
                                                   ObMySQLTransaction &trans,
                                                   const ObTableStatParam &param,
                                                   const int64_t saving_time,
                                                   const ObIArray<int64_t> &part_ids,
                                                   const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObOptColumnStat::Key, bool> having_stat_part_col_map;
  int64_t map_size = part_ids.count() * column_ids.count();
  bool is_specify_partition_gather = param.is_specify_partition_gather();
  bool is_specify_column_gather = param.is_specify_column_gather();
  if (part_ids.empty() || column_ids.empty()) {
  } else if (OB_FAIL(having_stat_part_col_map.create(map_size,
                                                     "PartColHashMap",
                                                     "PartColNode",
                                                     param.tenant_id_))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(generate_having_stat_part_col_map(ctx,
                                                       param.tenant_id_,
                                                       param.table_id_,
                                                       is_specify_partition_gather,
                                                       is_specify_column_gather,
                                                       part_ids,
                                                       column_ids,
                                                       having_stat_part_col_map))) {
    LOG_WARN("failed to calssify table stat part ids", K(ret));
  } else if (OB_FAIL(backup_having_column_stats(trans, param.tenant_id_, param.table_id_,
                                                is_specify_partition_gather || is_specify_column_gather,
                                                part_ids, column_ids,
                                                having_stat_part_col_map,
                                                saving_time))) {
    LOG_WARN("failed to backup have column part stats", K(ret));
  } else if (OB_FAIL(backup_no_column_stats(trans, param.tenant_id_, param.table_id_,
                                            part_ids, column_ids,
                                            having_stat_part_col_map,
                                            saving_time))) {
    LOG_WARN("failed to backup column part stats", K(ret));
  } else if (OB_FAIL(backup_histogram_stats(trans, param.tenant_id_, param.table_id_,
                                            is_specify_partition_gather,
                                            is_specify_column_gather,
                                            part_ids, column_ids,
                                            having_stat_part_col_map,
                                            saving_time))) {
    LOG_WARN("faile to do backup histogram stats", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsHistoryManager::generate_having_stat_part_col_map(ObExecContext &ctx,
                                                                 const uint64_t tenant_id,
                                                                 const uint64_t table_id,
                                                                 const bool is_specify_partition_gather,
                                                                 const bool is_specify_column_gather,
                                                                 const ObIArray<int64_t> &partition_ids,
                                                                 const ObIArray<uint64_t> &column_ids,
                                                                 hash::ObHashMap<ObOptColumnStat::Key, bool> &have_stat_part_col_map)
{
  int ret = OB_SUCCESS;
  ObSqlString partition_list;
  ObSqlString column_list;
  ObSqlString extra_partition_str;
  ObSqlString extra_column_str;
  ObSqlString extra_where_str;
  ObSqlString raw_sql;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_ISNULL(session) ||
      OB_UNLIKELY(partition_ids.empty() || column_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(session), K(partition_ids), K(column_ids));
  } else if (is_specify_partition_gather &&
             OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (is_specify_partition_gather &&
             OB_FAIL(extra_partition_str.append_fmt(" and partition_id in %s", partition_list.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (is_specify_column_gather &&
             OB_FAIL(gen_column_list(column_ids, column_list))) {
    LOG_WARN("failed to gen column list", K(ret));
  } else if (is_specify_column_gather &&
             OB_FAIL(extra_column_str.append_fmt(" and column_id in %s", column_list.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if ((is_specify_partition_gather || is_specify_column_gather) &&
             OB_FAIL(extra_where_str.append_fmt("%s%s",
                                                is_specify_partition_gather ? extra_partition_str.ptr() : " ",
                                                is_specify_column_gather ? extra_column_str.ptr() : " "))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(CHECK_COLUMN_STAT,
                                        share::OB_ALL_COLUMN_STAT_TNAME,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                        (is_specify_partition_gather || is_specify_column_gather) ? extra_where_str.ptr() : " "))) {
    LOG_WARN("failed to append fmt", K(ret));
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
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObObj tmp;
          int64_t idx1 = 0;
          int64_t idx2 = 1;
          int64_t partition_id = 0;
          int64_t column_id = 0;
          if (OB_FAIL(client_result->get_obj(idx1, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(partition_id))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(client_result->get_obj(idx2, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(column_id))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else {
            ObOptColumnStat::Key key(tenant_id, table_id, partition_id, static_cast<uint64_t>(column_id));
            if (OB_FAIL(have_stat_part_col_map.set_refactored(key, true))) {
              LOG_WARN("failed to set refactored", K(ret), K(key));
            }
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
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
  LOG_TRACE("generate having stat part col map", K(have_stat_part_col_map.size()), K(partition_ids), K(column_ids));
  return ret;
}

int ObDbmsStatsHistoryManager::backup_having_column_stats(ObMySQLTransaction &trans,
                                                          const uint64_t tenant_id,
                                                          const uint64_t table_id,
                                                          const bool is_specify_gather,
                                                          const ObIArray<int64_t> &partition_ids,
                                                          const ObIArray<uint64_t> &column_ids,
                                                          hash::ObHashMap<ObOptColumnStat::Key, bool> &having_stat_part_col_map,
                                                          const int64_t saving_time)
{
  int ret = OB_SUCCESS;
  if (having_stat_part_col_map.size() != 0) {
    ObSqlString raw_sql;
    ObSqlString select_sql;
    ObSqlString where_str;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret));
    } else if (OB_LIKELY(having_stat_part_col_map.size() == partition_ids.count() * column_ids.count())) {
      if (!is_specify_gather) {
        if (OB_FAIL(where_str.append_fmt(" tenant_id = %lu and table_id = %lu",
                                         share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                         share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
          LOG_WARN("failed to append fmt", K(ret));
        }
      } else {
        ObSqlString partition_list;
        ObSqlString column_list;
        if (OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
          LOG_WARN("failed to gen partition list", K(ret));
        } else if (OB_FAIL(gen_column_list(column_ids, column_list))) {
          LOG_WARN("failed to gen partition list", K(ret));
        } else if (OB_FAIL(where_str.append_fmt(" tenant_id = %lu and table_id = %lu and partition_id in %s and column_id in %s",
                                                share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                                share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                                partition_list.ptr(),
                                                column_list.ptr()))) {
          LOG_WARN("failed to append fmt", K(ret));
        }
      }
    } else {
      // tenant_id = xx and table_id = xx and ((partition_id in xx and column_id in xx) or ((partition_id, column_id) in ((xx))))
      ObSqlString partition_list;
      ObSqlString part_col_list;
      bool is_first_part_list = true;
      bool is_first_part_col_list = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
        bool all_get = true;
        ObSqlString tmp_part_col_list;
        bool is_first = true;
        for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
          ObOptColumnStat::Key key(tenant_id, table_id, partition_ids.at(i), column_ids.at(j));
          bool val = false;
          if (OB_FAIL(having_stat_part_col_map.get_refactored(key, val))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS; // continue
              all_get = false;
            } else {
              LOG_WARN("failed to get map", K(ret), K(key));
            }
          } else if (OB_FAIL(tmp_part_col_list.append_fmt("%s(%ld, %lu)", is_first ? " " : ", ",
                                                          partition_ids.at(i),
                                                          column_ids.at(j)))) {
            LOG_WARN("failed to append fmt", K(ret));
          } else {
            is_first = false;
          }
        }
        if (OB_SUCC(ret)) {
          if (all_get) {
            if (OB_FAIL(partition_list.append_fmt("%s%ld",
                                                  is_first_part_list ? " " : ", ",
                                                  partition_ids.at(i)))) {
              LOG_WARN("failed to append fmt", K(ret));
            } else {
              is_first_part_list = false;
            }
          } else if (tmp_part_col_list.empty()) {
            //do nothing
          } else if (OB_FAIL(part_col_list.append_fmt("%s%s",
                                                      is_first_part_col_list ? " " : ", ",
                                                      tmp_part_col_list.ptr()))) {
            LOG_WARN("failed to append fmt", K(ret));
          } else {
            is_first_part_col_list = false;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObSqlString all_column_list;
        ObSqlString part_col_where1;
        ObSqlString part_col_where2;
        if (OB_FAIL(gen_column_list(column_ids, all_column_list))) {
          LOG_WARN("failed to gen partition list", K(ret));
        } else if (!partition_list.empty() &&
                   OB_FAIL(part_col_where1.append_fmt("(partition_id in (%s) and column_id in %s)",
                                                       partition_list.ptr(),
                                                       all_column_list.ptr()))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else if (!part_col_list.empty() &&
                   OB_FAIL(part_col_where2.append_fmt("((partition_id, column_id) in (%s))",
                                                       part_col_list.ptr()))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else if (OB_FAIL(where_str.append_fmt(" tenant_id = %lu and table_id = %lu and (%s %s %s)",
                                                share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                                share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                                part_col_where1.empty() ? " " : part_col_where1.ptr(),
                                                !part_col_where1.empty() && !part_col_where2.empty() ? "or" : " ",
                                                part_col_where2.empty() ? " " : part_col_where2.ptr()))) {
          LOG_WARN("failed to append fmt", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(select_sql.append_fmt(SELECT_COLUMN_STAT,
                                        saving_time,
                                        data_version < DATA_VERSION_4_3_0_0 ? " " : ",cg_macro_blk_cnt, cg_micro_blk_cnt",
                                        share::OB_ALL_COLUMN_STAT_TNAME,
                                        where_str.ptr()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(raw_sql.append_fmt(INSERT_COLUMN_STAT_HISTORY,
                                            share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                            data_version < DATA_VERSION_4_3_0_0 ? " " : ",cg_macro_blk_cnt, cg_micro_blk_cnt",
                                            select_sql.ptr()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
      } else {
        LOG_TRACE("succeed to backup having column stats", K(raw_sql), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::backup_no_column_stats(ObMySQLTransaction &trans,
                                                      const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      const ObIArray<int64_t> &partition_ids,
                                                      const ObIArray<uint64_t> &column_ids,
                                                      hash::ObHashMap<ObOptColumnStat::Key, bool> &having_stat_part_col_map,
                                                      const int64_t saving_time)
{
  int ret = OB_SUCCESS;
  int64_t total_cnt = partition_ids.count() * column_ids.count();
  if (having_stat_part_col_map.size() < total_cnt) {
    int64_t idx_part = 0;
    ObObj null_obj;
    null_obj.set_null();
    ObArenaAllocator allocator("OptStatsHistory", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    ObObjPrintParams tmp_obj_print_params;
    ObString null_str;
    ObSqlString null_sql_str;
    ObString b_null_str;
    uint64_t data_version = 0;
    if (OB_FAIL(ObOptStatSqlService::get_obj_str(null_obj, allocator, null_str, tmp_obj_print_params))) {
      LOG_WARN("failed to get obj str", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(null_str, null_sql_str))) {
      LOG_WARN("failed to sql append hex escape str", K(ret));
    } else if (OB_FAIL(ObOptStatSqlService::get_obj_binary_hex_str(null_obj, allocator, b_null_str))) {
      LOG_WARN("failed to convert obj to binary string", K(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret));
    } else {
      ObSqlString values_list;
      int64_t cur_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
          ObOptColumnStat::Key key(tenant_id, table_id, partition_ids.at(i), column_ids.at(j));
          bool val = false;
          if (OB_FAIL(having_stat_part_col_map.get_refactored(key, val))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              ObSqlString value;
              if (OB_FAIL(value.append_fmt(COLUMN_STAT_MOCK_VALUE_PATTERN,
                                           share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                           share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                           partition_ids.at(i),
                                           column_ids.at(j),
                                           saving_time,
                                           saving_time,
                                           null_sql_str.ptr(),
                                           b_null_str.length(),
                                           b_null_str.ptr(),
                                           null_sql_str.ptr(),
                                           b_null_str.length(),
                                           b_null_str.ptr(),
                                           data_version < DATA_VERSION_4_3_0_0 ? " " : ",0 ,0"))) {
                LOG_WARN("failed to append fmt", K(ret));
              } else if (OB_FAIL(values_list.append_fmt("%s%s",
                                                        cur_cnt == 0 ? "VALUES " : ", ",
                                                        value.ptr()))) {
                LOG_WARN("failed to push back", K(ret));
              } else {
                ++ cur_cnt;
                if (cur_cnt == MAX_NUM_OF_WRITE_STATS) {
                  if (OB_SUCC(ret)) {
                    ObSqlString raw_sql;
                    int64_t affected_rows = 0;
                    if (OB_FAIL(raw_sql.append_fmt(INSERT_COLUMN_STAT_HISTORY,
                                                   share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                                   data_version < DATA_VERSION_4_3_0_0 ? " " : ",cg_macro_blk_cnt, cg_micro_blk_cnt",
                                                   values_list.ptr()))) {
                      LOG_WARN("failed to append fmt", K(ret));
                    } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
                      LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
                    } else {
                      cur_cnt = 0;
                      values_list.reuse();
                      LOG_TRACE("succeed to backup no table part stats", K(raw_sql), K(affected_rows));
                    }
                  }
                }
              }
            } else {
              LOG_WARN("failed to get map", K(ret), K(key));
            }
          }
        }
      }
      if (OB_SUCC(ret) && cur_cnt > 0) {
        if (OB_SUCC(ret)) {
          ObSqlString raw_sql;
          int64_t affected_rows = 0;
          if (OB_FAIL(raw_sql.append_fmt(INSERT_COLUMN_STAT_HISTORY,
                                         share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                         data_version < DATA_VERSION_4_3_0_0 ? " " : ",cg_macro_blk_cnt, cg_micro_blk_cnt",
                                         values_list.ptr()))) {
            LOG_WARN("failed to append fmt", K(ret));
          } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
            LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
          } else {
            LOG_TRACE("succeed to backup no table part stats", K(raw_sql), K(affected_rows));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::backup_histogram_stats(ObMySQLTransaction &trans,
                                                      const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      const bool is_specify_partition_gather,
                                                      const bool is_specify_column_gather,
                                                      const ObIArray<int64_t> &partition_ids,
                                                      const ObIArray<uint64_t> &column_ids,
                                                      hash::ObHashMap<ObOptColumnStat::Key, bool> &having_stat_part_col_map,
                                                      const int64_t saving_time)
{
  int ret = OB_SUCCESS;
  if (having_stat_part_col_map.size() != 0) {//only process have stat part col.
    ObSqlString raw_sql;
    ObSqlString where_str;
    ObSqlString extra_partition_str;
    ObSqlString extra_column_str;
    ObSqlString extra_where_str;
    ObSqlString partition_list;
    ObSqlString column_list;
    int64_t affected_rows = 0;
    if (is_specify_partition_gather && OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
      LOG_WARN("failed to gen partition list", K(ret));
    } else if (is_specify_partition_gather &&
               OB_FAIL(extra_partition_str.append_fmt(" and partition_id in %s", partition_list.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (is_specify_column_gather && OB_FAIL(gen_column_list(column_ids, column_list))) {
      LOG_WARN("failed to gen column list", K(ret));
    } else if (is_specify_column_gather &&
               OB_FAIL(extra_column_str.append_fmt(" and column_id in %s", column_list.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if ((is_specify_partition_gather || is_specify_column_gather) &&
               OB_FAIL(extra_where_str.append_fmt("%s%s",
                                                  is_specify_partition_gather ? extra_partition_str.ptr() : " ",
                                                  is_specify_column_gather ? extra_column_str.ptr() : " "))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_FAIL(where_str.append_fmt(" tenant_id = %lu and table_id = %lu %s",
                                            share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                            share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                            (is_specify_partition_gather || is_specify_column_gather) ? extra_where_str.ptr() : " "))) {
        LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_FAIL(raw_sql.append_fmt(INSERT_HISTOGRAM_STAT_HISTORY,
                                          share::OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
                                          saving_time,
                                          share::OB_ALL_HISTOGRAM_STAT_TNAME,
                                          where_str.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
    } else {
      LOG_TRACE("succeed to backup having column stats", K(raw_sql), K(affected_rows));
    }
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
  bool only_delete_one_batch = false;
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
  //Attempt to delete all opt stats at once. Since this is a synchronous operation,
  //to avoid impacting user feedback, a batch of data will be synchronously deleted first,
  //with the remaining data being cleaned up by an asynchronous task.
  } else if (specify_time == 0) {//try delete all statistics history
    if (OB_FAIL(time_str.append(" "))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(gather_time_str.append(" "))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      only_delete_one_batch = true;
    }
  } else if (OB_FAIL(time_str.append_fmt("WHERE savtime < usec_to_time(%ld)", specify_time))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(gather_time_str.append_fmt("WHERE start_time < usec_to_time(%ld)", specify_time))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {/*do nothing*/}
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = session->get_effective_tenant_id();
    int64_t start_time = ObTimeUtility::current_time();
    int64_t max_duration_time = BATCH_DELETE_MAX_QUERY_TIMEOUT;
    if (THIS_WORKER.is_timeout_ts_valid()) {
      max_duration_time = std::min(max_duration_time, THIS_WORKER.get_timeout_remain());
    }
    int64_t delete_flags = ObOptStatsDeleteFlags::DELETE_ALL;
    do {
      ObMySQLTransaction trans;
      ObMySQLTransaction trans1;
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", KR(ret));
      } else if (OB_FAIL(trans.start(ctx.get_sql_proxy(), tenant_id))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(trans1.start(ctx.get_sql_proxy(), gen_meta_tenant_id(tenant_id)))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_TAB_STAT_HISTORY) &&
                 OB_FAIL(do_delete_expired_stat_history(trans, tenant_id, start_time,
                                                        max_duration_time, time_str.ptr(),
                                                        share::OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                                        ObOptStatsDeleteFlags::DELETE_TAB_STAT_HISTORY,
                                                        delete_flags))) {
        LOG_WARN("failed to do delete expired stat history", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_COL_STAT_HISTORY) &&
                 OB_FAIL(do_delete_expired_stat_history(trans, tenant_id, start_time,
                                                        max_duration_time, time_str.ptr(),
                                                        share::OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                                        ObOptStatsDeleteFlags::DELETE_COL_STAT_HISTORY,
                                                        delete_flags))) {
        LOG_WARN("failed to do delete expired stat history", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_HIST_STAT_HISTORY) &&
                 OB_FAIL(do_delete_expired_stat_history(trans, tenant_id, start_time,
                                                        max_duration_time, time_str.ptr(),
                                                        share::OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
                                                        ObOptStatsDeleteFlags::DELETE_HIST_STAT_HISTORY,
                                                        delete_flags))) {
        LOG_WARN("failed to do delete expired stat history", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_TASK_GATHER_HISTORY) &&
                 OB_FAIL(do_delete_expired_stat_history(trans1, gen_meta_tenant_id(tenant_id), start_time,
                                                        max_duration_time, gather_time_str.ptr(),
                                                        share::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
                                                        ObOptStatsDeleteFlags::DELETE_TASK_GATHER_HISTORY,
                                                        delete_flags))) {
        LOG_WARN("failed to do delete expired stat history", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_TAB_GATHER_HISTORY) &&
                 OB_FAIL(do_delete_expired_stat_history(trans1, gen_meta_tenant_id(tenant_id), start_time,
                                                        max_duration_time, gather_time_str.ptr(),
                                                        share::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
                                                        ObOptStatsDeleteFlags::DELETE_TAB_GATHER_HISTORY,
                                                        delete_flags))) {
        LOG_WARN("failed to do delete expired stat history", K(ret));
      } else if ((delete_flags & ObOptStatsDeleteFlags::DELETE_USELESS_COL_STAT ||
                  delete_flags & ObOptStatsDeleteFlags::DELETE_USELESS_HIST_STAT) &&
                 OB_FAIL(remove_useless_column_stats(trans, tenant_id, start_time, max_duration_time, delete_flags))) {
        LOG_WARN("failed to remove useless column stats", K(ret));
      }
      if (OB_SUCC(ret)) {
        int tmp_ret1 = OB_SUCCESS;
        int tmp_ret2 = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret1 = trans.end(true))) {
          LOG_WARN("fail to commit transaction", K(tmp_ret1));
        }
        if (OB_SUCCESS != (tmp_ret2 = trans1.end(true))) {
          LOG_WARN("fail to commit transaction", K(tmp_ret2));
        }
        ret = tmp_ret1 != OB_SUCCESS ? tmp_ret1 : tmp_ret2;
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("fail to roll back transaction", K(tmp_ret));
        }
        if (OB_SUCCESS != (tmp_ret = trans1.end(false))) {
          LOG_WARN("fail to roll back transaction", K(tmp_ret));
        }
      }
    } while(OB_SUCC(ret) && !only_delete_one_batch && delete_flags != ObOptStatsDeleteFlags::DELETE_NONE);
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
  //TODO, we need split it, avoiding the column stat is too many and the memory isn't enough.
  if (OB_FAIL(fetch_table_stat_histrory(ctx, param, specify_time, all_tstats))) {
    LOG_WARN("failed to fetch table stat histrory", K(ret));
  } else if (OB_FAIL(fetch_column_stat_history(ctx, param, specify_time, all_cstats))) {
    LOG_WARN("failed to fetch column stat history", K(ret));
  } else if (all_tstats.empty() && all_cstats.empty()) {
    //do nothing
  } else {
    ObMySQLTransaction trans;
    //begin trans
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(backup_opt_stats(ctx, trans, param, ObTimeUtility::current_time()))) {
      LOG_WARN("failed to backup opt stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(), all_tstats, all_cstats))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
    //end trans
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
  uint64_t data_version = 0;
  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy()) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(param));
  } else if (OB_FAIL(gen_partition_list(param, partition_list))) {
    LOG_WARN("failed to gen partition list", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_COL_STATS_HISTROY,
                                        data_version < DATA_VERSION_4_3_0_0 ? " ": ",cg_macro_blk_cnt, cg_micro_blk_cnt",
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
          if (OB_FAIL(fill_column_stat_history(*param.allocator_, *client_result, col_stat, data_version >= DATA_VERSION_4_3_0_0))) {
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
                                                        ObOptColumnStat *&col_stat,
                                                        bool need_cg_info)
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
          if (OB_FAIL(ObOptStatSqlService::get_decompressed_llc_bitmap(allocator, bitmap_compress_lib_name[compress_type],
                                                                       bitmap_buf, bitmap_size,
                                                                       decomp_buf, decomp_size))) {
            COMMON_LOG(WARN, "decompress bitmap buffer failed.", K(ret));
          } else {
            col_stat->set_llc_bitmap(decomp_buf, decomp_size);
          }
        }
      }
    }
    if (OB_SUCC(ret) && need_cg_info) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, cg_macro_blk_cnt, *col_stat, int64_t, true, true, 0);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, cg_micro_blk_cnt, *col_stat, int64_t, true, true, 0);
      //will be used in the future, not removed.
      // if (OB_SUCC(ret)) {
      //   if (OB_FAIL(result.get_type("cg_skip_rate", obj_type))) {
      //     LOG_WARN("failed to get type", K(ret));
      //   } else if (OB_LIKELY(obj_type.is_double())) {
      //     EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, cg_skip_rate, *col_stat, int64_t);
      //   } else {
      //     EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, cg_skip_rate, *col_stat, int64_t);
      //   }
      // }
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
    for (int64_t i = 0; OB_SUCC(ret) && i < param.approx_part_infos_.count(); ++i) {
      if (OB_FAIL(partition_ids.push_back(param.approx_part_infos_.at(i).part_id_))) {
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
    } else if (OB_FAIL(gen_partition_list(partition_ids, partition_list))) {
      LOG_WARN("failed to gen partition list", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::gen_partition_list(const ObIArray<int64_t> &partition_ids,
                                                  ObSqlString &partition_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == partition_ids.count() - 1 ? ')' : ',');
    if (OB_FAIL(partition_list.append_fmt("%c%ld%c", prefix, partition_ids.at(i), suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsHistoryManager::gen_column_list(const ObIArray<uint64_t> &column_ids,
                                               ObSqlString &column_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == column_ids.count() - 1 ? ')' : ',');
    if (OB_FAIL(column_list.append_fmt("%c%lu%c", prefix, column_ids.at(i), suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

//here we remove useless column stats, because drop table won't delete column stats.
int ObDbmsStatsHistoryManager::remove_useless_column_stats(ObMySQLTransaction &trans,
                                                           uint64_t tenant_id,
                                                           const uint64_t start_time,
                                                           const uint64_t max_duration_time,
                                                           int64_t &delete_flags)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_col_stat_sql;
  ObSqlString delete_hist_stat_sql;
  int64_t affected_rows = 0;
  int64_t query_timeout = 0;
  if (delete_flags & ObOptStatsDeleteFlags::DELETE_USELESS_COL_STAT) {
    if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(start_time,
                                                          max_duration_time,
                                                          query_timeout))) {
      LOG_WARN("failed to get valid duration time", K(ret));
    } else if (OB_FAIL(delete_col_stat_sql.append_fmt("DELETE /*+QUERY_TIMEOUT(%ld)*/ FROM %s c WHERE (NOT EXISTS (SELECT 1 " \
                                                      "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
                                                      "AND t.table_id = c.table_id AND t.tenant_id = c.tenant_id AND db.database_name != '__recyclebin')) "\
                                                      "AND table_id > %ld limit %ld;",
                                                      query_timeout,
                                                      share::OB_ALL_COLUMN_STAT_TNAME,
                                                      share::OB_ALL_TABLE_TNAME,
                                                      share::OB_ALL_DATABASE_TNAME,
                                                      OB_MAX_INNER_TABLE_ID,
                                                      BATCH_DELETE_MAX_ROWCNT))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, delete_col_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(delete_col_stat_sql), K(ret));
    } else {
      delete_flags = affected_rows >= BATCH_DELETE_MAX_ROWCNT ? delete_flags : (delete_flags & ~ObOptStatsDeleteFlags::DELETE_USELESS_COL_STAT);
      LOG_TRACE("succeed to clean useless col stat", K(tenant_id), K(delete_col_stat_sql),
                                                     K(affected_rows),K(delete_flags));
    }
  }
  if (OB_SUCC(ret) && delete_flags & ObOptStatsDeleteFlags::DELETE_USELESS_HIST_STAT) {
    if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(start_time,
                                                          max_duration_time,
                                                          query_timeout))) {
      LOG_WARN("failed to get valid duration time", K(ret));
    } else if (OB_FAIL(delete_hist_stat_sql.append_fmt("DELETE /*+QUERY_TIMEOUT(%ld)*/ FROM %s hist WHERE (NOT EXISTS (SELECT 1 " \
                                                       "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
                                                       "AND t.table_id = hist.table_id AND t.tenant_id = hist.tenant_id AND db.database_name != '__recyclebin')) "\
                                                       "AND table_id > %ld limit %ld;",
                                                       query_timeout,
                                                       share::OB_ALL_HISTOGRAM_STAT_TNAME,
                                                       share::OB_ALL_TABLE_TNAME,
                                                       share::OB_ALL_DATABASE_TNAME,
                                                       OB_MAX_INNER_TABLE_ID,
                                                       BATCH_DELETE_MAX_ROWCNT))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, delete_hist_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(delete_hist_stat_sql), K(ret));
    } else {
      delete_flags = affected_rows >= BATCH_DELETE_MAX_ROWCNT ? delete_flags : (delete_flags & ~ObOptStatsDeleteFlags::DELETE_USELESS_HIST_STAT);
      LOG_TRACE("succeed to clean useless hist stat", K(tenant_id), K(delete_hist_stat_sql),
                                                      K(affected_rows),K(delete_flags));
    }
  }
  return ret;
}

int ObDbmsStatsHistoryManager::do_delete_expired_stat_history(ObMySQLTransaction &trans,
                                                              const uint64_t tenant_id,
                                                              const uint64_t start_time,
                                                              const uint64_t max_duration_time,
                                                              const char* specify_time_str,
                                                              const char* process_table_name,
                                                              ObOptStatsDeleteFlags cur_delete_flag,
                                                              int64_t &delete_flags)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  ObSqlString delete_sql;
  int64_t affected_rows;
  if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(start_time,
                                                        max_duration_time,
                                                        query_timeout))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(delete_sql.append_fmt(DELETE_STAT_HISTORY,
                                           query_timeout,
                                           process_table_name,
                                           specify_time_str,
                                           BATCH_DELETE_MAX_ROWCNT))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(delete_sql));
  } else if (OB_FAIL(trans.write(tenant_id, delete_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(delete_sql), K(ret));
  } else {
    delete_flags = affected_rows >= BATCH_DELETE_MAX_ROWCNT ? delete_flags : (delete_flags & ~cur_delete_flag);
    LOG_TRACE("Succeed to do delete expired stat history", K(tenant_id), K(delete_sql), K(affected_rows),
                                                           K(cur_delete_flag), K(delete_flags));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
