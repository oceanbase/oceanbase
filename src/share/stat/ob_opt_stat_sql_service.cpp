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
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "common/ob_partition_key.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"

#define FETCH_ALL_COLUMN_HISTORY_SQL      \
  "SELECT column_id, is_deleted FROM %s " \
  "WHERE tenant_id = %lu AND table_id = %lu AND schema_version = %lu"

#define ALL_DDL_OPERATION_COLUMN_NAME \
  "schema_version, "                  \
  "tenant_id, "                       \
  "user_id, "                         \
  "table_id, "                        \
  "table_name, "                      \
  "database_id, "                     \
  "database_name, "                   \
  "tablegroup_id, "                   \
  "operation_type "

#define ALL_HISTOGRAM_STAT_COLUMN_NAME \
  "endpoint_num, "                     \
  "b_endpoint_value,"                  \
  "endpoint_repeat_cnt"

#define FETCH_ALL_DDL_OPERATION_SQL_WITH_VERSION_RANGE \
  "SELECT " ALL_DDL_OPERATION_COLUMN_NAME " FROM %s WHERE schema_version > %lu AND schema_version <= %lu"

#define ALL_COLUMN_STAT_COLUMN_NAME \
  "tenant_id, "                     \
  "table_id, "                      \
  "partition_id, "                  \
  "column_id, "                     \
  "distinct_cnt, "                  \
  "null_cnt,"                       \
  "max_value, "                     \
  "min_value,"                      \
  "histogram_type,"                 \
  "bucket_cnt,"                     \
  "density"

#define ALL_COLUMN_STATISTICS "__all_column_stat"
#define ALL_TABLE_STATISTICS "__all_table_stat"
#define ALL_HISTOGRAM_STATISTICS "__all_histogram_stat"

#define USER_TAB_COL_STATISTICS "__all_column_statistic"
#define ALL_COL_STAT_COLUMN_NAME \
  "tenant_id, "                  \
  "table_id, "                   \
  "partition_id, "               \
  "column_id, "                  \
  "num_distinct, "               \
  "num_null,"                    \
  "min_value, "                  \
  "max_value"

#define FETCH_COL_STAT_SQL "SELECT " ALL_COLUMN_STAT_COLUMN_NAME " FROM __all_column_stat"
#define FETCH_COL_STAT_SQL_TMP "SELECT " ALL_COL_STAT_COLUMN_NAME " FROM " USER_TAB_COL_STATISTICS

#define FETCH_HISTOGRAM_STAT_SQL "SELECT " ALL_HISTOGRAM_STAT_COLUMN_NAME " FROM " ALL_HISTOGRAM_STATISTICS

#define ALL_TABLE_STAT_COLUMN_NAME \
  "tenant_id, "                    \
  "table_id, "                     \
  "partition_id, "                 \
  "partition_cnt, "                \
  "row_count, "                    \
  "data_size,"                     \
  "data_version "

#define INSERT_TABLE_STAT_SQL                \
  "REPLACE INTO __all_table_stat(tenant_id," \
  "table_id,"                                \
  "partition_id,"                            \
  "object_type,"                             \
  "last_analyzed,"                           \
  "sstable_row_cnt,"                         \
  "sstable_avg_row_len,"                     \
  "macro_blk_cnt,"                           \
  "micro_blk_cnt,"                           \
  "memtable_row_cnt,"                        \
  "memtable_avg_row_len) VALUES "

#define INSERT_COL_STAT_SQL                   \
  "REPLACE INTO __all_column_stat(tenant_id," \
  "table_id,"                                 \
  "partition_id,"                             \
  "column_id,"                                \
  "object_type,"                              \
  "last_analyzed,"                            \
  "distinct_cnt,"                             \
  "null_cnt,"                                 \
  "max_value,"                                \
  "b_max_value,"                              \
  "min_value,"                                \
  "b_min_value,"                              \
  "avg_len,"                                  \
  "distinct_cnt_synopsis,"                    \
  "distinct_cnt_synopsis_size,"               \
  "sample_size,"                              \
  "density,"                                  \
  "bucket_cnt,"                               \
  "histogram_type) VALUES "

#define INSERT_HISTOGRAM_STAT_SQL               \
  "INSERT INTO __all_histogram_stat(tenant_id," \
  "table_id,"                                   \
  "partition_id,"                               \
  "column_id,"                                  \
  "object_type,"                                \
  "endpoint_num,"                               \
  "endpoint_value,"                             \
  "b_endpoint_value,"                           \
  "endpoint_repeat_cnt) VALUES "

#define DELETE_HISTOGRAM_STAT_SQL "DELETE FROM __all_histogram_stat WHERE "
#define DELETE_COL_STAT_SQL "DELETE FROM __all_column_stat WHERE "

#define DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(sql_client)  \
  const bool did_use_weak = false;                         \
  const bool did_use_retry = false;                        \
  const int64_t snapshot_timestamp = OB_INVALID_TIMESTAMP; \
  const bool check_sys_variable = false;                   \
  ObSQLClientRetryWeak sql_client_retry_weak(              \
      sql_client, did_use_weak, did_use_retry, snapshot_timestamp, check_sys_variable);

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace common {
ObOptStatSqlService::ObOptStatSqlService() : inited_(false), mysql_proxy_(nullptr), config_(nullptr)
{}

ObOptStatSqlService::~ObOptStatSqlService()
{}

int ObOptStatSqlService::init(ObMySQLProxy* proxy, ObServerConfig* config)
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

int ObOptStatSqlService::fetch_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& stat)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    uint64_t tenant_id = extract_tenant_id(key.table_id_);
    uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql service has not been initialized.", K(ret));
    } else {
      // TODO for now, we only need 'last_analyzed' column data
      if (OB_FAIL(sql.append_fmt("SELECT last_analyzed FROM %s ", ALL_TABLE_STATISTICS))) {
        LOG_WARN("fail to append SQL stmt string.", K(sql), K(ret));
      } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld AND PARTITION_ID=%ld",
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_),
                     key.partition_id_))) {
        LOG_WARN("fail to append SQL where string.", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("result next failed, ", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else if (OB_FAIL(fill_table_stat(*result, stat))) {
        LOG_WARN("read stat from result failed. ", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::update_column_stat(const ObIArray<ObOptColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  ObSqlString temp_sql;
  ObSqlString insert_histogram_stat_sql;
  ObSqlString delete_histogram_stat_sql;
  ObSqlString column_stat_sql;
  ObSqlString table_stat_sql;
  ObArenaAllocator allocator(ObModIds::OB_BUFFER);
  int64_t current_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service not inited", K(ret));
  } else if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(column_stat_sql.append(INSERT_COL_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(insert_histogram_stat_sql.append(INSERT_HISTOGRAM_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_histogram_stat_sql.append(DELETE_HISTOGRAM_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else {
    uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
    if (column_stats.count() > 0) {
      if (OB_ISNULL(column_stats.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_stat should not be null", K(ret));
      } else {
        exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(extract_tenant_id(column_stats.at(0)->get_table_id()));
      }
    }
    // construct table stat sql
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      temp_sql.reset();
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(get_table_stat_sql(*column_stats.at(i), current_time, temp_sql))) {
        LOG_WARN("failed to get table stat sql", K(ret));
      } else if (OB_FAIL(table_stat_sql.append_fmt(
                     "(%s)%s", temp_sql.ptr(), (i == column_stats.count() - 1 ? ";" : ",")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // construct column stat sql
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      temp_sql.reset();
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(get_column_stat_sql(*column_stats.at(i), current_time, temp_sql))) {
        LOG_WARN("failed to get column stat", K(ret));
      } else if (OB_FAIL(column_stat_sql.append_fmt(
                     "(%s)%s", temp_sql.ptr(), (i == column_stats.count() - 1 ? ";" : ",")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // construct histogram delete column
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(delete_histogram_stat_sql.append_fmt(
                     "(tenant_id = %lu and table_id = %ld and partition_id = %ld and column_id = %ld) %s ",
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_stats.at(i)->get_table_id()),
                     column_stats.at(i)->get_partition_id(),
                     column_stats.at(i)->get_column_id(),
                     (i == column_stats.count() - 1 ? ";" : "or ")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // construct histogram insert sql
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      ObHistogram* histogram = NULL;
      if (OB_ISNULL(column_stats.at(i)) || OB_ISNULL(histogram = column_stats.at(i)->get_histogram())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(histogram), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < histogram->get_buckets().count(); j++) {
          temp_sql.reset();
          if (OB_ISNULL(histogram->get_buckets().at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(histogram->get_buckets().at(j)), K(ret));
          } else if (OB_FAIL(get_histogram_stat_sql(
                         *column_stats.at(i), allocator, *histogram->get_buckets().at(j), temp_sql))) {
            LOG_WARN("failed to get histogram sql", K(ret));
          } else if (OB_FAIL(insert_histogram_stat_sql.append_fmt("(%s)%s",
                         temp_sql.ptr(),
                         (i == column_stats.count() - 1 && j == histogram->get_buckets().count() - 1 ? ";" : ",")))) {
            LOG_WARN("failed to append sql", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    // update stat table
    if (OB_SUCC(ret)) {
      LOG_DEBUG("sql string of stat update",
          K(table_stat_sql),
          K(column_stat_sql),
          K(delete_histogram_stat_sql),
          K(insert_histogram_stat_sql));
      if (OB_FAIL(trans.start(mysql_proxy_))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, delete_histogram_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to exec sql", K(delete_histogram_stat_sql), K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, insert_histogram_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(insert_histogram_stat_sql), K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, column_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(column_stat_sql), K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, table_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(table_stat_sql), K(ret));
      } else { /*do nothing*/
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
  }
  return ret;
}

int ObOptStatSqlService::delete_column_stat(const ObIArray<ObOptColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  ObSqlString temp_sql;
  ObSqlString table_stat_sql;
  ObSqlString delete_column_stat_sql;
  ObSqlString delete_histogram_stat_sql;
  int64_t current_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service not inited", K(ret));
  } else if (OB_FAIL(table_stat_sql.append(INSERT_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_column_stat_sql.append(DELETE_COL_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(delete_histogram_stat_sql.append(DELETE_HISTOGRAM_STAT_SQL))) {
    LOG_WARN("failed to append sql", K(ret));
  } else {
    uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
    if (column_stats.count() > 0) {
      if (OB_ISNULL(column_stats.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_stat should not be null", K(ret));
      } else {
        exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(extract_tenant_id(column_stats.at(0)->get_table_id()));
      }
    }
    // construct table stat sql
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      temp_sql.reset();
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(get_table_stat_sql(*column_stats.at(i), current_time, temp_sql))) {
        LOG_WARN("failed to get table stat sql", K(ret));
      } else if (OB_FAIL(table_stat_sql.append_fmt(
                     "(%s)%s", temp_sql.ptr(), (i == column_stats.count() - 1 ? ";" : ",")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // construct column stat delete sql
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(delete_column_stat_sql.append_fmt(
                     "(tenant_id = %lu and table_id = %ld and partition_id = %ld and column_id = %ld) %s ",
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_stats.at(i)->get_table_id()),
                     column_stats.at(i)->get_partition_id(),
                     column_stats.at(i)->get_column_id(),
                     (i == column_stats.count() - 1 ? ";" : "or ")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // construct histogram delete column
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_stats.at(i)), K(ret));
      } else if (OB_FAIL(delete_histogram_stat_sql.append_fmt(
                     "(tenant_id = %lu and table_id = %ld and partition_id = %ld and column_id = %ld) %s ",
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_stats.at(i)->get_table_id()),
                     column_stats.at(i)->get_partition_id(),
                     column_stats.at(i)->get_column_id(),
                     (i == column_stats.count() - 1 ? ";" : "or ")))) {
        LOG_WARN("failed to append sql", K(ret));
      } else { /*do nothing*/
      }
    }
    // update table stat
    if (OB_SUCC(ret)) {
      LOG_DEBUG("sql string of stat update", K(delete_column_stat_sql), K(delete_histogram_stat_sql));
      if (OB_FAIL(trans.start(mysql_proxy_))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, delete_histogram_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to exec sql", K(delete_histogram_stat_sql), K(ret));
      } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, delete_column_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(delete_column_stat_sql), K(ret));
      } else if (affected_rows > 0 &&
                 OB_FAIL(mysql_proxy_->write(exec_tenant_id, table_stat_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(table_stat_sql), K(ret));
      } else { /*do nothing*/
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
  }
  return ret;
}

int ObOptStatSqlService::get_table_stat_sql(
    const ObOptColumnStat& stat, const int64_t current_time, ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = stat.get_table_id();
  uint64_t tenant_id = extract_tenant_id(table_id);
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
      OB_FAIL(dml_splicer.add_column("object_type", stat.get_stat_level())) ||
      OB_FAIL(dml_splicer.add_time_column("last_analyzed", current_time)) ||
      OB_FAIL(dml_splicer.add_column("sstable_row_cnt", 0)) ||
      OB_FAIL(dml_splicer.add_column("sstable_avg_row_len", 0)) ||
      OB_FAIL(dml_splicer.add_column("macro_blk_cnt", 0)) || OB_FAIL(dml_splicer.add_column("micro_blk_cnt", 0)) ||
      OB_FAIL(dml_splicer.add_column("memtable_row_cnt", 0)) ||
      OB_FAIL(dml_splicer.add_column("memtable_avg_row_len", 0))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObOptStatSqlService::get_column_stat_sql(
    const ObOptColumnStat& stat, const int64_t current_time, ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = stat.get_table_id();
  uint64_t tenant_id = extract_tenant_id(table_id);
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_ISNULL(stat.get_histogram())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null histogram", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
             OB_FAIL(dml_splicer.add_pk_column("column_id", stat.get_column_id())) ||
             OB_FAIL(dml_splicer.add_column("object_type", stat.get_stat_level())) ||
             OB_FAIL(dml_splicer.add_time_column("last_analyzed", current_time)) ||
             OB_FAIL(dml_splicer.add_column("distinct_cnt", 0)) || OB_FAIL(dml_splicer.add_column("null_cnt", 0)) ||
             OB_FAIL(dml_splicer.add_column("max_value", "")) || OB_FAIL(dml_splicer.add_column("b_max_value", "")) ||
             OB_FAIL(dml_splicer.add_column("min_value", "")) || OB_FAIL(dml_splicer.add_column("b_min_value", "")) ||
             OB_FAIL(dml_splicer.add_column("avg_len", 0)) ||
             OB_FAIL(dml_splicer.add_column("distinct_cnt_synopsis", "")) ||
             OB_FAIL(dml_splicer.add_column("distinct_cnt_synopsis_size", 0)) ||
             OB_FAIL(dml_splicer.add_column("sample_size", stat.get_histogram()->get_sample_size())) ||
             OB_FAIL(dml_splicer.add_column("density", stat.get_histogram()->get_density())) ||
             OB_FAIL(dml_splicer.add_column("bucket_cnt", stat.get_histogram()->get_bucket_cnt())) ||
             OB_FAIL(dml_splicer.add_column("histogram_type", stat.get_histogram()->get_type()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatSqlService::get_histogram_stat_sql(
    const ObOptColumnStat& stat, ObIAllocator& allocator, ObOptColumnStat::Bucket& bucket, ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  ObString endpoint_value;
  ObString b_endpoint_value;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = stat.get_table_id();
  uint64_t tenant_id = extract_tenant_id(table_id);
  uint64_t ext_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(get_obj_str(bucket.endpoint_value_, allocator, endpoint_value))) {
    LOG_WARN("failed to convert obj to string", K(ret));
  } else if (OB_FAIL(get_obj_binary_hex_str(bucket.endpoint_value_, allocator, b_endpoint_value))) {
    LOG_WARN("failed to convert obj to binary string", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
             OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
             OB_FAIL(dml_splicer.add_pk_column("column_id", stat.get_column_id())) ||
             OB_FAIL(dml_splicer.add_column("object_type", stat.get_stat_level())) ||
             OB_FAIL(dml_splicer.add_pk_column("endpoint_num", bucket.endpoint_num_)) ||
             OB_FAIL(dml_splicer.add_column("endpoint_value", ObHexEscapeSqlStr(endpoint_value))) ||
             OB_FAIL(dml_splicer.add_column("b_endpoint_value", b_endpoint_value)) ||
             OB_FAIL(dml_splicer.add_column("endpoint_repeat_cnt", bucket.endpoint_repeat_count_))) {
    LOG_WARN("failed to add dml splice values", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatSqlService::hex_str_to_obj(const char* buf, int64_t buf_len, ObIAllocator& allocator, ObObj& obj)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t ret_len = 0;
  char* resbuf = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (NULL == (resbuf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot allocate memory for deserializing obj.", K(buf_len), K(ret));
  } else if (buf_len != (ret_len = common::str_to_hex(
                             buf, static_cast<int32_t>(buf_len), resbuf, static_cast<int32_t>(buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer str to hex failed", K(buf), K(buf_len), K(ret_len), K(ret));
  } else if (OB_FAIL(obj.deserialize(resbuf, ret_len, pos))) {
    LOG_WARN("deserialize obj failed.", K(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

int ObOptStatSqlService::get_obj_str(const ObObj& obj, ObIAllocator& allocator, ObString& out_str)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (obj.is_string_type()) {
    if (OB_FAIL(obj.print_varchar_literal(buf, buf_len, pos))) {
      LOG_WARN("failed to print sql literal", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos))) {
      LOG_WARN("failed to print_sql_literal", K(ret));
    } else { /*do nothing*/
    }
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

int ObOptStatSqlService::get_obj_binary_hex_str(const ObObj& obj, ObIAllocator& allocator, ObString& out_str)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  char* hex_buf = NULL;
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

int ObOptStatSqlService::fill_table_stat(common::sqlclient::ObMySQLResult& result, ObOptTableStat& stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else {
    int64_t int_value = 0;
    if (OB_SUCCESS != (ret = result.get_timestamp("last_analyzed", NULL, int_value))) {
      LOG_WARN("fail to get column in row. ", "column_name", "last_analyzed", K(ret));
    } else {
      stat.set_last_analyzed(static_cast<int64_t>(int_value));
    }
  }
  return ret;
}

int ObOptStatSqlService::fetch_table_stat(uint64_t table_id, ObOptTableStat& stat)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const uint64_t tenant_id = extract_tenant_id(table_id);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql service is not initialized.", K(ret));
    } else {
      if (OB_FAIL(sql.append_fmt("SELECT last_analyzed, row_count, micro_blk_count, sstable_avg_row_len FROM %s ",
              ALL_TABLE_STATISTICS))) {
        LOG_WARN("fail to append SQL stmt string.", K(sql), K(ret));
      } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld LIMIT 1",
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
        LOG_WARN("fail to append SQL where string.", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("result next failed, ", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else if (OB_FAIL(fill_table_stat(*result, stat))) {
        LOG_WARN("read stat from result failed. ", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::fetch_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStat& stat)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    ObHistogram basic_histogram_info;
    const uint64_t tenant_id = extract_tenant_id(key.table_id_);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql service has not been initialized.", K(ret));
    } else if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments.", K(key), K(ret));
    } else if (OB_FAIL(sql.append(FETCH_COL_STAT_SQL))) {
      LOG_WARN("fail to append SQL stmt string.", K(FETCH_COL_STAT_SQL), K(ret));
    } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID=%lu AND TABLE_ID=%ld AND PARTITION_ID=%ld AND COLUMN_ID=%ld",
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_),
                   key.partition_id_,
                   key.column_id_))) {
      LOG_WARN("fail to append SQL where string.", K(FETCH_COL_STAT_SQL), K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("result next failed, ", K(ret));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_FAIL(fill_column_stat(key.table_id_, *result, stat, basic_histogram_info))) {
      LOG_WARN("read stat from result failed. ", K(key), K(ret));
    } else if (basic_histogram_info.get_type() == ObHistogram::Type::INVALID_TYPE) {
      // do nothing
    } else if (OB_FAIL(fetch_histogram_stat(key, basic_histogram_info, stat))) {
      LOG_WARN("fetch histogram statistics failed. ", K(key), K(ret));
    }
  }
  return ret;
}

int ObOptStatSqlService::fill_column_stat(const uint64_t table_id, common::sqlclient::ObMySQLResult& result,
    ObOptColumnStat& stat, ObHistogram& basic_histogram_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  ObHistogram::Type histogram_type = ObHistogram::Type::INVALID_TYPE;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  }

  stat.set_table_id(table_id);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, stat, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, stat, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, distinct_cnt, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, null_cnt, stat, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "histogram_type", histogram_type, ObHistogram::Type);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, bucket_cnt, basic_histogram_info, int64_t);
  EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, density, basic_histogram_info, double);

  if (OB_SUCC(ret)) {
    basic_histogram_info.set_type(histogram_type);
  }

  ObString str_field;
  common::ObObj obj;
  int64_t pos = 0;

  EXTRACT_VARCHAR_FIELD_MYSQL(result, "min_value", str_field);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj.deserialize(str_field.ptr(), str_field.length(), pos))) {
      LOG_WARN("deserialize_hex_cstr min value failed.", K(stat), K(ret));
    } else if (OB_FAIL(stat.store_min_value(obj))) {
      LOG_WARN("store min value failed.", K(stat), K(ret));
    }
  }

  EXTRACT_VARCHAR_FIELD_MYSQL(result, "max_value", str_field);
  if (OB_SUCC(ret)) {
    pos = 0;
    if (OB_FAIL(obj.deserialize(str_field.ptr(), str_field.length(), pos))) {
      LOG_WARN("deserialize_hex_cstr max value failed.", K(stat), K(ret));
    } else if (OB_FAIL(stat.store_max_value(obj))) {
      LOG_WARN("store max value failed.", K(stat), K(ret));
    }
  }

  UNUSED(tenant_id);
  return ret;
}

int ObOptStatSqlService::fetch_histogram_stat(
    const ObOptColumnStat::Key& key, const ObHistogram& basic_histogram_info, ObOptColumnStat& stat)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;

    const uint64_t tenant_id = extract_tenant_id(key.table_id_);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql service has not been initialized.", K(ret));
    } else if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments.", K(key), K(ret));
    } else if (OB_FAIL(sql.append(FETCH_HISTOGRAM_STAT_SQL))) {
      LOG_WARN("fail to append SQL stmt string.", K(FETCH_HISTOGRAM_STAT_SQL), K(ret));
    } else if (OB_FAIL(sql.append_fmt(
                   " WHERE TENANT_ID=%lu AND TABLE_ID=%ld AND PARTITION_ID=%ld AND COLUMN_ID=%ld ORDER BY ENDPOINT_NUM",
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_),
                   key.partition_id_,
                   key.column_id_))) {
      LOG_WARN("fail to append SQL where string.", K(FETCH_COL_STAT_SQL), K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
    } else if (OB_FAIL(stat.init_histogram(basic_histogram_info))) {
      LOG_WARN("initialize histogram failed", K(ret));
    } else {
      // do nothing
    }

    int64_t last_endpoint_num = 0;
    int64_t res_cnt = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("result next failed", K(ret));
        } else if (res_cnt == 0) {
          // no histogram data
          // Users are allowed to delete histogram data directly, so ignore error
          LOG_DEBUG("no histogram data found when trying to load");
          stat.set_histogram(NULL);
          ret = OB_SUCCESS;
          break;
        } else {
          // finish fetching all data
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(fill_bucket_stat(*result, stat, last_endpoint_num))) {
        LOG_WARN("fill bucket stat failed", K(ret));
      } else {
        ++res_cnt;
      }
    }
  }
  return ret;
}

int ObOptStatSqlService::fill_bucket_stat(
    sqlclient::ObMySQLResult& result, ObOptColumnStat& stat, int64_t& last_endpoint_num)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized.", K(ret));
  } else if (OB_ISNULL(stat.get_histogram())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("histogram is null.", K(ret));
  } else {
    ObArenaAllocator arena(ObModIds::OB_BUFFER);
    ObString str_field;
    ObObj obj;
    int64_t endpoint_num = 0;
    int64_t repeat_count = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "endpoint_num", endpoint_num, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "endpoint_repeat_cnt", repeat_count, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "b_endpoint_value", str_field);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hex_str_to_obj(str_field.ptr(), str_field.length(), arena, obj))) {
        LOG_WARN("deserialize object value failed.", K(stat), K(ret));
      } else if (OB_FAIL(stat.add_bucket(repeat_count, obj, endpoint_num - last_endpoint_num))) {
        LOG_WARN("store endpoint number failed.", K(stat), K(ret));
      } else {
        last_endpoint_num = endpoint_num;
      }
    }
  }
  return ret;
}

}  // end of namespace common
}  // end of namespace oceanbase
