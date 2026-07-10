/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_opt_catalog_stat_sql_service.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/stat/ob_opt_stat_sql_service.h" // For helper functions
#include "share/stat/catalog/ob_opt_catalog_table_stat_builder.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat_builder.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"

#define INSERT_CATALOG_TABLE_STAT_SQL "REPLACE INTO __all_catalog_table_stat(tenant_id," \
                                                                                "catalog_id," \
                                                                                "db_name," \
                                                                                "table_name," \
                                                                                "partition_value," \
                                                                                "schema_version," \
                                                                                "last_analyzed," \
                                                                                "last_modified," \
                                                                                "file_count," \
                                                                                "data_size," \
                                                                                "row_cnt," \
                                                                                "avg_row_len," \
                                                                                "stattype_locked," \
                                                                                "sample_size) VALUES " \

#define REPLACE_CATALOG_COL_STAT_SQL "REPLACE INTO __all_catalog_column_stat(tenant_id," \
                                                                               "catalog_id," \
                                                                               "db_name," \
                                                                               "table_name," \
                                                                               "partition_value," \
                                                                               "column_name," \
                                                                               "last_analyzed," \
                                                                               "last_modified," \
                                                                               "distinct_cnt," \
                                                                               "null_cnt," \
                                                                               "max_value," \
                                                                               "b_max_value," \
                                                                               "min_value," \
                                                                               "b_min_value," \
                                                                               "avg_len," \
                                                                               "distinct_cnt_synopsis," \
                                                                               "distinct_cnt_synopsis_size," \
                                                                               "sample_size," \
                                                                               "density," \
                                                                               "bucket_cnt," \
                                                                               "histogram_type) VALUES "


#define FETCH_ALL_COLUMN_STAT_SQL_COL_PREFIX "SELECT tenant_id, catalog_id, db_name," \
                                                                               "table_name," \
                                                                               "column_name," \
                                                                               "partition_value," \
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
                                                                               "sample_size," \
                                                                               "density," \
                                                                               "bucket_cnt," \
                                                                               "histogram_type " \
                                                                               "FROM oceanbase.__all_catalog_column_stat " \
                                                                               "WHERE tenant_id = %lu AND catalog_id = %lu AND db_name = "

#define FETCH_ALL_COLUMN_STAT_SQL_COL_SUFFIX " ORDER BY tenant_id, catalog_id, db_name, table_name, column_name;"

#define INSERT_CATALOG_TABLE_OPT_STAT_GATHER_SQL "INSERT INTO %s(tenant_id, task_id, catalog_id," \
                                                                               "db_name," \
                                                                               "table_name," \
                                                                               "ret_code," \
                                                                               "start_time," \
                                                                               "end_time," \
                                                                               "memory_used," \
                                                                               "stat_refresh_failed_list," \
                                                                               "properties," \
                                                                               "spare3) VALUES (%s);"
namespace oceanbase
{
namespace common
{

ObOptCatalogStatSqlService::ObOptCatalogStatSqlService() : inited_(false), mysql_proxy_(NULL)
{
}

ObOptCatalogStatSqlService::~ObOptCatalogStatSqlService()
{
}

int ObOptCatalogStatSqlService::init(ObMySQLProxy *mysql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mysql proxy is null", K(ret));
  } else {
    mysql_proxy_ = mysql_proxy;
    inited_ = true;
  }
  return ret;
}

/**
 * @brief ObOptCatalogStatSqlService::update_catalog_table_stat
 * Update catalog table statistics
 * Corresponds to ObOptStatSqlService::update_catalog_table_stat
 *
 * Key differences:
 * - Uses ObOptCatalogTableStat instead of ObOptTableStat
 * - Directly extracts catalog_id, db_name, table_name, partition_value
 *
 * @param tenant_id
 * @param conn
 * @param catalog_table_stat
 * @param table_param
 * @return
 */
int ObOptCatalogStatSqlService::update_catalog_table_stat(
    const uint64_t tenant_id,
    const share::ObOptCatalogTableStat *catalog_table_stat,
    const ObCatalogTableStatParam &table_param,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObSqlString table_stat_sql;
  ObSqlString tmp;
  int64_t current_time = ObTimeUtility::current_time();
  int64_t affected_rows = 0;
  ObString partition_value = catalog_table_stat->get_partition_value();

  if (OB_ISNULL(catalog_table_stat) || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog table stat or conn is null", K(ret), K(catalog_table_stat), K(conn));
  } else if (OB_FAIL(table_stat_sql.append_fmt(INSERT_CATALOG_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append catalog table stat sql", K(ret));
  } else if (OB_FAIL(get_catalog_table_stat_sql(tenant_id,
                                                *catalog_table_stat,
                                                table_param,
                                                current_time,
                                                false,
                                                tmp))) {
    LOG_WARN("failed to get catalog table stat sql", K(ret));
  } else if (OB_FAIL(table_stat_sql.append_fmt("(%s);", tmp.ptr()))) {
    LOG_WARN("failed to append table stat sql", K(ret));
  } else if (OB_FAIL(conn->execute_write(tenant_id, table_stat_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(table_stat_sql.string()), K(affected_rows));
  }
  return ret;
}

/**
 * @brief ObOptCatalogStatSqlService::update_catalog_column_stat
 * Update external column statistics
 * Corresponds to ObOptStatSqlService::update_catalog_column_stat
 *
 * Key differences:
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 * - Directly extracts catalog_id, db_name, table_name, partition_value, column_name
 *
 * @param schema_guard
 * @param exec_tenant_id
 * @param allocator
 * @param conn
 * @param catalog_column_stats
 * @param column_params
 * @param table_param
 * @param print_params
 * @return
 */
int ObOptCatalogStatSqlService::update_catalog_column_stat(
    const uint64_t exec_tenant_id,
    const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    const ObCatalogTableStatParam &table_param,
    const ObObjPrintParams &print_params,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();
  ObSqlString column_stats_sql;

  if (!inited_ || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(conn), K(inited_));
  } else if (OB_UNLIKELY(catalog_column_stats.empty()) || OB_ISNULL(catalog_column_stats.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external column stats is empty", K(ret));
  } else if (catalog_column_stats.count() != column_params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external column stats and params count mismatch",
             K(ret),
             K(catalog_column_stats.count()),
             K(column_params.count()));
  } else if (OB_FAIL(construct_catalog_column_stat_sql(exec_tenant_id,
                                                       catalog_column_stats,
                                                       column_params,
                                                       table_param,
                                                       current_time,
                                                       print_params,
                                                       schema_guard,
                                                       allocator,
                                                       column_stats_sql))) {
    LOG_WARN("failed to construct external column stat sql", K(ret));
  } else if (OB_FAIL(conn->execute_write(exec_tenant_id, column_stats_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(column_stats_sql));
  }
  return ret;
}

int ObOptCatalogStatSqlService::build_fetch_table_stat_sql(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const bool is_partitioned,
    const ObIArray<ObString> &partition_values,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // build common SELECT and WHERE clause
  if (OB_FAIL(sql.append_fmt(
          "SELECT partition_value, last_analyzed, file_count, data_size, row_cnt, avg_row_len "
          "FROM oceanbase.%s "
          "WHERE tenant_id = %lu AND catalog_id = %lu ",
          share::OB_ALL_CATALOG_TABLE_STAT_TNAME,
          share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          catalog_id))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(sql.append(" AND db_name = "))) {
    LOG_WARN("failed to append db name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(database_name, sql))) {
    LOG_WARN("failed to append database name", K(ret), K(database_name));
  } else if (OB_FAIL(sql.append(" AND table_name = "))) {
    LOG_WARN("failed to append table name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
    LOG_WARN("failed to append table name", K(ret), K(table_name));
  } else if (!is_partitioned || partition_values.empty()) {
    // non-partitioned table or fetch all partitions: no additional condition
  } else if (partition_values.count() == 1) {
    // single partition
    if (OB_FAIL(sql.append(" AND partition_value = "))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(partition_values.at(0), sql))) {
      LOG_WARN("failed to append partition value", K(ret));
    }
  } else {
    // multiple partitions: use IN clause
    if (OB_FAIL(sql.append(" AND partition_value IN ("))) {
      LOG_WARN("failed to append sql", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
      if (i > 0 && OB_FAIL(sql.append(", "))) {
        LOG_WARN("failed to append comma", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(partition_values.at(i), sql))) {
        LOG_WARN("failed to append partition value", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
      LOG_WARN("failed to append sql", K(ret));
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::parse_table_stat_row(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    sqlclient::ObMySQLResult &result,
    ObIAllocator &allocator,
    share::ObOptCatalogTableStat *&catalog_table_stat)
{
  int ret = OB_SUCCESS;
  catalog_table_stat = nullptr;

  int64_t schema_version = 0;
  ObString partition_value;
  int64_t last_analyzed = 0;
  int64_t file_count = 0;
  int64_t data_size = 0;
  int64_t row_cnt = 0;
  double avg_row_len_double = 0.0;

  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "schema_version", schema_version, int64_t);

  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "partition_value", partition_value);
  if (OB_FAIL(result.get_timestamp("last_analyzed", nullptr, last_analyzed))) {
    if (OB_ERR_NULL_VALUE == ret) {
      last_analyzed = 0;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to extract last_analyzed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "file_count", file_count, int64_t);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "data_size", data_size, int64_t);
  }

  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "row_cnt", row_cnt, int64_t);
  EXTRACT_DOUBLE_FIELD_MYSQL_SKIP_RET(result, "avg_row_len", avg_row_len_double, double);

  if (OB_FAIL(ret)) {
  } else {
    ObOptCatalogTableStatBuilder builder;
    if (OB_FAIL(builder.set_basic_info(tenant_id,
                                       catalog_id,
                                       database_name,
                                       table_name,
                                       partition_value))) {
      LOG_WARN("failed to set basic info", K(ret));
    } else if (OB_FAIL(builder.set_stat_info(schema_version,
                                             row_cnt,
                                             file_count,
                                             data_size,
                                             last_analyzed))) {
      LOG_WARN("failed to set stat info", K(ret));
    } else if (OB_FAIL(builder.build(allocator, catalog_table_stat))) {
      LOG_WARN("failed to build catalog table stat", K(ret));
    } else if (OB_ISNULL(catalog_table_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog table stat is null", K(ret));
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::fetch_catalog_table_stat_from_system_table(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &partition_values,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogTableStat *> &external_table_stats)
{
  int ret = OB_SUCCESS;
  external_table_stats.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", K(ret));
  } else if (database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database name or table name is empty", K(ret), K(database_name), K(table_name));
  }

  ObSqlString sql;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_fetch_table_stat_sql(tenant_id,
                                                catalog_id,
                                                database_name,
                                                table_name,
                                                table_schema->is_partitioned_table(),
                                                partition_values,
                                                sql))) {
    LOG_WARN("failed to build fetch table stat sql", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(ret));
      }
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        share::ObOptCatalogTableStat *stat = nullptr;
        if (OB_FAIL(parse_table_stat_row(tenant_id,
                                         catalog_id,
                                         database_name,
                                         table_name,
                                         *result,
                                         allocator,
                                         stat))) {
          LOG_WARN("failed to parse table stat row", K(ret));
        } else if (OB_FAIL(external_table_stats.push_back(stat))) {
          LOG_WARN("failed to push back stat", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(external_table_stats.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("table statistics not found", K(ret), K(tenant_id), K(catalog_id),
             K(database_name), K(table_name), K(partition_values));
  }
  return ret;
}

int ObOptCatalogStatSqlService::fetch_catalog_column_stats_from_system_table(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const share::schema::ObTableSchema *table_schema,
    const bool skip_partition_column_filter,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;
  catalog_column_stats.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not been initialized", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", K(ret));
  } else if (column_names.empty()) {
    // no columns to fetch, return success
  }

  ObSEArray<ObString, 8> filtered_column_names;
  if (OB_FAIL(ret)) {
  } else if (skip_partition_column_filter && OB_FAIL(filtered_column_names.assign(column_names))) {
    LOG_WARN("failed to assign column names", K(ret), K(column_names.count()));
  } else if (!skip_partition_column_filter && OB_FAIL(filter_partition_columns(table_schema, column_names, filtered_column_names))) {
    LOG_WARN("failed to filter partition columns", K(ret));
  } else if (filtered_column_names.empty()) {
    // all columns are partition columns, return success
  } else {
    ObSqlString sql;
    if (OB_FAIL(build_column_stat_sql(tenant_id,
                                      catalog_id,
                                      database_name,
                                      table_name,
                                      table_schema,
                                      partition_values,
                                      filtered_column_names,
                                      allocator,
                                      sql))) {
      LOG_WARN("failed to build column stat sql", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else {
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("failed to execute sql", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", K(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          share::ObOptCatalogColumnStat *col_stat = nullptr;
          if (OB_FAIL(parse_column_stat_row(tenant_id,
                                            catalog_id,
                                            database_name,
                                            table_name,
                                            *result,
                                            allocator,
                                            col_stat))) {
            LOG_WARN("failed to parse column stat row", K(ret));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column stat is null", K(ret));
          } else if (OB_FAIL(catalog_column_stats.push_back(col_stat))) {
            LOG_WARN("failed to push back column stat", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    // verify all required columns are found
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(catalog_column_stats.empty()) && !filtered_column_names.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("column statistics not found",
               K(ret),
               K(tenant_id),
               K(catalog_id),
               K(database_name),
               K(table_name));
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::get_catalog_table_stat_sql(
    const uint64_t tenant_id,
    const share::ObOptCatalogTableStat &catalog_table_stat,
    const ObCatalogTableStatParam &table_param,
    const int64_t current_time,
    bool is_delete,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  const uint64_t ext_tenant_id =
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);

  // Get partition_value from catalog_table_stat
  // Ensure empty string instead of NULL (partition_value column cannot be NULL)
  ObString partition_value = catalog_table_stat.get_partition_value();
  if (partition_value.ptr() == nullptr) {
    partition_value = ObString("");
  }

  // Align with ObOptStatSqlService::get_table_stat_sql: inner rowkey tenant_id via extract
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id))
      || OB_FAIL(dml_splicer.add_pk_column("catalog_id", catalog_table_stat.get_catalog_id()))
      || OB_FAIL(dml_splicer.add_pk_column("db_name", catalog_table_stat.get_database_name()))
      || OB_FAIL(dml_splicer.add_pk_column("table_name", catalog_table_stat.get_table_name()))
      || OB_FAIL(dml_splicer.add_pk_column("partition_value", ObHexEscapeSqlStr(partition_value)))
      || OB_FAIL(dml_splicer.add_column("schema_version", catalog_table_stat.get_schema_version()))
      || OB_FAIL(
          dml_splicer.add_time_column("last_analyzed", catalog_table_stat.get_last_analyzed()))
      || OB_FAIL(dml_splicer.add_time_column("last_modified", current_time))
      || OB_FAIL(dml_splicer.add_column("file_count", catalog_table_stat.get_file_num()))
      || OB_FAIL(dml_splicer.add_column("data_size", catalog_table_stat.get_data_size()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column("row_cnt", catalog_table_stat.get_row_count()))
             || OB_FAIL(dml_splicer.add_column("avg_row_len", catalog_table_stat.get_avg_row_len()))
             || OB_FAIL(dml_splicer.add_column("stattype_locked", table_param.gather_options_.stattype_))
             || OB_FAIL(
                 dml_splicer.add_column("sample_size", catalog_table_stat.get_sample_size()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql))) {
    LOG_WARN("failed to get sql string", K(ret));
  }

  return ret;
}

int ObOptCatalogStatSqlService::construct_catalog_column_stat_sql(
    const uint64_t exec_tenant_id,
    const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    const ObCatalogTableStatParam &table_param,
    const int64_t current_time,
    const ObObjPrintParams &print_params,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator,
    ObSqlString &column_stats_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  ObObjMeta min_meta;
  ObObjMeta max_meta;
  uint64_t data_version = 0;

  if (catalog_column_stats.count() != column_params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external column stats and params count mismatch",
             K(ret),
             K(catalog_column_stats.count()),
             K(column_params.count()));
  } else if (OB_FAIL(get_column_stat_min_max_meta(exec_tenant_id,
                                                  share::OB_ALL_CATALOG_COLUMN_STAT_TID,
                                                  &schema_guard,
                                                  min_meta,
                                                  max_meta))) {
    LOG_WARN("failed to get column stat min max meta", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(exec_tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < catalog_column_stats.count(); i++) {
    tmp.reset();
    if (OB_ISNULL(catalog_column_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("external column stat is null", K(ret));
    } else if (i == 0 && OB_FAIL(column_stats_sql.append_fmt(REPLACE_CATALOG_COL_STAT_SQL))) {
      LOG_WARN("failed to append external column stat sql", K(ret));
    } else if (OB_FAIL(get_catalog_column_stat_sql(exec_tenant_id,
                                                   *catalog_column_stats.at(i),
                                                   column_params.at(i),
                                                   table_param,
                                                   current_time,
                                                   min_meta,
                                                   max_meta,
                                                   print_params,
                                                   allocator,
                                                   tmp))) {
      LOG_WARN("failed to get external column stat",
               K(ret),
               K(*catalog_column_stats.at(i)),
               K(column_params.at(i)));
    } else if (OB_FAIL(column_stats_sql.append_fmt(
                   "(%s)%s",
                   tmp.ptr(),
                   (i == catalog_column_stats.count() - 1 ? ";" : ",")))) {
      LOG_WARN("failed to append sql", K(ret));
    }
  }
  LOG_TRACE("OPT:Succeed to construct external column stat sql", K(column_stats_sql));
  return ret;
}

/**
 * @brief ObOptCatalogStatSqlService::get_catalog_column_stat_sql
 * Helper function: get external column stat SQL
 * Corresponds to ObOptStatSqlService::get_catalog_column_stat_sql
 *
 * Key differences:
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 * - Directly extracts catalog_id, db_name, table_name, partition_value, column_name from
 * catalog_column_stat
 *
 * @param tenant_id
 * @param allocator
 * @param stat
 * @param column_param
 * @param table_param
 * @param current_time
 * @param min_meta
 * @param max_meta
 * @param sql_string
 * @param print_params
 * @return
 */
int ObOptCatalogStatSqlService::get_catalog_column_stat_sql(
    const uint64_t tenant_id,
    const share::ObOptCatalogColumnStat &stat,
    const ObCatalogColumnStatParam &column_param,
    const ObCatalogTableStatParam &table_param,
    const int64_t current_time,
    ObObjMeta min_meta,
    ObObjMeta max_meta,
    const ObObjPrintParams &print_params,
    ObIAllocator &allocator,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  ObString min_str, b_min_str;
  ObString max_str, b_max_str;
  const uint64_t ext_tenant_id =
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  char *llc_comp_buf = NULL;
  char *llc_hex_buf = NULL;
  int64_t llc_comp_size = 0;
  int64_t llc_hex_size = 0;
  uint64_t data_version = 0;

  // Catalog tables don't support histograms, so histogram-related checks are skipped
  if (OB_UNLIKELY(stat.get_num_distinct() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(ObOptStatSqlService::get_valid_obj_str(stat.get_min_value(),
                                                            min_meta,
                                                            allocator,
                                                            min_str,
                                                            print_params))
             || OB_FAIL(ObOptStatSqlService::get_valid_obj_str(stat.get_max_value(),
                                                               max_meta,
                                                               allocator,
                                                               max_str,
                                                               print_params))) {
    LOG_WARN("failed to get valid obj str", K(stat.get_min_value()), K(stat.get_max_value()));
  } else if (OB_FAIL(ObOptStatSqlService::get_obj_binary_hex_str(stat.get_min_value(),
                                                                 allocator,
                                                                 b_min_str))
             || OB_FAIL(ObOptStatSqlService::get_obj_binary_hex_str(stat.get_max_value(),
                                                                    allocator,
                                                                    b_max_str))) {
    LOG_WARN("failed to convert obj to str", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (stat.get_llc_bitmap_size() <= 0) {
    // do nothing
  } else {
    // Use the same compression library names as ObOptStatSqlService
    static const char *bitmap_compress_lib_name = "zstd_1.3.8";
    if (OB_FAIL(ObOptStatSqlService::get_compressed_llc_bitmap(
            allocator,
            bitmap_compress_lib_name, // if DATA_VERSION < DATA_VERSION_4_2_2_0, using
                                      // ObOptStatCompressType::ZLIB_COMPRESS.
            stat.get_llc_bitmap(),
            stat.get_llc_bitmap_size(),
            llc_comp_buf,
            llc_comp_size))) {
      LOG_WARN("failed to get compressed llc bit map", K(ret));
    } else if (FALSE_IT(llc_hex_size = llc_comp_size * 2 + 2)) {
      // 1 bytes are represented by 2 hex char (2 bytes)
      // 1 bytes for '\0', and 1 bytes just safe
    } else if (OB_ISNULL(llc_hex_buf = static_cast<char *>(allocator.alloc(llc_hex_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(llc_hex_buf), K(llc_hex_size));
    } else if (OB_FAIL(
                   common::to_hex_cstr(llc_comp_buf, llc_comp_size, llc_hex_buf, llc_hex_size))) {
      LOG_WARN("failed to convert to hex cstr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // Get partition_value from catalog_column_stat
    // Ensure empty string instead of NULL (partition_value column cannot be NULL)
    ObString partition_value = stat.get_partition_value();
    if (partition_value.ptr() == nullptr) {
      partition_value = ObString("");
    }

    // Align with ObOptStatSqlService::get_column_stat_sql: inner rowkey tenant_id via extract
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id))
        || OB_FAIL(dml_splicer.add_pk_column("catalog_id", stat.get_catalog_id()))
        || OB_FAIL(dml_splicer.add_pk_column("db_name", stat.get_database_name()))
        || OB_FAIL(dml_splicer.add_pk_column("table_name", stat.get_table_name()))
        || OB_FAIL(dml_splicer.add_pk_column("partition_value", ObHexEscapeSqlStr(partition_value)))
        || OB_FAIL(dml_splicer.add_pk_column("column_name", stat.get_column_name())) ||
        // Add statistics columns
        OB_FAIL(dml_splicer.add_time_column(
            "last_analyzed",
            stat.get_last_analyzed() == 0 ? current_time : stat.get_last_analyzed()))
        || OB_FAIL(dml_splicer.add_time_column("last_modified", current_time))
        || OB_FAIL(dml_splicer.add_column("distinct_cnt", stat.get_num_distinct()))
        || OB_FAIL(dml_splicer.add_column("null_cnt", stat.get_num_null()))
        || OB_FAIL(dml_splicer.add_column("max_value", ObHexEscapeSqlStr(max_str)))
        || OB_FAIL(dml_splicer.add_column("b_max_value", b_max_str))
        || OB_FAIL(dml_splicer.add_column("min_value", ObHexEscapeSqlStr(min_str)))
        || OB_FAIL(dml_splicer.add_column("b_min_value", b_min_str))
        || OB_FAIL(dml_splicer.add_column("avg_len", stat.get_avg_length()))
        || OB_FAIL(
            dml_splicer.add_column("distinct_cnt_synopsis", llc_hex_buf == NULL ? "" : llc_hex_buf))
        || OB_FAIL(dml_splicer.add_column("distinct_cnt_synopsis_size", llc_comp_size * 2)) ||
        // Catalog tables don't support histograms, use default values
        OB_FAIL(dml_splicer.add_column("sample_size", 0))
        || OB_FAIL(dml_splicer.add_long_double_column("density", 0.0))
        || OB_FAIL(dml_splicer.add_column("bucket_cnt", 0))
        || OB_FAIL(dml_splicer.add_column("histogram_type", ObHistType::INVALID_TYPE))) {
      LOG_WARN("failed to add dml splicer column", K(ret));
    } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
      LOG_WARN("failed to get sql string", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObOptCatalogStatSqlService::get_column_stat_min_max_meta
 * Helper function: get column stat min/max meta
 * Reuses implementation from ObOptStatSqlService
 *
 * @param schema_guard
 * @param tenant_id
 * @param table_id
 * @param min_meta
 * @param max_meta
 * @return
 */
int ObOptCatalogStatSqlService::get_column_stat_min_max_meta(
    const uint64_t tenant_id,
    const uint64_t table_id,
    share::schema::ObSchemaGetterGuard *schema_guard,
    ObObjMeta &min_meta,
    ObObjMeta &max_meta)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
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
      }
    }
    if (OB_SUCC(ret) && (!found_min_col || !found_max_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(found_min_col), K(found_max_col));
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::filter_partition_columns(
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &column_names,
    ObIArray<ObString> &filtered_column_names)
{
  int ret = OB_SUCCESS;
  filtered_column_names.reset();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    bool is_partitioned_table = table_schema->is_partitioned_table();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      const ObString &column_name = column_names.at(i);
      bool is_partition_col = false;

      if (is_partitioned_table) {
        const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(column_name);
        if (OB_ISNULL(col_schema)) {
          LOG_TRACE("[EXTERNAL_TABLE_STAT] Column schema not found, skipping",
                    "column_name",
                    column_name);
          // Skip this column, continue to next
        } else {
          is_partition_col = col_schema->is_tbl_part_key_column();
        }
      }

      if (!is_partition_col) {
        if (OB_FAIL(filtered_column_names.push_back(column_name))) {
          LOG_WARN("failed to push back filtered column name", K(ret));
        }
      } else {
        LOG_TRACE("[EXTERNAL_TABLE_STAT] Skipping partition column for statistics collection",
                  "column_name",
                  column_name);
      }
    }
  }

  return ret;
}

int ObOptCatalogStatSqlService::build_column_stat_sql(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &filtered_column_names,
    ObIAllocator &allocator,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    const uint64_t exec_tid = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t ext_tenant_id =
        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tid, tenant_id);
    const bool is_partitioned_table = table_schema->is_partitioned_table();
    // Build common SELECT ... FROM ... WHERE tenant_id/catalog_id/db_name/table_name
    if (OB_FAIL(sql.append_fmt(
            "SELECT column_name, distinct_cnt, null_cnt, max_value, b_max_value, "
            "       min_value, b_min_value, avg_len, distinct_cnt_synopsis, "
            "       distinct_cnt_synopsis_size, sample_size, density, bucket_cnt, "
            "       histogram_type, last_analyzed, partition_value "
            "FROM oceanbase.%s "
            "WHERE tenant_id = %lu AND catalog_id = %lu AND db_name = ",
            share::OB_ALL_CATALOG_COLUMN_STAT_TNAME,
            ext_tenant_id,
            catalog_id))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(database_name, sql))) {
      LOG_WARN("failed to append database name", K(ret), K(database_name));
    } else if (OB_FAIL(sql.append(" AND table_name = "))) {
      LOG_WARN("failed to append table name", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
      LOG_WARN("failed to append table name", K(ret), K(table_name));
    }

    // Append partition_value condition
    if (OB_SUCC(ret)) {
      if (!is_partitioned_table || partition_values.empty()) {
        // Case 1/2: Non-partitioned table or all partitions, no partition filter
      } else if (OB_LIKELY(1 == partition_values.count())) {
        // Case 3: Single partition
        if (OB_FAIL(sql.append(" AND partition_value = "))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(partition_values.at(0), sql))) {
          LOG_WARN("failed to append partition value", K(ret));
        }
      } else {
        // Case 4: Multiple partitions
        if (OB_FAIL(sql.append(" AND partition_value IN ("))) {
          LOG_WARN("failed to append partition IN", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
          if (i > 0 && OB_FAIL(sql.append(", "))) {
            LOG_WARN("failed to append comma", K(ret));
          } else if (OB_FAIL(sql_append_hex_escape_str(partition_values.at(i), sql))) {
            LOG_WARN("failed to append partition value", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
          LOG_WARN("failed to append closing paren", K(ret));
        }
      }
    }

    // Append column_name IN clause
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(" AND column_name IN ("))) {
        LOG_WARN("failed to append column IN", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < filtered_column_names.count(); ++i) {
        if (i > 0 && OB_FAIL(sql.append(", "))) {
          LOG_WARN("failed to append comma", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(filtered_column_names.at(i), sql))) {
          LOG_WARN("failed to append column name", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to append closing paren", K(ret));
      }
    }
  }

  return ret;
}

int ObOptCatalogStatSqlService::parse_column_stat_row(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    sqlclient::ObMySQLResult &result,
    ObIAllocator &allocator,
    share::ObOptCatalogColumnStat *&external_col_stat)
{
  int ret = OB_SUCCESS;
  external_col_stat = nullptr;

  ObString column_name;
  ObString partition_value_from_db;
  int64_t distinct_cnt = 0;
  int64_t null_cnt = 0;
  double avg_len_double = 0.0;
  int64_t avg_len = 0;
  int64_t sample_size = 0;
  int64_t last_analyzed = 0;

  // Extract required fields - fail if any required field extraction fails
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "column_name", column_name);
  if (OB_FAIL(ret)) {
    LOG_WARN("[EXTERNAL_TABLE_STAT] Failed to extract column_name", K(ret));
  }

  if (OB_SUCC(ret)) {
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "partition_value", partition_value_from_db);
    EXTRACT_INT_FIELD_MYSQL(result, "distinct_cnt", distinct_cnt, int64_t);
    if (OB_FAIL(ret)) {
      LOG_WARN("[EXTERNAL_TABLE_STAT] Failed to extract distinct_cnt",
               K(ret),
               "column_name",
               column_name);
    }
  }

  if (OB_SUCC(ret)) {
    EXTRACT_INT_FIELD_MYSQL(result, "null_cnt", null_cnt, int64_t);
    if (OB_FAIL(ret)) {
      LOG_WARN("[EXTERNAL_TABLE_STAT] Failed to extract null_cnt",
               K(ret),
               "column_name",
               column_name);
    }
  }

  if (OB_SUCC(ret)) {
    // avg_len is ObDoubleType in system table
    EXTRACT_DOUBLE_FIELD_MYSQL(result, "avg_len", avg_len_double, double);
    if (OB_FAIL(ret)) {
      LOG_WARN("[EXTERNAL_TABLE_STAT] Failed to extract avg_len",
               K(ret),
               "column_name",
               column_name);
    } else {
      avg_len = static_cast<int64_t>(avg_len_double);
    }
  }

  if (OB_SUCC(ret)) {
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "sample_size", sample_size, int64_t);

    // last_analyzed is ObTimestampType in system table
    if (OB_FAIL(result.get_timestamp("last_analyzed", NULL, last_analyzed))) {
      if (OB_ERR_COLUMN_NOT_FOUND == ret || OB_ERR_NULL_VALUE == ret) {
        last_analyzed = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[EXTERNAL_TABLE_STAT] Failed to extract column last_analyzed",
                 K(ret),
                 "column_name",
                 column_name);
      }
    }
  }

  // Parse additional optional fields
  int64_t bucket_cnt = 0;
  int64_t histogram_type = 0;
  double density = 0.0;
  ObString b_min_value, b_max_value;
  ObString llc_hex_str;
  int64_t llc_bitmap_size = 0;

  if (OB_SUCC(ret)) {
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "bucket_cnt", bucket_cnt, int64_t);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "histogram_type", histogram_type, int64_t);
    EXTRACT_DOUBLE_FIELD_MYSQL_SKIP_RET(result, "density", density, double);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "b_min_value", b_min_value);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "b_max_value", b_max_value);
    // Parse llc_bitmap for NDV estimation
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result,
                                     "distinct_cnt_synopsis_size",
                                     llc_bitmap_size,
                                     int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "distinct_cnt_synopsis", llc_hex_str);
  }

  // Build ObOptCatalogColumnStat using builder
  if (OB_SUCC(ret)) {
    ObOptCatalogColumnStatBuilder column_stat_builder(allocator);

    if (OB_FAIL(column_stat_builder.set_basic_info(tenant_id,
                                                   catalog_id,
                                                   database_name,
                                                   table_name,
                                                   partition_value_from_db,
                                                   column_name))) {
      LOG_WARN("failed to set basic info for external column stat",
               K(ret),
               "column_name",
               column_name);
    } else if (OB_FAIL(column_stat_builder.set_stat_info(
                   null_cnt,      // num_null
                   0,             // num_not_null (will be calculated if we have row_count)
                   distinct_cnt,  // num_distinct
                   avg_len,       // avg_length
                   last_analyzed, // last_analyzed
                   CS_TYPE_UTF8MB4_GENERAL_CI))) { // default collation
      LOG_WARN("failed to set stat info for external column stat",
               K(ret),
               "column_name",
               column_name);
    } else {
      // Set min/max values if available
      if (!b_min_value.empty()) {
        ObObj min_obj;
        if (OB_FAIL(ObDbmsCatalogStatsUtils::hex_str_to_obj(b_min_value.ptr(),
                                                            b_min_value.length(),
                                                            allocator,
                                                            min_obj))) {
          LOG_WARN("failed to parse min value", K(ret), "column_name", column_name);
        } else if (OB_FAIL(column_stat_builder.set_min_value(min_obj))) {
          LOG_WARN("failed to set min value", K(ret), "column_name", column_name);
        }
      }

      if (OB_SUCC(ret) && !b_max_value.empty()) {
        ObObj max_obj;
        if (OB_FAIL(ObDbmsCatalogStatsUtils::hex_str_to_obj(b_max_value.ptr(),
                                                            b_max_value.length(),
                                                            allocator,
                                                            max_obj))) {
          LOG_WARN("failed to parse max value", K(ret), "column_name", column_name);
        } else if (OB_FAIL(column_stat_builder.set_max_value(max_obj))) {
          LOG_WARN("failed to set max value", K(ret), "column_name", column_name);
        }
      }

      // Build external column stat
      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_stat_builder.build(allocator, external_col_stat))) {
          LOG_WARN("failed to build external column stat", K(ret), "column_name", column_name);
        } else if (OB_ISNULL(external_col_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("external column stat is null", K(ret), "column_name", column_name);
        }
      }

      // Decompress and set llc_bitmap for NDV estimation
      if (OB_SUCC(ret) && llc_bitmap_size > 0 && !llc_hex_str.empty()) {
        char *hex_to_bin_buf = NULL;
        if (OB_ISNULL(hex_to_bin_buf
                      = static_cast<char *>(allocator.alloc(llc_hex_str.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for bitmap", K(ret));
        } else {
          // Convert hex string to binary
          common::str_to_hex(llc_hex_str.ptr(),
                             llc_hex_str.length(),
                             hex_to_bin_buf,
                             llc_hex_str.length());
          // Decompress llc bitmap
          char *decomp_buf = NULL;
          int64_t decomp_size = share::ObOptCatalogColumnStat::NUM_BITMAP_BUCKET;
          const int64_t comp_bitmap_size = llc_hex_str.length() / 2;
          // Catalog tables always use zstd_1.3.8 compression
          if (OB_FAIL(ObOptStatSqlService::get_decompressed_llc_bitmap(
                  allocator,
                  bitmap_compress_lib_name[ObOptStatCompressType::ZSTD_1_3_8_COMPRESS],
                  hex_to_bin_buf,
                  comp_bitmap_size,
                  decomp_buf,
                  decomp_size))) {
            LOG_WARN("failed to decompress llc bitmap", K(ret));
          } else {
            external_col_stat->set_llc_bitmap(decomp_buf, decomp_size);
          }
        }
      }
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::batch_fetch_table_stats(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const ObIArray<ObString> &part_names,
    ObIArray<ObOptCatalogTableStat *> &all_part_stats,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  const int64_t PART_FETCH_BATCH_CNT = 32;
  const uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const bool need_part_filter = !part_names.empty();
  int64_t part_start_idx = 0;
  bool has_more_batch = true;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("catalog sql service has not been initialized.", K(ret));
  }

  while (OB_SUCC(ret) && has_more_batch) {
    ObSEArray<ObString, 32> batch_part_names;
    int64_t cur_batch_start_idx = part_start_idx;
    if (!need_part_filter) {
      // No partition filter: execute one full-table query.
      has_more_batch = false;
    } else {
      if (part_start_idx >= part_names.count()) {
        has_more_batch = false;
      } else {
        int64_t part_end_idx = part_start_idx + PART_FETCH_BATCH_CNT;
        if (part_end_idx > part_names.count()) {
          part_end_idx = part_names.count();
        }
        for (int64_t i = part_start_idx; OB_SUCC(ret) && i < part_end_idx; ++i) {
          if (OB_FAIL(batch_part_names.push_back(part_names.at(i)))) {
            LOG_WARN("failed to push back batch partition name",
                     K(ret),
                     K(i),
                     K(part_start_idx),
                     K(part_end_idx));
          }
        }
        part_start_idx = part_end_idx;
        if (part_start_idx >= part_names.count()) {
          has_more_batch = false;
        }
        if (OB_FAIL(ret)) {
        } else {
          bool has_empty_part = false;
          for (int64_t i = 0; !has_empty_part && i < batch_part_names.count(); ++i) {
            if (batch_part_names.at(i).empty()) {
              has_empty_part = true;
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (need_part_filter && batch_part_names.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("batch part names is empty", K(ret));
    } else {
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        ObSqlString part_list;
        if (OB_FAIL(sql.append_fmt("SELECT schema_version, partition_value, last_analyzed, file_count, "
                                   "data_size, row_cnt, avg_row_len, stattype_locked, sample_size "
                                   "FROM oceanbase.%s "
                                   "WHERE tenant_id = %lu AND catalog_id = %lu AND db_name = ",
                                   share::OB_ALL_CATALOG_TABLE_STAT_TNAME,
                                   share::schema::ObSchemaUtils::get_extract_tenant_id(
                                       exec_tenant_id,
                                       tenant_id),
                                   catalog_id))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(database_name, sql))) {
          LOG_WARN("failed to append database name", K(ret), K(database_name));
        } else if (OB_FAIL(sql.append(" AND table_name = "))) {
          LOG_WARN("failed to append table name", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
          LOG_WARN("failed to append table name", K(ret), K(table_name));
        } else if (OB_FAIL(generate_in_list(batch_part_names, part_list))) {
          LOG_WARN("failed to generate in list", K(ret), K(batch_part_names.count()));
        } else if (!part_list.empty() && OB_FAIL(sql.append(" AND partition_value in "))) {
          LOG_WARN("fail to append partition string.", K(ret));
        } else if (!part_list.empty() && OB_FAIL(sql.append(part_list.string()))) {
          LOG_WARN("fail to append partition filter to sql.", K(ret));
        } else if (conn != NULL && OB_FAIL(conn->execute_read(exec_tenant_id, sql.ptr(), res))) {
          LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
        } else if (conn == NULL
                   && OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
        }
        while (OB_SUCC(ret)) {
          ObOptCatalogTableStat stat;

          stat.set_catalog_id(catalog_id);
          stat.set_database_name(database_name);
          stat.set_table_name(table_name);
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(fill_catalog_table_stat(*result, stat))) {
            LOG_WARN("failed to fill table stat", K(ret));
          } else if (need_part_filter && stat.get_partition_value().empty()) {
            LOG_INFO("batch_fetch_table_stats skip global row in partition query",
                     K(part_start_idx),
                     K(batch_part_names.count()),
                     K(stat.get_partition_value()));
          } else {
            bool found_it = false;
            for (int64_t i = 0; OB_SUCC(ret) && i < all_part_stats.count(); ++i) {
              if (OB_ISNULL(all_part_stats.at(i))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected error", K(ret));
              } else if (all_part_stats.at(i)->get_catalog_id() == stat.get_catalog_id()
                         && all_part_stats.at(i)->get_database_name() == stat.get_database_name()
                         && all_part_stats.at(i)->get_table_name() == stat.get_table_name()
                         && all_part_stats.at(i)->get_partition_value()
                                == stat.get_partition_value()) {
                found_it = true;
                // Keep slot key fields (db/table/partition) stable.
                // Only refresh value fields from current query result.
                all_part_stats.at(i)->set_tenant_id(stat.get_tenant_id());
                all_part_stats.at(i)->set_catalog_id(stat.get_catalog_id());
                all_part_stats.at(i)->set_schema_version(stat.get_schema_version());
                all_part_stats.at(i)->set_row_count(stat.get_row_count());
                all_part_stats.at(i)->set_file_num(stat.get_file_num());
                all_part_stats.at(i)->set_data_size(stat.get_data_size());
                all_part_stats.at(i)->set_last_analyzed(stat.get_last_analyzed());
                all_part_stats.at(i)->set_sample_size(stat.get_sample_size());
                all_part_stats.at(i)->set_avg_row_len(stat.get_avg_row_len());
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
  }
  return ret;
}

int ObOptCatalogStatSqlService::generate_in_list(const ObIArray<ObString> &part_names,
                                                 ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  // Note: partition_value is a VARCHAR column, use hex escape to avoid special character issues
  for (int64_t i = 0; OB_SUCC(ret) && i < part_names.count(); i++) {
    if (i == 0) {
      if (OB_FAIL(sql_string.append("("))) {
        LOG_WARN("failed to append open paren", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append(", "))) {
        LOG_WARN("failed to append comma", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql_append_hex_escape_str(part_names.at(i), sql_string))) {
      LOG_WARN("failed to append partition value", K(ret));
    }
    if (OB_SUCC(ret) && i == part_names.count() - 1) {
      if (OB_FAIL(sql_string.append(")"))) {
        LOG_WARN("failed to append close paren", K(ret));
      }
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::fill_catalog_table_stat(common::sqlclient::ObMySQLResult &result,
                                                        ObOptCatalogTableStat &stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("catalog sql service has not been initialized.", K(ret));
  } else {
    int64_t schema_version = 0;
    ObString partition_value;
    int64_t last_analyzed = 0;
    int64_t file_count = 0;
    int64_t data_size = 0;
    int64_t row_cnt = 0;
    double avg_row_len_double = 0.0;
    int64_t sample_size = 0;
    ObObjMeta obj_type;

    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "schema_version", schema_version, int64_t);
    stat.set_schema_version(schema_version);

    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "partition_value", partition_value);
    stat.set_partition_value(partition_value);

    // Extract last_analyzed (timestamp type)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result.get_timestamp("last_analyzed", NULL, last_analyzed))) {
      LOG_WARN("fail to get column in row. ", "column_name", "last_analyzed", K(ret));
    } else {
      stat.set_last_analyzed(static_cast<int64_t>(last_analyzed));
    }

    // Extract file count and data size
    if (OB_FAIL(ret)) {
    } else {
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "file_count", file_count, int64_t);
      stat.set_file_num(file_count);
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "data_size", data_size, int64_t);
      stat.set_data_size(data_size);
    }

    // Extract row count
    if (OB_FAIL(ret)) {
    } else {
      EXTRACT_INT_FIELD_MYSQL(result, "row_cnt", row_cnt, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract row_cnt", K(ret));
      } else {
        stat.set_row_count(row_cnt);
      }
    }

    // Extract avg_row_len (may be double or int type)
    if (OB_FAIL(ret)) {
    } else {
      if (OB_FAIL(result.get_type("avg_row_len", obj_type))) {
        LOG_WARN("failed to get type", K(ret));
      } else if (OB_LIKELY(obj_type.is_double())) {
        double avg_row_len_double = 0.0;
        EXTRACT_DOUBLE_FIELD_MYSQL(result, "avg_row_len", avg_row_len_double, double);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extract avg_row_len", K(ret));
        } else {
          stat.set_avg_row_len(static_cast<int64_t>(avg_row_len_double));
        }
      } else {
        int64_t avg_row_len = 0;
        EXTRACT_INT_FIELD_MYSQL(result, "avg_row_len", avg_row_len, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extract avg_row_len", K(ret));
        } else {
          stat.set_avg_row_len(avg_row_len);
        }
      }
    }

    // Extract sample_size
    if (OB_FAIL(ret)) {
    } else {
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "sample_size", sample_size, int64_t);
      stat.set_sample_size(sample_size);
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::fetch_column_stat(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const ObIArray<ObString> &part_names,
    const ObIArray<ObString> &column_names,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  UNUSED(part_names);
  UNUSED(column_names);
  if (OB_UNLIKELY(catalog_column_stats.empty())) {
    // Empty is valid, just return success
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_, false, OB_INVALID_TIMESTAMP, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql service has not been initialized.", K(ret));
      } else if (OB_FAIL(sql.append_fmt(FETCH_ALL_COLUMN_STAT_SQL_COL_PREFIX,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(
                                            exec_tenant_id,
                                            tenant_id),
                                        catalog_id))) {
        LOG_WARN("fail to append SQL stmt string.", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(database_name, sql))) {
        LOG_WARN("failed to append database name", K(ret), K(database_name));
      } else if (OB_FAIL(sql.append(" AND table_name = "))) {
        LOG_WARN("failed to append table name", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
        LOG_WARN("failed to append table name", K(ret), K(table_name));
      } else if (OB_FAIL(sql.append(FETCH_ALL_COLUMN_STAT_SQL_COL_SUFFIX))) {
        LOG_WARN("failed to append suffix", K(ret));
      } else if (conn != NULL && OB_FAIL(conn->execute_read(exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (conn == NULL
                 && OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
      } else {
        int64_t row_count = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed, ", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ++row_count;
            if (OB_FAIL(fill_catalog_column_stat(allocator, *result, catalog_column_stats))) {
              LOG_WARN("read stat from result failed. ", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

// 新增函数实现
int ObOptCatalogStatSqlService::update_catalog_opt_stat_gather_stat(
    const ObOptStatGatherStat &gather_stat,
    const uint64_t &catalog_id,
    const ObString &db_name,
    const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString value_str;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = gen_meta_tenant_id(gather_stat.get_tenant_id());
  if (!is_valid_tenant_id(tenant_id)) {
    // do nothing
  } else if (OB_FAIL(get_catalog_gather_stat_value(gather_stat,
                                                   catalog_id,
                                                   db_name,
                                                   table_name,
                                                   value_str))) {
    LOG_WARN("failed to get catalog gather stat value", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(INSERT_CATALOG_TABLE_OPT_STAT_GATHER_SQL,
                                        share::OB_ALL_CATALOG_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
                                        value_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(raw_sql));
  } else {
    ObMySQLTransaction trans;
    LOG_TRACE("sql string of update catalog opt stat gather stat", K(raw_sql));
    if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(tenant_id, raw_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret));
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

int ObOptCatalogStatSqlService::get_catalog_gather_stat_value(
    const ObOptStatGatherStat &gather_stat,
    const uint64_t &catalog_id,
    const ObString &db_name,
    const ObString &table_name,
    ObSqlString &values_ptr)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t tenant_id = gather_stat.get_tenant_id();
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id))
      || OB_FAIL(dml_splicer.add_pk_column("task_id", gather_stat.get_task_id()))
      || OB_FAIL(dml_splicer.add_pk_column("catalog_id", catalog_id))
      || OB_FAIL(dml_splicer.add_pk_column("db_name", db_name))
      || OB_FAIL(dml_splicer.add_pk_column("table_name", table_name))
      || OB_FAIL(dml_splicer.add_column("ret_code", gather_stat.get_ret_code()))
      || OB_FAIL(dml_splicer.add_time_column("start_time", gather_stat.get_start_time()))
      || OB_FAIL(dml_splicer.add_time_column("end_time", gather_stat.get_end_time()))
      || OB_FAIL(dml_splicer.add_column("memory_used", gather_stat.get_memory_used()))
      || OB_FAIL(dml_splicer.add_column("stat_refresh_failed_list",
                                        gather_stat.get_stat_refresh_failed_list()))
      || OB_FAIL(dml_splicer.add_column("properties", gather_stat.get_properties()))
      || OB_FAIL(dml_splicer.add_column("spare3", gather_stat.get_gather_audit()))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(values_ptr))) {
    LOG_WARN("failed to get sql string", K(ret));
  }
  return ret;
}

int ObOptCatalogStatSqlService::fill_catalog_column_stat(
    ObIAllocator &allocator,
    common::sqlclient::ObMySQLResult &result,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("catalog sql service has not been initialized.", K(ret));
  } else {
    ObString column_name;
    ObString partition_value;
    int64_t distinct_cnt = 0;
    int64_t null_cnt = 0;
    double avg_len_double = 0.0;
    int64_t avg_len = 0;
    int64_t last_analyzed = 0;
    ObString b_min_value;
    ObString b_max_value;
    ObString llc_hex_str;
    int64_t llc_bitmap_size = 0;

    // Extract fields from result
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "column_name", column_name);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extract column_name", K(ret));
    }

    if (OB_SUCC(ret)) {
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "partition_value", partition_value);
      EXTRACT_INT_FIELD_MYSQL(result, "distinct_cnt", distinct_cnt, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract distinct_cnt", K(ret), K(column_name));
      }
    }

    if (OB_SUCC(ret)) {
      EXTRACT_INT_FIELD_MYSQL(result, "null_cnt", null_cnt, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract null_cnt", K(ret), K(column_name));
      }
    }

    if (OB_SUCC(ret)) {
      EXTRACT_DOUBLE_FIELD_MYSQL(result, "avg_len", avg_len_double, double);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract avg_len", K(ret), K(column_name));
      } else {
        avg_len = static_cast<int64_t>(avg_len_double);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_timestamp("last_analyzed", NULL, last_analyzed))) {
        if (OB_ERR_COLUMN_NOT_FOUND == ret || OB_ERR_NULL_VALUE == ret) {
          last_analyzed = 0;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to extract last_analyzed", K(ret), K(column_name));
        }
      }
    }

    if (OB_SUCC(ret)) {
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "b_min_value", b_min_value);
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "b_max_value", b_max_value);
      // Parse llc_bitmap for NDV estimation
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result,
                                       "distinct_cnt_synopsis_size",
                                       llc_bitmap_size,
                                       int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "distinct_cnt_synopsis", llc_hex_str);
    }

    // Find the matching stat in catalog_column_stats and update it
    if (OB_SUCC(ret)) {
      bool found_it = false;
      for (int64_t i = 0; OB_SUCC(ret) && !found_it && i < catalog_column_stats.count(); ++i) {
        if (OB_ISNULL(catalog_column_stats.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i));
        } else if (catalog_column_stats.at(i)->get_column_name() == column_name
                   && catalog_column_stats.at(i)->get_partition_value() == partition_value) {
          found_it = true;
          share::ObOptCatalogColumnStat *stat = catalog_column_stats.at(i);
          stat->set_num_distinct(distinct_cnt);
          stat->set_num_null(null_cnt);
          stat->set_avg_length(avg_len);
          stat->set_last_analyzed(last_analyzed);

          // Set min/max values if available
          if (!b_min_value.empty()) {
            ObObj min_obj;
            if (OB_FAIL(ObDbmsCatalogStatsUtils::hex_str_to_obj(b_min_value.ptr(),
                                                                b_min_value.length(),
                                                                allocator,
                                                                min_obj))) {
              LOG_WARN("failed to parse min value", K(ret), K(column_name));
            } else {
              stat->set_min_value(min_obj);
            }
          }

          if (OB_SUCC(ret) && !b_max_value.empty()) {
            ObObj max_obj;
            if (OB_FAIL(ObDbmsCatalogStatsUtils::hex_str_to_obj(b_max_value.ptr(),
                                                                b_max_value.length(),
                                                                allocator,
                                                                max_obj))) {
              LOG_WARN("failed to parse max value", K(ret), K(column_name));
            } else {
              stat->set_max_value(max_obj);
            }
          }

          // Decompress and set llc_bitmap for NDV estimation
          if (OB_SUCC(ret) && llc_bitmap_size > 0 && !llc_hex_str.empty()) {
            char *hex_to_bin_buf = NULL;
            if (OB_ISNULL(hex_to_bin_buf
                          = static_cast<char *>(allocator.alloc(llc_hex_str.length())))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory for bitmap", K(ret));
            } else {
              // Convert hex string to binary
              common::str_to_hex(llc_hex_str.ptr(),
                                 llc_hex_str.length(),
                                 hex_to_bin_buf,
                                 llc_hex_str.length());
              // Decompress llc bitmap
              char *decomp_buf = NULL;
              int64_t decomp_size = share::ObOptCatalogColumnStat::NUM_BITMAP_BUCKET;
              const int64_t comp_bitmap_size = llc_hex_str.length() / 2;
              // Catalog tables always use zstd_1.3.8 compression
              if (OB_FAIL(ObOptStatSqlService::get_decompressed_llc_bitmap(
                      allocator,
                      bitmap_compress_lib_name[ObOptStatCompressType::ZSTD_1_3_8_COMPRESS],
                      hex_to_bin_buf,
                      comp_bitmap_size,
                      decomp_buf,
                      decomp_size))) {
                LOG_WARN("failed to decompress llc bitmap", K(ret));
              } else {
                stat->set_llc_bitmap(decomp_buf, decomp_size);
              }
            }
          }
        }
      }
      // Not finding a match is not an error, just skip this row
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::batch_delete_table_stats(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &db_name,
    const ObString &table_name,
    const ObIArray<ObString> &partition_values,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (partition_values.empty()) {
    // Nothing to delete
  } else if (OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("connection is null", K(ret));
  } else {
    const uint64_t exec_tid = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t ext_tenant_id =
        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tid, tenant_id);
    // Build DELETE statement with IN clause for partition values
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND catalog_id = %lu "
                                "AND db_name = ",
                                share::OB_ALL_CATALOG_TABLE_STAT_TNAME,
                                ext_tenant_id,
                                catalog_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(db_name, sql))) {
      LOG_WARN("failed to append db name", K(ret), K(db_name));
    } else if (OB_FAIL(sql.append(" AND table_name = "))) {
      LOG_WARN("failed to append table name", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
      LOG_WARN("failed to append table name", K(ret), K(table_name));
    } else if (OB_FAIL(sql.append(" AND partition_value IN ("))) {
      LOG_WARN("failed to append partition IN", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
      const ObString &pv = partition_values.at(i);
      if (i > 0) {
        if (OB_FAIL(sql.append(", "))) {
          LOG_WARN("failed to append comma", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_append_hex_escape_str(pv, sql))) {
          LOG_WARN("failed to append partition value", K(ret), K(pv));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to append closing paren", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(conn->execute_write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute delete", K(ret), K(sql));
      } else {
        LOG_INFO("deleted table stats", K(affected_rows), K(partition_values.count()),
                 K(db_name), K(table_name));
      }
    }
  }
  return ret;
}

int ObOptCatalogStatSqlService::batch_delete_column_stats(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &db_name,
    const ObString &table_name,
    const ObIArray<ObString> &partition_values,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (partition_values.empty()) {
    // Nothing to delete
  } else if (OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("connection is null", K(ret));
  } else {
    const uint64_t exec_tid = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t ext_tenant_id =
        share::schema::ObSchemaUtils::get_extract_tenant_id(exec_tid, tenant_id);
    // Build DELETE statement with IN clause for partition values
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND catalog_id = %lu "
                                "AND db_name = ",
                                share::OB_ALL_CATALOG_COLUMN_STAT_TNAME,
                                ext_tenant_id,
                                catalog_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(db_name, sql))) {
      LOG_WARN("failed to append db name", K(ret), K(db_name));
    } else if (OB_FAIL(sql.append(" AND table_name = "))) {
      LOG_WARN("failed to append table name", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(table_name, sql))) {
      LOG_WARN("failed to append table name", K(ret), K(table_name));
    } else if (OB_FAIL(sql.append(" AND partition_value IN ("))) {
      LOG_WARN("failed to append partition IN", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
      const ObString &pv = partition_values.at(i);
      if (i > 0) {
        if (OB_FAIL(sql.append(", "))) {
          LOG_WARN("failed to append comma", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_append_hex_escape_str(pv, sql))) {
          LOG_WARN("failed to append partition value", K(ret), K(pv));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to append closing paren", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(conn->execute_write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute delete", K(ret), K(sql));
      } else {
        LOG_INFO("deleted column stats", K(affected_rows), K(partition_values.count()),
                 K(db_name), K(table_name));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
