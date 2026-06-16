/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OPT_EXTERNAL_STAT_SQL_SERVICE_H
#define OB_OPT_EXTERNAL_STAT_SQL_SERVICE_H

#include "share/stat/ob_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
}
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace common
{

/**
 * Catalog table statistics SQL service
 * Independent from internal table statistics SQL service (ObOptStatSqlService)
 * Used exclusively for catalog table statistics storage and retrieval
 *
 * Key differences:
 * - Uses ObOptCatalogTableStat instead of ObOptTableStat
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 * - Directly extracts catalog_id, db_name, table_name, partition_value, column_name
 */
class ObOptCatalogStatSqlService
{
public:
  ObOptCatalogStatSqlService();
  ~ObOptCatalogStatSqlService();

  int init(common::ObMySQLProxy *mysql_proxy);

  int update_catalog_table_stat(const uint64_t tenant_id,
                                const share::ObOptCatalogTableStat *catalog_table_stat,
                                const ObCatalogTableStatParam &table_param,
                                sqlclient::ObISQLConnection *conn);

  int update_catalog_column_stat(
      const uint64_t exec_tenant_id,
      const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
      const ObIArray<ObCatalogColumnStatParam> &column_params,
      const ObCatalogTableStatParam &table_param,
      const ObObjPrintParams &print_params,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObIAllocator &allocator,
      sqlclient::ObISQLConnection *conn);

  int fetch_catalog_table_stat_from_system_table(
      const uint64_t tenant_id,
      const uint64_t catalog_id,
      const ObString &database_name,
      const ObString &table_name,
      const share::schema::ObTableSchema *table_schema,
      const ObIArray<ObString> &partition_values,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogTableStat *> &external_table_stats);

  int fetch_catalog_column_stats_from_system_table(
      const uint64_t tenant_id,
      const uint64_t catalog_id,
      const ObString &database_name,
      const ObString &table_name,
      const share::schema::ObTableSchema *table_schema,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  int batch_fetch_table_stats(const uint64_t tenant_id,
                              const uint64_t catalog_id,
                              const ObString &database_name,
                              const ObString &table_name,
                              const ObIArray<ObString> &part_names,
                              ObIArray<share::ObOptCatalogTableStat *> &all_part_stats,
                              sqlclient::ObISQLConnection *conn);

  int fetch_column_stat(const uint64_t tenant_id,
                        const uint64_t catalog_id,
                        const ObString &database_name,
                        const ObString &table_name,
                        const ObIArray<ObString> &part_names,
                        const ObIArray<ObString> &column_names,
                        ObIAllocator &allocator,
                        ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
                        sqlclient::ObISQLConnection *conn);
  int update_catalog_opt_stat_gather_stat(const ObOptStatGatherStat &gather_stat,
                                          const uint64_t &catalog_id,
                                          const ObString &db_name,
                                          const ObString &table_name);

  // Batch delete table stats for specified partitions
  int batch_delete_table_stats(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               const ObString &db_name,
                               const ObString &table_name,
                               const ObIArray<ObString> &partition_values,
                               sqlclient::ObISQLConnection *conn);

  // Batch delete column stats for specified partitions
  int batch_delete_column_stats(const uint64_t tenant_id,
                                const uint64_t catalog_id,
                                const ObString &db_name,
                                const ObString &table_name,
                                const ObIArray<ObString> &partition_values,
                                sqlclient::ObISQLConnection *conn);

private:
  int get_catalog_gather_stat_value(const ObOptStatGatherStat &gather_stat,
                                    const uint64_t &catalog_id,
                                    const ObString &db_name,
                                    const ObString &table_name,
                                    ObSqlString &values_ptr);
  int get_catalog_table_stat_sql(const uint64_t tenant_id,
                                 const share::ObOptCatalogTableStat &catalog_table_stat,
                                 const ObCatalogTableStatParam &table_param,
                                 const int64_t current_time,
                                 bool is_delete,
                                 ObSqlString &sql);

  int construct_catalog_column_stat_sql(
      const uint64_t exec_tenant_id,
      const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats,
      const ObIArray<ObCatalogColumnStatParam> &column_params,
      const ObCatalogTableStatParam &table_param,
      const int64_t current_time,
      const ObObjPrintParams &print_params,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObIAllocator &allocator,
      ObSqlString &column_stats_sql);

  int get_catalog_column_stat_sql(const uint64_t tenant_id,
                                  const share::ObOptCatalogColumnStat &stat,
                                  const ObCatalogColumnStatParam &column_param,
                                  const ObCatalogTableStatParam &table_param,
                                  const int64_t current_time,
                                  ObObjMeta min_meta,
                                  ObObjMeta max_meta,
                                  const ObObjPrintParams &print_params,
                                  ObIAllocator &allocator,
                                  ObSqlString &sql_string);

  int get_column_stat_min_max_meta(const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   share::schema::ObSchemaGetterGuard *schema_guard,
                                   ObObjMeta &min_meta,
                                   ObObjMeta &max_meta);

  int filter_partition_columns(const share::schema::ObTableSchema *table_schema,
                               const ObIArray<ObString> &column_names,
                               ObIArray<ObString> &filtered_column_names);

  int build_column_stat_sql(const uint64_t tenant_id,
                            const uint64_t catalog_id,
                            const ObString &database_name,
                            const ObString &table_name,
                            const share::schema::ObTableSchema *table_schema,
                            const ObIArray<ObString> &partition_values,
                            const ObIArray<ObString> &filtered_column_names,
                            ObIAllocator &allocator,
                            ObSqlString &sql);

  int parse_column_stat_row(const uint64_t tenant_id,
                            const uint64_t catalog_id,
                            const ObString &database_name,
                            const ObString &table_name,
                            sqlclient::ObMySQLResult &result,
                            ObIAllocator &allocator,
                            share::ObOptCatalogColumnStat *&external_col_stat);

  int generate_in_list(const ObIArray<ObString> &part_names, ObSqlString &sql_string);
  int fill_catalog_table_stat(common::sqlclient::ObMySQLResult &result,
                              share::ObOptCatalogTableStat &stat);

  int fill_catalog_column_stat(ObIAllocator &allocator,
                               common::sqlclient::ObMySQLResult &result,
                               ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  int build_fetch_table_stat_sql(const uint64_t tenant_id,
                                 const uint64_t catalog_id,
                                 const ObString &database_name,
                                 const ObString &table_name,
                                 const bool is_partitioned,
                                 const ObIArray<ObString> &partition_values,
                                 ObSqlString &sql);

  int parse_table_stat_row(const uint64_t tenant_id,
                           const uint64_t catalog_id,
                           const ObString &database_name,
                           const ObString &table_name,
                           sqlclient::ObMySQLResult &result,
                           ObIAllocator &allocator,
                           share::ObOptCatalogTableStat *&catalog_table_stat);

private:
  bool inited_;
  common::ObMySQLProxy *mysql_proxy_;
};

} // namespace common
} // namespace oceanbase

#endif // OB_OPT_EXTERNAL_STAT_SQL_SERVICE_H
