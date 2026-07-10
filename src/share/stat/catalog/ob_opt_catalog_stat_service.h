/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OPT_EXTERNAL_STAT_SERVICE_H
#define OB_OPT_EXTERNAL_STAT_SERVICE_H

#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat_cache.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat_cache.h"
#include "share/stat/catalog/ob_opt_catalog_stat_sql_service.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/ob_lake_table_stat.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashset.h"
#include "lib/oblog/ob_log_module.h"
#include <type_traits>

namespace oceanbase
{
namespace sql
{
class ObSqlSchemaGuard;
}
namespace share
{
class ObILakeTableMetadata;
}
namespace common
{
/**
 * Catalog table statistics service
 * Independent from internal table statistics service (ObOptStatService)
 * Used exclusively for catalog table statistics loading and retrieval
 *
 * Key differences:
 * - Uses ObOptCatalogTableStat instead of ObOptTableStat
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 * - Directly uses ObOptCatalogStatSqlService (no conversion needed)
 * - No convert_internal_stats_to_external_stats() call
 */
class ObOptCatalogStatService
{
public:
  ObOptCatalogStatService();
  virtual ~ObOptCatalogStatService();

  virtual int init(common::ObMySQLProxy *proxy, ObServerConfig *config);
  int get_catalog_table_stat_from_cache(const share::ObOptCatalogTableStat::Key &key,
                             ObLakeTableStat &stat,
                             bool &found_stat);

  int batch_get_catalog_table_stats(const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    ObIArray<const share::ObOptCatalogTableStat::Key*> &keys,
                                    const ObIArray<ObString> &all_partition_values,
                                    sql::ObSqlSchemaGuard &schema_guard,
                                    ObLakeTableStat &stat);

  int get_catalog_column_stat_from_cache(ObIAllocator &alloc,
                                         const ObIArray<share::ObOptCatalogColumnStat::Key> &keys,
                                         ObIArray<ObLakeColumnStat*> &column_stats,
                                         ObIArray<ObString> &missed_columns);
  int batch_get_catalog_column_stats(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     ObIArray<const share::ObOptCatalogColumnStat::Key*> &keys,
                                     const ObIArray<ObString> &column_names,
                                     const ObIArray<ObString> &all_partition_values,
                                     sql::ObSqlSchemaGuard &schema_guard,
                                     ObIAllocator &allocator,
                                     const int64_t row_cnt,
                                     const double scale_ratio,
                                     ObIArray<ObLakeColumnStat*> &column_stats);

  ObOptCatalogStatSqlService &get_sql_service()
  {
    return cat_sql_service_;
  }
  int update_catalog_opt_stat_gather_stat(const ObOptStatGatherStat &gather_stat,
                                          const uint64_t &catalog_id,
                                          const ObString &db_name,
                                          const ObString &table_name);
  int erase_catalog_table_stat(const share::ObOptCatalogTableStat::Key &key);
  int erase_catalog_column_stat(const share::ObOptCatalogColumnStat::Key &key);
  int put_catalog_aggr_table_stat_to_cache(
          const uint64_t tenant_id,
          const uint64_t catalog_id,
          const ObString &database_name,
          const ObString &table_name,
          const ObLakeTableStat &table_stat);
  int put_catalog_aggr_column_stat_to_cache(
          const uint64_t tenant_id,
          const uint64_t catalog_id,
          const ObString &database_name,
          const ObString &table_name,
          const ObString &column_name,
          const ObLakeColumnStat &column_stat);

private:
  int load_catalog_table_stat_and_put_cache(const ObLoadCatalogTableStatParam &param,
                                            const ObIArray<ObString> &all_partition_values,
                                            sql::ObSqlSchemaGuard &schema_guard,
                                            const ObIArray<ObString> &key_partition_values);
  template <typename KeyT>
  int batch_load_catalog_table_stats_and_put_cache(const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   const ObIArray<const KeyT *> &keys,
                                                   const ObIArray<ObString> &all_partition_values,
                                                   sql::ObSqlSchemaGuard &schema_guard);

  int extract_column_names_from_table_schema(const share::schema::ObTableSchema &table_schema,
                                             ObIArray<ObString> &column_names);

  int fetch_catalog_table_statistics_from_catalog(
      const share::ObILakeTableMetadata *lake_table_metadata,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      sql::ObSqlSchemaGuard &schema_guard,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
      ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  int put_catalog_table_stats_to_cache(
      const uint64_t tenant_id,
      const uint64_t catalog_id,
      const ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
      const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  int generate_default_catalog_table_statistics(
      const share::ObILakeTableMetadata *lake_table_metadata,
      sql::ObSqlSchemaGuard &schema_guard,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
      ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  // When use_partition_key_ndv is true, partition key columns get a default
  // num_distinct derived from partition_values.count(). This must be disabled for
  // formats that backfill defaults with a single synthetic (global) partition value
  // (e.g. ODPS), otherwise the partition key NDV would be wrongly collapsed to 1.
  int fill_missing_catalog_column_statistics(
      const share::ObILakeTableMetadata *lake_table_metadata,
      const share::schema::ObTableSchema *table_schema,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      const bool use_partition_key_ndv,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats);

  int aggregate_table_stat_from_partitions(
      const ObIArray<share::ObOptCatalogTableStatHandle> &handles,
      ObLakeTableStat &stat);

  int aggregate_column_stats_from_partitions(
      ObIAllocator &allocator,
      const ObIArray<ObString> &column_names,
      const ObIArray<share::ObOptCatalogColumnStatHandle> &handles,
      const int64_t row_cnt,
      const double scale_ratio,
      ObIArray<ObLakeColumnStat*> &column_stats);

  int aggregate_single_column_stat_from_partitions(
      ObIAllocator &alloc,
      const ObIArray<const share::ObOptCatalogColumnStat *> &partition_col_stats,
      const int64_t row_cnt,
      const double scale_ratio,
      ObLakeColumnStat &column_stat);

  int fetch_catalog_table_stat_from_system_table(
      const ObLoadCatalogTableStatParam &param,
      const share::ObILakeTableMetadata *lake_table_metadata,
      const share::schema::ObTableSchema *table_schema,
      const ObIArray<ObString> &key_partition_values,
      const ObIArray<ObString> &column_names,
      ObIAllocator &stat_allocator,
      ObIArray<share::ObOptCatalogTableStat *> &partition_table_stats,
      ObIArray<share::ObOptCatalogColumnStat *> &partition_column_stats,
      bool &stats_from_system_table);

  int fetch_catalog_table_stat_from_catalog_and_cache(
      const ObLoadCatalogTableStatParam &param,
      const share::ObILakeTableMetadata *lake_table_metadata,
      const share::schema::ObTableSchema *table_schema,
      const ObIArray<ObString> &all_partition_values,
      const ObIArray<ObString> &key_partition_values,
      const ObIArray<ObString> &column_names,
      sql::ObSqlSchemaGuard &schema_guard,
      ObIAllocator &stat_allocator);

protected:
  bool inited_;
  static const int64_t DEFAULT_CATALOG_TAB_STAT_CACHE_PRIORITY = 1;
  static const int64_t DEFAULT_CATALOG_COL_STAT_CACHE_PRIORITY = 1;
  ObOptCatalogStatSqlService cat_sql_service_;

  share::ObOptCatalogTableStatCache catalog_table_stat_cache_;
  share::ObOptCatalogColumnStatCache catalog_column_stat_cache_;
};

template <typename KeyT>
int ObOptCatalogStatService::batch_load_catalog_table_stats_and_put_cache(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<const KeyT *> &keys,
    const ObIArray<ObString> &all_partition_values,
    sql::ObSqlSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 16> key_partition_values;
  if (OB_UNLIKELY(keys.empty()) || OB_ISNULL(keys.at(0))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid keys", K(ret), K(keys.count()));
  } else {
    const KeyT *first_key = keys.at(0);
    ObLoadCatalogTableStatParam param(tenant_id, first_key->catalog_id_, table_id);
    hash::ObHashSet<ObString> partition_value_set;
    if constexpr (std::is_same_v<KeyT, share::ObOptCatalogColumnStat::Key>) {
      if (OB_FAIL(partition_value_set.create(keys.count() * 2 + 512, "CatStatPartSet", "CatStatPartSet"))) {
        SQL_ENG_LOG(WARN, "failed to create partition value set", K(ret), K(keys.count()));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
        if (OB_ISNULL(keys.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "catalog stat key is null", K(ret), K(i));
        } else {
          const ObString &partition_value = keys.at(i)->partition_value_;
          if constexpr (std::is_same_v<KeyT, share::ObOptCatalogColumnStat::Key>) {
            // 多列共享同一 partition_value，需要去重后再回源
            if (OB_FAIL(partition_value_set.exist_refactored(partition_value))) {
              if (OB_HASH_EXIST == ret) {
                ret = OB_SUCCESS;
              } else if (OB_HASH_NOT_EXIST == ret) {
                if (OB_FAIL(partition_value_set.set_refactored(partition_value, 0))) {
                  SQL_ENG_LOG(WARN, "failed to add partition value to set", K(ret), K(i), K(partition_value));
                } else if (OB_FAIL(key_partition_values.push_back(partition_value))) {
                  SQL_ENG_LOG(WARN, "failed to push back partition value", K(ret), K(i), K(partition_value));
                }
              } else {
                SQL_ENG_LOG(WARN, "failed to check partition value in set", K(ret), K(i));
              }
            }
          } else {
            // 表统计信息每个分区只有一条 key，无需去重
            if (OB_FAIL(key_partition_values.push_back(partition_value))) {
              SQL_ENG_LOG(WARN, "failed to push back partition value", K(ret), K(i), K(partition_value));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(load_catalog_table_stat_and_put_cache(param,
                                                                      all_partition_values,  // 非分区粒度缓存用这个回源
                                                                      schema_guard,
                                                                      key_partition_values))) {  // 分区粒度缓存时用这个回源
      SQL_ENG_LOG(WARN, "failed to load catalog table stats and put cache", K(ret), K(param));
    }

    partition_value_set.destroy();
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OB_OPT_EXTERNAL_STAT_SERVICE_H
