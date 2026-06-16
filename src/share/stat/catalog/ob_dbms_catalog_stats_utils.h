/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_UTILS_H
#define OB_DBMS_CATALOG_STATS_UTILS_H

#include "share/schema/ob_schema_struct.h"
#include "share/stat/ob_stat_define.h"
#include "share/catalog/ob_catalog_properties.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{
class ObOptCatalogTableStat;
class ObOptCatalogColumnStat;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace common
{
struct ObOptCatalogStat;
struct ObCatalogTableStatParam;
struct ObCatalogColumnStatParam;
struct ObOptCatalogStatGatherParam;
struct ObCatalogExtPartitionInfo;

class ObObjPrintParams;

class ObDbmsCatalogStatsUtils
{
  static constexpr const char HIVE_DEFAULT_PARTITION[] = "__HIVE_DEFAULT_PARTITION__";
  static constexpr int64_t HIVE_DEFAULT_PARTITION_LEN = sizeof(HIVE_DEFAULT_PARTITION) - 1;

public:
  /**
   * @brief Classify external statistics into table stats and column stats
   *
   * Separates ObOptCatalogStat array into ObOptCatalogTableStat* and ObOptCatalogColumnStat*
   * arrays. Corresponds to ObDbmsStatsUtils::calssify_opt_stat for internal tables.
   *
   * @param catalog_stats Input array of ObOptCatalogStat
   * @param table_stats Output array of ObOptCatalogTableStat*
   * @param column_stats Output array of ObOptCatalogColumnStat*
   * @return OB_SUCCESS if successful, other error codes on failure
   */
  static int classify_catalog_opt_stat(const ObIArray<ObOptCatalogStat> &catalog_stats,
                                       ObIArray<share::ObOptCatalogTableStat *> &table_stats,
                                       ObIArray<share::ObOptCatalogColumnStat *> &column_stats);

  /**
   * @brief Convert hex string to object
   *
   * Deserializes an object from a hex string representation.
   * Used for parsing min/max values from system table.
   */
  static int hex_str_to_obj(const char *buf, int64_t buf_len, ObIAllocator &allocator, ObObj &obj);

  /**
   * @brief Check if a partition value is Hive default partition
   *
   * Hive uses "__HIVE_DEFAULT_PARTITION__" to represent NULL partition values.
   * This function checks if a partition value string matches this pattern.
   *
   * @param value Partition value string to check
   * @return true if the value is Hive default partition, false otherwise
   */
  static bool is_hive_default_partition(const ObString &value);

  static int init_catalog_table_stats(const int64_t cnt,
                                      ObIAllocator &allocator,
                                      ObIArray<share::ObOptCatalogTableStat *> &table_stats);

  static int init_catalog_col_stats(const int64_t col_cnt,
                                    ObIAllocator &allocator,
                                    ObIArray<share::ObOptCatalogColumnStat *> &col_stats);

  static int assign_col_param(const ObIArray<ObCatalogColumnStatParam> *src_col_params,
                              const int64_t start,
                              const int64_t end,
                              ObIArray<ObCatalogColumnStatParam> &target_col_params);

  static int deep_copy_string_helper(const int64_t buf_len,
                                     const ObString &str,
                                     char *buf,
                                     int64_t &pos,
                                     ObString &dst);

  /**
   * @brief Split batch write catalog statistics
   *
   * Corresponds to ObDbmsStatsUtils::split_batch_write for internal tables.
   * Writes table and column statistics to system tables in batches to avoid SQL being too long.
   */
  static int split_batch_write(const ObCatalogTableStatParam &table_param,
                               sql::ObExecContext &ctx,
                               sqlclient::ObISQLConnection *conn,
                               ObIArray<share::ObOptCatalogTableStat *> &table_stats,
                               ObIArray<share::ObOptCatalogColumnStat *> &column_stats);

  static int get_part_names_and_column_names(const ObCatalogTableStatParam &table_param,
                                             ObIArray<ObString> &part_names,
                                             ObIArray<ObString> &column_names);
  /**
   * @brief Batch write catalog statistics
   *
   * Corresponds to ObDbmsStatsUtils::batch_write for internal tables.
   * Writes a batch of table and column statistics to system tables.
   */
  static int batch_write(const uint64_t tenant_id,
                        const ObCatalogTableStatParam &table_param,
                        const ObIArray<ObCatalogColumnStatParam> &column_params,
                        const ObObjPrintParams &print_params,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        sqlclient::ObISQLConnection *conn,
                        ObIArray<share::ObOptCatalogTableStat *> &table_stats,
                        ObIArray<share::ObOptCatalogColumnStat *> &column_stats);

  static int get_current_opt_stats(ObIAllocator &allocator,
                                   sqlclient::ObISQLConnection *conn,
                                   const ObCatalogTableStatParam &param,
                                   ObIArray<share::ObOptCatalogTableStat *> &table_stats,
                                   ObIArray<share::ObOptCatalogColumnStat *> &column_stats);

  static int prepare_gather_stat_param(const ObCatalogTableStatParam &param,
                                       const StatLevel stat_level,
                                       const bool is_split_gather,
                                       const int64_t gather_vectorize,
                                       ObOptCatalogStatGatherParam &gather_param);

  // Iceberg: single global gather (basic estimator SQL without partition columns). Merges
  // partition_infos_ for file metadata and clears part_cols_.
  static int normalize_iceberg_gather_param_to_table_level(ObOptCatalogStatGatherParam &param);

  static int merge_split_gather_tab_stats(ObIArray<share::ObOptCatalogTableStat *> &all_tstats,
                                          ObIArray<share::ObOptCatalogTableStat *> &cur_all_tstats);

  static int sync_column_stat_last_analyzed(
      const ObIArray<share::ObOptCatalogTableStat *> &all_tstats,
      ObIArray<share::ObOptCatalogColumnStat *> &all_cstats);

  static bool find_part(const ObIArray<ObCatalogExtPartitionInfo> &part_infos,
                       const ObString &part_name,
                       bool is_sensitive_compare,
                       ObCatalogExtPartitionInfo &part);

  /**
   * @brief Filter catalog partition infos based on remote modify_time and local last_analyzed
   *
   * Compares the remote file modification time with the local last_analyzed timestamp
   * from system table to determine which partitions need to be collected.
   * Modifies param.part_infos_ to keep only partitions that need collection.
   *
   * Decision logic:
   * - If partition not in system table: needs collection
   * - If remote modify_time > local last_analyzed: needs collection, is_continue_collect = true
   * - If remote modify_time == local last_analyzed: skip (no change), is_continue_collect = false
   * - If remote modify_time < local last_analyzed: skip (anomaly, will log warning), is_continue_collect = false
   *
   * @param ctx ExecContext for database operations
   * @param param CatalogTableStatParam with part_infos_ to be filtered (will be modified)
   * @return OB_SUCCESS if successful, other error codes on failure
   */
  static int filter_catalog_partition_infos_by_modify_time(
      sql::ObExecContext &ctx,
      ObCatalogTableStatParam &param,
      bool &is_continue_collect,
      ObArray<ObString> &partitions_to_delete);

  static int filter_partitions_by_freshness(
      const share::ObLakeTableFormat &lake_table_format,
      const ObIArray<ObCatalogExtPartitionInfo> &new_all_partitions,
      const ObIArray<share::ObOptCatalogTableStat *> &existing_stats,
      ObIAllocator &allocator,
      ObArray<ObCatalogExtPartitionInfo> &partitions_to_collect,
      ObArray<ObString> &partitions_to_delete,
      ObIArray<share::ObOptCatalogTableStat *> &unchanged_partition_stats);

  // Delete stale partition stats (partitions that no longer exist in remote)
  static int delete_stale_partition_stats(sql::ObExecContext &ctx,
                                          const ObCatalogTableStatParam &param,
                                          const ObIArray<ObString> &partitions_to_delete);

private:
  static int init_stats_by_part_names_and_col_names(
      const ObCatalogTableStatParam &param,
      const ObIArray<ObString> &partition_names,
      const ObIArray<ObString> &column_names,
      ObIAllocator &allocator,
      ObIArray<share::ObOptCatalogTableStat *> &table_stats,
      ObIArray<share::ObOptCatalogColumnStat *> &column_stats);
  static int filter_nonpart_table_by_modify_time_(const ObCatalogTableStatParam &param,
                                                  ObMySQLTransaction &trans,
                                                  ObIAllocator &allocator,
                                                  bool &is_continue_collect);

  static int filter_part_table_by_modify_time_(
      ObMySQLTransaction &trans,
      ObIAllocator &allocator,
      ObCatalogTableStatParam &param,
      bool &is_continue_collect,
      ObArray<ObString> &partitions_to_delete);
};

} // namespace common
} // namespace oceanbase

#endif // OB_DBMS_CATALOG_STATS_UTILS_H
