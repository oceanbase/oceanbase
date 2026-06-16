/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H

#include "common/object/ob_obj_type.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/number/ob_number_v2.h"
#include "share/catalog/hive/ob_hive_metastore.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat_builder.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase {
namespace share {

// Forward declarations for schema types
namespace schema
{
class ObColumnSchemaV2;
class ObTableSchema;
} // namespace schema

class ObHiveCatalogStatHelper {
public:
  explicit ObHiveCatalogStatHelper(ObIAllocator &allocator);
  ~ObHiveCatalogStatHelper();

  // Default batch size for partition statistics fetching
  static const int64_t DEFAULT_PARTITION_BATCH_SIZE = 100;

  /**
   * @brief Fetch Hive table statistics and convert to OceanBase external
   * statistics
   */
  int fetch_hive_table_statistics(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats);

private:
  /**
   * @brief Fetch table-level statistics for non-partitioned tables
   */
  int fetch_table_level_statistics(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &column_names,
      ObOptCatalogTableStat *&catalog_table_stat,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats);

  /**
   * @brief Fetch partition-granularity statistics with batch HMS calls
   */
  int fetch_partitions_statistics_batch(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const std::vector<std::string> &partition_names,
      const ObIArray<ObString> &column_names,
      ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats,
      int64_t batch_size = DEFAULT_PARTITION_BATCH_SIZE);

  /**
   * @brief Convert Hive table statistics to OceanBase external statistics
   */
  int convert_hive_table_stats_to_external_stats(
      const ObILakeTableMetadata *table_metadata,
      const ObHiveBasicStats &basic_stats,
      const ApacheHive::TableStatsResult &table_stats_result,
      const ObIArray<ObString> &column_names,
      ObOptCatalogTableStat *&catalog_table_stat,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats);

  /**
   * @brief Create catalog table statistics from basic stats
   */
  int create_catalog_table_stat(const ObILakeTableMetadata *table_metadata,
                                 const ObHiveBasicStats &basic_stats,
                                 const ObString &partition_value,
                                 const int64_t partition_num,
                                 ObOptCatalogTableStat *&catalog_table_stat);

  /**
   * @brief Create external column statistics from Hive column statistics
   */
  int create_catalog_column_stats(
      const ObILakeTableMetadata *table_metadata,
      const std::vector<ApacheHive::ColumnStatisticsObj> &hive_column_stats,
      const ObIArray<ObString> &column_names, const ObString &partition_value,
      int64_t total_rows,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats);

  /**
   * @brief Extract column statistics from Hive ColumnStatisticsData
   */
  int extract_column_stat_from_hive_data(
      const ApacheHive::ColumnStatisticsData &stats_data, int64_t total_rows,
      common::ObObjType column_type, int64_t &num_nulls, int64_t &num_not_nulls,
      int64_t &num_distinct, int64_t &avg_length);

  /**
   * @brief Merge single Hive column statistics into external column stat
   * builder
   */
  int merge_hive_column_stat_to_builder(
      ObOptCatalogColumnStatBuilder *builder,
      const ApacheHive::ColumnStatisticsObj &hive_col_stat, int64_t total_rows);

  /**
   * @brief Convert Hive min/max values to ObObj for proper comparison
   */
  int convert_hive_value_to_obobj(
      const ApacheHive::ColumnStatisticsData &stats_data, bool is_min_value,
      common::ObObj &obj_value);

  int merge_table_stats(
      const std::vector<ObHiveBasicStats> &table_stats_results,
      ObHiveBasicStats &merged_stats);

  int arrange_partition_column_stats_by_schema_order(
      const std::vector<ApacheHive::ColumnStatisticsObj> &partition_column_stats,
      const ObIArray<const schema::ObColumnSchemaV2 *> &column_schemas,
      std::vector<const ApacheHive::ColumnStatisticsObj *> &arranged_column_stats);

  int merge_part_column_stat(const schema::ObTableSchema &table_schema,
                             std::vector<ApacheHive::Partition> &partitions,
                             std::vector<ObHiveBasicStats> &basic_stats,
                             ObIArray<const schema::ObColumnSchemaV2 *> &column_schemas,
                             ObIArray<ObOptCatalogColumnStatBuilder *> &column_builders);

  int merge_non_part_column_stats(int64_t total_rows,
                                  const std::vector<ApacheHive::ColumnStatisticsObj> &column_stats,
                                  ObIArray<const schema::ObColumnSchemaV2 *> &column_schemas,
                                  ObIArray<ObOptCatalogColumnStatBuilder *> &column_builders);

  int prepare_column_builders_and_schemas(
      ObIAllocator &allocator,
      const ObString &partition_value,
      const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &column_names,
      ObIArray<ObOptCatalogColumnStatBuilder *> &part_column_builders,
      ObIArray<ObOptCatalogColumnStatBuilder *> &non_part_column_builders,
      const schema::ObTableSchema *&table_schema,
      ObIArray<const schema::ObColumnSchemaV2 *> &part_column_schemas,
      ObIArray<const schema::ObColumnSchemaV2 *> &non_part_column_schemas);

  int collect_column_stats_from_builders(
      ObIArray<ObOptCatalogColumnStatBuilder *> &column_builders,
      ObIArray<ObOptCatalogColumnStat *> &catalog_column_stats);
  int convert_to_std_vector(const ObIArray<ObString> &ob_array,
                            std::vector<std::string> &std_vector);
  int merge_hive_column_data_to_builder(
      const ApacheHive::BooleanColumnStatsData &boolean_column_stat,
      int64_t total_rows,
      const schema::ObColumnSchemaV2 &column_schema,
      ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(const ApacheHive::LongColumnStatsData &long_column_stat,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(const ApacheHive::DoubleColumnStatsData &double_column_stat,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(const ApacheHive::StringColumnStatsData &string_column_stat,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(const ApacheHive::BinaryColumnStatsData &binary_column_stat,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::DecimalColumnStatsData &decimal_column_stat,
      int64_t total_rows,
      const schema::ObColumnSchemaV2 &column_schema,
      ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(const ApacheHive::DateColumnStatsData &date_column_stat,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::TimestampColumnStatsData &timestamp_column_stat,
      int64_t total_rows,
      const schema::ObColumnSchemaV2 &column_schema,
      ObOptCatalogColumnStatBuilder &builder);

  int merge_part_column_data_to_builder(const ObObj &part_val,
                                        int64_t total_rows,
                                        const schema::ObColumnSchemaV2 &column_schema,
                                        ObOptCatalogColumnStatBuilder &builder);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const schema::ObColumnSchemaV2 &column_schema,
                                  int64_t &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const schema::ObColumnSchemaV2 &column_schema,
                                  double &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const schema::ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Decimal &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const schema::ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Date &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const schema::ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Timestamp &stats_data,
                                  common::ObObj &obj_value);

private:
  ObIAllocator &allocator_;
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H