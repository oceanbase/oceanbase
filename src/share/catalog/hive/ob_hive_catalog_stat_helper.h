/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H

#include "common/object/ob_obj_type.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/number/ob_number_v2.h"
#include "share/catalog/hive/ob_hive_metastore.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/stat/ob_opt_external_column_stat.h"
#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "share/stat/ob_opt_external_table_stat.h"

namespace oceanbase {
namespace share {

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
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

private:
  /**
   * @brief Fetch table-level statistics for non-partitioned tables
   */
  int fetch_table_level_statistics(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Fetch and merge statistics from all partitions
   */
  int fetch_all_partitions_statistics(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const Strings &partition_names, const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Fetch and merge statistics from specified partitions
   */
  int fetch_specified_partitions_statistics(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Fetch and merge statistics from partitions with batch processing
   */
  int fetch_partitions_statistics_batch(
      ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
      const std::vector<std::string> &partition_names,
      const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats,
      int64_t batch_size = DEFAULT_PARTITION_BATCH_SIZE);

  /**
   * @brief Convert Hive table statistics to OceanBase external statistics
   */
  int convert_hive_table_stats_to_external_stats(
      const ObILakeTableMetadata *table_metadata,
      const ObHiveBasicStats &basic_stats,
      const ApacheHive::TableStatsResult &table_stats_result,
      const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Create external table statistics from basic stats
   */
  int create_external_table_stat(const ObILakeTableMetadata *table_metadata,
                                 const ObHiveBasicStats &basic_stats,
                                 const ObString &partition_value,
                                 const int64_t partition_num,
                                 ObOptExternalTableStat *&external_table_stat);

  /**
   * @brief Create external column statistics from Hive column statistics
   */
  int create_external_column_stats(
      const ObILakeTableMetadata *table_metadata,
      const std::vector<ApacheHive::ColumnStatisticsObj> &hive_column_stats,
      const ObIArray<ObString> &column_names, const ObString &partition_value,
      int64_t total_rows,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Extract column statistics from Hive ColumnStatisticsData
   */
  int extract_column_stat_from_hive_data(
      const ApacheHive::ColumnStatisticsData &stats_data, int64_t total_rows,
      common::ObObjType column_type, int64_t &num_nulls, int64_t &num_not_nulls,
      int64_t &num_distinct, int64_t &avg_length);

  /**
   * @brief Merge multiple partition statistics into aggregated statistics
   */
  int merge_partition_basic_stats(
      const std::vector<ObHiveBasicStats> &partition_basic_stats,
      ObHiveBasicStats &merged_stats);

  /**
   * @brief Merge single Hive column statistics into external column stat
   * builder
   */
  int merge_hive_column_stat_to_builder(
      ObOptExternalColumnStatBuilder *builder,
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
      const std::vector<ApacheHive::ColumnStatisticsObj>
          &partition_column_stats,
      const ObIArray<const ObColumnSchemaV2 *> &column_schemas,
      std::vector<const ApacheHive::ColumnStatisticsObj *>
          &arranged_column_stats);

  int merge_part_column_stat(const ObTableSchema &table_schema,
                             std::vector<ApacheHive::Partition> &partitions,
                             std::vector<ObHiveBasicStats> &basic_stats,
                             ObIArray<const ObColumnSchemaV2 *> &column_schemas,
                             ObIArray<ObOptExternalColumnStatBuilder *> &column_builders);

  int merge_non_part_column_stats(int64_t total_rows,
                                  const std::vector<ApacheHive::ColumnStatisticsObj> &column_stats,
                                  ObIArray<const ObColumnSchemaV2 *> &column_schemas,
                                  ObIArray<ObOptExternalColumnStatBuilder *> &column_builders);

  int prepare_column_builders_and_schemas(
      ObIAllocator &allocator, const ObString &partition_value,
      const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &column_names,
      ObIArray<ObOptExternalColumnStatBuilder *> &part_column_builders,
      ObIArray<ObOptExternalColumnStatBuilder *> &non_part_column_builders,
      const ObTableSchema *&table_schema,
      ObIArray<const ObColumnSchemaV2 *> &part_column_schemas,
      ObIArray<const ObColumnSchemaV2 *> &non_part_column_schemas);

  int collect_column_stats_from_builders(
      ObIArray<ObOptExternalColumnStatBuilder *> &column_builders,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);
  int convert_to_std_vector(const ObIArray<ObString> &ob_array,
                            std::vector<std::string> &std_vector);
  int merge_hive_column_data_to_builder(
      const ApacheHive::BooleanColumnStatsData &boolean_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::LongColumnStatsData &long_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::DoubleColumnStatsData &double_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::StringColumnStatsData &string_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::BinaryColumnStatsData &binary_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::DecimalColumnStatsData &decimal_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::DateColumnStatsData &date_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);
  int merge_hive_column_data_to_builder(
      const ApacheHive::TimestampColumnStatsData &timestamp_column_stat,
      int64_t total_rows, const ObColumnSchemaV2 &column_schema,
      ObOptExternalColumnStatBuilder &builder);

  int merge_part_column_data_to_builder(const ObObj &part_val,
                                        int64_t total_rows,
                                        const ObColumnSchemaV2 &column_schema,
                                        ObOptExternalColumnStatBuilder &builder);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const ObColumnSchemaV2 &column_schema,
                                  int64_t &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const ObColumnSchemaV2 &column_schema,
                                  double &stats_data, common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Decimal &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Date &stats_data,
                                  common::ObObj &obj_value);
  int convert_hive_value_to_obobj(ObIAllocator &allocator,
                                  const ObColumnSchemaV2 &column_schema,
                                  const ApacheHive::Timestamp &stats_data,
                                  common::ObObj &obj_value);

private:
  ObIAllocator &allocator_;
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_STAT_HELPER_H