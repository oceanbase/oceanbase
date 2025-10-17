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

#ifndef _SHARE_CATALOG_HIVE_OB_ICEBERG_CATALOG_STAT_HELPER_H
#define _SHARE_CATALOG_HIVE_OB_ICEBERG_CATALOG_STAT_HELPER_H

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/stat/ob_opt_external_column_stat.h"
#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "share/stat/ob_opt_external_table_stat.h"
#include "share/stat/ob_opt_external_table_stat_builder.h"
#include "sql/table_format/iceberg/spec/snapshot.h"
#include "sql/table_format/iceberg/spec/statistics.h"

namespace oceanbase {
namespace share {

using namespace common;

class ObIcebergCatalogStatHelper {
public:
  explicit ObIcebergCatalogStatHelper(ObIAllocator &allocator);
  ~ObIcebergCatalogStatHelper();

  /**
   * @brief Fetch Iceberg table statistics and convert to OceanBase external
   * statistics
   */
  int fetch_iceberg_table_statistics(
      const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &partition_values,
      const ObIArray<ObString> &column_names,
      ObOptExternalTableStat *&external_table_stat,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

private:
  /**
   * @brief Find the latest statistics file based on snapshot timestamp
   */
  int find_latest_statistics_file(
      const sql::iceberg::TableMetadata &table_metadata,
      std::optional<int64_t> target_snapshot_id,
      const sql::iceberg::StatisticsFile *&statistics_file,
      int64_t &snapshot_timestamp_ms);

  /**
   * @brief Parse NDV value from blob metadata properties
   */
  int parse_ndv_from_properties(
      const ObIArray<std::pair<ObString, ObString>> &properties,
      bool &found_ndv,
      int64_t &ndv_value);

  /**
   * @brief Build field_id to NDV mapping from blob metadata array
   */
  int build_field_id_to_ndv_map(
      const sql::iceberg::StatisticsFile *statistics_file,
      hash::ObHashMap<int32_t, int64_t> &field_ndv_map);

  /**
   * @brief Convert iceberg column statistics to external column statistics
   */
  int convert_iceberg_column_stats_to_external_stats(
      const ObILakeTableMetadata *table_metadata,
      const sql::iceberg::StatisticsFile *statistics_file,
      const ObIArray<ObString> &column_names, int64_t snapshot_timestamp_ms,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Create external table statistics with basic info
   */
  int create_external_table_stat(const ObILakeTableMetadata *table_metadata,
                                 int64_t snapshot_timestamp_ms,
                                 ObOptExternalTableStat *&external_table_stat);

  /**
   * @brief Get column field ID from table schema by column name
   */
  int get_column_field_id_by_name(
      const sql::iceberg::TableMetadata &table_metadata,
      const ObString &column_name, int32_t &field_id, bool &found);

  /**
   * @brief Get column field ID using pre-built map for fast lookup
   */
  int get_column_field_id_by_name_with_map(
      const hash::ObHashMap<ObString, int32_t> &column_name_to_field_id_map,
      const ObString &column_name, int32_t &field_id, bool &found);

  /**
   * @brief Find snapshot by snapshot ID to get timestamp
   */
  int find_snapshot_by_id(const sql::iceberg::TableMetadata &table_metadata,
                          int64_t snapshot_id,
                          const sql::iceberg::Snapshot *&snapshot, bool &found);

private:
  /**
   * @brief Extract table statistics from snapshot summary
   */
  int extract_table_stats_from_snapshot_summary(
      const sql::iceberg::Snapshot &snapshot, int64_t &row_count,
      int64_t &file_count, int64_t &data_size);

  /**
   * @brief Create default column statistics for missing columns
   */
  int create_default_column_statistics(
      const ObILakeTableMetadata *table_metadata,
      const ObIArray<ObString> &column_names, int64_t snapshot_timestamp_ms,
      ObIArray<ObOptExternalColumnStat *> &external_column_stats);

  /**
   * @brief Build column name to field ID mapping for fast lookup
   */
  int build_column_name_to_field_id_map(
      const sql::iceberg::TableMetadata &table_metadata,
      hash::ObHashMap<ObString, int32_t> &column_name_to_field_id_map);

  ObIAllocator &allocator_;
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_CATALOG_HIVE_OB_ICEBERG_CATALOG_STAT_HELPER_H