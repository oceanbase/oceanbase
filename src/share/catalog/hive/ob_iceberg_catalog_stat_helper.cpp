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

#define USING_LOG_PREFIX SHARE

#include "ob_iceberg_catalog_stat_helper.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"
#include "share/schema/ob_column_schema.h"
#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "share/stat/ob_opt_external_table_stat_builder.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/spec/schema.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase {
namespace share {

ObIcebergCatalogStatHelper::ObIcebergCatalogStatHelper(ObIAllocator &allocator)
    : allocator_(allocator) {}

ObIcebergCatalogStatHelper::~ObIcebergCatalogStatHelper() {}

int ObIcebergCatalogStatHelper::fetch_iceberg_table_statistics(
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;
  external_column_stats.reset();

  // Check parameters
  if (OB_ISNULL(table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata is null", K(ret));
  } else if (column_names.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column names should not be empty", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::ICEBERG !=
                         table_metadata->get_format_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table format should be iceberg", K(ret),
             K(table_metadata->get_format_type()));
  } else {
    // Down cast to iceberg table metadata
    const sql::iceberg::ObIcebergTableMetadata *iceberg_metadata =
        static_cast<const sql::iceberg::ObIcebergTableMetadata *>(
            table_metadata);
    const sql::iceberg::TableMetadata &iceberg_table_metadata =
        iceberg_metadata->table_metadata_;

    // Find the latest statistics file
    const sql::iceberg::StatisticsFile *statistics_file = nullptr;
    int64_t snapshot_timestamp_ms = 0;
    std::optional<int64_t> target_snapshot_id = std::nullopt;

    if (OB_FAIL(find_latest_statistics_file(iceberg_table_metadata,
                                            target_snapshot_id, statistics_file,
                                            snapshot_timestamp_ms))) {
      LOG_WARN("failed to find latest statistics file", K(ret));
    } else {
      // Create table statistics (works with or without statistics file)
      if (OB_FAIL(create_external_table_stat(
              table_metadata, snapshot_timestamp_ms, external_table_stat))) {
        LOG_WARN("failed to create external table stat", K(ret));
      } else if (OB_ISNULL(statistics_file)) {
        // No statistics file available, create default column statistics
        if (OB_FAIL(create_default_column_statistics(
                table_metadata, column_names, snapshot_timestamp_ms,
                external_column_stats))) {
          LOG_WARN("failed to create default column statistics", K(ret));
        }
      } else {
        // Convert iceberg column stats to external stats (handles missing
        // columns internally)
        if (OB_FAIL(convert_iceberg_column_stats_to_external_stats(
                table_metadata, statistics_file, column_names,
                snapshot_timestamp_ms, external_column_stats))) {
          LOG_WARN("failed to convert iceberg column stats to external stats",
                   K(ret));
        }
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::find_latest_statistics_file(
    const sql::iceberg::TableMetadata &table_metadata,
    std::optional<int64_t> target_snapshot_id,
    const sql::iceberg::StatisticsFile *&statistics_file,
    int64_t &snapshot_timestamp_ms) {
  int ret = OB_SUCCESS;
  statistics_file = nullptr;
  snapshot_timestamp_ms = 0;

  if (table_metadata.statistics.empty()) {
    // No statistics available
  } else if (target_snapshot_id.has_value()) {
    // Find statistics for specific snapshot
    for (int64_t i = 0; OB_SUCC(ret) && i < table_metadata.statistics.count();
         ++i) {
      const sql::iceberg::StatisticsFile *stat_file =
          table_metadata.statistics.at(i);
      if (OB_ISNULL(stat_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("statistics file is null", K(ret), K(i),
                 K(table_metadata.statistics));
      } else if (stat_file->snapshot_id == target_snapshot_id.value()) {
        statistics_file = stat_file;
        const sql::iceberg::Snapshot *snapshot = nullptr;
        bool snapshot_found = false;
        if (OB_FAIL(find_snapshot_by_id(table_metadata, stat_file->snapshot_id,
                                        snapshot, snapshot_found))) {
          LOG_WARN("failed to find snapshot by id", K(ret),
                   K(stat_file->snapshot_id));
        } else if (!snapshot_found || OB_ISNULL(snapshot)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snapshot not found", K(ret), K(stat_file->snapshot_id));
        } else {
          snapshot_timestamp_ms = snapshot->timestamp_ms;
          break;
        }
      }
    }
  } else {
    // Find the latest statistics based on snapshot timestamp
    int64_t latest_timestamp = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_metadata.statistics.count();
         ++i) {
      const sql::iceberg::StatisticsFile *stat_file =
          table_metadata.statistics.at(i);
      if (OB_ISNULL(stat_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("statistics file is null", K(ret), K(i));
      } else {
        const sql::iceberg::Snapshot *snapshot = nullptr;
        bool snapshot_found = false;
        if (OB_FAIL(find_snapshot_by_id(table_metadata, stat_file->snapshot_id,
                                        snapshot, snapshot_found))) {
          LOG_WARN("failed to find snapshot by id", K(ret),
                   K(stat_file->snapshot_id));
        } else if (!snapshot_found || OB_ISNULL(snapshot)) {
          LOG_DEBUG("snapshot not found for statistics file, skip it",
                    K(stat_file->snapshot_id));
        } else if (snapshot->timestamp_ms > latest_timestamp) {
          latest_timestamp = snapshot->timestamp_ms;
          statistics_file = stat_file;
          snapshot_timestamp_ms = snapshot->timestamp_ms;
        }
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::parse_ndv_from_properties(
    const ObIArray<std::pair<ObString, ObString>> &properties,
    bool &found_ndv,
    int64_t &ndv_value) {
  int ret = OB_SUCCESS;
  ndv_value = 0;
  found_ndv = false;
  char temp_buf[64];
  // Find NDV in properties
  for (int64_t i = 0; OB_SUCC(ret) && !found_ndv && i < properties.count(); ++i) {
    const std::pair<ObString, ObString> &property = properties.at(i);
    if (0 == property.first.case_compare("ndv")) {
      char *endptr = nullptr;
      const char *value_str = property.second.ptr();
      int64_t value_len = property.second.length();

      // Convert string to number
      if (value_len >= sizeof(temp_buf)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ndv value string too long", K(ret), K(value_len));
      } else {
        MEMCPY(temp_buf, value_str, value_len);
        temp_buf[value_len] = '\0';
        ndv_value = strtoll(temp_buf, &endptr, 10);
        if (endptr == temp_buf || *endptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ndv value", K(ret), K(property.second));
        } else {
          found_ndv = true;
        }
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::build_field_id_to_ndv_map(
    const sql::iceberg::StatisticsFile *statistics_file,
    hash::ObHashMap<int32_t, int64_t> &field_ndv_map) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(statistics_file)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statistics file is null", K(ret));
  } else {
    // Process all blob metadata to build field_id -> ndv mapping
    for (int64_t i = 0; OB_SUCC(ret) && i < statistics_file->blob_metadata.count(); ++i) {
      const sql::iceberg::BlobMetadata *blob_meta = statistics_file->blob_metadata.at(i);
      if (OB_ISNULL(blob_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("blob metadata is null", K(ret), K(i));
      } else {
        // Extract NDV from this blob metadata
        int64_t ndv_value = 0;
        bool found_ndv = false;
        if (OB_FAIL(parse_ndv_from_properties(blob_meta->properties, found_ndv, ndv_value))) {
          LOG_WARN("failed to parse ndv from properties", K(ret), K(i));
        } else if (found_ndv) {
          // Map NDV to all field IDs in this blob metadata
          for (int64_t j = 0; OB_SUCC(ret) && j < blob_meta->fields.count(); ++j) {
            int32_t field_id = blob_meta->fields.at(j);
            if (OB_FAIL(field_ndv_map.set_refactored(field_id, ndv_value))) {
              if (OB_HASH_EXIST == ret) {
                ret = OB_SUCCESS;
                LOG_INFO("field ndv map already exists", K(field_id), K(ndv_value));
              } else {
                LOG_WARN("failed to set field ndv map", K(ret), K(field_id), K(ndv_value));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::convert_iceberg_column_stats_to_external_stats(
    const ObILakeTableMetadata *table_metadata,
    const sql::iceberg::StatisticsFile *statistics_file,
    const ObIArray<ObString> &column_names, int64_t snapshot_timestamp_ms,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_metadata) || OB_ISNULL(statistics_file)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(table_metadata),
             KP(statistics_file));
  } else {
    const sql::iceberg::ObIcebergTableMetadata *iceberg_metadata =
        static_cast<const sql::iceberg::ObIcebergTableMetadata *>(
            table_metadata);
    const sql::iceberg::TableMetadata &iceberg_table_metadata =
        iceberg_metadata->table_metadata_;

    // Build field_id to NDV mapping from blob_metadata array
    hash::ObHashMap<int32_t, int64_t> field_ndv_map;
    // Build column name to field_id mapping for fast lookup
    hash::ObHashMap<ObString, int32_t> column_name_to_field_id_map;

    if (OB_FAIL(field_ndv_map.create(statistics_file->blob_metadata.count() + 128,
                                     "IcebergNdvMap"))) {
      LOG_WARN("failed to create field ndv map", K(ret));
    } else if (OB_FAIL(build_field_id_to_ndv_map(statistics_file, field_ndv_map))) {
      LOG_WARN("failed to build field id to ndv map", K(ret));
    } else if (OB_FAIL(build_column_name_to_field_id_map(iceberg_table_metadata,
                                                         column_name_to_field_id_map))) {
      LOG_WARN("failed to build column name to field id map", K(ret));
    } else {
      // Process each requested column
      for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
        const ObString &column_name = column_names.at(i);
        int32_t field_id = -1;
        bool found = false;
        int64_t ndv_value = 0;
        bool found_stats = false;

        if (OB_FAIL(get_column_field_id_by_name_with_map(column_name_to_field_id_map,
                                                          column_name, field_id, found))) {
          LOG_WARN("failed to get field id for column", K(ret), K(column_name));
        } else if (!found) {
          LOG_DEBUG("column not found in schema, will create default statistics",
                    K(column_name));
          // Column not found in schema, but still create default statistics
        } else {
          // Look up NDV value from the pre-built map
          if (OB_FAIL(field_ndv_map.get_refactored(field_id, ndv_value))) {
            if (OB_HASH_NOT_EXIST == ret) {
              // Field not found in map, use default value
              ret = OB_SUCCESS;
              ndv_value = 0;
              found_stats = false;
            } else {
              LOG_WARN("failed to get ndv from field map", K(ret), K(field_id));
            }
          } else {
            found_stats = true;
          }
        }

        // Create external column stat for all requested columns (found stats or
        // default)
        if (OB_SUCC(ret)) {
          ObOptExternalColumnStatBuilder stat_builder(allocator_);
          ObOptExternalColumnStat *external_column_stat = nullptr;

          if (OB_FAIL(stat_builder.set_basic_info(
                  table_metadata->tenant_id_, table_metadata->catalog_id_,
                  table_metadata->namespace_name_, table_metadata->table_name_,
                  ObString(""), // partition value
                  column_name))) {
            LOG_WARN("failed to set basic info for column stat builder", K(ret),
                     K(column_name));
          } else if (OB_FAIL(stat_builder.set_stat_info(
                         0,         // num_null
                         0,         // num_not_null
                         ndv_value, // num_distinct (0 for default, actual value if found)
                         0,         // avg_length
                         found_stats ? snapshot_timestamp_ms : 0, // last_analyzed
                         common::ObCollationType::CS_TYPE_INVALID))) {
            LOG_WARN("failed to set stat info for column stat builder", K(ret),
                     K(column_name));
          } else if (OB_FAIL(stat_builder.finalize_bitmap())) {
            LOG_WARN("failed to finalize bitmap", K(ret), K(column_name));
          } else if (OB_FAIL(
                         stat_builder.build(allocator_, external_column_stat))) {
            LOG_WARN("failed to build external column stat", K(ret),
                     K(column_name));
          } else if (OB_FAIL(
                         external_column_stats.push_back(external_column_stat))) {
            LOG_WARN("failed to add external column stat to array", K(ret),
                     K(column_name));
          }
        }
      }
    }

    // Clean up the hash maps
    field_ndv_map.destroy();
    column_name_to_field_id_map.destroy();
  }

  return ret;
}

int ObIcebergCatalogStatHelper::create_external_table_stat(
    const ObILakeTableMetadata *table_metadata, int64_t snapshot_timestamp_ms,
    ObOptExternalTableStat *&external_table_stat) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;
  int64_t row_count = 0;
  int64_t file_count = 0;
  int64_t data_size = 0;

  if (OB_ISNULL(table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata is null", K(ret));
  } else {
    const sql::iceberg::ObIcebergTableMetadata *iceberg_metadata =
        static_cast<const sql::iceberg::ObIcebergTableMetadata *>(
            table_metadata);
    const sql::iceberg::TableMetadata &iceberg_table_metadata =
        iceberg_metadata->table_metadata_;

    // Try to get current snapshot to extract table statistics
    const sql::iceberg::Snapshot *current_snapshot = nullptr;
    if (OB_FAIL(
            iceberg_table_metadata.get_current_snapshot(current_snapshot))) {
      LOG_WARN("failed to get current snapshot, will use default values",
               K(ret));
    } else if (OB_ISNULL(current_snapshot)) {
      LOG_INFO("current snapshot is null, will use default values");
    } else {
      // Extract statistics from snapshot summary
      if (OB_FAIL(extract_table_stats_from_snapshot_summary(
              *current_snapshot, row_count, file_count, data_size))) {
        LOG_WARN("failed to extract table stats from snapshot summary, use "
                 "default values",
                 K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObOptExternalTableStatBuilder stat_builder;

      if (OB_FAIL(stat_builder.set_basic_info(
              table_metadata->tenant_id_, table_metadata->catalog_id_,
              table_metadata->namespace_name_, table_metadata->table_name_,
              ObString("")))) { // partition value
        LOG_WARN("failed to set basic info for table stat builder", K(ret));
      } else if (OB_FAIL(stat_builder.set_stat_info(
                     row_count, file_count, data_size,
                     snapshot_timestamp_ms))) { // last_analyzed
        LOG_WARN("failed to set stat info for table stat builder", K(ret));
      } else if (OB_FAIL(stat_builder.build(allocator_, external_table_stat))) {
        LOG_WARN("failed to build external table stat", K(ret));
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::build_column_name_to_field_id_map(
    const sql::iceberg::TableMetadata &table_metadata,
    hash::ObHashMap<ObString, int32_t> &column_name_to_field_id_map) {
  int ret = OB_SUCCESS;

  // Get current schema
  const sql::iceberg::Schema *schema = nullptr;
  if (OB_FAIL(table_metadata.get_schema(table_metadata.current_schema_id,
                                        schema))) {
    LOG_WARN("failed to get current schema", K(ret),
             K(table_metadata.current_schema_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current schema is null", K(ret));
  } else {
    // Create the map
    if (OB_FAIL(column_name_to_field_id_map.create(schema->fields.count() + 16,
                                                   "ColumnNameMap"))) {
      LOG_WARN("failed to create column name to field id map", K(ret));
    } else {
      // Build column name to field ID mapping
      for (int64_t i = 0; OB_SUCC(ret) && i < schema->fields.count(); ++i) {
        const share::schema::ObColumnSchemaV2 *column_schema =
            schema->fields.at(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret), K(i));
        } else {
          int32_t field_id = sql::iceberg::ObIcebergUtils::get_iceberg_field_id(column_schema->get_column_id());
          if (OB_FAIL(column_name_to_field_id_map.set_refactored(column_schema->get_column_name(),
                                                                 field_id))) {
            LOG_WARN("failed to set column name to field id mapping", K(ret),
                     K(column_schema->get_column_name()), K(field_id));
          }
        }
      }
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::get_column_field_id_by_name_with_map(
    const hash::ObHashMap<ObString, int32_t> &column_name_to_field_id_map,
    const ObString &column_name, int32_t &field_id, bool &found) {
  int ret = OB_SUCCESS;
  field_id = -1;
  found = false;

  // First try exact match for best performance
  if (OB_FAIL(column_name_to_field_id_map.get_refactored(column_name, field_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get field id from map", K(ret), K(column_name));
    }
  } else {
    found = true;
  }

  // Note: If column is not found, found will be false but ret remains
  // OB_SUCCESS This is not an error condition, just means the column doesn't
  // exist in schema
  if (OB_SUCC(ret) && !found) {
    LOG_DEBUG("column not found in schema", K(column_name));
  }

  return ret;
}

int ObIcebergCatalogStatHelper::get_column_field_id_by_name(
    const sql::iceberg::TableMetadata &table_metadata,
    const ObString &column_name, int32_t &field_id, bool &found) {
  int ret = OB_SUCCESS;
  field_id = -1;
  found = false;

  // Get current schema
  const sql::iceberg::Schema *schema = nullptr;
  if (OB_FAIL(table_metadata.get_schema(table_metadata.current_schema_id,
                                        schema))) {
    LOG_WARN("failed to get current schema", K(ret),
             K(table_metadata.current_schema_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current schema is null", K(ret));
  } else {
    // Search for column by name in schema fields
    for (int64_t i = 0; OB_SUCC(ret) && i < schema->fields.count(); ++i) {
      const share::schema::ObColumnSchemaV2 *column_schema =
          schema->fields.at(i);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(i));
      } else if (0 ==
                 column_name.case_compare(column_schema->get_column_name())) {
        // Convert column_id back to original field_id by subtracting the
        // reserved offset
        field_id = sql::iceberg::ObIcebergUtils::get_iceberg_field_id(column_schema->get_column_id());
        found = true;
        break;
      }
    }

    // Note: If column is not found, found will be false but ret remains
    // OB_SUCCESS This is not an error condition, just means the column doesn't
    // exist in schema
    if (OB_SUCC(ret) && !found) {
      LOG_DEBUG("column not found in schema", K(column_name));
    }
  }

  return ret;
}

int ObIcebergCatalogStatHelper::find_snapshot_by_id(
    const sql::iceberg::TableMetadata &table_metadata, int64_t snapshot_id,
    const sql::iceberg::Snapshot *&snapshot, bool &found) {
  int ret = OB_SUCCESS;
  snapshot = nullptr;
  found = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < table_metadata.snapshots.count();
       ++i) {
    const sql::iceberg::Snapshot *snap = table_metadata.snapshots.at(i);
    if (OB_ISNULL(snap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("snapshot is null", K(ret), K(i));
    } else if (snap->snapshot_id == snapshot_id) {
      snapshot = snap;
      found = true;
      break;
    }
  }

  // Note: If snapshot is not found, found will be false but ret remains
  // OB_SUCCESS This is not an error condition, just means the snapshot doesn't
  // exist
  if (OB_SUCC(ret) && !found) {
    LOG_DEBUG("snapshot not found", K(snapshot_id));
  }

  return ret;
}

int ObIcebergCatalogStatHelper::extract_table_stats_from_snapshot_summary(
    const sql::iceberg::Snapshot &snapshot, int64_t &row_count,
    int64_t &file_count, int64_t &data_size) {
  int ret = OB_SUCCESS;
  row_count = 0;
  file_count = 0;
  data_size = 0;

  // Extract statistics from snapshot summary
  for (int64_t i = 0; OB_SUCC(ret) && i < snapshot.summary.count(); ++i) {
    const std::pair<ObString, ObString> &entry = snapshot.summary.at(i);
    const ObString &key = entry.first;
    const ObString &value = entry.second;

    if (0 == key.case_compare("total-records")) {
      char *endptr = nullptr;
      char temp_buf[64];
      int64_t value_len = value.length();

      if (value_len >= sizeof(temp_buf)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("total-records value string too long", K(ret), K(value_len));
      } else {
        MEMCPY(temp_buf, value.ptr(), value_len);
        temp_buf[value_len] = '\0';
        row_count = strtoll(temp_buf, &endptr, 10);
        if (endptr == temp_buf || *endptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid total-records value", K(ret), K(value));
        }
      }
    } else if (0 == key.case_compare("total-data-files")) {
      char *endptr = nullptr;
      char temp_buf[64];
      int64_t value_len = value.length();

      if (value_len >= sizeof(temp_buf)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("total-data-files value string too long", K(ret),
                 K(value_len));
      } else {
        MEMCPY(temp_buf, value.ptr(), value_len);
        temp_buf[value_len] = '\0';
        file_count = strtoll(temp_buf, &endptr, 10);
        if (endptr == temp_buf || *endptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid total-data-files value", K(ret), K(value));
        }
      }
    }
    // Note: iceberg snapshot summary typically doesn't include data size
    // information data_size remains 0 as it's not available in snapshot summary
  }

  return ret;
}

int ObIcebergCatalogStatHelper::create_default_column_statistics(
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &column_names, int64_t snapshot_timestamp_ms,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata is null", K(ret));
  } else if (column_names.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column names should not be empty", K(ret));
  } else {
    // Create default column statistics for each requested column
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      const ObString &column_name = column_names.at(i);
      ObOptExternalColumnStat *external_column_stat = nullptr;
      ObOptExternalColumnStatBuilder stat_builder(allocator_);

      if (OB_FAIL(stat_builder.set_basic_info(
              table_metadata->tenant_id_, table_metadata->catalog_id_,
              table_metadata->namespace_name_, table_metadata->table_name_,
              ObString(""), // partition value
              column_name))) {
        LOG_WARN("failed to set basic info for default column stat builder",
                 K(ret), K(column_name));
      } else if (OB_FAIL(stat_builder.set_stat_info(
                     0, // num_null
                     0, // num_not_null
                     0, // num_distinct
                     0, // avg_length
                     0, // last_analyzed
                     common::ObCollationType::CS_TYPE_INVALID))) {
        LOG_WARN("failed to set stat info for default column stat builder",
                 K(ret), K(column_name));
      } else if (OB_FAIL(stat_builder.finalize_bitmap())) {
        LOG_WARN("failed to finalize bitmap", K(ret), K(column_name));
      } else if (OB_FAIL(
                     stat_builder.build(allocator_, external_column_stat))) {
        LOG_WARN("failed to build default external column stat", K(ret),
                 K(column_name));
      } else if (OB_FAIL(
                     external_column_stats.push_back(external_column_stat))) {
        LOG_WARN("failed to add default external column stat to array", K(ret),
                 K(column_name));
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase