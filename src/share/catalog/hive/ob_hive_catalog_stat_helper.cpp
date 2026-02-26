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

#include "ob_hive_catalog_stat_helper.h"
#include "common/object/ob_obj_type.h"
#include "lib/charset/ob_template_helper.h"
#include "lib/number/ob_number_v2.h"
#include "lib/time/ob_time_utility.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "share/stat/ob_opt_external_table_stat_builder.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
namespace oceanbase {
namespace share {

// Medium int constants
#ifndef INT24_MIN
#define INT24_MIN (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX (16777215U)
#endif

ObHiveCatalogStatHelper::ObHiveCatalogStatHelper(ObIAllocator &allocator)
    : allocator_(allocator) {}

ObHiveCatalogStatHelper::~ObHiveCatalogStatHelper() {}

int ObHiveCatalogStatHelper::fetch_hive_table_statistics(
    ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;
  external_column_stats.reset();

  // Check parameters
  if (OB_ISNULL(client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table metadata is null", K(ret));
  } else if (column_names.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column names should not be empty", K(ret));
  }

  if (OB_SUCC(ret)) {
    // Get basic table information from metadata
    const ObString &ns_name = table_metadata->namespace_name_;
    const ObString &table_name = table_metadata->table_name_;
    const ObNameCaseMode case_mode = table_metadata->case_mode_;

    // List partition names to determine if it's a partitioned table
    Strings partition_names;
    if (OB_FAIL(client->list_partition_names(ns_name, table_name, case_mode,
                                             partition_names))) {
      LOG_WARN("failed to list partition names", K(ret), K(ns_name),
               K(table_name));
    } else {
      LOG_DEBUG("listed partition names", K(ret), K(ns_name), K(table_name),
                "partition_count", partition_names.size());

      if (partition_names.empty()) {
        // Non-partitioned table - get table-level statistics directly
        if (OB_FAIL(fetch_table_level_statistics(
                client, table_metadata, column_names, external_table_stat,
                external_column_stats))) {
          LOG_WARN("failed to fetch table level statistics", K(ret), K(ns_name),
                   K(table_name));
        }
      } else {
        // Partitioned table - get partition-level statistics and merge
        if (partition_values.empty()) {
          // Get statistics for all partitions and merge
          if (OB_FAIL(fetch_all_partitions_statistics(
                  client, table_metadata, partition_names, column_names,
                  external_table_stat, external_column_stats))) {
            LOG_WARN("failed to fetch all partitions statistics", K(ret),
                     K(ns_name), K(table_name));
          }
        } else {
          // Get statistics for specified partitions only
          if (OB_FAIL(fetch_specified_partitions_statistics(
                  client, table_metadata, partition_values, column_names,
                  external_table_stat, external_column_stats))) {
            LOG_WARN("failed to fetch specified partitions statistics", K(ret),
                     K(ns_name), K(table_name));
          }
        }
      }
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::fetch_table_level_statistics(
    ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;
  const ObString &ns_name = table_metadata->namespace_name_;
  const ObString &table_name = table_metadata->table_name_;
  const ObNameCaseMode case_mode = table_metadata->case_mode_;

  // Get basic statistics (row count, file count, data size)
  ObHiveBasicStats basic_stats;
  if (OB_FAIL(client->get_table_basic_stats(ns_name, table_name, case_mode,
                                            basic_stats))) {
    LOG_WARN("failed to get table basic stats", K(ret), K(ns_name),
             K(table_name));
  } else {
    LOG_DEBUG("got table basic stats", K(ret), K(ns_name), K(table_name),
              K(basic_stats));

    // Get detailed column statistics
    std::vector<std::string> std_column_names;
    for (int64_t i = 0; i < column_names.count(); ++i) {
      std_column_names.emplace_back(column_names.at(i).ptr(),
                                    column_names.at(i).length());
    }

    bool found = false;
    ApacheHive::TableStatsResult table_stats_result;
    if (OB_FAIL(client->get_table_statistics(ns_name, table_name, case_mode,
                                             std_column_names, found,
                                             table_stats_result))) {
      LOG_WARN("failed to get table statistics", K(ret), K(ns_name),
               K(table_name));
    } else if (!found) {
      LOG_DEBUG("table statistics not found, using basic stats only", K(ret),
                K(ns_name), K(table_name));
      // Create empty table stats result for consistency
      table_stats_result.tableStats.clear();
    }

    if (OB_SUCC(ret)) {
      // Convert Hive statistics to OceanBase external statistics
      if (OB_FAIL(convert_hive_table_stats_to_external_stats(
              table_metadata, basic_stats, table_stats_result, column_names,
              external_table_stat, external_column_stats))) {
        LOG_WARN("failed to convert hive table stats to external stats", K(ret),
                 K(ns_name), K(table_name));
      }
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::fetch_all_partitions_statistics(
    ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
    const Strings &partition_names, const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;

  // Convert Strings to std::vector<std::string> for compatibility
  std::vector<std::string> std_partition_names;
  for (const std::string &partition_name : partition_names) {
    std_partition_names.push_back(partition_name);
  }

  // Use batch processing to fetch all partition statistics
  if (OB_FAIL(fetch_partitions_statistics_batch(
          client, table_metadata, std_partition_names, column_names,
          external_table_stat, external_column_stats))) {
    LOG_WARN("failed to fetch all partitions statistics via batch processing",
             K(ret));
  }

  return ret;
}

int ObHiveCatalogStatHelper::fetch_specified_partitions_statistics(
    ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;

  // Convert partition values to partition names
  std::vector<std::string> specified_partition_names;
  for (int64_t i = 0; i < partition_values.count(); ++i) {
    specified_partition_names.emplace_back(partition_values.at(i).ptr(),
                                           partition_values.at(i).length());
  }

  // Use batch processing to fetch partition statistics
  if (OB_FAIL(fetch_partitions_statistics_batch(
          client, table_metadata, specified_partition_names, column_names,
          external_table_stat, external_column_stats))) {
    LOG_WARN(
        "failed to fetch specified partitions statistics via batch processing",
        K(ret));
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_table_stats(
    const std::vector<ObHiveBasicStats> &table_stats_results,
    ObHiveBasicStats &merged_stats) {
  int ret = OB_SUCCESS;
  for (const ObHiveBasicStats &table_stats_result : table_stats_results) {
    merged_stats.num_files_ += table_stats_result.num_files_;
    merged_stats.num_rows_ += table_stats_result.num_rows_;
    merged_stats.total_size_ += table_stats_result.total_size_;
  }
  return ret;
}

int ObHiveCatalogStatHelper::arrange_partition_column_stats_by_schema_order(
    const std::vector<ApacheHive::ColumnStatisticsObj> &partition_column_stats,
    const ObIArray<const ObColumnSchemaV2 *> &column_schemas,
    std::vector<const ApacheHive::ColumnStatisticsObj *>
        &arranged_column_stats) {
  int ret = OB_SUCCESS;
  const int64_t LINEAR_SEARCH_THRESHOLD = 50;

  if (partition_column_stats.size() < LINEAR_SEARCH_THRESHOLD) {
    // Use linear search for small data sets
    for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
      const ObColumnSchemaV2 *column_schema = column_schemas.at(i);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(i));
      } else {
        const ObString &column_name = column_schema->get_column_name();
        const ApacheHive::ColumnStatisticsObj *found_stat = nullptr;

        // Linear search through partition_column_stats
        for (int64_t j = 0; j < partition_column_stats.size(); ++j) {
          if (partition_column_stats[j].colName.length() ==
                  column_name.length() &&
              memcmp(partition_column_stats[j].colName.c_str(),
                     column_name.ptr(), column_name.length()) == 0) {
            found_stat = &partition_column_stats[j];
            break;
          }
        }

        arranged_column_stats.emplace_back(found_stat);
      }
    }
  } else {
    // Use hashmap for larger data sets
    hash::ObHashMap<ObString, const ApacheHive::ColumnStatisticsObj *>
        column_name_to_stat_map;
    if (OB_FAIL(column_name_to_stat_map.create(
            hash::cal_next_prime(partition_column_stats.size() + 128),
            "ColStatMap"))) {
      LOG_WARN("failed to create column name to stat map", K(ret));
    } else {
      // Build the mapping
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_column_stats.size();
           ++i) {
        ObString column_name =
            ObString::make_string(partition_column_stats[i].colName.c_str());
        if (OB_FAIL(column_name_to_stat_map.set_refactored(
                column_name, &partition_column_stats[i]))) {
          LOG_WARN("failed to set column name to stat mapping", K(ret),
                   K(column_name), K(i));
        }
      }

      // Arrange stats according to column_schemas order
      for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
        const ObColumnSchemaV2 *column_schema = column_schemas.at(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret), K(i));
        } else {
          const ObString &column_name = column_schema->get_column_name();
          const ApacheHive::ColumnStatisticsObj *found_stat = nullptr;

          int hash_ret =
              column_name_to_stat_map.get_refactored(column_name, found_stat);
          if (OB_HASH_NOT_EXIST == hash_ret) {
            // Column stat not found, use nullptr
            found_stat = nullptr;
          } else if (OB_SUCCESS != hash_ret) {
            ret = hash_ret;
            LOG_WARN("failed to get column stat from map", K(ret),
                     K(column_name));
            break;
          }
          if (OB_FAIL(column_name_to_stat_map.get_refactored(column_name,
                                                             found_stat))) {
            if (OB_HASH_NOT_EXIST == ret) {
              found_stat = nullptr;
            } else {
              LOG_WARN("failed to get column stat from map", K(ret),
                       K(column_name));
            }
          }
          if (OB_SUCC(ret)) {
            arranged_column_stats.emplace_back(found_stat);
          }
        }
      }

      if (column_name_to_stat_map.created()) {
        column_name_to_stat_map.destroy();
      }
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_part_column_stat(const ObTableSchema &table_schema,
                                                    std::vector<ApacheHive::Partition> &partitions,
                                                    std::vector<ObHiveBasicStats> &basic_stats,
                                                    ObIArray<const ObColumnSchemaV2 *> &column_schemas,
                                                    ObIArray<ObOptExternalColumnStatBuilder *> &column_builders)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("HivePartRow", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  ObSEArray<ObString, 4> partition_values;
  ObNewRow part_row;
  if (OB_UNLIKELY(column_schemas.count() != column_builders.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema and builders size mismatch", K(column_schemas.count()), K(column_builders.count()));
  } else if (OB_UNLIKELY(partitions.size() != basic_stats.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partitions and basic_stats size mismatch", K(partitions.size()), K(basic_stats.size()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.size(); ++i) {
      partition_values.reuse();
      part_row.reset();
      tmp_allocator.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < partitions[i].values.size(); ++j) {
        if (OB_FAIL(partition_values.push_back(ObString(partitions[i].values[j].c_str())))) {
          LOG_WARN("failed to push back partition values");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hive::ObHiveTableMetadata::calculate_part_val_from_string(table_schema,
                                                                                   true,
                                                                                   partition_values,
                                                                                   tmp_allocator,
                                                                                   part_row))) {
        LOG_WARN("failed to calculate partition value", K(partition_values));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < column_schemas.count(); ++j) {
        const ObColumnSchemaV2 *column_schema = column_schemas.at(j);
        ObOptExternalColumnStatBuilder *builder = column_builders.at(j);
        if (OB_ISNULL(column_schema) || OB_ISNULL(builder)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(column_schema), K(builder));
        } else if (OB_UNLIKELY(column_schema->get_part_key_pos() > part_row.get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected part key", K(column_schema->get_part_key_pos()), K(part_row));
        } else if (OB_FAIL(merge_part_column_data_to_builder(part_row.get_cell(column_schema->get_part_key_pos() - 1),
                                                             basic_stats[i].num_rows_,
                                                             *column_schema, *builder))) {
          LOG_WARN("failed to merge partition column data to builder");
        }
      }
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::merge_non_part_column_stats(
    int64_t total_rows,
    const std::vector<ApacheHive::ColumnStatisticsObj> &column_stats,
    ObIArray<const ObColumnSchemaV2 *> &column_schemas,
    ObIArray<ObOptExternalColumnStatBuilder *> &column_builders)
{
  int ret = OB_SUCCESS;
  std::vector<const ApacheHive::ColumnStatisticsObj *> arranged_column_stats;
  if (OB_UNLIKELY(column_schemas.count() != column_builders.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema and builders size mismatch", K(column_schemas.count()), K(column_builders.count()));
  } else if (OB_FAIL(arrange_partition_column_stats_by_schema_order(column_stats, column_schemas,
                                                                    arranged_column_stats))) {
    LOG_WARN("failed to arrange partition column stats by schema order");
  } else {
    // Process each column according to schema order
    for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
      const ObColumnSchemaV2 *column_schema = column_schemas.at(i);
      ObOptExternalColumnStatBuilder *builder = column_builders.at(i);
      const ApacheHive::ColumnStatisticsObj *column_stat_obj = arranged_column_stats.at(i);
      if (OB_ISNULL(column_schema) || OB_ISNULL(builder)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(column_schema), K(builder));
      } else if (OB_ISNULL(column_stat_obj)) {
        // do nothing
      } else {
        const ApacheHive::ColumnStatisticsData &partition_column_stat =
            column_stat_obj->statsData;
        const ObString &column_name = column_schema->get_column_name();

        // Merge the column stat data to builder
        if (partition_column_stat.__isset.booleanStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.booleanStats, total_rows,
                  *column_schema, *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.longStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.longStats, total_rows, *column_schema,
                  *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.doubleStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.doubleStats, total_rows, *column_schema,
                  *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.stringStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.stringStats, total_rows, *column_schema,
                  *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.binaryStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.binaryStats, total_rows, *column_schema,
                  *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.decimalStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.decimalStats, total_rows,
                  *column_schema, *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else if (partition_column_stat.__isset.timestampStats) {
          if (OB_FAIL(merge_hive_column_data_to_builder(
                  partition_column_stat.timestampStats, total_rows,
                  *column_schema, *builder))) {
            LOG_WARN("failed to merge hive column data to builder", K(ret),
                     K(column_name));
          }
        } else {
          LOG_TRACE("this column do not have stat data", K(ret), K(column_name));
        }
      }
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::prepare_column_builders_and_schemas(
    ObIAllocator &allocator, const ObString &partition_value,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &column_names,
    ObIArray<ObOptExternalColumnStatBuilder *> &part_column_builders,
    ObIArray<ObOptExternalColumnStatBuilder *> &non_part_column_builders,
    const ObTableSchema *&table_schema,
    ObIArray<const ObColumnSchemaV2 *> &part_column_schemas,
    ObIArray<const ObColumnSchemaV2 *> &non_part_column_schemas)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata is null", K(ret));
  } else if (OB_UNLIKELY(table_metadata->get_format_type() !=
                         ObLakeTableFormat::HIVE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata format type is not hive", K(ret),
             K(table_metadata->get_format_type()));
  } else {
    const sql::hive::ObHiveTableMetadata *hive_table_metadata =
        static_cast<const sql::hive::ObHiveTableMetadata *>(table_metadata);
    table_schema = &(hive_table_metadata->get_table_schema());
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      const ObString &column_name = column_names.at(i);
      const ObColumnSchemaV2 *column_schema = nullptr;
      ObOptExternalColumnStatBuilder *builder =
          OB_NEWx(ObOptExternalColumnStatBuilder, &allocator, allocator);
      if (OB_ISNULL(builder)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate column stat builder", K(ret), K(i));
      } else if (OB_FAIL(builder->set_basic_info(table_metadata->tenant_id_,
                                                 table_metadata->catalog_id_,
                                                 table_metadata->namespace_name_,
                                                 table_metadata->table_name_,
                                                 partition_value,
                                                 column_name))) {
        LOG_WARN("failed to set basic info for column stat builder", K(ret), K(i));
      } else if (OB_FAIL(builder->set_stat_info(0, 0, 0, 0, ObTimeUtility::current_time(),
                                                common::ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI))) {
        LOG_WARN("failed to set stat info for column stat builder", K(ret), K(i));
      } else if (OB_FAIL(builder->set_bitmap_type(ObExternalBitmapType::HIVE_AUTO_DETECT))) {
        LOG_WARN("failed to set bitmap type for hive column stat builder", K(ret), K(i));
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_name))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(ret), K(i), K(column_name));
      } else if (column_schema->is_part_key_column()) {
        if (OB_FAIL(part_column_builders.push_back(builder))) {
          LOG_WARN("failed to push back part column stat builder");
        } else if (OB_FAIL(part_column_schemas.push_back(column_schema))) {
          LOG_WARN("failed to push back part column schema");
        }
      } else if (OB_FAIL(non_part_column_builders.push_back(builder))) {
        LOG_WARN("failed to push back non part column stat builder");
      } else if (OB_FAIL(non_part_column_schemas.push_back(column_schema))) {
        LOG_WARN("failed to push back non part column schema");
      }
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::collect_column_stats_from_builders(
    ObIArray<ObOptExternalColumnStatBuilder *> &column_builders,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_builders.count(); ++i) {
    ObOptExternalColumnStatBuilder *builder = column_builders.at(i);
    ObOptExternalColumnStat *external_column_stat = nullptr;
    if (OB_ISNULL(builder)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stat builder is null", K(ret), K(i));
    } else if (OB_FAIL(builder->finalize_bitmap())) {
      LOG_WARN("failed to finalize bitmap for column stat builder", K(ret),
               K(i));
    } else if (OB_FAIL(builder->build(allocator_, external_column_stat))) {
      LOG_WARN("failed to build external column stat", K(ret), K(i));
    } else if (OB_FAIL(external_column_stats.push_back(external_column_stat))) {
      LOG_WARN("failed to push back external column stat", K(ret), K(i));
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::convert_to_std_vector(
    const ObIArray<ObString> &ob_array, std::vector<std::string> &std_vector) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ob_array.count(); ++i) {
    std_vector.emplace_back(ob_array.at(i).ptr(), ob_array.at(i).length());
  }
  return ret;
}

int ObHiveCatalogStatHelper::fetch_partitions_statistics_batch(
    ObHiveMetastoreClient *client, const ObILakeTableMetadata *table_metadata,
    const std::vector<std::string> &partition_names,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats,
    int64_t batch_size) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;
  if (OB_ISNULL(client) || OB_ISNULL(table_metadata) || column_names.empty() ||
      partition_names.empty() || batch_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(client), K(table_metadata),
             K(batch_size));
  } else if (OB_UNLIKELY(table_metadata->get_format_type() !=
                         ObLakeTableFormat::HIVE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table metadata format type is not hive", K(ret),
             K(table_metadata->get_format_type()));
  } else {
    common::ObArenaAllocator arena_allocator(
        "HiveStatBatch", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString global_partition_value;
    const ObString &ns_name = table_metadata->namespace_name_;
    const ObString &table_name = table_metadata->table_name_;
    const ObNameCaseMode case_mode = table_metadata->case_mode_;
    // Merge basic statistics across all batches
    ObHiveBasicStats merged_basic_stats;
    // Process partitions in batches
    std::vector<std::string> std_column_names;
    std::vector<std::string> batch_partition_names;
    std::vector<ObHiveBasicStats> batch_basic_stats;
    std::vector<ApacheHive::Partition> partitions;
    // Prepare column builders for merging
    ObSEArray<ObOptExternalColumnStatBuilder *, 16> part_column_builders;
    ObSEArray<ObOptExternalColumnStatBuilder *, 16> non_part_column_builders;
    const ObTableSchema *table_schema = nullptr;
    ObSEArray<const ObColumnSchemaV2 *, 16> part_column_schemas;
    ObSEArray<const ObColumnSchemaV2 *, 16> non_part_column_schemas;
    if (OB_FAIL(convert_to_std_vector(column_names, std_column_names))) {
      LOG_WARN("failed to convert column names to std vector", K(ret));
    } else if (OB_FAIL(prepare_column_builders_and_schemas(arena_allocator,
                                                           global_partition_value,
                                                           table_metadata,
                                                           column_names,
                                                           part_column_builders,
                                                           non_part_column_builders,
                                                           table_schema,
                                                           part_column_schemas,
                                                           non_part_column_schemas))) {
      LOG_WARN("failed to prepare column builders and schemas", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table schema");
    }

    for (int64_t batch_start = 0;
         OB_SUCC(ret) && batch_start < partition_names.size();
         batch_start += batch_size) {
      int64_t batch_end =
          std::min(batch_start + batch_size,
                   static_cast<int64_t>(partition_names.size()));
      bool found = false;
      ApacheHive::PartitionsStatsResult batch_stats_result;
      batch_basic_stats.clear();
      batch_partition_names.clear();
      partitions.clear();

      // Create batch partition list
      for (int64_t i = batch_start; i < batch_end; ++i) {
        if (partition_names[i].find("__HIVE_DEFAULT_PARTITION__") == std::string::npos) {
          batch_partition_names.emplace_back(partition_names[i]);
        }
      }

      LOG_TRACE("Processing partition batch", K(batch_start), K(batch_end),
                "batch_size", batch_partition_names.size(), "total_partitions",
                partition_names.size());

      // Get basic statistics for this batch
      if (batch_partition_names.empty()) {
        // do nothing
      } else if (OB_FAIL(client->get_partition_basic_stats(ns_name, table_name, case_mode,
                                                           batch_partition_names, partitions,
                                                           batch_basic_stats))) {
        LOG_WARN("failed to get partition basic stats for batch", K(ret),
                 K(ns_name), K(table_name), K(batch_start), K(batch_end));
      } else if (OB_UNLIKELY(batch_partition_names.size() != batch_basic_stats.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected size", K(ret), K(batch_partition_names.size()),
                 K(batch_basic_stats.size()));
      } else if (OB_FAIL(merge_table_stats(batch_basic_stats, merged_basic_stats))) {
        LOG_WARN("failed to merge table stats", K(ret));
      } else if (OB_FAIL(merge_part_column_stat(*table_schema, partitions, batch_basic_stats,
                                                part_column_schemas, part_column_builders))) {
        LOG_WARN("failed to merge part column stat");
      } else if (OB_FAIL(client->get_partition_statistics(ns_name, table_name, case_mode,
                                                          std_column_names, batch_partition_names,
                                                          found, batch_stats_result))) {
        LOG_WARN("failed to get partition statistics for batch", K(ret),
                 K(ns_name), K(table_name), K(batch_start), K(batch_end));
      } else if (found && !batch_stats_result.partStats.empty()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_partition_names.size(); ++i) {
          const std::string &partition_name = batch_partition_names[i];
          const ObHiveBasicStats &partition_basic_stats = batch_basic_stats[i];
          std::map<std::string, std::vector<ApacheHive::ColumnStatisticsObj> >::const_iterator itr = batch_stats_result.partStats.find(partition_name);
          if (itr != batch_stats_result.partStats.end()) {
            const std::vector<ApacheHive::ColumnStatisticsObj>
                &partition_column_stats = itr->second;
            if (OB_FAIL(merge_non_part_column_stats(partition_basic_stats.num_rows_,
                                                    partition_column_stats,
                                                    non_part_column_schemas,
                                                    non_part_column_builders))) {
              LOG_WARN("failed to merge partition column stats", K(ret),
                       K(ns_name), K(table_name), K(batch_start), K(batch_end),
                       K(partition_name.c_str()));
            }
          }
        }
      }
    }

    // Create final table statistics and column statistics
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_external_table_stat(table_metadata, merged_basic_stats, ObString(""),
                                             partition_names.size(), external_table_stat))) {
        LOG_WARN("failed to create external table stat", K(ret));
      } else if (OB_FAIL(collect_column_stats_from_builders(part_column_builders,
                                                            external_column_stats))) {
        LOG_WARN("failed to collect column stats from builders", K(ret));
      } else if (OB_FAIL(collect_column_stats_from_builders(non_part_column_builders,
                                                            external_column_stats))) {
        LOG_WARN("failed to collect column stats from builders", K(ret));
      }
    }

    // Clean up builders
    for (int64_t i = 0; i < part_column_builders.count(); ++i) {
      if (OB_NOT_NULL(part_column_builders.at(i))) {
        part_column_builders.at(i)->~ObOptExternalColumnStatBuilder();
      }
    }
    for (int64_t i = 0; i < non_part_column_builders.count(); ++i) {
      if (OB_NOT_NULL(non_part_column_builders.at(i))) {
        non_part_column_builders.at(i)->~ObOptExternalColumnStatBuilder();
      }
    }


    LOG_TRACE("Completed batch processing of partition statistics",
              "total_partitions", partition_names.size(), "batch_size",
              batch_size, "merged_rows", merged_basic_stats.num_rows_,
              "merged_files", merged_basic_stats.num_files_, "merged_size",
              merged_basic_stats.total_size_);
  }

  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_table_stats_to_external_stats(
    const ObILakeTableMetadata *table_metadata,
    const ObHiveBasicStats &basic_stats,
    const ApacheHive::TableStatsResult &table_stats_result,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;
  external_column_stats.reset();

  // Create table statistics
  if (OB_FAIL(create_external_table_stat(
          table_metadata, basic_stats, ObString(""), 1, external_table_stat))) {
    LOG_WARN("failed to create external table stat", K(ret));
  }

  // Create column statistics
  if (OB_SUCC(ret) && !table_stats_result.tableStats.empty()) {
    if (OB_FAIL(create_external_column_stats(
            table_metadata, table_stats_result.tableStats, column_names,
            ObString(""), basic_stats.num_rows_, external_column_stats))) {
      LOG_WARN("failed to create external column stats", K(ret));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::create_external_table_stat(
    const ObILakeTableMetadata *table_metadata,
    const ObHiveBasicStats &basic_stats, const ObString &partition_value,
    const int64_t partition_num, ObOptExternalTableStat *&external_table_stat) {
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;

  ObOptExternalTableStatBuilder table_stat_builder;
  if (OB_FAIL(table_stat_builder.set_basic_info(
          table_metadata->tenant_id_, table_metadata->catalog_id_,
          table_metadata->namespace_name_, table_metadata->table_name_,
          partition_value))) {
    LOG_WARN("failed to set basic info for table stat builder", K(ret));
  } else if (OB_FAIL(table_stat_builder.set_stat_info(
                 basic_stats.num_rows_, basic_stats.num_files_,
                 basic_stats.total_size_, ObTimeUtility::current_time()))) {
    LOG_WARN("failed to set stat info for table stat builder", K(ret));
  } else if (OB_FALSE_IT(table_stat_builder.add_partition_num(partition_num))) {
  } else if (OB_FAIL(
                 table_stat_builder.build(allocator_, external_table_stat))) {
    LOG_WARN("failed to build external table stat", K(ret));
  }

  return ret;
}

int ObHiveCatalogStatHelper::create_external_column_stats(
    const ObILakeTableMetadata *table_metadata,
    const std::vector<ApacheHive::ColumnStatisticsObj> &hive_column_stats,
    const ObIArray<ObString> &column_names, const ObString &partition_value,
    int64_t total_rows,
    ObIArray<ObOptExternalColumnStat *> &external_column_stats)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatBatch", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSEArray<ObOptExternalColumnStatBuilder *, 16> part_column_stat_builders;
  ObSEArray<ObOptExternalColumnStatBuilder *, 16> non_part_column_stat_builders;
  const ObTableSchema *table_schema = nullptr;
  ObSEArray<const ObColumnSchemaV2 *, 16> part_column_schemas;
  ObSEArray<const ObColumnSchemaV2 *, 16> non_part_column_schemas;

  if (OB_FAIL(prepare_column_builders_and_schemas(arena_allocator, partition_value, table_metadata, column_names,
                                                  part_column_stat_builders, non_part_column_stat_builders,
                                                  table_schema, part_column_schemas, non_part_column_schemas))) {
    LOG_WARN("failed to prepare column builders and schemas", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema");
  } else if (OB_UNLIKELY(!part_column_stat_builders.empty() || !part_column_schemas.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part column info should be empty", K(part_column_schemas));
  } else if (OB_FAIL(merge_non_part_column_stats(total_rows, hive_column_stats,
                                                 non_part_column_schemas,
                                                 non_part_column_stat_builders))) {
    LOG_WARN("failed to merge hive column stats", K(ret));
  } else if (OB_FAIL(collect_column_stats_from_builders(non_part_column_stat_builders, external_column_stats))) {
    LOG_WARN("failed to collect column stats from builders", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < non_part_column_stat_builders.count(); ++i) {
    if (OB_NOT_NULL(non_part_column_stat_builders.at(i))) {
      non_part_column_stat_builders.at(i)->~ObOptExternalColumnStatBuilder();
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::merge_partition_basic_stats(
    const std::vector<ObHiveBasicStats> &partition_basic_stats,
    ObHiveBasicStats &merged_stats) {
  int ret = OB_SUCCESS;

  merged_stats.reset();

  for (const ObHiveBasicStats &partition_stat : partition_basic_stats) {
    merged_stats.num_files_ += partition_stat.num_files_;
    merged_stats.num_rows_ += partition_stat.num_rows_;
    merged_stats.total_size_ += partition_stat.total_size_;
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::BooleanColumnStatsData &boolean_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;

  // Calculate basic statistics
  int64_t num_nulls = boolean_column_stat.numNulls;
  int64_t num_not_nulls =
      boolean_column_stat.numTrues + boolean_column_stat.numFalses;
  int64_t num_distinct = 0;
  int64_t avg_length = 1; // Boolean value size

  // For boolean type, distinct values can be at most 2 (true, false)
  if (boolean_column_stat.numTrues > 0 && boolean_column_stat.numFalses > 0) {
    num_distinct = 2;
  } else if (boolean_column_stat.numTrues > 0 ||
             boolean_column_stat.numFalses > 0) {
    num_distinct = 1;
  }

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Note: For boolean type, we don't merge bitmap as per requirement

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::LongColumnStatsData &long_column_stat, int64_t total_rows,
    const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatObj", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  // Calculate basic statistics
  int64_t num_nulls = long_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = long_column_stat.numDVs;
  int64_t avg_length = 8; // Long value size

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Merge min/max values if available
  if (OB_SUCC(ret) && long_column_stat.__isset.lowValue) {
    common::ObObj min_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema,
            const_cast<int64_t &>(long_column_stat.lowValue), min_obj))) {
      LOG_WARN("failed to convert hive low value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_min_value(min_obj))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_obj));
    }
  }

  if (OB_SUCC(ret) && long_column_stat.__isset.highValue) {
    common::ObObj max_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema,
            const_cast<int64_t &>(long_column_stat.highValue), max_obj))) {
      LOG_WARN("failed to convert hive high value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_max_value(max_obj))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_obj));
    }
  }

  // Merge bitmap if available
  if (OB_SUCC(ret) && long_column_stat.__isset.bitVectors &&
      !long_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(long_column_stat.bitVectors.c_str(),
                                     long_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(long_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::DoubleColumnStatsData &double_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatObj", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  // Calculate basic statistics
  int64_t num_nulls = double_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = double_column_stat.numDVs;
  int64_t avg_length = 8; // Double value size

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Merge min/max values if available
  if (OB_SUCC(ret) && double_column_stat.__isset.lowValue) {
    common::ObObj min_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema,
            const_cast<double &>(double_column_stat.lowValue), min_obj))) {
      LOG_WARN("failed to convert hive low value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_min_value(min_obj))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_obj));
    }
  }

  if (OB_SUCC(ret) && double_column_stat.__isset.highValue) {
    common::ObObj max_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema,
            const_cast<double &>(double_column_stat.highValue), max_obj))) {
      LOG_WARN("failed to convert hive high value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_max_value(max_obj))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_obj));
    }
  }

  // Merge bitmap if available
  if (OB_SUCC(ret) && double_column_stat.__isset.bitVectors &&
      !double_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(double_column_stat.bitVectors.c_str(),
                                     double_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(double_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::StringColumnStatsData &string_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;

  // Calculate basic statistics
  int64_t num_nulls = string_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = string_column_stat.numDVs;
  int64_t avg_length = static_cast<int64_t>(string_column_stat.avgColLen);

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Note: For string type, we don't merge min/max as per requirement

  // Merge bitmap if available
  if (OB_SUCC(ret) && string_column_stat.__isset.bitVectors &&
      !string_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(string_column_stat.bitVectors.c_str(),
                                     string_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(string_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::BinaryColumnStatsData &binary_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;

  // Calculate basic statistics
  int64_t num_nulls = binary_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct =
      0; // Binary type usually doesn't provide distinct count
  int64_t avg_length = static_cast<int64_t>(binary_column_stat.avgColLen);

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Note: For binary type, we don't merge min/max as per requirement

  // Merge bitmap if available
  if (OB_SUCC(ret) && binary_column_stat.__isset.bitVectors &&
      !binary_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(binary_column_stat.bitVectors.c_str(),
                                     binary_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(binary_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::DecimalColumnStatsData &decimal_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatObj", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  // Calculate basic statistics
  int64_t num_nulls = decimal_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = decimal_column_stat.numDVs;
  int64_t avg_length = 16; // Decimal typical size

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Merge min/max values if available
  if (OB_SUCC(ret) && decimal_column_stat.__isset.lowValue) {
    common::ObObj min_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(arena_allocator, column_schema,
                                            decimal_column_stat.lowValue,
                                            min_obj))) {
      LOG_WARN("failed to convert hive low value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_min_value(min_obj))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_obj));
    }
  }

  if (OB_SUCC(ret) && decimal_column_stat.__isset.highValue) {
    common::ObObj max_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(arena_allocator, column_schema,
                                            decimal_column_stat.highValue,
                                            max_obj))) {
      LOG_WARN("failed to convert hive high value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_max_value(max_obj))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_obj));
    }
  }

  // Merge bitmap if available
  if (OB_SUCC(ret) && decimal_column_stat.__isset.bitVectors &&
      !decimal_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(decimal_column_stat.bitVectors.c_str(),
                                     decimal_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(decimal_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::DateColumnStatsData &date_column_stat, int64_t total_rows,
    const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatObj", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  // Calculate basic statistics
  int64_t num_nulls = date_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = date_column_stat.numDVs;
  int64_t avg_length = 8; // Date value size

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Merge min/max values if available
  if (OB_SUCC(ret) && date_column_stat.__isset.lowValue) {
    common::ObObj min_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema, date_column_stat.lowValue, min_obj))) {
      LOG_WARN("failed to convert hive low value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_min_value(min_obj))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_obj));
    }
  }

  if (OB_SUCC(ret) && date_column_stat.__isset.highValue) {
    common::ObObj max_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(
            arena_allocator, column_schema, date_column_stat.highValue, max_obj))) {
      LOG_WARN("failed to convert hive high value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_max_value(max_obj))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_obj));
    }
  }

  // Merge bitmap if available
  if (OB_SUCC(ret) && date_column_stat.__isset.bitVectors &&
      !date_column_stat.bitVectors.empty()) {
    if (OB_FAIL(builder.merge_bitmap(date_column_stat.bitVectors.c_str(),
                                     date_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(date_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_hive_column_data_to_builder(
    const ApacheHive::TimestampColumnStatsData &timestamp_column_stat,
    int64_t total_rows, const ObColumnSchemaV2 &column_schema,
    ObOptExternalColumnStatBuilder &builder) {
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("HiveStatObj", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  // Calculate basic statistics
  int64_t num_nulls = timestamp_column_stat.numNulls;
  int64_t num_not_nulls = total_rows - num_nulls;
  int64_t num_distinct = timestamp_column_stat.numDVs;
  int64_t avg_length = 8; // Timestamp value size

  // Merge statistical values
  if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct,
                                        avg_length))) {
    LOG_WARN("failed to merge stat values", K(ret), K(num_nulls),
             K(num_not_nulls), K(num_distinct), K(avg_length));
  }

  // Merge min/max values if available
  if (OB_SUCC(ret) && timestamp_column_stat.__isset.lowValue) {
    common::ObObj min_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(arena_allocator, column_schema,
                                            timestamp_column_stat.lowValue,
                                            min_obj))) {
      LOG_WARN("failed to convert hive low value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_min_value(min_obj))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_obj));
    }
  }

  if (OB_SUCC(ret) && timestamp_column_stat.__isset.highValue) {
    common::ObObj max_obj;
    if (OB_FAIL(convert_hive_value_to_obobj(arena_allocator, column_schema,
                                            timestamp_column_stat.highValue,
                                            max_obj))) {
      LOG_WARN("failed to convert hive high value to obobj", K(ret));
    } else if (OB_FAIL(builder.merge_max_value(max_obj))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_obj));
    }
  }

  // Merge bitmap if available
  if (OB_SUCC(ret) && timestamp_column_stat.__isset.bitVectors &&
      !timestamp_column_stat.bitVectors.empty()) {
    if (OB_FAIL(
            builder.merge_bitmap(timestamp_column_stat.bitVectors.c_str(),
                                 timestamp_column_stat.bitVectors.size()))) {
      LOG_WARN("failed to merge bitmap", K(ret),
               K(timestamp_column_stat.bitVectors.size()));
    }
  }

  return ret;
}

int ObHiveCatalogStatHelper::merge_part_column_data_to_builder(const ObObj &part_val,
                                                               int64_t total_rows,
                                                               const ObColumnSchemaV2 &column_schema,
                                                               ObOptExternalColumnStatBuilder &builder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_val.is_null())) {
    int64_t num_nulls = total_rows;
    int64_t num_not_nulls = 0;
    int64_t num_distinct = 1;
    int64_t avg_length = 1;
    if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct, avg_length))) {
      LOG_WARN("failed to merge stat values");
    }
  } else {
    int64_t num_nulls = 0;
    int64_t num_not_nulls = total_rows;
    int64_t num_distinct = 1;
    int64_t avg_length = part_val.get_data_length();
    uint64_t hash_value = 0;
    if (OB_FAIL(builder.merge_stat_values(num_nulls, num_not_nulls, num_distinct, avg_length))) {
      LOG_WARN("failed to merge stat values");
    } else if (OB_FAIL(builder.merge_min_value(part_val))) {
      LOG_WARN("failed to merge min value", K(part_val));
    } else if (OB_FAIL(builder.merge_max_value(part_val))) {
      LOG_WARN("failed to merge max value", K(part_val));
    } else if (OB_FAIL(part_val.hash_murmur(hash_value, hash_value))) {
      LOG_WARN("failed to calc hash partition value", K(part_val));
    } else if (OB_FAIL(builder.add_hhl_value(hash_value))) {
      LOG_WARN("failed to add hhl value");
    }
  }
  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_value_to_obobj(
    ObIAllocator &allocator, const ObColumnSchemaV2 &column_schema,
    int64_t &stats_data, common::ObObj &obj_value) {
  int ret = OB_SUCCESS;

  const common::ObObjMeta &meta_type = column_schema.get_meta_type();
  const common::ObObjType obj_type = meta_type.get_type();

  // Convert int64_t value to appropriate ObObj type based on column schema
  switch (obj_type) {
  case common::ObTinyIntType:
    if (stats_data >= INT8_MIN && stats_data <= INT8_MAX) {
      obj_value.set_tinyint(static_cast<int8_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for tinyint", K(ret), K(stats_data));
    }
    break;

  case common::ObSmallIntType:
    if (stats_data >= INT16_MIN && stats_data <= INT16_MAX) {
      obj_value.set_smallint(static_cast<int16_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for smallint", K(ret), K(stats_data));
    }
    break;

  case common::ObMediumIntType:
    if (stats_data >= INT24_MIN && stats_data <= INT24_MAX) {
      obj_value.set_mediumint(static_cast<int32_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for mediumint", K(ret), K(stats_data));
    }
    break;

  case common::ObInt32Type:
    if (stats_data >= INT32_MIN && stats_data <= INT32_MAX) {
      obj_value.set_int32(static_cast<int32_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for int32", K(ret), K(stats_data));
    }
    break;

  case common::ObIntType:
    obj_value.set_int(stats_data);
    break;

  case common::ObUTinyIntType:
    if (stats_data >= 0 && stats_data <= UINT8_MAX) {
      obj_value.set_utinyint(static_cast<uint8_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for utinyint", K(ret), K(stats_data));
    }
    break;

  case common::ObUSmallIntType:
    if (stats_data >= 0 && stats_data <= UINT16_MAX) {
      obj_value.set_usmallint(static_cast<uint16_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for usmallint", K(ret), K(stats_data));
    }
    break;

  case common::ObUMediumIntType:
    if (stats_data >= 0 && stats_data <= UINT24_MAX) {
      obj_value.set_umediumint(static_cast<uint32_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for umediumint", K(ret),
               K(stats_data));
    }
    break;

  case common::ObUInt32Type:
    if (stats_data >= 0 && stats_data <= UINT32_MAX) {
      obj_value.set_uint32(static_cast<uint32_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for uint32", K(ret), K(stats_data));
    }
    break;

  case common::ObUInt64Type:
    if (stats_data >= 0) {
      obj_value.set_uint64(static_cast<uint64_t>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for uint64", K(ret), K(stats_data));
    }
    break;

  case common::ObYearType:
    if (stats_data >= 1901 && stats_data <= 2155) {
      obj_value.set_year(static_cast<uint8_t>(stats_data - 1900));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 value out of range for year", K(ret), K(stats_data));
    }
    break;

  case common::ObNumberType: {
    // Convert int64 to number
    number::ObNumber num;
    if (OB_FAIL(num.from(stats_data, allocator))) {
      LOG_WARN("failed to convert int64 to number", K(ret), K(stats_data));
    } else {
      obj_value.set_number(num);
    }
    break;
  }
  case common::ObTimestampType: {
    obj_value.set_timestamp(stats_data * USECS_PER_SEC);
    break;
  }
  case common::ObDecimalIntType: {
    ObDecimalIntBuilder builder;
    builder.from(stats_data);
    int32_t int_bytes = builder.get_int_bytes();
    ObDecimalInt *decint = nullptr;
    if (OB_ISNULL(
            decint = static_cast<ObDecimalInt *>(allocator.alloc(int_bytes)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for decimal int", K(ret),
               K(int_bytes));
    } else {
      MEMCPY(decint, builder.get_decimal_int(), int_bytes);
      obj_value.set_decimal_int(int_bytes, meta_type.get_scale(), decint);
    }
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported conversion from int64 to obj type", K(ret),
             K(obj_type), K(stats_data));
    break;
  }
  if (OB_SUCC(ret)) {
    obj_value.set_collation_type(meta_type.get_collation_type());
  }

  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_value_to_obobj(
    ObIAllocator &allocator, const ObColumnSchemaV2 &column_schema,
    double &stats_data, common::ObObj &obj_value) {
  int ret = OB_SUCCESS;

  const common::ObObjMeta &meta_type = column_schema.get_meta_type();
  const common::ObObjType obj_type = meta_type.get_type();

  // Convert double value to appropriate ObObj type based on column schema
  switch (obj_type) {
  case common::ObFloatType:
    obj_value.set_float(static_cast<float>(stats_data));
    break;

  case common::ObDoubleType:
    obj_value.set_double(stats_data);
    break;

  case common::ObUFloatType:
    if (stats_data >= 0) {
      obj_value.set_ufloat(static_cast<float>(stats_data));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("double value out of range for ufloat", K(ret), K(stats_data));
    }
    break;

  case common::ObUDoubleType:
    if (stats_data >= 0) {
      obj_value.set_udouble(stats_data);
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("double value out of range for udouble", K(ret), K(stats_data));
    }
    break;

  case common::ObNumberType: {
    // Convert double to number
    number::ObNumber num;
    const char *double_str = nullptr;
    char double_buf[64];
    int str_len = snprintf(double_buf, sizeof(double_buf), "%.15g", stats_data);
    double_str = double_buf;
    if (OB_FAIL(num.from(double_str, str_len, allocator))) {
      LOG_WARN("failed to convert double to number", K(ret), K(stats_data));
    } else {
      obj_value.set_number(num);
    }
    break;
  }

  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported conversion from double to obj type", K(ret),
             K(obj_type), K(stats_data));
    break;
  }

  if (OB_SUCC(ret)) {
    obj_value.set_collation_type(meta_type.get_collation_type());
  }

  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_value_to_obobj(
    ObIAllocator &allocator, const ObColumnSchemaV2 &column_schema,
    const ApacheHive::Decimal &stats_data, common::ObObj &obj_value) {
  int ret = OB_SUCCESS;
  // Use stack memory since set_decimal_int will copy the data
  char buf[64]; // 64 bytes should be enough for any decimal conversion
  const common::ObObjMeta &meta_type = column_schema.get_meta_type();
  const common::ObObjType obj_type = meta_type.get_type();
  const common::ObAccuracy &accuracy = column_schema.get_accuracy();
  // Convert Hive Decimal to ObObj using the same logic as parquet DataLoader
  if (obj_type == common::ObNumberType || obj_type == common::ObUNumberType ||
      ob_is_decimal_int_tc(obj_type) || obj_type == common::ObTimestampType) {
    const std::string &unscaled_data = stats_data.unscaled;
    if (unscaled_data.empty()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("hive decimal unscaled data is empty", K(ret));
    } else {
      // Calculate the required buffer size for decimal conversion
      int32_t data_len = static_cast<int32_t>(unscaled_data.length());
      if (ob_is_decimal_int_tc(obj_type)) {
        // For decimal_int types, calculate proper size based on precision
        int16_t precision = accuracy.get_precision();
        if (precision <= 9) {
          data_len = 4; // 32-bit
        } else if (precision <= 18) {
          data_len = 8; // 64-bit
        } else if (precision <= 38) {
          data_len = 16; // 128-bit
        } else {
          data_len = 32; // 256-bit or larger
        }
      } else {
        // For number type, use the original data length but ensure proper
        // alignment
        data_len = std::max(
            4, static_cast<int32_t>((unscaled_data.length() + 7) / 8 * 8));
      }

      if (data_len > sizeof(buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("decimal data too large for buffer", K(ret), K(data_len),
                 K(sizeof(buf)));
      } else {
        const char *str = unscaled_data.c_str();
        int32_t length = static_cast<int32_t>(unscaled_data.length());

        // Convert from big-endian (Hive format) to little-endian (OceanBase
        // format) This logic is based on
        // ObParquetTableRowIterator::to_numeric_hive
        MEMSET(buf, (*str >> 8), data_len); // Fill with sign extension

        if (data_len <= 4) {
          // For precision <= 9, use 32-bit
          MEMCPY(buf + 4 - length, str, length);
          uint32_t *res = reinterpret_cast<uint32_t *>(buf);
          uint32_t temp_v = *res;
          *res = ntohl(
              temp_v); // Convert from network byte order to host byte order
        } else {
          // For larger precisions, convert 8 bytes at a time
          int64_t pos = 0;
          int64_t temp_len = length;
          while (temp_len >= 8) {
            uint64_t temp_v =
                *(reinterpret_cast<const uint64_t *>(str + temp_len - 8));
            *(reinterpret_cast<uint64_t *>(buf + pos)) = ntohll(temp_v);
            pos += 8;
            temp_len -= 8;
          }
          if (temp_len > 0) {
            MEMCPY(buf + pos + 8 - temp_len, str, temp_len);
            uint64_t temp_v = *(reinterpret_cast<uint64_t *>(buf + pos));
            *(reinterpret_cast<uint64_t *>(buf + pos)) = ntohll(temp_v);
          }
        }

        // Now convert to the appropriate OceanBase type
        ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
        int32_t val_len = data_len;

        if (ob_is_decimal_int_tc(obj_type)) {
          // Set as decimal int directly, need to allocate memory for the data
          ObDecimalInt *allocated_decint = nullptr;
          if (OB_ISNULL(allocated_decint = static_cast<ObDecimalInt *>(allocator.alloc(val_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for decimal int", K(ret), K(val_len));
          } else {
            MEMCPY(allocated_decint, decint, val_len);
            obj_value.set_decimal_int(val_len, stats_data.scale, allocated_decint);
          }
        } else if (obj_type == common::ObNumberType ||
                   obj_type == common::ObUNumberType) {
          // Convert to number format
          number::ObNumber res_nmb;
          int16_t scale = stats_data.scale;
          if (OB_FAIL(wide::to_number(decint, val_len, scale, allocator,
                                      res_nmb))) {
            LOG_WARN("failed to convert decimal_int to number", K(ret),
                     K(scale));
          } else {
            obj_value.set_number(res_nmb);
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported conversion from hive decimal to obj type", K(ret),
             K(obj_type));
  }

  if (OB_SUCC(ret)) {
    obj_value.set_collation_type(meta_type.get_collation_type());
  }

  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_value_to_obobj(
    ObIAllocator &allocator, const ObColumnSchemaV2 &column_schema,
    const ApacheHive::Date &stats_data, common::ObObj &obj_value) {
  int ret = OB_SUCCESS;

  const common::ObObjMeta &meta_type = column_schema.get_meta_type();
  const common::ObObjType obj_type = meta_type.get_type();

  // Convert Hive Date to ObObj
  int64_t days_since_epoch = stats_data.daysSinceEpoch;

  switch (obj_type) {
  case common::ObDateType: {
    // Convert days since Unix epoch to OceanBase date format
    // OceanBase date is stored as days since 0000-01-01
    int32_t ob_date_val =
        static_cast<int32_t>(days_since_epoch + DAYS_FROM_ZERO_TO_BASE);
    obj_value.set_date(ob_date_val);
    break;
  }

  case common::ObDateTimeType: {
    // Convert to datetime with time part as 00:00:00
    int64_t datetime_val = days_since_epoch * USECS_PER_DAY + DATETIME_MIN_VAL;
    obj_value.set_datetime(datetime_val);
    break;
  }

  case common::ObTimestampType: {
    // Convert to timestamp
    int64_t timestamp_val = days_since_epoch * USECS_PER_SEC;
    obj_value.set_timestamp(timestamp_val);
    break;
  }

  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported conversion from hive date to obj type", K(ret),
             K(obj_type), K(days_since_epoch));
    break;
  }

  if (OB_SUCC(ret)) {
    obj_value.set_collation_type(meta_type.get_collation_type());
  }

  return ret;
}

int ObHiveCatalogStatHelper::convert_hive_value_to_obobj(
    ObIAllocator &allocator, const ObColumnSchemaV2 &column_schema,
    const ApacheHive::Timestamp &stats_data, common::ObObj &obj_value) {
  int ret = OB_SUCCESS;

  const common::ObObjMeta &meta_type = column_schema.get_meta_type();
  const common::ObObjType obj_type = meta_type.get_type();

  // Convert Hive Timestamp to ObObj
  int64_t seconds_since_epoch = stats_data.secondsSinceEpoch;

  switch (obj_type) {
  case common::ObTimestampType: {
    // Convert seconds to microseconds for OceanBase timestamp
    int64_t timestamp_val = seconds_since_epoch * USECS_PER_SEC;
    obj_value.set_timestamp(timestamp_val);
    break;
  }

  case common::ObDateTimeType: {
    // Convert to datetime
    int64_t datetime_val =
        seconds_since_epoch * USECS_PER_SEC + DATETIME_MIN_VAL;
    obj_value.set_datetime(datetime_val);
    break;
  }

  case common::ObTimestampLTZType: {
    // Convert to timestamp with local timezone
    int64_t timestamp_val = seconds_since_epoch * USECS_PER_SEC;
    obj_value.set_timestamp(timestamp_val);
    obj_value.set_type(common::ObTimestampLTZType);
    break;
  }

  case common::ObTimestampNanoType: {
    // Convert to nanosecond timestamp
    int64_t timestamp_val = seconds_since_epoch * NSECS_PER_SEC;
    obj_value.set_timestamp(timestamp_val);
    obj_value.set_type(common::ObTimestampNanoType);
    break;
  }

  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported conversion from hive timestamp to obj type", K(ret),
             K(obj_type), K(seconds_since_epoch));
    break;
  }

  if (OB_SUCC(ret)) {
    obj_value.set_collation_type(meta_type.get_collation_type());
  }

  return ret;
}

} // namespace share
} // namespace oceanbase