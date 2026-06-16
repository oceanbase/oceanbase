/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_opt_catalog_stat_service.h"
#include "sql/ob_sql_context.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat_builder.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat_builder.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/ob_stat_item.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "lib/time/ob_time_utility.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{
namespace common
{

using namespace share::schema;

ObOptCatalogStatService::ObOptCatalogStatService() : inited_(false)
{
}

ObOptCatalogStatService::~ObOptCatalogStatService()
{
}

int ObOptCatalogStatService::init(ObMySQLProxy *proxy, ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy) || OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(proxy), KP(config));
  } else if (OB_FAIL(cat_sql_service_.init(proxy))) {
    LOG_WARN("failed to init ext sql service", K(ret));
  } else if (OB_FAIL(catalog_table_stat_cache_.init("CatalogTabStatCache",
                                                    DEFAULT_CATALOG_TAB_STAT_CACHE_PRIORITY))) {
    LOG_WARN("failed to init catalog table stat cache", K(ret));
  } else if (OB_FAIL(catalog_column_stat_cache_.init("CatalogColStatCache",
                                                     DEFAULT_CATALOG_COL_STAT_CACHE_PRIORITY))) {
    LOG_WARN("failed to init external column stat cache", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObOptCatalogStatService::update_catalog_opt_stat_gather_stat(
    const ObOptStatGatherStat &gather_stat,
    const uint64_t &catalog_id,
    const ObString &db_name,
    const ObString &table_name)
{
  return cat_sql_service_.update_catalog_opt_stat_gather_stat(gather_stat,
                                                              catalog_id,
                                                              db_name,
                                                              table_name);
}

int ObOptCatalogStatService::erase_catalog_table_stat(const share::ObOptCatalogTableStat::Key &key)
{
  return catalog_table_stat_cache_.erase(key);
}

int ObOptCatalogStatService::erase_catalog_column_stat(
    const share::ObOptCatalogColumnStat::Key &key)
{
  return catalog_column_stat_cache_.erase(key);
}

int ObOptCatalogStatService::extract_column_names_from_table_schema(
    const share::schema::ObTableSchema &table_schema,
    ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = table_schema.get_column_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const share::schema::ObColumnSchemaV2 *column_schema = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret), K(i));
    } else if (column_schema->is_hidden() || column_schema->is_unused()) {
      // Skip hidden and unused columns
    } else if (OB_FAIL(column_names.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("failed to add column name", K(ret), K(column_schema->get_column_name_str()));
    }
  }
  LOG_TRACE("extracted column names from table schema", K(column_count), K(column_names.count()));
  return ret;
}

int ObOptCatalogStatService::get_catalog_table_stat_from_cache(const share::ObOptCatalogTableStat::Key &key,
                                                    ObLakeTableStat &stat,
                                                    bool &found_stat)
{
  int ret = OB_SUCCESS;
  found_stat = false;
  stat.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized", K(ret));
  } else {
    ObOptCatalogTableStatHandle handle;
    if (OB_FAIL(catalog_table_stat_cache_.get_value(key, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get catalog table stat from cache failed", K(ret), K(key));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(handle.stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
    } else {
      found_stat = true;
      stat.total_row_count_ = handle.stat_->get_row_count();
      stat.pruned_row_count_ = handle.stat_->get_row_count();
      stat.file_cnt_ = handle.stat_->get_file_num();
      stat.data_size_ = handle.stat_->get_data_size();
      stat.part_cnt_ = handle.stat_->get_partition_num();
      stat.last_analyzed_ = handle.stat_->get_last_analyzed();
    }
  }
  return ret;
}

int ObOptCatalogStatService::batch_get_catalog_table_stats(const uint64_t tenant_id,
                                                           const uint64_t table_id,
                                                           ObIArray<const share::ObOptCatalogTableStat::Key*> &keys,
                                                           const ObIArray<ObString> &all_partition_values,
                                                           sql::ObSqlSchemaGuard &schema_guard,
                                                           ObLakeTableStat &stat)
{
  int ret = OB_SUCCESS;
  ObSEArray<const share::ObOptCatalogTableStat::Key*, 4> regather_keys;
  ObSEArray<int64_t, 4> regather_handles_indices;
  ObSEArray<share::ObOptCatalogTableStatHandle, 4> handles;
  stat.reset();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(keys.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys array is empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_ISNULL(keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null key", K(ret), K(i));
      } else if (OB_UNLIKELY(!keys.at(i)->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid key at index", K(ret), K(i), KPC(keys.at(i)));
      } else {
        share::ObOptCatalogTableStatHandle handle;
        if (OB_FAIL(catalog_table_stat_cache_.get_value(*keys.at(i), handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("get catalog table stat from cache failed", K(ret), KPC(keys.at(i)));
          } else if (OB_FAIL(regather_keys.push_back(keys.at(i)))) {
            LOG_WARN("failed to push back regather key", K(ret), K(i));
          } else if (OB_FAIL(handles.push_back(handle))) {
            LOG_WARN("failed to push back handle", K(ret), K(i));
          } else if (OB_FAIL(regather_handles_indices.push_back(i))) {
            LOG_WARN("failed to push back regather handle index", K(ret), K(i));
          }
        } else if (OB_ISNULL(handle.stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), KPC(keys.at(i)));
        } else if (OB_FAIL(handles.push_back(handle))) {
          LOG_WARN("failed to push back handle", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret) && !regather_keys.empty()) {
      if (OB_FAIL(batch_load_catalog_table_stats_and_put_cache(tenant_id,
                                                               table_id,
                                                               regather_keys,
                                                               all_partition_values,
                                                               schema_guard))) {
        LOG_WARN("failed to batch load catalog table stats and put cache", K(ret), K(regather_keys));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < regather_keys.count(); ++i) {
          const int64_t handle_index = regather_handles_indices.at(i);
          if (OB_FAIL(catalog_table_stat_cache_.get_value(*regather_keys.at(i), handles.at(handle_index)))) {
            LOG_WARN("failed to get catalog table stat from cache after batch load",
                     K(ret),
                     K(i),
                     K(handle_index),
                     KPC(regather_keys.at(i)));
          } else if (OB_ISNULL(handles.at(handle_index).stat_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cache hit but value is NULL after batch load. BUG here.",
                     K(ret),
                     K(i),
                     K(handle_index),
                     KPC(regather_keys.at(i)));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(aggregate_table_stat_from_partitions(handles, stat))) {
        LOG_WARN("failed to aggregate catalog table stats from handles", K(ret), K(handles.count()));
      }
    }
    LOG_TRACE("lekou stat", K(ret), K(regather_keys.count()), K(handles.count()));
  }
  return ret;
}

int ObOptCatalogStatService::get_catalog_column_stat_from_cache(ObIAllocator &alloc,
                                                                const ObIArray<share::ObOptCatalogColumnStat::Key> &keys,
                                                                ObIArray<ObLakeColumnStat*> &column_stats,
                                                                ObIArray<ObString> &missed_columns)
{
  int ret = OB_SUCCESS;
  column_stats.reset();
  missed_columns.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (OB_UNLIKELY(!keys.at(i).is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid key at index", K(ret), K(i), K(keys.at(i)));
    } else {
      share::ObOptCatalogColumnStatHandle handle;
      if (OB_FAIL(catalog_column_stat_cache_.get_row(keys.at(i), handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get external column stat from cache failed", K(ret), K(keys.at(i)));
        } else if (OB_FAIL(missed_columns.push_back(keys.at(i).column_name_))) {
          LOG_WARN("failed to push back missed column", K(ret), K(keys.at(i).column_name_));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(handle.stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(keys.at(i)));
      } else {
        void *ptr = alloc.alloc(sizeof(ObLakeColumnStat));
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          ObLakeColumnStat *column_stat = new (ptr) ObLakeColumnStat();
          if (OB_FAIL(ob_write_obj(alloc, handle.stat_->get_min_value(), column_stat->min_val_))) {
            LOG_WARN("failed to deep copy min value", K(ret), KPC(handle.stat_));
          } else if (OB_FAIL(ob_write_obj(alloc, handle.stat_->get_max_value(), column_stat->max_val_))) {
            LOG_WARN("failed to deep copy max value", K(ret), KPC(handle.stat_));
          } else {
            column_stat->null_count_ = handle.stat_->get_num_null();
            column_stat->size_ = handle.stat_->get_avg_length();
            column_stat->record_count_ = handle.stat_->get_num_rows();
            column_stat->last_analyzed_ = handle.stat_->get_last_analyzed();
            column_stat->num_distinct_ = handle.stat_->get_num_distinct();
          }
          if (OB_SUCC(ret) && OB_FAIL(column_stats.push_back(column_stat))) {
            LOG_WARN("failed to push back column stat", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptCatalogStatService::batch_get_catalog_column_stats(const uint64_t tenant_id,
                                                            const uint64_t table_id,
                                                            ObIArray<const share::ObOptCatalogColumnStat::Key*> &keys,
                                                            const ObIArray<ObString> &column_names,
                                                            const ObIArray<ObString> &all_partition_values,
                                                            sql::ObSqlSchemaGuard &schema_guard,
                                                            ObIAllocator &allocator,
                                                            const int64_t row_cnt,
                                                            const double scale_ratio,
                                                            ObIArray<ObLakeColumnStat*> &column_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<const share::ObOptCatalogColumnStat::Key*, 4> regather_keys;
  ObSEArray<int64_t, 4> regather_handles_indices;
  ObSEArray<share::ObOptCatalogColumnStatHandle, 4> handles;
  column_stats.reset();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized", K(ret), K(keys));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(keys.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys array is empty", K(ret));
  } else {
    LOG_TRACE("begin get external column stats", K(keys));
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_ISNULL(keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null key at index", K(ret), K(i));
      } else if (OB_UNLIKELY(!keys.at(i)->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid key at index", K(ret), K(i), K(keys.at(i)));
      } else {
        share::ObOptCatalogColumnStatHandle handle;
        if (OB_FAIL(catalog_column_stat_cache_.get_row(*keys.at(i), handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("get external column stat from cache failed", K(ret), K(keys.at(i)));
          } else if (OB_FAIL(regather_keys.push_back(keys.at(i)))) {
            LOG_WARN("failed to push back regather key", K(ret), K(i));
          } else {
            LOG_TRACE("external column stat not found in cache", K(keys.at(i)));
            if (OB_FAIL(handles.push_back(handle))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (OB_FAIL(regather_handles_indices.push_back(i))) {
              LOG_WARN("failed to push back regather handle index", K(ret), K(i));
            }
          }
        } else if (OB_ISNULL(handle.stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(keys.at(i)));
        } else if (OB_FAIL(handles.push_back(handle))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !regather_keys.empty()) {
      if (OB_FAIL(batch_load_catalog_table_stats_and_put_cache(tenant_id,
                                                               table_id,
                                                               regather_keys,
                                                               all_partition_values,
                                                               schema_guard))) {
        LOG_WARN("failed to batch load catalog table stats and put cache", K(ret), K(regather_keys));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < regather_keys.count(); ++i) {
          const int64_t handle_index = regather_handles_indices.at(i);
          if (OB_FAIL(catalog_column_stat_cache_.get_row(*regather_keys.at(i), handles.at(handle_index)))) {
            LOG_WARN("get external column stat from cache after backfill failed",
                      K(ret),
                      K(i),
                      K(handle_index),
                      KPC(regather_keys.at(i)));
          }
        }
      }
    }


    if (OB_SUCC(ret) && OB_FAIL(aggregate_column_stats_from_partitions(allocator,
                                                                       column_names,
                                                                       handles,
                                                                       row_cnt,
                                                                       scale_ratio,
                                                                       column_stats))) {
      LOG_WARN("failed to aggregate catalog column stats from handles", K(ret), K(handles.count()));
    }

    LOG_TRACE("lekou stat", K(ret), K(regather_keys.count()), K(handles.count()));
  }
  return ret;
}

int ObOptCatalogStatService::fill_missing_catalog_column_statistics(
    const share::ObILakeTableMetadata *lake_table_metadata,
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;

  struct ColHashKey {
    ObString partition_value_;
    ObString column_name_;
    ColHashKey() : partition_value_(), column_name_() {}
    ColHashKey(const ObString &partition_value, const ObString &column_name)
      : partition_value_(partition_value), column_name_(column_name) {}
    bool operator==(const ColHashKey &other) const {
      return partition_value_ == other.partition_value_ && column_name_ == other.column_name_;
    }
    int hash(uint64_t &hash_val) const {
      hash_val = common::murmurhash(reinterpret_cast<const char *>(partition_value_.ptr()),
                                    partition_value_.length(), 0);
      hash_val = common::murmurhash(reinterpret_cast<const char *>(column_name_.ptr()),
                                    column_name_.length(), hash_val);
      return OB_SUCCESS;
    }
  };

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else if (column_names.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column names is empty", K(ret));
  } else {
    bool is_partitioned = OB_NOT_NULL(table_schema) && table_schema->is_partitioned_table();
    // Build hash set from existing column stats for O(1) lookup
    hash::ObHashSet<ColHashKey> existing_column_stats_set;
    if (OB_FAIL(existing_column_stats_set.create(catalog_column_stats.count() * 2 + 512,
                                                 "ExtColStatSet",
                                                 "ExtColStatSet"))) {
      LOG_WARN("failed to create existing column stats hash set",
               K(ret),
               K(catalog_column_stats.count()));
    } else {
      // Build hash set with existing column stats
      for (int64_t j = 0; OB_SUCC(ret) && j < catalog_column_stats.count(); ++j) {
        if (OB_NOT_NULL(catalog_column_stats.at(j))) {
          const ObString &existing_column_name = catalog_column_stats.at(j)->get_column_name();
          ColHashKey col_hash_key(catalog_column_stats.at(j)->get_partition_value(), existing_column_name);
          if (OB_FAIL(existing_column_stats_set.set_refactored(col_hash_key))) {
            LOG_WARN("failed to insert column stat key to hash set",
                     K(ret),
                     K(j),
                     K(existing_column_name));
          }
        }
      }

      // Only fill missing columns, do NOT reorder
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
        const ObString &partition_value = partition_values.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < column_names.count(); ++j) {
          const ObString &column_name = column_names.at(j);
          share::ObOptCatalogColumnStat *column_stat = nullptr;

          // Try to find existing column stat in hash set
          ColHashKey col_hash_key(partition_value, column_name);
          const int hash_ret = existing_column_stats_set.exist_refactored(col_hash_key);
          if (OB_HASH_EXIST == hash_ret) {
            // Column already exists, skip
            LOG_INFO("column stat already exists, skip filling",
                    K(i),
                    K(column_name));
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            // Column not found, generate default column stat and add it
            share::ObOptCatalogColumnStatBuilder column_stat_builder(allocator);
            int64_t num_distinct = 0;
            if (is_partitioned) {
              const share::schema::ObColumnSchemaV2 *col_schema =
                  table_schema->get_column_schema(column_name);
              if (OB_NOT_NULL(col_schema) && col_schema->is_tbl_part_key_column()) {
                num_distinct = std::max(static_cast<int64_t>(1),
                                        partition_values.count());
              }
            }

            if (OB_FAIL(column_stat_builder.set_basic_info(lake_table_metadata->tenant_id_,
                                                          lake_table_metadata->catalog_id_,
                                                          lake_table_metadata->namespace_name_,
                                                          lake_table_metadata->table_name_,
                                                          partition_value,
                                                          column_name))) {
              LOG_WARN("failed to set basic info for missing column stat",
                      K(ret),
                      K(i),
                      K(column_name));
            } else if (OB_FAIL(column_stat_builder.set_stat_info(
                          0,                              // num_null
                          0,                              // num_not_null
                          num_distinct,                   // num_distinct
                          0,                              // avg_length
                          0,                              // last_analyzed = 0
                          CS_TYPE_UTF8MB4_GENERAL_CI))) { // default collation
              LOG_WARN("failed to set stat info for missing column stat",
                      K(ret),
                      K(i),
                      K(column_name));
            } else if (OB_FAIL(column_stat_builder.build(allocator, column_stat))) {
              LOG_WARN("failed to build missing external column stat", K(ret), K(i), K(column_name));
            } else if (OB_ISNULL(column_stat)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("external column stat is null", K(ret), K(i), K(column_name));
            } else if (OB_FAIL(catalog_column_stats.push_back(column_stat))) {
              LOG_WARN("failed to add generated column stat",
                      K(ret),
                      K(i),
                      K(column_name));
            }
          } else {
            // Hash set error
            ret = hash_ret;
            LOG_WARN("failed to check column stat in hash set", K(ret), K(i), K(column_name));
          }
        }
      }

      // Clean up hash set
      existing_column_stats_set.destroy();
    }
  }

  return ret;
}

int ObOptCatalogStatService::generate_default_catalog_table_statistics(
    const share::ObILakeTableMetadata *lake_table_metadata,
    sql::ObSqlSchemaGuard &schema_guard,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = nullptr;
  catalog_table_stats.reset();
  catalog_column_stats.reset();

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(lake_table_metadata->tenant_id_,
                                                   lake_table_metadata->table_id_,
                                                   table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(lake_table_metadata->tenant_id_),
             K(lake_table_metadata->table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(lake_table_metadata->tenant_id_),
             K(lake_table_metadata->table_id_));
  } else {
    ObSEArray<ObString, 4> partition_values_to_generate;
    if (partition_values.empty()) {
      if (OB_FAIL(partition_values_to_generate.push_back(ObString("")))) {
        LOG_WARN("failed to push back default partition value", K(ret));
      }
    } else if (OB_FAIL(partition_values_to_generate.assign(partition_values))) {
      LOG_WARN("failed to assign partition values", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_values_to_generate.count(); ++i) {
      share::ObOptCatalogTableStatBuilder table_stat_builder;
      share::ObOptCatalogTableStat *catalog_table_stat = nullptr;
      if (OB_FAIL(table_stat_builder.set_basic_info(lake_table_metadata->tenant_id_,
                                                           lake_table_metadata->catalog_id_,
                                                           lake_table_metadata->namespace_name_,
                                                           lake_table_metadata->table_name_,
                                                           partition_values_to_generate.at(i)))) {
        LOG_WARN("failed to set basic info for default table stat", K(ret), K(i));
      } else if (OB_FAIL(table_stat_builder.set_stat_info(0, 0, 0, 0, 0))) {
        LOG_WARN("failed to set stat info for default table stat", K(ret), K(i));
      } else if (OB_FAIL(table_stat_builder.build(allocator, catalog_table_stat))) {
        LOG_WARN("failed to build default catalog table stat", K(ret), K(i));
      } else if (OB_ISNULL(catalog_table_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("catalog table stat is null", K(ret), K(i));
      } else if (OB_FAIL(catalog_table_stats.push_back(catalog_table_stat))) {
        LOG_WARN("failed to push back default table stat", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_missing_catalog_column_statistics(lake_table_metadata,
                                                              table_schema,
                                                              partition_values_to_generate,
                                                              column_names,
                                                              allocator,
                                                              catalog_column_stats))) {
      LOG_WARN("failed to generate missing external column statistics", K(ret));
    }
  }

  return ret;
}

int ObOptCatalogStatService::fetch_catalog_table_statistics_from_catalog(
    const share::ObILakeTableMetadata *lake_table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    sql::ObSqlSchemaGuard &schema_guard,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;
  catalog_table_stats.reset();
  catalog_column_stats.reset();

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else if (column_names.empty()) {
    LOG_INFO("no columns to fetch statistics", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard *schema_getter_guard = schema_guard.get_schema_guard();
    if (OB_ISNULL(schema_getter_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema getter guard is null", K(ret));
    } else {
      // Use the passed-in allocator instead of creating a local one
      share::ObCachedCatalogMetaGetter catalog_getter(*schema_getter_guard, allocator);
      // Fetch table and column statistics via catalog
      if (OB_FAIL(catalog_getter.fetch_table_statistics(allocator,
                                                        schema_guard,
                                                        lake_table_metadata,
                                                        partition_values,
                                                        column_names,
                                                        catalog_table_stats,
                                                        catalog_column_stats))) {
        LOG_WARN("failed to fetch table statistics via catalog", K(ret));
      } else if (catalog_table_stats.empty()) {
        // No table stat found, generate complete default statistics
        LOG_TRACE(
            "[EXTERNAL_TABLE_STAT] No statistics found in catalog, generating default statistics",
            "catalog_id",
            lake_table_metadata->catalog_id_,
            "database_name",
            lake_table_metadata->namespace_name_,
            "table_name",
            lake_table_metadata->table_name_);

        if (OB_FAIL(generate_default_catalog_table_statistics(lake_table_metadata,
                                                              schema_guard,
                                                              partition_values,
                                                              column_names,
                                                              allocator,
                                                              catalog_table_stats,
                                                              catalog_column_stats))) {
          LOG_WARN("failed to generate default catalog table statistics", K(ret));
        }
      } else {
        if (catalog_column_stats.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("catalog returned empty column statistics", K(ret), K(column_names.count()));
        }
      }
    }
  }

  return ret;
}

int ObOptCatalogStatService::put_catalog_table_stats_to_cache(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObIArray<share::ObOptCatalogTableStat *> &catalog_table_stats,
    const ObIArray<share::ObOptCatalogColumnStat *> &catalog_column_stats)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < catalog_table_stats.count(); ++i) {
    const share::ObOptCatalogTableStat *table_stat = catalog_table_stats.at(i);
    if (OB_ISNULL(table_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog table stat is null", K(ret), K(i));
    } else {
      share::ObOptCatalogTableStat::Key table_key;
      table_key.tenant_id_ = tenant_id;
      table_key.catalog_id_ = catalog_id;
      table_key.database_name_ = table_stat->get_database_name();
      table_key.table_name_ = table_stat->get_table_name();
      table_key.partition_value_ = table_stat->get_partition_value();

      if (OB_FAIL(catalog_table_stat_cache_.put_value(table_key, *table_stat))) {
        LOG_WARN("failed to put catalog table stat", K(ret), K(i), K(table_key));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < catalog_column_stats.count(); ++i) {
    const share::ObOptCatalogColumnStat *column_stat = catalog_column_stats.at(i);
    if (OB_ISNULL(column_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stat is null", K(ret), K(i));
    } else {
      share::ObOptCatalogColumnStat::Key column_key;
      column_key.tenant_id_ = tenant_id;
      column_key.catalog_id_ = catalog_id;
      column_key.database_name_ = column_stat->get_database_name();
      column_key.table_name_ = column_stat->get_table_name();
      column_key.partition_value_ = column_stat->get_partition_value();
      column_key.column_name_ = column_stat->get_column_name();

      if (OB_FAIL(catalog_column_stat_cache_.put_row(column_key, *column_stat))) {
        LOG_WARN("failed to put external column stat", K(ret), K(column_key));
      } else {
        LOG_TRACE("successfully put external column stat to cache", K(column_key));
      }
    }
  }

  return ret;
}

int ObOptCatalogStatService::put_catalog_aggr_table_stat_to_cache(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const ObLakeTableStat &table_stat)
{
  int ret = OB_SUCCESS;
  share::ObOptCatalogTableStat catalog_table_stat;
  share::ObOptCatalogTableStat::Key table_key;
  catalog_table_stat.set_tenant_id(tenant_id);
  catalog_table_stat.set_catalog_id(catalog_id);
  catalog_table_stat.set_database_name(database_name);
  catalog_table_stat.set_table_name(table_name);
  catalog_table_stat.set_partition_value(ObString());
  catalog_table_stat.set_row_count(table_stat.total_row_count_);
  catalog_table_stat.set_file_num(table_stat.file_cnt_);
  catalog_table_stat.set_data_size(table_stat.data_size_);
  catalog_table_stat.set_last_analyzed(table_stat.last_analyzed_);
  catalog_table_stat.set_partition_num(table_stat.part_cnt_);
  table_key.tenant_id_ = tenant_id;
  table_key.catalog_id_ = catalog_id;
  table_key.database_name_ = database_name;
  table_key.table_name_ = table_name;
  table_key.partition_value_ = ObString();
  if (OB_FAIL(catalog_table_stat_cache_.put_value(table_key, catalog_table_stat))) {
    LOG_WARN("failed to put catalog table stat to cache", K(ret), K(table_key));
  }
  return ret;
}

int ObOptCatalogStatService::put_catalog_aggr_column_stat_to_cache(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const ObString &column_name,
    const ObLakeColumnStat &column_stat)
{
  int ret = OB_SUCCESS;
  share::ObOptCatalogColumnStat catalog_column_stat;
  share::ObOptCatalogColumnStat::Key column_key;
  const int64_t num_not_null = column_stat.record_count_ >= column_stat.null_count_
                                   ? column_stat.record_count_ - column_stat.null_count_
                                   : 0;
  const int64_t avg_length = column_stat.size_;
  catalog_column_stat.set_tenant_id(tenant_id);
  catalog_column_stat.set_catalog_id(catalog_id);
  catalog_column_stat.set_database_name(database_name);
  catalog_column_stat.set_table_name(table_name);
  catalog_column_stat.set_partition_value(ObString());
  catalog_column_stat.set_column_name(column_name);
  catalog_column_stat.set_num_null(column_stat.null_count_);
  catalog_column_stat.set_num_not_null(num_not_null);
  catalog_column_stat.set_num_distinct(column_stat.num_distinct_);
  catalog_column_stat.set_avg_length(avg_length);
  catalog_column_stat.set_last_analyzed(column_stat.last_analyzed_);
  if (!column_stat.min_val_.is_min_value() && !column_stat.min_val_.is_null()) {
    catalog_column_stat.set_min_value(column_stat.min_val_);
  }
  if (!column_stat.max_val_.is_max_value() && !column_stat.max_val_.is_null()) {
    catalog_column_stat.set_max_value(column_stat.max_val_);
  }
  column_key.tenant_id_ = tenant_id;
  column_key.catalog_id_ = catalog_id;
  column_key.database_name_ = database_name;
  column_key.table_name_ = table_name;
  column_key.partition_value_ = ObString();  // 空分区表示全局
  column_key.column_name_ = column_name;
  if (OB_FAIL(catalog_column_stat_cache_.put_row(column_key, catalog_column_stat))) {
    LOG_WARN("failed to put catalog column stat to cache", K(ret), K(column_key), K(catalog_column_stat));
  }
  return ret;
}

int ObOptCatalogStatService::aggregate_table_stat_from_partitions(
    const ObIArray<share::ObOptCatalogTableStatHandle> &handles,
    ObLakeTableStat &stat)
{
  int ret = OB_SUCCESS;
  stat.reset();

  if (handles.empty()) {
    // no stat tables to aggregate
  } else {
    int64_t total_row_count = 0;
    int64_t total_data_size = 0;
    int64_t total_file_num = 0;
    int64_t total_partition_num = 0;
    int64_t last_analyzed = 0;
    bool has_global_stat = false;

    for (int64_t i = 0; OB_SUCC(ret) && !has_global_stat && i < handles.count(); ++i) {
      if (OB_ISNULL(handles.at(i).stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("catalog table stat is null", K(ret), K(i));
      } else if (!handles.at(i).stat_->is_valid()) {
        // skip invalid stat
      } else if (handles.at(i).stat_->is_global_table_stat()) {
        has_global_stat = true;
        total_row_count = handles.at(i).stat_->get_row_count();
        total_file_num = handles.at(i).stat_->get_file_num();
        total_data_size = handles.at(i).stat_->get_data_size();
        total_partition_num = handles.at(i).stat_->get_partition_num();
        last_analyzed = handles.at(i).stat_->get_last_analyzed();
      } else {
        total_row_count += handles.at(i).stat_->get_row_count();
        total_data_size += handles.at(i).stat_->get_data_size();
        total_file_num += handles.at(i).stat_->get_file_num();
        total_partition_num += handles.at(i).stat_->get_partition_num();
        if (handles.at(i).stat_->get_last_analyzed() > last_analyzed) {
          last_analyzed = handles.at(i).stat_->get_last_analyzed();
        }
      }
    }

    if (OB_SUCC(ret)) {
      stat.total_row_count_ = total_row_count;
      stat.pruned_row_count_ = total_row_count;
      stat.data_size_ = total_data_size;
      stat.part_cnt_ = total_partition_num;
      stat.file_cnt_ = total_file_num;
      stat.last_analyzed_ = last_analyzed;
      LOG_TRACE("aggregated catalog table stats from handles",
                K(ret),
                K(has_global_stat),
                K(handles.count()),
                K(stat));
    }
  }

  return ret;
}

int ObOptCatalogStatService::aggregate_single_column_stat_from_partitions(
    ObIAllocator &alloc,
    const ObIArray<const share::ObOptCatalogColumnStat *> &partition_col_stats,
    const int64_t row_cnt,
    const double scale_ratio,
    ObLakeColumnStat &column_stat)
{
  int ret = OB_SUCCESS;
  int64_t total_num_null = 0;
  int64_t total_num_not_null = 0;
  int64_t col_last_analyzed = 0;
  ObObj global_min_obj;
  ObObj global_max_obj;
  bool has_min = false;
  bool has_max = false;
  bool has_stat = false;
  double weighted_len_sum = 0.0;
  ObGlobalNdvEval ndv_eval;

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_stats.count(); ++i) {
    const share::ObOptCatalogColumnStat *catalog_stat = partition_col_stats.at(i);
    if (OB_ISNULL(catalog_stat)) {
    } else {
      has_stat = true;
      total_num_null += catalog_stat->get_num_null();
      total_num_not_null += catalog_stat->get_num_not_null();
      ndv_eval.add(catalog_stat->get_num_distinct(), catalog_stat->get_llc_bitmap());
      if (catalog_stat->get_last_analyzed() > col_last_analyzed) {
        col_last_analyzed = catalog_stat->get_last_analyzed();
      }
      if (!catalog_stat->get_min_value().is_null()) {
        if (!has_min || catalog_stat->get_min_value() < global_min_obj) {
          global_min_obj = catalog_stat->get_min_value();
          has_min = true;
        }
      }
      if (!catalog_stat->get_max_value().is_null()) {
        if (!has_max || catalog_stat->get_max_value() > global_max_obj) {
          global_max_obj = catalog_stat->get_max_value();
          has_max = true;
        }
      }
      if (catalog_stat->get_avg_length() > 0 && catalog_stat->get_num_rows() > 0) {
        weighted_len_sum += static_cast<double>(catalog_stat->get_avg_length())
                            * catalog_stat->get_num_rows();
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!has_stat) {
    column_stat.last_analyzed_ = 0;
  } else {
    const int64_t total_row_count = total_num_null + total_num_not_null;
    if (has_min && OB_FAIL(ob_write_obj(alloc, global_min_obj, column_stat.min_val_))) {
      LOG_WARN("failed to deep copy min value", K(ret));
    } else if (has_max && OB_FAIL(ob_write_obj(alloc, global_max_obj, column_stat.max_val_))) {
      LOG_WARN("failed to deep copy max value", K(ret));
    } else {
      column_stat.null_count_ = static_cast<int64_t>(total_num_null * scale_ratio);
      column_stat.record_count_ = row_cnt;
      column_stat.num_distinct_ = ndv_eval.get();
      if (scale_ratio < 1.0) {
        column_stat.num_distinct_ = ObOptSelectivity::scale_distinct(
            row_cnt, row_cnt / scale_ratio, column_stat.num_distinct_);
      }
      column_stat.last_analyzed_ = col_last_analyzed;
      if (total_row_count > 0) {
        column_stat.size_ = static_cast<int64_t>(round(weighted_len_sum / total_row_count));
      }
    }
  }
  return ret;
}

int ObOptCatalogStatService::aggregate_column_stats_from_partitions(
    ObIAllocator &allocator,
    const ObIArray<ObString> &column_names,
    const ObIArray<share::ObOptCatalogColumnStatHandle> &handles,
    const int64_t row_cnt,
    const double scale_ratio,
    ObIArray<ObLakeColumnStat*> &column_stats)
{
  int ret = OB_SUCCESS;
  column_stats.reset();

  if (handles.empty()) {
    // no stat columns to aggregate
  } else {
    // load_catalog_table_stat_and_put_cache 不一定保证列统计信息的顺序，所以用哈希保证正确性
    hash::ObHashMap<ObString, ObSEArray<const share::ObOptCatalogColumnStat *, 4>> column_stat_map;
    if (OB_FAIL(column_stat_map.create(handles.count() * 2 + 512, ObModIds::OB_HASH_BUCKET))) {
      LOG_WARN("failed to create column stat map", K(ret), K(handles.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < handles.count(); ++i) {
        const share::ObOptCatalogColumnStat *catalog_stat = handles.at(i).stat_;
        if (OB_ISNULL(catalog_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("catalog stat is null", K(ret), K(i));
        } else {
          const ObString &column_name = catalog_stat->get_column_name();
          ObSEArray<const share::ObOptCatalogColumnStat *, 4> col_stats_array;
          const int hash_ret = column_stat_map.get_refactored(column_name, col_stats_array);
          if (OB_HASH_NOT_EXIST == hash_ret) {
            ObSEArray<const share::ObOptCatalogColumnStat *, 4> new_array;
            if (OB_FAIL(new_array.push_back(catalog_stat))) {
              LOG_WARN("failed to push back column stat", K(ret), K(i), K(column_name));
            } else if (OB_FAIL(column_stat_map.set_refactored(column_name, new_array))) {
              LOG_WARN("failed to set column stat map", K(ret), K(i), K(column_name));
            }
          } else if (OB_SUCCESS == hash_ret) {
            if (OB_FAIL(col_stats_array.push_back(catalog_stat))) {
              LOG_WARN("failed to push back column stat", K(ret), K(i), K(column_name));
            } else if (OB_FAIL(column_stat_map.set_refactored(column_name, col_stats_array, 1))) {
              LOG_WARN("failed to update column stat map", K(ret), K(i), K(column_name));
            }
          } else {
            ret = hash_ret;
            LOG_WARN("failed to get column stat from map", K(ret), K(i), K(column_name));
          }
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
        const ObString &column_name = column_names.at(i);
        if (ObExternalTableUtils::is_hidden_external_column(column_name)) {
          // do nothing
        } else {
          ObSEArray<const share::ObOptCatalogColumnStat *, 4> col_stats_array;
          ObLakeColumnStat *column_stat = nullptr;
          void *ptr = nullptr;
          if (OB_FAIL(column_stat_map.get_refactored(column_name, col_stats_array))) {
            LOG_WARN("failed to get column stats from map", K(ret), K(i), K(column_name));
          } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObLakeColumnStat)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret), K(i), K(column_name));
          } else {
            column_stat = new (ptr) ObLakeColumnStat();
            column_stat->last_analyzed_ = 0;
            if (OB_FAIL(aggregate_single_column_stat_from_partitions(allocator,
                                                                    col_stats_array,
                                                                    row_cnt,
                                                                    scale_ratio,
                                                                    *column_stat))) {
              LOG_WARN("failed to aggregate single column stat from partitions",
                      K(ret),
                      K(i),
                      K(column_name));
            } else if (OB_FAIL(column_stats.push_back(column_stat))) {
              LOG_WARN("failed to push back column stat", K(ret), K(i), K(column_name));
            }
          }
        }
      }

      column_stat_map.destroy();
    }
  }

  return ret;
}

int ObOptCatalogStatService::load_catalog_table_stat_and_put_cache(
    const ObLoadCatalogTableStatParam &param,
    const ObIArray<ObString> &all_partition_values,
    sql::ObSqlSchemaGuard &schema_guard,
    const ObIArray<ObString> &key_partition_values)
{
  int ret = OB_SUCCESS;
  const share::ObILakeTableMetadata *lake_table_metadata = nullptr;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ObSEArray<ObString, 16> column_names;
  ObSEArray<share::ObOptCatalogTableStat *, 16>  catalog_table_stats;
  ObSEArray<share::ObOptCatalogColumnStat *, 16> catalog_column_stats;
  ObArenaAllocator stat_allocator("ExtStatFetch", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  bool stats_from_system_table = false;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input arguments", K(ret), K(param));
  } else if (OB_FAIL(schema_guard.get_lake_table_metadata(param.tenant_id_,
                                                          param.table_id_,
                                                          lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret), K(param));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret), K(param));
  } else if (OB_FAIL(
                 schema_guard.get_table_schema(param.tenant_id_, param.table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(param));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(param));
  } else if (OB_FAIL(extract_column_names_from_table_schema(*table_schema, column_names))) {
    LOG_WARN("failed to extract column names from table schema", K(ret));
  } else if (OB_FAIL(fetch_catalog_table_stat_from_system_table(param,
                                                                lake_table_metadata,
                                                                table_schema,
                                                                key_partition_values,
                                                                column_names,
                                                                stat_allocator,
                                                                catalog_table_stats,
                                                                catalog_column_stats,
                                                                stats_from_system_table))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to fetch catalog table stat from system table", K(ret));
    }
  }

  LOG_TRACE("fetch catalog statistic from system table", K(ret), K(stats_from_system_table));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!stats_from_system_table) {
    if (OB_FAIL(fetch_catalog_table_stat_from_catalog_and_cache(param,
                                                                lake_table_metadata,
                                                                table_schema,
                                                                all_partition_values,
                                                                key_partition_values,
                                                                column_names,
                                                                schema_guard,
                                                                stat_allocator))) {
      LOG_WARN("failed to fetch catalog table stat from catalog and cache", K(ret));
    }
  } else if (OB_FAIL(put_catalog_table_stats_to_cache(param.tenant_id_,
                                                      param.catalog_id_,
                                                      catalog_table_stats,
                                                      catalog_column_stats))) {
    LOG_WARN("failed to put catalog table stats to cache", K(ret));
  }

  return ret;
}

int ObOptCatalogStatService::fetch_catalog_table_stat_from_system_table(
    const ObLoadCatalogTableStatParam &param,
    const share::ObILakeTableMetadata *lake_table_metadata,
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &key_partition_values,
    const ObIArray<ObString> &column_names,
    ObIAllocator &stat_allocator,
    ObIArray<ObOptCatalogTableStat *> &partition_table_stats,
    ObIArray<ObOptCatalogColumnStat *> &partition_column_stats,
    bool &stats_from_system_table)
{
  int ret = OB_SUCCESS;
  partition_table_stats.reset();
  partition_column_stats.reset();
  stats_from_system_table = false;

  // Step 1: fetch table stats
  if (OB_FAIL(cat_sql_service_.fetch_catalog_table_stat_from_system_table(
          param.tenant_id_,
          param.catalog_id_,
          lake_table_metadata->namespace_name_,
          lake_table_metadata->table_name_,
          table_schema,
          key_partition_values,
          stat_allocator,
          partition_table_stats))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to fetch catalog table stat from system table", K(ret));
    }
  }

  // Step 2: fetch column stats
  if (OB_SUCC(ret) && !partition_table_stats.empty()) {
    if (OB_FAIL(cat_sql_service_.fetch_catalog_column_stats_from_system_table(
            param.tenant_id_,
            param.catalog_id_,
            lake_table_metadata->namespace_name_,
            lake_table_metadata->table_name_,
            table_schema,
            key_partition_values,
            column_names,
            stat_allocator,
            partition_column_stats))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to fetch catalog column stat from system table", K(ret));
      }
    }
  }

  // Fill missing columns if system table returned incomplete statistics
  // 需要考虑非分区表的场景
  int64_t expected_col_stat_cnt = key_partition_values.count() == 0 ? 1 : column_names.count() * key_partition_values.count();
  if (OB_SUCC(ret) && partition_column_stats.count() < expected_col_stat_cnt) {
    if (OB_FAIL(fill_missing_catalog_column_statistics(lake_table_metadata,
                                                       table_schema,
                                                       key_partition_values,
                                                       column_names,
                                                       stat_allocator,
                                                       partition_column_stats))) {
      LOG_WARN("failed to fill missing column statistics", K(ret));
    }
  }

  // Step 3: return partition-granularity stats directly
  if (OB_SUCC(ret) && !partition_table_stats.empty() && !partition_column_stats.empty()) {
    stats_from_system_table = true;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no stats found", K(ret));
  }

  // Step 4: cleanup on failure
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < partition_table_stats.count(); ++i) {
      if (OB_NOT_NULL(partition_table_stats.at(i))) {
        partition_table_stats.at(i)->~ObOptCatalogTableStat();
      }
    }
    for (int64_t i = 0; i < partition_column_stats.count(); ++i) {
      if (OB_NOT_NULL(partition_column_stats.at(i))) {
        partition_column_stats.at(i)->~ObOptCatalogColumnStat();
      }
    }
    partition_table_stats.reset();
    partition_column_stats.reset();
  }

  return ret;
}

int ObOptCatalogStatService::fetch_catalog_table_stat_from_catalog_and_cache(
    const ObLoadCatalogTableStatParam &param,
    const share::ObILakeTableMetadata *lake_table_metadata,
    const share::schema::ObTableSchema *table_schema,
    const ObIArray<ObString> &all_partition_values,
    const ObIArray<ObString> &key_partition_values,
    const ObIArray<ObString> &column_names,
    sql::ObSqlSchemaGuard &schema_guard,
    ObIAllocator &stat_allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::ObOptCatalogTableStat *, 16> catalog_table_stats;
  ObSEArray<share::ObOptCatalogColumnStat *, 16> catalog_column_stats;

  LOG_TRACE("[EXTERNAL_TABLE_STAT] Fetching from external catalog (HMS/Glue)",
            "tenant_id",
            param.tenant_id_,
            "catalog_id",
            param.catalog_id_,
            "database_name",
            lake_table_metadata->namespace_name_,
            "table_name",
            lake_table_metadata->table_name_,
            "format_type",
            lake_table_metadata->get_format_type());

  const ObIArray<ObString> *partition_values_to_fetch = &key_partition_values;
  if (OB_NOT_NULL(table_schema) && table_schema->is_partitioned_table()) {
    if (lake_table_metadata->get_format_type() == ObLakeTableFormat::ODPS
        || key_partition_values.empty()) {
      partition_values_to_fetch = &all_partition_values;
    } else {
      partition_values_to_fetch = &key_partition_values;
    }
  } else {
    partition_values_to_fetch = &key_partition_values;
  }

  if (OB_FAIL(fetch_catalog_table_statistics_from_catalog(lake_table_metadata,
                                                          *partition_values_to_fetch,
                                                          column_names,
                                                          schema_guard,
                                                          stat_allocator,
                                                          catalog_table_stats,
                                                          catalog_column_stats))) {
    LOG_WARN("failed to fetch catalog table statistics from catalog", K(ret));
  } else if (OB_FAIL(put_catalog_table_stats_to_cache(param.tenant_id_,
                                                      param.catalog_id_,
                                                      catalog_table_stats,
                                                      catalog_column_stats))) {
    LOG_WARN("failed to put catalog table stats to cache", K(ret));
  } else {
    LOG_TRACE("[EXTERNAL_TABLE_STAT] FINAL: Statistics cached and ready for optimizer",
              "tenant_id",
              param.tenant_id_,
              "catalog_id",
              param.catalog_id_,
              "table_id",
              param.table_id_,
              "database_name",
              lake_table_metadata->namespace_name_,
              "table_name",
              lake_table_metadata->table_name_,
              "source",
              "CATALOG",
              "table_stat_count",
              catalog_table_stats.count(),
              "column_stat_count",
              catalog_column_stats.count());
  }

  return ret;
}

} // namespace common
} // namespace oceanbase