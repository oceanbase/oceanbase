/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/hms/ob_hms_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/odps/ob_odps_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/iceberg/ob_iceberg_stats_freshness_checker.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_opt_catalog_stat_sql_service.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat_cache.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat_cache.h"
#include "share/stat/catalog/ob_opt_catalog_stat_service.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/ob_catalog_ext_partition_info.h"
#include "observer/mysql/obmp_utils.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "share/external_table/ob_external_table_utils.h"
#include <algorithm>

namespace oceanbase
{
using namespace sql;
namespace common
{

int ObDbmsCatalogStatsUtils::build_iceberg_partition_sql_clause(
    ObIAllocator &allocator,
    const common::ObCatalogExtPartitionInfo &partition_info,
    ObString &partition_clause)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  partition_clause.reset();
  if (OB_UNLIKELY(partition_info.iceberg_spec_id_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iceberg spec id", K(ret), K(partition_info));
  } else if (OB_UNLIKELY(partition_info.iceberg_part_names_.count()
                         != partition_info.iceberg_part_sql_literals_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iceberg partition field count does not match sql literal count",
             K(ret),
             K(partition_info));
  } else if (OB_FAIL(sql.append("PARTITION("))) {
    LOG_WARN("failed to start iceberg partition clause", K(ret));
  } else if (OB_FAIL(sql.append_fmt("spec_id=%ld", partition_info.iceberg_spec_id_))) {
    LOG_WARN("failed to append iceberg spec id", K(ret), K(partition_info.iceberg_spec_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.iceberg_part_names_.count(); ++i) {
      const ObString &field_name = partition_info.iceberg_part_names_.at(i);
      const ObString &sql_literal = partition_info.iceberg_part_sql_literals_.at(i);
      if (OB_FAIL(sql.append_fmt(", %.*s=", field_name.length(), field_name.ptr()))) {
        LOG_WARN("failed to append iceberg partition field name", K(ret), K(field_name), K(i));
      } else if (OB_FAIL(sql.append(sql_literal))) {
        LOG_WARN("failed to append iceberg partition sql literal", K(ret), K(sql_literal), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to close iceberg partition clause", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, sql.string(), partition_clause, true))) {
    LOG_WARN("failed to write iceberg partition clause", K(ret));
  }
  LOG_TRACE("catalog stat, iceberg partition clause", K(ret), K(partition_clause));
  return ret;
}

int ObDbmsCatalogStatsUtils::build_iceberg_partition_info(
                                            common::ObIAllocator &allocator,
                                            const sql::iceberg::PartitionSpec &partition_spec,
                                            const common::ObIArray<common::ObObj> &partition_values,
                                            common::ObCatalogExtPartitionInfo &partition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(partition_spec.fields.count() != partition_values.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition field count does not match values",
             K(ret), K(partition_spec.fields.count()), K(partition_values.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_spec.fields.count(); ++i) {
      const sql::iceberg::PartitionField *field = partition_spec.fields.at(i);
      ObString field_name;
      common::ObString partition_value_plain;
      common::ObString partition_value_sql_literal;
      if (OB_ISNULL(field)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition field is null", K(ret), K(i));
      } else if (FALSE_IT(field_name = field->name)) {
      } else if (partition_values.at(i).is_null()) {
        if (OB_FAIL(ob_write_string(allocator, ObString("NULL"), partition_value_plain, true))) {
          LOG_WARN("failed to write null partition value", K(ret), K(i));
        }
      } else if (OB_FAIL(observer::ObMPUtils::get_plain_str_literal(
                             allocator, partition_values.at(i), partition_value_plain))) {
        LOG_WARN("failed to print plain partition value", K(ret), K(i));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_info.partition_values_.push_back(partition_value_plain))) {
        LOG_WARN("failed to push partition value", K(ret));
      } else if (sql::iceberg::TransformType::Void == field->transform.transform_type) {
        // Void fields are not rendered in Iceberg PARTITION(...) syntax.
      } else {
        const common::ObObj &obj = partition_values.at(i);
        common::ObString field_name_copy;
        if (OB_FAIL(ob_write_string(allocator, field_name, field_name_copy, true))) {
          LOG_WARN("failed to write iceberg partition field name", K(ret), K(field_name));
        } else if (OB_FAIL(partition_info.iceberg_part_names_.push_back(field_name_copy))) {
          LOG_WARN("failed to push iceberg partition field name", K(ret), K(field_name));
        } else if (obj.is_null()) {
          if (OB_FAIL(ob_write_string(allocator, ObString("NULL"), partition_value_sql_literal, true))) {
            LOG_WARN("failed to write null partition sql literal", K(ret), K(i));
          }
        } else if (OB_FAIL(observer::ObMPUtils::get_user_sql_literal(allocator,
                                                                     obj,
                                                                     partition_value_sql_literal,
                                                                     ObObjPrintParams()))) {
          LOG_WARN("failed to print iceberg partition sql literal", K(ret), K(obj));
        }

        if (OB_SUCC(ret)
           && OB_FAIL(partition_info.iceberg_part_sql_literals_.push_back(partition_value_sql_literal))) {
          LOG_WARN("failed to push iceberg partition sql literal", K(ret), K(partition_value_sql_literal));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(partition_info.iceberg_spec_id_ = partition_spec.spec_id)) {
  } else if (OB_FAIL(ObExternalTableUtils::build_iceberg_partition_json_desc(
                         allocator,
                         partition_spec,
                         partition_values,
                         partition_info.partition_))) {
    LOG_WARN("failed to build iceberg partition json", K(ret));
  }

  return ret;
}

int ObDbmsCatalogStatsUtils::collect_iceberg_partition_infos(
                                    common::ObIAllocator &allocator,
                                    share::ObILakeTableMetadata *lake_table_metadata,
                                    common::ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos,
                                    int64_t &global_modified_ts)
{
  int ret = OB_SUCCESS;
  partition_infos.reset();
  global_modified_ts = -1;
  sql::iceberg::ObIcebergTableMetadata *iceberg_metadata = nullptr;
  sql::iceberg::Snapshot *current_snapshot = nullptr;
  common::ObArray<sql::iceberg::ManifestFile *> manifest_files;
  hash::ObHashMap<sql::iceberg::PartitionKey, int64_t> partition_index_map;
  if (OB_ISNULL(lake_table_metadata)
      || OB_UNLIKELY(share::ObLakeTableFormat::ICEBERG != lake_table_metadata->get_format_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iceberg metadata", K(ret));
  } else if (FALSE_IT(iceberg_metadata =
                          static_cast<sql::iceberg::ObIcebergTableMetadata *>(lake_table_metadata))) {
  } else if (OB_FAIL(iceberg_metadata->table_metadata_.get_current_snapshot(current_snapshot))) {
    if (OB_ERR_TABLE_SNAPSHOT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      current_snapshot = nullptr;
    } else {
      LOG_WARN("failed to get current snapshot", K(ret));
    }
  } else if (OB_ISNULL(current_snapshot)) {
    // empty iceberg table
  } else if (OB_FAIL(current_snapshot->get_manifest_files(iceberg_metadata->access_info_, manifest_files))) {
    LOG_WARN("failed to get manifest files", K(ret));
  } else if (OB_FAIL(partition_index_map.create(std::max<int64_t>(64, manifest_files.count() * 16),
                                                "IcePartIdx",
                                                "IcePartIdx"))) {
    LOG_WARN("failed to create iceberg partition map", K(ret), K(manifest_files.count()));
  } else {
    global_modified_ts = current_snapshot->timestamp_ms * 1000L;
    for (int64_t i = 0; OB_SUCC(ret) && i < manifest_files.count(); ++i) {
      sql::iceberg::ManifestFile *manifest_file = manifest_files.at(i);
      common::ObArray<sql::iceberg::ManifestEntry *> manifest_entries;
      if (OB_ISNULL(manifest_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("manifest file is null", K(ret), K(i));
      } else if (OB_FAIL(manifest_file->get_manifest_entries(iceberg_metadata->access_info_,
                                                             manifest_entries))) {
        LOG_WARN("failed to get manifest entries", K(ret), K(i), K(manifest_file->partition_spec_id));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < manifest_entries.count(); ++j) {
          sql::iceberg::ManifestEntry *manifest_entry = manifest_entries.at(j);
          if (OB_ISNULL(manifest_entry)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("manifest entry is null", K(ret), K(i), K(j));
          } else if (!manifest_entry->is_alive() || !manifest_entry->is_data_file()) {
            // only collect current live data files
          } else {
            sql::iceberg::PartitionKey partition_key;
            int64_t part_idx = -1;
            if (OB_FAIL(partition_key.init_from_manifest_entry(*manifest_entry))) {
              LOG_WARN("failed to init partition key", K(ret), K(i), K(j));
            } else {
              int hash_ret = partition_index_map.get_refactored(partition_key, part_idx);
              if (OB_HASH_NOT_EXIST == hash_ret) {
                common::ObCatalogExtPartitionInfo partition_info;
                partition_info.file_num_ = 1;
                partition_info.data_size_ = manifest_entry->data_file.file_size_in_bytes;
                partition_info.modify_ts_ = global_modified_ts;
                partition_info.schema_version_ = lake_table_metadata->lake_table_metadata_version_;
                if (OB_FAIL(build_iceberg_partition_info(allocator,
                                                         manifest_entry->partition_spec,
                                                         manifest_entry->data_file.partition,
                                                         partition_info))) {
                  LOG_WARN("failed to build iceberg partition info", K(ret), K(i), K(j));
                } else if (OB_FAIL(partition_infos.push_back(partition_info))) {
                  LOG_WARN("failed to push back iceberg partition info", K(ret));
                } else if (FALSE_IT(part_idx = partition_infos.count() - 1)) {
                } else if (OB_FAIL(partition_index_map.set_refactored(partition_key, part_idx))) {
                  LOG_WARN("failed to set partition key index", K(ret), K(part_idx));
                }
              } else if (OB_SUCCESS != hash_ret) {
                ret = hash_ret;
                LOG_WARN("failed to get partition key index", K(ret), K(i), K(j));
              } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= partition_infos.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid partition index", K(ret), K(part_idx), K(partition_infos.count()));
              } else {
                partition_infos.at(part_idx).file_num_ += 1;
                partition_infos.at(part_idx).data_size_ += manifest_entry->data_file.file_size_in_bytes;
              }
            }
          }
        }
      }
    }
  }

  if (partition_index_map.created()) {
    partition_index_map.destroy();
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::classify_catalog_opt_stat(
    const ObIArray<ObOptCatalogStat> &catalog_stats,
    ObIArray<share::ObOptCatalogTableStat *> &table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  table_stats.reset();
  column_stats.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < catalog_stats.count(); ++i) {
    const ObOptCatalogStat &catalog_stat = catalog_stats.at(i);

    // Add table stat
    if (OB_ISNULL(catalog_stat.table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog table stat is null", K(ret), K(i));
    } else {
      // Log catalog table stat info before classifying
      bool is_partitioned = !catalog_stat.table_stat_->get_partition_value().empty();
      if (OB_FAIL(table_stats.push_back(catalog_stat.table_stat_))) {
        LOG_WARN("failed to push back catalog table stat", K(ret));
      } else {
        // Add column stats
        for (int64_t j = 0; OB_SUCC(ret) && j < catalog_stat.column_stats_.count(); ++j) {
          if (OB_ISNULL(catalog_stat.column_stats_.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("external column stat is null", K(ret), K(i), K(j));
          } else if (OB_FAIL(column_stats.push_back(catalog_stat.column_stats_.at(j)))) {
            LOG_WARN("failed to push back external column stat", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDbmsCatalogStatsUtils::hex_str_to_obj(const char *buf,
                                            int64_t buf_len,
                                            ObIAllocator &allocator,
                                            ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t ret_len = 0;
  char *resbuf = NULL;
  if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (NULL == (resbuf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot allocate memory for deserializing obj.", K(buf_len), K(ret));
  } else if (buf_len
             != (ret_len = common::str_to_hex(buf,
                                              static_cast<int32_t>(buf_len),
                                              resbuf,
                                              static_cast<int32_t>(buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer str to hex failed", K(buf), K(buf_len), K(ret_len), K(ret));
  } else if (OB_FAIL(obj.deserialize(resbuf, ret_len, pos))) {
    LOG_WARN("deserialize obj failed.", K(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

bool ObDbmsCatalogStatsUtils::is_hive_default_partition(const ObString &value)
{
  bool bret = false;
  if (value.length() == HIVE_DEFAULT_PARTITION_LEN
      && memcmp(value.ptr(), HIVE_DEFAULT_PARTITION, HIVE_DEFAULT_PARTITION_LEN) == 0) {
    bret = true;
  }
  return bret;
}

int ObDbmsCatalogStatsUtils::init_catalog_table_stats(
    const int64_t cnt,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogTableStat *> &table_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    share::ObOptCatalogTableStat *opt_stat = NULL;
    void *p = NULL;
    if (OB_ISNULL(p = allocator.alloc(sizeof(share::ObOptCatalogTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for T", K(ret));
    } else if (OB_FALSE_IT(opt_stat = new (p) share::ObOptCatalogTableStat())) {
    } else if (OB_FAIL(table_stats.push_back(opt_stat))) {
      LOG_WARN("failed to push back ObOptCatalogTableStat", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::init_catalog_col_stats(
    const int64_t col_cnt,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogColumnStat *> &col_stats)
{
  int ret = OB_SUCCESS;
  if (col_cnt <= 0) {
    // do nothing
  } else if (OB_FAIL(col_stats.prepare_allocate(col_cnt))) {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      ObOptCatalogColumnStat *&col_stat = col_stats.at(i);
      if (OB_ISNULL(col_stat = ObOptCatalogColumnStat::malloc_new_column_stat(allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(col_stat));
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::assign_col_param(
    const ObIArray<ObCatalogColumnStatParam> *src_col_params,
    const int64_t start,
    const int64_t end,
    ObIArray<ObCatalogColumnStatParam> &target_col_params)
{
  int ret = OB_SUCCESS;
  if (start < 0 || end < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument start or end index must be great than 0", K(start), K(end));
  } else if (OB_ISNULL(src_col_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexepected null", K(ret));
  } else if (start > end) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument start larger thran end index", K(start), K(end));
  } else if (start >= src_col_params->count() || end > src_col_params->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument start larger thran end index",
             K(start),
             K(end),
             K(src_col_params->count()));
  } else {
    target_col_params.reset();
    target_col_params.reserve(end - start);
    for (int i = start; OB_SUCC(ret) && i < end; i++) {
      if (OB_FAIL(target_col_params.push_back(src_col_params->at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::deep_copy_string_helper(const int64_t buf_len,
                                                     const ObString &str,
                                                     char *buf,
                                                     int64_t &pos,
                                                     ObString &dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < pos + str.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(str.length()));
  } else {
    MEMCPY(buf + pos, str.ptr(), str.length());
    dst.assign_ptr(buf + pos, str.length());
    pos += str.length();
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::split_batch_write(
    const ObCatalogTableStatParam &table_param,
    ObExecContext &ctx,
    sqlclient::ObISQLConnection *conn,
    ObIArray<share::ObOptCatalogTableStat *> &table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  int64_t idx_tab_stat = 0;
  int64_t idx_col_stat = 0;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_my_session()));
  }
  while (OB_SUCC(ret)
         && (idx_tab_stat < table_stats.count() || idx_col_stat < column_stats.count())) {
    ObSEArray<share::ObOptCatalogTableStat *, 4> write_table_stats;
    ObSEArray<share::ObOptCatalogColumnStat *, 4> write_column_stats;
    ObSEArray<ObCatalogColumnStatParam, 4> write_column_params;

    if (OB_UNLIKELY(idx_tab_stat > table_stats.count() || idx_col_stat > column_stats.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpectd error",
               K(ret),
               K(idx_tab_stat),
               K(table_stats.count()),
               K(idx_col_stat),
               K(column_stats.count()));
    }

    // Process table stats.
    for (int64_t i = 0;
         OB_SUCC(ret) && i < MAX_NUM_OF_WRITE_STATS && idx_tab_stat < table_stats.count();
         ++i) {
      share::ObOptCatalogTableStat *cur_tab_stat = table_stats.at(idx_tab_stat);
      if (OB_ISNULL(cur_tab_stat)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument about table stat is null", K(ret));
      } else if (OB_FAIL(write_table_stats.push_back(cur_tab_stat))) {
        LOG_WARN("failed to push back table stat", K(ret));
      } else {
        ++idx_tab_stat;
      }
    }

    // Process column stats
    int64_t col_stat_cnt = 0;
    while (OB_SUCC(ret) && col_stat_cnt < MAX_NUM_OF_WRITE_STATS
           && idx_col_stat < column_stats.count()) {
      share::ObOptCatalogColumnStat *cur_ext_col_stat = column_stats.at(idx_col_stat);
      if (OB_ISNULL(cur_ext_col_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(cur_ext_col_stat));
      } else if (OB_FAIL(write_column_stats.push_back(cur_ext_col_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        bool found = false;
        const ObString &col_name = cur_ext_col_stat->get_column_name();
        for (int64_t j = 0; OB_SUCC(ret) && j < table_param.column_params_.count(); ++j) {
          const ObCatalogColumnStatParam &cur_param = table_param.column_params_.at(j);
          // Catalog tables use column_name for matching (case-sensitive)
          if (0 == col_name.case_compare(cur_param.column_name_)) {
            if (OB_FAIL(write_column_params.push_back(table_param.column_params_.at(j)))) {
              LOG_WARN("failed to push back column param", K(ret));
            } else {
              found = true;
            }
            break;
          }
        }
        if (OB_SUCC(ret) && !found) {
          // If column_params_ is empty or column not found, create a default column param
          // This can happen when column_params_ was not populated before calling
          // `split_batch_write`.
          if (OB_ISNULL(table_param.allocator_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator is null, cannot create default column param", K(ret));
          } else {
            ObCatalogColumnStatParam default_col_param;
            if (OB_FAIL(ob_write_string(*table_param.allocator_,
                                        col_name,
                                        default_col_param.column_name_))) {
              LOG_WARN("failed to write column name", K(ret), K(col_name));
            } else {
              default_col_param.set_valid_opt_col();
              default_col_param.set_need_basic_stat();
              default_col_param.set_need_avg_len();
              if (OB_FAIL(write_column_params.push_back(default_col_param))) {
                LOG_WARN("failed to push back default column param", K(ret));
              } else {
                LOG_TRACE("created default column param for column stat",
                          K(col_name),
                          "column_params_count",
                          table_param.column_params_.count());
                found = true;
              }
            }
          }
        }
        if (OB_SUCC(ret) && found) {
          ++col_stat_cnt;
          ++idx_col_stat;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Get schema_guard from sql_ctx for catalog tables
      share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
      if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_ctx or schema_guard is null", K(ret));
      } else {
        schema_guard = ctx.get_sql_ctx()->schema_guard_;
        if (OB_FAIL(batch_write(ctx.get_my_session()->get_effective_tenant_id(),
                                table_param,
                                write_column_params,
                                CREATE_OBJ_PRINT_PARAM(ctx.get_my_session()),
                                *schema_guard,
                                conn,
                                write_table_stats,
                                write_column_stats))) {
          LOG_WARN("failed to batch write external stats",
                   K(ret),
                   K(idx_tab_stat),
                   K(idx_col_stat));
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::batch_write(const uint64_t tenant_id,
                                         const ObCatalogTableStatParam &table_param,
                                         const ObIArray<ObCatalogColumnStatParam> &column_params,
                                         const ObObjPrintParams &print_params,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         sqlclient::ObISQLConnection *conn,
                                         ObIArray<share::ObOptCatalogTableStat *> &table_stats,
                                         ObIArray<share::ObOptCatalogColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  // Use ObOptStatManager singleton to get initialized external stat SQL service
  // This aligns with internal table statistics writing flow
  ObOptCatalogStatSqlService &sql_service
      = ObOptStatManager::get_instance().get_cat_stat_service().get_sql_service();
  LOG_TRACE("batch_write start", K(table_stats.count()), K(column_stats.count()), K(table_param));
  // Write table statistics using catalog table SQL service
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    if (OB_ISNULL(table_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog table stat is null", K(ret), K(i));
    } else if (OB_FAIL(sql_service.update_catalog_table_stat(tenant_id,
                                                             table_stats.at(i),
                                                             table_param,
                                                             conn))) {
      LOG_WARN("failed to update catalog table stat", K(ret), K(i));
    }
  }

  // Write column statistics using catalog table SQL service
  if (OB_SUCC(ret) && !column_stats.empty()) {
    ObIAllocator *allocator = table_param.allocator_;
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_FAIL(sql_service.update_catalog_column_stat(tenant_id,
                                                              column_stats,
                                                              column_params,
                                                              table_param,
                                                              print_params,
                                                              schema_guard,
                                                              *allocator,
                                                              conn))) {
      LOG_WARN("failed to update external column stats", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::get_current_opt_stats(
    ObIAllocator &allocator,
    sqlclient::ObISQLConnection *conn,
    const ObCatalogTableStatParam &param,
    ObIArray<share::ObOptCatalogTableStat *> &table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> partition_names;
  ObSEArray<ObString, 4> column_names;

  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", K(ret));
  } else if (OB_FAIL(get_part_names_and_column_names(param, partition_names, column_names))) {
    LOG_WARN("failed to get part names and column names", K(ret));
  } else if (OB_FAIL(init_stats_by_part_names_and_col_names(param,
                                                            partition_names,
                                                            column_names,
                                                            allocator,
                                                            table_stats,
                                                            column_stats))) {
    LOG_WARN("failed to init stats by partition names and column names", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObOptCatalogStatSqlService &sql_service
        = ObOptStatManager::get_instance().get_cat_stat_service().get_sql_service();
    if (OB_FAIL(sql_service.batch_fetch_table_stats(param.table_identity_.tenant_id_,
                                                    param.table_identity_.catalog_id_,
                                                    param.table_identity_.db_name_,
                                                    param.table_identity_.tab_name_,
                                                    partition_names,
                                                    table_stats,
                                                    conn))) {
      LOG_WARN("failed to fetch catalog table stats from system table", K(ret));
    } else if (OB_FAIL(sql_service.fetch_column_stat(param.table_identity_.tenant_id_,
                                                     param.table_identity_.catalog_id_,
                                                     param.table_identity_.db_name_,
                                                     param.table_identity_.tab_name_,
                                                     partition_names,
                                                     column_names,
                                                     allocator,
                                                     column_stats,
                                                     conn))) {
      LOG_WARN("failed to fetch catalog column stats from system table", K(ret));
    }
  }

  return ret;
}

int ObDbmsCatalogStatsUtils::get_part_names_and_column_names(
    const ObCatalogTableStatParam &table_param,
    ObIArray<ObString> &part_names,
    ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  // Collect partition names from table_param
  for (int64_t i = 0; OB_SUCC(ret) && i < table_param.part_infos_.count(); ++i) {
    if (table_param.part_infos_.at(i).partition_.empty()) {
      // Skip global partition value when preparing partition-level query.
    } else if (OB_FAIL(part_names.push_back(table_param.part_infos_.at(i).partition_))) {
      LOG_WARN("failed to push back partition name", K(ret));
    }
  }
  // Collect column names from table_param
  for (int64_t i = 0; OB_SUCC(ret) && i < table_param.column_params_.count(); ++i) {
    if (OB_FAIL(column_names.push_back(table_param.column_params_.at(i).column_name_))) {
      LOG_WARN("failed to push back column name", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::init_stats_by_part_names_and_col_names(
    const ObCatalogTableStatParam &param,
    const ObIArray<ObString> &partition_names,
    const ObIArray<ObString> &column_names,
    ObIAllocator &allocator,
    ObIArray<share::ObOptCatalogTableStat *> &table_stats,
    ObIArray<share::ObOptCatalogColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  if (partition_names.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition names which should not be empty", K(ret));
  } else if (OB_FAIL(table_stats.prepare_allocate(partition_names.count()))) {
    LOG_WARN("failed to prepare allocate table stats", K(ret));
  } else if (OB_FAIL(
                 column_stats.prepare_allocate(partition_names.count() * column_names.count()))) {
    LOG_WARN("failed to prepare allocate column stats", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_names.count(); ++i) {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptCatalogTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      table_stats.at(i) = new (ptr) ObOptCatalogTableStat();
      table_stats.at(i)->set_tenant_id(param.table_identity_.tenant_id_);
      table_stats.at(i)->set_catalog_id(param.table_identity_.catalog_id_);
      table_stats.at(i)->set_database_name(param.table_identity_.db_name_);
      table_stats.at(i)->set_table_name(param.table_identity_.tab_name_);
      table_stats.at(i)->set_partition_value(partition_names.at(i));
      for (int64_t j = 0; OB_SUCC(ret) && j < column_names.count(); ++j) {
        void *ptr1 = NULL;
        int64_t idx = i * column_names.count() + j;
        if (OB_UNLIKELY(idx >= column_stats.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(idx), K(column_stats.count()));
        } else if (OB_ISNULL(column_stats.at(idx)
                             = ObOptCatalogColumnStat::malloc_new_column_stat(allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(ptr1), K(column_stats.at(idx)));
        } else {
          column_stats.at(idx)->set_tenant_id(param.table_identity_.tenant_id_);
          column_stats.at(idx)->set_catalog_id(param.table_identity_.catalog_id_);
          column_stats.at(idx)->set_database_name(param.table_identity_.db_name_);
          column_stats.at(idx)->set_table_name(param.table_identity_.tab_name_);
          column_stats.at(idx)->set_partition_value(partition_names.at(i));
          column_stats.at(idx)->set_column_name(column_names.at(j));
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::prepare_gather_stat_param(const ObCatalogTableStatParam &param,
                                                       const StatLevel stat_level,
                                                       const bool is_split_gather,
                                                       const int64_t gather_vectorize,
                                                       ObOptCatalogStatGatherParam &gather_param)
{
  int ret = OB_SUCCESS;
  gather_param.table_identity_.tenant_id_ = param.table_identity_.tenant_id_;
  gather_param.table_identity_.catalog_id_ = param.table_identity_.catalog_id_;
  gather_param.table_identity_.catalog_name_ = param.table_identity_.catalog_name_;
  gather_param.table_identity_.db_name_ = param.table_identity_.db_name_;
  gather_param.table_identity_.tab_name_ = param.table_identity_.tab_name_;
  gather_param.stat_level_ = stat_level;
  if (stat_level == PARTITION_LEVEL) {
    if (OB_FAIL(gather_param.part_cols_.assign(param.part_cols_))) {
      LOG_WARN("failed to assign part cols", K(ret));
    }
    gather_param.is_specify_partition_ = param.part_infos_.count() != param.all_part_infos_.count();
  } else if (stat_level == TABLE_LEVEL) {
    // do nothing
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(gather_param.sample_info_.assign(param.gather_options_.sample_info_))) {
    LOG_WARN("failed to assign gather sample info", K(ret));
  }
  gather_param.degree_ = param.gather_options_.degree_;
  gather_param.max_duration_time_ = param.gather_options_.duration_time_;
  gather_param.allocator_ = param.allocator_;
  gather_param.gather_start_time_ = ObTimeUtility::current_time();
  gather_param.stattype_ = param.gather_options_.stattype_;
  gather_param.is_split_gather_ = is_split_gather;
  gather_param.need_approx_ndv_ = param.gather_options_.need_approx_ndv_;
  gather_param.gather_vectorize_ = gather_vectorize;
  gather_param.external_info_.location_ = param.external_info_.location_;
  gather_param.external_info_.access_info_ = param.external_info_.access_info_;
  gather_param.external_info_.format_type_ = param.external_info_.format_type_;
  gather_param.external_info_.lake_table_format_ = param.external_info_.lake_table_format_;
  // gather_param.is_async_gather_ = param.is_async_gather_;
  // gather_param.async_full_table_size_ = param.async_full_table_size_;
  // gather_param.is_auto_sample_size_ = param.is_auto_sample_size_;
  // gather_param.auto_sample_row_cnt_ = param.auto_sample_row_cnt_;
  gather_param.need_refine_min_max_ = param.gather_options_.need_refine_min_max_;
  // gather_param.part_level_ = param.part_level_;  // Not supported for catalog tables
  gather_param.consumer_group_id_ = param.gather_options_.consumer_group_id_;
  // gather_param.use_part_derive_global_ = param.use_part_derive_global_;
  if (OB_SUCC(ret) && OB_FAIL(gather_param.column_params_.assign(param.column_params_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::merge_split_gather_tab_stats(
    ObIArray<ObOptCatalogTableStat *> &all_tstats,
    ObIArray<ObOptCatalogTableStat *> &cur_all_tstats)
{
  int ret = OB_SUCCESS;
  if (all_tstats.empty()) {
    if (OB_FAIL(all_tstats.assign(cur_all_tstats))) {
      LOG_WARN("failed to assign");
    } else {
      cur_all_tstats.reset();
    }
  } else if (OB_UNLIKELY(all_tstats.count() != cur_all_tstats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(all_tstats), K(cur_all_tstats));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tstats.count(); ++i) {
      if (OB_ISNULL(all_tstats.at(i)) || OB_ISNULL(cur_all_tstats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(all_tstats.at(i)), K(cur_all_tstats.at(i)));
      } else if (!all_tstats.at(i)->is_valid() && cur_all_tstats.at(i)->is_valid()) {
        *all_tstats.at(i) = *cur_all_tstats.at(i);
        cur_all_tstats.at(i)->~ObOptCatalogTableStat();
        cur_all_tstats.at(i) = NULL;
      } else if (OB_UNLIKELY(
                     all_tstats.at(i)->get_catalog_id() != cur_all_tstats.at(i)->get_catalog_id()
                     || all_tstats.at(i)->get_database_name()
                            != cur_all_tstats.at(i)->get_database_name()
                     || all_tstats.at(i)->get_table_name() != cur_all_tstats.at(i)->get_table_name()
                     || all_tstats.at(i)->get_partition_value()
                            != cur_all_tstats.at(i)->get_partition_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(all_tstats.at(i)), K(cur_all_tstats.at(i)));
      } else {
        int64_t old_row_cnt = all_tstats.at(i)->get_row_count();
        int64_t cur_row_cnt = cur_all_tstats.at(i)->get_row_count();
        int64_t merged_row_cnt = std::max(old_row_cnt, cur_row_cnt);
        all_tstats.at(i)->set_row_count(merged_row_cnt);
        all_tstats.at(i)->set_file_num(
            std::max(all_tstats.at(i)->get_file_num(), cur_all_tstats.at(i)->get_file_num()));
        all_tstats.at(i)->set_data_size(
            std::max(all_tstats.at(i)->get_data_size(), cur_all_tstats.at(i)->get_data_size()));
        all_tstats.at(i)->set_last_analyzed(std::max(all_tstats.at(i)->get_last_analyzed(),
                                                     cur_all_tstats.at(i)->get_last_analyzed()));
        const int64_t old_sample_sz = all_tstats.at(i)->get_sample_size();
        const int64_t cur_sample_sz = cur_all_tstats.at(i)->get_sample_size();
        const int64_t merged_sample_sz = std::max(old_sample_sz, cur_sample_sz);
        all_tstats.at(i)->set_sample_size(merged_sample_sz);
        all_tstats.at(i)->set_avg_row_len(all_tstats.at(i)->get_avg_row_len()
                                          + cur_all_tstats.at(i)->get_avg_row_len());
        all_tstats.at(i)->set_schema_version(std::max(all_tstats.at(i)->get_schema_version(),
                                                      cur_all_tstats.at(i)->get_schema_version()));
        // free memory
        cur_all_tstats.at(i)->~ObOptCatalogTableStat();
        cur_all_tstats.at(i) = NULL;
      }
    }
    cur_all_tstats.reset();
  }
  return ret;
}

bool ObDbmsCatalogStatsUtils::find_part(const ObIArray<ObCatalogExtPartitionInfo> &part_infos,
                                        const ObString &part_name,
                                        bool is_sensitive_compare,
                                        ObCatalogExtPartitionInfo &part)
{
  bool found = false;
  for (int64_t i = 0; !found && i < part_infos.count(); ++i) {
    if ((is_sensitive_compare
         && ObCharset::case_sensitive_equal(part_name, part_infos.at(i).partition_))
        || (!is_sensitive_compare
            && ObCharset::case_insensitive_equal(part_name, part_infos.at(i).partition_))) {
      part = part_infos.at(i);
      found = true;
    }
  }
  return found;
}

int ObDbmsCatalogStatsUtils::sync_column_stat_last_analyzed(
    const ObIArray<share::ObOptCatalogTableStat *> &all_tstats,
    ObIArray<share::ObOptCatalogColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;

  // Find the latest last_analyzed from all partitions and global table stat
  int64_t latest_last_analyzed = 0;
  int64_t global_tstat_last_analyzed = 0;
  for (int64_t i = 0; i < all_tstats.count(); ++i) {
    if (OB_NOT_NULL(all_tstats.at(i))) {
      const ObString &part_val = all_tstats.at(i)->get_partition_value();
      int64_t cur_last_analyzed = all_tstats.at(i)->get_last_analyzed();
      if (!part_val.empty()) {
        // Partition stats
        if (cur_last_analyzed > latest_last_analyzed) {
          latest_last_analyzed = cur_last_analyzed;
        }
      } else {
        // Global table stat
        global_tstat_last_analyzed = cur_last_analyzed;
      }
    }
  }

  // Sync column stats last_analyzed with corresponding table stats
  for (int64_t i = 0; i < all_cstats.count(); ++i) {
    if (OB_NOT_NULL(all_cstats.at(i))) {
      const ObString &col_part = all_cstats.at(i)->get_partition_value();
      if (col_part.empty()) {
        // Global stats (empty partition_value)
        // Prefer latest partition's time, fallback to global table stat's time
        if (latest_last_analyzed > 0) {
          all_cstats.at(i)->set_last_analyzed(latest_last_analyzed);
        } else if (global_tstat_last_analyzed > 0) {
          all_cstats.at(i)->set_last_analyzed(global_tstat_last_analyzed);
        }
      } else {
        // Partition stats, match with corresponding table stat
        for (int64_t j = 0; j < all_tstats.count(); ++j) {
          if (OB_NOT_NULL(all_tstats.at(j))
              && ObCharset::case_sensitive_equal(col_part,
                                                 all_tstats.at(j)->get_partition_value())) {
            all_cstats.at(i)->set_last_analyzed(all_tstats.at(j)->get_last_analyzed());
            break;
          }
        }
      }
    }
  }

  return ret;
}

int ObDbmsCatalogStatsUtils::filter_nonpart_table_by_modify_time_(
    const ObCatalogTableStatParam &param,
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    bool &is_continue_collect)
{
  int ret = OB_SUCCESS;
  is_continue_collect = true;
  ObOptCatalogStatSqlService &sql_service
      = ObOptStatManager::get_instance().get_cat_stat_service().get_sql_service();
  ObArray<share::ObOptCatalogTableStat *> existing_table_stats;
  ObArray<share::ObOptCatalogColumnStat *> existing_column_stats;
  // Remote time unit is `second`.
  const int64_t remote_modify_time = param.global_modified_ts_;
  if (OB_FAIL(existing_table_stats.prepare_allocate(1))) {
    LOG_WARN("failed to prepare allocate non-part table stats", K(ret));
  } else {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptCatalogTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      existing_table_stats.at(0) = new (ptr) ObOptCatalogTableStat();
      existing_table_stats.at(0)->set_tenant_id(param.table_identity_.tenant_id_);
      existing_table_stats.at(0)->set_catalog_id(param.table_identity_.catalog_id_);
      existing_table_stats.at(0)->set_database_name(param.table_identity_.db_name_);
      existing_table_stats.at(0)->set_table_name(param.table_identity_.tab_name_);
      // Non-partitioned table uses empty partition_value
      existing_table_stats.at(0)->set_partition_value(ObString::make_empty_string());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_service.batch_fetch_table_stats(param.table_identity_.tenant_id_,
                                                         param.table_identity_.catalog_id_,
                                                         param.table_identity_.db_name_,
                                                         param.table_identity_.tab_name_,
                                                         ObSEArray<ObString, 1>(),
                                                         existing_table_stats,
                                                         trans.get_connection()))) {
    LOG_TRACE("failed to fetch existing stats, will proceed with collection", K(ret));
    ret = OB_SUCCESS;
  } else if (existing_table_stats.count() > 0 && OB_NOT_NULL(existing_table_stats.at(0))) {
    int64_t local_last_analyzed = existing_table_stats.at(0)->get_last_analyzed();
    if (local_last_analyzed == 0 && remote_modify_time == 0) {
      // do nothing, continue to collect, because the table never been collected yet.
    } else if (remote_modify_time > local_last_analyzed) {
      LOG_TRACE("non-partitioned table needs collection",
                K(remote_modify_time),
                K(local_last_analyzed));
    } else if (remote_modify_time == local_last_analyzed) {
      is_continue_collect = false;
      LOG_TRACE("non-partitioned table data unchanged, skipping collection", K(remote_modify_time));
      const_cast<ObCatalogTableStatParam &>(param).part_infos_.reset();
    } else {
      is_continue_collect = false;
      LOG_WARN("anomaly: remote modify_time < local_last_analyzed, skipping collection",
               K(remote_modify_time),
               K(local_last_analyzed),
               K(param.table_identity_.tab_name_));
      const_cast<ObCatalogTableStatParam &>(param).part_infos_.reset();
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::filter_part_table_by_modify_time_(
    ObMySQLTransaction &trans,
    ObIAllocator &allocator,
    ObCatalogTableStatParam &param,
    bool &is_continue_collect,
    ObArray<ObString> &partitions_to_delete)
{
  int ret = OB_SUCCESS;
  is_continue_collect = true;
  if (!param.part_infos_.empty()) {
    ObOptCatalogStatSqlService &sql_service
        = ObOptStatManager::get_instance().get_cat_stat_service().get_sql_service();
    ObArray<share::ObOptCatalogTableStat *> existing_table_stats;
    ObArray<share::ObOptCatalogColumnStat *> existing_column_stats;
    ObArray<ObString> partition_names;
    ObArray<ObString> column_names;

    if (OB_FAIL(get_part_names_and_column_names(param, partition_names, column_names))) {
      LOG_WARN("failed to get part names and column names", K(ret));
    } else if (OB_FAIL(init_stats_by_part_names_and_col_names(param,
                                                              partition_names,
                                                              column_names,
                                                              allocator,
                                                              existing_table_stats,
                                                              existing_column_stats))) {
      LOG_WARN("failed to init stats by part names and column names", K(ret));
    } else if (OB_FAIL(sql_service.batch_fetch_table_stats(param.table_identity_.tenant_id_,
                                                           param.table_identity_.catalog_id_,
                                                           param.table_identity_.db_name_,
                                                           param.table_identity_.tab_name_,
                                                           partition_names,
                                                           existing_table_stats,
                                                           trans.get_connection()))) {
      LOG_WARN("failed to fetch existing partition stats, will proceed with collection", K(ret));
    } else {
      ObArray<ObCatalogExtPartitionInfo> partitions_to_collect;
      ObArray<share::ObOptCatalogTableStat *> unchanged_partition_stats;
      if (OB_FAIL(filter_partitions_by_freshness(param.external_info_.lake_table_format_,
                                                 param.part_infos_,
                                                 existing_table_stats,
                                                 allocator,
                                                 partitions_to_collect,
                                                 partitions_to_delete,
                                                 unchanged_partition_stats))) {
        LOG_WARN("failed to filter partitions by freshness", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < unchanged_partition_stats.count(); ++i) {
          share::ObOptCatalogTableStat *stat = unchanged_partition_stats.at(i);
          if (OB_ISNULL(stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null stat in unchanged list", K(ret), K(i));
          } else {
            param.filtered_stats_.filtered_file_count_ += stat->get_file_num();
            param.filtered_stats_.filtered_data_size_ += stat->get_data_size();
            param.filtered_stats_.filtered_last_analyzed_
                = std::max(param.filtered_stats_.filtered_last_analyzed_, stat->get_last_analyzed());
            param.filtered_stats_.filtered_schema_version_
                = std::max(param.filtered_stats_.filtered_schema_version_, stat->get_schema_version());
          }
        }
        if (OB_SUCC(ret)) {
          param.part_infos_.reset();
          if (OB_FAIL(param.part_infos_.assign(partitions_to_collect))) {
            LOG_WARN("failed to assign partitions to collect", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (param.part_infos_.empty()) {
        is_continue_collect = false;
      }
    }
  } else {
    is_continue_collect = false;
    LOG_INFO("no partitions to gather", K(ret), K(is_continue_collect));
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::filter_catalog_partition_infos_by_modify_time(
    sql::ObExecContext &ctx,
    ObCatalogTableStatParam &param,
    bool &is_continue_collect,
    ObArray<ObString> &partitions_to_delete)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.table_identity_.tenant_id_))) {
      LOG_WARN("failed to start transaction", K(ret));
    } else {
      ObArenaAllocator allocator("CatFilterStats", OB_MALLOC_NORMAL_BLOCK_SIZE, param.table_identity_.tenant_id_);
      if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
        // Non-partitioned table: use existing logic, no partitions to delete
        if (OB_FAIL(filter_nonpart_table_by_modify_time_(param,
                                                         trans,
                                                         allocator,
                                                         is_continue_collect))) {
          LOG_WARN("failed to filter non-part table by modify time", K(ret));
        }
      } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ONE) {
        // Partitioned table: use new freshness checker
        if (OB_FAIL(filter_part_table_by_modify_time_(trans,
                                                      allocator,
                                                      param,
                                                      is_continue_collect,
                                                      partitions_to_delete))) {
          LOG_WARN("failed to filter part table", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("incorrect part level", K(ret), K(param.part_level_));
      }

      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to end transaction", K(tmp_ret), K(ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsUtils::filter_partitions_by_freshness(
    const share::ObLakeTableFormat &lake_table_format,
    const ObIArray<ObCatalogExtPartitionInfo> &new_all_partitions,
    const ObIArray<share::ObOptCatalogTableStat *> &existing_stats,
    ObIAllocator &allocator,
    ObArray<ObCatalogExtPartitionInfo> &partitions_to_collect,
    ObArray<ObString> &partitions_to_delete,
    ObIArray<share::ObOptCatalogTableStat *> &unchanged_partition_stats)
{
  int ret = OB_SUCCESS;
  ObICatalogStatsFreshnessChecker *checker = nullptr;

  // Create checker based on catalog type
  if (share::ObLakeTableFormat::INVALID == lake_table_format) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lake table format is invalid", K(ret));
  } else if (share::ObLakeTableFormat::HIVE == lake_table_format) {
    checker = OB_NEWx(ObHMSCatalogStatsFreshnessChecker, &allocator);
  } else if (share::ObLakeTableFormat::ICEBERG == lake_table_format) {
    checker = OB_NEWx(ObIcebergStatsFreshnessChecker, &allocator);
  } else if (share::ObLakeTableFormat::ODPS == lake_table_format) {
    checker = OB_NEWx(ObOdpsCatalogStatsFreshnessChecker, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported catalog type", K(ret), K(lake_table_format));
  }

  if (OB_SUCC(ret) && OB_ISNULL(checker)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate checker", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(checker->filter_partitions_need_collect(new_all_partitions,
                                                             existing_stats,
                                                             partitions_to_collect,
                                                             partitions_to_delete,
                                                             unchanged_partition_stats))) {
    LOG_WARN("failed to filter partitions", K(ret));
  }

  // Cleanup
  if (OB_NOT_NULL(checker)) {
    checker->~ObICatalogStatsFreshnessChecker();
    allocator.free(checker);
    checker = NULL;
  }

  return ret;
}

int ObDbmsCatalogStatsUtils::delete_stale_partition_stats(
    sql::ObExecContext &ctx,
    const ObCatalogTableStatParam &param,
    const ObIArray<ObString> &partitions_to_delete)
{
  int ret = OB_SUCCESS;
  if (partitions_to_delete.empty()) {
    // Nothing to delete
  } else if (OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.table_identity_.tenant_id_))) {
      LOG_WARN("failed to start transaction", K(ret));
    } else {
      ObOptCatalogStatSqlService &sql_service
          = ObOptStatManager::get_instance().get_cat_stat_service().get_sql_service();

      // Delete table stats for deleted partitions
      if (OB_FAIL(sql_service.batch_delete_table_stats(param.table_identity_.tenant_id_,
                                                        param.table_identity_.catalog_id_,
                                                        param.table_identity_.db_name_,
                                                        param.table_identity_.tab_name_,
                                                        partitions_to_delete,
                                                        trans.get_connection()))) {
        LOG_WARN("failed to delete table stats", K(ret));
      }

      // Delete column stats for deleted partitions
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_service.batch_delete_column_stats(param.table_identity_.tenant_id_,
                                                           param.table_identity_.catalog_id_,
                                                           param.table_identity_.db_name_,
                                                           param.table_identity_.tab_name_,
                                                           partitions_to_delete,
                                                           trans.get_connection()))) {
          LOG_WARN("failed to delete column stats", K(ret));
        }
      }

      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to end transaction", K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }

  LOG_INFO("delete stale partition stats",
           K(ret),
           K(param.table_identity_.db_name_),
           K(param.table_identity_.tab_name_),
           K(partitions_to_delete.count()));
  return ret;
}

} // namespace common
} // namespace oceanbase