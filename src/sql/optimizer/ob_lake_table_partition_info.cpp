/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "ob_lake_table_partition_info.h"

#include "plugin/v2/external_table/ob_ext_table_metadata.h"
#include "share/external_table/ob_external_table_part_info.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/location_cache/ob_location_service.h"
#include "share/object/ob_obj_cast.h"
#include "share/schema/ob_iceberg_table_schema.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/engine/basic/ob_consistent_hashing_load_balancer.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/file_prune/ob_ext_file_pruner.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"
#include "sql/optimizer/file_prune/ob_lake_table_optimizer_utils.h"
#include "sql/table_format/common/utils/ob_lake_table_executor.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::iceberg;

namespace sql
{

namespace {
struct ParallelIcebergManifestPruneTask
{
  ParallelIcebergManifestPruneTask(ObIcebergFilePrunner *iceberg_file_pruner,
                                  ObIArray<iceberg::ManifestFile*> &all_manifest_files,
                                  ObSEArray<bool, 16> &in_bound_array,
                                  const ObString &access_info,
                                  const int64_t begin,
                                  const int64_t end)
    : iceberg_file_pruner_(iceberg_file_pruner),
      all_manifest_files_(all_manifest_files),
      in_bound_array_(in_bound_array),
      access_info_(access_info),
      begin_(begin),
      end_(end)
  {}

  int operator()() const
  {
    int ret = OB_SUCCESS;
    for (int64_t i = begin_; OB_SUCC(ret) && i < end_; ++i) {
      bool in_bound = false;
      iceberg::ManifestFile *manifest_file = all_manifest_files_.at(i);
      if (OB_ISNULL(manifest_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null manifest file");
      } else if (OB_FAIL(iceberg_file_pruner_->prune_single_manifest_file(manifest_file, in_bound))) {
        LOG_WARN("failed to prune manifest files", K(ret));
      } else if (in_bound) {
        if (OB_FAIL(manifest_file->load_manifest_entry(access_info_))) {
          LOG_WARN("failed to load manifest entries", K(ret));
        }
      }
      in_bound_array_.at(i) = in_bound;
    }
    return ret;
  }

private:
  ObIcebergFilePrunner *iceberg_file_pruner_;
  ObIArray<iceberg::ManifestFile*> &all_manifest_files_;
  ObSEArray<bool, 16> &in_bound_array_;
  const ObString &access_info_;
  int64_t begin_;
  int64_t end_;
};
}

ObLakeTablePartitionInfo::~ObLakeTablePartitionInfo()
{
}

int ObLakeTablePartitionInfo::assign(const ObTablePartitionInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObLakeTablePartitionInfo &info = static_cast<const ObLakeTablePartitionInfo&>(other);
    if (OB_FAIL(ObTablePartitionInfo::assign(other))) {
      LOG_WARN("failed to assign table partition info");
    } else if (OB_ISNULL(info.file_pruner_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null file pruner");
    } else if (OB_FAIL(info.file_pruner_->clone(allocator_, file_pruner_))) {
      LOG_WARN("failed to clone file pruner");
    } else if (OB_FAIL(iceberg_file_descs_.assign(info.iceberg_file_descs_))) {
      LOG_WARN("failed to assign iceberg file descs");
    } else {
      is_hash_aggregate_ = info.is_hash_aggregate_;
      hash_count_ = info.hash_count_;
      first_bucket_partition_value_offset_ = info.first_bucket_partition_value_offset_;
    }
  }

  return ret;
}

int get_manifest_entries(const ObString &access_info,
                         ObIArray<iceberg::ManifestFile*> &manifest_files,
                         ObIArray<iceberg::ManifestEntry*> &manifest_entries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_files.count(); ++i) {
    if (OB_ISNULL(manifest_files.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else if (OB_FAIL(manifest_files.at(i)->get_manifest_entries(access_info, manifest_entries))) {
      LOG_WARN("failed to get manifest entries");
    }
  }
  return ret;
}

// 只保留扫描实际访问、且在保留 spec 中采用 Identity 变换的 source field。
static int get_accessed_identity_source_field_ids(const ObIArray<ObRawExpr *> &file_column_exprs,
                                                  const ObIArray<ObIcebergFileDesc *> &file_descs,
                                                  const iceberg::TableMetadata &table_metadata,
                                                  ObIArray<int32_t> &identity_source_field_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<int32_t, 16> accessed_source_field_ids;
  ObSEArray<int32_t, 8> visited_spec_ids;
  identity_source_field_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); ++i) {
    const ObRawExpr *expr = file_column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null external file column expr", K(ret), K(i));
    } else if (T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()
               || T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type()) {
      const uint64_t source_field_id = expr->get_column_idx();
      if (OB_UNLIKELY(source_field_id > INT32_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid iceberg source field id", K(ret), K(source_field_id), K(i));
      } else if (OB_FAIL(add_var_to_array_no_dup(accessed_source_field_ids,
                                                 static_cast<int32_t>(source_field_id)))) {
        LOG_WARN("failed to add accessed source field id", K(ret), K(source_field_id));
      }
    }
  }
  // 每个 spec 只处理一次；已找齐全部访问字段或覆盖全部 metadata spec 后即可提前结束。
  bool all_specs_visited = false;
  for (int64_t i = 0; OB_SUCC(ret) && !accessed_source_field_ids.empty() && !all_specs_visited
                      && identity_source_field_ids.count() < accessed_source_field_ids.count()
                      && i < file_descs.count();
       ++i) {
    const ObIcebergFileDesc *file_desc = file_descs.at(i);
    if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null iceberg file desc", K(ret), K(i), KP(file_desc));
    } else {
      const int32_t spec_id = file_desc->entry_->partition_spec_id;
      if (!has_exist_in_array(visited_spec_ids, spec_id)) {
        const iceberg::PartitionSpec *partition_spec = nullptr;
        if (OB_FAIL(visited_spec_ids.push_back(spec_id))) {
          LOG_WARN("failed to add retained partition spec id", K(ret), K(i), K(spec_id));
        } else if (OB_FAIL(table_metadata.get_partition_spec(spec_id, partition_spec))) {
          LOG_WARN("failed to get retained partition spec", K(ret), K(spec_id));
        } else if (OB_ISNULL(partition_spec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null retained partition spec", K(ret), K(spec_id));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < partition_spec->fields.count(); ++j) {
            const iceberg::PartitionField *field = partition_spec->fields.at(j);
            if (OB_ISNULL(field)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null partition field", K(ret), K(spec_id), K(j));
            } else if (iceberg::TransformType::Identity == field->transform.transform_type
                       && has_exist_in_array(accessed_source_field_ids, field->source_id)
                       && OB_FAIL(
                           add_var_to_array_no_dup(identity_source_field_ids, field->source_id))) {
              LOG_WARN("failed to add identity source field id", K(ret), K(field->source_id));
            }
          }
          all_specs_visited = visited_spec_ids.count() >= table_metadata.partition_specs.count();
        }
      }
    }
  }
  return ret;
}

static int get_file_column_expr_by_source_field_id(const int32_t source_field_id,
                                                   const ObIArray<ObRawExpr *> &file_column_exprs,
                                                   const ObRawExpr *&file_column_expr)
{
  int ret = OB_SUCCESS;
  file_column_expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(file_column_expr) && i < file_column_exprs.count();
       ++i) {
    const ObRawExpr *expr = file_column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null external file column expr", K(ret), K(i));
    } else if ((T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()
                || T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type())
               && source_field_id == static_cast<int64_t>(expr->get_column_idx())) {
      file_column_expr = expr;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find iceberg source field expression", K(ret), K(source_field_id));
  }
  return ret;
}

int ObLakeTablePartitionInfo::cast_iceberg_partition_value(const ObObj &source_value,
                                                           const ObRawExprResType &target_type,
                                                           ObIAllocator &allocator,
                                                           ObObj &target_value)
{
  int ret = OB_SUCCESS;
  ObAccuracy target_accuracy = target_type.get_accuracy();
  ObCastCtx cast_ctx(&allocator,
                     nullptr,
                     CM_NONE,
                     target_type.get_collation_type(),
                     &target_accuracy);
  ObObj cast_buffer;
  const ObObj *cast_result = nullptr;
  if (OB_FAIL(ObObjCaster::to_type(target_type.get_type(),
                                   cast_ctx,
                                   source_value,
                                   cast_buffer,
                                   cast_result))) {
    LOG_WARN("failed to cast iceberg partition value", K(ret), K(source_value), K(target_type));
  } else if (OB_ISNULL(cast_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null iceberg partition cast result", K(ret), K(source_value), K(target_type));
  } else {
    target_value = *cast_result;
  }
  return ret;
}

int ObLakeTablePartitionInfo::build_iceberg_partition_row(
    const iceberg::PartitionSpec &partition_spec,
    const iceberg::ManifestEntry &manifest_entry,
    const ObIArray<int32_t> &source_field_ids,
    const ObIArray<ObRawExpr *> &file_column_exprs,
    ObIAllocator &cast_allocator,
    ObIAllocator &partition_info_allocator,
    ObNewRow &partition_row)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObj, 8> identity_values;
  const ObIArray<ObObj> &partition_values = manifest_entry.data_file.partition;
  if (OB_UNLIKELY(partition_spec.fields.count() != partition_values.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition field and value count mismatch",
             K(ret),
             K(partition_spec.fields.count()),
             K(partition_values.count()));
  } else if (OB_FAIL(identity_values.reserve(source_field_ids.count() * 2))) {
    LOG_WARN("failed to reserve identity partition values", K(ret));
  }
  // 以 (source field id, value) 紧凑存储；某个 spec 无映射时仍回退读取文件列。
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_spec.fields.count(); ++i) {
    const iceberg::PartitionField *field = partition_spec.fields.at(i);
    if (OB_ISNULL(field)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition field", K(ret), K(i));
    } else if (iceberg::TransformType::Identity == field->transform.transform_type
               && has_exist_in_array(source_field_ids, field->source_id)) {
      const ObRawExpr *file_column_expr = nullptr;
      ObObj source_id;
      ObObj partition_value;
      source_id.set_int32(field->source_id);
      if (OB_FAIL(get_file_column_expr_by_source_field_id(field->source_id,
                                                          file_column_exprs,
                                                          file_column_expr))) {
        LOG_WARN("failed to get iceberg source field expression", K(ret), K(field->source_id));
      } else if (OB_FAIL(cast_iceberg_partition_value(partition_values.at(i),
                                                      file_column_expr->get_result_type(),
                                                      cast_allocator,
                                                      partition_value))) {
        LOG_WARN("failed to normalize iceberg partition value", K(ret), K(i), K(field->source_id));
      } else if (OB_FAIL(identity_values.push_back(source_id))) {
        LOG_WARN("failed to push identity source id", K(ret), K(i));
      } else if (OB_FAIL(identity_values.push_back(partition_value))) {
        LOG_WARN("failed to push identity partition value", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (identity_values.empty()) {
      partition_row.reset();
    } else {
      ObNewRow tmp_row;
      tmp_row.assign(identity_values.get_data(), identity_values.count());
      if (OB_FAIL(ob_write_row(partition_info_allocator, tmp_row, partition_row))) {
        LOG_WARN("failed to copy identity partition row", K(ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::build_iceberg_part_ids(ObLakeTablePartKeyMap &part_key_map,
                                                     ObIArray<ObIcebergFileDesc *> &file_descs)
{
  int ret = OB_SUCCESS;
  uint64_t next_part_idx = 0;
  if (file_descs.empty()) {
  } else {
    // 避免按文件数预分配 bucket；唯一分区增多时 Map 会按 2 倍扩容。
    const int64_t initial_bucket_count = file_descs.count() < 64 ? file_descs.count() : 64;
    if (OB_FAIL(part_key_map.create(initial_bucket_count, "PartKeyMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create partition key map", K(ret), K(initial_bucket_count));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
    ObIcebergFileDesc *file_desc = file_descs.at(i);
    ObLakeTablePartKey part_key;
    uint64_t part_idx = OB_INVALID_ID;
    if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null iceberg file desc", K(ret), K(i));
    } else if (OB_FAIL(part_key.from_manifest_entry(file_desc->entry_))) {
      LOG_WARN("failed to build partition key", K(ret), K(i));
    } else if (OB_FAIL(part_key_map.get_refactored(part_key, part_idx))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
        part_idx = next_part_idx++;
        if (OB_FAIL(part_key_map.set_refactored(part_key, part_idx))) {
          LOG_WARN("failed to set partition id", K(ret), K(part_idx));
        }
      } else {
        LOG_WARN("failed to get partition id", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      file_desc->part_idx_ = part_idx;
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::update_iceberg_file_part_ids()
{
  int ret = OB_SUCCESS;
  int64_t updated_file_count = 0;
  ObCandiTabletLocIArray &tablet_locs = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_locs.count(); ++i) {
    ObIArray<ObIOptLakeTableFile *> &files
        = tablet_locs.at(i).get_opt_lake_table_files_for_update();
    for (int64_t j = 0; OB_SUCC(ret) && j < files.count(); ++j) {
      ObIOptLakeTableFile *file = files.at(j);
      if (OB_ISNULL(file) || OB_UNLIKELY(!file->is_iceberg_file())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid iceberg file", K(ret), K(i), K(j), KP(file));
      } else {
        ObOptIcebergFile *iceberg_file = static_cast<ObOptIcebergFile *>(file);
        const int64_t file_desc_idx = iceberg_file->file_desc_idx_;
        if (OB_UNLIKELY(file_desc_idx < 0 || file_desc_idx >= iceberg_file_descs_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid iceberg file descriptor index", K(ret), K(file_desc_idx));
        } else {
          const ObIcebergFileDesc *file_desc = iceberg_file_descs_.at(file_desc_idx);
          if (OB_ISNULL(file_desc) || OB_UNLIKELY(OB_INVALID_ID == file_desc->part_idx_)
              || OB_UNLIKELY(file_desc->part_idx_
                             >= static_cast<uint64_t>(iceberg_file_descs_.count()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid iceberg partition id", K(ret), K(file_desc_idx), KP(file_desc));
          } else {
            iceberg_file->part_id_ = static_cast<int64_t>(file_desc->part_idx_);
            ++updated_file_count;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(updated_file_count != iceberg_file_descs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file count mismatch", K(ret), K(updated_file_count), K(iceberg_file_descs_.count()));
  }
  return ret;
}

int ObLakeTablePartitionInfo::prepare_iceberg_partition_infos(
    ObSqlSchemaGuard &sql_schema_guard,
    const ObIArray<ObRawExpr *> &file_column_exprs,
    ObIAllocator &partition_info_allocator,
    share::ObExternalTablePartInfoArray &partition_infos)
{
  int ret = OB_SUCCESS;
  ObILakeTableMetadata *lake_table_metadata = nullptr;
  ObIcebergTableMetadata *iceberg_table_metadata = nullptr;
  ObSEArray<int32_t, 8> identity_source_field_ids;
  ObLakeTablePartKeyMap part_key_map;
  ObArenaAllocator cast_allocator("IcebergPartCast");
  if (OB_UNLIKELY(partition_infos.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iceberg partition infos are not empty", K(ret), K(partition_infos.count()));
  } else if (file_column_exprs.empty() || iceberg_file_descs_.empty()) {
    // 没有保留文件列会消费 Identity 值，无需构造共享分区信息。
  } else if (OB_FAIL(sql_schema_guard.get_lake_table_metadata(get_ref_table_id(),
                                                              lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret), K(get_ref_table_id()));
  } else if (OB_ISNULL(lake_table_metadata)
             || ObLakeTableFormat::ICEBERG != lake_table_metadata->get_format_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iceberg table metadata", K(ret), KP(lake_table_metadata));
  } else {
    iceberg_table_metadata = down_cast<ObIcebergTableMetadata *>(lake_table_metadata);
    if (OB_FAIL(get_accessed_identity_source_field_ids(file_column_exprs,
                                                       iceberg_file_descs_,
                                                       iceberg_table_metadata->table_metadata_,
                                                       identity_source_field_ids))) {
      LOG_WARN("failed to get accessed identity source field ids", K(ret));
    }
  }

  // Hash 分桶阶段可能已经生成 part_idx_；Identity 路径优先复用，避免重复去重。
  bool part_ids_ready = true;
  for (int64_t i = 0; OB_SUCC(ret) && !identity_source_field_ids.empty() && part_ids_ready
                      && i < iceberg_file_descs_.count();
       ++i) {
    const ObIcebergFileDesc *file_desc = iceberg_file_descs_.at(i);
    if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null iceberg file desc", K(ret), K(i), KP(file_desc));
    } else {
      part_ids_ready = OB_INVALID_ID != file_desc->part_idx_;
    }
  }
  if (OB_FAIL(ret) || identity_source_field_ids.empty()) {
  } else if (!part_ids_ready
             && OB_FAIL(build_iceberg_part_ids(part_key_map, iceberg_file_descs_))) {
    LOG_WARN("failed to build iceberg partition ids", K(ret));
  } else if (OB_FAIL(update_iceberg_file_part_ids())) {
    LOG_WARN("failed to update iceberg file partition ids", K(ret));
  }

  int64_t partition_count = 0;
  if (OB_SUCC(ret) && !identity_source_field_ids.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < iceberg_file_descs_.count(); ++i) {
      const ObIcebergFileDesc *file_desc = iceberg_file_descs_.at(i);
      if (OB_ISNULL(file_desc) || OB_UNLIKELY(OB_INVALID_ID == file_desc->part_idx_)
          || OB_UNLIKELY(file_desc->part_idx_
                         >= static_cast<uint64_t>(iceberg_file_descs_.count()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid iceberg partition id", K(ret), K(i), KP(file_desc));
      } else {
        const int64_t current_partition_count = static_cast<int64_t>(file_desc->part_idx_) + 1;
        if (current_partition_count > partition_count) {
          partition_count = current_partition_count;
        }
      }
    }
  }

  // 稠密 part_idx_ 与数组下标一致，每个分区只保留一个代表文件来提取分区值。
  ObSEArray<ObIcebergFileDesc *, 16> unique_partition_files;
  if (OB_FAIL(ret) || 0 == partition_count) {
  } else if (OB_FAIL(unique_partition_files.reserve(partition_count))) {
    LOG_WARN("failed to reserve unique partition files", K(ret), K(partition_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_count; ++i) {
    if (OB_FAIL(unique_partition_files.push_back(nullptr))) {
      LOG_WARN("failed to initialize unique partition files", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < iceberg_file_descs_.count(); ++i) {
    ObIcebergFileDesc *file_desc = iceberg_file_descs_.at(i);
    if (OB_ISNULL(file_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null iceberg file desc", K(ret), K(i));
    } else if (partition_count > 0) {
      const int64_t part_idx = static_cast<int64_t>(file_desc->part_idx_);
      if (OB_UNLIKELY(part_idx < 0 || part_idx >= unique_partition_files.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid iceberg partition id", K(ret), K(part_idx));
      } else if (OB_ISNULL(unique_partition_files.at(part_idx))) {
        unique_partition_files.at(part_idx) = file_desc;
      }
    }
  }

  if (OB_FAIL(ret) || 0 == partition_count) {
  } else if (OB_FAIL(partition_infos.reserve(partition_count))) {
    LOG_WARN("failed to reserve iceberg partition infos", K(ret), K(partition_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_partition_files.count(); ++i) {
    ObIcebergFileDesc *file_desc = unique_partition_files.at(i);
    const iceberg::PartitionSpec *partition_spec = nullptr;
    share::ObExternalTablePartInfo part_info;
    if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)
        || OB_UNLIKELY(file_desc->part_idx_ != static_cast<uint64_t>(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unique partition file", K(ret), K(i), KP(file_desc));
    } else if (OB_FAIL(iceberg_table_metadata->table_metadata_.get_partition_spec(
                   file_desc->entry_->partition_spec_id,
                   partition_spec))) {
      LOG_WARN("failed to get partition spec", K(ret), K(file_desc->entry_->partition_spec_id));
    } else if (OB_ISNULL(partition_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition spec", K(ret));
    } else {
      part_info.part_id_ = i;
      cast_allocator.reuse();
      if (OB_FAIL(build_iceberg_partition_row(*partition_spec,
                                              *file_desc->entry_,
                                              identity_source_field_ids,
                                              file_column_exprs,
                                              cast_allocator,
                                              partition_info_allocator,
                                              part_info.list_row_value_))) {
        LOG_WARN("failed to build iceberg partition row", K(ret), K(i));
      } else if (OB_FAIL(partition_infos.set_part_pair_by_idx(i, part_info))) {
        LOG_WARN("failed to set iceberg partition info", K(ret), K(i));
      }
    }
  }
  if (part_key_map.created()) {
    const int tmp_ret = part_key_map.destroy();
    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to destroy part key map", K(ret));
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::prune_file_and_select_location(ObSqlSchemaGuard &sql_schema_guard,
                                                             const ObDMLStmt &stmt,
                                                             ObExecContext *exec_ctx,
                                                             const uint64_t table_id,
                                                             const uint64_t ref_table_id,
                                                             int64_t lake_table_snapshot_id,
                                                             const ObIArray<ObRawExpr*> &filter_exprs)
{
  int ret = OB_SUCCESS;
  ObILakeTableMetadata *lake_table_metadata = nullptr;
  if (OB_FAIL(sql_schema_guard.get_lake_table_metadata(ref_table_id, lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ref_table_id));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lake table metadata", KP(lake_table_metadata));
  } else if (share::is_iceberg_lake_table(lake_table_metadata->get_format_type())) {
    ObIcebergTableMetadata *iceberg_table_metadata
        = down_cast<ObIcebergTableMetadata *>(lake_table_metadata);
    const ObString &access_info = iceberg_table_metadata->access_info_;
    const TableItem *part_item = stmt.get_table_item_by_id(table_id);
    const ObIArray<ObString> *partition_names =
        OB_NOT_NULL(part_item) ? &part_item->part_names_ : nullptr;
    const ObIArray<ObObj> *partition_values =
        OB_NOT_NULL(part_item) && OB_NOT_NULL(part_item->ext_table_def_)
        ? &part_item->ext_table_def_->partition_values_ : nullptr;
    int32_t partition_spec_id = -1;
    if (OB_NOT_NULL(partition_names) && !partition_names->empty()) {
      if (OB_UNLIKELY(part_item->part_ids_.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid partition clause, should have partition spec id", K(ret));
        LOG_USER_WARN(OB_INVALID_ARGUMENT, "invalid partition clause, should have partition spec id");
      } else {
        partition_spec_id = static_cast<int32_t>(part_item->part_ids_.at(0));
      }
    }

    ObSEArray<iceberg::ManifestFile*, 16> all_manifest_files;
    ObSEArray<iceberg::ManifestFile*, 16> manifest_files;
    ObSEArray<iceberg::ManifestEntry*, 16> manifest_entries;
    ObLakeTablePartKeyMap part_key_map;
    iceberg::Snapshot *snapshot = NULL;
    bool enable_lake_table_parallel_resolving = true;
    bool use_parallel_iceberg_pruning = false;
    lake_table::ObLakeTableExecutor *exec_impl = NULL;
    int64_t manifest_file_cnt = 0;
    int64_t chunk_size = 0;
    int64_t task_cnt = 0;
    if (OB_FAIL(ObLakeTableOptimizerUtils::get_enable_lake_table_parallel_resolving(
            stmt, enable_lake_table_parallel_resolving))) {
      LOG_WARN("failed to get lake table parallel resolve config", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (lake_table_snapshot_id == -1L) {
      // do nothing 空表
      snapshot = NULL;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(iceberg_table_metadata->table_metadata_.get_snapshot_by_id(
                   lake_table_snapshot_id,
                   snapshot))) {
      LOG_WARN("failed to get snapshot", K(ret), K(lake_table_snapshot_id));
    } else if (OB_ISNULL(snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null snapshot");
    }

    ObIcebergFilePrunner *iceberg_file_pruner = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(iceberg_file_pruner = OB_NEWx(ObIcebergFilePrunner, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObIcebergFilePrunner", K(ret));
    } else if (OB_FAIL(iceberg_file_pruner->init(&sql_schema_guard,
                                                 stmt,
                                                 exec_ctx,
                                                 table_id,
                                                 ref_table_id,
                                                 iceberg_table_metadata->table_metadata_.partition_specs,
                                                 filter_exprs))) {
      LOG_WARN("failed to init table location", K(ret));
    } else if (NULL == snapshot) {
      // do nothing
      // 空表
    } else if (OB_FAIL(snapshot->get_manifest_files(access_info, all_manifest_files))) {
      LOG_WARN("failed to get manifest files");
    } else if (all_manifest_files.empty()) {
      // do nothing
    } else {
      if (enable_lake_table_parallel_resolving) {
        exec_impl = MTL(lake_table::ObLakeTableExecutor*);
        if (OB_ISNULL(exec_impl)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lake table executor MTL service is null");
        } else {
          manifest_file_cnt = all_manifest_files.count();
          chunk_size = ObExternalTableUtils::calc_parallel_task_chunk_size(
                                                          manifest_file_cnt,
                                                          exec_impl->get_thread_cnt());
          task_cnt = manifest_file_cnt <= 0 ? 0
                   : (manifest_file_cnt + chunk_size - 1) / chunk_size;
          use_parallel_iceberg_pruning = task_cnt > 1;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (use_parallel_iceberg_pruning) {
        lake_table::ObLakeTableTaskGroupHandle task_group;
        ObSEArray<bool, 16> in_bound_array;
        if (OB_FAIL(in_bound_array.prepare_allocate(manifest_file_cnt))) {
          LOG_WARN("failed to prepare allocate in bound array", K(ret));
        } else if (OB_FAIL(exec_impl->create_task_group(allocator_, task_cnt, task_group))) {
          LOG_WARN("failed to create lake table task group", K(ret), K(task_cnt));
        }
        for (int64_t begin = 0; OB_SUCC(ret) && begin < manifest_file_cnt; begin += chunk_size) {
          const int64_t end = (begin + chunk_size < manifest_file_cnt)
                                ? (begin + chunk_size) : manifest_file_cnt;
          if (OB_FAIL(exec_impl->Add(task_group,
                  ParallelIcebergManifestPruneTask(iceberg_file_pruner,
                                                   all_manifest_files,
                                                   in_bound_array,
                                                   access_info,
                                                   begin,
                                                   end)))) {
            LOG_WARN("failed to submit manifest parse task", K(ret), K(begin), K(end));
          }
        }

        if (task_group) {
          const int task_ret = task_group->wait();
          if (OB_SUCCESS != task_ret) {
            if (OB_SUCC(ret)) {
              ret = task_ret;
            }
            LOG_WARN("parallel load manifest failed", K(task_ret), K(ret));
          }
          exec_impl->destroy_task_group(allocator_, task_group);
        }

        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < manifest_file_cnt; ++i) {
            if (in_bound_array.at(i)) {
              if (OB_FAIL(all_manifest_files.at(i)->get_manifest_entries(access_info, manifest_entries))) {
                LOG_WARN("failed to get manifest entries", K(ret));
              }
            }
          }
        }
      } else {
        if (OB_FAIL(iceberg_file_pruner->prune_manifest_files(all_manifest_files, manifest_files))) {
          LOG_WARN("failed to prune manifest files", K(ret));
        } else if (OB_NOT_NULL(partition_names)
               && OB_NOT_NULL(partition_values)
               && !partition_names->empty()
               && OB_FAIL(iceberg_file_pruner->prune_manifest_files_by_partition_clause(
                              manifest_files, *partition_names, *partition_values, partition_spec_id,
                              iceberg_table_metadata->table_metadata_.partition_specs))) {
          LOG_WARN("failed to prune manifest files by partition clause");
        // 解析出的 ManifestEntry 裁剪之后还要用来获取统计信息，因此使用类的成员 allocator 生成。
        } else if (manifest_files.empty()) {
          // do nothing
        } else if (OB_FAIL(get_manifest_entries(access_info,
                                                manifest_files,
                                                manifest_entries))) {
          LOG_WARN("failed to get manifest entries", K(ret));
        }
      }
    }

    if (OB_FAIL(ret) || manifest_entries.empty()) {
      // do nothing
    } else if (OB_NOT_NULL(partition_names)
               && OB_NOT_NULL(partition_values)
               && !partition_names->empty()
               && OB_FAIL(iceberg_file_pruner->prune_manifest_entries_by_partition_clause(
                              *partition_names, *partition_values,
                              partition_spec_id, manifest_entries))) {
      LOG_WARN("failed to filter manifest entries by partition clause", K(ret));
    } else if (manifest_entries.empty()) {
      // do nothing (after PARTITION clause filtering)
    } else if (OB_FAIL(check_iceberg_use_hash_part(
                   iceberg_table_metadata->table_metadata_.partition_specs,
                   first_bucket_partition_value_offset_))) {
      LOG_WARN("failed to check iceberg use hash part");
    } else if (OB_FAIL(iceberg_file_pruner->prune_data_files(*exec_ctx,
                                                             manifest_entries,
                                                             iceberg_file_descs_))) {
      LOG_WARN("failed to prune data files");
    }

    // 这里不能接上面的 else if, 否则 manifest_files 为空的情况会出 bug
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(filter_files_by_sample(stmt, table_id, iceberg_file_descs_))) {
      LOG_WARN("failed to filter iceberg files by sample", K(ret));
    } else if (is_hash_aggregate()
               && OB_FAIL(build_iceberg_part_ids(part_key_map, iceberg_file_descs_))) {
      LOG_WARN("failed to build iceberg partition ids", K(ret));
    } else if (OB_FAIL(select_location_for_iceberg(exec_ctx, part_key_map, iceberg_file_descs_))) {
      LOG_WARN("failed to select location for iceberg");
    } else {
      candi_table_loc_.set_table_location_key(iceberg_file_pruner->get_table_id(),
                                              iceberg_file_pruner->get_ref_table_id());
      candi_table_loc_.set_is_lake_table(true);
      file_pruner_ = iceberg_file_pruner;
    }
    if (part_key_map.created()) {
      int tmp_ret = part_key_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destroy part key map", K(tmp_ret));
      }
    }
  } else if (share::is_hive_lake_table(lake_table_metadata->get_format_type())) {
    ObHiveFilePruner *hive_file_pruner = NULL;
    ObArray<ObHiveFileDesc> hive_files(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_));
    if (OB_ISNULL(hive_file_pruner = OB_NEWx(ObHiveFilePruner, &allocator_, allocator_))) {
      LOG_WARN("failed to allocate memory for ObHiveFilePrunner", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(hive_file_pruner->init(sql_schema_guard,
                                               stmt,
                                               exec_ctx,
                                               table_id,
                                               ref_table_id,
                                               filter_exprs))) {
      LOG_WARN("failed to init hive file prunner", K(ret));
    } else if (OB_FAIL(hive_file_pruner->prunner_files(*exec_ctx, hive_files))) {
      LOG_WARN("failed to init hive table location", K(ret));
    } else if (OB_FAIL(filter_files_by_sample(stmt, table_id, hive_files))) {
      LOG_WARN("failed to filter hive files by sample", K(ret));
    } else if (OB_FAIL(select_location_for_hive(exec_ctx, hive_files))) {
      LOG_WARN("failed to select location for hive");
    } else {
      candi_table_loc_.set_table_location_key(hive_file_pruner->get_table_id(),
                                              hive_file_pruner->get_ref_table_id());
      candi_table_loc_.set_is_lake_table(true);
      file_pruner_ = hive_file_pruner;
    }

  } else if (share::is_lake_plugin_table(lake_table_metadata->get_format_type())) {
    // cpp .so plugin-backed format: metadata is sql::ext_plugin::ObExtTableMetadata.
    // ObExtFilePruner drives the plugin contract's plan_create to obtain scan tasks
    // (one ObPluginSplitDesc per task, task_json_ = task JSON), then
    // select_location_for_plugin distributes them across PX servers — the existing
    // PX plumbing is reused unchanged.
    const sql::ext_plugin::ObExtTableMetadata *ext_table_metadata
        = static_cast<const sql::ext_plugin::ObExtTableMetadata *>(lake_table_metadata);
    ObExtFilePruner *ext_file_pruner = NULL;
    ObSEArray<ObPluginSplitDesc *, 16> plugin_splits;
    ObExtTableDispatchMode dispatch_mode = ObExtTableDispatchMode::ROUND_ROBIN;
    if (OB_ISNULL(ext_file_pruner = OB_NEWx(ObExtFilePruner, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObExtFilePruner", K(ret));
    } else if (OB_FAIL(ext_file_pruner->init(stmt,
                                             exec_ctx,
                                             table_id,
                                             ref_table_id,
                                             ext_table_metadata,
                                             filter_exprs))) {
      LOG_WARN("failed to init ext file pruner", K(ret));
    } else if (OB_FAIL(ext_file_pruner->prune_ext_splits(*exec_ctx,
                                                         plugin_splits,
                                                         dispatch_mode))) {
      LOG_WARN("failed to prune ext splits", K(ret));
    } else if (OB_FAIL(select_location_for_plugin(exec_ctx, plugin_splits, dispatch_mode))) {
      LOG_WARN("failed to select location for plugin", K(ret));
    } else {
      candi_table_loc_.set_table_location_key(ext_file_pruner->get_table_id(),
                                              ext_file_pruner->get_ref_table_id());
      candi_table_loc_.set_is_lake_table(true);
      file_pruner_ = ext_file_pruner;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get unsupported lake format");
  }
  return ret;
}

int ObLakeTablePartitionInfo::check_iceberg_use_hash_part(const ObIArray<iceberg::PartitionSpec*> &partition_specs,
                                                          int64_t &offset)
{
  int ret = OB_SUCCESS;
  iceberg::PartitionSpec* part_spec = nullptr;
  int64_t hash_count = 0;
  offset = -1;
  if (partition_specs.count() != 1) {
    // do nothing
  } else if (OB_ISNULL(part_spec = partition_specs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null partition spec");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (-1 == offset) && i < part_spec->fields.count(); ++i) {
      if (OB_ISNULL(part_spec->fields.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null field");
      } else if (part_spec->fields.at(i)->transform.transform_type == iceberg::TransformType::Bucket) {
        hash_count = part_spec->fields.at(i)->transform.param.value();
        offset = i;
      }
    }
    if (OB_SUCC(ret) && offset > -1) {
      if (OB_UNLIKELY(hash_count == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected hash count");
      } else {
        set_is_hash_aggregate(true);
        set_hash_count(hash_count);
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::select_location_for_iceberg(ObExecContext *exec_ctx,
                                                          ObLakeTablePartKeyMap &part_key_map,
                                                          ObIArray<ObIcebergFileDesc *> &file_descs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> all_servers;
  ObCandiTabletLocIArray &candi_tablet_locs = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  candi_tablet_locs.reset();
  ObDefaultLoadBalancer load_balancer;
  ObAddr addr;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_UNLIKELY(file_descs.empty())) {
    ObCandiTabletLoc* tablet_loc = nullptr;
    uint64_t part_id = 0;
    if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
    } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, GCTX.self_addr(), part_id))) {
      LOG_WARN("failed to init tablet loc by addr");
    // } else if (OB_FAIL(add_table_file(*tablet_loc, file_desc))) {
    //   LOG_WARN("failed to add table file");
    }
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(exec_ctx->get_my_session()->get_effective_tenant_id(),
                                                                all_servers))) {
    LOG_WARN("fail to get external table location");
  } else if (OB_FAIL(load_balancer.add_server_list(all_servers))) {
    LOG_WARN("failed to add server list");
  } else if (is_hash_aggregate()) {
    /* 将分区按照bucket分区定义划分
     * 考虑到存在partition by (c1, bucket(c2), 4) 的场景，需要把bucket_idx相同，但是c1值不同的分区居合道一起，
     * 因此使用了一个 hash map 记录是否已经为 bucket_idx 生成过 tablet loc，使用一个 hash map 记录每个iceberg
     * part idx 映射的 tablet loc。
    */
    // iceberg part idx -> tablet loc idx
    hash::ObHashMap<int64_t, int64_t> part_idx_map;
    // bucket idx -> tablet loc idx
    hash::ObHashMap<int64_t, int64_t> bucket_idx_map;
    // bucket idx, used to get the exact bucket count to init candi_tablet_locs
    hash::ObHashSet<int64_t> bucket_idx_set;
    if (OB_FAIL(part_idx_map.create(part_key_map.size(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create part idx map");
    } else if (OB_FAIL(bucket_idx_map.create(all_servers.count(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create bucket idx map");
    } else if (OB_FAIL(bucket_idx_set.create(all_servers.count(), "BucketIdxSet", "LakeTableLoc"))) {
      LOG_WARN("failed to create bucket idx set");
    }
    ObLakeTablePartKeyMap::const_iterator iter = part_key_map.begin();
    for (; OB_SUCC(ret) && iter != part_key_map.end(); ++iter) {
      int32_t bucket_idx = -1;
      if (OB_FAIL(get_bucket_idx(iter->first, first_bucket_partition_value_offset_, bucket_idx))) {
        LOG_WARN("failed to get hash part idx", K(ret));
      } else if (OB_FAIL(bucket_idx_set.set_refactored(bucket_idx))) {
        LOG_WARN("failed to set bucket idx", K(ret), K(bucket_idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(candi_tablet_locs.reserve(bucket_idx_set.size()))) {
      LOG_WARN("failed to reserve candi tablet locs", K(ret));
    } else {
      iter = part_key_map.begin();
    }
    for (; OB_SUCC(ret) && iter != part_key_map.end(); ++iter) {
      int32_t bucket_idx = -1;
      int64_t tablet_loc_idx = -1;
      if (OB_FAIL(get_bucket_idx(iter->first, first_bucket_partition_value_offset_, bucket_idx))) {
        LOG_WARN("failed to get hash part idx");
      } else if (OB_FAIL(bucket_idx_map.get_refactored(bucket_idx, tablet_loc_idx))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          tablet_loc_idx = candi_tablet_locs.count();
          ObCandiTabletLoc* tablet_loc = candi_tablet_locs.alloc_place_holder();
          if (OB_ISNULL(tablet_loc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
          } else if (OB_FAIL(bucket_idx_map.set_refactored(bucket_idx, tablet_loc_idx))) {
            LOG_WARN("failed to set bucket idx map");
          } else if (OB_FAIL(load_balancer.select_server(bucket_idx, addr))) {
            LOG_WARN("failed to select server");
          } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, static_cast<uint64_t>(bucket_idx)))) {
            LOG_WARN("failed to init tablet loc by addr");
          }
        } else {
          LOG_WARN("failed to get tablet loc");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_idx_map.set_refactored(iter->second, tablet_loc_idx))) {
        LOG_WARN("failed to set part idx map");
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      int64_t tablet_loc_idx = -1;
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      if (OB_ISNULL(file_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null file desc");
      } else if (OB_FAIL(part_idx_map.get_refactored(file_desc->part_idx_, tablet_loc_idx))) {
        LOG_WARN("failed to get tablet loc");
      } else if (OB_UNLIKELY(tablet_loc_idx < 0 || tablet_loc_idx >= candi_tablet_locs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(tablet_loc_idx));
      } else if (OB_FAIL(add_table_file(candi_tablet_locs.at(tablet_loc_idx), file_desc, i))) {
        LOG_WARN("failed to add table file");
      }
    }

    if (part_idx_map.created()) {
      int tmp_ret = part_idx_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory part idx map", K(tmp_ret));
      }
    }
    if (bucket_idx_map.created()) {
      int tmp_ret = bucket_idx_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory bucket idx map", K(tmp_ret));
      }
    }
    if (bucket_idx_set.created()) {
      int tmp_ret = bucket_idx_set.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory bucket idx set", K(tmp_ret));
      }
    }
  } else {
    uint64_t last_part_id = 0;
    hash::ObHashMap<ObAddr, int64_t> tablet_loc_map;
    if (OB_FAIL(tablet_loc_map.create(all_servers.count(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create tablet loc map");
    } else if (OB_FAIL(candi_tablet_locs.reserve(all_servers.count()))) {
      LOG_WARN("failed to reserve candi tablet locs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      int64_t idx = -1;
      if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null file desc");
      } else if (OB_FAIL(load_balancer.select_server(file_desc->entry_->data_file.file_path, addr))) {
        LOG_WARN("failed to select server");
      } else if (OB_FAIL(tablet_loc_map.get_refactored(addr, idx))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          idx = candi_tablet_locs.count();
          ObCandiTabletLoc* tablet_loc = candi_tablet_locs.alloc_place_holder();
          if (OB_ISNULL(tablet_loc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
          } else if (OB_FAIL(tablet_loc_map.set_refactored(addr, idx))) {
            LOG_WARN("failed to set tablet loc map");
          } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, ++last_part_id))) {
            LOG_WARN("failed to init tablet loc by addr");
          }
        } else {
          LOG_WARN("failed to get tablet loc");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(idx < 0 || idx >= candi_tablet_locs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(idx));
      } else if (OB_FAIL(add_table_file(candi_tablet_locs.at(idx), file_desc, i))) {
        LOG_WARN("failed to add table file");
      }
    }
    if (tablet_loc_map.created()) {
      int tmp_ret = tablet_loc_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destroy tablet loc map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::select_location_for_hive(ObExecContext *exec_ctx,
                                                       ObIArray<ObHiveFileDesc> &file_descs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> all_servers;
  ObCandiTabletLocIArray &candi_tablet_locs
      = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  candi_tablet_locs.reset();
  ObDefaultLoadBalancer load_balancer;
  ObAddr addr;
  bool use_file_size_load_balance = false;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_NOT_NULL(exec_ctx->get_query_ctx())
             && OB_FAIL(exec_ctx->get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
                    ObOptParamHint::EXTERNAL_TABLE_FILE_SIZE_LOAD_BALANCE,
                    use_file_size_load_balance))) {
    LOG_WARN("failed to get external table file size load balance hint", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(
                 exec_ctx->get_my_session()->get_effective_tenant_id(),
                 all_servers))) {
    LOG_WARN("fail to get external table location");
  } else if (OB_FAIL(load_balancer.add_server_list(all_servers))) {
    LOG_WARN("failed to add server list");
  } else {
    uint64_t last_part_id = 0;
    hash::ObHashMap<ObAddr, int64_t> tablet_loc_map;
    if (OB_FAIL(tablet_loc_map.create(all_servers.count(),
                                      "TabletHiveMap",
                                      "HiveTableLoc"))) {
      LOG_WARN("failed to create tablet loc map");
    } else if (file_descs.empty()) {
      ObCandiTabletLoc* tablet_loc = nullptr;
      uint64_t part_id = 0;
      if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
      } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, GCTX.self_addr(), part_id))) {
        LOG_WARN("failed to init tablet loc by addr");
      }
    } else if (OB_FAIL(candi_tablet_locs.reserve(all_servers.count()))) {
      LOG_WARN("failed to reserve candi tablet locs", K(ret));
    } else if (use_file_size_load_balance) {
      ObArray<int64_t> file_assigned_idxs;
      ObSEArray<int64_t, 16> tablet_loc_idxs;
      if (OB_FAIL(ObExternalTableUtils::calc_assigned_files_to_sqcs(
              file_descs, file_assigned_idxs, all_servers.count()))) {
        LOG_WARN("failed to assign hive files to sqcs by size", K(ret));
      } else if (file_assigned_idxs.count() != file_descs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hive file assignment result", K(ret),
                 K(file_assigned_idxs.count()), K(file_descs.count()));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); ++i) {
        if (OB_FAIL(tablet_loc_idxs.push_back(-1))) {
          LOG_WARN("failed to init tablet location indexes", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
        const int64_t idx = file_assigned_idxs.at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= all_servers.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid assigned sqc index", K(ret), K(idx), K(all_servers.count()));
        } else {
          int64_t &tablet_loc_idx = tablet_loc_idxs.at(idx);
          if (tablet_loc_idx < 0) {
            ObCandiTabletLoc *tablet_loc = candi_tablet_locs.alloc_place_holder();
            if (OB_ISNULL(tablet_loc)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to alloc place holder for ObCandiTabletLoc", K(ret));
            } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc,
                                                       all_servers.at(idx),
                                                       idx + 1))) {
              LOG_WARN("failed to init tablet loc by addr", K(ret));
            } else {
              tablet_loc_idx = candi_tablet_locs.count() - 1;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(tablet_loc_idx < 0 || tablet_loc_idx >= candi_tablet_locs.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected idx", K(ret), K(tablet_loc_idx));
          } else if (OB_FAIL(add_table_file_for_hive(candi_tablet_locs.at(tablet_loc_idx),
                                                     file_descs.at(i)))) {
            LOG_WARN("failed to add hive table file", K(ret));
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
        int64_t idx = -1;
        ObCandiTabletLoc *tablet_loc = nullptr;
        if (OB_FAIL(load_balancer.select_server(file_descs.at(i).file_path_, addr))) {
          LOG_WARN("failed to select server");
        } else if (OB_FAIL(tablet_loc_map.get_refactored(addr, idx))) {
          if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
            ret = OB_SUCCESS;
            idx = candi_tablet_locs.count();
            if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to alloc place holder for ObCandiTabletLoc", K(ret));
            } else if (OB_FAIL(tablet_loc_map.set_refactored(addr, idx))) {
              LOG_WARN("failed to set tablet loc map", K(ret));
            } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, ++last_part_id))) {
              LOG_WARN("failed to init tablet loc by addr", K(ret));
            }
          } else {
            LOG_WARN("failed to get tablet loc");
          }
        }
        if (OB_FAIL(ret)) {
        } else if  (OB_UNLIKELY(idx < 0 || idx >= candi_tablet_locs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected idx", K(idx));
        } else if (OB_FAIL(add_table_file_for_hive(candi_tablet_locs.at(idx), file_descs.at(i)))) {
          LOG_WARN("failed to add table file");
        }
      }
    }
    if (tablet_loc_map.created()) {
      int tmp_ret = tablet_loc_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory tablet loc map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::get_bucket_idx(const ObLakeTablePartKey &part_key,
                                             const int64_t offset,
                                             int32_t &bucket_idx)
{
  int ret = OB_SUCCESS;
  const ObObj *part_value = nullptr;
  if (OB_FAIL(part_key.get_partition_value(offset, part_value))) {
    LOG_WARN("failed to get hash partition value", K(ret), K(offset));
  } else if (OB_ISNULL(part_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash partition value is null", K(ret), K(offset));
  } else if (part_value->is_null()) {
    bucket_idx = hash_count_;
  } else if (!part_value->is_int32()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash part value is not int", KPC(part_value));
  } else {
    bucket_idx = part_value->get_int32();
  }
  return ret;
}

int ObLakeTablePartitionInfo::init_tablet_loc_by_addr(ObCandiTabletLoc &tablet_loc,
                                                      const ObAddr &addr,
                                                      const uint64_t part_id)
{
  int ret = OB_SUCCESS;
  ObLSLocation location;
  if (OB_FAIL(ObDASLocationRouter::get_external_table_ls_location(location, &addr))) {
    LOG_WARN("failed to get external table location");
  } else {
    ObObjectID first_level_part_id = OB_INVALID_ID;
    ObTabletID mock_tablet_id = ObTabletID(part_id);
    if (OB_FAIL(tablet_loc.set_part_loc_with_only_readable_replica(part_id,
                                                                   first_level_part_id,
                                                                   mock_tablet_id,
                                                                   location,
                                                                   ObRoutePolicyType::READONLY_ZONE_FIRST))) {
      LOG_WARN("failed to set partition location with only readable replica", K(location));
    } else {
      tablet_loc.set_selected_replica_idx(0);
    }
    LOG_TRACE("set partition location with only readable replica", K(location), K(tablet_loc));
  }
  return ret;
}

int ObLakeTablePartitionInfo::add_table_file(ObCandiTabletLoc &tablet_loc,
                                             ObIcebergFileDesc *file_desc,
                                             const int64_t file_desc_idx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObIOptLakeTableFile*>& files = tablet_loc.get_opt_lake_table_files_for_update();
  ObIOptLakeTableFile *file = nullptr;
  if (OB_FAIL(ObIOptLakeTableFile::create_opt_lake_table_file_by_type(allocator_, LakeFileType::ICEBERG, file))) {
    LOG_WARN("failed to create opt lake table file by type");
  } else if (OB_ISNULL(file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate place holder for ObIOptLakeTableFile");
  } else if (OB_FAIL(files.push_back(file))) {
    LOG_WARN("failed to push back opt lake table file");
  } else if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null file desc");
  } else {
    ObOptIcebergFile *iceberg_file = static_cast<ObOptIcebergFile *>(file);
    iceberg_file->file_url_ = file_desc->entry_->data_file.file_path;
    iceberg_file->file_size_ = file_desc->entry_->data_file.file_size_in_bytes;
    iceberg_file->modification_time_ = file_desc->entry_->snapshot_id;
    iceberg_file->file_format_ = file_desc->entry_->data_file.file_format;
    iceberg_file->record_count_ = file_desc->entry_->data_file.record_count;
    iceberg_file->part_id_ = OB_INVALID_ID == file_desc->part_idx_
                                 ? OB_INVALID_INDEX_INT64
                                 : static_cast<int64_t>(file_desc->part_idx_);
    iceberg_file->partition_spec_id_ = file_desc->entry_->partition_spec_id;
    iceberg_file->file_desc_idx_ = file_desc_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_desc->delete_files_.size(); i++) {
      const iceberg::ManifestEntry *delete_entry = file_desc->delete_files_.at(i);
      ObLakeDeleteFile *delete_file = NULL;
      if (OB_ISNULL(delete_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete_entry is null", K(ret));
      } else if (OB_ISNULL(delete_file = OB_NEWx(ObLakeDeleteFile, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory");
      } else {
        delete_file->file_url_ = delete_entry->data_file.file_path;
        delete_file->file_size_ = delete_entry->data_file.file_size_in_bytes;
        delete_file->modification_time_ = delete_entry->snapshot_id;
        delete_file->file_format_ = delete_entry->data_file.file_format;
        delete_file->is_file_scoped_ =
            delete_entry->data_file.referenced_data_file.has_value();

        switch (delete_entry->data_file.content) {
          case iceberg::DataFileContent::POSITION_DELETES: {
            if (delete_entry->is_deletion_vector_file()) {
              if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_5_1_0) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not support deletion vector file", K(ret), K(GET_MIN_CLUSTER_VERSION()));
              } else if (!delete_entry->data_file.content_offset.has_value()
                  || !delete_entry->data_file.content_size_in_bytes.has_value()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("delete file content offset or size do not have value");
              } else {
                delete_file->dv_content_offset_ = delete_entry->data_file.content_offset.value();
                delete_file->dv_content_size_in_bytes_
                    = delete_entry->data_file.content_size_in_bytes.value();
                delete_file->type_ = ObLakeDeleteFileType::DELETION_VECTOR;
              }
            } else {
              delete_file->type_ = ObLakeDeleteFileType::POSITION_DELETE;
            }
            break;
          }
          case iceberg::DataFileContent::EQUALITY_DELETES: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support equality delete file");
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid delete file content", K(delete_entry->data_file.content));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(iceberg_file->delete_files_.push_back(delete_file))) {
          LOG_WARN("failed to push back delete file");
        }
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::add_table_file_for_hive(ObCandiTabletLoc &tablet_loc,
                                                      ObHiveFileDesc &file_desc)
{
  int ret = OB_SUCCESS;
  ObIArray<ObIOptLakeTableFile*>& files = tablet_loc.get_opt_lake_table_files_for_update();
  ObIOptLakeTableFile *file = nullptr;
  if (OB_FAIL(ObIOptLakeTableFile::create_opt_lake_table_file_by_type(allocator_, LakeFileType::HIVE, file))) {
    LOG_WARN("failed to create opt lake table file by type");
  } else if (OB_ISNULL(file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate place holder for ObIOptLakeTableFile");
  } else if (OB_FAIL(files.push_back(file))) {
    LOG_WARN("failed to push back opt lake table file");
  } else {
    ObOptHiveFile *hive_file = static_cast<ObOptHiveFile*>(file);
    hive_file->file_url_ = file_desc.file_path_;
    hive_file->part_id_ = file_desc.part_id_;
    hive_file->file_size_ = file_desc.file_size_;
    hive_file->modification_time_ = file_desc.modify_ts_;
  }
  return ret;
}

int ObLakeTablePartitionInfo::select_location_for_plugin(
    ObExecContext *exec_ctx,
    ObIArray<ObPluginSplitDesc *> &plugin_splits,
    ObExtTableDispatchMode dispatch_mode)
{
  int ret = OB_SUCCESS;
  if (dispatch_mode == ObExtTableDispatchMode::ROUND_ROBIN) {
    if (OB_FAIL(select_location_for_plugin_round_robin(exec_ctx, plugin_splits))) {
      LOG_WARN("failed to select plugin location for round robin");
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported dispatch mode", K(dispatch_mode));
  }
  return ret;
}

int ObLakeTablePartitionInfo::select_location_for_plugin_round_robin(
    ObExecContext *exec_ctx,
    ObIArray<ObPluginSplitDesc *> &plugin_splits)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> all_servers;
  ObCandiTabletLocIArray &candi_tablet_locs
      = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  candi_tablet_locs.reset();
  ObDefaultLoadBalancer load_balancer;
  ObAddr addr;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (plugin_splits.empty()) {
    // empty table: create a single tablet loc on self
    ObCandiTabletLoc *tablet_loc = nullptr;
    uint64_t part_id = 0;
    if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
    } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, GCTX.self_addr(), part_id))) {
      LOG_WARN("failed to init tablet loc by addr");
    }
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(
                 exec_ctx->get_my_session()->get_effective_tenant_id(),
                 all_servers))) {
    LOG_WARN("fail to get external table location");
  } else if (OB_FAIL(load_balancer.add_server_list(all_servers))) {
    LOG_WARN("failed to add server list");
  } else {
    uint64_t last_part_id = 0;
    hash::ObHashMap<ObAddr, int64_t> tablet_loc_map;
    if (OB_FAIL(tablet_loc_map.create(all_servers.count(),
                                      "TabletPlgMap",
                                      "PluginTblLoc"))) {
      LOG_WARN("failed to create tablet loc map");
    } else if (OB_FAIL(candi_tablet_locs.reserve(all_servers.count()))) {
      LOG_WARN("failed to reserve candi tablet locs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < plugin_splits.count(); ++i) {
      int64_t idx = -1;
      ObPluginSplitDesc *split_desc = plugin_splits.at(i);
      if (OB_ISNULL(split_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null plugin split desc");
      } else if (OB_FAIL(load_balancer.select_server(i, addr))) {
        LOG_WARN("failed to select server");
      } else if (OB_FAIL(tablet_loc_map.get_refactored(addr, idx))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          idx = candi_tablet_locs.count();
          ObCandiTabletLoc *tablet_loc = candi_tablet_locs.alloc_place_holder();
          if (OB_ISNULL(tablet_loc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
          } else if (OB_FAIL(tablet_loc_map.set_refactored(addr, idx))) {
            LOG_WARN("failed to set tablet loc map");
          } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, ++last_part_id))) {
            LOG_WARN("failed to init tablet loc by addr");
          }
        } else {
          LOG_WARN("failed to get tablet loc");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(idx < 0 || idx >= candi_tablet_locs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(idx));
      } else if (OB_FAIL(add_table_file_for_plugin(candi_tablet_locs.at(idx),
                                                   split_desc))) {
        LOG_WARN("failed to add table file for plugin");
      }
    }
    if (tablet_loc_map.created()) {
      int tmp_ret = tablet_loc_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destroy tablet loc map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::add_table_file_for_plugin(ObCandiTabletLoc &tablet_loc,
                                                        ObPluginSplitDesc *split_desc)
{
  int ret = OB_SUCCESS;
  ObIArray<ObIOptLakeTableFile*>& files = tablet_loc.get_opt_lake_table_files_for_update();
  ObIOptLakeTableFile *file = nullptr;
  if (OB_ISNULL(split_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null plugin split desc");
  } else if (OB_FAIL(ObIOptLakeTableFile::create_opt_lake_table_file_by_type(allocator_,
                                                                              LakeFileType::EXT_PLUGIN,
                                                                              file))) {
    LOG_WARN("failed to create plugin lake table file");
  } else if (OB_ISNULL(file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate plugin lake table file", K(ret));
  } else if (OB_FAIL(files.push_back(file))) {
    LOG_WARN("failed to push back plugin lake table file", K(ret));
  } else {
    ObOptPluginFile *plugin_file = static_cast<ObOptPluginFile *>(file);
    if (OB_FAIL(ob_write_string(allocator_, split_desc->task_json_, plugin_file->task_json_))) {
      LOG_WARN("failed to copy plugin task json", K(ret));
    } else {
      plugin_file->record_count_ = split_desc->record_count_;
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::get_partition_values(ObIArray<ObString> &partition_values) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_pruner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file pruner is null");
  } else if (OB_FAIL(partition_values.assign(file_pruner_->partition_values_))) {
    LOG_WARN("failed to assign partition values", K(ret));
  }
  return ret;
}

template <typename T>
int ObLakeTablePartitionInfo::filter_files_by_sample(const ObDMLStmt &stmt,
                                                     const uint64_t table_id,
                                                     ObIArray<T> &files)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = stmt.get_table_item_by_id(table_id);
  bool need_filter = false;
  if (OB_NOT_NULL(table_item) && OB_NOT_NULL(table_item->sample_info_)) {
    const SampleInfo &si = *table_item->sample_info_;
    need_filter = si.is_row_sample()
                  && -1 == static_cast<int64_t>(si.seed_)
                  && files.count() > 0
                  && si.percent_ > 0 && si.percent_ < 100;
  }
  if (need_filter) {
    const SampleInfo &si = *table_item->sample_info_;
    const int64_t total_files = files.count();
    bool is_file_sample = false;
    int64_t target_count = 0;
    ObSEArray<int64_t, 16> indices;
    if (OB_FAIL(share::ObExternalTableUtils::generate_file_sample_indices(
            total_files, si.percent_, is_file_sample, target_count, indices))) {
      LOG_WARN("failed to generate file sample indices", K(ret));
    } else if (is_file_sample) {
      ObSEArray<T, 16> filtered;
      for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
        int64_t idx = indices.at(i);
        if (idx >= 0 && idx < total_files) {
          OZ (filtered.push_back(files.at(idx)));
        }
      }
      if (OB_SUCC(ret)) {
        files.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < filtered.count(); ++i) {
          OZ (files.push_back(filtered.at(i)));
        }
        LOG_INFO("[FILE SAMPLE] pruner level file sample filter",
                 K(total_files), K(target_count), K(indices),
                 "remaining_files", files.count());
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
