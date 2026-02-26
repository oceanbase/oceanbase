/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_hive_file_pruner.h"

#include "common/ob_smart_call.h"
#include "common/ob_smart_var.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObHiveFilePruner::ObHiveFilePruner(common::ObIAllocator &allocator)
    : ObILakeTableFilePruner(allocator, PrunnerType::HIVE), sql_schema_guard_(),
      hive_part_bounds_(allocator_), part_column_ids_(allocator_)
{
}

void ObHiveFilePruner::reset()
{
  ObILakeTableFilePruner::reset();
  sql_schema_guard_ = NULL;
  hive_part_bounds_.reset();
  part_column_ids_.reset();
}

int ObHiveFilePruner::assign(const ObILakeTableFilePruner &o)
{
  int ret = OB_SUCCESS;
  if (this != &o) {
    reset();
    const ObHiveFilePruner &other = static_cast<const ObHiveFilePruner &>(o);
    if (OB_FAIL(ObILakeTableFilePruner::assign(o))) {
      LOG_WARN("assign loc meta failed", K(ret), K(other.loc_meta_));
    } else if (OB_FAIL(hive_part_bounds_.assign(other.hive_part_bounds_))) {
      LOG_WARN("failed to assign hive part bounds");
    } else if (OB_FAIL(part_column_ids_.assign(other.part_column_ids_))) {
      LOG_WARN("failed to assign part column ids");
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return ret;
}

int ObHiveFilePruner::init(ObSqlSchemaGuard &sql_schema_guard,
                           const ObDMLStmt &stmt,
                           ObExecContext *exec_ctx,
                           const uint64_t table_id,
                           const uint64_t ref_table_id,
                           const ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  sql_schema_guard_ = &sql_schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("table location init twice", K(ret));
  } else if (OB_FAIL(sql_schema_guard.get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table scehema", K(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(exec_ctx));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    int64_t part_key_nums = table_schema->get_partition_key_column_num();
    if (part_key_nums > 0) {
      is_partitioned_ = true;
      if (OB_FAIL(part_column_ids_.init(table_schema->get_partition_key_column_num()))) {
        LOG_WARN("failed to init part column ids", K(ret));
      } else if (OB_FAIL(table_schema->get_partition_key_info().get_column_ids(part_column_ids_))) {
        LOG_WARN("failed to get part column ids", K(ret));
      }
    } else {
      is_partitioned_ = false;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_column_meta_info(stmt))) {
      LOG_WARN("failed to generate column meta info");
    } else if (OB_FAIL(generate_partition_bound(stmt, exec_ctx, table_schema, filter_exprs))) {
      LOG_WARN("failed to generate partition bound", K(ret));
    } else if (need_all_) {
      // do nothing
    } else if (OB_FAIL(ObLakeTablePushDownFilter::generate_pd_filter_spec(allocator_,
                                                                      *exec_ctx,
                                                                      &stmt,
                                                                      filter_exprs,
                                                                      file_filter_spec_))) {
      LOG_WARN("failed to generate pd filter spec");
    }
  }
  return ret;
}

int ObHiveFilePruner::generate_partition_bound(const ObDMLStmt &stmt,
                                               ObExecContext *exec_ctx,
                                               const ObTableSchema *table_schema,
                                               const ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKeyInfo &part_key_info = table_schema->get_partition_key_info();
  if (filter_exprs.empty() || !is_partitioned_) {
    need_all_ = true;
  } else if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(exec_ctx));
  } else {
    ObArenaAllocator tmp_allocator("FilePrunnerTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
    const ObDataTypeCastParams dtc_params
        = ObBasicSessionInfo::create_dtc_params(exec_ctx->get_my_session());

    ObArray<ObHivePartFieldBound *> tmp_part_field_bounds;
    for (int64_t j = 0; OB_SUCC(ret) && j < part_key_info.get_size(); ++j) {
      ObQueryRangeArray ranges;
      ObHivePartFieldBound *part_field_bound
          = OB_NEWx(ObHivePartFieldBound, &allocator_, allocator_);
      if (OB_ISNULL(part_field_bound)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator memory for ObPartFieldBound", K(ret));
      } else {
        part_field_bound->column_id_ = part_key_info.get_column(j)->column_id_;
        ObSEArray<ColumnItem, 1> part_columns;
        ColumnItem *column_item = nullptr;
        tmp_allocator.reuse();
        ObPreRangeGraph pre_range_graph(tmp_allocator);
        bool dummy_single_ranges = false;
        if (OB_ISNULL(column_item = stmt.get_column_item_by_id(loc_meta_.table_loc_id_,
                                                               part_field_bound->column_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column item", K(ret), K(loc_meta_), K(part_field_bound->column_id_));
        } else if (OB_FAIL(part_columns.push_back(*column_item))) {
          LOG_WARN("failed to push back column item", K(ret));
        } else if (OB_FAIL(pre_range_graph.preliminary_extract_query_range(part_columns,
                                                                           filter_exprs,
                                                                           exec_ctx,
                                                                           NULL,
                                                                           NULL,
                                                                           false,
                                                                           true))) {
          LOG_WARN("failed to preliminary extract query range",
                   K(ret),
                   K(part_columns),
                   K(filter_exprs));
        } else if (OB_FAIL(pre_range_graph.get_tablet_ranges(allocator_,
                                                             *exec_ctx,
                                                             ranges,
                                                             dummy_single_ranges,
                                                             dtc_params))) {
          LOG_WARN("failed to get tablet ranges", K(ret));
        } else if (OB_FAIL(build_field_bound_from_ranges(ranges, *part_field_bound))) {
          LOG_WARN("failed to build field bound from ranges", K(ret));
        } else if (OB_FAIL(part_field_bound->range_exprs_.assign(pre_range_graph.get_range_exprs()))) {
          LOG_WARN("failed to assign range exprs");
        } else if (ranges.count() == 1) {
          ObNewRange *range = ranges.at(0);
          if (range->is_false_range()) {
            part_field_bound->is_always_false_ = true;
          } else if (range->is_whole_range()) {
            part_field_bound->is_whole_range_ = true;
          }
        }
      }
      OZ(tmp_part_field_bounds.push_back(part_field_bound), K(part_field_bound));
    }
    OZ(hive_part_bounds_.assign(tmp_part_field_bounds));
  }
  return ret;
}

int ObHiveFilePruner::build_field_bound_from_ranges(ObIArray<ObNewRange *> &ranges,
                                                    ObHivePartFieldBound &part_field_bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_field_bound.bounds_.init(ranges.count()))) {
    LOG_WARN("failed to init fixed array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    ObFieldBound *field_bound = OB_NEWx(ObFieldBound, &allocator_);
    if (OB_ISNULL(field_bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator memory for ObFieldBound", K(ret));
    } else if (OB_ISNULL(ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null range", K(ret));
    } else if (OB_FAIL(field_bound->from_range(*ranges.at(i)))) {
      LOG_WARN("failed to init field bound from range", K(ret));
    } else if (OB_FAIL(part_field_bound.bounds_.push_back(field_bound))) {
      LOG_WARN("failed to push back field bound", K(ret));
    }
  }
  return ret;
}

int ObHiveFilePruner::prunner_files(ObExecContext &exec_ctx,
                                    ObIArray<ObHiveFileDesc> &filtered_files)
{
  int ret = OB_SUCCESS;

  // TODO 支持hdfs类型
  if (OB_FAIL(prune_partition_by_hms(exec_ctx, filtered_files))) {
    LOG_WARN("failed to prune partition.", K(ret), K(exec_ctx));
  }

  return ret;
}

int ObHiveFilePruner::prune_partition_by_hms(ObExecContext &exec_ctx,
                                             ObIArray<ObHiveFileDesc> &filtered_files)
{
  int ret = OB_SUCCESS;
  ObArray<PartitionInfo *> partition_infos;
  ModulePageAllocator block_allocator(allocator_);
  ObSEArray<int64_t, 16> selected_part_idxs;
  ObSEArray<ObString, 4> partition_column_names;
  partition_infos.set_block_allocator(block_allocator);

  ObString empty_patten;
  const ObTableSchema *table_schema = NULL;
  const share::ObILakeTableMetadata *metadata = NULL;
  ObArenaAllocator tmp_allocator;
  ObCachedCatalogMetaGetter catalog_meta_getter{*sql_schema_guard_->get_schema_guard(),
                                                tmp_allocator};
  int64_t refresh_interval_sec = 0;

  ObSEArray<int64_t, 4> tmp_part_id;
  ObSEArray<ObString, 4> tmp_part_path;
  ObSEArray<int64_t, 4> tmp_part_modify_ts;

  if (OB_FAIL(sql_schema_guard_->get_table_schema(loc_meta_.ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(loc_meta_.ref_table_id_));
  } else if (OB_ISNULL(metadata
                       = sql_schema_guard_->get_table_metadata(table_schema->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table metadata", K(ret), K(loc_meta_.ref_table_id_));
  } else if (OB_FAIL(catalog_meta_getter.get_cache_refresh_interval_sec(metadata,
                                                                        refresh_interval_sec))) {
    LOG_WARN("failed to get refresh_interval_sec");
  } else if (OB_FAIL(ObExternalTableUtils::collect_partitions_info_with_cache(*table_schema,
                                                                              *sql_schema_guard_,
                                                                              allocator_,
                                                                              refresh_interval_sec
                                                                                  * 1000,
                                                                              partition_infos))) {
    LOG_WARN("failed get table partitions from cache", K(ret), K(table_schema));
  } else {
    LOG_TRACE("get partition from cache: ", K(refresh_interval_sec), K(partition_infos));

    ObHivePushDownFilter file_filter(exec_ctx, file_filter_spec_, &part_column_ids_);
    if (!need_all_ && OB_FAIL(file_filter.init(column_ids_, column_metas_))) {
      LOG_WARN("failed to init skip filter executor");
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
      const PartitionInfo *partition_info = partition_infos.at(i);
      ObArray<ObString> partition_values;
      bool in_bound = false;
      if (OB_ISNULL(partition_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_info is null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < partition_info->partition_values_.count(); ++j) {
          OZ(partition_values.push_back(partition_info->partition_values_.at(j)));
        }
      }
      ObNewRow ob_part_row;
      if (OB_FAIL(ret)) {
        LOG_WARN("not expected error", K(ret));
      } else if (OB_FAIL(hive::ObHiveTableMetadata::calculate_part_val_from_string(*table_schema,
                                                                                   is_partitioned_,
                                                                                   partition_values,
                                                                                   tmp_allocator,
                                                                                   ob_part_row))) {
        LOG_WARN("failed to calculate partition value", K(ret), K(i), K(partition_info));
      } else {
        if (need_all_ || !is_partitioned_) {
          in_bound = true;
          LOG_TRACE("get one partition: ", K(*partition_info));
        } else if (ob_part_row.get_count() != hive_part_bounds_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_row size should equal with partition_values",
                   K(ob_part_row),
                   K(hive_part_bounds_));
        } else if (check_one_row_part_column(ob_part_row)) {
          bool is_filtered = false;

          if (OB_FAIL(file_filter.filter(ob_part_row, is_filtered))) {
            LOG_WARN("failed to check file filter range");
          } else if (is_filtered) {
            in_bound = false;
            LOG_TRACE("filtered partition: ", K(*partition_info));
          } else {
            in_bound = true;
            LOG_TRACE("get one partition: ", K(*partition_info));
          }
        }
      }

      if (OB_SUCC(ret) && in_bound) {
        int64_t part_index = 1;
        bool partition_exists = false;
        ObPartition **partitions_array = table_schema->get_part_array();
        if (OB_FAIL(selected_part_idxs.push_back(i))) {
          LOG_WARN("failed to push back part index", K(ret));
        }
        for (int64_t j = 0;
             OB_SUCC(ret) && !partition_exists && j < table_schema->get_partition_num();
             ++j) {
          ObPartition *partition = partitions_array[j];
          if (partition->get_list_row_values().count() != 1) {
            // 对于hive来说，每个分区只会有1个值
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Each partition of a hive table should have only one partition value",
                     K(ret),
                     K(partition));
          } else if (ob_part_row == partition->get_list_row_values().at(0)) {
            // 这个分区已经被添加过了，不需要再添加了
            part_index = partition->get_part_id();
            partition_exists = true;
          } else {
            // 找一个没人用的最大的part_id
            part_index = part_index <= partition->get_part_id() ? partition->get_part_id() + 1
                                                                : part_index;
          }
        }

        if (OB_SUCC(ret)) {
          if (!partition_exists) {
            // 添加新的分区信息
            ObTableSchema *tmp_update_table_schema = const_cast<ObTableSchema *>(table_schema);
            ObSqlString tmp_part_name;
            ObString part_name;
            ObPartition partition;
            OX(partition.set_part_id(part_index));
            if (OB_FAIL(tmp_part_name.append_fmt("P%ld", part_index))) {
              LOG_WARN("failed to append part name", K(ret));
            } else if (OB_FAIL(partition.set_external_location(
                           const_cast<ObString &>(partition_info->path_)))) {
              LOG_WARN("failed to set external location", K(ret), K(partition_info->path_));
            } else if (OB_FAIL(ob_write_string(allocator_,
                                               tmp_part_name.string(),
                                               part_name,
                                               true /*c_style*/))) {
              LOG_WARN("failed to write part name", K(ret));
            } else if (OB_FAIL(partition.set_part_name(part_name))) {
              LOG_WARN("set partition name failed", K(ret));
            } else if (OB_FAIL(partition.add_list_row(ob_part_row))) {
              LOG_WARN("add list row failed", K(ret));
            } else if (OB_FAIL(tmp_update_table_schema->add_partition(partition))) {
              LOG_WARN("failed to add partition", K(ret));
            }
            LOG_TRACE("add one partition to schema: ", K(partition));
          }

          if (OB_SUCC(ret)) {
            tmp_part_id.push_back(part_index);
            tmp_part_path.push_back(partition_info->path_);
            tmp_part_modify_ts.push_back(partition_info->modify_ts_);
          }
        }
      }
    }

    // 批量获取分区下的文件列表
    if (tmp_part_path.count() > 0) {
      OZ(ObExternalTableUtils::collect_external_file_list_with_cache(
          table_schema->get_tenant_id(),
          tmp_part_path,
          tmp_part_id,
          tmp_part_modify_ts,
          table_schema->get_external_file_location_access_info(),
          empty_patten,
          allocator_,
          refresh_interval_sec * 1000,
          filtered_files));
      LOG_TRACE("get file list from cache: ", K(filtered_files));
    }

    if (OB_SUCC(ret) && is_partitioned_ && !selected_part_idxs.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_key_column_num(); ++i) {
        ObString *partition_column_name = NULL;
        if (OB_ISNULL(partition_column_name = partition_column_names.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for partition column name", K(ret));
        } else if (OB_FAIL(table_schema->get_part_key_column_name(i, *partition_column_name))) {
          LOG_WARN("failed to get partition column name", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(partition_values_.prepare_allocate(selected_part_idxs.count()))) {
        LOG_WARN("failed to init partition values", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < selected_part_idxs.count(); ++i) {
        const PartitionInfo *partition_info = partition_infos.at(selected_part_idxs.at(i));
        ObString &partition_value = partition_values_.at(i);
        if (OB_ISNULL(partition_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_info is null", K(ret));
        } else if (partition_info->partition_.empty()) {
          if (OB_FAIL(construct_partition_values(allocator_,
                                                  partition_column_names,
                                                  partition_info->partition_values_,
                                                  partition_value))) {
            LOG_WARN("failed to construct partition values", K(ret),
                      K(partition_column_names), K(partition_info->partition_values_));
          }
        } else if (OB_FAIL(ob_write_string(allocator_, partition_info->partition_, partition_value))) {
          LOG_WARN("failed to write partition value", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObHiveFilePruner::check_one_row_part_column(ObNewRow ob_part_row)
{
  bool contain = true;
  for (int64_t i = 0; contain && i < ob_part_row.get_count(); ++i) {
    ObObj &cell = ob_part_row.get_cell(i);
    ObHivePartFieldBound &field_bound = *hive_part_bounds_.at(i);
    if (field_bound.is_always_false_) {
      contain = false;
    } else if (field_bound.is_whole_range_) {
      continue;
    }
    contain = check_one_part(cell, field_bound);
  }

  return contain;
}

bool ObHiveFilePruner::check_one_part(ObObj &part_val, ObHivePartFieldBound &field_bounds)
{
  bool contain = false;
  ObFixedArray<ObFieldBound *, ObIAllocator> bounds = field_bounds.bounds_;
  for (int64_t i = 0; !contain && i < bounds.count(); ++i) {
    ObFieldBound *bound = bounds.at(i);
    if (bound->is_valid_range_) {
      int cmp_lower = part_val.compare(bound->lower_bound_);
      if (cmp_lower == 0 && bound->include_lower_) {
        contain = true;
      } else if (cmp_lower > 0) {
        int cmp_upper = part_val.compare(bound->upper_bound_);
        {
          if ((cmp_upper == 0 && bound->include_upper_) || cmp_upper < 0) {
            contain = true;
          }
        }
      }
    }
  }
  return contain;
}

ObHivePartFieldBound::ObHivePartFieldBound(common::ObIAllocator &allocator)
    : allocator_(allocator), column_id_(OB_INVALID_ID), is_whole_range_(false),
      is_always_false_(false), bounds_(allocator), range_exprs_(allocator)
{
}

void ObHivePartFieldBound::reset()
{
  column_id_ = OB_INVALID_ID;
  is_whole_range_ = false;
  is_always_false_ = false;
  for (int64_t i = 0; i < bounds_.count(); ++i) {
    if (OB_NOT_NULL(bounds_.at(i))) {
      allocator_.free(bounds_.at(i));
    }
  }
  bounds_.reset();
  range_exprs_.reset();
}

int ObHivePartFieldBound::assign(const ObHivePartFieldBound &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    column_id_ = other.column_id_;
    is_whole_range_ = other.is_whole_range_;
    is_always_false_ = other.is_always_false_;
    if (OB_FAIL(bounds_.assign(other.bounds_))) {
      LOG_WARN("failed to assign field bound", K(ret));
    } else if (OB_FAIL(range_exprs_.assign(other.range_exprs_))) {
      LOG_WARN("failed to assign range exprs");
    }
  }
  return ret;
}

int ObHivePartFieldBound::deep_copy(ObHivePartFieldBound &src)
{
  int ret = OB_SUCCESS;
  column_id_ = src.column_id_;
  is_whole_range_ = src.is_whole_range_;
  is_always_false_ = src.is_always_false_;
  if (OB_FAIL(bounds_.init(src.bounds_.count()))) {
    LOG_WARN("failed to init fixed array", K(ret));
  } else if (OB_FAIL(range_exprs_.assign(src.range_exprs_))) {
    LOG_WARN("failed to assign range exprs");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src.bounds_.count(); ++i) {
    ObFieldBound *bound = OB_NEWx(ObFieldBound, &allocator_);
    if (OB_ISNULL(src.bounds_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null filed bound", K(ret));
    } else if (OB_ISNULL(bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObFieldBound", K(ret));
    } else if (OB_FAIL(bound->deep_copy(allocator_, *src.bounds_.at(i)))) {
      LOG_WARN("failed to deep copy field bound", K(ret));
    } else if (OB_FAIL(bounds_.push_back(bound))) {
      LOG_WARN("failed to push back bound", K(ret));
    }
  }
  return ret;
}

int ObHivePushDownFilter::HivePartitionFilterParamBuilder::build(const int32_t ext_tbl_col_id,
                                                                const ObColumnMeta &column_meta,
                                                                blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  param.set_uncertain();

  int64_t part_column_idx = -1;
  for (int i = 0; i < part_column_ids_.count(); ++i) {
    if (ext_tbl_col_id == normalization_column_id(part_column_ids_.at(i))) {
      part_column_idx = i;
      break;
    }
  }

  if (part_column_idx == -1) {
    // 非分区列不做过滤
  } else {
    ObObj null_value;
    null_value.set_int(0);
    if (OB_FAIL(param.null_count_.from_obj_enhance(null_value))) {
      LOG_WARN("failed to form obj enhance");
    } else if (OB_FAIL(param.min_datum_.from_obj_enhance(row_.get_cell(part_column_idx)))) {
      LOG_WARN("failed to form obj enhance");
    } else if (OB_FAIL(param.max_datum_.from_obj_enhance(row_.get_cell(part_column_idx)))) {
      LOG_WARN("failed to form obj enhance");
    } else {
      param.is_min_prefix_ = false;
      param.is_max_prefix_ = false;
    }
  }
  return ret;
}

int ObHivePushDownFilter::filter(ObNewRow row, bool &is_filtered)
{
  int ret = OB_SUCCESS;
  HivePartitionFilterParamBuilder param_builder(row, *part_column_ids_);
  if (OB_FAIL(apply_skipping_index_filter(ObExternalTablePushdownFilter::PushdownLevel::FILE,
                                          param_builder,
                                          is_filtered,
                                          1))) {
    LOG_WARN("fail to apply skipping index filter", K(ret));
  }
  return ret;
}

int ObHiveFilePruner::construct_partition_values(common::ObIAllocator &allocator,
                                                 const common::ObIArray<common::ObString> &partition_column_names,
                                                 const common::ObIArrayWrap<common::ObString> &partition_values,
                                                 common::ObString &partition_value_str)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(partition_column_names.count() != partition_values.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition column names count not match partition values count", K(ret),
             K(partition_column_names.count()), K(partition_values.count()));
  } else if (partition_column_names.empty()) {
    partition_value_str.reset();
  } else {
    SMART_VAR(char[OB_MAX_PARTITION_EXPR_LENGTH], temp_buf) {
      int64_t pos = 0;
      const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;

      for (int64_t i = 0; OB_SUCC(ret) && i < partition_column_names.count(); ++i) {
        if (0 != i) {
          if (OB_FAIL(databuff_printf(temp_buf, buf_len, pos, "/"))) {
            LOG_WARN("failed to add separator", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          const ObString &col_name = partition_column_names.at(i);
          const ObString &value = partition_values.at(i);

          if (OB_FAIL(databuff_printf(temp_buf, buf_len, pos, "%.*s=%.*s",
                                      col_name.length(), col_name.ptr(),
                                      value.length(), value.ptr()))) {
            LOG_WARN("failed to add column name and value", K(ret), K(col_name), K(value));
          }
        }
      }

      if (OB_SUCC(ret)) {
        char *result_buf = NULL;
        if (OB_ISNULL(result_buf = static_cast<char*>(allocator.alloc(pos + 1)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for partition value string", K(ret), K(pos));
        } else {
          MEMCPY(result_buf, temp_buf, pos);
          result_buf[pos] = '\0';
          partition_value_str.assign_ptr(result_buf, static_cast<int32_t>(pos));
        }
      }
    }
  }

  return ret;
}

int ObHiveFilePruner::get_part_id_and_range_exprs(ObIArray<uint64_t> &part_column_ids,
                                                  ObIArray<ObRawExpr*> &range_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < hive_part_bounds_.count(); ++i) {
    ObHivePartFieldBound *part_field_bound = hive_part_bounds_.at(i);
    if (OB_ISNULL(part_field_bound)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part field bound is null");
    } else if (OB_FAIL(part_column_ids.push_back(part_field_bound->column_id_))) {
      LOG_WARN("failed to push back part column id");
    } else if (OB_FAIL(append(range_exprs, part_field_bound->range_exprs_))) {
      LOG_WARN("failed to append range exprs");
    }
  }
  return ret;
}