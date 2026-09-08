/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/file_prune/ob_ext_file_pruner.h"

#include "sql/resolver/dml/ob_hint.h"  // ObOptParamHint::EXT_TABLE_OPTIONS
#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"
#include "plugin/v2/external_table/ob_ext_table_metadata.h"
#include "plugin/v2/host/ob_ext_host_provider.h"
#include "plugin/v2/host/ob_ext_malloc_guard.h"
#include "plugin/v2/external_table/ob_ext_plugin_util.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // build_options_json, parse_scan_tasks_json
#include "share/rc/ob_tenant_base.h"
#include "share/object/ob_obj_cast.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"

#include <string>

namespace oceanbase
{
namespace sql
{
using namespace ext_plugin;

int ObExtFilePruner::cast_task_partition_value(
    const share::ObExtTaskPartitionValue &source,
    const ObColumnMeta &column_meta,
    ObObj &result)
{
  int ret = OB_SUCCESS;
  ObObj string_value;
  ObObj casted;
  if (source.is_null_) {
    result.set_null();
  } else {
    string_value.set_varchar(source.value_);
    string_value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObCastCtx cast_ctx(&allocator_, nullptr, CM_NONE, column_meta.cs_type_);
    if (OB_FAIL(ObObjCaster::to_type(
            column_meta.type_, column_meta.cs_type_, cast_ctx, string_value, casted))) {
      LOG_WARN("failed to cast plugin partition value", K(ret), K(source.field_id_));
    } else if (OB_FAIL(ob_write_obj(allocator_, casted, result))) {
      LOG_WARN("failed to copy plugin partition value", K(ret), K(source.field_id_));
    }
  }
  return ret;
}

int ObExtFilePruner::build_task_partition_info(
    const share::ObExtScanTask &task,
    const ObIArray<uint64_t> &partition_col_ids,
    const ObIArray<int64_t> &partition_col_idxs,
    const int64_t part_id,
    share::ObExternalTablePartInfo &part_info)
{
  int ret = OB_SUCCESS;
  share::ObExtTaskPartitionValues values;
  ObObj *cells = nullptr;
  if (OB_FAIL(share::ObExtTaskPartitionValues::parse(
          allocator_, task.task_json, task.task_json_len, values))) {
    LOG_WARN("failed to parse task partition values", K(ret));
  } else if (partition_col_ids.empty()) {
    if (0 != values.count_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unpartitioned plugin task has partition values", K(ret), K(values.count_));
    }
  } else if (values.count_ != partition_col_ids.count()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("plugin task partition values do not match schema", K(ret),
             K(values.count_), K(partition_col_ids.count()));
  } else if (!partition_col_ids.empty()) {
    cells = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * partition_col_ids.count()));
    if (OB_ISNULL(cells)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate plugin partition row", K(ret), K(partition_col_ids.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_ids.count(); ++i) {
        new (&cells[i]) ObObj();
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_ids.count(); ++i) {
      const uint64_t column_id = partition_col_ids.at(i);
      const share::ObExtTaskPartitionValue &source = values.values_[i];
      const int64_t column_idx = partition_col_idxs.at(i);
      if (column_id < OB_APP_MIN_COLUMN_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid plugin partition column id", K(ret), K(column_id), K(i));
      } else {
        const int64_t expected_field_id = column_id - OB_APP_MIN_COLUMN_ID;
        if (expected_field_id != source.field_id_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("plugin task partition field id does not match schema", K(ret),
                   K(expected_field_id), K(source.field_id_), K(column_id), K(i));
        }
        if (OB_SUCC(ret)) {
          // Unreferenced partition columns have no query column meta; leave their cells unconverted.
          if (OB_INVALID_INDEX != column_idx) {
            if (OB_UNLIKELY(column_idx >= column_metas_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("plugin partition column meta is missing", K(ret), K(column_idx),
                       K(column_metas_.count()));
            } else {
              const ObColumnMeta &column_meta = column_metas_.at(column_idx);
              if (OB_FAIL(cast_task_partition_value(source, column_meta, cells[i]))) {
                LOG_WARN("failed to cast plugin partition value", K(ret), K(column_id), K(i));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      part_info.part_id_ = part_id;
      part_info.list_row_value_.assign(cells, partition_col_ids.count());
    }
  }
  return ret;
}

int ObExtFilePruner::append_scan_task_splits(
    const share::ObExtScanTask &task,
    const int64_t part_id,
    const ObPluginReaderType reader_type,
    const share::ObExtFileScanDescriptor *ob_file_scan,
    ObIArray<ObPluginSplitDesc *> &splits)
{
  int ret = OB_SUCCESS;
  int64_t split_count = 1;
  if (ObPluginReaderType::PLUGIN != reader_type) {
    if (OB_ISNULL(ob_file_scan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("plugin OB file scan descriptor is null", K(ret), K(reader_type));
    } else {
      split_count = ob_file_scan->file_count_;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < split_count; ++i) {
    ObPluginSplitDesc *desc = OB_NEWx(ObPluginSplitDesc, &allocator_);
    if (OB_ISNULL(desc)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc plugin split", K(ret), K(i));
    } else {
      desc->part_id_ = part_id;
      desc->reader_type_ = reader_type;
      if (ObPluginReaderType::PLUGIN == reader_type) {
        desc->record_count_ = task.row_count > 0 ? task.row_count : 0;
        if (OB_FAIL(ob_write_string(
                allocator_,
                ObString(task.task_json_len, task.task_json),
                desc->plugin_task_json_))) {
          LOG_WARN("failed to copy plugin task json", K(ret));
        }
      } else {
        desc->file_size_ = ob_file_scan->files_[i].byte_size_;
        // Preserve the per-file count while lowering one plugin split into physical
        // file tasks. The root split count covers all files and cannot be copied to
        // each file without double counting.
        desc->record_count_ = ob_file_scan->files_[i].row_count_;
        if (OB_FAIL(ob_write_string(
                allocator_, ob_file_scan->files_[i].file_path_, desc->file_url_))) {
          LOG_WARN("failed to copy plugin OB file url", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(splits.push_back(desc))) {
          LOG_WARN("failed to push plugin split", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObExtFilePruner::build_plugin_splits(
    const char *tasks_json,
    const int32_t tasks_len,
    const ObIArray<uint64_t> &partition_col_ids,
    const ObIArray<ObRawExpr *> &partition_filter_exprs,
    const ObString &predicate_json,
    ObIArray<ObPluginSplitDesc *> &splits)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_alloc("ExtPrunerTask");
  share::ObExtScanTaskArray scan_tasks = share::ObExtScanTaskArray();
  share::ObExtFileScanDescriptor *first_ob_file_scan = nullptr;
  ObPluginReaderType reader_type = ObPluginReaderType::PLUGIN;
  share::ObExternalTablePartInfo part_info;
  ObSEArray<int64_t, 4> partition_col_idxs;
  if (OB_FAIL(share::parse_scan_tasks_json(task_alloc, tasks_json, tasks_len, scan_tasks))) {
    LOG_WARN("failed to parse scan tasks json", K(ret));
  } else {
    if (scan_tasks.partition_filter_applied) {
      if (OB_FAIL(proven_part_column_ids_.assign(partition_col_ids))) {
        LOG_WARN("failed to store plugin planning proof columns", K(ret));
      } else if (OB_FAIL(proven_part_filter_exprs_.assign(partition_filter_exprs))) {
        LOG_WARN("failed to store plugin planning proof expressions", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (scan_tasks.count > 0) {
        if (OB_FAIL(share::ObExtFileScanDescriptor::parse(
                task_alloc,
                scan_tasks.tasks[0].task_json,
                scan_tasks.tasks[0].task_json_len,
                first_ob_file_scan))) {
          LOG_WARN("failed to parse plugin OB file scan descriptor", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(first_ob_file_scan)) {
        if (share::ObExtFileScanFormat::PARQUET == first_ob_file_scan->file_format_) {
          reader_type = ObPluginReaderType::OB_PARQUET;
        } else if (share::ObExtFileScanFormat::ORC == first_ob_file_scan->file_format_) {
          reader_type = ObPluginReaderType::OB_ORC;
        } else {
          ret = OB_INVALID_DATA;
          LOG_WARN("unsupported plugin OB file format",
                   K(ret), K(first_ob_file_scan->file_format_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!partition_col_ids.empty() && scan_tasks.count > 0) {
        if (OB_FAIL(partition_infos_.reserve(scan_tasks.count))) {
          LOG_WARN("failed to reserve plugin partition infos", K(ret), K(scan_tasks.count));
        }
        // Column metadata is fixed for this plan. Map each partition column to its
        // column_ids_/column_metas_ index once and reuse the mapping for all tasks.
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_ids.count(); ++i) {
          int64_t column_idx = OB_INVALID_INDEX;
          for (int64_t j = 0; OB_INVALID_INDEX == column_idx && j < column_ids_.count(); ++j) {
            if (partition_col_ids.at(i) == column_ids_.at(j)) {
              column_idx = j;
            }
          }
          if (OB_FAIL(partition_col_idxs.push_back(column_idx))) {
            LOG_WARN("failed to store plugin partition column index", K(ret), K(i));
          }
        }
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < scan_tasks.count; ++i) {
      const int64_t part_id = partition_col_ids.empty() ? 0 : i + 1;
      if (OB_FAIL(build_task_partition_info(
              scan_tasks.tasks[i], partition_col_ids, partition_col_idxs, part_id, part_info))) {
        LOG_WARN("failed to build plugin task partition info", K(ret), K(i));
      } else if (!partition_col_ids.empty()) {
        if (OB_FAIL(partition_infos_.set_part_pair_by_idx(i, part_info))) {
          LOG_WARN("failed to store plugin task partition info", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("ext pruner plan_create result", K(predicate_json),
               "predicate_pushed", !predicate_json.empty(),
               "partition_filter_applied", scan_tasks.partition_filter_applied,
               "scan_task_count", scan_tasks.count, K(reader_type));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < scan_tasks.count; ++i) {
      const share::ObExtScanTask &task = scan_tasks.tasks[i];
      const int64_t part_id = partition_col_ids.empty() ? 0 : i + 1;
      share::ObExtFileScanDescriptor *ob_file_scan = nullptr;
      if (0 == i) {
        ob_file_scan = first_ob_file_scan;
      } else if (OB_FAIL(share::ObExtFileScanDescriptor::parse(
                     task_alloc, task.task_json, task.task_json_len, ob_file_scan))) {
        LOG_WARN("failed to parse plugin OB file scan descriptor", K(ret), K(i));
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(first_ob_file_scan) != OB_ISNULL(ob_file_scan)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("plugin OB file scan plan is not atomic", K(ret), K(i));
        } else if (OB_NOT_NULL(ob_file_scan)
                   && ob_file_scan->file_format_ != first_ob_file_scan->file_format_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("plugin OB file scan plan has mixed formats", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append_scan_task_splits(
                task, part_id, reader_type, ob_file_scan, splits))) {
          LOG_WARN("failed to append plugin scan-task splits", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

ObExtFilePruner::ObExtFilePruner(ObIAllocator &allocator)
    : ObILakeTableFilePruner(allocator), table_uri_(), access_info_(),
      plugin_format_(share::ObLakeTableFormat::INVALID), ext_metadata_(nullptr), filter_exprs_(),
      proven_part_column_ids_(), proven_part_filter_exprs_(), partition_infos_(allocator),
      exec_ctx_(nullptr)
{
}

int ObExtFilePruner::copy_partition_infos_to(
    ObIAllocator &target_allocator,
    share::ObExternalTablePartInfoArray &target) const
{
  int ret = OB_SUCCESS;
  const int64_t partition_info_count = partition_infos_.count();
  share::ObExternalTablePartInfo target_info;
  if (partition_info_count > 0 && OB_FAIL(target.reserve(partition_info_count))) {
    LOG_WARN("failed to reserve plugin partition infos", K(ret), K(partition_info_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_info_count; ++i) {
    const share::ObExternalTablePartInfo &source = partition_infos_.at(i);
    target_info.part_id_ = source.part_id_;
    if (OB_FAIL(ob_write_row(
            target_allocator, source.list_row_value_, target_info.list_row_value_))) {
      LOG_WARN("failed to copy plugin partition row", K(ret), K(i));
    } else if (OB_FAIL(ob_write_string(
                   target_allocator, source.partition_spec_, target_info.partition_spec_))) {
      LOG_WARN("failed to copy plugin partition spec", K(ret), K(i));
    } else if (OB_FAIL(target.set_part_pair_by_idx(i, target_info))) {
      LOG_WARN("failed to store plugin partition info", K(ret), K(i));
    }
  }
  return ret;
}

int ObExtFilePruner::clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const
{
  int ret = OB_SUCCESS;
  pruner = nullptr;
  ObExtFilePruner *tmp = nullptr;
  if (OB_ISNULL(tmp = OB_NEWx(ObExtFilePruner, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObExtFilePruner");
  } else if (OB_FAIL(tmp->assign(*this))) {
    LOG_WARN("failed to assign ext file pruner");
  } else {
    pruner = tmp;
  }
  return ret;
}

int ObExtFilePruner::assign(const ObILakeTableFilePruner &o)
{
  int ret = OB_SUCCESS;
  const ObExtFilePruner &other = static_cast<const ObExtFilePruner &>(o);
  if (OB_FAIL(ObILakeTableFilePruner::assign(other))) {
    LOG_WARN("failed to assign ObILakeTableFilePruner", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, other.table_uri_, table_uri_))) {
    LOG_WARN("failed to deep copy table uri", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, other.access_info_, access_info_))) {
    LOG_WARN("failed to deep copy access info", K(ret));
  } else if (OB_FAIL(
                 ob_write_string(allocator_, other.ext_options_hint_, ext_options_hint_, true))) {
    LOG_WARN("failed to deep copy ext_options hint", K(ret));
  } else if (OB_FAIL(proven_part_column_ids_.assign(other.proven_part_column_ids_))) {
    LOG_WARN("failed to copy proven partition column ids", K(ret));
  } else if (OB_FAIL(proven_part_filter_exprs_.assign(other.proven_part_filter_exprs_))) {
    LOG_WARN("failed to copy proven partition filter exprs", K(ret));
  } else if (OB_FAIL(other.copy_partition_infos_to(allocator_, partition_infos_))) {
    LOG_WARN("failed to copy plugin partition infos", K(ret));
  } else {
    plugin_format_ = other.plugin_format_;
    ext_metadata_ = other.ext_metadata_;
    exec_ctx_ = other.exec_ctx_;
  }
  // filter_exprs_ are optimizer-stage pointers, not deep-copied in assign
  return ret;
}

int ObExtFilePruner::get_part_id_and_range_exprs(
    ObIArray<uint64_t> &part_column_ids,
    ObIArray<ObRawExpr *> &range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_column_ids.assign(proven_part_column_ids_))) {
    LOG_WARN("failed to assign proven partition column ids", K(ret));
  } else if (OB_FAIL(range_exprs.assign(proven_part_filter_exprs_))) {
    LOG_WARN("failed to assign proven partition filter exprs", K(ret));
  }
  return ret;
}

int ObExtFilePruner::init(const ObDMLStmt &stmt,
                          ObExecContext *exec_ctx,
                          const uint64_t table_id,
                          const uint64_t ref_table_id,
                          const ext_plugin::ObExtTableMetadata *ext_metadata,
                          const ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ext file pruner already inited", K(ret));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    if (OB_ISNULL(ext_metadata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ext table metadata is null", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ext_metadata->get_table_location(),
                                       table_uri_, true))) {
      LOG_WARN("failed to deep copy table uri", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ext_metadata->get_access_info(),
                                       access_info_, true))) {
      LOG_WARN("failed to deep copy access info", K(ret));
    } else if (OB_FAIL(generate_column_meta_info(stmt))) {
      LOG_WARN("failed to generate column meta info", K(ret));
    } else if (OB_FAIL(filter_exprs_.assign(filter_exprs))) {
      LOG_WARN("failed to store filter exprs", K(ret));
    } else {
      plugin_format_ = ext_metadata->get_format_type();
      ext_metadata_ = ext_metadata;
      exec_ctx_ = exec_ctx;
      inited_ = true;
      // Pull the per-query EXT_TABLE_OPTIONS opt_param hint (an opaque JSON
      // string) and deep-copy it; it is later passed verbatim into the plugin's
      // options_json under OB_EXT_K_EXT_OPTIONS. OB never inspects the contents.
      // A missing/invalid hint simply leaves ext_options_hint_ empty (no tuning).
      if (OB_NOT_NULL(stmt.get_query_ctx())) {
        ObObj opt_val;
        int64_t opt_idx = common::OB_INVALID_INDEX;
        if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_opt_param(
                ObOptParamHint::EXT_TABLE_OPTIONS, opt_val, &opt_idx))) {
          LOG_WARN("failed to get ext_table_options hint", K(ret));
          ret = OB_SUCCESS;  // non-fatal: scan without per-query tuning
          ext_options_hint_.reset();
        } else if (opt_idx != common::OB_INVALID_INDEX && opt_val.is_varchar()) {
          if (OB_FAIL(ob_write_string(allocator_, opt_val.get_varchar(),
                                      ext_options_hint_, true))) {
            LOG_WARN("failed to deep copy ext_table_options hint", K(ret));
            ret = OB_SUCCESS;  // non-fatal
            ext_options_hint_.reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObExtFilePruner::prune_ext_splits(ObExecContext &exec_ctx,
                                      ObIArray<ObPluginSplitDesc *> &splits,
                                      ObExtTableDispatchMode &dispatch_mode)
{
  int ret = OB_SUCCESS;
  dispatch_mode = ObExtTableDispatchMode::ROUND_ROBIN;
  proven_part_column_ids_.reset();
  proven_part_filter_exprs_.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ext file pruner not inited", K(ret));
  } else {
    const share::ObPluginSlot slot = share::lake_plugin_slot_of(plugin_format_);
    const ObExtTablePluginApi *api =
        share::ObExtFormatRegistry::get_instance().get_plugin_by_slot(slot);
    if (OB_ISNULL(api) || OB_ISNULL(api->plan_create) || OB_ISNULL(api->tasks_destroy)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "external table plugin not loaded");
      LOG_WARN("ext plugin not loaded for format", K(ret), K(plugin_format_), K(slot));
    } else {
      const char *fmt = (OB_NOT_NULL(api->format_name)) ? api->format_name() : nullptr;
      ObExtMallocGuard guard(fmt);  // tag plugin mallocs from plan_create

      ObExtHostCtx host_ctx;
      if (OB_FAIL(host_ctx.default_fs.set_path(table_uri_))) {
        LOG_WARN("failed to set fs path", K(ret));
      } else if (OB_FAIL(host_ctx.default_fs.set_access_info(access_info_))) {
        LOG_WARN("failed to set fs access info", K(ret));
      }
      host_ctx.default_fs.set_cache_options(ObExternalFileCacheOptions(true /*enable_page_cache*/,
                                                                       true /*enable_disk_cache*/));
      host_ctx.default_fs.set_tenant_id(MTL_ID());
      host_ctx.select_arrow_pool(true);
      host_ctx.pool->set_attr(get_ext_mem_attr(fmt));
      host_ctx.executor = nullptr;  // planning runs inline
      ObExtTableHostApi host;
      build_ext_host_api(host, &host_ctx);

      // options: location + access info + optional catalog_context (T0 opaque
      // blob from load_schema) + optional ext_options hint. catalog_context and
      // ext_options are embedded as raw JSON objects (no quoting/escaping).
      // Arena copies are NUL-terminated and valid for the whole stage.
      const ObString &loc_str = host_ctx.default_fs.path();
      const ObString &acc_str = host_ctx.default_fs.access_info();
      const ObString &catalog_ctx = ext_metadata_->get_catalog_context_json();
      const bool has_catalog_ctx = !catalog_ctx.empty();
      const bool has_ext_options = !ext_options_hint_.empty();
      const char *keys[4] = {"location", "access_info", nullptr, nullptr};
      const char *vals[4] = {loc_str.ptr(), acc_str.ptr(), nullptr, nullptr};
      const char *raw_keys[2] = {nullptr, nullptr};
      int32_t opt_count = 2;
      int32_t raw_count = 0;
      if (has_catalog_ctx) {
        keys[opt_count] = OB_EXT_K_CATALOG_CONTEXT;
        vals[opt_count] = catalog_ctx.ptr();
        raw_keys[raw_count++] = OB_EXT_K_CATALOG_CONTEXT;
        ++opt_count;
      }
      if (has_ext_options) {
        keys[opt_count] = OB_EXT_K_EXT_OPTIONS;
        vals[opt_count] = ext_options_hint_.ptr();
        raw_keys[raw_count++] = OB_EXT_K_EXT_OPTIONS;
        ++opt_count;
      }
      ObString options_json;
      ObArenaAllocator opt_alloc("ExtPrunerOpt");
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(share::build_options_json(opt_alloc, keys, vals, opt_count,
                                                   options_json,
                                                   raw_keys, raw_count))) {
        LOG_WARN("failed to build options json", K(ret));
      } else {
        // predicate_json carries the complete convertible scan predicate.
        // partition_filter_json is an exact partition-only planning candidate.
        // Residuals are removable only after plan_create reports partition_filter_applied.
        // On conversion failure OB evaluates all filters.
        ObString predicate_json;
        ObString partition_filter_json;
        ObSEArray<ObRawExpr *, 4> partition_filter_exprs;
        const common::ObIArray<uint64_t> &part_col_ids = ext_metadata_->get_partition_col_ids();
        if (OB_FAIL(ext_predicate::build_predicate_json_from_raw_expr(
                allocator_, exec_ctx_, filter_exprs_, part_col_ids, predicate_json,
                partition_filter_json, partition_filter_exprs))) {
          LOG_WARN("failed to build predicate json, scanning without pushdown",
                   K(ret), K(part_col_ids.count()));
          ret = OB_SUCCESS;  // degrade gracefully: no pushdown, OB filters rows
          predicate_json.reset();
          partition_filter_json.reset();
          partition_filter_exprs.reset();
        }
        const char *predicate_cstr =
            predicate_json.empty() ? nullptr : predicate_json.ptr();
        const char *partition_filter_cstr =
            partition_filter_json.empty() ? nullptr : partition_filter_json.ptr();
        char *tasks_json = nullptr;
        int32_t tasks_len = 0;
        // desired_task_count is a PX parallelism hint the plugin may ignore.
        // The plugin returns an OB errno verbatim and logs its own diagnostic
        // (with plugin-side source location) via host->log before returning —
        // grep "[ExtPlugin]" in observer.log for the stack.
        int rc = api->plan_create(loc_str.ptr(), options_json.ptr(),
                                  partition_filter_cstr, predicate_cstr,
                                  /*limit*/ -1, /*desired_task_count*/ 1,
                                  &host, &tasks_json, &tasks_len);
        if (rc != OB_SUCCESS || OB_ISNULL(tasks_json) || tasks_len <= 0) {
          ret = (rc != OB_SUCCESS) ? rc : OB_ERR_UNEXPECTED;
          LOG_WARN("plugin plan_create failed", K(ret));
        } else if (OB_FAIL(build_plugin_splits(
                       tasks_json, tasks_len, part_col_ids, partition_filter_exprs,
                       predicate_json, splits))) {
          LOG_WARN("failed to build plugin splits", K(ret));
        }
        // Release the plugin's output buffer via the plugin's tasks_destroy (the
        // plugin owns the release — it may be static/own-alloc/host-alloc).
        ob_ext_tasks_destroy(api, tasks_json, tasks_len, &host);
      }
    }
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
