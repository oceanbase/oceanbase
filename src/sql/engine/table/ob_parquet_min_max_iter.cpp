/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_parquet_min_max_iter.h"

#include "lib/ob_running_mode.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_compatibility_control.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase {
namespace sql {

static bool is_contain_field_id(std::shared_ptr<parquet::FileMetaData> file_meta)
{
  bool contains_id = true;
  for (int i = 0; i < file_meta->schema()->num_columns() && contains_id; ++i) {
    const parquet::schema::Node *root_node = file_meta->schema()->GetColumnRoot(i);
    if (root_node->field_id() < 0) {
      contains_id = false;
    }
  }
  return contains_id;
}

ObParquetMinMaxIter::~ObParquetMinMaxIter()
{
  if (nullptr != bit_vector_cache_) {
    allocator_.free(bit_vector_cache_);
    bit_vector_cache_ = nullptr;
  }
  file_prebuffer_.destroy();
}

int ObParquetMinMaxIter::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
  mem_attr_ = ObMemAttr(MTL_ID(), "PqMmIter");
  allocator_.set_attr(mem_attr_);
  arrow_alloc_.init(MTL_ID());
  options_ = ObExternalTableAccessOptions::lazy_defaults();
  if (OB_SUCC(ret) && options_.enable_prebuffer_) {
    OZ(file_prebuffer_.init(options_.cache_options_, scan_param->timeout_));
  }
  if (OB_FAIL(ret)) {
  } else {
    OZ(ObExternalTableRowIterator::init(scan_param));
    for (int64_t i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      ObExpr *meta_expr = file_meta_column_exprs_.at(i);
      if (OB_ISNULL(meta_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null meta expr");
      } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        need_partition_info_ = true;
        break;
      }
    }
    const ExprFixedArray *agg_exprs = scan_param->aggregate_exprs_;
    if (OB_ISNULL(agg_exprs) || agg_exprs->empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate exprs is null or empty in aggregate pushdown mode", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs->count(); ++i) {
      ObExpr *agg_expr = agg_exprs->at(i);
      if (OB_ISNULL(agg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregate expr is null", K(ret), K(i));
      } else if (T_FUN_SYS_EXT_MIN != agg_expr->type_ && T_FUN_SYS_EXT_MAX != agg_expr->type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(
            OB_NOT_SUPPORTED,
            "aggregate pushdown for non ext_min/ext_max functions on parquet external table");
        LOG_WARN("only ext_min/ext_max aggregate pushdown is supported for parquet",
                 K(ret),
                 K(agg_expr->type_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    OZ(file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ(file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
  }
  if (OB_FAIL(ret)) {
  } else {
    bool insensitive_feature_enabled = false;
    uint64_t compat_version = 0;
    if (OB_FAIL(scan_param->op_->get_eval_ctx().exec_ctx_.get_my_session()->get_compatibility_version(
            compat_version))) {
      LOG_WARN("failed to get compatibility version", K(ret));
    } else if (OB_FAIL(ObCompatControl::check_feature_enable(
                   compat_version,
                   ObCompatFeatureType::EXTERNAL_COLUMN_NAME_CASE_INSENSITIVE,
                   insensitive_feature_enabled))) {
      LOG_WARN("failed to check feature enable", K(ret));
    } else if (insensitive_feature_enabled) {
      is_col_name_case_sensitive_
          = scan_param->external_file_format_.parquet_format_.column_name_case_sensitive_;
    } else {
      is_col_name_case_sensitive_ = true;
    }
  }
  return ret;
}

void ObParquetMinMaxIter::reset()
{
  state_.reuse();
  url_.reuse();
  file_meta_.reset();
  file_reader_.reset();
  file_prebuffer_.destroy();
  agg_res_datums_.reset();
}

int ObParquetMinMaxIter::create_file_reader(const ObString &data_path,
                                            const ObString file_content_digest,
                                            const int64_t file_size,
                                            const int64_t modify_time)
{
  int ret = OB_SUCCESS;
  arrow::Status status = arrow::Status::OK();
  BEGIN_CATCH_EXCEPTIONS
  std::shared_ptr<ObArrowFile> cur_file = std::make_shared<ObArrowFile>(data_access_driver_,
                                                                        data_path.ptr(),
                                                                        &arrow_alloc_);
  ObExternalFileUrlInfo file_info(scan_param_->external_file_location_,
                                  scan_param_->external_file_access_info_,
                                  data_path,
                                  file_content_digest,
                                  file_size,
                                  modify_time);
  ObExternalFileCacheOptions cache_options(options_.enable_memory_cache_,
                                           options_.enable_disk_cache_);
  if (options_.enable_prebuffer_) {
    cur_file->set_file_prebuffer(&file_prebuffer_);
  }
  cur_file->set_timeout_timestamp(scan_param_->timeout_);
  read_props_.enable_buffered_stream();
  if (OB_FAIL(cur_file.get()->open(file_info, cache_options))) {
    LOG_WARN("failed to open file", K(ret));
  } else {
    file_reader_ = parquet::ParquetFileReader::Open(cur_file, read_props_);
    if (!file_reader_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create parquet reader failed", K(ret));
    } else {
      file_meta_ = file_reader_->metadata();
    }
  }
  END_CATCH_EXCEPTIONS
  return ret;
}

int ObParquetMinMaxIter::next_file()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObString location = scan_param_->external_file_location_;
  int64_t task_idx = 0;
  int64_t file_size = -1;
  ObFileScanTask *scan_task = nullptr;

  do {
    ret = OB_SUCCESS;
    file_meta_.reset();
    file_reader_.reset();
    if ((task_idx = state_.file_idx_++) >= scan_param_->scan_tasks_.count()) {
      ret = OB_ITER_END;
    } else {
      state_.is_delete_file_loaded_ = false;
      state_.cur_row_group_begin_row_id_ = 0;
      file_size = -1;
      int64_t modify_time = 0;
      ObString file_content_digest;
      url_.reuse();

      scan_task = static_cast<ObFileScanTask *>(scan_param_->scan_tasks_.at(task_idx));
      if (OB_ISNULL(scan_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null scan task", K(ret), K(task_idx));
      } else {
        state_.cur_file_url_ = scan_task->file_url_;
      }

      if (OB_FAIL(ret)) {
      } else if (!share::ObExternalTableUtils::is_abs_url(state_.cur_file_url_)) {
        const char *split_char = "/";
        OZ(url_.append_fmt("%.*s%s%.*s",
                           location.length(),
                           location.ptr(),
                           (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                           state_.cur_file_url_.length(),
                           state_.cur_file_url_.ptr()));
      } else {
        OZ(url_.assign(state_.cur_file_url_));
      }

      if (OB_FAIL(ret)) {
      } else if (is_dummy_file(state_.cur_file_url_)) {
        // dummy file
      } else {
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
          file_size = scan_task->file_size_;
          modify_time = scan_task->modification_time_;
          file_content_digest = scan_task->content_digest_;
        }
        if (file_size < 0 || modify_time <= 0) {
          OZ(ObExternalTableUtils::collect_file_basic_info(
              scan_param_->external_file_location_,
              scan_param_->external_file_access_info_,
              url_.string(),
              allocator_,
              file_size,
              modify_time,
              file_content_digest));
        }
        if (OB_FAIL(ret)) {
        } else if (file_size > 0) {
          OZ(create_file_reader(url_.string(), file_content_digest, file_size, modify_time));
          if (OB_SUCC(ret)) {
            ObString expr_file_url;
            if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
              ObSqlString full_name;
              if (ip_port_.empty()) {
                OZ(gen_ip_port(allocator_));
              }
              OZ(full_name.append_fmt("%.*s%%%.*s",
                                      ip_port_.length(),
                                      ip_port_.ptr(),
                                      state_.cur_file_url_.length(),
                                      state_.cur_file_url_.ptr()));
              OZ(ob_write_string(allocator_, full_name.string(), expr_file_url));
            } else {
              expr_file_url = state_.cur_file_url_;
            }
            for (int i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
              file_url_ptrs_.at(i) = expr_file_url.ptr();
              file_url_lens_.at(i) = expr_file_url.length();
            }
          }
        }
      }
      LOG_DEBUG("current external file", K(url_), K(ret));
    }
  } while (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret
           || (OB_SUCC(ret) && 0 == file_size));

  if (OB_SUCC(ret)) {
    int64_t part_id = scan_task->part_id_;
    if (need_partition_info_ && part_id != 0 && state_.part_id_ != part_id) {
      state_.part_id_ = part_id;
      bool is_external_object = is_external_object_id(scan_param_->table_param_->get_table_id());
      if (OB_LIKELY(is_external_object)) {
        OZ(calc_file_part_list_value_by_array(part_id,
                                              allocator_,
                                              scan_param_->partition_infos_,
                                              state_.part_list_val_));
      } else {
        OZ(calc_file_partition_list_value(part_id, allocator_, state_.part_list_val_));
      }
    }

    state_.cur_file_id_ = scan_task->file_id_;
    state_.cur_row_group_idx_ = scan_task->first_lineno_;
    state_.end_row_group_idx_ = scan_task->last_lineno_;
    if (OB_NOT_NULL(file_meta_.get())) {
      state_.end_row_group_idx_
          = std::min(static_cast<int64_t>(file_meta_->num_row_groups()), state_.end_row_group_idx_);
    }
    column_index_type_ = scan_param_->external_file_format_.parquet_format_.column_index_type_;
    if (OB_SUCC(ret) && OB_NOT_NULL(file_meta_.get())) {
      if (is_iceberg_lake_table() && is_contain_field_id(file_meta_)) {
        column_index_type_ = sql::ColumnIndexType::ID;
      }
    }
  }
  return ret;
}

int ObParquetMinMaxIter::compute_column_id_by_index_type(int index, int &file_col_id)
{
  int ret = OB_SUCCESS;
  file_col_id = -1;
  switch (column_index_type_) {
    case sql::ColumnIndexType::ID: {
      int64_t target_field_id = file_column_exprs_.at(index)->extra_;
      for (int j = 0; j < file_meta_->schema()->num_columns(); ++j) {
        const parquet::schema::Node *root_node = file_meta_->schema()->GetColumnRoot(j);
        if (root_node->field_id() == target_field_id) {
          file_col_id = j;
          break;
        }
      }
      break;
    }
    case sql::ColumnIndexType::NAME: {
      ObDataAccessPathExtraInfo *data_access_info =
          static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(index)->extra_info_);
      if (OB_ISNULL(data_access_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data access info is null", K(ret), K(index));
      } else {
        for (int i = 0; i < file_meta_->schema()->num_columns(); i++) {
          const std::string &field_path = file_meta_->schema()->GetColumnRoot(i)->name();
          ObString field_path_obstr(field_path.length(), field_path.c_str());
          if (!is_col_name_case_sensitive_
                  ? data_access_info->data_access_path_.case_compare(field_path_obstr) == 0
                  : data_access_info->data_access_path_.compare(field_path_obstr) == 0) {
            file_col_id = i;
            break;
          }
        }
      }
      break;
    }
    case sql::ColumnIndexType::POSITION: {
      file_col_id = file_column_exprs_.at(index)->extra_ - 1;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unknown column_index_type", K(ret), K(column_index_type_));
      break;
  }
  return ret;
}

int ObParquetMinMaxIter::collect_partition_aggregate()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *agg_exprs = scan_param_->aggregate_exprs_;
  if (OB_ISNULL(agg_exprs) || agg_exprs->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate exprs is null or empty in aggregate pushdown mode", K(ret));
  } else if (OB_FAIL(agg_res_datums_.prepare_allocate(agg_exprs->count()))) {
    LOG_WARN("failed to prepare allocate agg_res_datums", K(ret), K(agg_exprs->count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs->count(); ++i) {
    agg_res_datums_.at(i).set_null();
  }
  bool first_file = true;
  int64_t cur_part_id = 0;
  int64_t file_cnt = 0;
  while (OB_SUCC(ret)) {
    if (!first_file) {
      int64_t next_idx = state_.file_idx_;
      if (next_idx >= scan_param_->scan_tasks_.count()) {
        break;
      }
      ObFileScanTask *next_task = static_cast<ObFileScanTask *>(scan_param_->scan_tasks_.at(next_idx));
      if (OB_NOT_NULL(next_task) && next_task->part_id_ != cur_part_id) {
        LOG_TRACE("partition boundary hit, stop collecting",
                  K(cur_part_id),
                  K(next_task->part_id_),
                  K(file_cnt),
                  K(next_idx));
        break;
      }
    }
    if (OB_FAIL(next_file())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to open next file", K(ret));
      }
    } else {
      if (first_file) {
        cur_part_id = state_.part_id_;
        first_file = false;
      }
      ++file_cnt;
      if (OB_FAIL(collect_file_meta_statistics())) {
        LOG_WARN("failed to collect file meta statistics", K(ret), K(state_.cur_file_url_));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObParquetMinMaxIter::collect_aggregate_from_files()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *agg_exprs = scan_param_->aggregate_exprs_;
  if (OB_ISNULL(agg_exprs) || agg_exprs->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate exprs is null or empty in aggregate pushdown mode", K(ret));
  } else if (OB_FAIL(agg_res_datums_.prepare_allocate(agg_exprs->count()))) {
    LOG_WARN("failed to prepare allocate agg_res_datums", K(ret), K(agg_exprs->count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs->count(); ++i) {
    agg_res_datums_.at(i).set_null();
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(next_file())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to open next file", K(ret));
      }
    } else if (OB_FAIL(collect_file_meta_statistics())) {
      LOG_WARN("failed to collect file meta statistics", K(ret), K(state_.cur_file_url_));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObParquetMinMaxIter::collect_file_meta_statistics()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *agg_exprs = scan_param_->aggregate_exprs_;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_ISNULL(agg_exprs) || OB_ISNULL(file_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate exprs or file meta is null", K(ret), KP(agg_exprs), KP(file_meta_.get()));
  } else {
    ObSEArray<int, 4> parquet_col_idxs;
    ObSEArray<ObColumnMeta, 4> column_metas;
    for (int64_t agg_idx = 0; OB_SUCC(ret) && agg_idx < agg_exprs->count(); ++agg_idx) {
      ObExpr *agg_expr = agg_exprs->at(agg_idx);
      int parquet_col_idx = -1;
      ObColumnMeta column_meta;
      if (OB_ISNULL(agg_expr) || agg_expr->arg_cnt_ < 1 || OB_ISNULL(agg_expr->args_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid aggregate expr", K(ret), K(agg_idx), KP(agg_expr));
      } else if (OB_FAIL(resolve_agg_parquet_column(agg_expr, agg_idx, parquet_col_idx, column_meta))) {
        LOG_WARN("failed to resolve agg parquet column", K(ret), K(agg_idx));
      } else if (OB_FAIL(parquet_col_idxs.push_back(parquet_col_idx))) {
        LOG_WARN("failed to push back parquet col idx", K(ret));
      } else if (OB_FAIL(column_metas.push_back(column_meta))) {
        LOG_WARN("failed to push back column meta", K(ret));
      }
    }

    for (int64_t rg_idx = 0; OB_SUCC(ret) && rg_idx < file_meta_->num_row_groups(); ++rg_idx) {
      for (int64_t agg_idx = 0; OB_SUCC(ret) && agg_idx < agg_exprs->count(); ++agg_idx) {
        if (parquet_col_idxs.at(agg_idx) >= 0) {
          if (OB_FAIL(merge_rowgroup_statistics(agg_exprs->at(agg_idx),
                                                agg_idx,
                                                rg_idx,
                                                parquet_col_idxs.at(agg_idx),
                                                column_metas.at(agg_idx),
                                                eval_ctx))) {
            LOG_WARN("failed to merge rowgroup statistics", K(ret), K(rg_idx), K(agg_idx));
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetMinMaxIter::resolve_agg_parquet_column(ObExpr *agg_expr,
                                                    const int64_t agg_idx,
                                                    int &parquet_col_idx,
                                                    ObColumnMeta &column_meta)
{
  int ret = OB_SUCCESS;
  ObExpr *col_expr = agg_expr->args_[0];
  int64_t column_expr_index = -1;
  int64_t file_col_expr_index = -1;
  parquet_col_idx = -1;

  if (OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col expr is null", K(ret), K(agg_idx));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count() && column_expr_index < 0; ++i) {
    if (column_exprs_.at(i) == col_expr) {
      column_expr_index = i;
      break;
    }
  }
  if (OB_SUCC(ret) && column_expr_index < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr not found in column_exprs_", K(ret), K(agg_idx), KP(col_expr));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count() && file_col_expr_index < 0; ++i) {
    if (mapping_column_ids_.at(i).second == static_cast<uint64_t>(column_expr_index)) {
      file_col_expr_index = i;
      break;
    }
  }
  if (OB_SUCC(ret) && file_col_expr_index < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file column expr index not found in mapping_column_ids_",
             K(ret),
             K(agg_idx),
             K(column_expr_index));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(file_col_expr_index >= file_column_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_col_expr_index out of range",
             K(ret),
             K(file_col_expr_index),
             K(file_column_exprs_.count()));
  } else if (OB_FAIL(compute_column_id_by_index_type(file_col_expr_index, parquet_col_idx))) {
    LOG_WARN("failed to compute column id by index type", K(ret), K(agg_idx));
  } else if (parquet_col_idx >= 0) {
    if (OB_FAIL(column_meta.from_ob_expr(file_column_exprs_.at(file_col_expr_index)))) {
      LOG_WARN("failed to get column meta", K(ret));
    }
  }
  return ret;
}

int ObParquetMinMaxIter::merge_rowgroup_statistics(ObExpr *agg_expr,
                                                   const int64_t agg_idx,
                                                   const int64_t rg_idx,
                                                   const int parquet_col_idx,
                                                   const ObColumnMeta &column_meta,
                                                   ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<parquet::Statistics> stat
      = file_meta_->RowGroup(rg_idx)->ColumnChunk(parquet_col_idx)->statistics();
  if (nullptr == stat || !stat->HasMinMax()) {
    LOG_TRACE("no statistics", K(rg_idx), K(parquet_col_idx));
  } else {
    std::string min_val = stat->EncodeMin();
    std::string max_val = stat->EncodeMax();
    LOG_TRACE("min max val", K(min_val.c_str()), K(max_val.c_str()));
    parquet::Type::type parquet_type = file_meta_->schema()->Column(parquet_col_idx)->physical_type();
    parquet::SortOrder::type sort_order = stat->descr()->sort_order();
    const parquet::LogicalType *log_type
        = file_meta_->schema()->Column(parquet_col_idx)->logical_type().get();
    blocksstable::ObStorageDatum rg_min_datum;
    blocksstable::ObStorageDatum rg_max_datum;
    rg_min_datum.set_null();
    rg_max_datum.set_null();
    ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);
    if (OB_FAIL(ObParquetTableRowIterator::read_min_max_datum(min_val,
                                                              max_val,
                                                              parquet_type,
                                                              log_type,
                                                              column_meta,
                                                              sort_order,
                                                              eval_ctx,
                                                              rg_min_datum,
                                                              rg_max_datum,
                                                              alloc_guard.get_allocator()))) {
      LOG_WARN("failed to read min max datum", K(ret), K(rg_idx), K(parquet_col_idx));
    } else if (rg_min_datum.is_null() || rg_max_datum.is_null()) {
      LOG_TRACE("datum is null", K(rg_idx), K(parquet_col_idx));
    } else if (OB_FAIL(merge_agg_datum(agg_expr, agg_idx, rg_min_datum, rg_max_datum))) {
      LOG_WARN("failed to merge agg datum", K(ret), K(agg_idx));
    }
  }
  return ret;
}

int ObParquetMinMaxIter::merge_agg_datum(ObExpr *agg_expr,
                                         const int64_t agg_idx,
                                         const blocksstable::ObStorageDatum &rg_min_datum,
                                         const blocksstable::ObStorageDatum &rg_max_datum)
{
  int ret = OB_SUCCESS;
  bool is_min_agg = (T_FUN_SYS_EXT_MIN == agg_expr->type_);
  const blocksstable::ObStorageDatum &src_datum = is_min_agg ? rg_min_datum : rg_max_datum;
  if (agg_res_datums_.at(agg_idx).is_null()) {
    if (OB_FAIL(agg_res_datums_.at(agg_idx).deep_copy(src_datum, allocator_))) {
      LOG_WARN("failed to deep copy datum", K(ret));
    }
  } else {
    ObDatumCmpFuncType cmp_func = agg_expr->basic_funcs_->null_first_cmp_;
    int cmp_ret = 0;
    if (OB_FAIL(cmp_func(agg_res_datums_.at(agg_idx), src_datum, cmp_ret))) {
      LOG_WARN("failed to compare datum", K(ret));
    } else if ((is_min_agg && cmp_ret > 0) || (!is_min_agg && cmp_ret < 0)) {
      agg_res_datums_.at(agg_idx).reuse();
      if (OB_FAIL(agg_res_datums_.at(agg_idx).deep_copy(src_datum, allocator_))) {
        LOG_WARN("failed to deep copy datum", K(ret));
      }
    }
  }
  return ret;
}

int ObParquetMinMaxIter::output_aggregate_result(int64_t &count)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *agg_exprs = scan_param_->aggregate_exprs_;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_ISNULL(agg_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate exprs is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs->count(); ++i) {
    ObExpr *agg_expr = agg_exprs->at(i);
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate expr is null", K(ret), K(i));
    } else {
      ObDatum *datums = agg_expr->locate_batch_datums(eval_ctx);
      if (agg_res_datums_.at(i).is_null()) {
        datums[0].set_null();
      } else {
        datums[0] = agg_res_datums_.at(i);
      }
      agg_expr->set_evaluated_flag(eval_ctx);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_file_meta_column(1, eval_ctx))) {
    LOG_WARN("failed to calc file meta column for aggregate result", K(ret));
  } else if (OB_FAIL(calc_column_convert(1, eval_ctx))) {
    LOG_WARN("failed to calc column convert for aggregate result", K(ret));
  } else {
    count = 1;
  }
  return ret;
}

int ObParquetMinMaxIter::calc_file_meta_column(const int64_t read_count, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
    ObExpr *meta_expr = file_meta_column_exprs_.at(i);
    if (meta_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
      StrDiscVec *text_vec = static_cast<StrDiscVec *>(meta_expr->get_vector(eval_ctx));
      OZ(meta_expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, read_count));
      if (OB_SUCC(ret)) {
        text_vec->set_ptrs(file_url_ptrs_.get_data());
        text_vec->set_lens(file_url_lens_.get_data());
      }
    } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
      OZ(meta_expr->init_vector_for_write(eval_ctx, VEC_UNIFORM_CONST, read_count));
      OZ(fill_file_partition_expr(meta_expr, state_.part_list_val_));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", KPC(meta_expr));
    }
    meta_expr->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObParquetMinMaxIter::calc_column_convert(const int64_t read_count, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_dependent_exprs_);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
    if (column_need_conv_.at(i)) {
      if (!column_conv_exprs.at(i)->get_eval_info(eval_ctx).is_evaluated(eval_ctx)) {
        OZ(column_conv_exprs.at(i)->init_vector_default(eval_ctx, read_count));
        OZ(column_conv_exprs.at(i)->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
        column_conv_exprs.at(i)->set_evaluated_projected(eval_ctx);
      }
      if (OB_SUCC(ret)) {
        ObExpr *to = column_exprs_.at(i);
        ObExpr *from = column_conv_exprs.at(i);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum = static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < read_count && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx);
          if (src != dst) {
            MEMCPY(dst, src, read_count * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
        } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
          LOG_WARN("assign vector header failed", K(ret));
        }
        column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
  }
  return ret;
}

int ObParquetMinMaxIter::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t read_count = 0;
  UNUSED(capacity);
  ObMallocHookAttrGuard guard(mem_attr_);
  if (state_.file_idx_ >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
  } else if (need_partition_info_) {
    if (OB_FAIL(collect_partition_aggregate())) {
      LOG_WARN("failed to collect partition aggregate", K(ret));
    } else if (OB_FAIL(output_aggregate_result(read_count))) {
      LOG_WARN("failed to output aggregate result", K(ret));
    } else {
      count = read_count;
    }
  } else if (OB_FAIL(collect_aggregate_from_files())) {
    LOG_WARN("failed to collect aggregate from files", K(ret));
  } else if (OB_FAIL(output_aggregate_result(read_count))) {
    LOG_WARN("failed to output aggregate result", K(ret));
  } else {
    state_.file_idx_ = scan_param_->key_ranges_.count();
    count = read_count;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
