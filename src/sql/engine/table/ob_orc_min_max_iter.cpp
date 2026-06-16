/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_orc_min_max_iter.h"
#include "ob_orc_table_row_iter.h"

#include "lib/ob_running_mode.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_compatibility_control.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {
namespace sql {

ObOrcMinMaxIter::~ObOrcMinMaxIter()
{
  if (nullptr != bit_vector_cache_) {
    allocator_.free(bit_vector_cache_);
    bit_vector_cache_ = nullptr;
  }
  file_prebuffer_.destroy();
}

int ObOrcMinMaxIter::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
  mem_attr_ = ObMemAttr(MTL_ID(), "OrcMmIter");
  allocator_.set_attr(mem_attr_);
  temp_allocator_.set_attr(ObMemAttr(MTL_ID(), "OrcMmIterTmp"));
  orc_alloc_.init(MTL_ID());
  options_ = ObExternalTableAccessOptions::lazy_defaults();
  if (OB_SUCC(ret) && options_.enable_prebuffer_) {
    OZ(file_prebuffer_.init(options_.cache_options_, scan_param->timeout_));
  }
  OZ(name_to_id_.create(512, mem_attr_));
  OZ(iceberg_id_to_type_.create(512, mem_attr_));
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
            "aggregate pushdown for non ext_min/ext_max functions on orc external table");
        LOG_WARN("only ext_min/ext_max aggregate pushdown is supported for orc",
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
          = scan_param->external_file_format_.orc_format_.column_name_case_sensitive_;
    } else {
      is_col_name_case_sensitive_ = true;
    }
  }
  return ret;
}

void ObOrcMinMaxIter::reset()
{
  state_.reuse();
  url_.reuse();
  reader_.reset();
  name_to_id_.reuse();
  iceberg_id_to_type_.reuse();
  file_prebuffer_.destroy();
  agg_res_datums_.reset();
}

int ObOrcMinMaxIter::create_file_reader(const ObString &data_path, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  try {
    std::unique_ptr<ObOrcFileAccess> inStream(
        new ObOrcFileAccess(data_access_driver_, data_path.ptr(), file_size));
    inStream->set_timeout_timestamp(scan_param_->timeout_);
    if (options_.enable_prebuffer_ && OB_STORAGE_FILE != data_access_driver_.get_storage_type()) {
      inStream->set_file_prebuffer(&file_prebuffer_);
    }
    orc::ReaderOptions options;
    options.setMemoryPool(orc_alloc_);
    reader_ = orc::createReader(std::move(inStream), options);
    if (!reader_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc create reader failed", K(ret));
    }
  } catch (const ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("fail to read orc file", K(ret));
    }
  } catch (const std::exception &e) {
    if (OB_SUCC(ret)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());
      LOG_WARN("unexpected error", K(ret), "Info", e.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    }
  }
  return ret;
}

int ObOrcMinMaxIter::to_dot_column_path(common::ObIArray<ObString> &col_names, ObString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_string;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_names.count(); i++) {
    if (i > 0) {
      OZ(tmp_string.append("."));
    }
    OZ(tmp_string.append(col_names.at(i)));
  }
  OZ(ob_write_string(allocator_, tmp_string.string(), path));
  return ret;
}

int ObOrcMinMaxIter::build_type_name_id_map(const orc::Type *type,
                                             common::ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  CK(type != nullptr);
  if (OB_FAIL(ret)) {
  } else if (orc::TypeKind::STRUCT == type->getKind()) {
    for (size_t i = 0; OB_SUCC(ret) && i < type->getSubtypeCount(); ++i) {
      const std::string &cpp_field_name = type->getFieldName(i);
      ObString field_name;
      OZ(ob_write_string(allocator_, ObString(cpp_field_name.c_str()), field_name));
      OZ(col_names.push_back(field_name));
      ObString path;
      OZ(to_dot_column_path(col_names, path));

      if (!is_col_name_case_sensitive_) {
        ObString capitalize_str;
        OZ(ob_simple_low_to_up(allocator_, path, capitalize_str));
        OZ(name_to_id_.set_refactored(
            capitalize_str, type->getSubtype(i)->getColumnId(), 1));
      } else {
        OZ(name_to_id_.set_refactored(path, type->getSubtype(i)->getColumnId(), 1));
      }
      OZ(build_type_name_id_map(type->getSubtype(i), col_names));
      if (OB_FAIL(ret)) {
      } else if (col_names.count() > 0) {
        col_names.pop_back();
      }
    }
  } else {
    for (size_t j = 0; OB_SUCC(ret) && j < type->getSubtypeCount(); ++j) {
      OZ(build_type_name_id_map(type->getSubtype(j), col_names));
    }
  }
  return ret;
}

int ObOrcMinMaxIter::build_iceberg_id_to_type_map(const orc::Type *type)
{
  int ret = OB_SUCCESS;
  CK(type != nullptr);
  if (OB_SUCC(ret)) {
    if (type->hasAttributeKey(ObExternalTableRowIterator::ICEBERG_ID_KEY)) {
      const std::string &id_val =
          type->getAttributeValue(ObExternalTableRowIterator::ICEBERG_ID_KEY);
      int64_t iceberg_id = 0;
      if (OB_FAIL(c_str_to_int(id_val.c_str(), iceberg_id))) {
        LOG_WARN("fail to convert iceberg.id to int", K(ret), K(id_val.c_str()));
      } else if (OB_FAIL(iceberg_id_to_type_.set_refactored(iceberg_id, type, 0))) {
        LOG_WARN("fail to set iceberg id to type", K(ret), K(iceberg_id));
      }
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < type->getSubtypeCount(); ++i) {
      OZ(build_iceberg_id_to_type_map(type->getSubtype(i)));
    }
  }
  return ret;
}

int ObOrcMinMaxIter::next_file()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObString location = scan_param_->external_file_location_;
  int64_t task_idx = 0;
  int64_t file_size = -1;
  ObFileScanTask *scan_task = nullptr;

  do {
    ret = OB_SUCCESS;
    reader_.reset();
    name_to_id_.reuse();
    iceberg_id_to_type_.reuse();
    if (data_access_driver_.is_opened()) {
      data_access_driver_.close();
    }
    if ((task_idx = state_.file_idx_++) >= scan_param_->scan_tasks_.count()) {
      ret = OB_ITER_END;
    } else {
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
        OZ(url_.append_fmt(
            "%.*s%s%.*s",
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
          ObExternalFileUrlInfo file_info(scan_param_->external_file_location_,
                                          scan_param_->external_file_access_info_,
                                          url_.string(),
                                          file_content_digest, file_size, modify_time);
          ObExternalFileCacheOptions cache_options(options_.enable_memory_cache_,
                                                   options_.enable_disk_cache_);
          if (OB_FAIL(data_access_driver_.open(file_info, cache_options))) {
            if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret) {
              ret = OB_SUCCESS;
              file_size = 0;
            } else {
              LOG_WARN("fail to open file", K(ret), K(file_info), K(state_.cur_file_url_));
            }
          }
          if (OB_SUCC(ret) && file_size > 0) {
            OZ(create_file_reader(url_.string(), file_size));
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
      }
      LOG_DEBUG("current external file", K(url_), K(ret));
    }
  } while (OB_SUCC(ret) && OB_UNLIKELY(0 == file_size));

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
    column_index_type_ = scan_param_->external_file_format_.orc_format_.column_index_type_;

    if (OB_SUCC(ret) && OB_NOT_NULL(reader_.get())) {
      try {
        const orc::Type &root_type = reader_->getType();
        ObArray<ObString> col_names;
        if (OB_FAIL(SMART_CALL(build_type_name_id_map(&root_type, col_names)))) {
          LOG_WARN("fail to build type name id map", K(ret));
        }
        if (OB_SUCC(ret) && is_iceberg_lake_table()) {
          if (OB_FAIL(SMART_CALL(build_iceberg_id_to_type_map(&root_type)))) {
            LOG_WARN("fail to build iceberg id to type map", K(ret));
          } else {
            column_index_type_ = sql::ColumnIndexType::ID;
          }
        }
      } catch (const std::exception &e) {
        if (OB_SUCC(ret)) {
          ret = OB_ORC_READ_ERROR;
          LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
          LOG_WARN("unexpected error", K(ret), "Info", e.what());
        }
      } catch (...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOrcMinMaxIter::compute_orc_column_id(ObExpr *file_col_expr, int64_t &orc_col_id)
{
  int ret = OB_SUCCESS;
  orc_col_id = -1;
  if (OB_ISNULL(file_col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file col expr is null", K(ret));
  } else {
    switch (column_index_type_) {
      case sql::ColumnIndexType::NAME: {
        ObDataAccessPathExtraInfo *data_access_info =
            static_cast<ObDataAccessPathExtraInfo *>(file_col_expr->extra_info_);
        if (OB_ISNULL(data_access_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data access info is null", K(ret));
        } else {
          ObString col_name = data_access_info->data_access_path_;
          if (!is_col_name_case_sensitive_) {
            ObString capitalize_str;
            if (OB_FAIL(ob_simple_low_to_up(allocator_, col_name, capitalize_str))) {
              LOG_WARN("fail to capitalize column name", K(ret));
            } else if (OB_FAIL(name_to_id_.get_refactored(capitalize_str, orc_col_id))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                orc_col_id = -1;
              } else {
                LOG_WARN("fail to get column id by name", K(ret), K(capitalize_str));
              }
            }
          } else {
            if (OB_FAIL(name_to_id_.get_refactored(col_name, orc_col_id))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                orc_col_id = -1;
              } else {
                LOG_WARN("fail to get column id by name", K(ret), K(col_name));
              }
            }
          }
        }
        break;
      }
      case sql::ColumnIndexType::POSITION: {
        orc_col_id = file_col_expr->extra_;
        break;
      }
      case sql::ColumnIndexType::ID: {
        int64_t iceberg_id = file_col_expr->extra_;
        const orc::Type *type = nullptr;
        int tmp_ret = iceberg_id_to_type_.get_refactored(iceberg_id, type);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          orc_col_id = -1;
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to get orc type by iceberg id", K(ret), K(iceberg_id));
        } else if (OB_NOT_NULL(type)) {
          orc_col_id = type->getColumnId();
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unknown orc column_index_type", K(ret), K(column_index_type_));
        break;
    }
  }
  return ret;
}

int ObOrcMinMaxIter::collect_partition_aggregate()
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
      ObFileScanTask *next_task =
          static_cast<ObFileScanTask *>(scan_param_->scan_tasks_.at(next_idx));
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

int ObOrcMinMaxIter::collect_aggregate_from_files()
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

int ObOrcMinMaxIter::collect_file_meta_statistics()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *agg_exprs = scan_param_->aggregate_exprs_;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_ISNULL(agg_exprs) || OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate exprs or reader is null", K(ret), KP(agg_exprs), KP(reader_.get()));
  } else {
    for (int64_t agg_idx = 0; OB_SUCC(ret) && agg_idx < agg_exprs->count(); ++agg_idx) {
      ObExpr *agg_expr = agg_exprs->at(agg_idx);
      if (OB_ISNULL(agg_expr) || agg_expr->arg_cnt_ < 1 || OB_ISNULL(agg_expr->args_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid aggregate expr", K(ret), K(agg_idx), KP(agg_expr));
      } else {
        ObExpr *col_expr = agg_expr->args_[0];
        int64_t column_expr_index = -1;
        int64_t file_col_expr_index = -1;

        for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count() && column_expr_index < 0;
             ++i) {
          if (column_exprs_.at(i) == col_expr) {
            column_expr_index = i;
          }
        }
        if (OB_SUCC(ret) && column_expr_index < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr not found in column_exprs_", K(ret), K(agg_idx), KP(col_expr));
        }

        for (int64_t i = 0;
             OB_SUCC(ret) && i < mapping_column_ids_.count() && file_col_expr_index < 0;
             ++i) {
          if (mapping_column_ids_.at(i).second == static_cast<uint64_t>(column_expr_index)) {
            file_col_expr_index = i;
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
        } else {
          ObExpr *file_col_expr = file_column_exprs_.at(file_col_expr_index);
          if (OB_ISNULL(file_col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("file column expr is null", K(ret), K(agg_idx), K(file_col_expr_index));
          } else {
            int64_t orc_col_id = -1;
            if (OB_FAIL(compute_orc_column_id(file_col_expr, orc_col_id))) {
              LOG_WARN("failed to compute orc column id", K(ret), K(agg_idx));
            } else if (orc_col_id >= 0) {
              ObColumnMeta column_meta;
              if (OB_FAIL(column_meta.from_ob_expr(file_col_expr))) {
                LOG_WARN("failed to get column meta", K(ret));
              } else if (OB_FAIL(merge_file_statistics(
                             agg_expr, agg_idx, orc_col_id, column_meta, eval_ctx))) {
                LOG_WARN("failed to merge file statistics", K(ret), K(orc_col_id));
              }
            }
          }
        }
      }
    }
    temp_allocator_.reuse();
  }
  return ret;
}

int ObOrcMinMaxIter::merge_file_statistics(ObExpr *agg_expr,
                                           const int64_t agg_idx,
                                           const int64_t orc_col_id,
                                           const ObColumnMeta &column_meta,
                                           ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  try {
    std::unique_ptr<orc::Statistics> file_stat = reader_->getStatistics();
    if (OB_ISNULL(file_stat)) {
      LOG_TRACE("no file statistics", K(orc_col_id));
    } else {
      const orc::ColumnStatistics *col_stat = file_stat->getColumnStatistics(orc_col_id);
      if (OB_ISNULL(col_stat)) {
        LOG_TRACE("no column statistics", K(orc_col_id));
      } else {
        const orc::Type &root_type = reader_->getType();
        const orc::Type *orc_type =
            ObOrcTableRowIterator::find_orc_type_by_column_id(&root_type, orc_col_id);
        if (OB_ISNULL(orc_type)) {
          LOG_TRACE("orc type not found", K(orc_col_id));
        } else {
          blocksstable::ObStorageDatum file_min_datum;
          blocksstable::ObStorageDatum file_max_datum;
          file_min_datum.set_null();
          file_max_datum.set_null();
          if (OB_FAIL(read_orc_min_max_datum(
                  col_stat, orc_type, column_meta, file_min_datum, file_max_datum))) {
            LOG_WARN("failed to read orc min max datum", K(ret), K(orc_col_id));
          } else if (file_min_datum.is_null() || file_max_datum.is_null()) {
            LOG_TRACE("datum is null", K(orc_col_id));
          } else if (OB_FAIL(merge_agg_datum(agg_expr, agg_idx, file_min_datum, file_max_datum))) {
            LOG_WARN("failed to merge agg datum", K(ret), K(agg_idx));
          }
        }
      }
    }
  } catch (const ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("fail to read orc file", K(ret));
    }
  } catch (const std::exception &e) {
    if (OB_SUCC(ret)) {
      ret = OB_ORC_READ_ERROR;
      LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
      LOG_WARN("unexpected error", K(ret), "Info", e.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    }
  }
  return ret;
}

int ObOrcMinMaxIter::read_orc_min_max_datum(const orc::ColumnStatistics *col_stat,
                                            const orc::Type *orc_type,
                                            const ObColumnMeta &column_meta,
                                            blocksstable::ObStorageDatum &min_datum,
                                            blocksstable::ObStorageDatum &max_datum)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMinMaxFilterParam param;
  if (OB_FAIL(convert_orc_statistics(col_stat, orc_type, column_meta, param))) {
    LOG_WARN("failed to convert orc statistics", K(ret));
  } else if (!param.min_datum_.is_null() && !param.max_datum_.is_null()) {
    if (OB_FAIL(min_datum.deep_copy(param.min_datum_, temp_allocator_))) {
      LOG_WARN("failed to deep copy min datum", K(ret));
    } else if (OB_FAIL(max_datum.deep_copy(param.max_datum_, temp_allocator_))) {
      LOG_WARN("failed to deep copy max datum", K(ret));
    }
  }
  return ret;
}

int ObOrcMinMaxIter::merge_agg_datum(ObExpr *agg_expr,
                                     const int64_t agg_idx,
                                     const blocksstable::ObStorageDatum &stripe_min_datum,
                                     const blocksstable::ObStorageDatum &stripe_max_datum)
{
  int ret = OB_SUCCESS;
  bool is_min_agg = (T_FUN_SYS_EXT_MIN == agg_expr->type_);
  const blocksstable::ObStorageDatum &src_datum = is_min_agg ? stripe_min_datum : stripe_max_datum;
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

int64_t ObOrcMinMaxIter::calc_tz_adjust_us(const orc::Type *orc_type,
                                           const ObObjType ob_type,
                                           const ObSQLSessionInfo *session)
{
  int64_t res = 0;
  int ret = OB_SUCCESS;
  bool is_utc_src = (orc_type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT);
  bool is_utc_dst = (lib::is_mysql_mode() && ObTimestampType == ob_type)
                    || (lib::is_oracle_mode() && ObTimestampLTZType == ob_type);
  if (is_utc_src != is_utc_dst) {
    int32_t tmp_offset = 0;
    if (OB_NOT_NULL(session)
        && OB_NOT_NULL(session->get_timezone_info())
        && OB_SUCCESS == session->get_timezone_info()->get_timezone_offset(0, tmp_offset)) {
      res = SEC_TO_USEC(tmp_offset) * (is_utc_src ? 1 : -1);
    }
  }
  LOG_DEBUG("tz adjust", K(is_utc_src), K(is_utc_dst), K(res), K(ob_type));
  return res;
}

int ObOrcMinMaxIter::convert_orc_statistics(const orc::ColumnStatistics *orc_stat,
                                            const orc::Type *orc_type,
                                            const ObColumnMeta &col_meta,
                                            blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  param.set_uncertain();
  const orc::TypeKind type_kind = orc_type->getKind();
  bool has_null = orc_stat->hasNull();
  switch (ob_obj_type_class(col_meta.type_)) {
    case ObIntTC: {
      ret = ObOrcTableRowIterator::convert_integer_type_statistics(orc_stat, orc_type, col_meta, param);
      break;
    }
    case ObYearTC: {
      break;
    }
    case ObFloatTC:
    case ObDoubleTC: {
      const ObScale scale = col_meta.scale_;
      if (lib::is_mysql_mode() && (scale > SCALE_UNKNOWN_YET && scale < OB_NOT_FIXED_SCALE)) {
      } else {
        ret = ObOrcTableRowIterator::convert_real_type_statistics(orc_stat, orc_type, col_meta, param);
      }
      break;
    }
    case ObNumberTC:
    case ObDecimalIntTC: {
      ret = ObOrcTableRowIterator::convert_decimal_type_statistics(orc_stat, orc_type, col_meta, param);
      break;
    }
    case ObDateTC:
    case ObMySQLDateTC:
    case ObDateTimeTC:
    case ObMySQLDateTimeTC:
    case ObTimeTC:
    case ObOTimestampTC: {
      if (lib::is_oracle_mode() && col_meta.get_type() == ObDateType) {
      } else {
        const int64_t adjust_us = calc_tz_adjust_us(
            orc_type, col_meta.type_, scan_param_->op_->get_eval_ctx().exec_ctx_.get_my_session());
        ret = ObOrcTableRowIterator::convert_temporal_type_statistics(orc_stat, orc_type, col_meta, adjust_us, param);
        break;
      }
    }
    case ObStringTC:
    case ObTextTC: {
      if (ObCharset::is_bin_sort(col_meta.cs_type_)
          && (orc::TypeKind::STRING == type_kind || orc::TypeKind::VARCHAR == type_kind
              || orc::TypeKind::CHAR == type_kind)) {
        ret = ObOrcTableRowIterator::convert_string_type_statistics(orc_stat, col_meta, temp_allocator_, param, has_null);
      }
      break;
    }
    default: {
      break;
    }
  }
  if (OB_SUCC(ret) && !param.min_datum_.is_null() && !param.max_datum_.is_null()) {
    const bool all_null = (orc_stat->getNumberOfValues() == 0);
    int64_t null_count = 0;
    if (all_null) {
      null_count = INT64_MAX;
    } else if (has_null) {
      null_count = INT64_MAX - 1;
    } else {
      null_count = 0;
    }
    param.null_count_.set_int(null_count);
  }
  return ret;
}

int ObOrcMinMaxIter::output_aggregate_result(int64_t &count)
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

int ObOrcMinMaxIter::calc_file_meta_column(const int64_t read_count, ObEvalCtx &eval_ctx)
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

int ObOrcMinMaxIter::calc_column_convert(const int64_t read_count, ObEvalCtx &eval_ctx)
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
          ObDatum *from_datum =
              static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
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

int ObOrcMinMaxIter::get_next_rows(int64_t &count, int64_t capacity)
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
