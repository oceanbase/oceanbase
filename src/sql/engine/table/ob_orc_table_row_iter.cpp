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
 #define USING_LOG_PREFIX SQL_ENG
#include "ob_orc_table_row_iter.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "share/external_table/ob_external_table_utils.h"


namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

int ObOrcTableRowIterator::to_dot_column_path(ObIArray<ObString> &col_names, ObString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_string;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_names.count(); i++) {
    if (i > 0) {
      OZ (tmp_string.append("."));
    }
    OZ (tmp_string.append(col_names.at(i)));
  }
  OZ (ob_write_string(allocator_, tmp_string.string(), path));
  return ret;
}


/**
  * Recurses over a type tree and build two maps
  * map<TypeName, ColumnId>, map<ColumnId, Type>
  */
int ObOrcTableRowIterator::build_type_name_id_map(const orc::Type* type, ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  CK (type != nullptr);
  OZ (id_to_type_.set_refactored(type->getColumnId(), type, 0));
  if (OB_FAIL(ret)) {
  } else if (orc::TypeKind::STRUCT == type->getKind()) {
    for (size_t i = 0; OB_SUCC(ret) && i < type->getSubtypeCount(); ++i) {
      const std::string& cpp_field_name = type->getFieldName(i);
      ObString field_name;
      OZ (ob_write_string(allocator_, ObString(cpp_field_name.c_str()), field_name));
      OZ (col_names.push_back(field_name));
      ObString path;
      OZ (to_dot_column_path(col_names, path));
      OZ (name_to_id_.set_refactored(path, type->getSubtype(i)->getColumnId(), 0));
      OZ (build_type_name_id_map(type->getSubtype(i), col_names));
      if (OB_FAIL(ret)) {
      } else if (col_names.count() > 0) {
        col_names.pop_back();
      }
    }
  } else {
    // other non-primitive type
    for (size_t j = 0; OB_SUCC(ret) && j < type->getSubtypeCount(); ++j) {
      OZ (build_type_name_id_map(type->getSubtype(j), col_names));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::compute_column_id_by_index_type(int64_t index, int64_t &orc_col_id)
{
  int ret = OB_SUCCESS;
  switch (scan_param_->external_file_format_.orc_format_.column_index_type_) {
    case sql::ColumnIndexType::NAME: {
      ObString col_name;
      ObDataAccessPathExtraInfo *data_access_info =
        static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(index)->extra_info_);
      col_name = data_access_info->data_access_path_;
      OZ (name_to_id_.get_refactored(col_name, orc_col_id));
      break;
    }
    case sql::ColumnIndexType::POSITION: {
      orc_col_id = file_column_exprs_.at(index)->extra_;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unknown orc column_index_type", K(ret), K(scan_param_->external_file_format_.orc_format_.column_index_type_));
      break;
  }
  return ret;
}

int ObOrcTableRowIterator::prepare_read_orc_file()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
    int64_t orc_col_id = -1;
    OZ(compute_column_id_by_index_type(i, orc_col_id));
    CK (orc_col_id != -1);
    const orc::Type *type = nullptr;
    OZ (id_to_type_.get_refactored(orc_col_id, type));
    CK (type != nullptr);

    if (OB_SUCC(ret)) {
      column_indexs_.at(i) = orc_col_id;
      load_funcs_.at(i) = DataLoader::select_load_function(file_column_exprs_.at(i)->datum_meta_,
                                                           *type);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(load_funcs_.at(i))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      if (i >= row_reader_->getSelectedType().getSubtypeCount()) {
        //error for report
        LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_));
        const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
        LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, "", ob_type);
      } else {
        std::string p_type = row_reader_->getSelectedType().getSubtype(i) == nullptr ?
                "INVALID ORC TYPE" : row_reader_->getSelectedType().getSubtype(i)->toString();
        int64_t pos = 0;
        ObArrayWrap<char> buf;
        ObArray<ObString> extended_type_info;
        ObDatumMeta &meta = file_column_exprs_.at(i)->datum_meta_;
        const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
        if (OB_SUCCESS == buf.allocate_array(allocator_, 100)) {
          ob_sql_type_str(buf.get_data(), buf.count(), pos, meta.type_,
                          OB_MAX_VARCHAR_LENGTH, meta.precision_, meta.scale_, meta.cs_type_,
                          extended_type_info);
          if (pos < buf.count()) {
            buf.at(pos++) = '\0';
            ob_type = buf.get_data();
          }
        }
        LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_),
                  K(ObString(p_type.length(), p_type.data())));
        LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, p_type.c_str(), ob_type);
      }
    } else if (OB_FAIL(prepare_filter_col_meta(column_indexs_, mapping_column_ids_, mapping_column_exprs_))) {
      LOG_WARN("fail to prepare filter col meta", K(ret), K(column_indexs_.count()), K(mapping_column_exprs_.count()));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;

  CK (scan_param != nullptr);
  CK (scan_param->op_ != nullptr);
  CK (scan_param->ext_column_convert_exprs_ != nullptr);
  if (OB_SUCC(ret)) {
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    int64_t column_cnt = scan_param->ext_column_convert_exprs_->count();
    mem_attr_ = ObMemAttr(MTL_ID(), "OrcRowIter");
    allocator_.set_attr(mem_attr_);
    temp_allocator_.set_attr(ObMemAttr(MTL_ID(), "OrcRowIterTemp"));
    orc_alloc_.init(MTL_ID());
    make_external_table_access_options(eval_ctx.exec_ctx_.get_my_session()->get_stmt_type());
    if (options_.enable_prebuffer_) {
      OZ(file_prebuffer_.init(options_.cache_options_, scan_param->timeout_));
    }
    OZ (id_to_type_.create(512, mem_attr_));
    OZ (name_to_id_.create(512, mem_attr_));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (ObExternalTablePushdownFilter::init(scan_param));
    reader_metrics_.reset();

    if (OB_SUCC(ret) && OB_ISNULL(bit_vector_cache_)) {
      void *mem = nullptr;
      if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
      } else {
        bit_vector_cache_ = to_bit_vector(mem);
        bit_vector_cache_->reset(eval_ctx.max_batch_size_);
      }
    }

    ObArray<ObExpr*> file_column_exprs;
    ObArray<ObExpr*> mapping_column_exprs;
    ObArray<uint64_t> mapping_column_ids;
    ObArray<ObExpr*> file_meta_column_exprs;
    bool mapping_generated = !scan_param->ext_mapping_column_exprs_->empty()
                              && !scan_param->ext_mapping_column_ids_->empty();
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
      ObExpr* ext_file_column_expr = scan_param->ext_file_column_exprs_->at(i);
      if (OB_ISNULL(ext_file_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ptr", K(ret));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL
                  || ext_file_column_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        OZ (file_meta_column_exprs.push_back(ext_file_column_expr));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        OZ (file_column_exprs.push_back(ext_file_column_expr));
        OZ (mapping_column_exprs.push_back(mapping_generated
          ? scan_param->ext_mapping_column_exprs_->at(i) : nullptr));
        OZ (mapping_column_ids.push_back(mapping_generated
          ? scan_param->ext_mapping_column_ids_->at(i) : OB_INVALID_ID));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", KPC(ext_file_column_expr));
      }
    }
    OZ (file_column_exprs_.assign(file_column_exprs));
    OZ (mapping_column_exprs_.assign(mapping_column_exprs));
    OZ (mapping_column_ids_.assign(mapping_column_ids));
    const sql::ColumnIndexType index_type =
                                  scan_param_->external_file_format_.orc_format_.column_index_type_;
    if (OB_SUCC(ret) && index_type == sql::ColumnIndexType::NAME) {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        ObDataAccessPathExtraInfo *data_access_info =
          static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
        if (data_access_info == nullptr ||
            data_access_info->data_access_path_.ptr() == nullptr ||
            data_access_info->data_access_path_.length() == 0) {
          ret = OB_EXTERNAL_ACCESS_PATH_ERROR;
        }
      }
    }
    OZ (file_meta_column_exprs_.assign(file_meta_column_exprs));

    if (OB_SUCC(ret) && file_column_exprs_.count() > 0) {
      OZ (column_indexs_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (load_funcs_.allocate_array(allocator_, file_column_exprs_.count()));
    }

    if (OB_SUCC(ret)) {
      OZ (file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
      OZ (file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    }
  }
  return ret;
}

#define CATCH_ORC_EXCEPTIONS                                  \
  catch (const ObErrorCodeException &ob_error) {              \
    if (OB_SUCC(ret)) {                                       \
      ret = ob_error.get_error_code();                        \
      LOG_WARN("fail to read orc file", K(ret));              \
    }                                                         \
  } catch (const std::exception& e) {                         \
    if (OB_SUCC(ret)) {                                       \
      ret = OB_ERR_UNEXPECTED;                                \
      LOG_WARN("unexpected error", K(ret), "Info", e.what()); \
    }                                                         \
  } catch(...) {                                              \
    if (OB_SUCC(ret)) {                                       \
      ret = OB_ERR_UNEXPECTED;                                \
      LOG_WARN("unexpected error", K(ret));                   \
    }                                                         \
  }

int ObOrcTableRowIterator::next_row_range()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!state_.next_row_range())) { // move to next range
    // all ranges of the current stripe have been visited, and it need to move to the next stripe
    if (OB_FAIL(next_stripe())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next stripe", K(ret));
      }
    } else if (OB_UNLIKELY(!state_.has_row_range())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iterator state", K(ret), K_(state));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(state_.has_row_range())) {
    const int64_t cur_row_range_idx = state_.cur_row_range_idx_;
    const SelectedRowRange &row_range = row_ranges_.at(cur_row_range_idx);
    state_.cur_range_row_count_ = row_range.num_rows;
    LOG_TRACE("show current state info", K_(state), K(row_range));
    if (OB_UNLIKELY(state_.orc_reader_cur_row_id_ > row_range.first_row_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc reader row id is unexpected", K(ret), K_(state), K(row_range));
    } else if (state_.orc_reader_cur_row_id_ < row_range.first_row_id) {
      LOG_TRACE("seek to row", K_(state), K(row_range));
      try {
        if (row_reader_) {
          row_reader_->seekToRow(row_range.first_row_id);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read empty rows", K(ret), K(row_range));
        }
      } CATCH_ORC_EXCEPTIONS
      state_.orc_reader_cur_row_id_ = row_range.first_row_id;
    }
  }
  return ret;
}

int ObOrcTableRowIterator::next_stripe()
{
  int ret = OB_SUCCESS;
  do {
    if (OB_UNLIKELY(!state_.next_stripe())) { // move to next stripe
      do {
        // all stripes in the orc file have been read, found next file which has stripe to read.
        if (OB_FAIL(next_file())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next stripe", K(ret));
          }
        }
      } while (OB_SUCC(ret) && !state_.has_stripe());
    }

    if (OB_SUCC(ret)) {
      try {
        if (OB_FAIL(select_row_ranges(state_.cur_stripe_idx_))) {
          LOG_WARN("fail to select row ranges", K(ret), K(state_.cur_stripe_idx_));
        }
      } CATCH_ORC_EXCEPTIONS
    }
  } while (OB_SUCC(ret) && !state_.has_row_range());
  return ret;
}

#undef CATCH_ORC_EXCEPTIONS

int ObOrcTableRowIterator::select_row_ranges(const int64_t stripe_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!reader_ || !row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null reader", K(ret));
  } else {
    std::unique_ptr<orc::StripeInformation> stripe = reader_->getStripe(stripe_idx);
    if (!stripe) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc stripe info is null", K(ret));
    } else {
      const int64_t stripe_num_rows = stripe->getNumberOfRows();
      const int64_t first_row_id = state_.next_stripe_first_row_id_;
      state_.next_stripe_first_row_id_ += stripe_num_rows;
      bool build_whole_stripe_range = true;
      if (has_pushdown_filter()) {
        if (OB_FAIL(select_row_ranges_by_pushdown_filter(*stripe,
                                                         stripe_idx,
                                                         first_row_id,
                                                         stripe_num_rows,
                                                         build_whole_stripe_range))) {
          LOG_WARN("fail to select row ranges by pushdown filters", K(ret));
        }
      }
      if (OB_SUCC(ret) && build_whole_stripe_range) {
        if (OB_FAIL(ensure_row_range_array(1))) {
          LOG_WARN("fail to ensure row range array", K(ret));
        } else {
          SelectedRowRange whole_stripe_range;
          whole_stripe_range.start_row_group_idx = 0;
          whole_stripe_range.end_row_group_idx = 0;
          whole_stripe_range.first_row_id = first_row_id;
          whole_stripe_range.num_rows = stripe_num_rows;
          row_ranges_.at(0) = whole_stripe_range;
          state_.cur_row_range_idx_ = 0;
          state_.end_row_range_idx_ = 0;
        }
      }
      if (OB_SUCC(ret) && state_.has_row_range() && options_.enable_prebuffer_ &&
          file_column_exprs_.count() > 0) {
        if (OB_FAIL(pre_buffer_data(*stripe))) {
          LOG_WARN("fail to pre buffer date", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::select_row_ranges_by_pushdown_filter(
    const orc::StripeInformation &stripe,
    const int64_t stripe_idx,
    const int64_t stripe_first_row_id,
    const int64_t stripe_num_rows,
    bool &build_whole_stripe_range)
{
  int ret = OB_SUCCESS;
  build_whole_stripe_range = false;
  // pre buffer row index first if there are pushdown filters
  if (options_.enable_prebuffer_ && OB_FAIL(pre_buffer_row_index(stripe))) {
    LOG_WARN("fail to pre buffer row index", K(ret), K(stripe_idx));
  } else {
    bool is_stripe_filtered = false;
    int64_t groups_filtered = 0;
    int64_t groups_in_stripe = 0;
    // filter stripe by stripe statistic.
    std::unique_ptr<orc::StripeStatistics> stripe_stat = reader_->getStripeStatistics(stripe_idx);
    if (OB_UNLIKELY(!stripe_stat)) {
      // no stripe statistics, do nothing
      build_whole_stripe_range = true;
    } else if (OB_FAIL(filter_by_statistic(STRIPE_LEVEL, stripe_stat.get(), is_stripe_filtered))) {
      LOG_WARN("fail to apply skipping index filter", K(ret), K(stripe_idx));
    } else if (!is_stripe_filtered) {
      // filter row groups by row index.
      const int64_t row_index_stride = reader_->getRowIndexStride();
      if (OB_UNLIKELY(row_index_stride <= 0)) {
        // row index is disabled, need to build whole stripe range.
        build_whole_stripe_range = true;
      } else {
        groups_in_stripe = (stripe_num_rows + row_index_stride - 1) / row_index_stride;
        if (OB_FAIL(ensure_row_range_array(groups_in_stripe))) {
          LOG_WARN("fail to ensure row range array", K(ret));
        } else {
          try {
            RowIndexStatisticsWrapper row_index_stat_wrapper = RowIndexStatisticsWrapper(*stripe_stat);
            SelectedRowRange last_row_range;
            int64_t row_range_idx = 0;
            int64_t first_row_id = stripe_first_row_id;
            for (int64_t idx = 0; OB_SUCC(ret) && idx < groups_in_stripe; ++idx) {
              bool is_filtered = false;
              row_index_stat_wrapper.set_row_index(idx);
              if (OB_FAIL(filter_by_statistic(ROW_INDEX_LEVEL, &row_index_stat_wrapper,
                  is_filtered))) {
                LOG_WARN("fail to apply skipping index filter", K(ret), K(idx));
              } else if (is_filtered) {
                ++groups_filtered;
                if (!last_row_range.is_empty()) {
                  // store and reset the last row range
                  row_ranges_.at(row_range_idx++) = last_row_range;
                  last_row_range.reset();
                }
              } else if (last_row_range.is_empty()) {
                // init last_row_range as first range
                last_row_range.start_row_group_idx = idx;
                last_row_range.end_row_group_idx = idx;
                last_row_range.first_row_id = first_row_id;
                last_row_range.num_rows = row_index_stride;
              } else {
                // merge this range to last_row_range
                last_row_range.end_row_group_idx = idx;
                last_row_range.num_rows += row_index_stride;
              }
              first_row_id += row_index_stride;
            }
            if (OB_SUCC(ret)) {
              if (!last_row_range.is_empty()) {
                const int64_t skipped_rows_count = last_row_range.first_row_id - stripe_first_row_id;
                const int64_t remain_rows_in_stripe = stripe_num_rows - skipped_rows_count;
                last_row_range.num_rows = remain_rows_in_stripe;
                row_ranges_.at(row_range_idx++) = last_row_range;
              }
              state_.cur_row_range_idx_ = 0;
              state_.end_row_range_idx_ = row_range_idx - 1;
              LOG_TRACE("orc iterator state after row group", K_(state));
            }
          } catch(const ObErrorCodeException &ob_error) {
            if (OB_SUCC(ret)) {
              ret = ob_error.get_error_code();
              LOG_WARN("fail to read orc file", K(ret));
            }
          } catch(const std::exception& e) {
            if (OB_SUCC(ret)) {
              // exception when get orc row index statistics, select all row group to read
              build_whole_stripe_range = true;
            }
          } catch(...) {
            if (OB_SUCC(ret)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // update reader metrics
      if (build_whole_stripe_range) {  // no statistic or read orc exception
        ++reader_metrics_.selected_stripe_count;
        reader_metrics_.selected_row_group_count += groups_in_stripe;
      } else if (is_stripe_filtered) {
        ++reader_metrics_.skipped_stripe_count;
      } else {
        ++reader_metrics_.selected_stripe_count;
        reader_metrics_.selected_row_group_count += (groups_in_stripe - groups_filtered);
        reader_metrics_.skipped_row_group_count += groups_filtered;
      }
    }
  }
  return ret;
}


int ObOrcTableRowIterator::ensure_row_range_array(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > row_ranges_.count())) {
    const int64_t new_array_size = next_pow2(size);
    if (OB_FAIL(row_ranges_.allocate_array(allocator_, static_cast<size_t>(new_array_size)))) {
      LOG_WARN("fail to allocate row group array", K(ret), K(new_array_size), K(size));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::next_file()
{
  int ret = OB_SUCCESS;
  state_.reset_cur_file_state();
  CK (scan_param_ != nullptr);
  CK (scan_param_->op_ != nullptr);
  if (OB_SUCC(ret)) {
    // init all meta
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    ObString location = scan_param_->external_file_location_;
    int64_t task_idx = 0;
    int64_t file_size = 0;
    if (data_access_driver_.is_opened()) {
      data_access_driver_.close();
    }

    do {
      if ((task_idx = state_.file_idx_++) >= scan_param_->key_ranges_.count()) {
        ret = OB_ITER_END;
        if (has_pushdown_filter()) {
          LOG_TRACE("orc filter pushdown info", K_(reader_metrics));
          reader_metrics_.reset();
        }
      } else {
        state_.cur_file_url_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
        url_.reuse();
        const char *split_char = "/";
        OZ (url_.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                          (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                                          state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));

        if (OB_SUCC(ret)) {
          ObExternalFileUrlInfo file_info(scan_param_->external_file_location_,
                                          scan_param_->external_file_access_info_, url_.string());
          ObExternalFileCacheOptions cache_options(options_.enable_page_cache_,
                                                   options_.enable_disk_cache_);
          file_size = 0;
          if (OB_FAIL(data_access_driver_.open(file_info, cache_options))) {
            if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret) {
              ret = OB_SUCCESS;
              file_size = 0;
            } else {
              LOG_WARN("fail to open file", K(ret), K(file_info));
            }
          } else if (OB_FAIL(data_access_driver_.get_file_size(file_size))) {
            LOG_WARN("fail to get file size", K(ret));
          }
          if (OB_SUCC(ret) && file_size > 0) {
            ObString expr_file_url;
            if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
              ObSqlString full_name;
              if (ip_port_.empty()) {
                OZ(gen_ip_port(allocator_));
              }
              OZ (full_name.append_fmt("%.*s%%%.*s", ip_port_.length(), ip_port_.ptr(),
                                      state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
              OZ (ob_write_string(allocator_, full_name.string(), expr_file_url));
            } else {
              expr_file_url = state_.cur_file_url_;
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
              file_url_ptrs_.at(i) = expr_file_url.ptr();
              file_url_lens_.at(i) = expr_file_url.length();
            }
          }
        }
        LOG_DEBUG("current external file", K(url_), K(file_size));
      }
    } while (OB_SUCC(ret) && OB_UNLIKELY(0 >= file_size)); //skip not exist or empty file

    if (OB_SUCC(ret)) {
      // read orc file footer
      int64_t part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
      if (part_id != 0 && state_.part_id_ != part_id) {
        state_.part_id_ = part_id;
        OZ (calc_file_partition_list_value(part_id, allocator_, state_.part_list_val_));
      }

      state_.cur_file_id_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
      try {
        std::unique_ptr<ObOrcFileAccess> inStream(new ObOrcFileAccess(data_access_driver_,
                                                          url_.ptr(), file_size));
        if (options_.enable_prebuffer_) {
          inStream->set_file_prebuffer(&file_prebuffer_);
        }
        inStream->set_timeout_timestamp(scan_param_->timeout_);
        orc::ReaderOptions options;
        options.setMemoryPool(orc_alloc_);
        reader_ = orc::createReader(std::move(inStream), options);
        if (!reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orc create reader failed", K(ret));
          throw std::bad_exception();
        }
        orc::RowReaderOptions rowReaderOptions;
        std::list<std::string> include_names_list;
        std::list<uint64_t> include_ids_list;
        switch (scan_param_->external_file_format_.orc_format_.column_index_type_) {
          case sql::ColumnIndexType::NAME: {
            for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
              ObDataAccessPathExtraInfo *data_access_info =
                  static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
              include_names_list.push_front(std::string(data_access_info->data_access_path_.ptr(),
                                                  data_access_info->data_access_path_.length()));
            }
            rowReaderOptions.include(include_names_list);
            break;
          }
          case sql::ColumnIndexType::POSITION: {
            for (uint64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
              include_ids_list.push_back(file_column_exprs_.at(i)->extra_ - 1);
            }
            rowReaderOptions.include(include_ids_list);
            break;
          }
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unknown orc column_index_type",
                    K(ret), K(scan_param_->external_file_format_.orc_format_.column_index_type_));
            break;
        }

        if (OB_SUCC(ret)) {
          try {
            row_reader_ = reader_->createRowReader(rowReaderOptions);
          } catch(const std::exception& e) {
            if (OB_SUCC(ret)) {
              ret = OB_ORC_READ_ERROR;
              LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
              LOG_WARN("unexpected error", K(ret), "Info", e.what());
            }
          } catch(...) {
            if (OB_SUCC(ret)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!row_reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create row reader failed", K(ret));
        } else {
          ObArray<ObString> col_names;
          id_to_type_.reuse();
          name_to_id_.reuse();
          if (OB_FAIL(SMART_CALL(build_type_name_id_map(&row_reader_->getSelectedType(),
                                                        col_names)))) {
            LOG_WARN("fail to build type name id map", K(ret));
          } else if (OB_FAIL(prepare_read_orc_file())) {
            LOG_WARN("fail to prepare read orc file", K(ret));
          } else if (OB_FAIL(filter_file(task_idx))) {
            LOG_WARN("fail to filter file and stripes", K(ret));
          } else if (OB_UNLIKELY(!state_.has_stripe())) {
            // no stripe after filter
          } else { // create orc read batch for reuse.
            int64_t capacity = MAX(1, eval_ctx.max_batch_size_);
            orc_batch_ = row_reader_->createRowBatch(capacity);
          }
        }
      } catch(const ObErrorCodeException &ob_error) {
        if (OB_SUCC(ret)) {
          ret = ob_error.get_error_code();
          LOG_WARN("fail to read orc file", K(ret));
        }
      } catch(const std::exception& e) {
        if (OB_SUCC(ret)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());
          LOG_WARN("unexpected error", K(ret), "Info", e.what());
        }
      } catch(...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::filter_file(const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  bool file_skipped = false;
  std::unique_ptr<orc::Statistics> orc_col_stat = reader_->getStatistics();
  if (!has_pushdown_filter()) {
    // do nothing, no filter to skip
  } else if (!orc_col_stat) {
    // no column statistics, do nothing
  } else if (OB_FAIL(filter_by_statistic(PushdownLevel::FILE, orc_col_stat.get(), file_skipped))) {
    LOG_WARN("fail to apply skipping index filter", K(ret));
  } else if (file_skipped) {
    ++reader_metrics_.skipped_file_count;
  } else {
    ++reader_metrics_.selected_file_count;
  }
  if (OB_SUCC(ret) && !file_skipped) {
    // resolve stripe index by task id
    int64_t start_lineno;
    int64_t end_lineno;
    if (OB_FAIL(ObExternalTableUtils::resolve_line_number_range(
        scan_param_->key_ranges_.at(task_idx), ObExternalTableUtils::ROW_GROUP_NUMBER,
        start_lineno, end_lineno))) {
      LOG_WARN("fail to resolve line number range", K(ret), K(task_idx));
    } else {
      const int64_t nstripes = reader_->getNumberOfStripes();
      LOG_TRACE("read file access: number of stipes", K(nstripes), K(url_));
      state_.cur_stripe_idx_ = start_lineno - 1;
      state_.end_stripe_idx_ = std::min(nstripes - 1, end_lineno);
      if (state_.has_stripe() && OB_UNLIKELY(state_.cur_stripe_idx_ > 0)) {
        // Since the starting stripe index of the current task does not start from 0,
        // the previous rows need to be skipped.
        const int64_t cur_stripe_idx = state_.cur_stripe_idx_;
        for (int64_t stripe_idx = 0; OB_SUCC(ret) && stripe_idx < cur_stripe_idx; ++stripe_idx) {
          std::unique_ptr<orc::StripeInformation> stripe = reader_->getStripe(stripe_idx);
          if (OB_UNLIKELY(!stripe)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null stripe is unexpected", K(ret), K(stripe_idx));
          } else {
            state_.next_stripe_first_row_id_ += stripe->getNumberOfRows();
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::filter_by_statistic(const PushdownLevel filter_level,
                                               const orc::Statistics *orc_stat,
                                               bool &skipped)
{
  int ret = OB_SUCCESS;
  OrcMinMaxFilterParamBuilder param_builder(this, orc_stat);
  if (OB_FAIL(apply_skipping_index_filter(filter_level, param_builder, skipped))) {
    LOG_WARN("fail to apply skipping index filter", K(ret));
  }
  // free temp allocator after filtered
  temp_allocator_.reset();
  return ret;
}

int ObOrcTableRowIterator::pre_buffer_row_index(const orc::StripeInformation &stripe)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_column_range_slices())) {
    LOG_WARN("fail to init column range slices", K(ret));
  } else {
    const int64_t offset = stripe.getOffset();
    const int64_t row_index_length = stripe.getIndexLength();
    ObFilePreBuffer::ReadRange read_range(offset, row_index_length);
    ObFilePreBuffer::ColumnRangeSlices *slices = column_range_slices_.at(0);
    ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
    if (OB_ISNULL(slices)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slices is null", K(ret));
    } else if (OB_FAIL(slices->range_list_.push_back(read_range))) {
      LOG_WARN("fail to push back range", K(ret));
    } else if (OB_FAIL(column_range_slice_list.push_back(slices))) {
      LOG_WARN("fail to push back range", K(ret));
    } else if (OB_FAIL(file_prebuffer_.pre_buffer(column_range_slice_list))) {
      LOG_WARN("fail to pre buffer row index", K(ret));
    } else {
      LOG_TRACE("pre buffer row index", K(read_range));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::pre_buffer_data(const orc::StripeInformation &stripe)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_column_range_slices())) {
    LOG_WARN("fail to init column range slices", K(ret));
  } else {
    const int64_t n_streams = stripe.getNumberOfStreams();
    const std::vector<bool> &selected_columns = row_reader_->getSelectedColumns();
    ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < n_streams; ++i) {
      std::unique_ptr<orc::StreamInformation> stream = stripe.getStreamInformation(i);
      uint64_t orc_col_id = 0;
      if (!stream) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stream in stripe", K(ret), K(n_streams), K(i));
      } else if (FALSE_IT(orc_col_id = stream->getColumnId())) {
      } else if (OB_UNLIKELY(orc_col_id >= selected_columns.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col id is out of range", K(ret), K(orc_col_id), K(selected_columns.size()));
      } else if (selected_columns[orc_col_id]) {
        const orc::StreamKind stream_kind = stream->getKind();
        const int64_t offset = static_cast<int64_t>(stream->getOffset());
        const int64_t length = static_cast<int64_t>(stream->getLength());
        ObFilePreBuffer::ReadRange read_range(offset, length);
        if (orc::StreamKind::StreamKind_DATA == stream_kind ||
            orc::StreamKind::StreamKind_DICTIONARY_DATA == stream_kind ||
            orc::StreamKind::StreamKind_DICTIONARY_COUNT == stream_kind ||
            orc::StreamKind::StreamKind_PRESENT == stream_kind ||
            orc::StreamKind::StreamKind_LENGTH == stream_kind ||
            orc::StreamKind::StreamKind_SECONDARY == stream_kind) {
          ObFilePreBuffer::ColumnRangeSlices *slices = column_range_slices_.at(orc_col_id);
          if (OB_ISNULL(slices)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("slices is null", K(ret), K(orc_col_id));
          } else if (OB_FAIL(slices->range_list_.push_back(read_range))) {
            LOG_WARN("fail to push back range", K(ret), K(orc_col_id));
          } else {
            LOG_TRACE("pre buffer", K(orc_col_id), K(stream_kind), K(read_range));
          }
        }
      }
    }
    // add column range to pre buffer
    for (int64_t i = 0; OB_SUCC(ret) && i < column_range_slices_.count(); ++i) {
      if (OB_NOT_NULL(column_range_slices_.at(i)) &&
          !column_range_slices_.at(i)->range_list_.empty()) {
        ret = column_range_slice_list.push_back(column_range_slices_.at(i));
      }
    }
    if (OB_SUCC(ret) && !column_range_slice_list.empty()) {
      ret = file_prebuffer_.pre_buffer(column_range_slice_list);
    }
  }
  return ret;
}

int ObOrcTableRowIterator::init_column_range_slices()
{
  int ret = OB_SUCCESS;
  const int64_t selected_col_count = row_reader_->getSelectedColumns().size();
  if (OB_UNLIKELY(column_range_slices_.count() < selected_col_count)) {
    common::ObArrayWrap<ObFilePreBuffer::ColumnRangeSlices *> new_col_range_slice;
    if (OB_FAIL(new_col_range_slice.allocate_array(allocator_, selected_col_count))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else {
      // copy old array
      int64_t i = 0;
      for ( ; OB_SUCC(ret) && i < column_range_slices_.count(); ++i) {
        new_col_range_slice.at(i) = column_range_slices_.at(i);
      }
      // init slice for reuse.
      for ( ; OB_SUCC(ret) && i < selected_col_count; ++i) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObFilePreBuffer::ColumnRangeSlices)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else {
          new_col_range_slice.at(i) = new(buf)ObFilePreBuffer::ColumnRangeSlices;
        }
      }
      if (OB_SUCC(ret)) {
        column_range_slices_ = new_col_range_slice;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(column_range_slices_.count() != selected_col_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count mismatch", K(ret));
  } else {
    // clear last pre buffer range.
    for (int64_t i = 0; OB_SUCC(ret) && i < column_range_slices_.count(); ++i) {
      if (OB_ISNULL(column_range_slices_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column range array is null", K(ret), K(i));
      } else {
        column_range_slices_.at(i)->range_list_.reuse();
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::OrcMinMaxFilterParamBuilder::build(
    const int64_t ext_tbl_col_id,
    const ObExpr *expr,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orc_row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orc row iterator is null", K(ret));
  } else if (OB_ISNULL(orc_stat_)) {
    // no orc statistics, do nothing
  } else {
    const int orc_col_id = ext_tbl_col_id;
    const orc::ColumnStatistics *col_stat = orc_stat_->getColumnStatistics(orc_col_id);
    const orc::Type *orc_type = NULL;
    if (!col_stat) {
      // no orc column statistics
    } else if (OB_FAIL(orc_row_iter_->id_to_type_.get_refactored(orc_col_id, orc_type))) {
      LOG_WARN("fail to get orc type", K(ret), K(orc_col_id));
    } else if (OB_ISNULL(orc_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc type is null", K(ret), K(orc_col_id));
    } else if (OB_FAIL(orc_row_iter_->convert_orc_statistics(col_stat, orc_type,
                                                             expr->datum_meta_, expr->obj_meta_.has_lob_header(),
                                                             param))) {
      LOG_WARN("fail to convert orc statistics", K(ret));
    }
  }
  return ret;
}

static OB_INLINE int convert_integer_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const orc::Type *orc_type,
    const ObDatumMeta &col_meta,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  const orc::TypeKind type_kind = orc_type->getKind();
  if (orc::TypeKind::BOOLEAN == type_kind) {
    const orc::BooleanColumnStatistics *bool_col_stat =
      dynamic_cast<const orc::BooleanColumnStatistics*>(orc_stat);
    if (OB_ISNULL(bool_col_stat)) {
    } else if (bool_col_stat->hasCount()) {
      const int64_t min_val = (0 == bool_col_stat->getFalseCount()) ? 1 : 0;
      const int64_t max_val = (0 == bool_col_stat->getTrueCount()) ? 0 : 1;
      param.min_datum_.set_int(min_val);
      param.max_datum_.set_int(max_val);
    }
  } else if (orc::TypeKind::BYTE == type_kind ||
              orc::TypeKind::SHORT == type_kind ||
              orc::TypeKind::INT == type_kind ||
              orc::TypeKind::LONG == type_kind) {
    const orc::IntegerColumnStatistics *int_col_stat =
      dynamic_cast<const orc::IntegerColumnStatistics*>(orc_stat);
    if (OB_ISNULL(int_col_stat)) {
    } else if (int_col_stat->hasMaximum() && int_col_stat->hasMinimum()) {
      param.min_datum_.set_int(int_col_stat->getMinimum());
      param.max_datum_.set_int(int_col_stat->getMaximum());
      LOG_DEBUG("ORC min-max", K(int_col_stat->getMinimum()), K(int_col_stat->getMaximum()));
    }
  }
  return ret;
}

static OB_INLINE int convert_real_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const orc::Type *orc_type,
    const ObDatumMeta &col_meta,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  const orc::DoubleColumnStatistics *double_col_stat =
      dynamic_cast<const orc::DoubleColumnStatistics*>(orc_stat);
  if (OB_ISNULL(double_col_stat)) {
  } else if (double_col_stat->hasMaximum() && double_col_stat->hasMinimum() &&
      std::isfinite(double_col_stat->getSum())) {
    const double minimum = double_col_stat->getMinimum();
    const double maximum = double_col_stat->getMaximum();
    if (ob_is_float_tc(col_meta.type_)) {
      param.min_datum_.set_float(static_cast<float>(minimum));
      param.max_datum_.set_float(static_cast<float>(maximum));
    } else {
      param.min_datum_.set_double(minimum);
      param.max_datum_.set_double(maximum);
    }
  }
  return ret;
}

static OB_INLINE int128_t orc_int128_to_ob_int128(orc::Int128 &orc_int)
{
  int128_t ob_int128 = 0;
  ob_int128.items_[1] = orc_int.getHighBits();
  ob_int128.items_[0] = orc_int.getLowBits();
  return ob_int128;
}

static OB_INLINE int decimal128_to_number(const int128_t &decimal_int, const ObScale scale,
                                          ObDatum &out)
{
  int ret = OB_SUCCESS;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber res_nmb;
  if (OB_FAIL(wide::to_number(decimal_int, scale, tmp_alloc, res_nmb))) {
    LOG_WARN("fail to cast decimal_int to number", K(ret));
  } else {
    out.set_number(res_nmb);
  }
  return ret;
}

static OB_INLINE int convert_decimal_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const orc::Type *orc_type,
    const ObDatumMeta &col_meta,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  const orc::TypeKind type_kind = orc_type->getKind();
  int128_t min_val = 0;
  int128_t max_val = 0;
  bool has_min_max = false;
  if (type_kind == orc::TypeKind::DECIMAL) {
    const orc::DecimalColumnStatistics *decimal_col_stat =
      dynamic_cast<const orc::DecimalColumnStatistics*>(orc_stat);
    if (OB_ISNULL(decimal_col_stat)) {
    } else if (decimal_col_stat->hasMaximum() && decimal_col_stat->hasMinimum()) {
      const orc::Decimal &orc_dec_min = decimal_col_stat->getMinimum();
      const orc::Decimal &orc_dec_max = decimal_col_stat->getMaximum();
      if (orc_dec_min.scale > col_meta.scale_ || orc_dec_max.scale > col_meta.scale_) {
        // unexpected scale for decimal column statistics, ignore it.
      } else {
        orc::Int128 orc_int_min = orc_dec_min.value;
        orc::Int128 orc_int_max = orc_dec_max.value;
        bool is_overflow = false;
        if (orc_dec_min.scale < col_meta.scale_) {
          const int32_t power = col_meta.scale_ - orc_dec_min.scale;
          orc_int_min = orc::scaleUpInt128ByPowerOfTen(orc_int_min, power, is_overflow);
        }
        if (!is_overflow && orc_dec_max.scale < col_meta.scale_) {
          const int32_t power = col_meta.scale_ - orc_dec_max.scale;
          orc_int_max = orc::scaleUpInt128ByPowerOfTen(orc_int_max, power, is_overflow);
        }
        if (!is_overflow) {
          min_val = orc_int128_to_ob_int128(orc_int_min);
          max_val = orc_int128_to_ob_int128(orc_int_max);
          has_min_max = true;
        }
      }
    }
  } else if (orc::TypeKind::BYTE == type_kind ||
              orc::TypeKind::SHORT == type_kind ||
              orc::TypeKind::INT == type_kind ||
              orc::TypeKind::LONG == type_kind) {
    const orc::IntegerColumnStatistics *int_col_stat =
      dynamic_cast<const orc::IntegerColumnStatistics*>(orc_stat);
    if (OB_ISNULL(int_col_stat)) {
    } else if (int_col_stat->hasMaximum() && int_col_stat->hasMinimum()) {
      min_val = int_col_stat->getMinimum();
      max_val = int_col_stat->getMaximum();
      has_min_max = true;
    }
  } else if (orc::TypeKind::BOOLEAN == type_kind) {
    const orc::BooleanColumnStatistics *bool_col_stat =
      dynamic_cast<const orc::BooleanColumnStatistics*>(orc_stat);
    if (OB_ISNULL(bool_col_stat)) {
    } else if (bool_col_stat->hasCount()) {
      min_val = (0 == bool_col_stat->getFalseCount()) ? 1 : 0;
      max_val = (0 == bool_col_stat->getTrueCount()) ? 0 : 1;
      has_min_max = true;
    }
  }
  if (OB_SUCC(ret) && has_min_max) {
    if (ob_is_decimal_int_tc(col_meta.type_)) {
#define SET_MIN_MAX(type) \
  const type##_t decint_min = min_val; \
  const type##_t decint_max = max_val; \
  param.min_datum_.set_decimal_int(decint_min); \
  param.max_datum_.set_decimal_int(decint_max);

      const ObPrecision precision = col_meta.precision_;
      if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
        SET_MIN_MAX(int32);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
        SET_MIN_MAX(int64);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
        SET_MIN_MAX(int128);
      }

#undef SET_MIN_MAX
    } else if (ob_is_number_tc(col_meta.type_)) {
      if (OB_FAIL(decimal128_to_number(min_val, col_meta.scale_, param.min_datum_))) {
        LOG_WARN("fail to store decimal int128 to number", K(ret));
      } else if (OB_FAIL(decimal128_to_number(max_val, col_meta.scale_, param.max_datum_))) {
        LOG_WARN("fail to store decimal int128 to number", K(ret));
      }
    }
  }
  return ret;
}

static OB_INLINE int convert_orc_date_to_ob_temporal_statistics(
    const orc::ColumnStatistics *orc_stat,
    const ObDatumMeta &col_meta,
    const int64_t adjust_us,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  bool has_min_max = false;
  int32_t min_val = 0;
  int32_t max_val = 0;
  const orc::DateColumnStatistics *date_col_stat =
    dynamic_cast<const orc::DateColumnStatistics*>(orc_stat);
  if (OB_ISNULL(date_col_stat)) {
  } else if (date_col_stat->hasMaximum() && date_col_stat->hasMinimum()) {
    min_val = date_col_stat->getMinimum();
    max_val = date_col_stat->getMaximum();
    has_min_max = true;
  }
  if (has_min_max) {
    const ObObjType col_type = col_meta.type_;
    if (ob_is_date_tc(col_type)) {
      param.min_datum_.set_date(min_val);
      param.max_datum_.set_date(max_val);
    } else if (ob_is_datetime(col_type)) {
      param.min_datum_.set_datetime(static_cast<int64_t>(min_val) * USECS_PER_DAY);
      param.max_datum_.set_datetime(static_cast<int64_t>(max_val) * USECS_PER_DAY);
    } else if (ob_is_mysql_date_tc(col_type)) {
      ObMySQLDate min_md;
      ObMySQLDate max_md;
      if (OB_FAIL(ObTimeConverter::date_to_mdate(min_val, min_md))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(min_val));
      } else if (OB_FAIL(ObTimeConverter::date_to_mdate(max_val, max_md))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(max_val));
      } else {
        param.min_datum_.set_mysql_date(min_md);
        param.max_datum_.set_mysql_date(max_md);
      }
    } else if (ob_is_mysql_datetime_tc(col_type)) {
      const int64_t min_dt = static_cast<int64_t>(min_val) * USECS_PER_DAY;
      const int64_t max_dt = static_cast<int64_t>(max_val) * USECS_PER_DAY;
      ObMySQLDateTime min_mdt;
      ObMySQLDateTime max_mdt;
      if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(min_dt, min_mdt))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(min_val));
      } else if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(max_dt, max_mdt))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(max_val));
      } else {
        param.min_datum_.set_mysql_datetime(min_mdt);
        param.max_datum_.set_mysql_datetime(max_mdt);
      }
    } else {
      const int64_t adjusted_min_val = static_cast<int64_t>(min_val) * USECS_PER_DAY + adjust_us;
      const int64_t adjusted_max_val = static_cast<int64_t>(max_val) * USECS_PER_DAY + adjust_us;
      if (ObTimestampType == col_type) {
        param.min_datum_.set_timestamp(adjusted_min_val);
        param.max_datum_.set_timestamp(adjusted_max_val);
      } else if (ObTimeType == col_type) {
        // ignore this, the time of date type is always zero.
      } else if (ob_is_otimestamp_type(col_type)) {
        ObOTimestampData min_data;
        ObOTimestampData max_data;
        min_data.time_us_ = adjusted_min_val;
        max_data.time_us_ = adjusted_max_val;
        param.min_datum_.set_otimestamp_tiny(min_data);
        param.max_datum_.set_otimestamp_tiny(max_data);
      }
    }
  }
  return ret;
}

static OB_INLINE int64_t orc_timestamp_to_ob_timestamp(const int64_t second, const int32_t nanos,
                                                       const int64_t adjust_us)
{
  return second * USECS_PER_SEC + nanos / NSECS_PER_USEC + adjust_us;
}

static OB_INLINE int64_t orc_stat_ts_to_ob_timestamp(const int64_t utc, const int32_t nanos,
                                                     const int64_t adjust_us)
{
  const int64_t second = utc / NSECS_PER_USEC;
  const int32_t nano = static_cast<int32_t>((utc % NSECS_PER_USEC) * USECS_PER_SEC) + nanos;
  return orc_timestamp_to_ob_timestamp(second, nano, adjust_us);
}

static OB_INLINE int convert_orc_ts_temporal_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const ObDatumMeta &col_meta,
    const int64_t adjust_us,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  const orc::TimestampColumnStatistics *timestamp_col_stat =
    dynamic_cast<const orc::TimestampColumnStatistics*>(orc_stat);
  if (OB_ISNULL(timestamp_col_stat)) {
  } else if (timestamp_col_stat->hasMaximum() && timestamp_col_stat->hasMinimum()) {
    const int64_t min_utc = timestamp_col_stat->getMinimum();
    const int32_t min_nanos = timestamp_col_stat->getMinimumNanos();
    const int64_t max_utc = timestamp_col_stat->getMaximum();
    const int32_t max_nanos = timestamp_col_stat->getMaximumNanos();
    const int64_t min_ts = orc_stat_ts_to_ob_timestamp(min_utc, min_nanos, adjust_us);
    const int64_t max_ts = orc_stat_ts_to_ob_timestamp(max_utc, max_nanos, adjust_us);
    LOG_TRACE("orc timestamp stat", K(min_utc), K(min_nanos), K(min_ts),
                                    K(max_utc), K(max_nanos), K(max_ts));
    const ObObjType col_type = col_meta.type_;
    if (ObTimestampType == col_type) {
      param.min_datum_.set_timestamp(min_ts);
      param.max_datum_.set_timestamp(max_ts);
    } else if (ObDateTimeType == col_type) {
      param.min_datum_.set_datetime(min_ts);
      param.max_datum_.set_datetime(max_ts);
    } else if (ObMySQLDateTimeType == col_type) {
      ObMySQLDateTime min_mdt_val;
      ObMySQLDateTime max_mdt_val;
      if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(min_ts, min_mdt_val))) {
        LOG_WARN("fail to cast datetime to mysql datetime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(max_ts, max_mdt_val))) {
        LOG_WARN("fail to cast datetime to mysql datetime", K(ret));
      } else {
        param.min_datum_.set_mysql_datetime(min_mdt_val);
        param.max_datum_.set_mysql_datetime(max_mdt_val);
      }
    } else if (ob_is_date_tc(col_type)) {
      param.min_datum_.set_date(min_ts / USECS_PER_DAY);
      param.max_datum_.set_date(max_ts / USECS_PER_DAY);
    } else if (ob_is_mysql_date_tc(col_type)) {
      ObMySQLDate min_md;
      ObMySQLDate max_md;
      if (OB_FAIL(ObTimeConverter::date_to_mdate(min_ts / USECS_PER_DAY, min_md))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(min_ts));
      } else if (OB_FAIL(ObTimeConverter::date_to_mdate(max_ts / USECS_PER_DAY, max_md))) {
        LOG_WARN("fail to cast date to mysql data", K(ret), K(max_ts));
      } else {
        param.min_datum_.set_mysql_date(min_md);
        param.max_datum_.set_mysql_date(max_md);
      }
    } else if (ObTimeType == col_type) {
      // ignore this, the time result from orc-timestamp is invalid.
    } else if (ob_is_otimestamp_type(col_type)) {
      ObOTimestampData min_data;
      ObOTimestampData max_data;
      min_data.time_us_ = min_ts;
      min_data.time_ctx_.set_tail_nsec(min_nanos % NSECS_PER_USEC);
      max_data.time_us_ = max_ts;
      max_data.time_ctx_.set_tail_nsec(max_nanos % NSECS_PER_USEC);
      param.min_datum_.set_otimestamp_tiny(min_data);
      param.max_datum_.set_otimestamp_tiny(max_data);
    }
  }
  return ret;
}

static OB_INLINE int convert_temporal_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const orc::Type *orc_type,
    const ObDatumMeta &col_meta,
    const int64_t adjust_us,
    blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  const orc::TypeKind type_kind = orc_type->getKind();
  if (orc::TypeKind::DATE == type_kind) {
    ret = convert_orc_date_to_ob_temporal_statistics(orc_stat, col_meta, adjust_us, param);
  } else if (orc::TypeKind::TIMESTAMP == type_kind ||
             orc::TypeKind::TIMESTAMP_INSTANT == type_kind) {
    ret = convert_orc_ts_temporal_type_statistics(orc_stat, col_meta, adjust_us, param);
  }
  return ret;
}

static OB_INLINE int convert_string_type_statistics(
    const orc::ColumnStatistics *orc_stat,
    const ObDatumMeta &col_meta,
    const bool has_lob_header,
    ObIAllocator &allocator,
    blocksstable::ObMinMaxFilterParam &param,
    bool &has_null)
{
  int ret = OB_SUCCESS;
  const orc::StringColumnStatistics *string_col_stat =
      dynamic_cast<const orc::StringColumnStatistics*>(orc_stat);
  if (OB_ISNULL(string_col_stat)) {
  } else if (string_col_stat->hasMaximum() && string_col_stat->hasMinimum()) {
    const std::string &min_str = string_col_stat->getMinimum();
    const std::string &max_str = string_col_stat->getMaximum();
    ObString min_ob_str = ObString(min_str.size(), min_str.data());
    ObString max_ob_str = ObString(max_str.size(), max_str.data());
    if (ob_is_string_tc(col_meta.type_)) {
      // Shallow copy is allowed here because the memory lifetime of orc_stat ensures that the
      // filter has been calculated.
      param.min_datum_.set_string(min_ob_str);
      param.max_datum_.set_string(max_ob_str);
    } else if (ob_is_text_tc(col_meta.type_)) {
      if (OB_FAIL(ObTextStringHelper::string_to_templob_result(col_meta.type_, has_lob_header, allocator, min_ob_str,
          param.min_datum_))) {
        LOG_WARN("fail to string to templob result", K(ret));
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(col_meta.type_, has_lob_header, allocator,
          max_ob_str, param.max_datum_))) {
        LOG_WARN("fail to string to templob result", K(ret));
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() && min_str.empty()) {
      // oracle empty string is equivalent to null
      has_null = true;
    }
  }
  return ret;
}

int ObOrcTableRowIterator::convert_orc_statistics(const orc::ColumnStatistics *orc_stat,
                                                  const orc::Type *orc_type,
                                                  const ObDatumMeta &col_meta,
                                                  const bool has_lob_header,
                                                  blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  param.set_uncertain();
  const orc::TypeKind type_kind = orc_type->getKind();
  bool has_null = orc_stat->hasNull();
  switch (ob_obj_type_class(col_meta.type_)) {
    case ObIntTC: {
      ret = convert_integer_type_statistics(orc_stat, orc_type, col_meta, param);
      break;
    }
    case ObYearTC: {
      if (orc::TypeKind::INT == type_kind) {
        // Not supported, because the int32 to ob-year is non-monotonic casting, e.g.
        // int32_t    year(int8_t)
        // 1      ->  1901
        // 2000   ->  2108
        // 20000  ->  1932
        // can support min/max filter by check whether val is within the range of int8 later.
      }
      break;
    }
    case ObFloatTC:
    case ObDoubleTC: {
      const ObScale scale = col_meta.scale_;
      if (lib::is_mysql_mode() && (scale > SCALE_UNKNOWN_YET && scale < OB_NOT_FIXED_SCALE)) {
        // For fixed float/double type in mysql mode, comparison rules do not directly use
        // raw double values but consider scale truncation and rounding semantics.
        // min/max filters may incorrectly exclude values that would be considered in-range under
        // MySQL's scale-aware comparison rules. Hence, such filters  are intentionally disabled
        // for these types.
      } else {
        ret = convert_real_type_statistics(orc_stat, orc_type, col_meta, param);
      }
      break;
    }
    case ObNumberTC:
    case ObDecimalIntTC: {
      ret = convert_decimal_type_statistics(orc_stat, orc_type, col_meta, param);
      break;
    }
    case ObDateTC:
    case ObMySQLDateTC:
    case ObDateTimeTC:
    case ObMySQLDateTimeTC:
    case ObTimeTC:
    case ObOTimestampTC: {
      if (lib::is_oracle_mode() && col_meta.get_type() == ObDateType) {
        // ObDateType in oracle mode is unexpected
      } else {
        const int64_t adjust_us = DataLoader::calc_tz_adjust_us(
        orc_type, col_meta.type_, scan_param_->op_->get_eval_ctx().exec_ctx_.get_my_session());
        ret = convert_temporal_type_statistics(orc_stat, orc_type, col_meta, adjust_us, param);
        break;
      }
    }
    case ObStringTC:
    case ObTextTC: {
      if (ObCharset::is_bin_sort(col_meta.cs_type_) && (
          orc::TypeKind::STRING == type_kind ||
          orc::TypeKind::VARCHAR == type_kind ||
          orc::TypeKind::CHAR == type_kind)) {
        ret = convert_string_type_statistics(orc_stat, col_meta, has_lob_header, temp_allocator_,
                                             param, has_null);
      }
      break;
    }
    default: {
      break;
    }
  }
  if (OB_SUCC(ret) && !param.min_datum_.is_null() && !param.max_datum_.is_null()) {
    // set null count
    const bool all_null = (orc_stat->getNumberOfValues() == 0);
    int64_t null_count = 0;
    if (all_null) {
      null_count = MOCK_ROW_COUNT;
    } else if (has_null) {
      null_count = MOCK_ROW_COUNT - 1;
    } else {
      null_count = 0;
    }
    param.null_count_.set_int(null_count);
  }
  return ret;
}

bool ObOrcTableRowIterator::DataLoader::is_orc_read_utc(const orc::Type *type)
{
  // TIMESTAMP_INSTANT 是utc时间
  return type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT;
}

bool ObOrcTableRowIterator::DataLoader::is_ob_type_store_utc(const ObObjType &type)
{
  return (lib::is_mysql_mode() && ObTimestampType == type)
         || (lib::is_oracle_mode() && ObTimestampLTZType == type);
}

int64_t ObOrcTableRowIterator::DataLoader::calc_tz_adjust_us(const orc::Type *orc_type,
                                                             const ObObjType ob_type,
                                                             const ObSQLSessionInfo *session)
{
  int64_t res = 0;
  int ret = OB_SUCCESS;
  bool is_utc_src = is_orc_read_utc(orc_type);
  bool is_utc_dst = is_ob_type_store_utc(ob_type);
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

int64_t ObOrcTableRowIterator::DataLoader::calc_tz_adjust_us()
{
  return calc_tz_adjust_us(col_type_, file_col_expr_->datum_meta_.type_,
                           eval_ctx_.exec_ctx_.get_my_session());
}

int ObOrcTableRowIterator::DataLoader::load_data_for_col(LOAD_FUNC &func)
{
  return (this->*func)();
}

ObOrcTableRowIterator::DataLoader::LOAD_FUNC ObOrcTableRowIterator::DataLoader::select_load_function(
    const ObDatumMeta &datum_type, const orc::Type &type)
{
  LOAD_FUNC func = NULL;

  // int size = static_cast<int>(type.getSubtypeCount());
  // if (size > 1) {
  //   LOG_USER_ERROR(OB_NOT_SUPPORTED, "Non-primitive type now are");
  //   throw std::invalid_argument(type.toString());
  // }
  const orc::Type* col_desc = &type;
  orc::TypeKind type_kind = col_desc->getKind();
  if (ob_is_integer_type(datum_type.type_)) {
    func = &DataLoader::load_int64_vec;
    int orc_data_len = 0;
    switch (type_kind) {
        case orc::TypeKind::BOOLEAN:
        case orc::TypeKind::BYTE:
          orc_data_len = 1;
          break;
        case orc::TypeKind::SHORT:
          orc_data_len = 2;
          break;
        case orc::TypeKind::INT:
          orc_data_len = 4;
          break;
        case orc::TypeKind::LONG:
          orc_data_len = 8;
          break;
        default:
          func = NULL;
    }
    //sign and width
    ObObj temp_obj;
    temp_obj.set_int(datum_type.type_, 0);
    if (ob_is_unsigned_type(datum_type.type_)) {
      func = NULL;
    }
    if (temp_obj.get_tight_data_len() < orc_data_len) {
      func = NULL;
    }
  } else if (ob_is_year_tc(datum_type.type_) && orc::TypeKind::INT == type_kind) {
    func = &DataLoader::load_year_vec;
  } else if (ob_is_string_tc(datum_type.type_) || ob_is_enum_or_set_type(datum_type.type_)) {
    //convert orc enum/string to enum/string vector
    switch (type_kind) {
      case orc::TypeKind::STRING:
      case orc::TypeKind::VARCHAR:
      case orc::TypeKind::BINARY:
        func = &DataLoader::load_string_col;
        break;
      case orc::TypeKind::CHAR:
        if (ob_is_char(datum_type.type_, datum_type.cs_type_)) {
          func = &DataLoader::load_string_col;
          break;
        }
      default:
        func = NULL;
    }
  } else if (ob_is_number_or_decimal_int_tc(datum_type.type_)) {
    //convert orc int storing as int32/int64 to number/decimal vector
    if (type_kind == orc::TypeKind::DECIMAL) {
      if (col_desc->getPrecision() != ((datum_type.precision_ == -1) ? 38 : datum_type.precision_)
        || col_desc->getScale() != datum_type.scale_) {
        func = NULL;
      } else if (col_desc->getPrecision() == 0 || col_desc->getPrecision() > 18) {
        func = &DataLoader::load_dec128_vec;
      } else {
        func = &DataLoader::load_dec64_vec;
      }
    } else if (type_kind == orc::TypeKind::INT
               || type_kind == orc::TypeKind::LONG
               || type_kind == orc::TypeKind::SHORT
               || type_kind == orc::TypeKind::BYTE
               || type_kind == orc::TypeKind::BOOLEAN) {
      func = &DataLoader::load_int64_to_number_vec;
    } else {
      func = NULL;
    }
  } else if (ob_is_date_or_mysql_date(datum_type.type_) ||
             ob_is_datetime_or_mysql_datetime(datum_type.type_) ||
             ob_is_time_tc(datum_type.type_) ||
             ob_is_otimestamp_type(datum_type.type_) ||
             ObTimestampType == datum_type.type_) {
    switch (type_kind) {
      // Values of TIMESTAMP type are stored in the writer timezone in the Orc file.
      // Values are read back in the reader timezone. However, the writer timezone
      // information in the Orc stripe footer is optional and may be missing. What is
      // more, stripes in the same Orc file may have different writer timezones (though
      // unlikely). So we cannot tell the exact timezone of values read back. In the adapter
      // implementations, we set both writer and
      // reader timezone to UTC to avoid any conversion so users can get the same values
      // as written. To get rid of this burden, TIMESTAMP_INSTANT type is always preferred
      // over TIMESTAMP type.
      case orc::TypeKind::TIMESTAMP:
      case orc::TypeKind::TIMESTAMP_INSTANT:
        LOG_DEBUG("show type kind", K(type_kind), K(orc::TypeKind::TIMESTAMP_INSTANT));
        if (ob_is_date_or_mysql_date(datum_type.type_) ||
             ob_is_datetime_or_mysql_datetime(datum_type.type_) ||
             ob_is_time_tc(datum_type.type_) ||
             ObTimestampType == datum_type.type_ ||
             ObTimestampLTZType == datum_type.type_ ||
             ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_vec;
        }
        break;
      case orc::TypeKind::DATE:
        if (ob_is_date_tc(datum_type.type_)) {
          func = &DataLoader::load_int32_vec;
        } else if (ob_is_time_tc(datum_type.type_) ||
                   ob_is_mysql_date_tc(datum_type.type_) ||
                   ObTimestampType == datum_type.type_ ||
                   ObTimestampLTZType == datum_type.type_) {
          func = &DataLoader::load_date_to_time_or_stamp;
        }
        break;
      default:
        func = NULL;
    }
  } else if (orc::TypeKind::FLOAT == type_kind && ObFloatType == datum_type.type_) {
    func = &DataLoader::load_float;
  } else if (orc::TypeKind::DOUBLE == type_kind && ObDoubleType == datum_type.type_) {
    func = &DataLoader::load_double;
  }
  return func;
}

int ObOrcTableRowIterator::get_data_column_batch_idxs(const orc::Type *type, const int col_id, ObIArray<int> &idxs)
{
  int ret = OB_SUCCESS;
  CK (type != NULL);
  OZ (idxs.reserve(type->getSubtypeCount()));
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    const orc::Type *cur = type;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < type->getSubtypeCount(); i++) {
      if (type->getSubtype(i)->getColumnId() == col_id) {
        OZ (idxs.push_back(i));
        found = true;
      } else if (type->getSubtype(i)->getColumnId() < col_id && col_id < type->getSubtype(i)->getMaximumColumnId()) {
        OZ (idxs.push_back(i));
        type = type->getSubtype(i);
      } else {
        //do nothing
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cur == type && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get data colum batch failed", K(ret));
    }
  }
  return ret;
}

ObExternalTableAccessOptions &
ObOrcTableRowIterator::make_external_table_access_options(stmt::StmtType stmt_type)
{
  if (stmt::T_INSERT == stmt_type) {
    options_ = ObExternalTableAccessOptions::disable_cache_defaults();
  } else {
    options_ = ObExternalTableAccessOptions::lazy_defaults();
  }
  return options_;
}

int ObOrcTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_convert_exprs_);
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);
  if (OB_UNLIKELY(!state_.has_rows_in_row_range())) {
    if (OB_FAIL(next_row_range())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next row group", K(ret));
      }
    } else if (OB_UNLIKELY(!state_.has_rows_in_row_range())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iterator state", K(ret), K_(state));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!file_column_exprs_.count()) {
    read_count = std::min(capacity, state_.remain_rows_in_range());
  } else if (OB_UNLIKELY(!row_reader_ || !orc_batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row reader is null", K(ret));
  } else {
    try {
      int64_t remain_rows = state_.remain_rows_in_range();
      orc_batch_->capacity = std::min(capacity, remain_rows);
      if (row_reader_->next(*orc_batch_)) {
        //ok
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read next batch failed", K(ret), K(state_));
      }
    } catch(const ObErrorCodeException &ob_error) {
      if (OB_SUCC(ret)) {
        ret = ob_error.get_error_code();
        LOG_WARN("fail to read orc file", K(ret));
      }
    } catch(const std::exception& e) {
      if (OB_SUCC(ret)) {
        ret = OB_ORC_READ_ERROR;
        LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
        LOG_WARN("unexpected error", K(ret), "Info", e.what());
      }
    } catch(...) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      }
    }

    //load vec data from parquet file to file column expr
    for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      if (OB_ISNULL(file_column_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file column expr is null", K(ret));
      } else {
        int idx = -1;
        int64_t col_id = -1;
        const orc::Type *col_type = nullptr;
        ObString col_name;
        OZ(compute_column_id_by_index_type(i, col_id));

        OZ (id_to_type_.get_refactored(col_id, col_type));
        ObArray<int> idxs;
        OZ (get_data_column_batch_idxs(&row_reader_->getSelectedType(), col_id, idxs));
        DataLoader loader(eval_ctx, file_column_exprs_.at(i), orc_batch_, capacity, idxs, read_count, col_type);
        OZ (file_column_exprs_.at(i)->init_vector_for_write(
              eval_ctx, file_column_exprs_.at(i)->get_default_res_format(), eval_ctx.max_batch_size_));
        OZ (loader.load_data_for_col(load_funcs_.at(i)));
        if (OB_SUCC(ret)) {
          file_column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
        }
        if (OB_SUCC(ret) && read_count == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read result count is zero", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && read_count > 0) {
    //fill expr results from metadata
    for (int64_t i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      ObExpr *meta_expr = file_meta_column_exprs_.at(i);
      CK (OB_NOT_NULL(meta_expr));
      if (OB_FAIL(ret)) {
      } else if (meta_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
        StrDiscVec *text_vec = static_cast<StrDiscVec *>(meta_expr->get_vector(eval_ctx));
        CK (OB_NOT_NULL(text_vec));
        OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, read_count));
        if (OB_SUCC(ret)) {
          text_vec->set_ptrs(file_url_ptrs_.get_data());
          text_vec->set_lens(file_url_lens_.get_data());
        }
      } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_UNIFORM, read_count));
        OZ (fill_file_partition_expr(meta_expr, state_.part_list_val_, read_count));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", KPC(meta_expr));
      }
      meta_expr->set_evaluated_projected(eval_ctx);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      //column_conv_exprs is 1-1 mapped to column_exprs
      //calc gen column exprs
      CK (OB_NOT_NULL(column_conv_exprs.at(i)));
      if (OB_FAIL(ret)) {
      } else if (!column_conv_exprs.at(i)->get_eval_info(eval_ctx).evaluated_) {
        OZ (column_conv_exprs.at(i)->init_vector_default(eval_ctx, read_count));
        CK (OB_NOT_NULL(bit_vector_cache_));
        OZ (column_conv_exprs.at(i)->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
        OX (column_conv_exprs.at(i)->set_evaluated_projected(eval_ctx));
      }
      //assign gen column exprs value to column exprs(output exprs)
      if (OB_SUCC(ret)) {
        ObExpr *to = column_exprs_.at(i);
        ObExpr *from = column_conv_exprs.at(i);
        CK (OB_NOT_NULL(to));
        CK (OB_NOT_NULL(from));
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
        if (OB_FAIL(ret)) {
        } else if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
          CK (OB_NOT_NULL(from_datum));
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          CK (OB_NOT_NULL(to_vec));
          if (OB_SUCC(ret)) {
            ObDatum *to_datums = to_vec->get_datums();
            CK (OB_NOT_NULL(to_datums));
            for (int64_t j = 0; j < read_count && OB_SUCC(ret); j++) {
              to_datums[j] = *from_datum;
            }
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
          CK (OB_NOT_NULL(uni_vec));
          if (OB_SUCC(ret)) {
            ObDatum *src = uni_vec->get_datums();
            ObDatum *dst = to->locate_batch_datums(eval_ctx);
            CK (OB_NOT_NULL(src));
            CK (OB_NOT_NULL(dst));
            if (OB_SUCC(ret) && src != dst) {
              MEMCPY(dst, src, read_count * sizeof(ObDatum));
            }
            OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          }
        } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
          LOG_WARN("assign vector header failed", K(ret));
        }
        column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
    OZ (calc_exprs_for_rowid(read_count, state_));
  }
  if (OB_SUCC(ret)) {
    state_.cur_range_read_row_count_ += read_count;
    reader_metrics_.read_rows_count += read_count;
    count = read_count;
    LOG_TRACE("orc read row", K_(state), K(read_count));
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  row_count_ = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int64_vec));
    CK (VEC_FIXED == int64_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else if (!long_batch->hasNulls) {
            CK (OB_NOT_NULL(long_batch->data.data()));
            CK (OB_NOT_NULL(int64_vec->get_data()));
            if (OB_SUCC(ret)) {
              MEMCPY(pointer_cast<int64_t*>(int64_vec->get_data()), long_batch->data.data(), sizeof(int64_t) * row_count_);
            }
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              CK (OB_NOT_NULL(long_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                if (OB_ISNULL(valid_bytes)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("orc not null batch valid bytes is null", K(ret));
                } else if (*valid_bytes == 1) {
                  int64_vec->set_int(i, long_batch->data[i]);
                } else {
                  int64_vec->set_null(i);
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int64 vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_year_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      } else {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(long_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (long_batch->hasNulls) {
                CK (OB_NOT_NULL(long_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    int32_vec->set_year(i, long_batch->data[i]);
                  } else {
                    int32_vec->set_null(i);
                  }
                }
              } else {
                int32_vec->set_year(i, long_batch->data[i]);
              }
            }
          }
        }
      }
    }
  }
  LOG_DEBUG("load year vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int32_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (long_batch->hasNulls) {
                CK (OB_NOT_NULL(long_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
                  } else {
                    int32_vec->set_null(i);
                  }
                }
              } else {
                int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int32 vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(text_vec));
    CK (VEC_DISCRETE == text_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          bool is_oracle_mode = lib::is_oracle_mode();
          bool is_byte_length = is_oracle_byte_length(
              is_oracle_mode, file_col_expr_->datum_meta_.length_semantics_);
          orc::StringVectorBatch *string_batch = dynamic_cast<orc::StringVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!string_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (string_batch->hasNulls) {
                CK (OB_NOT_NULL(string_batch->data.data()));
                CK (OB_NOT_NULL(string_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(string_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    if (string_batch->length[i] == 0 && is_oracle_mode) {
                      text_vec->set_null(i);
                    } else {
                      if (OB_UNLIKELY(string_batch->length[i] > file_col_expr_->max_length_
                                  && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                              pointer_cast<const char *>(string_batch->data[i]),
                                                                              string_batch->length[i]) > file_col_expr_->max_length_))) {
                        ret = OB_ERR_DATA_TOO_LONG;
                        LOG_WARN("data too long", K(ret), K(string_batch->length[i]), K(file_col_expr_->max_length_));
                      } else {
                        text_vec->set_string(i, string_batch->data[i], string_batch->length[i]);
                      }
                    }
                  } else {
                    text_vec->set_null(i);
                  }
                }
              } else {
                CK (OB_NOT_NULL(string_batch->length.data()));
                if (OB_FAIL(ret)) {
                } else if (string_batch->length[i] == 0 && is_oracle_mode) {
                  text_vec->set_null(i);
                } else {
                  CK (OB_NOT_NULL(string_batch->data.data()));
                  if (OB_FAIL(ret)) {
                  } else if (OB_UNLIKELY(string_batch->length[i] > file_col_expr_->max_length_
                                && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                            pointer_cast<const char *>(string_batch->data[i]),
                                                                            string_batch->length[i]) > file_col_expr_->max_length_))) {
                    ret = OB_ERR_DATA_TOO_LONG;
                    LOG_WARN("data too long", K(ret), K(string_batch->length[i]), K(file_col_expr_->max_length_));
                  } else {
                    text_vec->set_string(i, string_batch->data[i], string_batch->length[i]);
                  }
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_timestamp_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    int64_t adjust_us = calc_tz_adjust_us();
    LOG_DEBUG("adjust value", K(adjust_us));
    CK (OB_NOT_NULL(dec_vec));
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          row_count_ = batch_->numElements;
          orc::TimestampVectorBatch *timestamp_batch = dynamic_cast<orc::TimestampVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!timestamp_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              const uint8_t* valid_bytes = nullptr;
              CK (OB_NOT_NULL(timestamp_batch->data.data()));
              CK (OB_NOT_NULL(timestamp_batch->nanoseconds.data()));
              if (OB_SUCC(ret) && timestamp_batch->hasNulls) {
                CK (OB_NOT_NULL(timestamp_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(timestamp_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!timestamp_batch->hasNulls || *valid_bytes == 1) {
                int64_t adjusted_value = orc_timestamp_to_ob_timestamp(
                    timestamp_batch->data[i], timestamp_batch->nanoseconds[i], adjust_us);
                if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_timestamp(i, adjusted_value);
                } else if (ObDateTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_datetime(i, adjusted_value);
                } else if (ObDateType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_date(i, adjusted_value / USECS_PER_DAY);
                } else if (ObMySQLDateTimeType == file_col_expr_->datum_meta_.type_) {
                  ObMySQLDateTime mdt_value;
                  ret = ObTimeConverter::datetime_to_mdatetime(adjusted_value, mdt_value);
                  dec_vec->set_mysql_datetime(i, mdt_value);
                } else if (ObMySQLDateType == file_col_expr_->datum_meta_.type_) {
                  ObMySQLDate md_value;
                  ret = ObTimeConverter::date_to_mdate(adjusted_value / USECS_PER_DAY, md_value);
                  dec_vec->set_mysql_date(i, md_value);
                } else if (ObTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_time(i, adjusted_value);
                } else {
                  ObOTimestampData data;
                  data.time_us_ = adjusted_value;
                  data.time_ctx_.set_tail_nsec(timestamp_batch->nanoseconds[i] % NSECS_PER_USEC);
                  dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
                }
              } else {
                dec_vec->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}


int ObOrcTableRowIterator::DataLoader::load_date_to_time_or_stamp()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    int64_t adjust_us = calc_tz_adjust_us();
    LOG_DEBUG("show adjust value in date to ts", K(adjust_us));
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          row_count_ = batch_->numElements;
          orc::LongVectorBatch *date_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(date_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic dec64 batch cast failed", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
            const uint8_t* valid_bytes = nullptr;
            CK (OB_NOT_NULL(date_batch->data.data()));
            if (OB_SUCC(ret)) {
              if (date_batch->hasNulls) {
                CK (OB_NOT_NULL(date_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(date_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!date_batch->hasNulls || *valid_bytes == 1) {
                int64_t adjusted_value = date_batch->data[i] * USECS_PER_DAY + adjust_us;
                if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_timestamp(i, adjusted_value);
                } else if (ObDateTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_datetime(i, date_batch->data[i] * USECS_PER_DAY);
                } else if (ObDateType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_date(i, adjusted_value / USECS_PER_DAY);
                } else if (ObMySQLDateTimeType == file_col_expr_->datum_meta_.type_) {
                  ObMySQLDateTime mdt_value;
                  ret = ObTimeConverter::datetime_to_mdatetime(date_batch->data[i] * USECS_PER_DAY, mdt_value);
                  dec_vec->set_mysql_datetime(i, mdt_value);
                } else if (ObMySQLDateType == file_col_expr_->datum_meta_.type_) {
                  ObMySQLDate md_value;
                  ret = ObTimeConverter::date_to_mdate(adjusted_value / USECS_PER_DAY, md_value);
                  dec_vec->set_mysql_date(i, md_value);
                } else if (ObTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_time(i, adjusted_value);
                } else {
                  ObOTimestampData data;
                  data.time_us_ = adjusted_value;
                  dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
                }
              } else {
                dec_vec->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(vec));
    CK (VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::Decimal64VectorBatch *dec64_batch = dynamic_cast<orc::Decimal64VectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(dec64_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic dec64 batch cast failed", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
            CK (OB_NOT_NULL(dec64_batch->values.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = nullptr;
              if (dec64_batch->hasNulls) {
                CK (OB_NOT_NULL(dec64_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(dec64_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!dec64_batch->hasNulls || *valid_bytes == 1) {
                if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
                  ObDecimalInt *decint = NULL;
                  int32_t int_bytes = 0;
                  if (OB_FAIL(wide::from_integer(dec64_batch->values[i], tmp_alloc_g.get_allocator(), decint,
                                                int_bytes, file_col_expr_->datum_meta_.precision_))) {
                    LOG_WARN("fail to from integer", K(ret));
                  } else if (OB_ISNULL(decint)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("int to dec failed", K(ret));
                  } else {
                    vec->set_decimal_int(i, decint, int_bytes);
                  }
                } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
                  ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
                  number::ObNumber res_nmb;
                  if (OB_FAIL(res_nmb.from(dec64_batch->values[i], tmp_alloc_g.get_allocator()))) {
                    LOG_WARN("fail to from number", K(ret));
                  } else {
                    vec->set_number(i, res_nmb);
                  }
                }
              } else {
                file_col_expr_->get_vector(eval_ctx_)->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::to_numeric(const int64_t idx, const int64_t int_value)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    ObDecimalInt *decint = NULL;
    int32_t int_bytes = 0;
    if (OB_FAIL(wide::from_integer(int_value, tmp_alloc_g.get_allocator(), decint,
                                   int_bytes, file_col_expr_->datum_meta_.precision_))) {
      LOG_WARN("fail to from integer", K(ret));
    } else {
      vec->set_decimal_int(idx, decint, int_bytes);
    }
  } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
    ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(int_value, tmp_alloc_g.get_allocator()))) {
      LOG_WARN("fail to from number", K(ret));
    } else {
      vec->set_number(idx, res_nmb);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int64_to_number_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  row_count_ = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              CK (OB_NOT_NULL(long_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                if (OB_ISNULL(valid_bytes)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("orc not null batch valid bytes is null", K(ret));
                } else if (*valid_bytes == 1) {
                  OZ (to_numeric(i, long_batch->data[i]));
                } else {
                  file_col_expr_->get_vector(eval_ctx_)->set_null(i);
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int64 to number vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec128_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::Decimal128VectorBatch *dec128_batch = dynamic_cast<orc::Decimal128VectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(dec128_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic dec128 batch cast failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
          CK (OB_NOT_NULL(dec128_batch->values.data()));
          if (OB_SUCC(ret)) {
            const uint8_t* valid_bytes = nullptr;
            if (dec128_batch->hasNulls) {
              CK (OB_NOT_NULL(dec128_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                valid_bytes = reinterpret_cast<const uint8_t*>(dec128_batch->notNull.data()) + i;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!dec128_batch->hasNulls || *valid_bytes == 1) {
              ObDecimalInt *decint = NULL;
              int32_t int_bytes = sizeof(int128_t);
              int128_t val = orc_int128_to_ob_int128(dec128_batch->values[i]);
              void *data = nullptr;

              if (OB_ISNULL(data = tmp_alloc_g.get_allocator().alloc(int_bytes))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
              } else {
                decint = reinterpret_cast<ObDecimalInt *>(data);
                *decint->int128_v_ = val;
              }
              if (OB_FAIL(ret)) {
              } else if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
                ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
                CK (OB_NOT_NULL(vec));
                CK (VEC_FIXED == vec->get_format());
                if (OB_SUCC(ret)) {
                  vec->set_decimal_int(i, decint, int_bytes);
                }
              } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
                ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
                CK (OB_NOT_NULL(vec));
                if (OB_SUCC(ret)) {
                  number::ObNumber res_nmb;
                  if (OB_FAIL(wide::to_number(decint, int_bytes, file_col_expr_->datum_meta_.scale_,
                                      tmp_alloc_g.get_allocator(), res_nmb))) {
                    LOG_WARN("fail to from", K(ret));
                  } else {
                    vec->set_number(i, res_nmb);
                  }
                }
              }
            } else {
              file_col_expr_->get_vector(eval_ctx_)->set_null(i);
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_float()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *float_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  CK (OB_NOT_NULL(float_vec));
  CK (VEC_FIXED == float_vec->get_format());
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::DoubleVectorBatch *double_batch = dynamic_cast<orc::DoubleVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(double_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic double batch cast failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
          CK (OB_NOT_NULL(double_batch->data.data()));
          if (OB_FAIL(ret)) {
          } else if (double_batch->hasNulls) {
            CK (OB_NOT_NULL(double_batch->notNull.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(double_batch->notNull.data()) + i;
              if (*valid_bytes == 1) {
                float_vec->set_float(i, (float)double_batch->data[i]);
              } else {
                float_vec->set_null(i);
              }
            }
          } else {
            float_vec->set_float(i, (float)double_batch->data[i]);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_double()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  CK (OB_NOT_NULL(double_vec));
  CK (VEC_FIXED == double_vec->get_format());
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::DoubleVectorBatch *double_batch = dynamic_cast<orc::DoubleVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(double_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic double batch cast failed", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (!double_batch->hasNulls) {
          CK (OB_NOT_NULL(double_batch->data.data()));
          CK (OB_NOT_NULL(double_vec->get_data()));
          if (OB_SUCC(ret)) {
            MEMCPY(pointer_cast<double*>(double_vec->get_data()), double_batch->data.data(), sizeof(double) * row_count_);
          }
        } else {
          for (int64_t i = 0; i < row_count_; i++) {
            CK (OB_NOT_NULL(double_batch->notNull.data()));
            CK (OB_NOT_NULL(double_batch->data.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(double_batch->notNull.data()) + i;
              CK (OB_NOT_NULL(valid_bytes));
              if (OB_FAIL(ret)) {
              } else if (*valid_bytes == 1) {
                double_vec->set_double(i, double_batch->data[i]);
              } else {
                double_vec->set_null(i);
              }
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}


int ObOrcTableRowIterator::get_next_row()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

void ObOrcTableRowIterator::reset() {
  // reset state_ to initial values for rescan
  state_.reuse();
  file_prebuffer_.destroy();
}

DEF_TO_STRING(ObOrcIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("ob_external_iterator_state");
  J_COLON();
  pos += ObExternalIteratorState::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(cur_stripe_idx),
       K_(end_stripe_idx),
       K_(next_stripe_first_row_id),
       K_(cur_row_range_idx),
       K_(end_row_range_idx),
       K_(cur_range_read_row_count),
       K_(cur_range_row_count),
       K_(orc_reader_cur_row_id));
  J_OBJ_END();
  return pos;
}

}
}