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
  OZ (id_to_type_.set_refactored(type->getColumnId(), type, 1 /*overwrite*/));
  if (OB_FAIL(ret)) {
  } else if (orc::TypeKind::STRUCT == type->getKind()) {
    for (size_t i = 0; OB_SUCC(ret) && i < type->getSubtypeCount(); ++i) {
      const std::string& cpp_field_name = type->getFieldName(i);
      ObString field_name;
      OZ (ob_write_string(allocator_, ObString(cpp_field_name.c_str()), field_name));
      OZ (col_names.push_back(field_name));
      ObString path;
      OZ (to_dot_column_path(col_names, path));
      OZ (name_to_id_.set_refactored(path, type->getSubtype(i)->getColumnId(), 1 /*overwrite*/));
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

int ObOrcTableRowIterator::build_iceberg_id_to_type_map(const orc::Type* type)
{

  int ret = OB_SUCCESS;
  CK (type != nullptr);

  if (OB_SUCC(ret)) {
    if (type->hasAttributeKey(ICEBERG_ID_KEY)) {
      // 有 iceberg.id 属性，转为数字做 key
      const std::string &id_val = type->getAttributeValue(ICEBERG_ID_KEY);
      int64_t iceberg_id = 0;
      if (OB_FAIL(c_str_to_int(id_val.c_str(), iceberg_id))) {
        LOG_WARN("fail to convert iceberg.id to int", K(ret), K(id_val.c_str()));
      } else if (OB_FAIL(iceberg_id_to_type_.set_refactored(iceberg_id, type, 0))) {
        LOG_WARN("fail to set iceberg id to type", K(ret), K(iceberg_id));
      }
    }
  }

  // non-primitive type
  for (size_t j = 0; OB_SUCC(ret) && j < type->getSubtypeCount(); ++j) {
    OZ (build_iceberg_id_to_type_map(type->getSubtype(j)));
  }

  return ret;
}

int ObOrcTableRowIterator::prepare_read_orc_file()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
    int64_t orc_col_id = -1;
    OZ (compute_column_id_by_table_type(i, orc_col_id));
    CK (orc_col_id != -1);
    orc::ColumnVectorBatch *batch = nullptr;
    const orc::Type *type = nullptr;
    ObColumnDefaultValue *default_value = is_lake_table() ?
                                          &colid_default_value_arr_.at(i) : nullptr;

    if (OB_SUCC(ret)) {
      int tmp_ret = is_iceberg_lake_table() && file_contains_attribute_key_ ?
                    iceberg_id_to_type_.get_refactored(orc_col_id, type) :
                    id_to_type_.get_refactored(orc_col_id, type);

      if (OB_HASH_NOT_EXIST == tmp_ret && is_lake_table()) {
        type = nullptr;
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to get id to type", K(ret), K(orc_col_id));
      }
      if (OB_SUCC(ret)) {
        column_indexs_.at(i) = type == nullptr ? -1 : orc_col_id;
      }
    }
    if (OB_SUCC(ret)) {
      bool need_init_project_loader = true;
      if (is_eager_column_.count() > 0 && is_eager_column_.at(i)) {
        OrcRowReader &eager_reader = sector_reader_->get_eager_reader();
        if (OB_FAIL(init_data_loader(i, orc_col_id, type, eager_reader, default_value))) {
          LOG_WARN("fail to init data loader", K(ret), K(i));
        } else if (!is_dup_project_.at(i)) {
          // the column only in eager reader, no need to init project loader
          need_init_project_loader = false;
        }
      }
      if (OB_SUCC(ret) && need_init_project_loader) {
        if (OB_FAIL(init_data_loader(i, orc_col_id, type, project_reader_, default_value))) {
          LOG_WARN("fail to init data loader", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && has_skip_index_filter_) {
    ObSEArray<ObColumnMeta, 8> column_metas;
    ObSEArray<uint64_t, 8> column_ids;
    if (OB_FAIL(column_metas.prepare_allocate(mapping_column_ids_.count()))) {
      LOG_WARN("failed to prepare allocate column metas");
    } else if (OB_FAIL(column_ids.prepare_allocate(mapping_column_ids_.count()))) {
      LOG_WARN("failed to prepare allocate column ids");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count(); ++i) {
      column_ids.at(i) = mapping_column_ids_.at(i).first;
      if (OB_INVALID_ID == mapping_column_ids_.at(i).first) {
        // do nothing
      } else if (OB_FAIL(column_metas.at(i).from_ob_expr(
                                            column_exprs_.at(mapping_column_ids_.at(i).second)))) {
        LOG_WARN("failed to init column meta from ob expr");
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(prepare_filter_col_meta(column_indexs_, column_ids, column_metas))) {
      LOG_WARN("fail to prepare filter col meta",
              K(ret), K(column_indexs_.count()), K(column_metas.count()));
    }
  }
  if (OB_SUCC(ret) && !has_pushdown_filter()) {
    // disable row indexes caching for no filter column
    reader_ctx_.rowIndexes = nullptr;
  }
  return ret;
}

int ObOrcTableRowIterator::init_data_loader(int64_t i, int64_t orc_col_id, const orc::Type *type,
                                            OrcRowReader &reader, ObColumnDefaultValue *default_value)
{
  int ret = OB_SUCCESS;
  orc::ColumnVectorBatch *batch = nullptr;
  ObExpr* column_expr = get_column_expr_by_id(i);
  if (type == nullptr && is_lake_table()) {
    // init data loader for iceberg table with default value
    if (OB_FAIL(reader.data_loaders_.at(i).init(column_expr, default_value))) {
      LOG_WARN("fail to init data loader", K(ret), K(i));
    } else {
      CK (reader.data_loaders_.at(i).has_load_func());
    }
  } else if (OB_UNLIKELY(!reader.row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row reader is null", K(ret));
  } else if (OB_FAIL(get_data_column_batch(&reader.row_reader_->getSelectedType(),
      dynamic_cast<const orc::StructVectorBatch *>(reader.orc_batch_.get()), orc_col_id, batch))) {
    LOG_WARN("fail to get data column batch", K(ret), K(i));
  } else if (OB_FAIL(reader.data_loaders_.at(i).init(column_expr, batch, type))) {
    LOG_WARN("fail to init data loader", K(ret), K(i));
  } else if (!reader.data_loaders_.at(i).has_load_func()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    if (i >= reader.row_reader_->getSelectedType().getSubtypeCount()) {
      //error for report
      LOG_WARN("not supported type", K(ret), K(column_expr->datum_meta_));
      const char *ob_type = ob_obj_type_str(column_expr->datum_meta_.type_);
      LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, "", ob_type);
    } else {
      std::string p_type = reader.row_reader_->getSelectedType().getSubtype(i) == nullptr ?
              "INVALID ORC TYPE" : reader.row_reader_->getSelectedType().getSubtype(i)->toString();
      int64_t pos = 0;
      ObArrayWrap<char> buf;
      ObArray<ObString> extended_type_info;
      ObDatumMeta &meta = column_expr->datum_meta_;
      const char *ob_type = ob_obj_type_str(column_expr->datum_meta_.type_);
      if (OB_SUCCESS == buf.allocate_array(allocator_, 100)) {
        ob_sql_type_str(buf.get_data(), buf.count(), pos, meta.type_,
                        OB_MAX_VARCHAR_LENGTH, meta.precision_, meta.scale_, meta.cs_type_,
                        extended_type_info);
        if (pos < buf.count()) {
          buf.at(pos++) = '\0';
          ob_type = buf.get_data();
        }
      }
      LOG_WARN("not supported type", K(ret), K(column_expr->datum_meta_),
                K(ObString(p_type.length(), p_type.data())));
      LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, p_type.c_str(), ob_type);
    }
  }
  return ret;
}

int ObOrcTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;

  CK (scan_param != nullptr);
  CK (scan_param->op_ != nullptr);
  CK (scan_param->ext_column_dependent_exprs_ != nullptr);
  if (OB_SUCC(ret)) {
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    int64_t column_cnt = scan_param->ext_column_dependent_exprs_->count();
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
    OZ (iceberg_id_to_type_.create(512, mem_attr_));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (ObExternalTablePushdownFilter::init(scan_param->pd_storage_filters_,
                                            scan_param->ext_tbl_filter_pd_level_,
                                            scan_param->column_ids_,
                                            eval_ctx));
    OZ (reader_profile_.register_metrics(&reader_metrics_, "READER_METRICS"));
    OZ (data_access_driver_.register_io_metrics(reader_profile_, "IO_METRICS"));
    OZ (file_prebuffer_.register_metrics(reader_profile_, "PREBUFFER_METRICS"));

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
    const sql::ColumnIndexType index_type =
                                  scan_param_->external_file_format_.orc_format_.column_index_type_;
    // todo(zhengjin)
    if (OB_SUCC(ret) && !is_iceberg_lake_table() && index_type == sql::ColumnIndexType::NAME) {
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

    if (is_lake_table()) {
      OZ(ObExternalTableRowIterator::init_default_batch(file_column_exprs_));
    }
    if (is_iceberg_lake_table()) {
      OZ(ObExternalTableRowIterator::init_for_iceberg(&options_));
    }

    if (OB_SUCC(ret) && file_column_exprs_.count() > 0) {
      OZ (column_indexs_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (project_reader_.data_loaders_.allocate_array(allocator_, file_column_exprs_.count()));
    }
    if (OB_SUCC(ret)) {
      if (scan_param_->ext_enable_late_materialization_ &&
          scan_param->pd_storage_filters_ != nullptr) {
        // build filter expr rels for late materialization
        OZ (build_filter_expr_rels(scan_param->pd_storage_filters_, this));
      }
    }

    if (OB_SUCC(ret)) {
      OZ (file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
      OZ (file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
      OZ (row_ranges_.allocate_array(allocator_, 16));
      OZ (init_query_flag());
    }
  }
  return ret;
}

int ObOrcTableRowIterator::init_query_flag()
{
  int ret = OB_SUCCESS;
  query_flag_ = 0;
  if (0 == file_column_exprs_.count()) {
    // count aggregation query, only need to read file row count
    // the query is like 'select count(*) from tbl'
    is_count_aggr_ = true;
  } else if (scan_param_->pd_storage_filters_ != nullptr) {
    has_skip_index_filter_ = has_skip_index_filter();
    const int64_t total_column_count = is_eager_column_.count();
    int64_t eager_column_count = 0;
    bool has_dup_project_column = false;
    for (int64_t i = 0; i < total_column_count; ++i) {
      if (is_eager_column_.at(i)) {
        ++eager_column_count;
      }
      if (is_dup_project_.at(i)) {
        has_dup_project_column = true;
      }
    }
    if (eager_column_count > 0) {
      has_eager_column_ = true;
      if (eager_column_count == total_column_count) {
        // all columns are eager columns
        if (!has_dup_project_column) {
          // the query is like 'select count(*) from tbl where c1 > 10 and c2 > 20'
          is_count_aggr_with_filter_ = true;
        }
      }
    } else if (filter_expr_rels_.size() > 0) {
      is_file_meta_filter_ = true;
    }
    if (has_skip_index_filter_ || (has_eager_column_ && !is_count_aggr_with_filter_)) {
      // pre buffer row index for skip index or lazy column seek
      need_pre_buffer_index_ = options_.enable_prebuffer_;
    }
  }
  LOG_TRACE("orc query flag", K_(is_count_aggr), K_(is_count_aggr_with_filter), K_(is_file_meta_filter),
            K_(has_skip_index_filter), K_(has_eager_column), K_(need_pre_buffer_index));
  return ret;
}

int ObOrcTableRowIterator::init_sector_reader()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_sector_reader_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(sizeof(SectorReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for sector reader", K(ret));
    } else {
      inner_sector_reader_ = new (mem) SectorReader(allocator_);
      if (OB_FAIL(inner_sector_reader_->init(this))) {
        LOG_WARN("failed to init sector reader", K(ret));
      }
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
        // if eager reader is not null, the access reader is followed by eager reader, we no need to
        // seek the row reader to the first row id of the row range.
        if (sector_reader_ != nullptr) {
          sector_reader_->seek_to_row(row_range.first_row_id);
        } else if (project_reader_.row_reader_) {
          project_reader_.row_reader_->seekToRow(row_range.first_row_id);
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

int ObOrcTableRowIterator::select_row_ranges(const int64_t stripe_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null reader", K(ret));
  } else if (is_count_aggr_) {
    // mark whole file range for count aggregation query
    int64_t file_rows_count = reader_->getNumberOfRows();
    if (is_iceberg_lake_table() && OB_NOT_NULL(delete_bitmap_)) {
      file_rows_count -= delete_bitmap_->get_cardinality();
    }
    SelectedRowRange whole_stripe_range;
    whole_stripe_range.first_row_id = 0;
    whole_stripe_range.num_rows = file_rows_count;
    row_ranges_.at(0) = whole_stripe_range;
    state_.cur_row_range_idx_ = 0;
    state_.end_row_range_idx_ = 0;
  } else {
    reader_ctx_.currentStripeInfo = reader_->getStripe(stripe_idx);
    if (!reader_ctx_.currentStripeInfo) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc stripe info is null", K(ret));
    } else {
      // start new stripe, set stripe info to cached reader context
      const int64_t stripe_num_rows = reader_ctx_.currentStripeInfo->getNumberOfRows();
      const int64_t first_row_id = state_.next_stripe_first_row_id_;
      state_.next_stripe_first_row_id_ += stripe_num_rows;
      bool build_whole_stripe_range = true;
      if (has_pushdown_filter()) {
        if (OB_FAIL(select_row_ranges_by_pushdown_filter(stripe_idx,
                                                         first_row_id,
                                                         stripe_num_rows,
                                                         build_whole_stripe_range))) {
          LOG_WARN("fail to select row ranges by pushdown filters", K(ret));
        }
      } else if (need_pre_buffer_index_ && OB_FAIL(pre_buffer(true /* row index */))) {
        // pre buffer row index for lazy seek
        LOG_WARN("fail to pre buffer row index", K(ret));
      }
      if (OB_SUCC(ret) && build_whole_stripe_range) {
        SelectedRowRange whole_stripe_range;
        whole_stripe_range.first_row_id = first_row_id;
        whole_stripe_range.num_rows = stripe_num_rows;
        row_ranges_.at(0) = whole_stripe_range;
        state_.cur_row_range_idx_ = 0;
        state_.end_row_range_idx_ = 0;
      }
      if (OB_SUCC(ret) && state_.has_row_range() && options_.enable_prebuffer_) {
        if (OB_FAIL(pre_buffer(false /* row index */))) {
          LOG_WARN("fail to pre buffer data", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::select_row_ranges_by_pushdown_filter(
    const int64_t stripe_idx,
    const int64_t stripe_first_row_id,
    const int64_t stripe_num_rows,
    bool &build_whole_stripe_range)
{
  int ret = OB_SUCCESS;
  build_whole_stripe_range = false;
  bool is_stripe_filtered = false;
  int64_t groups_filtered = 0;
  int64_t groups_in_stripe = 0;
  // filter stripe by stripe statistic.
  std::unique_ptr<orc::StripeStatistics> stripe_stat =
    reader_->getStripeStatistics(stripe_idx, /*includeRowIndex*/ false);
  if (OB_UNLIKELY(!stripe_stat)) {
    // no stripe statistics, do nothing
    build_whole_stripe_range = true;
  } else if (OB_FAIL(filter_by_statistic(STRIPE_LEVEL, stripe_stat.get(), is_stripe_filtered))) {
    LOG_WARN("fail to apply skipping index filter", K(ret), K(stripe_idx));
  } else if (!is_stripe_filtered) {
    if (need_pre_buffer_index_ && OB_FAIL(pre_buffer(true /* row index */))) {
      LOG_WARN("fail to pre buffer row index", K(ret));
    } else {
      stripe_stat = reader_->getStripeStatistics(stripe_idx, /*includeRowIndex*/ true,
                                                 selected_columns_.get_data());
      if (OB_UNLIKELY(!stripe_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get stripe statistics", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
    // filter row groups by row index.
    const int64_t row_index_stride = reader_->getRowIndexStride();
    if (OB_UNLIKELY(row_index_stride <= 0)) {
      // row index is disabled, need to build whole stripe range.
      build_whole_stripe_range = true;
    } else {
      groups_in_stripe = (stripe_num_rows + row_index_stride - 1) / row_index_stride;
      if (OB_UNLIKELY(groups_in_stripe > row_ranges_.count())) {
        const int64_t new_array_size = next_pow2(groups_in_stripe);
        if (OB_FAIL(row_ranges_.allocate_array(allocator_, static_cast<size_t>(new_array_size)))) {
          LOG_WARN("fail to allocate row group array", K(ret), K(new_array_size));
        }
      }
      if (OB_SUCC(ret)) {
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
              last_row_range.first_row_id = first_row_id;
              last_row_range.num_rows = row_index_stride;
            } else {
              // merge this range to last_row_range
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
  }
  if (OB_SUCC(ret)) {
    // update reader metrics
    if (build_whole_stripe_range) { // no statistic or read orc exception
      ++reader_metrics_.selected_row_group_count_;
      reader_metrics_.selected_page_count_ += groups_in_stripe;
    } else if (is_stripe_filtered) {
      ++reader_metrics_.skipped_row_group_count_;
    } else {
      ++reader_metrics_.selected_row_group_count_;
      reader_metrics_.selected_page_count_ += (groups_in_stripe - groups_filtered);
      reader_metrics_.skipped_page_count_ += groups_filtered;
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
    int64_t file_size = -1;
    if (data_access_driver_.is_opened()) {
      data_access_driver_.close();
    }

    do {
      if ((task_idx = state_.file_idx_++) >= scan_param_->key_ranges_.count()) {
        ret = OB_ITER_END;
      } else {
        state_.cur_file_url_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
        file_size = -1;
        int64_t modify_time = 0;
        ObString file_content_digest;
        url_.reuse();
        const char *split_char = "/";

        if (OB_FAIL(ret)) {
        } else if (!is_iceberg_lake_table() && !is_abs_url(state_.cur_file_url_)) {
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
          // do nothing
        } else {
          if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
            file_size = scan_param_->key_ranges_.at(task_idx)
                          .get_start_key()
                          .get_obj_ptr()[ObExternalTableUtils::FILE_SIZE]
                          .get_int();
            modify_time = scan_param_->key_ranges_.at(task_idx)
                            .get_start_key()
                            .get_obj_ptr()[ObExternalTableUtils::MODIFY_TIME]
                            .get_int();
            file_content_digest = scan_param_->key_ranges_.at(task_idx)
                                    .get_start_key()
                                    .get_obj_ptr()[ObExternalTableUtils::CONTENT_DIGEST]
                                    .get_string();
          }
          if (file_size < 0 || modify_time <= 0) {
            OZ(ObExternalTableUtils::collect_file_basic_info(
              scan_param_->external_file_location_, scan_param_->external_file_access_info_,
              url_.string(), allocator_, file_size, modify_time, file_content_digest));
          }
          if (OB_FAIL(ret)) {
          } else if (file_size > 0) {
            ObExternalFileUrlInfo file_info(scan_param_->external_file_location_,
                                            scan_param_->external_file_access_info_, url_.string(),
                                            file_content_digest, file_size, modify_time);
            ObExternalFileCacheOptions cache_options(options_.enable_page_cache_,
                                                     options_.enable_disk_cache_);
            if (OB_FAIL(data_access_driver_.open(file_info, cache_options))) {
              if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret) {
                ret = OB_SUCCESS;
                file_size = 0;
              } else {
                LOG_WARN("fail to open file", K(ret), K(file_info), K(state_.cur_file_url_));
              }
            }
            if (OB_SUCC(ret)) {
              ObString expr_file_url;
              if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
                ObSqlString full_name;
                if (ip_port_.empty()) {
                  OZ(gen_ip_port(allocator_));
                }
                OZ(full_name.append_fmt("%.*s%%%.*s", ip_port_.length(), ip_port_.ptr(),
                                        state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
                OZ(ob_write_string(allocator_, full_name.string(), expr_file_url));
              } else {
                expr_file_url = state_.cur_file_url_;
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
                file_url_ptrs_.at(i) = expr_file_url.ptr();
                file_url_lens_.at(i) = expr_file_url.length();
              }
            }
          }
        }
        LOG_DEBUG("current external file", K(url_), K(file_size));
      }
    } while (OB_SUCC(ret) && OB_UNLIKELY(0 == file_size)); //skip not exist or empty file

    if (OB_SUCC(ret)) {
      // read orc file footer
      int64_t part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
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

      state_.cur_file_id_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
      if (OB_SUCC(ret)) {
        // init or reuse sector reader for current orc file
        sector_reader_ = nullptr;
        if (has_eager_column_ || is_file_meta_filter_) {
          OZ (init_sector_reader());
          sector_reader_ = inner_sector_reader_;
        } else if (is_iceberg_lake_table()) {
          const bool has_delete_file = !scan_param_->key_ranges_.at(state_.file_idx_ - 1).get_start_key().get_obj_ptr()[ObExternalTableUtils::DELETE_FILE_URLS].is_null();
          if (has_delete_file) {
            OZ (init_sector_reader());
            sector_reader_ = inner_sector_reader_;
          }
        }
      }
      OZ(create_file_reader(url_.string(), data_access_driver_, file_prebuffer_, file_size, reader_));
      try {
        iceberg_id_to_type_.reuse();
        if (OB_FAIL(ret)) {
        } else if (!reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orc create reader failed", K(ret));
          throw std::bad_exception();
        } else if (is_count_aggr_) {
          // mark the file is big stripe, the rows count of the stripe is file rows count
          state_.cur_stripe_idx_ = 0;
          state_.end_stripe_idx_ = 0;
          if (is_iceberg_lake_table()) {
            OZ (build_delete_bitmap(state_.cur_file_url_, state_.file_idx_ - 1));
          }
        } else if (OB_FAIL(create_row_readers())) {
          LOG_WARN("fail to create row readers", K(ret));
        } else {
          id_to_type_.reuse();
          name_to_id_.reuse();

          if (!is_iceberg_lake_table() || !file_contains_attribute_key_) {
            if (project_reader_.row_reader_) {
              ObArray<ObString> col_names;
              if (OB_FAIL(SMART_CALL(build_type_name_id_map(&project_reader_.row_reader_->getSelectedType(),
                                                            col_names)))) {
                LOG_WARN("fail to build type name id map", K(ret));
              }
            }
            if (OB_SUCC(ret) && has_eager_column_ && sector_reader_->get_eager_reader().row_reader_) {
              ObArray<ObString> col_names;
              if (OB_FAIL(SMART_CALL(build_type_name_id_map(&sector_reader_->get_eager_reader().row_reader_->getSelectedType(),
                                                            col_names)))) {
                LOG_WARN("fail to build type name id map", K(ret));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(prepare_read_orc_file())) {
            LOG_WARN("fail to prepare read orc file", K(ret));
          } else if (OB_FAIL(filter_file(task_idx))) {
            LOG_WARN("fail to filter file and stripes", K(ret));
          } else if (OB_UNLIKELY(!state_.has_stripe())) {
            // no stripe after filter
          } else if (is_iceberg_lake_table() && OB_FAIL(build_delete_bitmap(state_.cur_file_url_,
                                                                        state_.file_idx_ - 1))) {
            LOG_WARN("fail to build delete bitmap", K(ret));
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

bool ObOrcTableRowIterator::is_contain_attribute_key(const orc::Type *type)
{
  bool contains_id = false;

  if (type != nullptr && type->hasAttributeKey(ICEBERG_ID_KEY)) {
    contains_id = true;
  }

  // non-primitive type
  for (size_t j = 0; j < type->getSubtypeCount(); ++j) {
    contains_id &= is_contain_attribute_key(type->getSubtype(j));
  }

  return contains_id;
}

int ObOrcTableRowIterator::create_row_readers()
{
  int ret = OB_SUCCESS;
  std::list<std::string> project_column_names;
  std::list<uint64_t> project_column_ids;
  std::list<std::string> eager_column_names;
  std::list<uint64_t> eager_column_ids;

  if (is_iceberg_lake_table()) {
    orc::RowReaderOptions rowReaderOptions;
    all_row_reader_ = reader_->createRowReader(rowReaderOptions);
    const orc::Type *all_orc_type = &all_row_reader_->getSelectedType();

    file_contains_attribute_key_ = is_contain_attribute_key(all_orc_type);
    if (file_contains_attribute_key_) {
      OZ (build_iceberg_id_to_type_map(all_orc_type));
    }
  }

  ColumnIndexType column_index_type = is_iceberg_lake_table() && file_contains_attribute_key_ ?
      sql::ColumnIndexType::ID : scan_param_->external_file_format_.orc_format_.column_index_type_;

  switch (column_index_type) {
    case sql::ColumnIndexType::NAME: {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        ObDataAccessPathExtraInfo *data_access_info =
            static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
        if (OB_SUCC(ret)) {
          std::string col_name(data_access_info->data_access_path_.ptr(),
                                data_access_info->data_access_path_.length());
          bool is_project_column = true;
          if (is_eager_column_.count() > 0 && is_eager_column_.at(i)) {
            eager_column_names.push_front(col_name);
            if (!is_dup_project_.at(i)) {
              is_project_column = false;
            }
          }
          if (is_project_column) {
            project_column_names.push_front(col_name);
          }
        }
      }
      break;
    }
    case sql::ColumnIndexType::POSITION: {
      for (uint64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        bool is_project_column = true;
        int64_t column_id = file_column_exprs_.at(i)->extra_ - 1;
        if (is_eager_column_.count() > 0 && is_eager_column_.at(i)) {
          eager_column_ids.push_back(column_id);
          if (!is_dup_project_.at(i)) {
            is_project_column = false;
          }
        }
        if (is_project_column) {
          project_column_ids.push_back(column_id);
        }
      }
      break;
    }
    case sql::ColumnIndexType::ID: {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        int64_t column_id = file_column_exprs_.at(i)->extra_;
        CK (column_id != -1);

        if (OB_SUCC(ret)) {
          const orc::Type *type = nullptr;
          int tmp_ret = iceberg_id_to_type_.get_refactored(column_id, type);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            type = nullptr;
            ret = OB_SUCCESS;
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("fail to get id to type", K(ret), K(column_id));
          }

          if (OB_SUCC(ret) && type != nullptr) {
            bool is_project_column = true;
            int64_t orc_col_id = type->getColumnId() - 1;
            if (is_eager_column_.count() > 0 && is_eager_column_.at(i)) {
              eager_column_ids.push_back(orc_col_id);
              if (!is_dup_project_.at(i)) {
                is_project_column = false;
              }
            }
            if (is_project_column) {
              project_column_ids.push_back(orc_col_id);
            }
          }
        }
      }
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
      reader_->setCachedReaderContext(&reader_ctx_);
      int64_t capacity = MAX(1, scan_param_->op_->get_eval_ctx().max_batch_size_);
      if (project_column_ids.size() > 0) {
        if (is_hive_lake_table()) {
          project_reader_.init_for_hive_table(capacity, project_column_ids, reader_.get());
        } else {
          project_reader_.init(capacity, project_column_ids, reader_.get());
        }
      } else if (project_column_names.size() > 0) {
        project_reader_.init(capacity, project_column_names, reader_.get());
      } else {
        project_reader_.row_id_ = 0;
      }
      if (sector_reader_ != nullptr) {
        if (eager_column_names.size() > 0) {
          sector_reader_->get_eager_reader().init(capacity, eager_column_names, reader_.get());
        } else if (eager_column_ids.size() > 0) {
          if (is_hive_lake_table()) {
            sector_reader_->get_eager_reader().init_for_hive_table(capacity, eager_column_ids, reader_.get());
          } else {
            sector_reader_->get_eager_reader().init(capacity, eager_column_ids, reader_.get());
          }
        } else {
          sector_reader_->get_eager_reader().row_id_ = 0;
        }
      }
      if (OB_FAIL(init_selected_columns())) {
        LOG_WARN("fail to init selected columns", K(ret));
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
  }
  return ret;
}

int ObOrcTableRowIterator::init_selected_columns()
{
  int ret = OB_SUCCESS;
  int64_t column_count = 0;
  if (project_reader_.row_reader_) {
    column_count = project_reader_.row_reader_->getSelectedColumns().size();
  } else if (NULL != sector_reader_ && sector_reader_->get_eager_reader().row_reader_) {
    column_count = sector_reader_->get_eager_reader().row_reader_->getSelectedColumns().size();
  }
  if (OB_UNLIKELY(column_count > selected_columns_.count())) {
    if (OB_FAIL(selected_columns_.allocate_array(allocator_, column_count))) {
      LOG_WARN("fail to allocate array", K(ret));
    }
  }
  if (OB_SUCC(ret) && selected_columns_.count() > 0) {
    MEMSET(selected_columns_.get_data(), 0x00, selected_columns_.count());
    if (project_reader_.row_reader_) {
      const std::vector<bool> &project_columns = project_reader_.row_reader_->getSelectedColumns();
      for (int64_t i = 0; OB_SUCC(ret) && i < project_columns.size(); ++i) {
        if (project_columns[i]) {
          selected_columns_.at(i) = true;
        }
      }
    }
    if (NULL != sector_reader_ && sector_reader_->get_eager_reader().row_reader_) {
      const std::vector<bool> &eager_columns = sector_reader_->get_eager_reader().row_reader_->getSelectedColumns();
      for (int64_t i = 0; OB_SUCC(ret) && i < eager_columns.size(); ++i) {
        if (eager_columns[i] && !selected_columns_.at(i)) {
          selected_columns_.at(i) = true;
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
  if (!has_pushdown_filter()) {
    // do nothing, no filter to skip
  } else {
    std::unique_ptr<orc::Statistics> orc_col_stat = reader_->getStatistics();
    if (!orc_col_stat) {
      // no column statistics, do nothing
    } else if (OB_FAIL(filter_by_statistic(PushdownLevel::FILE, orc_col_stat.get(), file_skipped))) {
      LOG_WARN("fail to apply skipping index filter", K(ret));
    } else if (file_skipped) {
      ++reader_metrics_.skipped_file_count_;
    } else {
      ++reader_metrics_.selected_file_count_;
    }
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

int ObOrcTableRowIterator::pre_buffer(const bool row_index)
{
  int ret = OB_SUCCESS;
  if (OB_STORAGE_FILE == data_access_driver_.get_storage_type()) {
    // local file io cost is lower, so disable pre buffer.
  } else if (OB_FAIL(init_column_range_slices())) {
    LOG_WARN("fail to init column range slices", K(ret));
  } else if (OB_UNLIKELY(!reader_ctx_.currentStripeInfo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stripe info is null", K(ret));
  } else if (OB_UNLIKELY(selected_columns_.count() == 0)) {
    // no selected columns, skip pre buffer
  } else {
    ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
    const int64_t n_streams = reader_ctx_.currentStripeInfo->getNumberOfStreams();
    for (int64_t i = 0; OB_SUCC(ret) && i < n_streams; ++i) {
      std::unique_ptr<orc::StreamInformation> stream = reader_ctx_.currentStripeInfo->getStreamInformation(i);
      uint64_t orc_col_id = 0;
      if (!stream) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stream in stripe", K(ret), K(n_streams), K(i));
      } else if (FALSE_IT(orc_col_id = stream->getColumnId())) {
      } else if (OB_UNLIKELY(orc_col_id >= selected_columns_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col id is out of range", K(ret), K(orc_col_id), K(selected_columns_.count()));
      } else if (selected_columns_.at(orc_col_id)) {
        const orc::StreamKind stream_kind = stream->getKind();
        bool pre_buffer_stream = false;
        if (row_index) {
          pre_buffer_stream = (orc::StreamKind::StreamKind_ROW_INDEX == stream_kind);
        } else {
          pre_buffer_stream = (orc::StreamKind::StreamKind_DATA == stream_kind ||
                               orc::StreamKind::StreamKind_DICTIONARY_DATA == stream_kind ||
                               orc::StreamKind::StreamKind_DICTIONARY_COUNT == stream_kind ||
                               orc::StreamKind::StreamKind_PRESENT == stream_kind ||
                               orc::StreamKind::StreamKind_LENGTH == stream_kind ||
                               orc::StreamKind::StreamKind_SECONDARY == stream_kind);
        }
        if (pre_buffer_stream) {
          const int64_t offset = static_cast<int64_t>(stream->getOffset());
          const int64_t length = static_cast<int64_t>(stream->getLength());
          ObFilePreBuffer::ReadRange read_range(offset, length);
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
  const int64_t selected_col_count = selected_columns_.count();
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
  if (OB_SUCC(ret)) {
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
    const int32_t ext_tbl_col_id,
    const ObColumnMeta &column_meta,
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
    } else {
      if (orc_row_iter_->is_iceberg_lake_table() && orc_row_iter_->file_contains_attribute_key_) {
        if (OB_FAIL(orc_row_iter_->iceberg_id_to_type_.get_refactored(orc_col_id, orc_type))) {
          LOG_WARN("fail to get orc type", K(ret), K(orc_col_id));
        }
      } else {
        if (OB_FAIL(orc_row_iter_->id_to_type_.get_refactored(orc_col_id, orc_type))) {
          LOG_WARN("fail to get orc type", K(ret), K(orc_col_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(orc_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orc type is null", K(ret), K(orc_col_id));
      } else if (OB_FAIL(orc_row_iter_->convert_orc_statistics(col_stat, orc_type,
                                                              column_meta,
                                                              param))) {
        LOG_WARN("fail to convert orc statistics", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::OrcFilterColumnLoader::load(const common::ObIArray<uint64_t> &col_ids)
{
  return orc_row_iter_->load_filter_column(col_ids);
}

int ObOrcTableRowIterator::load_filter_column(const common::ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
    const uint64_t col_id = col_ids.at(i);
    FilterExprRel* filter_expr_rel;
    if (OB_ISNULL(filter_expr_rel = filter_expr_rels_.get(col_id))) {
      LOG_WARN("fail to get filter expr ctx", K(ret), K(col_id));
    } else if (OB_UNLIKELY(!filter_expr_rel->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter expr ctx is invalid", K(ret), K(col_id));
    } else if (filter_expr_rel->projected_) {
      // skip projected column
    } else {
      // load data for non-projected column
      const int64_t file_col_expr_idx = filter_expr_rel->file_col_expr_index_;
      OrcRowReader &eager_reader = sector_reader_->get_eager_reader();
      const int64_t row_count = eager_reader.orc_batch_ ?
      eager_reader.orc_batch_->numElements : eval_ctx.max_batch_size_;
      bool column_need_conv = true;
      if (filter_expr_rel->is_file_meta_column_) {
        if (file_col_expr_idx == -1) {
          // the file meta column is the row id column
          OZ (calc_exprs_for_rowid(row_count, state_, false /* update_state */));
          column_need_conv = false;
        } else {
          ObExpr *meta_expr = file_meta_column_exprs_.at(file_col_expr_idx);
          if (OB_FAIL(fill_file_meta_column(eval_ctx, meta_expr, row_count))) {
            LOG_WARN("fail to fill file meta column", K(ret));
          }
        }
      } else {
        DataLoader &data_loader = eager_reader.data_loaders_.at(file_col_expr_idx);
        const uint64_t column_expr_idx = mapping_column_ids_.at(file_col_expr_idx).second;
        if (OB_FAIL(data_loader.load_data_for_col(eval_ctx))) {
          LOG_WARN("fail to load data for col", K(ret), K(file_col_expr_idx));
        } else {
          column_need_conv = (column_expr_idx == OB_INVALID_ID ||
                              column_need_conv_.at(column_expr_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (column_need_conv && OB_FAIL(project_column(eval_ctx,
                                                            filter_expr_rel->column_conv_expr_,
                                                            filter_expr_rel->column_expr_,
                                                            row_count))) {
        LOG_WARN("fail to project column", K(ret));
      } else {
        filter_expr_rel->projected_ = true;
      }
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

static OB_INLINE int128_t orc_int64_to_ob_int128(const int64_t orc_int)
{
  int128_t ob_int128 = 0;
  ob_int128.items_[1] = 0;
  ob_int128.items_[0] = orc_int;
  return ob_int128;
}

static OB_INLINE int128_t orc_int128_to_ob_int128(const orc::Int128 &orc_int)
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
    const ObColumnMeta &col_meta,
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
      if (OB_FAIL(ObTextStringHelper::string_to_templob_result(col_meta.type_, col_meta.has_lob_header_, allocator, min_ob_str,
          param.min_datum_))) {
        LOG_WARN("fail to string to templob result", K(ret));
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(col_meta.type_, col_meta.has_lob_header_, allocator,
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
                                                  const ObColumnMeta &col_meta,
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
        ret = convert_string_type_statistics(orc_stat, col_meta, temp_allocator_, param, has_null);
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

int ObOrcTableRowIterator::DataLoader::init(ObExpr *file_col_expr,
                                            const orc::ColumnVectorBatch *batch,
                                            const orc::Type *col_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_col_expr) || OB_ISNULL(batch) || OB_ISNULL(col_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_col_expr), K(batch), K(col_type));
  } else {
    reset();
    file_col_expr_ = file_col_expr;
    batch_ = batch;
    col_type_ = col_type;
    col_def_ = nullptr;
    load_func_ = select_load_function(file_col_expr->datum_meta_, *col_type);
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::init(ObExpr *file_col_expr,
                                            const ObColumnDefaultValue *col_def)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_col_expr) || OB_ISNULL(col_def)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_col_expr), K(col_def));
  } else {
    reset();
    file_col_expr_ = file_col_expr;
    col_def_ = col_def;
    load_func_ = &DataLoader::load_default;
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

int64_t ObOrcTableRowIterator::DataLoader::calc_tz_adjust_us(ObEvalCtx &eval_ctx)
{
  return calc_tz_adjust_us(col_type_, file_col_expr_->datum_meta_.type_,
                           eval_ctx.exec_ctx_.get_my_session());
}

int ObOrcTableRowIterator::DataLoader::load_data_for_col(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_col_expr_->init_vector_for_write(eval_ctx,
      file_col_expr_->get_default_res_format(), eval_ctx.max_batch_size_))) {
    LOG_WARN("fail to init vector for write", K(ret));
  } else if (OB_FAIL((this->*load_func_)(eval_ctx))) {
    LOG_WARN("fail to load data for col", K(ret));
  } else {
    file_col_expr_->set_evaluated_projected(eval_ctx);
  }
  return ret;
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
      // Do not change this restriction unless you also change the load function load_dec64_vec
      if (col_desc->getPrecision() > ((datum_type.precision_ == -1) ? 38 : datum_type.precision_)
        || col_desc->getScale() != datum_type.scale_) {
        func = NULL;
      } else if (col_desc->getPrecision() == 0 || col_desc->getPrecision() > 18) {
        func = &DataLoader::load_dec128_vec;
      } else if (col_desc->getPrecision() <= 18) {
        if (datum_type.precision_ > 18) {
          func = &DataLoader::load_dec64_to_dec128_vec;
        } else {
          func = &DataLoader::load_dec64_vec;
        }
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
  } else if (orc::TypeKind::FLOAT == type_kind) {
    if (ObFloatType == datum_type.type_) {
      func = &DataLoader::load_float;
    } else if (ObDoubleType == datum_type.type_) {
      func = &DataLoader::load_double;
    }
  } else if (orc::TypeKind::DOUBLE == type_kind && ObDoubleType == datum_type.type_) {
    func = &DataLoader::load_double;
  }
  return func;
}

int ObOrcTableRowIterator::get_data_column_batch(
    const orc::Type *type, const orc::StructVectorBatch *root_batch,
    const int col_id, orc::ColumnVectorBatch *&batch)
{
  int ret = OB_SUCCESS;
  CK (type != NULL);
  CK (root_batch != NULL);
  bool found = false;
  const orc::Type *cur_type = type;
  const orc::StructVectorBatch *cur_batch = root_batch;
  batch = nullptr;

  if (is_iceberg_lake_table() && file_contains_attribute_key_) {
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < cur_type->getSubtypeCount(); i++) {
      const std::string &id_val = cur_type->getSubtype(i)->getAttributeValue(ICEBERG_ID_KEY);
      if (id_val == std::to_string(col_id)) {
        batch = cur_batch->fields[i];
        found = true;
      }
    }
  } else {
    while (OB_SUCC(ret) && !found) {
      for (int64_t i = 0; OB_SUCC(ret) && !found && i < cur_type->getSubtypeCount(); i++) {
        if (cur_type->getSubtype(i)->getColumnId() == col_id) {
          batch = cur_batch->fields[i];
          found = true;
        } else if (cur_type->getSubtype(i)->getColumnId() < col_id && col_id < cur_type->getSubtype(i)->getMaximumColumnId()) {
          cur_batch = dynamic_cast<const orc::StructVectorBatch *>(cur_batch->fields[i]);
          cur_type = cur_type->getSubtype(i);
          CK (OB_NOT_NULL(cur_batch));
          break;
        } else {
          //do nothing
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!found || batch == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get data colum batch failed", K(ret), K(col_id), K(found), KP(batch));
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

int ObOrcTableRowIterator::compute_column_id_by_table_type(int64_t index, int64_t &orc_col_id)
{
  int ret = OB_SUCCESS;
  if (!is_iceberg_lake_table() || !file_contains_attribute_key_) {
    OZ (compute_column_id_by_index_type(index, orc_col_id));
  } else {
    orc_col_id = file_column_exprs_.at(index)->extra_;
  }
  return ret;
}

int ObOrcTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_dependent_exprs_);
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);
  if (sector_reader_ != nullptr && !sector_reader_->is_finished()) {
    // sector reader is still not finished, project rows from sector bitmap,
    // no need to move to next range.
  } else if (OB_UNLIKELY(!state_.has_rows_in_row_range())) {
    if (OB_FAIL(next_row_range())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next row group", K(ret));
      }
    } else if (OB_UNLIKELY(!state_.has_rows_in_row_range()) && !is_count_aggr_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iterator state", K(ret), K_(state));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_count_aggr_) {
    read_count = std::min(capacity, state_.remain_rows_in_range());
    state_.cur_range_read_row_count_ += read_count;
  } else if (OB_FAIL(next_batch(read_count, capacity))) {
    LOG_WARN("fail to next batch", K(ret), K(capacity), K(read_count));
  }
  if (OB_SUCC(ret) && read_count > 0) {
    if (!is_count_aggr_ && !is_count_aggr_with_filter_) {
      // load vec data from orc file to file column expr
      for (int64_t i = 0; OB_SUCC(ret) && i < project_reader_.data_loaders_.count(); ++i) {
        DataLoader &data_loader = project_reader_.data_loaders_.at(i);
        if (data_loader.has_load_func() && OB_FAIL(data_loader.load_data_for_col(eval_ctx))) {
          LOG_WARN("fail to load data for col", K(ret));
        }
      }
    }
    //fill expr results from metadata
    for (int64_t i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      OZ (fill_file_meta_column(eval_ctx, file_meta_column_exprs_.at(i), read_count));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      //column_conv_exprs is 1-1 mapped to column_exprs
      //calc gen column exprs
      if (column_sel_mask_.at(i) && column_need_conv_.at(i)) {
        OZ (project_column(eval_ctx, column_conv_exprs.at(i), column_exprs_.at(i), read_count));
      }
    }
    OZ (calc_exprs_for_rowid(read_count, state_));
  }
  if (OB_SUCC(ret)) {
    reader_metrics_.read_rows_count_ += read_count;
    count = read_count;
    LOG_TRACE("orc read row", K_(state), K(read_count));
  }
  return ret;
}

int ObOrcTableRowIterator::fill_file_meta_column(ObEvalCtx &eval_ctx, ObExpr *meta_expr,
                                                 const int64_t read_count)
{
  int ret = OB_SUCCESS;
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
  return ret;
}

int ObOrcTableRowIterator::project_column(ObEvalCtx &eval_ctx, const ObExpr *from, const ObExpr *to,
                                          const int64_t read_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(from), K(to));
  } else {
    if (!from->get_eval_info(eval_ctx).evaluated_) {
      CK (OB_NOT_NULL(bit_vector_cache_));
      OZ (from->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
      OX (from->set_evaluated_projected(eval_ctx));
    }
    //assign gen column exprs value to column exprs(output exprs)
    if (OB_SUCC(ret)) {
      VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
      VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
      if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
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
      to->set_evaluated_projected(eval_ctx);
    }
  }
  return ret;
}

int ObOrcTableRowIterator::next_batch(int64_t &read_count, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  try {
    if (sector_reader_ != nullptr) {
      if (OB_FAIL(sector_reader_->next(read_count, capacity))) {
        LOG_WARN("fail to next batch", K(ret));
      }
    } else {
      int64_t read_rows = MIN(capacity, state_.remain_rows_in_range());
      if (OB_UNLIKELY(!project_reader_.next_batch(read_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to read next batch", K(ret));
      } else {
        read_count = read_rows;
        state_.cur_range_read_row_count_ += read_rows;
      }
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
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int64_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(int64_vec));
    CK (VEC_FIXED == int64_vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::LongVectorBatch *long_batch = dynamic_cast<const orc::LongVectorBatch *>(batch_);
      CK (OB_NOT_NULL(long_batch));
      CK (OB_NOT_NULL(long_batch->data.data()));
      CK (OB_NOT_NULL(int64_vec->get_data()));
      if (OB_FAIL(ret)) {
      } else if (!long_batch->hasNulls) {
        MEMCPY(pointer_cast<int64_t*>(int64_vec->get_data()), long_batch->data.data(),
          sizeof(int64_t) * long_batch->numElements);
      } else {
        CK (OB_NOT_NULL(long_batch->notNull.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
          const char not_null = long_batch->notNull[i];
          if (not_null == 1) {
            int64_vec->set_int(i, long_batch->data[i]);
          } else {
            int64_vec->set_null(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_year_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::LongVectorBatch *long_batch = dynamic_cast<const orc::LongVectorBatch *>(batch_);
      CK (OB_NOT_NULL(long_batch));
      CK (OB_NOT_NULL(long_batch->data.data()));
      CK (OB_NOT_NULL(int32_vec->get_data()));
      if (OB_FAIL(ret)) {
      } else if (!long_batch->hasNulls) {
        for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
          int32_vec->set_year(i, long_batch->data[i]);
        }
      } else {
        CK (OB_NOT_NULL(long_batch->notNull.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
          const char not_null = long_batch->notNull.data()[i];
          if (not_null == 1) {
            int32_vec->set_year(i, long_batch->data[i]);
          } else {
            int32_vec->set_null(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int32_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::LongVectorBatch *long_batch = dynamic_cast<const orc::LongVectorBatch *>(batch_);
      CK (OB_NOT_NULL(long_batch));
      CK (OB_NOT_NULL(long_batch->data.data()));
      CK (OB_NOT_NULL(int32_vec->get_data()));
      if (OB_FAIL(ret)) {
      } else if (!long_batch->hasNulls) {
        for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
          int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
        }
      } else {
        CK (OB_NOT_NULL(long_batch->notNull.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
          const char not_null = long_batch->notNull.data()[i];
          if (not_null == 1) {
            int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
          } else {
            int32_vec->set_null(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_default(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = file_col_expr_->get_vector(eval_ctx);
  CK (OB_NOT_NULL(col_def_));
  OZ (ObExternalTableRowIterator::set_default_batch(file_col_expr_->datum_meta_, *col_def_, vec));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_string_col(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(text_vec));
    CK (VEC_DISCRETE == text_vec->get_format());
    if (OB_SUCC(ret)) {
      const bool is_oracle_mode = lib::is_oracle_mode();
      const bool is_byte_length = is_oracle_byte_length(
        is_oracle_mode, file_col_expr_->datum_meta_.length_semantics_);
      const orc::StringVectorBatch *string_batch =
        dynamic_cast<const orc::StringVectorBatch *>(batch_);
      CK (OB_NOT_NULL(string_batch));
      CK (OB_NOT_NULL(string_batch->data.data()));
      CK (OB_NOT_NULL(string_batch->length.data()));
      if (OB_FAIL(ret)) {
      } else if (string_batch->hasNulls) {
        CK (OB_NOT_NULL(string_batch->notNull.data()));
        text_vec->set_ptrs(const_cast<char **>(string_batch->data.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < string_batch->numElements; i++) {
          const char not_null = string_batch->notNull.data()[i];
          if (not_null == 1) {
            const int64_t length = string_batch->length[i];
            const char *data = string_batch->data[i];
            if (length == 0 && is_oracle_mode) {
              text_vec->set_null(i);
            } else {
              if (OB_UNLIKELY(length > file_col_expr_->max_length_ &&
                  (is_byte_length ||
                     ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, data, length) > file_col_expr_->max_length_))) {
                ret = OB_ERR_DATA_TOO_LONG;
                LOG_WARN("data too long", K(ret), K(length), K(file_col_expr_->max_length_));
              } else {
                text_vec->set_length(i, length);
              }
            }
          } else {
            text_vec->set_null(i);
          }
        }
      } else {
        text_vec->set_ptrs(const_cast<char **>(string_batch->data.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < string_batch->numElements; i++) {
          const int64_t length = string_batch->length[i];
          const char *data = string_batch->data[i];
          if (length == 0 && is_oracle_mode) {
            text_vec->set_null(i);
          } else if (OB_UNLIKELY(length > file_col_expr_->max_length_ &&
               (is_byte_length ||
                 ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, data, length) > file_col_expr_->max_length_))) {
            ret = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("data too long", K(ret), K(length), K(file_col_expr_->max_length_));
          } else {
            text_vec->set_length(i, length);
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_timestamp_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    int64_t adjust_us = calc_tz_adjust_us(eval_ctx);
    LOG_DEBUG("adjust value", K(adjust_us));
    CK (OB_NOT_NULL(dec_vec));
    if (OB_SUCC(ret)) {
      const orc::TimestampVectorBatch *timestamp_batch =
       dynamic_cast<const orc::TimestampVectorBatch *>(batch_);
      CK (OB_NOT_NULL(timestamp_batch));
      CK (OB_NOT_NULL(timestamp_batch->data.data()));
      CK (OB_NOT_NULL(timestamp_batch->nanoseconds.data()));
      if (OB_SUCC(ret) && timestamp_batch->hasNulls) {
        CK (OB_NOT_NULL(timestamp_batch->notNull.data()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < timestamp_batch->numElements; i++) {
        const char not_null = timestamp_batch->hasNulls ? timestamp_batch->notNull.data()[i] : 1;
        if (not_null == 1) {
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
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_date_to_time_or_stamp(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    int64_t adjust_us = calc_tz_adjust_us(eval_ctx);
    LOG_DEBUG("show adjust value in date to ts", K(adjust_us));
    if (OB_SUCC(ret)) {
      const orc::LongVectorBatch *date_batch = dynamic_cast<const orc::LongVectorBatch *>(batch_);
      CK (OB_NOT_NULL(date_batch));
      CK (OB_NOT_NULL(date_batch->data.data()));
      if (OB_SUCC(ret) && date_batch->hasNulls) {
        CK (OB_NOT_NULL(date_batch->notNull.data()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < date_batch->numElements; i++) {
        const char not_null = date_batch->hasNulls ? date_batch->notNull.data()[i] : 1;
        if (not_null == 1) {
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
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec64_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    // only decimal int type is supported
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(vec));
    CK (VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::Decimal64VectorBatch *dec64_batch = dynamic_cast<const orc::Decimal64VectorBatch *>(batch_);
      CK (OB_NOT_NULL(dec64_batch));
      CK (OB_NOT_NULL(dec64_batch->values.data()));
      // ensure the precision and scale of the dec64_batch is the same as the file_col_expr_
      if (OB_FAIL(ret)) {
      } else if (!dec64_batch->hasNulls) {
        char *data = vec->get_data();
        if (vec->get_length() == 4) {
          int32_t *int32_data = reinterpret_cast<int32_t *>(data);
          for (int64_t i = 0; OB_SUCC(ret) && i < dec64_batch->numElements; i++) {
            int32_t dec_int = static_cast<int32_t>(dec64_batch->values[i]);
            int32_data[i] = dec_int;
          }
        } else if (vec->get_length() == 8) {
          MEMCPY(pointer_cast<int64_t*>(vec->get_data()), dec64_batch->values.data(),
            sizeof(int64_t) * dec64_batch->numElements);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid decimal int length", K(ret), K(vec->get_length()));
        }
      } else {
        CK (OB_NOT_NULL(dec64_batch->notNull.data()));
        if (vec->get_length() == 4) {
          char *data = vec->get_data();
          int32_t *int32_data = reinterpret_cast<int32_t *>(data);
          for (int64_t i = 0; OB_SUCC(ret) && i < dec64_batch->numElements; i++) {
            const char not_null = dec64_batch->notNull.data()[i];
            if (not_null == 1) {
              int32_t dec_int = static_cast<int32_t>(dec64_batch->values[i]);
              int32_data[i] = dec_int;
            } else {
              vec->set_null(i);
            }
          }
        } else if (vec->get_length() == 8) {
          char *data = vec->get_data();
          int64_t *int64_data = reinterpret_cast<int64_t *>(data);
          for (int64_t i = 0; OB_SUCC(ret) && i < dec64_batch->numElements; i++) {
            const char not_null = dec64_batch->notNull.data()[i];
            if (not_null == 1) {
              int64_data[i] = dec64_batch->values[i];
            } else {
              vec->set_null(i);
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid decimal int length", K(ret), K(vec->get_length()));
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int64_to_number_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObBitmapNullVectorBase *vec = static_cast<ObBitmapNullVectorBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(vec));
    CK (VEC_DISCRETE == vec->get_format() || VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::LongVectorBatch *long_batch = dynamic_cast<const orc::LongVectorBatch *>(batch_);
      CK (OB_NOT_NULL(long_batch));
      CK (OB_NOT_NULL(long_batch->data.data()));
      if (OB_SUCC(ret) && long_batch->hasNulls) {
        CK (OB_NOT_NULL(long_batch->notNull.data()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < long_batch->numElements; i++) {
        const char not_null = long_batch->hasNulls ? long_batch->notNull.data()[i] : 1;
        const int64_t int_value = long_batch->data[i];
        if (not_null == 1) {
          if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
            ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
            ObDecimalInt *decint = NULL;
            int32_t int_bytes = 0;
            if (OB_FAIL(wide::from_integer(int_value, tmp_alloc_g.get_allocator(), decint,
                                          int_bytes, file_col_expr_->datum_meta_.precision_))) {
              LOG_WARN("fail to from integer", K(ret));
            } else {
              fixed_vec->set_decimal_int(i, decint, int_bytes);
            }
          } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
            ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vec);
            number::ObNumber res_nmb;
            if (OB_FAIL(res_nmb.from(int_value, tmp_alloc_g.get_allocator()))) {
              LOG_WARN("fail to from number", K(ret));
            } else {
              discrete_vec->set_number(i, res_nmb);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
          }
        } else {
          vec->set_null(i);
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec128_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObBitmapNullVectorBase *vec = static_cast<ObBitmapNullVectorBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(vec));
    CK (VEC_DISCRETE == vec->get_format() || VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::Decimal128VectorBatch *dec128_batch = dynamic_cast<const orc::Decimal128VectorBatch *>(batch_);
      CK (OB_NOT_NULL(dec128_batch));
      CK (OB_NOT_NULL(dec128_batch->values.data()));
      if (OB_SUCC(ret) && dec128_batch->hasNulls) {
        CK (OB_NOT_NULL(dec128_batch->notNull.data()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < dec128_batch->numElements; i++) {
        const char not_null = dec128_batch->hasNulls ? dec128_batch->notNull.data()[i] : 1;
        if (not_null == 1) {
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
            ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
            fixed_vec->set_decimal_int(i, decint, int_bytes);
          } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
            ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vec);
            number::ObNumber res_nmb;
            if (OB_FAIL(wide::to_number(decint, int_bytes, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
              LOG_WARN("fail to from", K(ret));
            } else {
              discrete_vec->set_number(i, res_nmb);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
          }
        } else {
          vec->set_null(i);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec64_to_dec128_vec(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObBitmapNullVectorBase *vec = static_cast<ObBitmapNullVectorBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(vec));
    CK (VEC_DISCRETE == vec->get_format() || VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::Decimal64VectorBatch *dec64_batch = dynamic_cast<const orc::Decimal64VectorBatch *>(batch_);
      CK (OB_NOT_NULL(dec64_batch));
      CK (OB_NOT_NULL(dec64_batch->values.data()));
      if (OB_SUCC(ret) && dec64_batch->hasNulls) {
        CK (OB_NOT_NULL(dec64_batch->notNull.data()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < dec64_batch->numElements; i++) {
        const char not_null = dec64_batch->hasNulls ? dec64_batch->notNull.data()[i] : 1;
        if (not_null == 1) {
          ObDecimalInt *decint = NULL;
          int32_t int_bytes = sizeof(int128_t);
          int128_t val = orc_int64_to_ob_int128(dec64_batch->values[i]);
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
            ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
            fixed_vec->set_decimal_int(i, decint, int_bytes);
          } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
            ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vec);
            number::ObNumber res_nmb;
            if (OB_FAIL(wide::to_number(decint, int_bytes, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
              LOG_WARN("fail to from", K(ret));
            } else {
              discrete_vec->set_number(i, res_nmb);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
          }
        } else {
          vec->set_null(i);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_float(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *float_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(float_vec));
    CK (VEC_FIXED == float_vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::DoubleVectorBatch *double_batch = dynamic_cast<const orc::DoubleVectorBatch *>(batch_);
      CK (OB_NOT_NULL(double_batch));
      CK (OB_NOT_NULL(double_batch->data.data()));
      CK (OB_NOT_NULL(float_vec->get_data()));
      if (OB_FAIL(ret)) {
      } else if (!double_batch->hasNulls) {
        for (int64_t i = 0; OB_SUCC(ret) && i < double_batch->numElements; i++) {
          float_vec->set_float(i, (float)double_batch->data[i]);
        }
      } else {
        CK (OB_NOT_NULL(double_batch->notNull.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < double_batch->numElements; i++) {
          const char not_null = double_batch->notNull.data()[i];
          if (not_null == 1) {
            float_vec->set_float(i, (float)double_batch->data[i]);
          } else {
            float_vec->set_null(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_double(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx));
    CK (OB_NOT_NULL(double_vec));
    CK (VEC_FIXED == double_vec->get_format());
    if (OB_SUCC(ret)) {
      const orc::DoubleVectorBatch *double_batch = dynamic_cast<const orc::DoubleVectorBatch *>(batch_);
      CK (OB_NOT_NULL(double_batch));
      CK (OB_NOT_NULL(double_batch->data.data()));
      CK (OB_NOT_NULL(double_vec->get_data()));
      if (OB_FAIL(ret)) {
      } else if (!double_batch->hasNulls) {
        MEMCPY(pointer_cast<double*>(double_vec->get_data()), double_batch->data.data(),
          sizeof(double) * double_batch->numElements);
      } else {
        CK (OB_NOT_NULL(double_batch->notNull.data()));
        for (int64_t i = 0; OB_SUCC(ret) && i < double_batch->numElements; i++) {
          const char not_null = double_batch->notNull.data()[i];
          if (not_null == 1) {
            double_vec->set_double(i, double_batch->data[i]);
          } else {
            double_vec->set_null(i);
          }
        }
      }
    }
  }
  return ret;
}

template<typename T>
void ObOrcTableRowIterator::OrcRowReader::init(int64_t capacity,
                                               const std::list<T>& include_columns,
                                               orc::Reader *reader)
{
  orc::RowReaderOptions rowReaderOptions;
  rowReaderOptions.include(include_columns);
  row_reader_ = reader->createRowReader(rowReaderOptions);
  // create orc read batch for reuse.
  orc_batch_ = row_reader_->createRowBatch(capacity);
  row_id_ = 0;
}

void ObOrcTableRowIterator::OrcRowReader::init_for_hive_table(int64_t capacity,
                                                        const std::list<uint64_t>& include_columns,
                                                        orc::Reader *reader)
{
  orc::RowReaderOptions rowReaderOptions;
  row_reader_ = reader->createRowReader(rowReaderOptions);
  int64_t col_cnt =  row_reader_->getSelectedColumns().size();
  std::list<uint64_t> filtered_column_ids;
  std::list<uint64_t>::const_iterator it;
  for (it = include_columns.begin(); it != include_columns.end(); ++it) {
    uint64_t col_id = *it;
    if (col_id < col_cnt - 1) {
      filtered_column_ids.push_back(col_id);
    }
  }
  rowReaderOptions.include(filtered_column_ids);
  row_reader_ = reader->createRowReader(rowReaderOptions);
  orc_batch_ = row_reader_->createRowBatch(capacity);
  row_id_ = 0;
}

int ObOrcTableRowIterator::SectorReader::init(ObOrcTableRowIterator *orc_row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orc_row_iter) || OB_ISNULL(orc_row_iter->scan_param_) ||
      OB_ISNULL(orc_row_iter->scan_param_->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    orc_row_iter_ = orc_row_iter;
    max_batch_size_ = orc_row_iter->scan_param_->op_->get_eval_ctx().max_batch_size_;
    const int64_t array_size = max_batch_size_ * sizeof(uint16_t) * 2;
    void *array_mem = nullptr;
    if (OB_FAIL(bitmap_.init(MAX_SECTOR_ROW_COUNT))) {
      LOG_WARN("fail to init bitmap", K(ret));
    } else if (OB_ISNULL(array_mem = allocator_.alloc(array_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      skip_ = reinterpret_cast<uint16_t *>(array_mem);
      next_ = skip_ + max_batch_size_;
      sector_begin_ = 0;
      sector_end_ = 0;
      sector_size_ = 0;
    }
  }
  if (OB_SUCC(ret) && orc_row_iter->has_eager_column_) {
    int64_t file_column_count = orc_row_iter->file_column_exprs_.count();
    if (OB_FAIL(eager_reader_.data_loaders_.allocate_array(allocator_, file_column_count))) {
      LOG_WARN("fail to allocate memory", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::SectorReader::next(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  OrcRowReader &project_reader = orc_row_iter_->project_reader_;
  bool has_active_row = !is_finished();
  if (OB_UNLIKELY(!has_active_row)) {
    // build next sector bitmap
    if (orc_row_iter_->is_count_aggr_with_filter_) {
      if (OB_FAIL(popcnt_rows_by_filter(has_active_row))) {
        LOG_WARN("fail to popcnt rows by filter", K(ret));
      }
    } else if (OB_FAIL(build_sector_bitmap_by_filter(has_active_row))) {
      LOG_WARN("fail to build sector bitmap by filter", K(ret));
    } else if (has_active_row && (project_reader.row_id_ < eager_reader_.row_id_)) {
      // seek to row id
      if (project_reader.row_reader_) {
        project_reader.row_reader_->seekToRow(eager_reader_.row_id_);
      }
      project_reader.row_id_ = eager_reader_.row_id_;
    }
  }
  if (OB_SUCC(ret) && has_active_row) {
    if (orc_row_iter_->is_count_aggr_with_filter_) {
      count = MIN(capacity, sector_end_ - sector_begin_);
      sector_begin_ += count;
    } else {
      orc::ProjectArgument arg;
      if (OB_FAIL(next_sector_range(capacity, arg))) {
        LOG_WARN("fail to next", K(ret));
      } else if (project_reader.row_reader_ &&
          !project_reader.row_reader_->next(*project_reader.orc_batch_, arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to read next batch", K(ret));
      } else {
        count = arg.numValues;
        project_reader.row_id_ += arg.numRead;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(is_finished())) {
      eager_reader_.row_id_ += sector_size_;
      reset_sector_state();
    }
  }
  return ret;
}

int ObOrcTableRowIterator::SectorReader::build_sector_bitmap_by_filter(bool &has_active_row)
{
  int ret = OB_SUCCESS;
  ObPushdownOperator *op = orc_row_iter_->scan_param_->op_;
  ObEvalCtx &eval_ctx = op->get_eval_ctx();
  ObOrcIteratorState &state = orc_row_iter_->state_;
  while (OB_SUCC(ret) && !has_active_row && state.has_rows_in_row_range()) {
    bitmap_.reuse();
    const int64_t remain_rows = state.remain_rows_in_range();
    sector_size_ = MIN(remain_rows, MAX_SECTOR_ROW_COUNT);
    int64_t eval_count = 0;
    if (orc_row_iter_->has_eager_column_ || orc_row_iter_->is_file_meta_filter_) {
      while (OB_SUCC(ret) && eval_count < sector_size_) {
        orc_row_iter_->clear_filter_expr_evaluated_flag();
        int64_t batch_size = MIN(max_batch_size_, sector_size_ - eval_count);
        if (OB_UNLIKELY(!eager_reader_.next_batch(batch_size))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to read filter batch", K(ret), K(batch_size));
        } else {
          ObPushdownFilterExecutor *filter = orc_row_iter_->scan_param_->pd_storage_filters_;
          OrcFilterColumnLoader filter_column_loader(orc_row_iter_);
          if (OB_NOT_NULL(filter)) {
            if (OB_FAIL(filter->execute(nullptr, &filter_column_loader, 0, batch_size))) {
              LOG_WARN("fail to execute filter", K(ret));
            } else if (OB_FAIL(bitmap_.append_bitmap(*filter->get_result(), eval_count, false))) {
              LOG_WARN("fail to append bitmap", K(ret));
            } else {
              eval_count += batch_size;
            }
          }
        }
      }
    } else {
      bitmap_.reuse(true);
      eval_count = sector_size_;
    }
    OZ (merge_bitmap_with_delete_bitmap(&bitmap_, eval_count, eager_reader_.row_id_));
    state.cur_range_read_row_count_ += eval_count;
    if (OB_SUCC(ret)) {
      has_active_row = !bitmap_.is_all_false(0, eval_count - 1);
      if (has_active_row) {
        // trim bitmap to avoid unnecessary skip in the last batch
        int64_t last_valid_idx = -1;
        if (OB_FAIL(bitmap_.next_valid_idx(0, eval_count, true, last_valid_idx))) {
          LOG_WARN("fail to get last valid idx", K(ret));
        } else if (OB_UNLIKELY(last_valid_idx == -1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last valid idx is -1", K(ret), K(last_valid_idx));
        } else {
          sector_end_ = last_valid_idx + 1;
        }
      } else { // update current project base row id
        eager_reader_.row_id_ += eval_count;
        reset_sector_state();
      }
    }
  }
  op->clear_evaluated_flag();
  return ret;
}

int ObOrcTableRowIterator::SectorReader::popcnt_rows_by_filter(bool &has_active_row)
{
  int ret = OB_SUCCESS;
  ObPushdownOperator *op = orc_row_iter_->scan_param_->op_;
  ObOrcIteratorState &state = orc_row_iter_->state_;
  while (OB_SUCC(ret) && state.has_rows_in_row_range()) {
    orc_row_iter_->clear_filter_expr_evaluated_flag();
    int64_t batch_size = MIN(max_batch_size_, state.remain_rows_in_range());
    if (OB_UNLIKELY(!eager_reader_.next_batch(batch_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to read filter batch", K(ret));
    } else {
      ObPushdownFilterExecutor *filter = orc_row_iter_->scan_param_->pd_storage_filters_;
      OrcFilterColumnLoader filter_column_loader(orc_row_iter_);
      if (OB_NOT_NULL(filter)) {
        if (OB_FAIL(filter->execute(nullptr, &filter_column_loader, 0, batch_size))) {
          LOG_WARN("fail to execute filter", K(ret));
        } else if (OB_FAIL(merge_bitmap_with_delete_bitmap(filter->get_result(), batch_size,
                                                          eager_reader_.row_id_))) {
          LOG_WARN("fail to merge bitmap with delete bitmap", K(ret));
        } else {
          sector_size_ += filter->get_result()->popcnt();
          state.cur_range_read_row_count_ += batch_size;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    sector_end_ = sector_size_;
    has_active_row = sector_size_ > 0;
  }
  return ret;
}

int ObOrcTableRowIterator::SectorReader::next_sector_range(const int64_t capacity,
                                                           orc::ProjectArgument &arg)
{
  int ret = OB_SUCCESS;
  // opt this function.
  const uint8_t *start_row = bitmap_.get_data() + sector_begin_;
  const uint8_t *end_row = bitmap_.get_data() + sector_end_;
  const uint8_t *cur_row = start_row;
  int64_t cur_skip_count = 0;
  int64_t cur_next_count = 0;
  int64_t num_values = 0;
  int64_t array_pos = 0;
  while (cur_row < end_row && num_values < capacity) {
    // fast patch, next 8 bytes are all 0, skip it.
    if (cur_row + sizeof(uint64_t) <= end_row &&
        0 == *reinterpret_cast<const uint64_t *>(cur_row)) {
      if (cur_next_count > 0) {
        skip_[array_pos] = cur_skip_count;
        next_[array_pos] = cur_next_count;
        ++array_pos;
        cur_skip_count = 0;
        cur_next_count = 0;
      }
      cur_row += sizeof(uint64_t);
      cur_skip_count += sizeof(uint64_t);
    } else {
      const bool is_skipped = (*cur_row == 0);
      if (is_skipped) {
        if (cur_next_count > 0) {
          skip_[array_pos] = cur_skip_count;
          next_[array_pos] = cur_next_count;
          ++array_pos;
          cur_skip_count = 0;
          cur_next_count = 0;
        }
        ++cur_skip_count;
      } else {
        ++cur_next_count;
        ++num_values;
      }
      ++cur_row;
    }
  }
  if (OB_SUCC(ret)) {
    if (cur_skip_count > 0 || cur_next_count > 0) {
      skip_[array_pos] = cur_skip_count;
      next_[array_pos] = cur_next_count;
      ++array_pos;
    }
    int64_t num_read = cur_row - start_row;
    sector_begin_ += num_read;
    arg.numValues = num_values;
    arg.numRead = num_read;
    arg.skip = skip_;
    arg.next = next_;
    arg.size = array_pos;
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

int ObOrcTableRowIterator::create_file_reader(const ObString& data_file_path,
                                              ObExternalFileAccess& file_access_driver,
                                              ObFilePreBuffer& file_prebuffer,
                                              const int64_t file_size,
                                              std::unique_ptr<orc::Reader>& delete_reader)
{
  int ret = OB_SUCCESS;
  try {
    std::unique_ptr<ObOrcFileAccess> inStream(new ObOrcFileAccess(file_access_driver,
                                                      data_file_path.ptr(), file_size));
    inStream->set_timeout_timestamp(scan_param_->timeout_);
    if (options_.enable_prebuffer_ && OB_STORAGE_FILE != file_access_driver.get_storage_type()) {
      inStream->set_file_prebuffer(&file_prebuffer);
    }
    orc::ReaderOptions options;
    options.setMemoryPool(orc_alloc_);
    delete_reader = orc::createReader(std::move(inStream), options);
    if (!delete_reader) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orc create reader failed", K(ret));
      throw std::bad_exception();
    }
  } CATCH_ORC_EXCEPTIONS

  return ret;
}

int ObOrcTableRowIterator::SectorReader::merge_bitmap_with_delete_bitmap(ObBitmap *bitmap,
                                                                  const int64_t eval_count,
                                                                  const int64_t sector_start_row_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (OB_NOT_NULL(orc_row_iter_->delete_bitmap_) &&
            !orc_row_iter_->delete_bitmap_->is_empty_type()
            && orc_row_iter_->delete_bitmap_->get_range_cardinality(sector_start_row_id,
                                                            sector_start_row_id + eval_count) > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < eval_count; i++) {
      int64_t global_row_id = sector_start_row_id + i;
      if (orc_row_iter_->delete_bitmap_->is_contains(global_row_id)) {
        // 如果该行在 delete_bitmap_ 中被标记为删除，则直接在 bitmap 中设置为 false
        if (OB_FAIL(bitmap->set(i, false))) {
          LOG_WARN("fail to set bitmap to false for deleted row", K(ret));
        }
      }
    }
  }

  return ret;
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
#undef CATCH_ORC_EXCEPTIONS
}
}
