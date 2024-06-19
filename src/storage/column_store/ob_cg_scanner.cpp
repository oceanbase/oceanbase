/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_cg_scanner.h"
#include "common/sql_mode/ob_sql_mode_utils.h"

namespace oceanbase
{
namespace storage
{
ObCGScanner::~ObCGScanner()
{
  reset();
}

int ObCGScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  int64_t data_row_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGScanner has been inited", K(ret));
  } else if (OB_UNLIKELY(!wrapper.is_valid() || !wrapper.get_sstable()->is_major_or_ddl_merge_sstable() ||
                         !iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObCGScanner", K(ret), K(wrapper), K(iter_param));
  } else if (FALSE_IT(table_wrapper_ = wrapper)) {
  } else if (OB_FAIL(table_wrapper_.get_loaded_column_store_sstable(sstable_))) {
    LOG_WARN("fail to get sstable", K(ret), K(wrapper));
  } else if (OB_UNLIKELY(!sstable_->is_normal_cg_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected not normal cg sstable", K(ret), KPC_(sstable));
  } else if (OB_FAIL(prefetcher_.init(get_type(), *sstable_, iter_param, access_ctx))) {
    LOG_WARN("fail to init prefetcher, ", K(ret));
  } else if (OB_FAIL(table_wrapper_.get_merge_row_cnt(iter_param, data_row_cnt))) {
    LOG_WARN("fail to get merge row cnt", K(ret), K(iter_param), K(sstable_row_cnt_), K(table_wrapper_));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_row_cnt_ = data_row_cnt;
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
  }

  if (OB_SUCC(ret)) {
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

int ObCGScanner::switch_context(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObCGScanner is not inited");
  } else if (OB_UNLIKELY(!wrapper.is_valid() || !wrapper.get_sstable()->is_major_or_ddl_merge_sstable() ||
                         !iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(wrapper), K(iter_param));
  } else if (FALSE_IT(table_wrapper_ = wrapper)) {
  } else if (OB_FAIL(table_wrapper_.get_loaded_column_store_sstable(sstable_))) {
    LOG_WARN("fail to get sstable", K(ret), K(wrapper));
  } else if (OB_UNLIKELY(!sstable_->is_normal_cg_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected not normal cg sstable", K(ret), KPC_(sstable));
  } else {
    int64_t data_row_cnt = 0;
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(get_type(), *sstable_, iter_param, access_ctx))) {
        LOG_WARN("fail to init prefetcher, ", K(ret));
      }
    } else if (OB_FAIL(prefetcher_.switch_context(get_type(), *sstable_, iter_param, access_ctx))) {
      LOG_WARN("Fail to switch context for prefetcher", K(ret));
    } else if (OB_FAIL(table_wrapper_.get_merge_row_cnt(iter_param, data_row_cnt))) {
      LOG_WARN("fail to get merge row cnt", K(ret), K(iter_param), K(data_row_cnt), K(table_wrapper_));
    }
    if (OB_SUCC(ret)) {
      iter_param_ = &iter_param;
      access_ctx_ = &access_ctx;
      sstable_row_cnt_ = data_row_cnt;
      is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    }
  }
  return ret;
}

void ObCGScanner::reset()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_scanner_, ObIMicroBlockRowScanner);
  is_inited_ = false;
  is_reverse_scan_ = false;
  sstable_ = nullptr;
  table_wrapper_.reset();
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  query_index_range_.reset();
  prefetcher_.reset();
  is_new_range_ = false;
  current_ = OB_INVALID_CS_ROW_ID;
  sstable_row_cnt_ = OB_INVALID_CS_ROW_ID;
}

void ObCGScanner::reuse()
{
  if (nullptr != micro_scanner_) {
    micro_scanner_->reuse();
  }
  sstable_ = nullptr;
  table_wrapper_.reset();
  query_index_range_.reset();
  prefetcher_.reuse();
  is_new_range_ = false;
  current_ = OB_INVALID_CS_ROW_ID;
  sstable_row_cnt_ = OB_INVALID_CS_ROW_ID;
}

bool ObCGScanner::start_of_scan() const
{
  return (is_reverse_scan_ && current_ == query_index_range_.end_row_id_) ||
      (!is_reverse_scan_ && current_ == query_index_range_.start_row_id_);
}

bool ObCGScanner::end_of_scan() const
{
  return current_ < query_index_range_.start_row_id_
      || current_ > query_index_range_.end_row_id_
      || current_ == OB_INVALID_CS_ROW_ID;
}

int ObCGScanner::init_micro_scanner()
{
  int ret = OB_SUCCESS;
  if (nullptr != micro_scanner_) {
  } else if (OB_UNLIKELY(!sstable_->is_major_or_ddl_merge_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected not major sstable", K(ret), KPC_(sstable));
  } else if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowScanner,
                                                  access_ctx_->stmt_allocator_,
                                                  *access_ctx_->stmt_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
  } else if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("Fail to init micro scanner", K(ret));
  }
  return ret;
}

void ObCGScanner::get_data_range(const ObMicroIndexInfo &data_info, ObCSRange &range)
{
  const ObCSRange &data_range = data_info.get_row_range();
  range.start_row_id_ = MAX(query_index_range_.start_row_id_ - data_range.start_row_id_,  0);
  range.end_row_id_ = MIN(query_index_range_.end_row_id_, data_range.end_row_id_) - data_range.start_row_id_;
}

int ObCGScanner::locate(
    const ObCSRange &range,
    const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGScanner not init", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(range));
  } else if (range.start_row_id_ >= sstable_row_cnt_) {
    ret = OB_ITER_END;
  } else {
    is_new_range_ = true;
    query_index_range_.start_row_id_ = range.start_row_id_;
    query_index_range_.end_row_id_ = MIN(range.end_row_id_, sstable_row_cnt_ - 1);
    current_ = is_reverse_scan_ ? query_index_range_.end_row_id_ : query_index_range_.start_row_id_;
    const ObCGBitmap *locate_bitmap = bitmap;
    if (nullptr != bitmap) {
      if (bitmap->is_all_true()) {
        locate_bitmap = nullptr;
      } else if (OB_FAIL(bitmap->get_first_valid_idx(range, is_reverse_scan_, current_))) {
        LOG_WARN("Fail to get first valid idx", K(ret), K_(is_reverse_scan), K(range));
      } else {
        if (is_reverse_scan_) {
          query_index_range_.end_row_id_ = current_;
        } else {
          query_index_range_.start_row_id_ = current_;
        }
      }
    }

    if (OB_FAIL(ret) || end_of_scan()) {
    } else if (OB_FAIL(prefetcher_.locate(query_index_range_, locate_bitmap))) {
      LOG_WARN("Fail to locate range", K(ret), K_(query_index_range), K_(current));
    } else if (OB_FAIL(prefetcher_.prefetch())) {
      LOG_WARN("Fail to prefetch", K(ret));
    }
  }
  LOG_TRACE("[COLUMNSTORE] CGScanner locate range", K(ret), "tablet_id", iter_param_->tablet_id_, "cg_idx", iter_param_->cg_idx_,
            "type", get_type(), K(range), KP(bitmap));
  return ret;
}

int ObCGScanner::open_cur_data_block()
{
  int ret = OB_SUCCESS;
  if (prefetcher_.cur_micro_data_fetch_idx_ < 0 ||
      prefetcher_.cur_micro_data_fetch_idx_ >= prefetcher_.micro_data_prefetch_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K_(prefetcher));
  } else {
    if (nullptr == micro_scanner_) {
      if (OB_FAIL(init_micro_scanner())) {
        LOG_WARN("fail to init micro scanner", K(ret));
      }
    } else if (OB_UNLIKELY(!micro_scanner_->is_valid())) {
      if (OB_FAIL(micro_scanner_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
        LOG_WARN("Failed to switch new table context", K(ret), KPC(access_ctx_));
      }
    }
    if (OB_SUCC(ret) && is_new_range_) {
      micro_scanner_->reuse();
      is_new_range_ = false;
    }

    if (OB_SUCC(ret)) {
      ObMicroBlockData block_data;
      blocksstable::ObMicroIndexInfo &micro_info = prefetcher_.current_micro_info();
      ObMicroBlockDataHandle &micro_handle = prefetcher_.current_micro_handle();
      ObCSRange data_range;
      get_data_range(micro_info, data_range);
      if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
        LOG_WARN("Fail to get block data", K(ret), K(micro_handle));
      } else if (OB_FAIL(micro_scanner_->open_column_block(
                  micro_handle.macro_block_id_,
                  block_data,
                  data_range))) {
        LOG_WARN("Fail to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
      }
      if (OB_SUCC(ret)) {
        const ObCSRange &cs_range = micro_info.get_row_range();
        if (is_reverse_scan_) {
          current_ = MIN(cs_range.end_row_id_, query_index_range_.end_row_id_);
        } else {
          current_ = MAX(cs_range.start_row_id_, query_index_range_.start_row_id_);
        }
        access_ctx_->inc_micro_access_cnt();
        EVENT_INC(ObStatEventIds::BLOCKSCAN_BLOCK_CNT);
        ++access_ctx_->table_store_stat_.pushdown_micro_access_cnt_;
        LOG_TRACE("[COLUMNSTORE] open data block", "row_range", cs_range);
        LOG_DEBUG("Success to open micro block", K(ret), K(prefetcher_.cur_micro_data_fetch_idx_),
                  K(micro_info), K(micro_handle), KPC(this), K(common::lbt()));
      }
    }
  }
  return ret;
}

int ObCGScanner::apply_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    const int64_t row_count,
    const ObCGBitmap *parent_bitmap,
    ObCGBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGScanner not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= row_count || row_count != result_bitmap.size() || nullptr == filter_info.filter_ ||
                         (nullptr != prefetcher_.sstable_index_filter_ &&
                          filter_info.filter_ != prefetcher_.sstable_index_filter_->get_pushdown_filter()) ||
                         (nullptr != parent && nullptr == parent_bitmap))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count), K(result_bitmap.size()), KP(filter_info.filter_),
             KP(parent), KP(parent_bitmap));
  } else if (end_of_scan() || row_count_left() < row_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected scanner", K(ret), K(row_count), KPC(this));
  } else {
    prefetcher_.cur_micro_data_read_idx_ = prefetcher_.cur_micro_data_fetch_idx_;

    while (OB_SUCC(ret)) {
      if (end_of_scan()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(inner_filter(parent, filter_info, parent_bitmap, result_bitmap))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to apply filter", K(ret));
        } else if (!prefetcher_.read_finish()) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  LOG_TRACE("[COLUMNSTORE] apply filter info in cg", K(ret), K_(query_index_range), K(row_count),
            "bitmap_size", result_bitmap.size(),  "popcnt", result_bitmap.popcnt());
  return ret;
}

int ObCGScanner::get_next_valid_block(sql::ObPushdownFilterExecutor *parent,
                                      const ObCGBitmap *parent_bitmap,
                                      ObCGBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (prefetcher_.cur_micro_data_fetch_idx_ >= prefetcher_.micro_data_prefetch_idx_ - 1) {
      ret = OB_ITER_END;
      LOG_DEBUG("Calc to end of prefetched data", K(ret), K(prefetcher_.cur_micro_data_fetch_idx_),
                K(prefetcher_.micro_data_prefetch_idx_));
    } else {
      if (prefetcher_.cur_micro_data_fetch_idx_ > -1) {
        prefetcher_.current_micro_handle().reset();
      }
      ++prefetcher_.cur_micro_data_fetch_idx_;
      ++prefetcher_.cur_micro_data_read_idx_;
      const ObMicroIndexInfo &index_info = prefetcher_.current_micro_info();
      const ObCSRange &row_range = index_info.get_row_range();
      if (index_info.is_filter_always_false()) {
        if (OB_FAIL(result_bitmap.set_bitmap_batch(
                    MAX(query_index_range_.start_row_id_, row_range.start_row_id_),
                    MIN(query_index_range_.end_row_id_, row_range.end_row_id_),
                    false))) {
          LOG_WARN("Fail to set bitmap batch", K(ret), K(row_range));
        }
      } else if (index_info.is_filter_always_true()) {
        if (OB_FAIL(result_bitmap.set_bitmap_batch(
                    MAX(query_index_range_.start_row_id_, row_range.start_row_id_),
                    MIN(query_index_range_.end_row_id_, row_range.end_row_id_),
                    true))) {
          LOG_WARN("Fail to set bitmap batch", K(ret), K(row_range));
        }
      } else if (nullptr != parent && ObCGScanner::can_skip_filter(
              *parent, *parent_bitmap, prefetcher_.current_micro_info().get_row_range())) {
        continue;
      } else {
        break;
      }
    }
  }
  return ret;
}

bool ObCGScanner::can_skip_filter(const sql::ObPushdownFilterExecutor &parent,
                                  const ObCGBitmap &parent_bitmap,
                                  const ObCSRange &row_range)
{
  return (parent.is_logic_and_node() && parent_bitmap.is_all_false(row_range)) ||
      (parent.is_logic_or_node() && parent_bitmap.is_all_true(row_range));
}

int ObCGScanner::inner_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    const ObCGBitmap *parent_bitmap,
    ObCGBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t access_count = 0;
  const common::ObBitmap *bitmap = nullptr;
  while (OB_SUCC(ret)) {
    if (end_of_scan()) {
      ret = OB_ITER_END;
    } else if (is_new_range_ || OB_ITER_END == micro_scanner_->end_of_block()) {
      if (OB_FAIL(get_next_valid_block(parent, parent_bitmap, result_bitmap))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next valid index", K(ret));
        }
      } else if (OB_FAIL(open_cur_data_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to open cur data block", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObMicroIndexInfo &index_info = prefetcher_.current_micro_info();
      const ObCSRange &row_range = index_info.get_row_range();
      uint32_t offset = row_range.start_row_id_ > query_index_range_.start_row_id_ ? (row_range.start_row_id_ - query_index_range_.start_row_id_) : 0;

      index_info.pre_process_filter(*filter_info.filter_);
      if (OB_FAIL(micro_scanner_->filter_micro_block_in_cg(
                  parent, filter_info, parent_bitmap, row_range.start_row_id_, access_count))) {
        LOG_WARN("Fail to apply filter", K(ret));
      } else if (OB_ISNULL(bitmap = filter_info.filter_->get_result()) || bitmap->size() != access_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null filter bitmap", K(ret), KPC(filter_info.filter_), K(access_count));
      } else if (OB_FAIL(result_bitmap.append_bitmap(*bitmap, offset, false))) {
        LOG_WARN("Fail to append bitmap", K(ret), K(offset), KPC(bitmap), K(result_bitmap));
      } else {
        current_ = is_reverse_scan_ ? current_ - access_count : current_ + access_count;
      }
      index_info.post_process_filter(*filter_info.filter_);
    }
  }
  return ret;
}

//////////////////////////// Column Group Projection Scanner ////////////////////////////////////////
ObCGRowScanner::~ObCGRowScanner()
{
  reset();
}

void ObCGRowScanner::reset()
{
  if (nullptr != access_ctx_ && nullptr != access_ctx_->stmt_allocator_) {
    if (nullptr != cell_data_ptrs_) {
      access_ctx_->stmt_allocator_->free(cell_data_ptrs_);
      cell_data_ptrs_ = nullptr;
    }
    if (nullptr != row_ids_) {
      access_ctx_->stmt_allocator_->free(row_ids_);
      row_ids_ = nullptr;
    }
    if (nullptr != len_array_) {
      access_ctx_->stmt_allocator_->free(len_array_);
      len_array_ = nullptr;
    }
  }
  filter_bitmap_ = nullptr;
  read_info_ = nullptr;
  datum_infos_.reset();
  col_params_.reset();
  ObCGScanner::reset();
}

void ObCGRowScanner::reuse()
{
  ObCGScanner::reuse();
  filter_bitmap_ = nullptr;
}

int ObCGRowScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!iter_param.vectorized_enabled_ || nullptr == iter_param.op_ || !wrapper.is_valid() ||
                  nullptr == iter_param.output_exprs_ || nullptr == iter_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(wrapper), K(iter_param));
  } else if (OB_FAIL(ObCGScanner::init(iter_param, access_ctx, wrapper))) {
    LOG_WARN("Fail to init cg scanner", K(ret));
  } else {
    read_info_ = iter_param.get_read_info();
    col_params_.set_allocator(access_ctx_->stmt_allocator_);
    void *buf = nullptr;
    void *len_array_buf = nullptr;
    int64_t expr_count = iter_param.output_exprs_->count();
    const share::schema::ObColumnParam *col_param = nullptr;
    sql::ObEvalCtx &eval_ctx = iter_param.op_->get_eval_ctx();
    const common::ObIArray<int32_t>* out_cols_projector = iter_param.out_cols_project_;
    int64_t sql_batch_size = iter_param.op_->get_batch_size();
    const common::ObIArray<share::schema::ObColumnParam *>* out_cols_param = iter_param.get_col_params();
    const bool use_new_format = iter_param.use_new_format();
    if (OB_UNLIKELY(sql_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected sql batch size", K(ret), K(sql_batch_size), K(iter_param));
    } else if (OB_FAIL(col_params_.init(expr_count))) {
      LOG_WARN("Fail to init col params", K(ret));
      // TODO: remove these later
    } else if (OB_ISNULL(buf = access_ctx.stmt_allocator_->alloc(sizeof(char *) * sql_batch_size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cell data", K(ret), K(sql_batch_size));
    } else if (FALSE_IT(cell_data_ptrs_ = reinterpret_cast<const char **>(buf))) {
    } else if (OB_ISNULL(buf = access_ctx.stmt_allocator_->alloc(sizeof(int32_t) * sql_batch_size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc row_ids", K(ret), K(sql_batch_size));
    } else if (FALSE_IT(row_ids_ = reinterpret_cast<int32_t *>(buf))) {
    } else if (OB_ISNULL(len_array_buf = access_ctx.stmt_allocator_->alloc(sizeof(uint32_t) * sql_batch_size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc len_array_buf", K(ret), K(sql_batch_size));
    } else if (FALSE_IT(len_array_ = reinterpret_cast<uint32_t *>(len_array_buf))) {
    } else if (!iter_param.enable_pd_aggregate()) {
      bool need_padding = common::is_pad_char_to_full_length(access_ctx.sql_mode_);
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; i++) {
        col_param = nullptr;
        int64_t col_offset = out_cols_projector->at(i);
        const common::ObObjMeta &obj_meta = read_info_->get_columns_desc().at(col_offset).col_type_;
        if (need_padding && obj_meta.is_fixed_len_char_type()) {
          col_param = out_cols_param->at(col_offset);
        } else if (obj_meta.is_lob_storage() || obj_meta.is_decimal_int()) {
          col_param = out_cols_param->at(col_offset);
        }
        if (OB_FAIL(col_params_.push_back(col_param))) {
          LOG_WARN("Failed to push back col param", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        datum_infos_.set_allocator(access_ctx_->stmt_allocator_);
        if (OB_FAIL(datum_infos_.init(expr_count))) {
          LOG_WARN("Failed to init datum infos", K(ret), K(expr_count));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; i++) {
            sql::ObExpr *expr = iter_param.output_exprs_->at(i);
            common::ObDatum *datums = nullptr;
            if (OB_ISNULL(datums = expr->locate_batch_datums(eval_ctx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected null datums", K(ret), K(i), KPC(expr));
            } else if (OB_UNLIKELY(!use_new_format &&
                                   !(expr->is_variable_res_buf()) &&
                                   datums->ptr_ != eval_ctx.frames_[expr->frame_idx_] + expr->res_buf_off_)) {
              ret = OB_ERR_SYS;
              LOG_ERROR("Unexpected sql expr datum buffer", K(ret), KP(datums->ptr_), K(eval_ctx), KPC(expr));
            } else if (OB_FAIL(datum_infos_.push_back(ObSqlDatumInfo(datums, iter_param.output_exprs_->at(i))))) {
              LOG_WARN("fail to push back datum", K(ret), K(datums));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

int ObCGRowScanner::get_next_rows(uint64_t &count, const uint64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_rows(count, capacity, 0/*datum_offset*/))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to inner get next rows", K(ret));
    }
  }
  return ret;
}

int ObCGRowScanner::get_next_rows(uint64_t &count, const uint64_t capacity, const int64_t datum_offset)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGScanner not inited", K(ret));
  } else if (end_of_scan() || prefetcher_.is_empty_range()) {
    ret = OB_ITER_END;
  } else {
    int64_t batch_size = MIN(iter_param_->op_->get_batch_size(), capacity);
    prefetcher_.recycle_block_data();
    if (nullptr != micro_scanner_) {
      micro_scanner_->reserve_reader_memory(false);
    }
    while (OB_SUCC(ret) && count < batch_size) {
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      // prefetched micro data maybe empty when using skip index
      } else if (end_of_scan() || prefetcher_.is_empty_range()) {
        ret = OB_ITER_END;
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(fetch_rows(batch_size, count, datum_offset))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to fetch rows", K(ret));
        } else if (!prefetcher_.read_finish()) {
          ret = OB_SUCCESS;
          if (prefetcher_.is_prefetched_full()) {
            break;
          }
        }
      }
    }
  }
  if (count > 0 && datum_infos_.count() > 0) {
    if (iter_param_->op_->enable_rich_format_) {
      LOG_TRACE("[COLUMNSTORE] get next rows in cg", K(ret), K_(query_index_range), K_(current), K(count), K(capacity),
                "new format datums",
                sql::ToStrVectorHeader(*datum_infos_.at(0).expr_,
                                       iter_param_->op_->get_eval_ctx(),
                                       nullptr,
                                       sql::EvalBound(datum_offset + count, true)));
    } else {
      LOG_TRACE("[COLUMNSTORE] get next rows in cg", K(ret), K_(query_index_range), K_(current), K(count), K(capacity),
                "datums", ObArrayWrap<ObDatum>(datum_infos_.at(0).datum_ptr_, datum_offset + count));
    }
  }
  return ret;
}

int ObCGRowScanner::fetch_rows(const int64_t batch_size, uint64_t &count, const int64_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (is_new_range_) {
    prefetcher_.cur_micro_data_fetch_idx_++;
    prefetcher_.cur_micro_data_read_idx_++;
    if (OB_FAIL(open_cur_data_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to open data block", K(ret), K_(current));
      }
    }
  }

  int64_t row_cap = 0;
  while (OB_SUCC(ret) && count < batch_size) {
    if (count > 0) {
      micro_scanner_->reserve_reader_memory(true);
    }
    if (end_of_scan()) {
      ret = OB_ITER_END;
    } else if (0 != prefetcher_.current_micro_info().get_row_range().compare(current_)) {
      if (prefetcher_.cur_micro_data_fetch_idx_ == prefetcher_.micro_data_prefetch_idx_ - 1) {
        ret = OB_ITER_END;
        LOG_DEBUG("Calc to end of prefetched data", K(ret), K(prefetcher_.cur_micro_data_fetch_idx_),
                  K(prefetcher_.micro_data_prefetch_idx_));
      } else if (FALSE_IT(++prefetcher_.cur_micro_data_fetch_idx_)) {
      } else if (OB_FAIL(open_cur_data_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to open data block", K(ret), K_(current));
        }
      }
    } else if (OB_FAIL(convert_bitmap_to_cs_index(row_ids_,
                                                  row_cap,
                                                  current_,
                                                  query_index_range_,
                                                  prefetcher_.current_micro_info().get_row_range(),
                                                  filter_bitmap_,
                                                  batch_size - count,
                                                  is_reverse_scan_))) {
      LOG_WARN("Fail to get row ids", K(ret), K_(current), K_(query_index_range));
    } else if (0 == row_cap) {
    } else if (OB_FAIL(inner_fetch_rows(row_cap, datum_offset + count))) {
      LOG_WARN("Fail to get next rows", K(ret));
    } else {
      count += row_cap;
    }
  }
  return ret;
}

int ObCGRowScanner::inner_fetch_rows(const int64_t row_cap, const int64_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_scanner_->get_next_rows(*iter_param_->out_cols_project_,
                                            col_params_,
                                            row_ids_,
                                            cell_data_ptrs_,
                                            row_cap,
                                            datum_infos_,
                                            datum_offset,
                                            len_array_))) {
    LOG_WARN("Fail to get next rows", K(ret));
  }
  return ret;
}

int ObCGRowScanner::locate(
    const ObCSRange &range,
    const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGScanner::locate(range, bitmap))) {
    LOG_WARN("Fail to locate", K(ret));
  } else {
    filter_bitmap_ = (nullptr != bitmap && !bitmap->is_all_true()) ? bitmap : nullptr;
  }
  return ret;
}

int ObCGRowScanner::deep_copy_projected_rows(const int64_t datum_offset, const uint64_t count)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = iter_param_->op_->get_batch_size();
  sql::ObEvalCtx &eval_ctx = iter_param_->op_->get_eval_ctx();
  if (OB_UNLIKELY(datum_offset + count > batch_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), K(count), K(batch_size));
  } else if (!iter_param_->use_new_format()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_infos_.count(); ++i) {
      common::ObDatum *datums = datum_infos_.at(i).datum_ptr_;
      sql::ObExpr *e = iter_param_->output_exprs_->at(i);
      if (OBJ_DATUM_STRING == e->obj_datum_map_) {
        for (int64_t batch_idx = datum_offset; OB_SUCC(ret) && batch_idx < datum_offset + count; ++batch_idx) {
          char *ptr = nullptr;
          ObDatum &datum = e->locate_expr_datum(eval_ctx, batch_idx);
          if (!datum.is_null()) {
            if (OB_ISNULL(ptr = e->get_str_res_mem(eval_ctx, datum.len_, batch_idx))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMCPY(const_cast<char *>(ptr), datum.ptr_, datum.len_);
              datum.ptr_ = ptr;
            }
          }
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_param_->output_exprs_->count(); ++i) {
      sql::ObExpr *expr = iter_param_->output_exprs_->at(i);
      VectorFormat format = expr->get_format(eval_ctx);
      if (format == VEC_DISCRETE) {
        ObDiscreteFormat *discrete_format = static_cast<ObDiscreteFormat *>(expr->get_vector(eval_ctx));
        for (int64_t batch_idx = datum_offset; OB_SUCC(ret) && batch_idx < datum_offset + count; ++batch_idx) {
          if (!discrete_format->is_null(batch_idx)) {
            const char *payload = nullptr;
            ObLength length = 0;
            discrete_format->get_payload(batch_idx, payload, length);
            char *ptr = nullptr;
            if (OB_ISNULL(ptr = expr->get_str_res_mem(eval_ctx, length, batch_idx))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("Failed to allocate memory", K(ret));
            } else {
              MEMCPY(const_cast<char *>(ptr), payload, length);
              discrete_format->set_payload_shallow(batch_idx, ptr, length);
            }
          }
        }
      } else if (OB_UNLIKELY(format != VEC_FIXED && format != VEC_DISCRETE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected data format", K(ret), K(format), KPC(expr));
      }
    }
  }
  if (count > 0 && !datum_infos_.empty()) {
    if (iter_param_->op_->enable_rich_format_) {
      LOG_TRACE("[COLUMNSTORE] get next rows in cg", K(ret), K(datum_offset), K(count),
                "new format datums",
                sql::ToStrVectorHeader(*datum_infos_.at(0).expr_,
                                       iter_param_->op_->get_eval_ctx(),
                                       nullptr,
                                       sql::EvalBound(datum_offset + count, true)));
    } else {
      LOG_DEBUG("[COLUMNSTORE] deep copy projected rows", K(ret), K(datum_offset), K(count),
                "datums", ObArrayWrap<ObDatum>(datum_infos_.at(0).datum_ptr_, datum_offset + count));
    }
  }
  return ret;
}

int ObCGScanner::build_index_filter(sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == access_ctx_ || nullptr == access_ctx_->stmt_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected access context", K(ret), KP_(access_ctx), KP(access_ctx_->stmt_allocator_));
  } else if (OB_FAIL(ObSSTableIndexFilterFactory::build_sstable_index_filter(
              true, iter_param_->get_read_info(), filter, access_ctx_->allocator_, prefetcher_.sstable_index_filter_))) {
    LOG_WARN("Failed to construct skip filter", K(ret), K(filter));
  }

  return ret;
}

//////////////////////////// ObCGSingleRowScanner ////////////////////////////////////////
int ObCGSingleRowScanner::get_next_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGScanner not inited", K(ret));
  } else if (end_of_scan() || prefetcher_.is_empty_range()) {
    ret = OB_ITER_END;
  } else {
    prefetcher_.recycle_block_data();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      // prefetched micro data maybe empty when using skip index
      } else if (end_of_scan() || prefetcher_.is_empty_range()) {
        ret = OB_ITER_END;
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(fetch_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to fetch rows", K(ret));
        } else if (!prefetcher_.read_finish()) {
          ret = OB_SUCCESS;
          if (prefetcher_.is_prefetched_full()) {
            break;
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObCGSingleRowScanner::fetch_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (is_new_range_) {
    prefetcher_.cur_micro_data_fetch_idx_++;
    prefetcher_.cur_micro_data_read_idx_++;
    if (OB_FAIL(open_cur_data_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to open data block", K(ret), K_(current));
      }
    }
  }
  int64_t row_cap = 0;
  while (OB_SUCC(ret)) {
    if (end_of_scan()) {
      ret = OB_ITER_END;
    } else if (0 != prefetcher_.current_micro_info().get_row_range().compare(current_)) {
      if (prefetcher_.cur_micro_data_fetch_idx_ == prefetcher_.micro_data_prefetch_idx_ - 1) {
        ret = OB_ITER_END;
      } else if (FALSE_IT(++prefetcher_.cur_micro_data_fetch_idx_)) {
      } else if (OB_FAIL(open_cur_data_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to open data block", K(ret), K_(current));
        }
      }
    } else if (OB_FAIL(inner_fetch_row(datum_row))) {
      LOG_WARN("Fail to get next rows", K(ret));
    } else {
      break;
    }
  }
  return ret;
}

int ObCGSingleRowScanner::inner_fetch_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_scanner_->get_next_row(datum_row))) {
    LOG_WARN("Failed to get next row", K(ret), K_(current));
  } else if (OB_UNLIKELY(1 != datum_row->count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected column count in ObDatumRow", K(ret), K_(datum_row->count));
  } else {
    current_ += is_reverse_scan_ ? -1 : 1;
  }
  return ret;
}

}
}
