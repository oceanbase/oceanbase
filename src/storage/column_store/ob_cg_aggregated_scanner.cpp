// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "ob_cg_aggregated_scanner.h"
#include "storage/access/ob_aggregated_store.h"
#include "storage/access/ob_aggregated_store_vec.h"

namespace oceanbase
{
namespace storage
{

ObCGAggregatedScanner::ObCGAggregatedScanner()
  : ObCGRowScanner(),
    agg_group_(nullptr),
    cur_processed_row_count_(0),
    need_access_data_(false),
    need_get_row_ids_(false),
    is_agg_finished_(false)
{}

ObCGAggregatedScanner::~ObCGAggregatedScanner()
{
  reset();
}

void ObCGAggregatedScanner::reset()
{
  ObCGRowScanner::reset();
  need_access_data_ = false;
  need_get_row_ids_ = false;
  is_agg_finished_ = false;
  if (nullptr != agg_group_) {
    if (OB_UNLIKELY(!agg_group_->is_vec())) {
      FREE_PTR_FROM_CONTEXT(access_ctx_, agg_group_, ObAggGroupBase);
    } else {
      agg_group_ = nullptr;
    }
  }
  cur_processed_row_count_ = 0;
}

void ObCGAggregatedScanner::reuse()
{
  ObCGRowScanner::reuse();
  cur_processed_row_count_ = 0;
}

int ObCGAggregatedScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGAggregatedScanner has been inited", K(ret));
  } else if (OB_UNLIKELY(!wrapper.is_valid() || !wrapper.get_sstable()->is_major_or_ddl_merge_sstable() ||
                         !iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(wrapper), K(iter_param));
  } else if (OB_UNLIKELY(nullptr == iter_param.aggregate_exprs_ ||
                         0 == iter_param.aggregate_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregated expr count", K(ret), KPC(iter_param.output_exprs_));
  } else if (OB_FAIL(check_need_access_data(iter_param, access_ctx))) {
    LOG_WARN("Fail to check need access data", K(ret));
  }
  if (OB_SUCC(ret) && (need_access_data_ || need_get_row_ids_)) {
    if (OB_FAIL(ObCGRowScanner::init(iter_param, access_ctx, wrapper))) {
      LOG_WARN("Fail to init cg scanner", K(ret));
    } else if (iter_param.enable_base_skip_index()) {
      prefetcher_.set_agg_group(agg_group_);
    }
  }
  if (OB_SUCC(ret)) {
    iter_param_ = &iter_param;
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  }
  return ret;
}

int ObCGAggregatedScanner::locate(
    const ObCSRange &range,
    const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGAggregatedScanner not init", K(ret));
  } else {
    cur_processed_row_count_ = nullptr == bitmap ? range.get_row_count() : bitmap->popcnt();
    if ((!need_access_data_ && !need_get_row_ids_) || check_agg_finished()) {
    } else if (OB_FAIL(ObCGRowScanner::locate(range, bitmap))) {
      LOG_WARN("Fail to locate cg scanner", K(ret));
    }
  }
  return ret;
}

int ObCGAggregatedScanner::get_next_rows(uint64_t &count, const uint64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGAggregatedScanner not init", K(ret));
  } else if (check_agg_finished() || 0 == cur_processed_row_count_) {
    ret = OB_ITER_END;
  } else if (!need_access_data_ && !need_get_row_ids_) {
    ObPushdownRowIdCtx pd_row_id_ctx;
    pd_row_id_ctx.begin_ = 0;
    pd_row_id_ctx.end_ = cur_processed_row_count_ - 1;
    if (OB_FAIL(agg_group_->eval_batch(iter_param_,
                                       access_ctx_,
                                       0/*col_offset*/,
                                       nullptr/*reader*/,
                                       pd_row_id_ctx,
                                       false/*reserve_memory*/))) {
      LOG_WARN("Fail to eval batch rows", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("query interrupt", K(ret));
      } else if (OB_FAIL(ObCGRowScanner::get_next_rows(count, capacity, 0/*datum_offset*/))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next rows", K(ret));
        }
      } else if (check_agg_finished()) {
        ret = OB_ITER_END;
      }
    }
  }
  if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("Unexpected, ret should be OB_ITER_END", K(ret));
    if (OB_SUCCESS == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    count = cur_processed_row_count_;
    cur_processed_row_count_ = 0;
  }
  return ret;
}

int ObCGAggregatedScanner::inner_fetch_rows(const int64_t batch_size, uint64_t &count, const int64_t datum_offset)
{
  UNUSED(datum_offset);
  int ret = OB_SUCCESS;
  int64_t row_cap = 0;
  if (iter_param_->use_new_format()) {
    micro_scanner_->reserve_reader_memory(false);
    ObAggGroupVec *agg_group_vec = static_cast<ObAggGroupVec *>(agg_group_);
    const ObCSRange &data_range = prefetcher_.current_micro_info().get_row_range();
    const int64_t upper_bound = MIN(query_index_range_.end_row_id_, data_range.end_row_id_);
    const int64_t lower_bound = MAX(query_index_range_.start_row_id_, data_range.start_row_id_);
    ObPushdownRowIdCtx pd_row_id_ctx;
    if (OB_ISNULL(agg_group_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null agg group", K(ret), KP(agg_group_vec));
    } else if (nullptr == filter_bitmap_) {
      // empty row_ids
      pd_row_id_ctx.reuse();
      pd_row_id_ctx.begin_ = current_ - data_range.start_row_id_;
      pd_row_id_ctx.end_ = upper_bound - data_range.start_row_id_;
      pd_row_id_ctx.bound_row_id_ = is_reverse_scan_ ? lower_bound : upper_bound;
      pd_row_id_ctx.is_reverse_ = is_reverse_scan_;
      if (OB_FAIL(agg_group_vec->agg_pushdown_decoder(micro_scanner_->get_reader(), 0/*col_offset*/, pd_row_id_ctx))) {
        LOG_WARN("fail to pushdown aggregate to decoder", K(ret), K(pd_row_id_ctx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (nullptr == filter_bitmap_ && agg_group_vec->is_agg_finish(pd_row_id_ctx)) {
      current_ = is_reverse_scan_ ? lower_bound - 1 : upper_bound + 1;
      count += pd_row_id_ctx.get_row_count();
      if (end_of_scan()) {
        ret = OB_ITER_END;
      }
      LOG_TRACE("all aggregate has been pushdown to decoder", K(ret), K(agg_group_vec->agg_row_id_), K(pd_row_id_ctx));
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
    } else {
      pd_row_id_ctx.reuse();
      pd_row_id_ctx.row_ids_ = row_ids_;
      pd_row_id_ctx.row_cap_ = row_cap;
      pd_row_id_ctx.bound_row_id_ = get_bound_row_id();
      pd_row_id_ctx.is_reverse_ = is_reverse_scan_;
      if (nullptr != filter_bitmap_ && OB_FAIL(agg_group_vec->agg_pushdown_decoder(micro_scanner_->get_reader(), 0/*col_offset*/, pd_row_id_ctx))) {
        LOG_WARN("fail to pushdown aggregate to decoder", K(ret), K(pd_row_id_ctx));
      } else if (agg_group_vec->is_agg_finish(pd_row_id_ctx)) {
        count += row_cap;
        LOG_DEBUG("all aggregate has been pushdown to decoder", K(ret), K(agg_group_vec->agg_row_id_), K(pd_row_id_ctx));
      } else if (OB_FAIL(micro_scanner_->get_next_rows(*iter_param_->out_cols_project_,
                                                        col_params_,
                                                        row_ids_,
                                                        cell_data_ptrs_,
                                                        row_cap,
                                                        datum_infos_,
                                                        0, /*datum offset*/
                                                        len_array_,
                                                        is_padding_mode_,
                                                        !access_ctx_->block_row_store_->filter_is_null()))) {
        LOG_WARN("fail to get next rows", K(ret));
      } else if (OB_FAIL(agg_group_vec->eval_batch(iter_param_, access_ctx_, 0/*col_offset*/,
                                                   micro_scanner_->get_reader(), pd_row_id_ctx, false))) {
        LOG_WARN("fail to aggregate batch", K(ret));
      } else {
        count += row_cap;
        if (row_cap > 0 && datum_infos_.count() > 0) {
          LOG_DEBUG("[COLUMNSTORE] get next rows in cg", K(ret), K_(query_index_range), K_(current), K(row_cap),
            "new format datums", sql::ToStrVectorHeader(*datum_infos_.at(0).expr_, iter_param_->op_->get_eval_ctx(),
                                                        nullptr,  sql::EvalBound(row_cap, true)));
        }
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
  } else {
    ObPushdownRowIdCtx pd_row_id_ctx(row_ids_, row_cap);
    if (OB_FAIL(agg_group_->eval_batch(iter_param_, access_ctx_, 0/*col_offset*/, 
                                       micro_scanner_->get_reader(), pd_row_id_ctx, false))) {
      LOG_WARN("fail to aggregate batch", K(ret));
    } else {
      count += row_cap;
    }
  }
  return ret;
}

int ObCGAggregatedScanner::check_need_access_data(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == access_ctx.block_row_store_ ||
                  nullptr == iter_param.aggregate_exprs_ ||
                  0 == iter_param.aggregate_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregated expr count", K(ret), KPC(iter_param.aggregate_exprs_));
  } else if (iter_param.use_new_format()) {
    const sql::ObExpr *output_expr = nullptr;
    ObAggregatedStoreVec *agg_store_vec = static_cast<ObAggregatedStoreVec *>(access_ctx.block_row_store_);
    ObAggGroupVec *agg_group_vec = nullptr;
    if (OB_UNLIKELY(iter_param.output_exprs_->count() > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid output expr count", K(iter_param.output_exprs_->count()));
    } else if (iter_param.output_exprs_->count() == 1) {
      output_expr = iter_param.output_exprs_->at(0);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(agg_store_vec->get_agg_group(output_expr, agg_group_vec))) {
        LOG_WARN("Failed to get aggregate group", K(ret));
      } else {
        need_access_data_ |= agg_group_vec->need_access_data_;
        need_get_row_ids_ |= agg_group_vec->need_get_row_ids_;
        agg_group_ = agg_group_vec;
      }
    }
  } else {
    ObAggregatedStore *agg_store = static_cast<ObAggregatedStore *>(access_ctx.block_row_store_);
    ObCGAggCells *agg_cells = nullptr;
    agg_cells = OB_NEWx(ObCGAggCells, access_ctx.stmt_allocator_);
    if (OB_ISNULL(agg_cells)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < iter_param.aggregate_exprs_->count(); ++i) {
        ObAggCell *cell = nullptr;
        if (OB_FAIL(agg_store->get_agg_cell(iter_param.aggregate_exprs_->at(i), cell))) {
          LOG_WARN("Fail to get agg cell", K(ret));
        } else if (OB_FAIL(agg_cells->add_agg_cell(cell))) {
          LOG_WARN("Fail to push back", K(ret));
        } else if (!need_access_data_) {
          need_access_data_ = cell->need_access_data();
        }
      }
      // need_get_row_ids_ not used in vec 1.0 now, keep it the same as need_access_data_
      need_get_row_ids_ = need_access_data_; 
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(agg_cells)) {
        agg_cells->~ObCGAggCells();
        access_ctx.stmt_allocator_->free(agg_cells);
        agg_cells = nullptr;
      }
    } else {
      agg_group_ = agg_cells;
    }
  }
  return ret;
}

OB_INLINE bool ObCGAggregatedScanner::check_agg_finished()
{
  if (is_agg_finished_) {
  } else {
    is_agg_finished_ = agg_group_->check_finished();
  }
  return is_agg_finished_;
}

}
}
