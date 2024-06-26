// Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "ob_cg_group_by_scanner.h"
#include "storage/access/ob_vector_store.h"

namespace oceanbase
{
namespace storage
{

ObCGGroupByScanner::ObCGGroupByScanner()
    : ObCGRowScanner(),
    output_exprs_(nullptr),
    group_by_agg_idxs_(),
    group_by_cell_(nullptr),
    index_prefetcher_()
{}

ObCGGroupByScanner::~ObCGGroupByScanner()
{
  reset();
}

void ObCGGroupByScanner::reuse()
{
  ObCGRowScanner::reuse();
  index_prefetcher_.reuse();
}

void ObCGGroupByScanner::reset()
{
  ObCGRowScanner::reset();
  output_exprs_ = nullptr;
  group_by_agg_idxs_.reset();
  group_by_cell_ = nullptr;
  index_prefetcher_.reset();
}

int ObCGGroupByScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGRowScanner::init(iter_param, access_ctx, wrapper))) {
    LOG_WARN("Failed to init ObCGRowScanner", K(ret));
  } else if (OB_FAIL(index_prefetcher_.init(get_type(), *sstable_, iter_param, access_ctx))) {
    LOG_WARN("fail to init index prefetcher, ", K(ret));
  } else if (OB_UNLIKELY(nullptr == iter_param.output_exprs_ ||
                         0 == iter_param.output_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected group by expr count", K(ret), KPC(iter_param.output_exprs_));
  } else {
    output_exprs_ = iter_param.output_exprs_;
    group_by_cell_ = (static_cast<ObVectorStore*>(access_ctx.block_row_store_))->get_group_by_cell();
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  }
  return ret;
}

int ObCGGroupByScanner::switch_context(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGRowScanner::switch_context(iter_param, access_ctx, wrapper))) {
    LOG_WARN("Fail to switch context for cg row scanner", K(ret));
  } else if (!index_prefetcher_.is_valid()) {
    if (OB_FAIL(index_prefetcher_.init(get_type(), *sstable_, iter_param, access_ctx))) {
      LOG_WARN("fail to init prefetcher, ", K(ret));
    }
  } else if (OB_FAIL(index_prefetcher_.switch_context(get_type(), *sstable_, iter_param, access_ctx))) {
    LOG_WARN("Fail to switch context for prefetcher", K(ret));
  }
  return ret;
}

int ObCGGroupByScanner::init_group_by_info()
{
  int ret = OB_SUCCESS;
  ObGroupByAggIdxArray agg_idxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_->count(); ++i) {
    agg_idxs.reuse();
    if (OB_ISNULL(output_exprs_->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null output expr", K(ret));
    } else if (OB_FAIL(group_by_cell_->assign_agg_cells(output_exprs_->at(i), agg_idxs))) {
      LOG_WARN("Failed to extact agg cells", K(ret));
    } else if (OB_FAIL(group_by_agg_idxs_.push_back(agg_idxs))) {
      LOG_WARN("Failed to push back", K(ret));
    }
  }
  return ret;
}

int ObCGGroupByScanner::decide_group_size(int64_t &group_size)
{
  int ret = OB_SUCCESS;
  group_size = -1;
  // must have been located
  if (OB_UNLIKELY(!is_new_range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, should be new range", K(ret));
  }
  while (OB_SUCC(ret) && -1 == group_size) {
    if (end_of_scan()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(index_prefetcher_.prefetch())) {
      LOG_WARN("Fail to prefetch micro block", K(ret), K_(index_prefetcher));
    } else if (index_prefetcher_.read_wait()) {
      continue;
    } else {
      index_prefetcher_.cur_micro_data_fetch_idx_++;
      index_prefetcher_.cur_micro_data_read_idx_++;
      is_new_range_ = false;
      const ObCSRange &micro_data_range = index_prefetcher_.current_micro_info().get_row_range();
      group_size = MIN(query_index_range_.end_row_id_, micro_data_range.end_row_id_) -
          MAX(query_index_range_.start_row_id_, micro_data_range.start_row_id_) + 1;
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(group_size), K_(query_index_range), K_(index_prefetcher));
  return ret;
}

int ObCGGroupByScanner::decide_can_group_by(const int32_t group_by_col, bool &can_group_by)
{
  int ret = OB_SUCCESS;
  // must have been located
  if (OB_UNLIKELY(!is_new_range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, should be new range", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (end_of_scan()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected end of scan", K(ret), KPC(this));
      } else if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      } else if (prefetcher_.read_wait()) {
        continue;
      } else {
        prefetcher_.cur_micro_data_fetch_idx_++;
        prefetcher_.cur_micro_data_read_idx_++;
        is_new_range_ = false;
        int64_t row_cnt = 0;
        int64_t read_cnt = 0;
        int64_t distinct_cnt = 0;
        if (OB_FAIL(open_cur_data_block())) {
          LOG_WARN("Failed to open data block", K(ret));
        } else if (OB_FAIL(micro_scanner_->check_can_group_by(group_by_col, row_cnt, read_cnt, distinct_cnt, can_group_by))) {
          LOG_WARN("Failed to check group by", K(ret));
        } else if (can_group_by && OB_FAIL(group_by_cell_->decide_use_group_by(
                    row_cnt, read_cnt, distinct_cnt, filter_bitmap_, can_group_by))) {
          LOG_WARN("Failed to decide use group by", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObCGGroupByScanner::read_distinct(const int32_t group_by_col)
{
  const char **cell_data = group_by_cell_->get_cell_datas();
  return micro_scanner_->read_distinct(group_by_col,
      nullptr == cell_data ? cell_data_ptrs_ : cell_data, *group_by_cell_);
}

int ObCGGroupByScanner::read_reference(const int32_t group_by_col)
{
  int ret = OB_SUCCESS;
  int64_t row_cap = 0;
  if (end_of_scan()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(convert_bitmap_to_cs_index(row_ids_,
                                                row_cap,
                                                current_,
                                                query_index_range_,
                                                prefetcher_.current_micro_info().get_row_range(),
                                                filter_bitmap_,
                                                group_by_cell_->get_batch_size(),
                                                is_reverse_scan_))) {
    LOG_WARN("Fail to get row ids", K(ret), K_(current), K_(query_index_range));
  } else if (OB_FAIL(micro_scanner_->read_reference(group_by_col, row_ids_, row_cap, *group_by_cell_))) {
    LOG_WARN("Failed to read ref", K(ret));
  }
  return ret;
}

int ObCGGroupByScanner::calc_aggregate(const bool is_group_by_col)
{
  int ret = OB_SUCCESS;
  uint64_t read_row_count = 0;
  uint64_t remain_row_count = group_by_cell_->get_ref_cnt();
  int64_t ref_offset = 0;
  if (is_group_by_col) {
    if (OB_FAIL(do_group_by_aggregate(remain_row_count, is_group_by_col, ref_offset))) {
      LOG_WARN("Failed to do group by aggregate", K(ret));
    }
  } else {
    while (OB_SUCC(ret) && 0 < remain_row_count) {
      read_row_count = 0;
      if (OB_FAIL(get_next_rows(read_row_count, remain_row_count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next rows", K(ret));
        } else if (OB_UNLIKELY(read_row_count != remain_row_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected output row count", K(ret), K(read_row_count), K(remain_row_count));
        } else {
          remain_row_count -= read_row_count;
        }
      } else if (OB_UNLIKELY(0 == read_row_count || read_row_count > remain_row_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected output row count", K(ret), K(read_row_count), K(remain_row_count));
      } else if (OB_FAIL(do_group_by_aggregate(read_row_count, is_group_by_col, ref_offset))) {
        LOG_WARN("Failed to do group by aggregate", K(ret));
      } else {
        remain_row_count -= read_row_count;
        ref_offset += read_row_count;
      }
    }
  }
  return ret;
}

int ObCGGroupByScanner::do_group_by_aggregate(const uint64_t count, const bool is_group_by_col, const int64_t ref_offset)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < datum_infos_.count(); ++i) {
    common::ObDatum *col_datums = is_group_by_col ? group_by_cell_->get_group_by_col_datums() : datum_infos_.at(i).datum_ptr_;
    ObGroupByAggIdxArray &agg_idxs = group_by_agg_idxs_.at(i);
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_idxs.count(); ++i) {
      const int32_t agg_idx = agg_idxs.at(i);
      if (OB_FAIL(group_by_cell_->eval_batch(col_datums, count, agg_idx, is_group_by_col, false, ref_offset))) {
        LOG_WARN("Failed to eval batch", K(ret));
      }
    }
  }
  return ret;
}

int ObCGGroupByScanner::locate_micro_index(const ObCSRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGGroupByScanner not init", K(ret));
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

    if (OB_FAIL(ret) || end_of_scan()) {
    } else if (OB_FAIL(index_prefetcher_.locate(query_index_range_, nullptr))) {
      LOG_WARN("Fail to locate range", K(ret), K_(query_index_range), K_(current));
    } else if (OB_FAIL(index_prefetcher_.prefetch())) {
      LOG_WARN("Fail to prefetch", K(ret));
    }
  }
  LOG_TRACE("[COLUMNSTORE] CGGroupByScanner locate micro index", K(ret), "tablet_id", iter_param_->tablet_id_, "cg_idx", iter_param_->cg_idx_,
            "type", get_type(), K(range));
  return ret;
}

}
}
