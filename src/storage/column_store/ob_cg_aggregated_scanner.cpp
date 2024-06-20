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

namespace oceanbase
{
namespace storage
{

ObCGAggregatedScanner::ObCGAggregatedScanner()
  : ObCGRowScanner(),
    need_access_data_(false),
    is_agg_finished_(false),
    cg_agg_cells_(),
    cur_processed_row_count_(0)
{}

ObCGAggregatedScanner::~ObCGAggregatedScanner()
{
  reset();
}

void ObCGAggregatedScanner::reset()
{
  ObCGRowScanner::reset();
  need_access_data_ = false;
  is_agg_finished_ = false;
  cg_agg_cells_.reset();
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
  } else if (OB_UNLIKELY(nullptr == iter_param.output_exprs_ ||
                         0 == iter_param.output_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregated expr count", K(ret), KPC(iter_param.output_exprs_));
  } else if (OB_FAIL(check_need_access_data(iter_param,
                                            static_cast<ObAggregatedStore*>(access_ctx.block_row_store_)))) {
    LOG_WARN("Fail to check need access data", K(ret));
  } else if (need_access_data_) {
    if (OB_FAIL(ObCGRowScanner::init(iter_param, access_ctx, wrapper))) {
      LOG_WARN("Fail to init cg scanner", K(ret));
    } else if (iter_param.enable_skip_index()) {
      prefetcher_.set_cg_agg_cells(cg_agg_cells_);
    }
  }
  if (OB_SUCC(ret)) {
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
    if (!need_access_data_ || check_agg_finished()) {
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
  } else if (check_agg_finished()) {
    ret = OB_ITER_END;
  } else if (!need_access_data_) {
    if (OB_FAIL(cg_agg_cells_.process(*iter_param_, *access_ctx_, 0/*col_idx*/, nullptr/*reader*/, nullptr/*row_ids*/, cur_processed_row_count_))) {
      LOG_WARN("Fail to process agg cells", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObCGRowScanner::get_next_rows(count, capacity, 0/*datum_offset*/))) {
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
    ret = OB_ERR_UNEXPECTED;
  } else {
    count = cur_processed_row_count_;
    cur_processed_row_count_ = 0;
  }
  return ret;
}

int ObCGAggregatedScanner::inner_fetch_rows(const int64_t row_cap, const int64_t datum_offset)
{
  UNUSED(datum_offset);
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_scanner_->get_aggregate_result(0/*col_idx*/, row_ids_, row_cap, cg_agg_cells_))) {
    LOG_WARN("Fail to get aggregate result", K(ret));
  }
  return ret;
}

int ObCGAggregatedScanner::check_need_access_data(const ObTableIterParam &iter_param, ObAggregatedStore *agg_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_param.output_exprs_->count(); ++i) {
    ObAggCell *cell = nullptr;
    if (OB_FAIL(agg_store->get_agg_cell(iter_param.output_exprs_->at(i), cell))) {
      LOG_WARN("Fail to get agg cell", K(ret));
    } else if (OB_FAIL(cg_agg_cells_.add_agg_cell(cell))) {
      LOG_WARN("Fail to push back", K(ret));
    } else if (!need_access_data_) {
       need_access_data_ = cell->need_access_data();
    }
  }
  return ret;
}

bool ObCGAggregatedScanner::check_agg_finished()
{
  if (is_agg_finished_) {
  } else {
    is_agg_finished_ = cg_agg_cells_.check_finished();
  }
  return is_agg_finished_;
}

}
}
