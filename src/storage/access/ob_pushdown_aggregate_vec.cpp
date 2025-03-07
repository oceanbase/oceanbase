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

#define USING_LOG_PREFIX STORAGE

#include "ob_pushdown_aggregate_vec.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper{
int init_count_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                         ObIAllocator &allocator, IAggregate *&agg);
int init_min_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg);
int init_max_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg);
int init_sum_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg,
                       int32 *tmp_res_size = NULL);
int init_approx_count_distinct_synopsis_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                                  ObIAllocator &allocator, IAggregate *&agg);
// TODO@fengshang, need to support sum_opnsize vec 2.0
int init_sum_opnsize_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                               ObIAllocator &allocator, IAggregate *&agg);
int init_rb_build_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                            ObIAllocator &allocator, IAggregate *&agg);
}
}
}
namespace storage
{
using namespace aggregate;
ObAggCellVec::ObAggCellVec(const int64_t agg_idx, const ObAggCellVecBasicInfo &basic_info, common::ObIAllocator &allocator)
  : ObAggCellBase(allocator),
    agg_idx_(agg_idx),
    basic_info_(basic_info),
    aggregate_(nullptr),
    padding_allocator_("ObStorageAgg", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  result_datum_.set_null();
  default_datum_.set_nop();
}

ObAggCellVec::~ObAggCellVec()
{
  reset();
}

int ObAggCellVec::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMaxAggCellVec has been inited", K(ret));
  } else {
    switch (agg_type_) {
      case PD_COUNT: {
        if (OB_FAIL(helper::init_count_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init Count aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_MIN: {
        if (OB_FAIL(helper::init_min_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init MIN aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_MAX: {
        if (OB_FAIL(helper::init_max_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init MAX aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_HLL: {
        if (OB_FAIL(helper::init_approx_count_distinct_synopsis_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init HLL aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_SUM_OP_SIZE: {
        if (OB_FAIL(helper::init_sum_opnsize_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init SumOpNSize aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_SUM: {
        if (OB_FAIL(helper::init_sum_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init SUM aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      case PD_RB_BUILD: {
        if (OB_FAIL(helper::init_rb_build_aggregate(basic_info_.agg_ctx_, agg_idx_, allocator_, aggregate_))) {
          LOG_WARN("Failed to init rb build aggregate", K(ret), K_(agg_idx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected aggregate type", K(ret), K_(agg_type));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(aggregate_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregate is null", K(ret), KP_(aggregate));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObAggCellVec::reset()
{
  ObAggCellBase::reset();
  if (nullptr != aggregate_) {
    aggregate_->destroy();
    allocator_.free(aggregate_);
    aggregate_ = nullptr;
  }
  padding_allocator_.reset();
  default_datum_.set_nop();
}

void ObAggCellVec::reuse()
{
  ObAggCellBase::reuse();
  padding_allocator_.reuse();
}

int ObAggCellVec::eval(
    blocksstable::ObStorageDatum &datum,
    const int64_t row_count,
    const int64_t agg_row_idx/*0*/)
{
  UNUSED(row_count);
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments to aggregate one row", K(ret), K(row));
  } else if (datum.is_null()) {
  } else if (OB_FAIL(pad_column_if_need(datum))) {
    LOG_WARN("Failed to pad column", K(ret), KPC(this));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    if (OB_FAIL(aggregate_->add_one_row(basic_info_.agg_ctx_, 0, 1, datum.is_null(),
                                      datum.ptr_, datum.len_, agg_idx_, agg_cell))) {
      LOG_WARN("Failed to add one row in aggregate", K(ret), K_(agg_idx), K(datum), KP(agg_cell));
    }
  }
  LOG_DEBUG("[PD_AGGREGATE] aggregate one row", K(ret), K(datum), K(row_count), K(agg_row_idx), KPC(this));
  return ret;
}

int ObAggCellVec::eval_batch(
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t col_offset,
    const int32_t *row_ids,
    const int64_t row_count,
    const int64_t row_offset, /*0*/
    const int64_t agg_row_idx /*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  const ObBatchRows &brs = basic_info_.brs_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == row || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to aggregate batch rows", K(ret), K(row), K(row_count));
  } else if (nullptr != reader && can_pushdown_decoder(reader, col_offset, row_ids, row_count)) {
    if (OB_FAIL(reader->get_aggregate_result(col_offset, row_ids, row_count, *this))) {
      LOG_WARN("Failed to get aggregate result", K(ret));
    }
  } else if (OB_LIKELY(brs.size_ > 0)) {
    sql::EvalBound bound(brs.size_, row_offset, row_offset + row_count, brs.all_rows_active_);
    CK(OB_NOT_NULL(brs.skip_));
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    if (OB_FAIL(aggregate_->add_batch_rows(basic_info_.agg_ctx_, agg_idx_, *brs.skip_, bound, agg_cell))) {
      LOG_WARN("Failed to add batch rows in max cell", K(ret), KP(agg_cell));
    }
  }
  LOG_DEBUG("[PD_AGGREGATE] aggregate batch rows", K(ret), K(row_count), K(row_offset), K(agg_row_idx),
                K(reader), K(col_offset), K(row_ids), K(brs), KPC(this));
  return ret;
}

int ObAggCellVec::eval_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null compact row", K(ret), K(row));
  } else {
    if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret), K(is_lob_col()), K(index_info));
    } else if (OB_UNLIKELY(skip_index_datum_.is_null())){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected skip index datum is null", K(ret), K(index_info));
    } else if (OB_FAIL(eval(skip_index_datum_, 1))) {
      LOG_WARN("Failed to eval skip index datum", K(ret), K_(skip_index_datum));
    }
  }
  LOG_DEBUG("[PD_AGGREGATE] aggregate index info", K(ret), K_(skip_index_datum), K(is_cg), K(agg_row_idx), KPC(this));
  return ret;
}

int ObAggCellVec::eval_batch_in_group_by(
    common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == refs || distinct_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(refs), K(distinct_cnt));
  } else {
    const ObBatchRows &brs = basic_info_.brs_;
    const sql::ObAggrInfo &agg_info = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_);
    const bool read_distinct_val = is_group_by_col || is_default_datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const uint32_t distinct_ref = refs[i];
      const uint32_t dictinct_datum_offset = is_default_datum ? 0 : refs[i];
      if (read_distinct_val) {
        blocksstable::ObStorageDatum storage_datum;
        storage_datum.shallow_copy_from_datum(datums[dictinct_datum_offset]);
        if (OB_FAIL(eval(storage_datum, 1, distinct_ref))) {
          LOG_WARN("Failed to eval one datum", K(ret), K(storage_datum));
        }
      } else if (OB_FAIL(eval_batch(nullptr, basic_info_.col_offset_, nullptr, 1, i, distinct_ref))) {
        LOG_WARN("Failed to eval one row in group by", K(ret), K(i), K(distinct_ref));
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN] eval batch rows in group by pushdown", K(ret), K(datums), K(refs),
                K(count), K(distinct_cnt), K(is_group_by_col), K(is_default_datum), KPC(this));
  }
  return ret;
}

int ObAggCellVec::collect_result(
    const bool fill_output,
    const sql::ObExpr* group_by_col_expr,
    const int32_t output_start_idx,
    const int32_t row_start_idx,
    const int32_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObCompactRow **rows = const_cast<const ObCompactRow **>(basic_info_.rows_);
  const sql::ObAggrInfo &agg_info = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_);
  sql::ObExpr *agg_expr = agg_info.expr_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == rows || nullptr == agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments to collect aggregate result", K(ret), KP(rows), KP(agg_expr));
  } else if (OB_FAIL(aggregate_->collect_batch_group_results(basic_info_.agg_ctx_, agg_idx_,
                output_start_idx, batch_size, rows, basic_info_.row_meta_, row_start_idx, false))) {
    LOG_WARN("Failed to collect results", K(ret));
  } else {
    sql::ObEvalCtx &eval_ctx = basic_info_.agg_ctx_.eval_ctx_;
    sql::ObExpr *output_expr = agg_info.param_exprs_.count() > 0 ? agg_info.param_exprs_.at(0) : nullptr;
    if (fill_output && output_expr != nullptr
        && OB_FAIL(fill_output_expr_if_need(output_expr, group_by_col_expr, eval_ctx, batch_size))) {
      LOG_WARN("Failed to fill output expr", K(ret), K(fill_output), K(group_by_col_expr), K(output_expr), K(batch_size));
    } else {
      sql::ObEvalInfo &eval_info = agg_info.expr_->get_eval_info(eval_ctx);
      eval_info.evaluated_ = true;
      if (nullptr != output_expr) {
        sql::ObEvalInfo &output_eval_info = output_expr->get_eval_info(eval_ctx);
        output_eval_info.evaluated_ = true;
      }
    }
  }
  LOG_DEBUG("[PD_AGGREGATE] collect result", K(ret), K(fill_output), K(group_by_col_expr),
              K(output_start_idx), K(row_start_idx), K(batch_size), KPC(this));
  return ret;
}

int ObAggCellVec::fill_output_expr_if_need(
    sql::ObExpr *output_expr,
    const sql::ObExpr *group_by_col_expr,
    sql::ObEvalCtx &eval_ctx,
    const int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == group_by_col_expr || output_expr != group_by_col_expr) {
    if (OB_FAIL(storage::init_expr_vector_header(*output_expr, eval_ctx,
              eval_ctx.max_batch_size_, output_expr->get_default_res_format()))) {
      LOG_WARN("Failed to init vector header", K(ret), KPC(output_expr), K_(eval_ctx.max_batch_size));
    } else {
      for (int32_t i = 0; i < batch_size; ++i) {
        output_expr->get_vector(eval_ctx)->set_null(i);
      }
    }
  }
  LOG_DEBUG("check need fill output expr", K(output_expr), K(group_by_col_expr), KPC(this));
  return ret;
}

// TODO (wenye): need to optimize for min/max, because eval_batch() is deep copy
// maybe project column to aggregate expr again.
// [start_offset, end_offset)
int ObAggCellVec::copy_output_rows(const int32_t start_offset, const int32_t end_offset)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_offset; OB_SUCC(ret) && i < end_offset; ++i) {
    if (OB_FAIL(eval_batch(nullptr, basic_info_.col_offset_, nullptr, 1, i, i))) {
      LOG_WARN("Failed to eval one row", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(collect_result(false/*fill_output*/,
                                             nullptr/*group_by_col_expr*/,
                                             start_offset,
                                             start_offset,
                                             end_offset - start_offset))) {
    LOG_WARN("Failed to collect result", K(ret), K(start_offset), K(end_offset));
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN] copy rows in group by pushdown", K(ret), K(start_offset), K(end_offset), KPC(this));
  return ret;
}

int ObAggCellVec::can_use_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const int32_t col_index,
    bool &can_agg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else {
    if (index_info.has_agg_data() && can_use_index_info()) {
      if (OB_FAIL(read_agg_datum(index_info, col_index))) {
        LOG_WARN("Failed to read agg datum", K(ret), K_(basic_info), K(col_index), K(index_info));
      } else {
        can_agg = !skip_index_datum_.is_null();
      }
    } else {
      can_agg = false;
    }
  }
  return ret;
}

int ObAggCellVec::read_agg_datum(
    const blocksstable::ObMicroIndexInfo &index_info,
    const int32_t col_index)
{
  int ret = OB_SUCCESS;
  if (nullptr == agg_row_reader_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObAggRowReader)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc agg row reader", K(ret));
    } else {
      agg_row_reader_ = new (buf) blocksstable::ObAggRowReader();
    }
  }
  if (OB_SUCC(ret)) {
    skip_index_datum_.reuse();
    skip_index_datum_.set_null();
    blocksstable::ObSkipIndexColMeta meta;
    // TODO: @baichangmin.bcm fix col_index in cg, use 0 temporarily
    meta.col_idx_ = col_index;
    switch (agg_type_) {
      case PD_COUNT:
      case PD_SUM_OP_SIZE: {
        meta.col_type_ = blocksstable::SK_IDX_NULL_COUNT;
        break;
      }
      case PD_MIN: {
        meta.col_type_ = blocksstable::SK_IDX_MIN;
        break;
      }
      case PD_MAX: {
        meta.col_type_ = blocksstable::SK_IDX_MAX;
        break;
      }
      case PD_SUM: {
        meta.col_type_ = blocksstable::SK_IDX_SUM;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected agg type", K(agg_type_));
      }
    }
    if (OB_SUCC(ret)) {
      agg_row_reader_->reset();
      if (OB_FAIL(agg_row_reader_->init(index_info.agg_row_buf_, index_info.agg_buf_size_))) {
        LOG_WARN("Fail to init aggregate row reader", K(ret));
      } else if (OB_FAIL(agg_row_reader_->read(meta, skip_index_datum_))) {
        LOG_WARN("Failed read aggregate row", K(ret), K(meta));
      }
    }
  }
  return ret;
}

int ObAggCellVec::pad_column_if_need(blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  padding_allocator_.reuse();
  if (!basic_info_.need_padding()) {
  } else if (OB_FAIL(pad_column(basic_info_.col_param_->get_meta_type(),
                                basic_info_.col_param_->get_accuracy(),
                                padding_allocator_, datum))) {
    LOG_WARN("Fail to pad column", K(ret), K_(basic_info), KPC(this));
  }
  return ret;
}

int ObAggCellVec::get_def_datum(const blocksstable::ObStorageDatum *&default_datum)
{
  int ret = OB_SUCCESS;
  if (!default_datum_.is_nop()) {
    default_datum = &default_datum_;
  } else {
    const ObObj &def_cell = basic_info_.col_param_->get_orig_default_value();
    if (!def_cell.is_nop_value()) {
      if (OB_FAIL(default_datum_.from_obj_enhance(def_cell))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
      } else if (def_cell.is_lob_storage() && !def_cell.is_null()) {
        // lob def value must have no lob header when not null, should add lob header for default value
        ObString data = default_datum_.get_string();
        ObString out;
        if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
          LOG_WARN("failed to fill lob header for column.", K(ret), K(def_cell), K(data));
        } else {
          default_datum_.set_string(out);
        }
      }
      default_datum = &default_datum_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, virtual column is not supported", K(ret), K(basic_info_.col_offset_));
    }
  }
  return ret;
}

ObCountAggCellVec::ObCountAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator,
    const bool exclude_null)
      : ObAggCellVec(agg_idx, basic_info, allocator),
        exclude_null_(exclude_null)
{
  agg_type_ = PD_COUNT;
}

int ObCountAggCellVec::eval(
    blocksstable::ObStorageDatum &datum,
    const int64_t row_count,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null row", K(ret));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    if (!exclude_null_) {
      data += row_count;
    } else if (!datum.is_null()) {
      data += row_count;
    }
    LOG_DEBUG("[PD_COUNT_AGGREGATE] aggregate one row", K(ret), K(datum),
                K(row_count), K(agg_row_idx), K(data), KPC(this));
  }
  return ret;
}

int ObCountAggCellVec::eval_batch(
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t col_offset,
    const int32_t *row_ids,
    const int64_t row_count,
    const int64_t row_offset, /*0*/
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(row_count < 0 || nullptr == row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to aggregate batch rows", K(ret), K(row_count), K(row));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    if (!need_get_row_ids()) {
      data += row_count;
    } else if (nullptr == reader) { // row scan or group by pushdown
      if (OB_FAIL(ObAggCellVec::eval_batch(reader, col_offset, row_ids, row_count, row_offset, agg_row_idx))) {
        LOG_WARN("Failed to aggregate batch rows", K(ret));
      }
    } else { // block scan and only has count aggregate in one column
      int64_t valid_row_count = 0;
      if (OB_ISNULL(row_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected, row_ids is null", K(ret), KPC(this), K(row_count));
      } else if (OB_FAIL(reader->get_row_count(col_offset, row_ids, row_count, false, basic_info_.col_param_, valid_row_count))) {
        LOG_WARN("Failed to get row count from micro block decoder", K(ret), KPC(this), K(row_count));
      } else {
        data += valid_row_count;
      }
    }
    LOG_DEBUG("[PD_COUNT_AGGREGATE] aggregate eval batch rows", K(ret), K(col_offset), K(row_count),
                K(row_offset), K(agg_row_idx), K(data), K(reader), K(row_ids), KPC(this));
  }
  return ret;
}

int ObCountAggCellVec::eval_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row) || OB_UNLIKELY(!is_cg && (!index_info.can_blockscan(is_lob_col()) ||
                                                      index_info.is_left_border() || index_info.is_right_border()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, row must not be null or the micro index info must can blockscan and not border",
                K(ret), K(row), K(is_cg), K(index_info));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    if (!exclude_null_) {
      data += index_info.get_row_count();
    } else if (OB_UNLIKELY(skip_index_datum_.is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null skip index datum", K(ret), K(index_info));
    } else {
      data += index_info.get_row_count() - skip_index_datum_.get_int();
    }
    LOG_DEBUG("[PD_COUNT_AGGREGATE] aggregate index info", K(ret), K(data), K(is_cg), K(agg_row_idx),
                K(index_info.get_row_count()), K(skip_index_datum_.get_int()), KPC(this));
  }
  return ret;
}

#define ADD_ONE_ROW(distinct_ref)                                                                                   \
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[distinct_ref]->get_extra_payload(basic_info_.row_meta_));  \
  char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);                              \
  int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);                                                           \
  data += 1;

int ObCountAggCellVec::eval_batch_in_group_by(
    common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == refs || distinct_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(refs), K(distinct_cnt));
  } else if (!exclude_null_) {
    for (int i = 0; i < count; ++i) {
      ADD_ONE_ROW(refs[i]);
    }
  } else if (is_group_by_col) {
    for (int64_t i = 0; i < count; ++i) {
      if (!datums[refs[i]].is_null()) {
        ADD_ONE_ROW(refs[i]);
      }
    }
  } else if (OB_UNLIKELY(is_default_datum)) {
    if (!datums[0].is_null()) {
      for (int64_t i = 0; i < count; ++i) {
        ADD_ONE_ROW(refs[i]);
      }
    }
  } else {
    sql::ObExpr *project_expr = get_project_expr();
    sql::ObEvalCtx &eval_ctx = basic_info_.agg_ctx_.eval_ctx_;
    if (OB_ISNULL(project_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected aggregate or project expr", KPC(project_expr));
    } else {
      for (int64_t i = 0; i < count; ++i) {
        if (!project_expr->get_vector(eval_ctx)->is_null(i)) {
          ADD_ONE_ROW(refs[i]);
        }
      }
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN] eval batch rows in group by pushdown", K(ret), K(datums), K(refs), K(count),
                K(distinct_cnt), K(is_group_by_col), K(is_default_datum), KPC(this));
  return ret;
}
#undef ADD_ONE_ROW

int ObCountAggCellVec::copy_output_rows(const int32_t start_offset, const int32_t end_offset)
{
  int ret = OB_SUCCESS;
  sql::ObExpr *agg_expr = get_agg_expr();
  sql::ObEvalCtx &eval_ctx = basic_info_.agg_ctx_.eval_ctx_;
  if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate or output expr", K(ret), KPC(agg_expr));
  } else if (exclude_null_) {
    sql::ObExpr *project_expr = get_project_expr();
    if (OB_ISNULL(project_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected aggregate or project expr", K(ret), KPC(project_expr));
    } else {
      if (lib::is_oracle_mode()) {
        for (int64_t i = start_offset; i < end_offset; ++i) {
          project_expr->get_vector(eval_ctx)->is_null(i)
            ? agg_expr->get_vector(eval_ctx)->set_number(i, common::number::ObNumber::get_zero())
            : agg_expr->get_vector(eval_ctx)->set_number(i, common::number::ObNumber::get_positive_one());
        }
      } else {
        for (int64_t i = start_offset; i < end_offset; ++i) {
          project_expr->get_vector(eval_ctx)->is_null(i)
            ? agg_expr->get_vector(eval_ctx)->set_int(i, 0)
            : agg_expr->get_vector(eval_ctx)->set_int(i, 1);
        }
      }
    }
  } else if (lib::is_oracle_mode()) {
    for (int64_t i = start_offset; i < end_offset; ++i) {
      agg_expr->get_vector(eval_ctx)->set_number(i, common::number::ObNumber::get_positive_one());
    }
  } else {
    for (int64_t i = start_offset; i < end_offset; ++i) {
      agg_expr->get_vector(eval_ctx)->set_int(i, 1);
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN] copy rows in group by pushdown", K(ret), K(start_offset), K(end_offset), KPC(this));
  return ret;
}

int ObCountAggCellVec::can_use_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const int32_t col_index,
    bool &can_agg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (!exclude_null_) {
    can_agg = true;
  } else {
    if (index_info.has_agg_data() && ObAggCellVec::can_use_index_info()) {
      if (OB_FAIL(read_agg_datum(index_info, col_index))) {
        LOG_WARN("Failed to read agg datum", K(ret), K_(basic_info), K(col_index), K(index_info));
      } else {
        can_agg = !skip_index_datum_.is_null();
      }
    } else {
      can_agg = false;
    }
  }
  return ret;
}

ObMaxAggCellVec::ObMaxAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator)
      : ObAggCellVec(agg_idx, basic_info, allocator)
{
  agg_type_ = PD_MAX;
}

ObMinAggCellVec::ObMinAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator)
      : ObAggCellVec(agg_idx, basic_info, allocator)
{
  agg_type_ = PD_MIN;
}

ObSumAggCellVec::ObSumAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator)
      : ObAggCellVec(agg_idx, basic_info, allocator),
        cast_datum_()
{
  agg_type_ = PD_SUM;
}

void ObSumAggCellVec::reuse()
{
  ObAggCellVec::reuse();
  cast_datum_.reuse();
  cast_datum_.set_null();
}

int ObSumAggCellVec::eval(
    blocksstable::ObStorageDatum &datum,
    const int64_t row_count,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSumAggCellVec not inited", K(ret));
  } else if (datum.is_null()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL(ObAggCellVec::eval(datum, i, agg_row_idx))) {
        LOG_WARN("Failed to sum single value", K(ret), K(datum));
      }
    }
  }
  return ret;
}

int ObSumAggCellVec::eval_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  blocksstable::ObStorageDatum *eval_datum = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSumAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid null compact row", K(ret));
  } else if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret), K(is_lob_col()), K(index_info));
  } else {
    const ObExpr *agg_expr = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_).expr_;
    const ObObjTypeClass res_tc = ob_obj_type_class(agg_expr->datum_meta_.type_);
    if (ObObjTypeClass::ObDecimalIntTC == res_tc) {
      // cast number to decimal
      int16_t out_scale = agg_expr->datum_meta_.scale_;
      ObDecimalIntBuilder tmp_alloc;
      ObDecimalInt *decint = nullptr;
      int32_t int_bytes = 0;
      const number::ObNumber nmb(skip_index_datum_.get_number());
      const ObScale in_scale = nmb.get_scale();
      const ObPrecision out_prec = agg_expr->datum_meta_.precision_;
      int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
      if (OB_FAIL(wide::from_number(nmb, tmp_alloc, in_scale, decint, int_bytes))) {
        LOG_WARN("from_number failed", K(ret), K(out_scale));
      } else if (sql::ObDatumCast::need_scale_decimalint(in_scale, int_bytes, out_scale, out_bytes)) {
        // upcasting
        ObDecimalIntBuilder res_val;
        if (OB_FAIL(sql::ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, out_prec,
                                            agg_expr->extra_, res_val))) {
          LOG_WARN("scale decimal int failed", K(ret), K(in_scale), K(out_scale));
        } else {
          cast_datum_.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
        }
      } else {
        cast_datum_.set_decimal_int(decint, int_bytes);
      }
      eval_datum = &cast_datum_;
    } else {
      eval_datum = &skip_index_datum_;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(eval_datum->is_null())){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected skip index datum is null", K(ret), K(index_info));
    } else if (OB_FAIL(eval(*eval_datum, 1))) {
      LOG_WARN("Failed to eval skip index datum", K(ret), K_(skip_index_datum));
    }
  }
  LOG_DEBUG("[PD_AGGREGATE] aggregate index info", K(ret), KPC(eval_datum), K(is_cg), K(agg_row_idx), KPC(this));
  return ret;
}

ObHyperLogLogAggCellVec::ObHyperLogLogAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common:: ObIAllocator &allocator)
      : ObAggCellVec(agg_idx, basic_info, allocator)
{
  agg_type_ = PD_HLL;
}

ObSumOpNSizeAggCellVec::ObSumOpNSizeAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator,
    const bool exclude_null)
      : ObAggCellVec(agg_idx, basic_info, allocator),
        op_nsize_(0),
        exclude_null_(exclude_null)
{
  agg_type_ = PD_SUM_OP_SIZE;
}

int ObSumOpNSizeAggCellVec::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAggCellVec::init())) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else if (OB_FAIL(set_op_nsize())) {
    LOG_WARN("Failed to get op size", K(ret));
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::set_op_nsize()
{
  int ret = OB_SUCCESS;
  ObObjDatumMapType type = OBJ_DATUM_MAPPING_MAX;
  const sql::ObExpr *proj_expr = get_project_expr();
  if (OB_ISNULL(proj_expr) || OB_UNLIKELY(T_REF_COLUMN != proj_expr->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is null", K(ret), KPC(proj_expr));
  } else if (FALSE_IT(type = proj_expr->obj_datum_map_)) {
  } else if (OB_UNLIKELY(type >= common::OBJ_DATUM_MAPPING_MAX || type <= common::OBJ_DATUM_NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type", K(ret), K(type));
  } else if (is_fixed_length_type()) {
    op_nsize_ = sizeof(ObDatum) + common::ObDatum::get_reserved_size(type);
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::get_datum_op_nsize(blocksstable::ObStorageDatum &datum, int64_t &length)
{
  int ret = OB_SUCCESS;
  const ObExpr *proj_expr = get_project_expr();
  if (!is_lob_col() || datum.is_null()) {
    length = sizeof(ObDatum) + datum.len_;
  } else if (OB_ISNULL(proj_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid agg expr", K(ret), KPC(proj_expr));
  } else {
    ObLobLocatorV2 locator(datum.get_string(), proj_expr->obj_meta_.has_lob_header());
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      LOG_WARN("Failed to get lob data byte len", K(ret), K(locator));
    } else {
      length = sizeof(ObDatum) + lob_data_byte_len;
    }
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::eval(
    blocksstable::ObStorageDatum &datum,
    const int64_t row_count,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null row", K(ret));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    int64_t length = 0;
    if (OB_FAIL(pad_column_if_need(datum))) {
      LOG_WARN("Failed to pad column", K(ret), K(datum));
    } else if (OB_FAIL(get_datum_op_nsize(datum, length))) {
      LOG_WARN("Failed to get datum length", K(ret), K(datum));
    } else {
      data += length * row_count;
    }
    LOG_DEBUG("[PD_SUMOPNSIZE_AGGREGATE] aggregate one row", K(ret), K(datum),
                K(row_count), K(agg_row_idx), K(data), KPC(this));
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::eval_batch(
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t col_offset,
    const int32_t *row_ids,
    const int64_t row_count,
    const int64_t row_offset, /*0*/
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(row_count < 0 || nullptr == row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to aggregate batch rows", K(ret), K(row_count), KP(row), KPC(this));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    if (!need_get_row_ids()) {
      data += row_count * op_nsize_;
    } else if (nullptr == reader || !is_fixed_length_type()) { // nullptr == reader means row scan or group by pushdown
      if (OB_FAIL(ObAggCellVec::eval_batch(reader, col_offset, row_ids, row_count, row_offset, agg_row_idx))) {
        LOG_WARN("Failed to aggregate batch rows", K(ret));
      }
    } else {
      int64_t valid_row_count = 0;
      if (OB_FAIL(reader->get_row_count(col_offset, row_ids, row_count, false, basic_info_.col_param_, valid_row_count))) {
        LOG_WARN("Failed to get row count from micro block reader", K(ret), K(row_count), KPC(this));
      } else {
        data += (row_count - valid_row_count) * sizeof(ObDatum) + valid_row_count * op_nsize_;
      }
    }
    LOG_DEBUG("[PD_SUMOPNSIZE_AGGREGATE] aggregate eval batch rows", K(ret), K(col_offset), K(row_count),
                K(row_offset), K(agg_row_idx), K(data), K(reader), K(row_ids), KPC(this));
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::eval_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg,
    const int64_t agg_row_idx/*0*/)
{
  int ret = OB_SUCCESS;
  AggrRowPtr row = static_cast<char *>(basic_info_.rows_[agg_row_idx]->get_extra_payload(basic_info_.row_meta_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCountAggCellVec not inited", K(ret));
  } else if (OB_ISNULL(row) || OB_UNLIKELY(!is_cg && (!index_info.can_blockscan(is_lob_col()) ||
                                                      index_info.is_left_border() || index_info.is_right_border()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, row must not be null or the micro index info must can blockscan and not border",
                K(ret), K(row), K(is_cg), K(index_info));
  } else {
    char *agg_cell = basic_info_.agg_ctx_.row_meta().locate_cell_payload(agg_idx_, row);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    if (!exclude_null_) {
      data += index_info.get_row_count() * op_nsize_;
    } else if (OB_UNLIKELY(skip_index_datum_.is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null skip index datum", K(ret), K(index_info));
    } else {
      const int64_t null_count = skip_index_datum_.get_int();
      data += (index_info.get_row_count() - null_count) * op_nsize_ + null_count * sizeof(ObDatum);
    }
    LOG_DEBUG("[PD_SUMOPNSIZE_AGGREGATE] aggregate index info", K(ret), K(data), K(is_cg), K(agg_row_idx),
                K(index_info.get_row_count()), K(skip_index_datum_.get_int()), KPC(this));
  }
  return ret;
}

int ObSumOpNSizeAggCellVec::can_use_index_info(
    const blocksstable::ObMicroIndexInfo &index_info,
    const int32_t col_index,
    bool &can_agg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not init", K(ret));
  } else if (!exclude_null_ && is_fixed_length_type()) {
    can_agg = true;
  } else {
    if (index_info.has_agg_data() && can_use_index_info()) {
      if (OB_FAIL(read_agg_datum(index_info, col_index))) {
        LOG_WARN("Failed to read agg datum", K(ret), K_(basic_info), K(col_index), K(index_info));
      } else {
        can_agg = !skip_index_datum_.is_null();
      }
    } else {
      can_agg = false;
    }
  }
  return ret;
}

ObRbBuildAggCellVec::ObRbBuildAggCellVec(
    const int64_t agg_idx,
    const ObAggCellVecBasicInfo &basic_info,
    common::ObIAllocator &allocator)
      : ObAggCellVec(agg_idx, basic_info, allocator)
{
  agg_type_ = PD_RB_BUILD;
}

int ObPDAggVecFactory::alloc_cell(
    const ObAggCellVecBasicInfo &basic_info,
    ObAggCellVec *&agg_cell,
    const int64_t agg_idx,
    const bool exclude_null)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  agg_cell = nullptr;
  const sql::ObExprOperatorType type = basic_info.agg_ctx_.aggr_infos_.at(agg_idx).real_aggr_type_;
  switch (type) {
    case T_FUN_COUNT: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCountAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObCountAggCellVec(agg_idx, basic_info, allocator_, exclude_null))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for COUNT agg cell", K(ret));
      }
      break;
    }
    case T_FUN_MAX: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMaxAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObMaxAggCellVec(agg_idx, basic_info, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for MAX agg cell", K(ret));
      }
      break;
    }
    case T_FUN_MIN: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMinAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObMinAggCellVec(agg_idx, basic_info, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for MIN agg cell", K(ret));
      }
      break;
    }
    case T_FUN_SUM: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSumAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObSumAggCellVec(agg_idx, basic_info, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for SUM agg cell", K(ret));
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObHyperLogLogAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObHyperLogLogAggCellVec(agg_idx, basic_info, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for HyperLogLog agg cell", K(ret));
      }
      break;
    }
    case T_FUN_SUM_OPNSIZE: {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSumOpNSizeAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObSumOpNSizeAggCellVec(agg_idx, basic_info, allocator_, exclude_null))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for SumOpNSize agg cell", K(ret));
      }
      break;
    }
    case T_FUN_SYS_RB_BUILD_AGG:
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObRbBuildAggCellVec))) ||
          OB_ISNULL(agg_cell = new (buf) ObRbBuildAggCellVec(agg_idx, basic_info, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for rb build agg cell", K(ret));
      }
      break;
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Not supported aggregate type", K(ret), K(type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(agg_cell->init())) {
    LOG_WARN("Failed to init agg cell", K(ret));
  }
  return ret;
}

void ObPDAggVecFactory::release(common::ObIArray<ObAggCellVec *> &agg_cells)
{
  for (int64_t i = 0; i < agg_cells.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells.at(i);
    if (OB_NOT_NULL(agg_cell)) {
      agg_cell->~ObAggCellVec();
      allocator_.free(agg_cell);
    }
  }
  agg_cells.reset();
}

ObGroupByCellVec::ObGroupByCellVec(
    const int64_t batch_size,
    sql::ObEvalCtx &eval_ctx,
    sql::ObBitVector *skip_bit,
    common::ObIAllocator &allocator)
    : ObGroupByCellBase(batch_size, allocator),
      pd_agg_ctx_(batch_size, eval_ctx, skip_bit, allocator),
      group_by_col_datum_buf_(nullptr),
      tmp_group_by_datum_buf_(nullptr),
      agg_cells_(),
      agg_cell_factory_vec_(allocator),
      eval_ctx_(eval_ctx),
      tmp_datum_allocator_("PDGroupBy", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      group_by_datum_allocator_("PDGroupBy", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObGroupByCellVec::~ObGroupByCellVec()
{
  reset();
}

void ObGroupByCellVec::reset()
{
  ObGroupByCellBase::reset();
  agg_cell_factory_vec_.release(agg_cells_);
  if (OB_NOT_NULL(group_by_col_datum_buf_)) {
    group_by_col_datum_buf_->reset();
    group_by_datum_allocator_.free(group_by_col_datum_buf_);
    group_by_col_datum_buf_ = nullptr;
  }
  if (OB_NOT_NULL(tmp_group_by_datum_buf_)) {
    tmp_group_by_datum_buf_->reset();
    tmp_datum_allocator_.free(tmp_group_by_datum_buf_);
    tmp_group_by_datum_buf_ = nullptr;
  }
  tmp_datum_allocator_.reset();
  group_by_datum_allocator_.reset();
}

void ObGroupByCellVec::reuse()
{
  ObGroupByCellBase::reuse();
  for (int64_t i = 0; i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->reuse();
  }
  pd_agg_ctx_.reuse_batch();
}

int ObGroupByCellVec::init(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObGroupByCellVec has been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == param.iter_param_.group_by_cols_project_ ||
                  0 == param.iter_param_.group_by_cols_project_->count() ||
                  nullptr == out_cols_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param.iter_param_));
  } else {
    const common::ObIArray<int32_t> &out_cols_projector = *param.iter_param_.out_cols_project_;
    const common::ObIArray<share::schema::ObColumnParam *> &out_cols_param = *param.iter_param_.get_col_params();
    group_by_col_offset_ = param.iter_param_.group_by_cols_project_->at(0);
    blocksstable::ObStorageDatum null_datum;
    null_datum.set_null();
    for (int64_t i = 0; OB_SUCC(ret) && i < param.output_exprs_->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == param.output_exprs_->at(i)->type_) {
        LOG_TRACE("Group by pushdown in batch nlj", K(ret));
        continue;
      } else if (nullptr == param.output_sel_mask_ || param.output_sel_mask_->at(i)) {
        int32_t col_offset = param.iter_param_.out_cols_project_->at(i);
        sql::ObExpr *expr = param.output_exprs_->at(i);
        if (group_by_col_offset_ == col_offset) {
          const common::ObObjMeta &obj_meta = out_cols_param.at(out_cols_projector.at(i))->get_meta_type();
          if (is_pad_char_to_full_length(context.sql_mode_) && obj_meta.is_fixed_len_char_type()) {
            group_by_col_param_ = out_cols_param.at(out_cols_projector.at(i));
          }
          group_by_col_expr_ = expr;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pd_agg_ctx_.init(param, MIN(batch_size_, USE_GROUP_BY_MAX_DISTINCT_CNT)))) {
      LOG_WARN("Failed to init agg context", K(ret), K(param));
    } else if (OB_FAIL(init_agg_cells(param, context, eval_ctx, false))) {
      LOG_WARN("Failed to init agg cells", K(ret));
    } else {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(uint32_t) * batch_size_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory", K(ret));
      } else {
        refs_buf_ = reinterpret_cast<uint32_t*>(buf);
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObGroupByCellVec::init_vector_header(
    const sql::ObExprPtrIArray *agg_exprs,
    const bool init_group_by_col)
{
  int ret = OB_SUCCESS;
  if (init_group_by_col) {
    if (OB_FAIL(storage::init_expr_vector_header(*group_by_col_expr_, eval_ctx_,
                eval_ctx_.max_batch_size_, group_by_col_expr_->get_default_res_format()))) {
      LOG_WARN("Failed to init vector for group by column output expr", KPC_(group_by_col_expr), K(eval_ctx_.max_batch_size_));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(storage::init_exprs_vector_header(agg_exprs, eval_ctx_, eval_ctx_.max_batch_size_))) {
    LOG_WARN("Failed to init vector for agg exprs", K(ret), K(eval_ctx_.max_batch_size_));
  }
  return ret;
}

int ObGroupByCellVec::eval_batch(
    common::ObDatum *datums,
    const int64_t count,
    const int32_t agg_idx,
    const bool is_group_by_col,
    const bool is_default_datum,
    const uint32_t ref_offset)
{
  int ret = OB_SUCCESS;
  const int64_t sorted_agg_idx = pd_agg_ctx_.cols_offset_map_.at(agg_idx).agg_idx_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(agg_idx >= agg_cells_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(agg_idx), K(agg_cells_.count()));
  } else if (OB_UNLIKELY(0 == distinct_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, not load distinct yet", K(ret));
  } else if (OB_FAIL(agg_cells_.at(sorted_agg_idx)->eval_batch_in_group_by(
      datums, count, refs_buf_ + ref_offset, distinct_cnt_, is_group_by_col, is_default_datum))) {
    LOG_WARN("Failed to eval batch with in group by", K(ret));
  }
  return ret;
}

int ObGroupByCellVec::copy_output_row(const int64_t batch_idx, const ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  // just shallow copy output vector to agg
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (batch_idx == 1 && OB_FAIL(init_exprs_uniform_header(iter_param.aggregate_exprs_,
                                            eval_ctx_, eval_ctx_.max_batch_size_))) {
    LOG_WARN("Failed to init uniform vector", K(ret), K(iter_param));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->copy_output_rows(batch_idx - 1, batch_idx))) {
      LOG_WARN("Failed to copy output row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_distinct_cnt(batch_idx);
  }
  return ret;
}

int ObGroupByCellVec::copy_output_rows(const int64_t batch_idx, const ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  // just shallow copy output vector to agg
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(init_vector_header(iter_param.aggregate_exprs_, false))) {
    LOG_WARN("Failed to init vector header", K(ret), K(iter_param));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->copy_output_rows(0, batch_idx))) {
      LOG_WARN("Failed to copy output row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_distinct_cnt(batch_idx);
  }
  return ret;
}

int ObGroupByCellVec::add_distinct_null_value()
{
  int ret = OB_SUCCESS;
  if (distinct_cnt_ + 1 > group_by_col_datum_buf_->get_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected distinct cnt", K(ret), K(distinct_cnt_), K(batch_size_), KPC(group_by_col_datum_buf_));
  } else {
    common::ObDatum *datums = get_group_by_col_datums_to_fill();
    datums[distinct_cnt_].set_null();
    distinct_cnt_++;
  }
  return ret;
}

int ObGroupByCellVec::extract_distinct()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(ref_cnt_));
  } else {
    common::ObDatum *group_by_col_datums = group_by_col_datum_buf_->get_datums();
    common::ObDatum *tmp_group_by_datums = tmp_group_by_datum_buf_->get_datums();
    for (int64_t i = 0; OB_SUCC(ret) && i < ref_cnt_; ++i) {
      uint32_t &ref = refs_buf_[i];
      if (OB_UNLIKELY(ref >= group_by_col_datum_buf_->get_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected ref", K(ret), K(ref), K(batch_size_));
      } else {
        int16_t &distinct_projector = distinct_projector_buf_->at(ref);
        if (-1 == distinct_projector) {
          // distinct val is not extracted yet
          if (OB_FAIL(group_by_col_datums[distinct_cnt_].from_storage_datum(tmp_group_by_datums[ref], group_by_col_expr_->obj_datum_map_))) {
            LOG_WARN("Failed to clone datum", K(ret), K(tmp_group_by_datums[ref]), K(group_by_col_expr_->obj_datum_map_));
          } else {
            distinct_projector = distinct_cnt_;
            ref = distinct_cnt_;
            distinct_cnt_++;
          }
        } else {
          // distinct val is already extracted
          ref = distinct_projector;
        }

      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN] extract distinct", K(ret), K(ref_cnt_), K(distinct_cnt_));
  }
  return ret;
}

int ObGroupByCellVec::prepare_tmp_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (nullptr == distinct_projector_buf_ &&
      OB_FAIL(new_group_by_buf((int16_t*)nullptr, 0, sizeof(int16_t), allocator_, distinct_projector_buf_))) {
    LOG_WARN("Failed to new buf", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(distinct_projector_buf_->reserve(size))) {
      LOG_WARN("Failed to reserver buf", K(ret), K(size));
    } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(size, true, tmp_datum_allocator_,
                tmp_group_by_datum_buf_, common::OBJ_DATUM_NUMBER_RES_SIZE))) {
      LOG_WARN("Failed to new tmp group by buf", K(ret), K(size));
    } else {
      distinct_projector_buf_->fill_items(-1);
      need_extract_distinct_ = true;
    }
  }
  return ret;
}

int ObGroupByCellVec::reserve_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(size, true, group_by_datum_allocator_,
              group_by_col_datum_buf_, common::OBJ_DATUM_NUMBER_RES_SIZE))) {
    LOG_WARN("Failed to prepare aggregate datum buf", K(ret), K(size));
  } else if (OB_FAIL(pd_agg_ctx_.prepare_aggregate_rows(size))) {
    LOG_WARN("Failed to prepare aggregate rows", K(ret), K(size));
  }
  return ret;
}

int ObGroupByCellVec::output_extra_group_by_result(int64_t &count, const ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (projected_cnt_ >= distinct_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(init_vector_header(iter_param.aggregate_exprs_, true))) {
    LOG_WARN("Failed to init vector header", K(ret), K(iter_param));
  } else {
    count = MIN(row_capacity_, distinct_cnt_ - projected_cnt_);
    const VectorFormat format = group_by_col_expr_->get_format(eval_ctx_);
    common::ObDatum *col_datums = group_by_col_datum_buf_->get_datums();
    for (int64_t i = 0; i < count; ++i) {
      if (VEC_DISCRETE == format) {
        static_cast<ObDiscreteFormat *>(group_by_col_expr_->get_vector(eval_ctx_))->set_datum(i, col_datums[projected_cnt_ + i]);
      } else {
        static_cast<ObFixedLengthBase *>(group_by_col_expr_->get_vector(eval_ctx_))->set_datum(i, col_datums[projected_cnt_ + i]);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
      if (OB_FAIL(agg_cells_.at(i)->collect_result(true/*fill_output*/, group_by_col_expr_, 0, projected_cnt_, count))) {
        LOG_WARN("Failed to collect result for agg cell", K(ret), K(i), K_(projected_cnt), K(count));
      }
    }
    if (OB_SUCC(ret)) {
      projected_cnt_ += count;
      if (projected_cnt_ >= distinct_cnt_) {
        ret = OB_ITER_END;
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN] output group by result", K(ret), K(format), K(count), K_(projected_cnt), K_(distinct_cnt));
  }
  return ret;
}

int ObGroupByCellVec::pad_column_in_group_by(const int64_t row_cap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  } else if (nullptr != group_by_col_param_ &&
      group_by_col_param_->get_meta_type().is_fixed_len_char_type() &&
      OB_FAIL(storage::pad_on_rich_format_columns(
              group_by_col_param_->get_accuracy(),
              group_by_col_param_->get_meta_type().get_collation_type(),
              row_cap,
              0,
              padding_allocator_,
              *group_by_col_expr_,
              eval_ctx_))) {
    LOG_WARN("Failed pad on rich format columns", K(ret), KPC_(group_by_col_expr));
  }
  return ret;
}

int ObGroupByCellVec::assign_agg_cells(const sql::ObExpr *col_expr, common::ObIArray<int32_t> &agg_idxs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGroupByCellVec is not inited", K(ret), K_(is_inited));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = get_sorted_cell(i);
    ObPDAggType agg_type = agg_cell->get_type();
    if (agg_cell->is_assigned_to_group_by_processor()) {
    } else if (OB_UNLIKELY(ObPDAggType::PD_FIRST_ROW == agg_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected agg type in vec2", K(ret), K(agg_type));
    } else if ((ObPDAggType::PD_COUNT == agg_cell->get_type() && !agg_cell->need_get_row_ids()) ||
                col_expr == agg_cell->get_project_expr()) {
      if (OB_FAIL(agg_idxs.push_back(i))) {
        LOG_WARN("Failed to push back", K(ret));
      } else {
        agg_cell->set_assigned_to_group_by_processor();
      }
    }
  }
  return ret;
}

int ObGroupByCellVec::init_agg_cells(
    const ObTableAccessParam &param,
    const ObTableAccessContext &context,
    sql::ObEvalCtx &eval_ctx,
    const bool is_for_single_row)
{
  int ret = OB_SUCCESS;
  ObAggCellVec *agg_cell = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.aggregate_exprs_->count(); ++i) {
    int32_t col_offset = param.iter_param_.agg_cols_project_->at(i);
    int32_t col_index = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? -1 : param.iter_param_.read_info_->get_columns_index().at(col_offset);
    const share::schema::ObColumnParam *col_param = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : param.iter_param_.get_col_params()->at(col_offset);
    sql::ObExpr *agg_expr = param.aggregate_exprs_->at(i);
    bool exclude_null = false;
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg expr", K(ret));
    } else if (T_FUN_COUNT == agg_expr->type_) {
      if (OB_COUNT_AGG_PD_COLUMN_ID != col_offset) {
        exclude_null = col_param->is_nullable_for_write();
      }
    }
    if (OB_SUCC(ret)) {
      ObAggCellVecBasicInfo basic_info(pd_agg_ctx_.agg_ctx_, pd_agg_ctx_.rows_, pd_agg_ctx_.row_meta_, pd_agg_ctx_.batch_rows_,
                                       col_offset, col_param, is_pad_char_to_full_length(context.sql_mode_));
      if (OB_FAIL(agg_cell_factory_vec_.alloc_cell(basic_info, agg_cell, i, exclude_null))) {
        LOG_WARN("Failed to alloc aggregate cell", K(ret));
      } else if (OB_FAIL(agg_cells_.push_back(agg_cell))) {
        LOG_WARN("Failed to push agg cell", K(ret));
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
