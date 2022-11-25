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
#include "ob_aggregated_store.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/number/ob_number_v2.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/ob_index_block_row_struct.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
namespace oceanbase
{
namespace storage
{

ObAggCell::ObAggCell(
    const int32_t col_idx,
    const share::schema::ObColumnParam *col_param,
    sql::ObExpr *expr,
    common::ObIAllocator &allocator)
    : col_idx_(col_idx), datum_(), col_param_(col_param), expr_(expr), allocator_(allocator)
{
}

ObAggCell::~ObAggCell()
{
  reset();
}

void ObAggCell::reset()
{
  col_idx_ = -1;
  expr_ = nullptr;
}

void ObAggCell::reuse()
{
}

int ObAggCell::fill_result(sql::ObEvalCtx &ctx,bool need_padding)
{
  int ret = OB_SUCCESS;
  ObDatum &result = expr_->locate_datum_for_write(ctx);
  if (OB_FAIL(fill_default_if_need(datum_))) {
    LOG_WARN("Failed to fill default", K(ret), K(*this));
  } else if (need_padding && OB_FAIL(pad_column_if_need(datum_))) {
    LOG_WARN("Failed to pad column", K(ret), K(*this));
  } else if (OB_FAIL(result.from_storage_datum(datum_, expr_->obj_datum_map_))) {
    LOG_WARN("Failed to from storage datum", K(ret), K(datum_), K(result), K(*this));
  } else {
    sql::ObEvalInfo &eval_info = expr_->get_eval_info(ctx);
    eval_info.evaluated_ = true;
    LOG_DEBUG("fill result", K(result));
  }
  return ret;
}

int ObAggCell::fill_default_if_need(blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, col param is null", K(ret), K(col_idx_));
  } else if (datum.is_nop()) {
    datum.reuse();
    const ObObj &def_cell = col_param_->get_orig_default_value();
    if (!def_cell.is_nop_value()) {
      if (OB_FAIL(datum.from_obj_enhance(def_cell))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, virtual column is not supported", K(ret), K(col_idx_));
    }
  }
  return ret;
}

int ObAggCell::pad_column_if_need(blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, col param is null", K(ret), K(col_idx_));
  } else if (OB_FAIL(pad_column(col_param_->get_meta_type(), col_param_->get_accuracy(), allocator_, datum))) {
    LOG_WARN("Fail to pad column", K(ret), K(col_idx_), K(*this));
  }
  return ret;
}

ObFirstRowAggCell::ObFirstRowAggCell(
    const int32_t col_idx,
    const share::schema::ObColumnParam *col_param,
    sql::ObExpr *expr,
    common::ObIAllocator &allocator)
    : ObAggCell(col_idx, col_param, expr, allocator), aggregated_(false)
{
}

void ObFirstRowAggCell::reset()
{
  ObAggCell::reset();
  aggregated_ = false;
}

int ObFirstRowAggCell::process(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    if (OB_FAIL(datum_.deep_copy(row.storage_datums_[col_idx_], allocator_))) {
      LOG_WARN("Failed to deep copy datum", K(ret), K(row), K(col_idx_));
    } else {
      aggregated_ = true;
    }
  }

  return ret;
}

int ObFirstRowAggCell::process(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t *row_ids,
    const int64_t row_count)
{
  UNUSEDx(reader, row_ids, row_count);
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, must be aggregated in single row", K(ret));
  }
  return ret;
}

int ObFirstRowAggCell::process(const blocksstable::ObMicroIndexInfo &index_info)
{
  UNUSED(index_info);
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, must be aggregated in single row", K(ret));
  }
  return ret;
}

int ObFirstRowAggCell::fill_result(sql::ObEvalCtx &ctx, bool need_padding)
{
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Uexpected, must be aggregated in single row", K(ret));
  } else if (OB_FAIL(ObAggCell::fill_result(ctx, need_padding))) {
    LOG_WARN("Failed to fill result", K(ret), KPC(this));
  }
  return ret;
}

ObCountAggCell::ObCountAggCell(
    const int32_t col_idx,
    const share::schema::ObColumnParam *col_param,
    sql::ObExpr *expr,
    common::ObIAllocator &allocator,
    bool exclude_null)
    : ObAggCell(col_idx, col_param, expr, allocator), exclude_null_(exclude_null), row_count_(0)
{
}

void ObCountAggCell::reset()
{
  ObAggCell::reset();
  exclude_null_ = false;
  row_count_ = 0;
}

void ObCountAggCell::reuse()
{
  row_count_ = 0;
}

int ObCountAggCell::process(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before count single row", K(row_count_));
  if (!exclude_null_) {
    ++row_count_;
  } else if (OB_FAIL(fill_default_if_need(row.storage_datums_[col_idx_]))) {
    LOG_WARN("Failed to fill default", K(ret), K(*this));
  } else {
    row_count_ += row.storage_datums_[col_idx_].is_null() ? 0 : 1;
  }
  LOG_DEBUG("after count single row", K(ret), K(row_count_));
  return ret;
}

int ObCountAggCell::process(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before count batch rows", K(row_count), K(row_count_));
  if (!exclude_null_) {
    row_count_ += row_count;
  } else {
    int64_t valid_row_count = 0;
    if (OB_ISNULL(row_ids)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Uexpected, row_ids is null", K(ret), K(*this), K(row_count));
    } else if (OB_FAIL(reader->get_row_count(col_idx_, row_ids, row_count, false, valid_row_count))) {
      LOG_WARN("Failed to get row count from micro block decoder", K(ret), K(*this), K(row_count));
    } else {
      row_count_ += valid_row_count;
    }
  }
  LOG_DEBUG("after count batch rows", K(ret), K(row_count), K(row_count_));
  return ret;
}

int ObCountAggCell::process(const blocksstable::ObMicroIndexInfo &index_info)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before count index info", K(index_info.get_row_count()), K(row_count_));
  if (!index_info.can_blockscan() || index_info.is_left_border() || index_info.is_right_border()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Uexpected, the micro index info must can blockscan and not border", K(ret));
  } else if (!exclude_null_) {
    row_count_ += index_info.get_row_count();
  } else {
    // TODO, extract from pre-agginfo
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("exclude null is not supported", K(ret));
  }
  LOG_DEBUG("after count index info", K(ret), K(index_info.get_row_count()), K(row_count_));
  return ret;
}

int ObCountAggCell::fill_result(sql::ObEvalCtx &ctx, bool need_padding)
{
  UNUSED(need_padding);
  int ret = OB_SUCCESS;
  ObDatum &result = expr_->locate_datum_for_write(ctx);
  sql::ObEvalInfo &eval_info = expr_->get_eval_info(ctx);
  if (lib::is_oracle_mode()) {
    common::number::ObNumber result_num;
    char local_buff[common::number::ObNumber::MAX_BYTE_LEN];
    common::ObDataBuffer local_alloc(local_buff, common::number::ObNumber::MAX_BYTE_LEN );
    if (OB_FAIL(result_num.from(row_count_, local_alloc))) {
      LOG_WARN("Failed to cons number from int", K(ret), K(row_count_));
    } else {
      result.set_number(result_num);
      eval_info.evaluated_ = true;
    }
  } else {
    result.set_int(row_count_);
    eval_info.evaluated_ = true;
  }
  LOG_DEBUG("fill result", K(result));
  return ret;
}

ObAggRow::ObAggRow(common::ObIAllocator &allocator) :
    agg_cells_(allocator),
    need_exclude_null_(false),
    allocator_(allocator)
{
}

ObAggRow::~ObAggRow()
{
  reset();
}

void ObAggRow::reset()
{
  for (int64_t i = 0; i < agg_cells_.count(); ++i) {
    if (agg_cells_.at(i)) {
      allocator_.free(agg_cells_.at(i));
    }
  }
  agg_cells_.reset();
  need_exclude_null_ = false;
}

void ObAggRow::reuse()
{
  for (int i = 0; i < agg_cells_.count(); ++i) {
    if (agg_cells_.at(i)) {
      agg_cells_.at(i)->reuse();
    }
  }
}

int ObAggRow::init(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
  if (OB_ISNULL(out_cols_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null out cols param", K(ret), K_(param.iter_param));
  } else if (OB_FAIL(agg_cells_.init(param.output_exprs_->count() + param.aggregate_exprs_->count()))) {
    LOG_WARN("Failed to init agg cells array", K(ret), K(param.output_exprs_->count()));
  } else {
    void *buf = nullptr;
    ObAggCell *cell = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < param.output_exprs_->count(); ++i) {
      // mysql compatibility, select a,count(a), output first value of a
      int32_t col_idx = param.iter_param_.out_cols_project_->at(i);
      const share::schema::ObColumnParam *col_param = out_cols_param->at(col_idx);
      sql::ObExpr *expr = param.output_exprs_->at(i);
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObFirstRowAggCell))) ||
          OB_ISNULL(cell = new(buf) ObFirstRowAggCell(col_idx, col_param, expr, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memroy for agg cell", K(ret), K(i));
      } else if (OB_FAIL(agg_cells_.push_back(cell))) {
        LOG_WARN("Failed to push back agg cell", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.aggregate_exprs_->count(); ++i) {
        int32_t col_idx = param.iter_param_.agg_cols_project_->at(i);
        sql::ObExpr *expr = param.aggregate_exprs_->at(i);
        if (T_FUN_COUNT == expr->type_) {
          bool exclude_null = false;
          const share::schema::ObColumnParam *col_param = nullptr;
          if (OB_COUNT_AGG_PD_COLUMN_ID != col_idx) {
            col_param = out_cols_param->at(col_idx);
            exclude_null = col_param->is_nullable_for_write();
          } else {
            exclude_null = false;
          }
          need_exclude_null_ = need_exclude_null_ || exclude_null;
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCountAggCell))) ||
              OB_ISNULL(cell = new(buf) ObCountAggCell(col_idx, col_param, expr, allocator_, exclude_null))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("Failed to alloc memroy for agg cell", K(ret), K(i));
          } else if (OB_FAIL(agg_cells_.push_back(cell))) {
            LOG_WARN("Failed to push back agg cell", K(ret), K(i));
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Agg min/max/sum is not supported", K(ret));
        }
      }
    }
  }
  return ret;
}

ObAggregatedStore::ObAggregatedStore(const int64_t batch_size, sql::ObEvalCtx &eval_ctx, ObTableAccessContext &context)
    : ObBlockBatchedRowStore(batch_size, eval_ctx, context),
      is_firstrow_aggregated_(false),
      agg_row_(*context_.stmt_allocator_)
{
}

ObAggregatedStore::~ObAggregatedStore()
{
  reset();
}

void ObAggregatedStore::reset()
{
  ObBlockBatchedRowStore::reset();
  agg_row_.reset();
  is_firstrow_aggregated_ = false;
}

void ObAggregatedStore::reuse()
{
  ObBlockBatchedRowStore::reuse();
  iter_end_flag_ = IterEndState::PROCESSING;
}

int ObAggregatedStore::init(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.output_exprs_) ||
      OB_ISNULL(param.iter_param_.out_cols_project_) ||
      OB_ISNULL(param.aggregate_exprs_) ||
      OB_ISNULL(param.iter_param_.agg_cols_project_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate pushdown expr and projector", K(ret), K(param.output_exprs_),
        K(param.iter_param_.out_cols_project_),
        K(param.aggregate_exprs_), K(param.iter_param_.agg_cols_project_));
  } else if (param.output_exprs_->count() != param.iter_param_.out_cols_project_->count() ||
      param.aggregate_exprs_->count() != param.iter_param_.agg_cols_project_->count() ||
      param.aggregate_exprs_->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate count", K(ret), K(param.output_exprs_->count()),
        K(param.iter_param_.out_cols_project_->count()),
        K(param.aggregate_exprs_->count()), K(param.iter_param_.agg_cols_project_->count()));
  } else if (OB_FAIL(ObBlockBatchedRowStore::init(param))) {
    LOG_WARN("Failed to init ObBlockBatchedRowStore", K(ret));
  } else if (OB_FAIL(agg_row_.init(param))) {
    LOG_WARN("Failed to init agg cells", K(ret));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObAggregatedStore::fill_index_info(const blocksstable::ObMicroIndexInfo &index_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
       ObAggCell *cell = agg_row_.at(i);
       if (OB_FAIL(cell->process(index_info))) {
         LOG_WARN("Failed to process agg cell", K(ret), K(i), K(*cell));
       }
    }
  }
  return ret;
}

int ObAggregatedStore::fill_rows(
     const int64_t group_idx,
     blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    const common::ObBitmap *bitmap)
{
  UNUSED(group_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else {
    int64_t row_count = 0;
    bool is_reverse = begin_index > end_index;
    int64_t covered_row_count = is_reverse ? begin_index - end_index : end_index - begin_index;
    // if should check null or not whole block is covered
    // must get valid rows
     bool need_get_row_ids = false;
    int64_t micro_row_count = 0;
    if (OB_FAIL(reader->get_row_count(micro_row_count))) {
      LOG_WARN("Failed to get micro row count", K(ret));
    } else if(FALSE_IT(need_get_row_ids = agg_row_.need_exclude_null() || micro_row_count != covered_row_count)) {
    } else if (!need_get_row_ids) {
      row_count = nullptr == bitmap ? covered_row_count : bitmap->popcnt();
      for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
        ObAggCell *cell = agg_row_.at(i);
        if (OB_FAIL(cell->process(reader, nullptr, row_count))) {
          LOG_WARN("Failed to process agg cell", K(ret), K(i), K(*cell), K(begin_index), K(end_index));
        }
      }
      if (OB_SUCC(ret)) {
        begin_index = end_index;
      }
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(get_row_ids(reader, begin_index, end_index, row_count, false, bitmap))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(end_index));
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
            ObAggCell *cell = agg_row_.at(i);
            if (OB_FAIL(cell->process(reader, row_ids_, row_count))) {
              LOG_WARN("Failed to process agg cell", K(ret), K(i), K(*cell), K(begin_index), K(end_index));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAggregatedStore::fill_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
      ObAggCell *cell = agg_row_.at(i);
      if (OB_FAIL(cell->process(row))) {
        LOG_WARN("Failed to process agg cell", K(ret), K(i), K(row), K(*cell));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_firstrow_aggregated_ = true;
  }
  return ret;
}

int ObAggregatedStore::collect_aggregated_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else if (!is_firstrow_aggregated_) {
    // just ret OB_ITER_END if no row aggregated
    ret = OB_ITER_END;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
      ObAggCell *cell = agg_row_.at(i);
      if (OB_FAIL(cell->fill_result(eval_ctx_, is_pad_char_to_full_length(context_.sql_mode_)))) {
        LOG_WARN("Failed to fill agg result", K(ret), K(i), K(*cell));
      }
    }
  }
  return ret;
}
} /* namespace storage */
} /* namespace oceanbase */
