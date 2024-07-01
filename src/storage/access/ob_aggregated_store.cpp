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
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/lob/ob_lob_manager.h"
namespace oceanbase
{
namespace storage
{

void ObCGAggCells::reset()
{
  agg_cells_.reset();
}

bool ObCGAggCells::check_finished() const
{
  bool finised = true;
  for (int i = 0; finised && i < agg_cells_.count(); ++i) {
    finised = agg_cells_.at(i)->finished();
  }
  return finised;
}

int ObCGAggCells::can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg)
{
  int ret = OB_SUCCESS;
  can_agg = true;
  for (int i = 0; OB_SUCC(ret) && can_agg && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->can_use_index_info(index_info, true, can_agg))) {
      LOG_WARN("fail to check can use index info", K(i), KPC(agg_cells_.at(i)), K(index_info));
    }
  }
  return ret;
}

int ObCGAggCells::add_agg_cell(ObAggCell *cell)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cell)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, cell is null", K(ret));
  } else if (OB_FAIL(agg_cells_.push_back(cell))) {
    LOG_WARN("Fail to push back", K(ret));
  }
  return ret;
}

int ObCGAggCells::process(const blocksstable::ObMicroIndexInfo &index_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->eval_index_info(index_info, true/*is_cg*/))) {
      LOG_WARN("Fail to agg index info", K(ret), KPC(agg_cells_.at(i)));
    }
  }
  return ret;
}

int ObCGAggCells::process(blocksstable::ObStorageDatum &datum, const uint64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (agg_cells_.at(i)->finished()) {
    } else if (OB_FAIL(agg_cells_.at(i)->eval(datum, row_count))) {
      LOG_WARN("Fail to eval agg cell", K(ret), K(datum), K(row_count));
    }
  }
  return ret;
}

int ObCGAggCells::process(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (agg_cells_.at(i)->finished()) {
    } else if (OB_FAIL(agg_cells_.at(i)->eval_micro_block(iter_param, context, col_offset, reader, row_ids, row_count))) {
      LOG_WARN("Fail to eval micro", K(ret));
    }
  }
  return ret;
}

ObAggRow::ObAggRow(common::ObIAllocator &allocator) :
    agg_cells_(allocator),
    dummy_agg_cells_(allocator),
    can_use_index_info_(false),
    need_access_data_(false),
    has_lob_column_out_(false),
    allocator_(allocator),
    agg_cell_factory_(allocator)
{
}

ObAggRow::~ObAggRow()
{
  reset();
}

void ObAggRow::reset()
{
  agg_cell_factory_.release(agg_cells_);
  agg_cells_.reset();
  agg_cell_factory_.release(dummy_agg_cells_);
  dummy_agg_cells_.reset();
  can_use_index_info_ = false;
  need_access_data_ = false;
  has_lob_column_out_ = false;
}

void ObAggRow::reuse()
{
  for (int i = 0; i < agg_cells_.count(); ++i) {
    if (agg_cells_.at(i)) {
      agg_cells_.at(i)->reuse();
    }
  }
}

int ObAggRow::init(const ObTableAccessParam &param, const ObTableAccessContext &context, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
  if (OB_ISNULL(out_cols_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null out cols param", K(ret), K_(param.iter_param));
  } else if (OB_FAIL(agg_cells_.init(param.aggregate_exprs_->count()))) {
    LOG_WARN("Failed to init agg cells array", K(ret), K(param.aggregate_exprs_->count()));
  } else if (OB_FAIL(dummy_agg_cells_.init(param.output_exprs_->count()))) {
    LOG_WARN("Failed to init first row agg cells array", K(ret), K(param.output_exprs_->count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.output_exprs_->count(); ++i) {
      // mysql compatibility, select a,count(a), output the first value of a
      // from 4.3, this non-standard scalar group by will not pushdown to storage
      // so we can just set an determined value to output_exprs_ as it's never be used
      if (T_PSEUDO_GROUP_ID == param.output_exprs_->at(i)->type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Unexpected group idx expr", K(ret));
      } else if (nullptr == param.output_sel_mask_ || param.output_sel_mask_->at(i)) {
        ObAggCell *cell = nullptr;
        int32_t col_offset = param.iter_param_.out_cols_project_->at(i);
        int32_t col_index = param.iter_param_.read_info_->get_columns_index().at(col_offset);
        const share::schema::ObColumnParam *col_param = out_cols_param->at(col_offset);
        sql::ObExpr *expr = param.output_exprs_->at(i);
        ObAggCellBasicInfo basic_info(col_offset, col_index, col_param, expr,
                                      batch_size, is_pad_char_to_full_length(context.sql_mode_));
        if (OB_FAIL(agg_cell_factory_.alloc_cell(basic_info, dummy_agg_cells_))) {
          LOG_WARN("Failed to alloc agg cell", K(ret), K(i));
        } else if (FALSE_IT(cell = dummy_agg_cells_.at(dummy_agg_cells_.count() - 1))) {
        } else if (OB_UNLIKELY(PD_FIRST_ROW != cell->get_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected agg type", K(ret), KPC(cell));
        } else {
          static_cast<ObFirstRowAggCell*>(cell)->set_determined_value();
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_lob_column_out_ = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.aggregate_exprs_->count(); ++i) {
        int32_t col_offset = param.iter_param_.agg_cols_project_->at(i);
        int32_t col_index = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? -1 : param.iter_param_.read_info_->get_columns_index().at(col_offset);
        const share::schema::ObColumnParam *col_param = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : out_cols_param->at(col_offset);
        bool exclude_null = false;
        sql::ObExpr *agg_expr = param.aggregate_exprs_->at(i);
        if (OB_ISNULL(agg_expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Unexpected null agg expr", K(ret));
        } else if (T_FUN_COUNT == agg_expr->type_ || T_FUN_SUM_OPNSIZE == agg_expr->type_) {
          if (OB_COUNT_AGG_PD_COLUMN_ID != col_offset) {
            exclude_null = col_param->is_nullable_for_write();
          }
          // T_FUN_SUM_OPNISZE need_access_data() depends on exclude_null and type,
          // so deferred judgment in ObAggRow::check_need_access_data()
          need_access_data_ = T_FUN_COUNT == agg_expr->type_ ? (need_access_data_ || exclude_null) : true;
        } else {
          need_access_data_ = true;
        }
        ObAggCellBasicInfo basic_info(col_offset, col_index, col_param, agg_expr,
                                      batch_size, is_pad_char_to_full_length(context.sql_mode_));
        if (OB_FAIL(agg_cell_factory_.alloc_cell(basic_info, agg_cells_, exclude_null))) {
          LOG_WARN("Failed to alloc agg cell", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

bool ObAggRow::found_ref_column(const ObTableAccessParam &param, const int32_t agg_col_offset)
{
  bool found = false;
  for (int64_t i = 0; i < param.output_exprs_->count(); ++i) {
    if (param.iter_param_.out_cols_project_->at(i) == agg_col_offset) {
      found = true;
      break;
    }
  }
  return found;
}

bool ObAggRow::check_need_access_data()
{
  if (!need_access_data_) {
  } else {
    need_access_data_ = false;
    for (int64_t i = 0; !need_access_data_ && i < agg_cells_.count(); ++i) {
      need_access_data_ = agg_cells_.at(i)->need_access_data();
    }
  }
  return need_access_data_;
}

ObAggregatedStore::ObAggregatedStore(const int64_t batch_size, sql::ObEvalCtx &eval_ctx, ObTableAccessContext &context)
    : ObBlockBatchedRowStore(batch_size, eval_ctx, context),
      agg_row_(*context_.stmt_allocator_),
      agg_flat_row_mode_(false),
      row_buf_()
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
  agg_flat_row_mode_ = false;
  row_buf_.reset();
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
  } else if (OB_FAIL(agg_row_.init(param, context_, batch_size_))) {
    LOG_WARN("Failed to init agg cells", K(ret));
  } else if (OB_FAIL(check_agg_in_row_mode(param.iter_param_))) {
    LOG_WARN("Failed to check agg in row mode", K(ret));
  } else if (agg_flat_row_mode_ &&
             OB_FAIL(row_buf_.init(*context_.stmt_allocator_, param.iter_param_.get_max_out_col_cnt()))) {
    LOG_WARN("Fail to init datum row buf", K(ret));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObAggregatedStore::check_agg_in_row_mode(const ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  int64_t agg_cnt = 0;
  ObAggCell *cell = nullptr;
  const ObITableReadInfo *read_info = nullptr;
  if (OB_ISNULL(read_info = iter_param.get_read_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null read info", K(ret), K(iter_param));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
    if (OB_ISNULL(cell = agg_row_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpecte null agg cell", K(ret), K(i));
    } else if (OB_COUNT_AGG_PD_COLUMN_ID == cell->get_col_offset()) {
    } else if (cell->get_col_offset() >= read_info->get_request_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected col idx", K(ret), K(i), KPC(cell), K(read_info->get_request_count()));
    } else if (ObPDAggType::PD_FIRST_ROW != cell->get_type()) {
      agg_cnt++;
    }
  }
  if (OB_SUCC(ret)) {
    agg_flat_row_mode_ =
        agg_cnt > AGG_ROW_MODE_COUNT_THRESHOLD ||
        (double) agg_cnt/read_info->get_request_count() > AGG_ROW_MODE_RATIO_THRESHOLD;
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
    set_aggregated_in_prefetch();
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
       ObAggCell *cell = agg_row_.at(i);
       if (OB_FAIL(cell->eval_index_info(index_info))) {
         LOG_WARN("Failed to eval index info", K(ret), K(i), K(*cell));
       }
    }
  }
  return ret;
}

int ObAggregatedStore::fill_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockRowScanner *scanner,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  UNUSED(group_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else if (OB_ISNULL(scanner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null scanner", K(ret));
  } else {
    int64_t row_count = 0;
    bool is_reverse = begin_index > end_index;
    int64_t covered_row_count = is_reverse ? begin_index - end_index : end_index - begin_index;
    // if should check null or not whole block is covered
    // must get valid rows
     bool need_get_row_ids = false;
    int64_t micro_row_count = 0;
    blocksstable::ObIMicroBlockReader *reader = scanner->get_reader();
    if (OB_FAIL(reader->get_row_count(micro_row_count))) {
      LOG_WARN("Failed to get micro row count", K(ret));
    } else if(FALSE_IT(need_get_row_ids = agg_row_.check_need_access_data() || micro_row_count != covered_row_count)) {
    } else if (!need_get_row_ids) {
      row_count = nullptr == res.bitmap_ ? covered_row_count : res.bitmap_->popcnt();
      if (0 == row_count) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
          ObAggCell *cell = agg_row_.at(i);
          if (OB_FAIL(cell->eval_micro_block(*iter_param_, context_, cell->get_col_offset(), reader, nullptr, row_count))) {
            LOG_WARN("Failed to eval micro", K(ret), K(i), K(*cell), K(begin_index), K(end_index));
          }
        }
      }
      if (OB_SUCC(ret)) {
        begin_index = end_index;
      }
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(get_row_ids(reader, begin_index, end_index, row_count, false, res))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(end_index));
          }
        } else if (0 == row_count) {
        } else if (agg_flat_row_mode_ && blocksstable::ObIMicroBlockReader::Reader == reader->get_type()) {
          // for flat block, do aggregate in row mode in some case
           blocksstable::ObMicroBlockReader *block_reader = static_cast<blocksstable::ObMicroBlockReader*>(reader);
           if (OB_FAIL(block_reader->get_aggregate_result(*iter_param_, context_, row_ids_, row_count, row_buf_, agg_row_.get_agg_cells()))) {
             LOG_WARN("Failed to get aggregate", K(ret));
           }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
            ObAggCell *cell = agg_row_.at(i);
            if (OB_FAIL(cell->eval_micro_block(*iter_param_, context_, cell->get_col_offset(), reader, row_ids_, row_count))) {
              LOG_WARN("Failed to eval micro", K(ret), K(i), K(*cell), K(begin_index), K(end_index));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAggregatedStore::fill_rows(const int64_t group_idx, const int64_t row_count)
{
  UNUSEDx(group_idx, row_count);
  int ret = OB_SUCCESS;
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
      if (OB_FAIL(cell->eval(row.storage_datums_[cell->get_col_offset()]))) {
        LOG_WARN("Failed to eval agg cell", K(ret), K(i), K(row), K(*cell));
      }
    }
  }
  return ret;
}

int ObAggregatedStore::collect_aggregated_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else if (!has_data()) {
    // just ret OB_ITER_END if no row aggregated
    ret = OB_ITER_END;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_agg_count(); ++i) {
      ObAggCell *cell = agg_row_.at(i);
      if (OB_FAIL(cell->collect_result(eval_ctx_))) {
        LOG_WARN("Failed to fill agg result", K(ret), K(i), K(*cell));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_row_.get_dummy_agg_count(); ++i) {
      ObAggCell *cell = agg_row_.at_dummy(i);
      if (OB_FAIL(cell->collect_result(eval_ctx_))) {
        LOG_WARN("Failed to fill agg result", K(ret), K(i), K(*cell));
      }
    }
  }
  return ret;
}

bool ObAggregatedStore::has_data()
{
  bool has_data = false;
  for (int64_t i = 0; !has_data && i < agg_row_.get_agg_count(); ++i) {
    has_data = agg_row_.at(i)->is_aggregated();
  }
  return has_data;
}

int ObAggregatedStore::get_agg_cell(const sql::ObExpr *expr, ObAggCell *&agg_cell)
{
  int ret = OB_SUCCESS;
  agg_cell = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStore is not inited", K(ret), K(*this));
  } else {
    for (int64_t i = 0; i < agg_row_.get_agg_count(); ++i) {
      ObAggCell *cell = agg_row_.at(i);
      if (cell->get_agg_expr() == expr) {
        agg_cell = cell;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == agg_cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null agg cell", K(ret), KPC(expr));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
