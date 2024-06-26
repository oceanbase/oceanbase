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
#include "ob_co_sstable_row_scanner.h"
#include "ob_cg_scanner.h"
#include "ob_cg_tile_scanner.h"
#include "ob_cg_group_by_scanner.h"
#include "ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/access/ob_vector_store.h"

namespace oceanbase
{
namespace storage
{

const ObCOSSTableRowScanner::ScanState ObCOSSTableRowScanner::STATE_TRANSITION[MAX_STATE]
= {FILTER_ROWS, BEGIN, END, END};

ObCOSSTableRowScanner::ObCOSSTableRowScanner()
  : ObStoreRowIterator(),
    row_scanner_(nullptr),
    range_idx_(0),
    is_new_group_(false),
    reverse_scan_(false),
    is_limit_end_(false),
    state_(BEGIN),
    blockscan_state_(MAX_STATE),
    group_by_project_idx_(0),
    group_size_(0),
    batch_size_(1),
    column_group_cnt_(-1),
    current_(OB_INVALID_CS_ROW_ID),
    end_(OB_INVALID_CS_ROW_ID),
    pending_end_row_id_(OB_INVALID_CS_ROW_ID),
    iter_param_(nullptr),
    access_ctx_(nullptr),
    rows_filter_(nullptr),
    project_iter_(nullptr),
    getter_project_iter_(nullptr),
    group_by_cell_(nullptr),
    batched_row_store_(nullptr),
    cg_param_pool_(nullptr),
    range_(nullptr),
    group_by_iters_(),
    getter_projector_()
{
  type_ = ObStoreRowIterator::IteratorCOScan;
}

ObCOSSTableRowScanner::~ObCOSSTableRowScanner()
{
  reset();
}

int ObCOSSTableRowScanner::init(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  ObSSTable *row_sstable = nullptr;
  if (OB_UNLIKELY(!table->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(table), KP(context.block_row_store_), K(param));
  } else if (OB_ISNULL(row_sstable = static_cast<ObSSTable *>(table))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null row store cg", K(ret));
  } else if (OB_FAIL(init_row_scanner(param, context, row_sstable, query_range))) {
    LOG_WARN("Fail to init row scanner", K(ret), K(param), KPC(row_sstable));
  } else if (OB_FAIL(init_cg_param_pool(context))) {
    LOG_WARN("Fail to init cg param pool", K(ret));
  } else if (param.vectorized_enabled_ && param.enable_pd_blockscan() && param.enable_pd_filter()) {
    if (OB_FAIL(init_rows_filter(param, context, table))) {
      LOG_WARN("Fail to init rows filter", K(ret));
    } else if (OB_FAIL(init_project_iter(param, context, table))) {
      LOG_WARN("Fail to init project iter", K(ret));
    } else if (param.enable_pd_group_by() && OB_FAIL(init_group_by_info(context))) {
      LOG_WARN("Failed to init group by info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_project_iter_for_single_row(param, context, table))) {
      LOG_WARN("Fail to init project iter for single row", K(ret));
    } else if (param.has_lob_column_out()
        && (nullptr == context.lob_locator_helper_
            || !context.lob_locator_helper_->enable_lob_locator_v2()
            || !context.lob_locator_helper_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lob locator", K(ret), KP_(context.lob_locator_helper), K(param));
    }
  }
  if (OB_SUCC(ret)) {
    is_sstable_iter_ = true;
    iter_param_ = &param;
    access_ctx_ = &context;
    batch_size_ = param.get_storage_rowsets_size();
    reverse_scan_ = context.query_flag_.is_reverse_scan();
    batched_row_store_ = static_cast<ObBlockBatchedRowStore*>(context.block_row_store_);
    block_row_store_ = context.block_row_store_;
    column_group_cnt_ = static_cast<ObCOSSTableV2*>(table)->get_cs_meta().get_column_group_count();
  }
  return ret;
}

void ObCOSSTableRowScanner::reset()
{
  ObStoreRowIterator::reset();
  FREE_PTR_FROM_CONTEXT(access_ctx_, row_scanner_, ObSSTableRowScanner);
  FREE_PTR_FROM_CONTEXT(access_ctx_, rows_filter_, ObCOSSTableRowsFilter);
  FREE_PTR_FROM_CONTEXT(access_ctx_, project_iter_, ObICGIterator);
  FREE_PTR_FROM_CONTEXT(access_ctx_, getter_project_iter_, ObICGIterator);
  FREE_PTR_FROM_CONTEXT(access_ctx_, cg_param_pool_, ObCGIterParamPool);
  range_idx_ = 0;
  group_by_project_idx_ = 0;
  group_by_iters_.reset();
  group_by_cell_ = nullptr;
  is_new_group_ = false;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  batched_row_store_ = nullptr;
  current_ = OB_INVALID_CS_ROW_ID;
  end_ = OB_INVALID_CS_ROW_ID;
  group_size_ = 0;
  batch_size_ = 1;
  reverse_scan_ = false;
  state_ = BEGIN;
  blockscan_state_ = MAX_STATE;
  range_ = nullptr;
  is_limit_end_ = false;
  pending_end_row_id_ = OB_INVALID_CS_ROW_ID;
  column_group_cnt_ = -1;
  getter_projector_.reset();
}

void ObCOSSTableRowScanner::reuse()
{
  ObStoreRowIterator::reuse();
  if (nullptr != row_scanner_) {
    row_scanner_->reuse();
  }
  if (nullptr != rows_filter_) {
    rows_filter_->reuse();
  }
  if (nullptr != project_iter_) {
    project_iter_->reuse();
  }
  if (nullptr != getter_project_iter_) {
    getter_project_iter_->reuse();
  }
  range_idx_ = 0;
  is_new_group_ = false;
  current_ = OB_INVALID_CS_ROW_ID;
  end_ = OB_INVALID_CS_ROW_ID;
  group_size_ = 0;
  batch_size_ = 1;
  reverse_scan_ = false;
  state_ = BEGIN;
  blockscan_state_ = MAX_STATE;
  range_ = nullptr;
  is_limit_end_ = false;
  pending_end_row_id_ = OB_INVALID_CS_ROW_ID;
}

int ObCOSSTableRowScanner::get_next_rows()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !batched_row_store_->is_end()) {
    LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [start]", K(ret), K_(state), K_(blockscan_state),
              K_(current), K_(group_size), K_(end));
    switch(state_) {
      case BEGIN: {
        blockscan_state_ = BLOCKSCAN_RANGE;
        if (OB_FAIL(row_scanner_->get_blockscan_start(current_, range_idx_, blockscan_state_))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to get blockscan start", K(ret), KPC(this));
          } else {
            blockscan_state_ = MAX_STATE;
            state_ = END;
          }
        } else if (BLOCKSCAN_FINISH == blockscan_state_) {
          blockscan_state_ = MAX_STATE;
          ret = OB_PUSHDOWN_STATUS_CHANGED;
        } else {
          LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [change to filter_rows]", K(ret), K_(state), K_(blockscan_state),
                    K_(current), K_(group_size), K_(end));
          state_ = FILTER_ROWS;
        }
        break;
      }
      case FILTER_ROWS: {
        if (is_limit_end_) {
          state_ = END;
          batched_row_store_->set_limit_end();
        } else if (OB_FAIL(filter_rows(blockscan_state_))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to filter rows", K(ret), KPC(this));
          } else if (MAX_STATE == blockscan_state_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected blockscan state", K(ret), K_(blockscan_state));
          } else {
            ret = OB_SUCCESS;
            state_ = STATE_TRANSITION[blockscan_state_];
          }
        } else {
          LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [change to project_rows]", K(ret), K_(state), K_(blockscan_state),
                    K_(current), K_(group_size), K_(end));
          state_ = PROJECT_ROWS;
        }
        break;
      }
      case PROJECT_ROWS: {
        if (OB_FAIL(fetch_rows())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            update_current(group_size_);
            LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [change to filter_rows]", K(ret), K_(state), K_(blockscan_state),
                      K_(current), K_(group_size), K_(end));
            state_ = FILTER_ROWS;
          } else {
            LOG_WARN("Fail to fetch rows", K(ret), KPC(this));
          }
        }
        break;
      }
      case END: {
        ret = OB_ITER_END;
        LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [change to iter_end]", K(ret), K_(state), K_(blockscan_state),
                  K_(current), K_(group_size), K_(end));
        if (BLOCKSCAN_FINISH == blockscan_state_) {
          ret = OB_PUSHDOWN_STATUS_CHANGED;
        } else {
          batched_row_store_->set_end();
        }
        blockscan_state_ = MAX_STATE;
        break;
      }
    }
    LOG_DEBUG("[COLUMNSTORE] COScanner get_next_rows [end]", K(ret), K_(state), K_(blockscan_state),
              K_(current), K_(group_size), K_(end), K(batched_row_store_->is_end()));
  }
  LOG_TRACE("[COLUMNSTORE] COScanner get_next_rows", K(ret), K_(state), K_(blockscan_state), K_(current),
            K_(group_size), K_(end), K(batched_row_store_->is_end()), "can_blockscan", can_blockscan());
  return ret;
}

int ObCOSSTableRowScanner::init_row_scanner(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (nullptr == row_scanner_) {
    if (OB_ISNULL(row_scanner_ = OB_NEWx(ObSSTableRowScanner<ObCOPrefetcher>, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc row scanner", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_scanner_->init(param, context, table, query_range))) {
    LOG_WARN("Fail to init row scanner", K(ret), K(param), KPC(table));
  } else {
    range_ = static_cast<const blocksstable::ObDatumRange *>(query_range);
  }
  return ret;
}

int ObCOSSTableRowScanner::get_group_idx(int64_t &group_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != range_idx_ || nullptr == range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected range", K(ret), K(range_idx_), KP(range_));
  } else {
    group_idx = range_->get_group_idx();
  }
  return ret;
}

int ObCOSSTableRowScanner::init_cg_param_pool(ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  if (nullptr == cg_param_pool_) {
    if (OB_ISNULL(cg_param_pool_ = OB_NEWx(ObCGIterParamPool,
                                          context.stmt_allocator_,
                                          *context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg param map", K(ret));
    }
  }
  context.cg_param_pool_ = cg_param_pool_;
  return ret;
}

int ObCOSSTableRowScanner::init_rows_filter(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!context.block_row_store_->filter_pushdown())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, filter is not pushdown", K(ret));
  } else if (!context.block_row_store_->filter_is_null()) {
    if (nullptr == rows_filter_) {
      if (OB_ISNULL(rows_filter_ = OB_NEWx(ObCOSSTableRowsFilter, context.stmt_allocator_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc rows filter", K(ret));
      } else if (OB_FAIL(rows_filter_->init(row_param, context, table))) {
        LOG_WARN("Fail to init rows filter", K(ret), K(row_param), KPC(table));
      }
    } else if (OB_FAIL(rows_filter_->switch_context(row_param, context, table,
        column_group_cnt_ != static_cast<ObCOSSTableV2*>(table)->get_cs_meta().get_column_group_count()))) {
      LOG_WARN("Failed to switch context for filter", K(ret));
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::init_project_iter(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2* co_sstable = static_cast<ObCOSSTableV2*>(table);
  common::ObSEArray<ObTableIterParam*, 8> iter_params;
  if (OB_FAIL(construct_cg_iter_params(row_param, context, iter_params))) {
    LOG_WARN("Failed to construct cg scan params", K(ret));
  } else if (nullptr == project_iter_) {
    if (1 == iter_params.count()) {
      ObICGIterator *cg_scanner = nullptr;
      if (OB_FAIL(co_sstable->cg_scan(*iter_params.at(0), context, cg_scanner, true, false))) {
        LOG_WARN("Failed to cg scan", K(ret));
      } else {
        project_iter_ = cg_scanner;
        if (ObICGIterator::OB_CG_ROW_SCANNER == cg_scanner->get_type() ||
            ObICGIterator::OB_CG_GROUP_BY_SCANNER == cg_scanner->get_type()) {
          static_cast<ObCGRowScanner *>(cg_scanner)->set_project_type(nullptr == rows_filter_);
        }
      }
    } else if (OB_ISNULL(project_iter_ = OB_NEWx(ObCGTileScanner, context.stmt_allocator_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg tile scanner", K(ret));
    } else if (OB_FAIL(static_cast<ObCGTileScanner*>(project_iter_)->init(iter_params, false, nullptr == rows_filter_, context, co_sstable))) {
      LOG_WARN("Fail to init cg tile scanner", K(ret), K(iter_params));
    }
  } else if (OB_FAIL(ObCOSSTableRowsFilter::switch_context_for_cg_iter(true, false, nullptr == rows_filter_, co_sstable, context, iter_params,
      column_group_cnt_ != co_sstable->get_cs_meta().get_column_group_count(), project_iter_))) {
    LOG_WARN("Fail to switch context for cg iter", K(ret));
  }
  LOG_DEBUG("[COLUMNSTORE] init project iter", K(ret), KPC(project_iter_), K(row_param));
  return ret;
}

int ObCOSSTableRowScanner::init_project_iter_for_single_row(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2* co_sstable = static_cast<ObCOSSTableV2*>(table);
  common::ObSEArray<ObTableIterParam*, 8> iter_params;
  getter_projector_.set_allocator(context.stmt_allocator_);
  if (co_sstable->is_all_cg_base()) {
    // use all cg if exists for getter
  } else if (OB_FAIL(init_fixed_array_param(getter_projector_, row_param.get_out_col_cnt()))) {
    LOG_WARN("Failed to reserve getter projector", K(ret));
  } else if (OB_FAIL(construct_cg_iter_params_for_single_row(row_param, context, iter_params))) {
    LOG_WARN("Failed to construct cg scan params", K(ret));
  } else if (iter_params.empty()) {
    if (OB_NOT_NULL(getter_project_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected non nullptr getter_project_iter", K(ret), KP_(getter_project_iter));
    }
  } else if (nullptr == getter_project_iter_) {
    if (1 == iter_params.count()) {
      ObICGIterator *cg_scanner = nullptr;
      if (OB_FAIL(co_sstable->cg_scan(*iter_params.at(0), context, cg_scanner, true, true))) {
        LOG_WARN("Failed to init cg canner for single row", K(ret));
      } else {
        getter_project_iter_ = cg_scanner;
      }
    } else if (OB_ISNULL(getter_project_iter_ = OB_NEWx(ObCGTileScanner, context.stmt_allocator_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc cg tile scanner", K(ret));
    } else if (OB_FAIL(static_cast<ObCGTileScanner*>(getter_project_iter_)->init(iter_params, true, false, context, co_sstable))) {
      LOG_WARN("Failed to init cg tile scanner", K(ret), K(iter_params));
    }
  } else if (OB_FAIL(ObCOSSTableRowsFilter::switch_context_for_cg_iter(true, true, false, co_sstable, context, iter_params,
      column_group_cnt_ != co_sstable->get_cs_meta().get_column_group_count(), getter_project_iter_))) {
    LOG_WARN("Failed to switch context for cg iter", K(ret));
  }
  LOG_DEBUG("[COLUMNSTORE] init project iter for single row", K(ret), K_(getter_project_iter), K(row_param));
  return ret;
}

int ObCOSSTableRowScanner::construct_cg_iter_params_for_single_row(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    common::ObIArray<ObTableIterParam*> &iter_params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_param.is_valid() || nullptr == row_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_param));
  } else {
    ObTableIterParam* cg_param = nullptr;
    const common::ObIArray<int32_t> *access_cgs = nullptr;
    const int64_t rowkey_cnt = row_param.get_read_info()->get_rowkey_count();
    const ObColumnIndexArray &cols_index = row_param.get_read_info()->get_columns_index();
    if (OB_ISNULL(access_cgs = row_param.get_read_info()->get_cg_idxs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null access cg index", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_param.out_cols_project_->count(); ++i) {
        const int32_t col_offset = row_param.out_cols_project_->at(i);
        const int32_t col_index = cols_index.at(col_offset);
        sql::ObExpr* expr = row_param.output_exprs_ == nullptr ? nullptr : row_param.output_exprs_->at(i);
        if ((nullptr == expr || !is_group_idx_expr(expr)) && col_index >= rowkey_cnt) {
          int32_t cg_idx = access_cgs->at(col_offset);
          if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, row_param, expr, cg_param))) {
            LOG_WARN("Fail to get cg iter param", K(ret), K(i), K(cg_idx), K(row_param), KPC(access_cgs));
          } else if (OB_FAIL(iter_params.push_back(cg_param))) {
            LOG_WARN("Fail to push back cg iter param", K(ret), K(cg_param));
          } else if (OB_FAIL(getter_projector_.push_back(col_offset))) {
            LOG_WARN("Fail to push back projector idx", K(ret));
          }
          LOG_DEBUG("[COLUMNSTORE] cons one cg param", K(ret), K(cg_idx), KPC(cg_param));
        }
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::construct_cg_iter_params(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    common::ObIArray<ObTableIterParam*> &iter_params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_param.is_valid() ||
                  nullptr == row_param.output_exprs_ ||
                  nullptr == row_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_param));
  } else {
    ObTableIterParam* cg_param = nullptr;
    const common::ObIArray<int32_t> *access_cgs = nullptr;
    // Assert only one column in one column group
    if (row_param.enable_pd_aggregate()) {
      if (OB_FAIL(construct_cg_agg_iter_params(row_param, context, iter_params))) {
        LOG_WARN("Fail to cons agg iter_params", K(ret));
      }
    } else if (0 == row_param.output_exprs_->count()) {
      const uint32_t cg_idx = OB_CS_VIRTUAL_CG_IDX;
      if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, row_param, *row_param.output_exprs_,
                                                 cg_param, row_param.enable_pd_aggregate()))) {
        LOG_WARN("Fail to get cg iter param", K(ret), K(cg_idx), K(row_param));
      } else if (OB_FAIL(iter_params.push_back(cg_param))) {
        LOG_WARN("Fail to push back cg iter param", K(ret), K(cg_param));
      }
    } else if (OB_ISNULL(access_cgs = row_param.get_read_info()->get_cg_idxs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null access cg index", K(ret));
    } else {
      int32_t idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_param.output_exprs_->count(); ++i) {
        const bool need_iter_param = (nullptr == row_param.output_sel_mask_ || row_param.output_sel_mask_->at(i));
        if (!is_group_idx_expr(row_param.output_exprs_->at(i)) && need_iter_param) {
          int32_t cg_idx = access_cgs->at(row_param.out_cols_project_->at(i));
          if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, row_param, row_param.output_exprs_->at(i), cg_param))) {
            LOG_WARN("Fail to get cg iter param", K(ret), K(i), K(cg_idx), K(row_param), KPC(access_cgs));
          } else if (OB_FAIL(iter_params.push_back(cg_param))) {
            LOG_WARN("Fail to push back cg iter param", K(ret), K(cg_param));
          } else if (row_param.enable_pd_group_by() &&
                     row_param.out_cols_project_->at(i) == row_param.group_by_cols_project_->at(0)) {
            group_by_project_idx_ = idx;
          }
          idx++;
          LOG_DEBUG("[COLUMNSTORE] cons one cg param", K(ret), K(cg_idx), KPC(cg_param));
        }
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::construct_cg_agg_iter_params(
    const ObTableIterParam &row_param,
    ObTableAccessContext &context,
    common::ObIArray<ObTableIterParam*> &iter_params)
{
  int ret = OB_SUCCESS;
  ObTableIterParam* cg_param = nullptr;
  if (OB_UNLIKELY(nullptr == row_param.aggregate_exprs_ ||
                  row_param.aggregate_exprs_->count() < 1 ||
                  nullptr == row_param.output_exprs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid agg argument", K(ret), K(row_param));
  } else if (0 == row_param.output_exprs_->count()) {
    // only COUNT(*) and without filter
    const uint32_t cg_idx = OB_CS_VIRTUAL_CG_IDX;
    if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, row_param, *row_param.aggregate_exprs_,
        cg_param, row_param.enable_pd_aggregate()))) {
      LOG_WARN("Fail to get cg iter param", K(ret), K(cg_idx), K(row_param));
    } else if (OB_FAIL(iter_params.push_back(cg_param))) {
      LOG_WARN("Fail to push back cg iter param", K(ret), K(cg_param));
    }
    LOG_DEBUG("[COLUMNSTORE] cons one cg param", K(ret), K(cg_idx), KPC(cg_param));
  } else {
    int64_t agg_cnt = 0;
    common::ObSEArray<sql::ObExpr*, 4> exprs;
    const int64_t access_col_cnt =  row_param.output_exprs_->count();
    const common::ObIArray<int32_t> *access_cgs = row_param.get_read_info()->get_cg_idxs();
    if (OB_ISNULL(access_cgs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null cg indexs", K(ret), KPC(row_param.get_read_info()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < access_col_cnt; ++i) {
      exprs.reuse();
      const int32_t out_col_offset = row_param.out_cols_project_->at(i);
      const uint32_t cg_idx = access_cgs->at(out_col_offset);
      for (int64_t j = 0; OB_SUCC(ret) && j < row_param.agg_cols_project_->count(); ++j) {
        const int32_t agg_col_offset = row_param.agg_cols_project_->at(j);
        if ((0 == i && OB_COUNT_AGG_PD_COLUMN_ID == agg_col_offset) || out_col_offset == agg_col_offset) {
          if (OB_FAIL(exprs.push_back(row_param.aggregate_exprs_->at(j)))) {
            LOG_WARN("Fail to push back", K(ret), K(i), K(j), K(access_col_cnt), K(access_cgs));
          } else {
            agg_cnt++;
          }
        }
      }
      if (OB_FAIL(ret) || 0 == exprs.count()) {
      } else if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, row_param, exprs, cg_param, row_param.enable_pd_aggregate()))) {
        LOG_WARN("Fail to get cg iter param", K(ret), K(cg_idx), K(row_param));
      } else if (OB_FAIL(iter_params.push_back(cg_param))) {
        LOG_WARN("Fail to push back cg iter param", K(ret), K(cg_param));
      }
      LOG_DEBUG("[COLUMNSTORE] cons one cg param", K(ret), K(cg_idx), KPC(cg_param));
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(agg_cnt != row_param.aggregate_exprs_->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected touched agg cnt", K(ret), K(agg_cnt),
                  K(row_param.aggregate_exprs_->count()), K(access_cgs));
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::filter_rows(BlockScanState &blockscan_state)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("[COLUMNSTORE] COScanner filter_rows [start]", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  if (iter_param_->has_lob_column_out()) {
    access_ctx_->reuse_lob_locator_helper();
  }
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("query interrupt", K(ret));
  } else if (nullptr != group_by_cell_) {
    ret = filter_group_by_rows();
  } else if (nullptr != access_ctx_->limit_param_) {
    ret = filter_rows_with_limit(blockscan_state);
  } else {
    ret = filter_rows_without_limit(blockscan_state);
  }
  if (iter_param_->has_lob_column_out()) {
    access_ctx_->reuse_lob_locator_helper();
  }
  LOG_TRACE("[COLUMNSTORE] COScanner filter_rows [end]", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  return ret;
}

// TODO(yht146439) merge filter_rows_with_limit and filter_rows_without_limit
int ObCOSSTableRowScanner::filter_rows_with_limit(BlockScanState &blockscan_state)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("COScanner filter_rows_with_limit begin", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  while (OB_SUCC(ret) && !is_limit_end_) {
    ObCSRowId begin = current_;
    const ObCGBitmap* result_bitmap = nullptr;
    if (OB_FAIL(inner_filter(begin, group_size_, result_bitmap, blockscan_state))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to inner filter", K(ret));
      }
    } else if (nullptr != result_bitmap && result_bitmap->is_all_false()) {
      update_current(group_size_);
    } else {
      int64_t begin_idx = begin;
      int64_t end_idx = begin + group_size_;
      if (OB_FAIL(check_limit(result_bitmap, is_limit_end_, begin_idx, end_idx))) {
        LOG_WARN("Fail to check limit", K(ret), K(begin));
      } else if (begin_idx >= end_idx) {
        // skip this group
        update_current(group_size_);
      } else if (OB_FAIL(project_iter_->locate(ObCSRange(begin_idx, end_idx - begin_idx), result_bitmap))) {
        LOG_WARN("Fail to locate project iter", K(ret), K(begin), K(current_), K(group_size_));
      } else {
        break;
      }
    }
  }
  LOG_DEBUG("COScanner filter_rows_with_limit end", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  return ret;
}

// do filter for each group, continues if groups are true or false, for the group which is not continuous true,
// store it in pending_end_row_id_ and will be located in next round
int ObCOSSTableRowScanner::filter_rows_without_limit(BlockScanState &blockscan_state)
{
  int ret = OB_SUCCESS;
  bool need_do_filter = true;
  ObCSRowId current_start_row_id = current_;
  ObCSRowId continuous_end_row_id = OB_INVALID_CS_ROW_ID;
  const ObCGBitmap* result_bitmap = nullptr;
  LOG_DEBUG("COScanner filter_rows_with_limit begin", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  while (OB_SUCC(ret) && need_do_filter) {
    int64_t current_group_size = 0;
    if (OB_INVALID_CS_ROW_ID != pending_end_row_id_) {
      group_size_ = reverse_scan_ ? current_ - pending_end_row_id_ + 1 : pending_end_row_id_ - current_ + 1;
      if (OB_UNLIKELY(group_size_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected pending range", K(ret), K(pending_end_row_id_), K(current_));
      } else if (nullptr != rows_filter_ && OB_ISNULL(result_bitmap = rows_filter_->get_result_bitmap())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected result bitmap", K(ret), KPC(rows_filter_));
      } else if (nullptr != result_bitmap && result_bitmap->is_all_false()) {
        update_current(group_size_);
        current_start_row_id = current_;
      } else {
        need_do_filter = false;
      }
      pending_end_row_id_ = OB_INVALID_CS_ROW_ID;
    }
    if (OB_FAIL(ret) || !need_do_filter) {
    } else if (OB_FAIL(inner_filter(current_start_row_id, current_group_size, result_bitmap, blockscan_state))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to inner filter", K(ret));
      }
    } else if (OB_FAIL(update_continuous_range(current_start_row_id, current_group_size,
               result_bitmap, continuous_end_row_id, need_do_filter))) {
      LOG_WARN("Fail to update continuous range", K(ret));
    } else if (need_do_filter) {
    } else if (OB_INVALID_CS_ROW_ID != continuous_end_row_id) {
      group_size_ = reverse_scan_ ? current_ - continuous_end_row_id + 1 : continuous_end_row_id - current_ + 1;
      result_bitmap = nullptr;
      if (OB_UNLIKELY(group_size_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected continuous range", K(ret), K(continuous_end_row_id), K(current_));
      }
    } else if (OB_UNLIKELY(OB_INVALID_CS_ROW_ID != pending_end_row_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected continuous range", K(ret), K(pending_end_row_id_));
    } else {
      group_size_ = current_group_size;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(project_iter_->locate(
              ObCSRange(reverse_scan_ ? current_ - group_size_ + 1 : current_, group_size_),
              result_bitmap))) {
    LOG_WARN("Fail to locate", K(ret), K(current_), K(group_size_), KP(result_bitmap));
  }
  LOG_DEBUG("COScanner filter_rows_with_limit end", K(ret), K_(state), K_(blockscan_state),
            K_(current), K_(group_size), K_(end));
  return ret;
}

int ObCOSSTableRowScanner::inner_filter(
    ObCSRowId &begin,
    int64_t &group_size,
    const ObCGBitmap *&result_bitmap,
    BlockScanState &blockscan_state)
{
  int ret = OB_SUCCESS;
  if (can_forward_row_scanner() &&
      OB_FAIL(row_scanner_->forward_blockscan(end_, blockscan_state, begin))) {
    LOG_WARN("Fail to forward blockscan border", K(ret));
  } else if (end_of_scan()) {
    LOG_DEBUG("cur scan finished, update state", K(blockscan_state_), K(state_), KPC(this));
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_group_size(begin, group_size))) {
    LOG_WARN("Fail to get filter row count", K(ret));
  } else if (OB_UNLIKELY(group_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row count to do filter", K(ret), K(group_size));
  } else if (FALSE_IT(begin = reverse_scan_ ? begin - group_size + 1 : begin)) {
  } else if (nullptr != rows_filter_) {
    if (OB_FAIL(rows_filter_->apply(ObCSRange(begin, group_size)))) {
      LOG_WARN("Fail to apply rows filter", K(ret), K(begin), K(group_size));
    } else if (OB_ISNULL(result_bitmap = rows_filter_->get_result_bitmap())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected result bitmap", K(ret), KPC(rows_filter_));
    } else {
      int64_t select_cnt = result_bitmap->popcnt();
      EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, select_cnt);
    }
  } else {
    EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, group_size);
  }
  if (OB_SUCC(ret)) {
    EVENT_ADD(ObStatEventIds::BLOCKSCAN_ROW_CNT, group_size);
    access_ctx_->table_store_stat_.logical_read_cnt_ += group_size;
    access_ctx_->table_store_stat_.physical_read_cnt_ += group_size;
    LOG_TRACE("[COLUMNSTORE] COSSTableRowScanner inner filter", K(ret), "begin", begin, "count", group_size,
              "filtered", nullptr == rows_filter_ ? 0 : 1, "popcnt", nullptr == result_bitmap ? group_size : result_bitmap->popcnt());
  }
  return ret;
}

int ObCOSSTableRowScanner::update_continuous_range(
    ObCSRowId &current_start_row_id,
    const int64_t current_group_size,
    const ObCGBitmap *result_bitmap,
    ObCSRowId &continuous_end_row_id,
    bool &continue_filter)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_CS_ROW_ID != continuous_end_row_id &&
      ((!reverse_scan_ && current_start_row_id != continuous_end_row_id + 1) ||
       (reverse_scan_ && current_start_row_id + current_group_size != continuous_end_row_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected range, not continuous", K(ret), K(current_start_row_id), K(continuous_end_row_id));
  } else {
    continue_filter = true;
    bool group_is_true = true;
    if (nullptr != result_bitmap) {
      group_is_true = result_bitmap->is_all_true();
    }
    bool filter_tree_can_continuous =
        rows_filter_ == nullptr ? true : rows_filter_->can_continuous_filter();
    if (group_is_true && filter_tree_can_continuous) {
      // current group is true, continue do filter if not reach end
      if (reverse_scan_) {
        continuous_end_row_id = current_start_row_id;
        current_start_row_id = continuous_end_row_id - 1;
        continue_filter = current_start_row_id >= end_;
      } else {
        continuous_end_row_id = current_start_row_id + current_group_size - 1;
        current_start_row_id = continuous_end_row_id + 1;
        continue_filter = current_start_row_id <= end_;
      }
    } else if ((nullptr != result_bitmap && result_bitmap->is_all_false()) && OB_INVALID_CS_ROW_ID == continuous_end_row_id) {
      // current group is false and no continuous true range before, skip this group and continue do filter
      update_current(current_group_size);
      current_start_row_id = current_;
    } else {
      continue_filter = false;
    }
    if (!group_is_true && !continue_filter && OB_INVALID_CS_ROW_ID != continuous_end_row_id) {
      pending_end_row_id_ = reverse_scan_ ? current_start_row_id : current_start_row_id + current_group_size - 1;
    } else {
      // no continuous true range before, will project current group
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::fetch_rows()
{
  int ret = OB_SUCCESS;
  if (iter_param_->has_lob_column_out()) {
    access_ctx_->reuse_lob_locator_helper();
  }
  if (nullptr == group_by_cell_) {
    ret = fetch_output_rows();
  } else {
    ret = fetch_group_by_rows();
  }
  return ret;
}

int ObCOSSTableRowScanner::fetch_output_rows()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !batched_row_store_->is_end()) {
    uint64_t count = 0;
    uint64_t batch_size = iter_param_->op_->eval_ctx_.get_batch_size();
    if (nullptr != access_ctx_->limit_param_)  {
      batch_size = MIN(batch_size,
                       access_ctx_->limit_param_->offset_ + access_ctx_->limit_param_->limit_ - access_ctx_->out_cnt_);
    }
    if (OB_FAIL(project_iter_->get_next_rows(count, batch_size))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get rows from project iter", K(ret));
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret || OB_ITER_END == ret)) {
      if (count > 0) {
        int64_t group_idx = 0;
        access_ctx_->out_cnt_ += count;
        if (OB_FAIL(get_group_idx(group_idx))) {
          LOG_WARN("Fail to get group idx", K(ret));
        } else if (OB_FAIL(batched_row_store_->fill_rows(group_idx, count))) {
          LOG_WARN("Fail to fill rows", K(ret), K(group_idx), K(count));
        }
      } else if (OB_SUCCESS == ret) {
        continue;
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::inner_get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObCSRowId row_id;
  const ObDatumRow *cg_datum_row = nullptr;
  if (OB_UNLIKELY(OB_INVALID_CS_ROW_ID != current_ &&
                  ((reverse_scan_ && current_ > end_) || (!reverse_scan_ && current_ < end_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(reverse_scan_), K(current_), K(end_));
  } else if (END == state_ && SCAN_FINISH == blockscan_state_) {
    ret = OB_ITER_END;
    // 1. Fetch rowkey columns from rowkey column group.
  } else if (OB_FAIL(row_scanner_->inner_get_next_row_with_row_id(store_row, row_id))) {
    if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret && OB_ITER_END != ret)) {
      LOG_WARN("Fail to get next row from row scanner", K(ret));
    } else if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
      state_ = BEGIN;
      blockscan_state_ = BLOCKSCAN_RANGE;
    }
  } else if (nullptr == getter_project_iter_) {
    // All columns have been fetched in the row scanner.
  } else {
    // 3. Seek position in cg iterator.
    if (OB_FAIL(getter_project_iter_->locate({row_id, 1}))) {
      LOG_WARN("Failed to locate", K(ret), KPC_(getter_project_iter), K(row_id));
      // 4. Fetch one row in all cg iterators, datum will be set in gext_next_row.
    } else if (OB_FAIL(getter_project_iter_->get_next_row(cg_datum_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected OB_ITER_END in getter_project_iter ", K(ret), KPC_(getter_project_iter), K(row_id));
      } else {
        LOG_WARN("Failed to get next row", K(ret), KPC_(getter_project_iter), K(row_id));
      }
    } else if (OB_UNLIKELY(getter_projector_.count() != cg_datum_row->count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected datum row count", K(ret), K(getter_projector_.count()), K(cg_datum_row->count_));
    } else {
      for (int64_t i = 0; i < cg_datum_row->count_; ++i) {
        store_row->storage_datums_[getter_projector_.at(i)] = cg_datum_row->storage_datums_[i];
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (!iter_param_->vectorized_enabled_) {
    // TODO(hanling): Support block scan for non-vectorized query.
  } else if (OB_FAIL(row_scanner_->refresh_blockscan_checker(rowkey))) {
    LOG_WARN("Fail to refresh row scanner blockscan", K(ret), K(rowkey));
  } else {
    state_ = BEGIN;
  }
  LOG_DEBUG("refresh blockscan", K(rowkey), K(current_), K(end_));
  return ret;
}

int ObCOSSTableRowScanner::get_next_group_size(const ObCSRowId begin, int64_t &group_size)
{
  int ret = OB_SUCCESS;
  group_size = 0;
  if (reverse_scan_) {
    if (begin < end_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected rowid", K(begin), K(end_));
    } else {
      group_size = MIN(batch_size_, begin - end_ + 1);
    }
  } else if (begin > end_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected rowid", K(begin), K(end_));
  } else {
    group_size = MIN(batch_size_, end_ - begin + 1);
  }
  return ret;
}

int ObCOSSTableRowScanner::check_limit(
    const ObCGBitmap *bitmap,
    bool &limit_end,
    ObCSRowId &begin_idx,
    ObCSRowId &end_idx)
{
  int ret = OB_SUCCESS;
  limit_end = false;
  if (nullptr != access_ctx_->limit_param_) {
    int64_t row_count = nullptr == bitmap ? end_idx - begin_idx : bitmap->popcnt();
    if (access_ctx_->out_cnt_ + row_count - access_ctx_->limit_param_->offset_ >= access_ctx_->limit_param_->limit_) {
      limit_end = true;
    }
    if (access_ctx_->out_cnt_ + row_count <= access_ctx_->limit_param_->offset_) {
      end_idx = begin_idx;
      access_ctx_->out_cnt_ += row_count;
    } else {
      int64_t skip_count = MAX(0, access_ctx_->limit_param_->offset_ - access_ctx_->out_cnt_);
      access_ctx_->out_cnt_ += skip_count;
      if (0 == skip_count) {
      } else if (nullptr == bitmap) {
        if (!reverse_scan_) {
          begin_idx += skip_count;
        } else {
          end_idx -= skip_count;
        }
      } else {
        // TODO optimize this
        if (!reverse_scan_) {
          for (; 0 != skip_count && begin_idx < end_idx; begin_idx++) {
            if (bitmap->test(begin_idx)) {
              skip_count--;
            }
          }
        } else {
          for (; 0 != skip_count && end_idx > begin_idx; end_idx--) {
            if (bitmap->test(end_idx - 1)) {
              skip_count--;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::filter_group_by_rows()
{
  int ret = OB_SUCCESS;
  const ObCGBitmap *result_bitmap = nullptr;
  ObICGGroupByProcessor *group_by_processor = group_by_iters_.at(0);
  while(OB_SUCC(ret)) {
    if (can_forward_row_scanner() &&
        OB_FAIL(row_scanner_->forward_blockscan(end_, blockscan_state_, current_))) {
      LOG_WARN("Fail to forward blockscan border", K(ret));
    } else if (end_of_scan()) {
      LOG_DEBUG("cur scan finished, update state", K(blockscan_state_), K(state_), KPC(this));
      ret = OB_ITER_END;
    } else if (OB_FAIL(group_by_processor->locate_micro_index(ObCSRange(current_, end_ - current_ + 1)))) {
      LOG_WARN("Failed to locate", K(ret));
    } else if (OB_FAIL(group_by_processor->decide_group_size(group_size_))) {
      LOG_WARN("Failed to decide group size", K(ret));
    } else if (nullptr != rows_filter_) {
      if (OB_FAIL(rows_filter_->apply(ObCSRange(current_, group_size_)))) {
        LOG_WARN("Fail to apply rows filter", K(ret), K(current_), K(group_size_));
      } else if (OB_ISNULL(result_bitmap = rows_filter_->get_result_bitmap())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected result bitmap", K(ret), KPC(rows_filter_));
      } else {
        EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, result_bitmap->popcnt());
        if (result_bitmap->is_all_false()) {
          update_current(group_size_);
          continue;
        }
      }
    } else {
      EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, group_size_);
    }
    if (OB_SUCC(ret)) {
      break;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(project_iter_->locate(
              ObCSRange(current_, group_size_),
              result_bitmap))) {
    LOG_WARN("Fail to locate", K(ret), K(current_), K(group_size_), KP(result_bitmap));
  } else {
    is_new_group_ = true;
    EVENT_ADD(ObStatEventIds::BLOCKSCAN_ROW_CNT, group_size_);
    access_ctx_->table_store_stat_.logical_read_cnt_ += group_size_;
    access_ctx_->table_store_stat_.physical_read_cnt_ += group_size_;
  }
  LOG_TRACE("[GROUP BY PUSHDOWN]", K(ret), K(current_), K(end_), K(blockscan_state_),
            "popcnt", nullptr == result_bitmap ? group_size_ : result_bitmap->popcnt());
  return ret;
}

int ObCOSSTableRowScanner::fetch_group_by_rows()
{
  int ret = OB_SUCCESS;
  const int32_t group_by_col_offset = 0; // TODO(yht146439) multiple columns in cg
  ObVectorStore *vector_store = dynamic_cast<ObVectorStore *>(batched_row_store_);
  ObICGGroupByProcessor *group_by_processor = group_by_iters_.at(0);
  bool can_group_by = false;
  bool already_init_uniform = false;
  sql::ObEvalCtx &eval_ctx = iter_param_->op_->get_eval_ctx();
  if (group_by_cell_->is_processing()) {
    can_group_by = true;
  } else {
    can_group_by = is_new_group_;
    if (can_group_by && OB_FAIL(group_by_processor->decide_can_group_by(group_by_col_offset, can_group_by))) {
      LOG_WARN("Failed to check group by info", K(ret));
    }
    is_new_group_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (can_group_by) {
    int64_t output_cnt = 0;
    if (!group_by_cell_->is_processing()) {
      if (iter_param_->use_uniform_format() &&
          OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx))) {
        LOG_WARN("Failed to init uniform header", K(ret));
      } else if (OB_FAIL(do_group_by())) {
        LOG_WARN("Failed to do group by", K(ret));
      } else if (!group_by_cell_->is_exceed_sql_batch()) {
        output_cnt = group_by_cell_->get_distinct_cnt();
      } else {
        group_by_cell_->reset_projected_cnt();
        group_by_cell_->set_is_processing(true);
        already_init_uniform = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (group_by_cell_->is_processing()) {
      if (!already_init_uniform &&
          iter_param_->use_uniform_format() &&
          OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx))) {
        LOG_WARN("Failed to init uniform header", K(ret));
      } else if (OB_FAIL(group_by_cell_->output_extra_group_by_result(output_cnt))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          group_by_cell_->set_is_processing(false);
        } else {
          LOG_WARN("Failed to fill rows", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(group_by_cell_->pad_column_in_group_by(output_cnt))) {
      LOG_WARN("Failed to pad column in group by", K(ret), K(output_cnt));
    } else {
      int64_t group_idx = 0;
      if (OB_FAIL(get_group_idx(group_idx))) {
        LOG_WARN("Fail to get group idx", K(ret));
      } else if (OB_FAIL(batched_row_store_->fill_rows(group_idx, output_cnt))) {
        LOG_WARN("Fail to fill rows", K(ret), K(group_idx), K(output_cnt));
      } else if (!group_by_cell_->is_processing()) {
        ret = OB_ITER_END;
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), KPC(group_by_cell_));
  } else if (OB_FAIL(fetch_output_rows())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to fetch output rows", K(ret));
    }
  } else if (iter_param_->use_uniform_format() &&
      OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx, false))) {
    LOG_WARN("Failed to init uniform header", K(ret));
  } else if (OB_FAIL(group_by_cell_->copy_output_rows(vector_store->get_row_count()))) {
    LOG_WARN("Failed to copy output rows", K(ret));
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), KPC(group_by_cell_));
  return ret;
}

int ObCOSSTableRowScanner::do_group_by()
{
  int ret = OB_SUCCESS;
  const int32_t group_by_col_offset = 0; // TODO(yht146439) multiple columns in cg
  ObICGGroupByProcessor *group_by_processor = group_by_iters_.at(0);
  if (OB_FAIL(group_by_processor->read_distinct(group_by_col_offset))) {
    LOG_WARN("Failed to read distinct", K(ret));
  } else if (group_by_cell_->need_read_reference()) {
    const bool need_extract_distinct = group_by_cell_->need_extract_distinct();
    const bool need_do_aggregate = group_by_cell_->need_do_aggregate();
    if (need_extract_distinct) {
      group_by_cell_->set_distinct_cnt(0);
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(group_by_processor->read_reference(group_by_col_offset))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to read ref", K(ret));
        }
      } else if (0 == group_by_cell_->get_ref_cnt()) {
        continue;
      } else if (need_extract_distinct && OB_FAIL(group_by_cell_->extract_distinct())) {
        LOG_WARN("Failed to extract distinct", K(ret));
      } else if (need_do_aggregate) {
        if (OB_FAIL(group_by_cell_->check_distinct_and_ref_valid())) {
          LOG_WARN("Failed to check valid", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < group_by_iters_.count(); ++i) {
          if (OB_FAIL(group_by_iters_.at(i)->calc_aggregate(0 == i/*is_group_by_col*/))) {
            LOG_WARN("Failed to get next group by rows", K(ret), K(i));
          }
        }
      }
    }
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Unexpected ret, should be OB_ITER_END", K(ret));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(group_by_cell_->collect_result())) {
      LOG_WARN("Failed to collect result", K(ret));
    }
  }
  return ret;
}

int ObCOSSTableRowScanner::init_group_by_info(ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;

  ObICGGroupByProcessor *group_by_processor = nullptr;
  ObVectorStore *vector_store = static_cast<ObVectorStore*>(context.block_row_store_);
  group_by_cell_ = vector_store->get_group_by_cell();
  if (OB_UNLIKELY(context.query_flag_.is_reverse_scan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, group by can not pushdown in reverse scan", K(ret));
  } else if (group_by_iters_.count() > 0) {
  } else if (ObICGIterator::OB_CG_TILE_SCANNER == project_iter_->get_type()) {
    ObCGTileScanner *tile_iter = static_cast<ObCGTileScanner*>(project_iter_);
    ObIArray<ObICGIterator*> &cg_scanners = tile_iter->get_inner_cg_scanners();
    // push cg scanner of group by col at first position
    if (OB_UNLIKELY(group_by_project_idx_ >= cg_scanners.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected group by project idx", K(ret), K(group_by_project_idx_), K(cg_scanners.count()));
    } else if (OB_FAIL(push_group_by_processor(cg_scanners.at(group_by_project_idx_)))) {
      LOG_WARN("Failed to push group by processor", K(ret), K(group_by_project_idx_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_scanners.count(); ++i) {
      if (i == group_by_project_idx_) {
      } else if (OB_FAIL(push_group_by_processor(cg_scanners.at(i)))) {
        LOG_WARN("Failed to push group by processor", K(ret), K(i));
      }
    }
  } else if (OB_FAIL(push_group_by_processor(project_iter_))) {
    LOG_WARN("Failed to push group by processor", K(ret));
  }
  LOG_TRACE("[GROUP BY PUSHDOWN]", K(ret), K(group_by_project_idx_), K(group_by_iters_), KPC(group_by_cell_));
  return ret;
}

int ObCOSSTableRowScanner::push_group_by_processor(ObICGIterator *cg_iterator)
{
  int ret = OB_SUCCESS;
  ObICGGroupByProcessor *group_by_processor = nullptr;
  if (OB_ISNULL(cg_iterator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret));
  } else if (OB_UNLIKELY(!ObICGIterator::is_valid_group_by_cg_scanner(cg_iterator->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cg scanner", K(ret), K(cg_iterator->get_type()));
  } else if (OB_ISNULL(group_by_processor = dynamic_cast<ObICGGroupByProcessor*>(cg_iterator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null group_by_processor", K(ret), KPC(cg_iterator));
  } else if (OB_FAIL(group_by_processor->init_group_by_info())) {
    LOG_WARN("Failed to init group by info", K(ret));
  } else if (OB_FAIL(group_by_iters_.push_back(group_by_processor))) {
    LOG_WARN("Failed to push back", K(ret));
  }
  return ret;
}

}
}
