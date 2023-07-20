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

#include "ob_multiple_merge.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_compare.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_storage_util.h"
#include "ob_vector_store.h"
#include "ob_aggregated_store.h"
#include "ob_dml_param.h"
#include "lib/worker.h"
#include "sql/engine/ob_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;
namespace storage
{

ObMultipleMerge::ObMultipleMerge()
    : padding_allocator_("MULTI_MERGE_PAD"),
      iters_(),
      iter_pool_(NULL),
      access_param_(NULL),
      access_ctx_(NULL),
      tables_(),
      cur_row_(),
      unprojected_row_(),
      out_cols_projector_(NULL),
      curr_scan_index_(0),
      curr_rowkey_(),
      nop_pos_(),
      row_stat_(),
      scan_cnt_(0),
      need_padding_(false),
      need_fill_default_(false),
      need_fill_virtual_columns_(false),
      need_output_row_with_nop_(false),
      inited_(false),
      range_idx_delta_(0),
      get_table_param_(),
      read_memtable_only_(false),
      block_row_store_(nullptr),
      out_project_cols_(),
      lob_reader_(),
      scan_state_(ScanState::NONE)
{
}

ObMultipleMerge::~ObMultipleMerge()
{
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (OB_NOT_NULL(iters_.at(i))) {
      iters_.at(i)->~ObStoreRowIterator();
    }
  }
  if (nullptr != block_row_store_) {
    block_row_store_->~ObBlockRowStore();
    if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
      access_ctx_->stmt_allocator_->free(block_row_store_);
    }
    block_row_store_ = nullptr;
  }
  if (nullptr != iter_pool_) {
    iter_pool_->~ObStoreRowIterPool();
    if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
      access_ctx_->stmt_allocator_->free(iter_pool_);
    }
    iter_pool_ = nullptr;
  }
}

int ObMultipleMerge::init(
    const ObTableAccessParam &param,
    ObTableAccessContext &context,
    const ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMultipleMerge has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())
          || OB_UNLIKELY(!context.is_valid())
          || OB_UNLIKELY(!get_table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(param), K(context), K(get_table_param));
  } else if (OB_FAIL(cur_row_.init(*context.stmt_allocator_, param.get_max_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(unprojected_row_.init(*context.stmt_allocator_, param.get_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(*context.stmt_allocator_, param.get_max_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Fail to init nop pos, ", K(ret));
  } else if (NULL != param.get_op() && (NULL == param.output_exprs_ || NULL == param.row2exprs_projector_
              || OB_FAIL(param.row2exprs_projector_->init(
                      *param.output_exprs_,
                      *param.get_op(),
                      *param.iter_param_.out_cols_project_)))) {
    if (OB_SUCCESS == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output expr is NULL or row2exprs_projector_ is NULL", K(ret), K(param));
    } else {
      LOG_WARN("init row to expr projector failed", K(ret));
    }
  } else if (OB_FAIL(init_lob_reader(param.iter_param_, context))) {
    LOG_WARN("Fail to add lob reader", K(ret));
  }
  if (OB_SUCC(ret)) {
    need_padding_ = is_pad_char_to_full_length(context.sql_mode_);
    need_fill_default_ = true;
    need_output_row_with_nop_ = true;
    need_fill_virtual_columns_ = NULL != param.row2exprs_projector_ && param.row2exprs_projector_->has_virtual();
    iters_.reuse();
    access_param_ = &param;
    access_ctx_ = &context;
    row_stat_.reset();
    cur_row_.count_ = access_param_->iter_param_.out_cols_project_->count();
    scan_state_ = ScanState::NONE;
    read_memtable_only_ = false;
    for (int64_t i = cur_row_.get_column_count(); i < param.get_out_col_cnt(); ++i) {
      cur_row_.storage_datums_[i].set_nop();
    }
    unprojected_row_.count_ = 0;
    get_table_param_ = get_table_param;
    if (OB_SUCC(ret)) {
      const ObITableReadInfo *read_info = access_param_->iter_param_.get_read_info();
      if (OB_ISNULL(read_info)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null read_info", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param.iter_param_.out_cols_project_->count(); i++) {
          if (OB_FAIL(out_project_cols_.push_back(read_info->get_columns_desc().at(param.iter_param_.out_cols_project_->at(i))))) {
            STORAGE_LOG(WARN, "Failed to push back col desc", K(ret));
          } else if (out_project_cols_.at(i).col_type_.is_lob_storage()) {
            out_project_cols_.at(i).col_type_.set_has_lob_header();
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_read_tables())) {
      STORAGE_LOG(WARN, "fail to prepare read tables", K(ret));
    } else if (OB_FAIL(alloc_row_store(context, param))) {
      LOG_WARN("fail to alloc row store", K(ret));
    } else if (param.iter_param_.is_use_iter_pool() && OB_FAIL(alloc_iter_pool(*context.stmt_allocator_))) {
      LOG_WARN("Failed to init iter pool", K(ret));
    } else {
      access_ctx_->iter_pool_ = iter_pool_;
      access_ctx_->block_row_store_ = block_row_store_;
      inited_ = true;
      LOG_TRACE("succ to init multiple merge", K(*this));
    }
  }
  LOG_DEBUG("init multiple merge", K(ret), KP(this), K(param), K(context), K(get_table_param));
  return ret;
}

int ObMultipleMerge::switch_param(
    const ObTableAccessParam &param,
    ObTableAccessContext &context,
    const ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  access_param_ = &param;
  access_ctx_ = &context;
  get_table_param_ = get_table_param;

  if (OB_FAIL(prepare_read_tables())) {
    STORAGE_LOG(WARN, "Failed to prepare read tables", K(ret), K(*this));
  } else {
    if (OB_FAIL(init_lob_reader(param.iter_param_, context))) {
      STORAGE_LOG(WARN, "Failed to init read tables", K(ret), K(*this));
    }
  }
  STORAGE_LOG(TRACE, "switch param", K(ret), KP(this), K(param), K(context), K(get_table_param));
  return ret;
}

int ObMultipleMerge::reset_tables()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not inited", K(ret));
  } else if (OB_FAIL(prepare())) {
    STORAGE_LOG(WARN, "fail to prepare", K(ret));
  } else if (OB_FAIL(calc_scan_range())) {
    STORAGE_LOG(WARN, "fail to calculate scan range", K(ret));
  } else if (OB_FAIL(is_range_valid())) {
    // skip construct iters if range is not valid any more
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to check is range valid", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(construct_iters())) {
    STORAGE_LOG(WARN, "fail to construct iters", K(ret));
  } else {
    curr_rowkey_.reset();
    curr_scan_index_ = 0;
  }

  return ret;
}

int ObMultipleMerge::save_curr_rowkey()
{
  int ret = OB_SUCCESS;
  // TODO remove this after delete store_rowkey from memtable
  const ObColDescIArray *rowkey_col_descs = nullptr;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else if (ScanState::BATCH == scan_state_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "can not do refresh in batch", K(ret), K(scan_state_),
                K_(unprojected_row), KPC_(block_row_store));
  } else if (unprojected_row_.is_valid() && ScanState::NONE != scan_state_) {
    ObDatumRowkey tmp_rowkey;
    if (OB_FAIL(tmp_rowkey.assign(unprojected_row_.storage_datums_,
                                  access_param_->iter_param_.get_schema_rowkey_count()))) {
      STORAGE_LOG(WARN, "Failed to assign tmp rowkey", K(ret), K_(unprojected_row));
    } else if (OB_FAIL(tmp_rowkey.deep_copy(curr_rowkey_, *access_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy rowkey", K(ret));
    } else if (OB_ISNULL(rowkey_col_descs = access_param_->iter_param_.get_out_col_descs())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
    } else if (OB_FAIL(curr_rowkey_.prepare_memtable_readable(*rowkey_col_descs, *access_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(curr_rowkey_));
    } else {
      curr_scan_index_ = unprojected_row_.scan_index_;
      STORAGE_LOG(DEBUG, "save current rowkey", K(tmp_rowkey), K(curr_scan_index_));
    }
  } else {
    curr_rowkey_.reset();
    curr_scan_index_ = 0;
  }

  return ret;
}

int ObMultipleMerge::project_row(const ObDatumRow &unprojected_row,
    const ObIArray<int32_t> *projector,
    const int64_t range_idx_delta,
    ObDatumRow &projected_row)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  projected_row.row_flag_ = unprojected_row.row_flag_;
  projected_row.mvcc_row_flag_ = unprojected_row.mvcc_row_flag_;
  projected_row.scan_index_ = unprojected_row.scan_index_ + range_idx_delta;
  projected_row.snapshot_version_ = unprojected_row.snapshot_version_;
  projected_row.group_idx_ = unprojected_row.group_idx_;
  nop_pos_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < projected_row.get_column_count(); ++i) {
    idx = nullptr == projector ? i : projector->at(i);
    if (OB_UNLIKELY(idx >= unprojected_row.get_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected project idx", K(ret), K(unprojected_row), K(projected_row), KPC(projector));
    } else if (idx >= 0) {
      projected_row.storage_datums_[i] = unprojected_row.storage_datums_[idx];
    } else {
      projected_row.storage_datums_[i].set_nop();
    }
    if (OB_SUCC(ret) && projected_row.storage_datums_[i].is_nop()) {
      nop_pos_.nops_[nop_pos_.count_++] = static_cast<int16_t>(i);
    }
  }

  STORAGE_LOG(DEBUG, "Project row", K(ret), K(unprojected_row), K(projected_row), KPC(projector), K(nop_pos_));
  return ret;
}

int ObMultipleMerge::project2output_exprs(ObDatumRow &unprojected_row, ObDatumRow &cur_row)
{
  int ret = OB_SUCCESS;
  nop_pos_.reset();
  // need set group_idx_ which is checked in ObTableScanRangeArrayRowIterator::get_next_row.
  cur_row.group_idx_ = unprojected_row.group_idx_;
  // in function multi_get_rows, the flag is used to identify whether the row exists or not
  cur_row.row_flag_ = unprojected_row.row_flag_;
  if (OB_FAIL(access_param_->row2exprs_projector_->project(
              *access_param_->output_exprs_, unprojected_row.storage_datums_,
              nop_pos_.nops_, nop_pos_.count_))) {
    LOG_WARN("Fail to project row", K(unprojected_row), K(cur_row));
  } else {
    LOG_DEBUG("Project row", K(unprojected_row), K(cur_row));
  }
  return ret;
}

int ObMultipleMerge::get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool not_using_static_engine = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else if (FALSE_IT(not_using_static_engine = (nullptr == access_param_->output_exprs_))) {
  } else if (access_param_->iter_param_.enable_pd_aggregate()) {
    ret = get_next_aggregate_row(row);
  } else {
    row = nullptr;
    if (need_padding_) {
      padding_allocator_.reuse();
    }
    reuse_lob_locator();
    while (OB_SUCC(ret)) {
      if (access_ctx_->is_limit_end()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(refresh_table_on_demand())) {
        STORAGE_LOG(WARN, "fail to refresh table on demand", K(ret));
      } else {
        if (OB_FAIL(inner_get_next_row(unprojected_row_))) {
          if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            ret = OB_SUCCESS;
            scan_state_ = ScanState::BATCH;
            continue;
          } else if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Fail to inner get next row, ", K(ret), KP(this));
          }
        } else if (need_read_lob_columns(unprojected_row_)) {
          if (OB_FAIL(handle_lob_before_fuse_row())) {
            LOG_WARN("Fail to handle lobs, ", K(ret), KP(this));
          }
        }
        if (OB_SUCC(ret)) {
          scan_state_ = ScanState::SINGLE_ROW;
        }
      }

      if (OB_SUCC(ret)) {
        if (nullptr != access_ctx_->table_scan_stat_) {
          access_ctx_->table_scan_stat_->access_row_cnt_++;
        }
        if (OB_FAIL(fill_group_idx_if_need(unprojected_row_))) {
          LOG_WARN("Failed to fill iter idx", K(ret), KPC(access_param_), K(unprojected_row_));
        } else if (OB_FAIL(process_fuse_row(not_using_static_engine, unprojected_row_, row))) {
          LOG_WARN("get row from fuse failed", K(ret), K(unprojected_row_));
        } else if (OB_NOT_NULL(access_param_->get_op()) &&
            OB_FAIL(access_param_->get_op()->write_trans_info_datum(unprojected_row_))) {
          LOG_WARN("write trans_info to expr datum failed", K(ret), K(unprojected_row_));
        } else if (nullptr != row) {
          break;
        }
      }
    }

    if (OB_ITER_END == ret) {
      update_and_report_tablet_stat();
      scan_state_ = ScanState::NONE;
    }
    if (OB_SUCC(ret)) {
      if (NULL != access_ctx_->table_scan_stat_) {
        access_ctx_->table_scan_stat_->out_row_cnt_++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "chaser debug get next", KPC(row), K(unprojected_row_), K(ret));
    if (nullptr != row && OB_FAIL(row->prepare_new_row(out_project_cols_))) {
      STORAGE_LOG(WARN, "Failed to prepare new row", K(ret));
    }
  }
  return ret;
}

int ObMultipleMerge::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (access_param_->iter_param_.enable_pd_aggregate()) {
    ObDatumRow *row = nullptr;
    if (OB_FAIL(get_next_aggregate_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get aggregate row", K(ret));
      }
    } else {
      count = 1;
    }
  } else if (ObQRIterType::T_SINGLE_GET == get_type()) {
    ObDatumRow *row = nullptr;
    sql::ObEvalCtx *eval_ctx = nullptr;
    if (OB_ISNULL(access_param_->get_op())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected access param: null op", K(ret));
    } else if (FALSE_IT(eval_ctx = &access_param_->get_op()->get_eval_ctx())) {
    } else if (FALSE_IT(eval_ctx->reuse(min(capacity, access_param_->get_op()->get_batch_size())))) {
    } else if (OB_FAIL(get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get single row", K(ret));
      }
    } else {
      count = 1;
    }
  } else {
    ret = get_next_normal_rows(count, capacity);
  }
  return ret;
}

/*
 * fill elements into datum vector of exprs
 * count: rows filled in expr
 * capacity: max rows returned
 * return: OB_ITER_END -> finished flag
 */
int ObMultipleMerge::get_next_normal_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMultipleMerge has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(nullptr == access_param_->get_op()
                         || !access_param_->get_op()->is_vectorized()
                         || !access_param_->iter_param_.vectorized_enabled_
                         || nullptr == block_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect pushdown operator in vectorized", K(ret), K(access_param_->iter_param_.pd_storage_flag_),
             K(access_param_->get_op()), K(access_param_->get_op()->is_vectorized()), K(block_row_store_),
             K(access_param_->iter_param_.vectorized_enabled_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(refresh_table_on_demand())) {
    LOG_WARN("fail to refresh table on demand", K(ret));
  } else {
    ObVectorStore *vector_store = reinterpret_cast<ObVectorStore *>(block_row_store_);
    int64_t batch_size = min(capacity, access_param_->get_op()->get_batch_size());
    vector_store->reuse_capacity(batch_size);
    if (need_padding_) {
      padding_allocator_.reuse();
    }
    reuse_lob_locator();
    while (OB_SUCC(ret) && !vector_store->is_end()) {
      bool can_batch = false;
      if (access_ctx_->is_limit_end()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(can_batch_scan(can_batch))) {
        LOG_WARN("fail to check can batch scan", K(ret));
      } else if (can_batch) {
        scan_state_ = ScanState::BATCH;
        if (OB_FAIL(inner_get_next_rows())) {
          if (OB_LIKELY(OB_PUSHDOWN_STATUS_CHANGED == ret || OB_ITER_END == ret)) {
            // OB_ITER_END should use fuse to make sure no greater key in dynamic data
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next rows fast", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (vector_store->is_end()) {
        // 1. batch full; 2. end of micro block with row returned;
        // 3. pushdown changed with row returned; 4. limit/offset end
      } else {
        if (OB_FAIL(inner_get_next_row(unprojected_row_))) {
          if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            ret = OB_SUCCESS;
            vector_store->set_end();
            scan_state_ = ScanState::BATCH;
            continue;
          } else if (OB_ITER_END != ret) {
            LOG_WARN("Fail to inner get next row, ", K(ret), KP(this));
          }
        } else if (need_read_lob_columns(unprojected_row_)) {
          if (OB_FAIL(handle_lob_before_fuse_row())) {
            LOG_WARN("Fail to handle lobs, ", K(ret), KP(this));
          }
        }
        if (OB_SUCC(ret)) {
          scan_state_ = ScanState::SINGLE_ROW;
        }

        if (OB_SUCC(ret)) {
          // back to normal path
          ObDatumRow *out_row = nullptr;
          if (OB_FAIL(fill_group_idx_if_need(unprojected_row_))) {
            LOG_WARN("Failed to fill iter idx", K(ret), KPC(access_param_), K(unprojected_row_));
          } else if (OB_FAIL(process_fuse_row(nullptr == access_param_->output_exprs_, unprojected_row_, out_row))) {
            LOG_WARN("get row from fuse failed", K(ret), K(unprojected_row_));
          } else if (nullptr != out_row) {
            if (OB_FAIL(access_param_->get_op()->deep_copy(access_param_->output_exprs_, vector_store->get_row_count()))) {
              LOG_WARN("fail to deep copy row", K(ret));
            } else if (OB_FAIL(access_param_->get_op()->write_trans_info_datum(unprojected_row_))) {
              LOG_WARN("write trans_info to expr datum failed", K(ret), K(unprojected_row_));
            } else if (OB_FAIL(vector_store->fill_row(unprojected_row_))) {
              LOG_WARN("fail to aggregate row", K(ret));
            }
          }
          if (nullptr != access_ctx_->table_scan_stat_) {
            access_ctx_->table_scan_stat_->access_row_cnt_++;
          }
        }
      }
    }

    count = vector_store->get_row_count();
    if (nullptr != access_ctx_->table_scan_stat_) {
      access_ctx_->table_scan_stat_->out_row_cnt_ += count;
    }
  }
  if (OB_ITER_END == ret) {
    update_and_report_tablet_stat();
    scan_state_ = ScanState::NONE;
  }
  LOG_TRACE("[Vectorized] get next rows", K(ret), K(count), K(capacity), KPC(block_row_store_));

  return ret;
}

// This function only returns one aggregated row to upper layer, i.e.
// first time: aggregated row, ret = OB_SUCCESS
// second time: ret = OB_ITER_END
int ObMultipleMerge::get_next_aggregate_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObMultipleMerge has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(nullptr == block_row_store_ || access_param_->iter_param_.need_fill_group_idx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggregate pushdown status", K(ret),
             K(access_param_->get_op()), K(block_row_store_), K(access_param_->iter_param_.need_fill_group_idx()));
  } else if (OB_UNLIKELY(nullptr != access_ctx_->range_array_pos_ &&
             access_ctx_->range_array_pos_->count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect aggregate pushdown status", K(ret),
             K(access_ctx_->range_array_pos_->count()));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObAggregatedStore *agg_row_store = reinterpret_cast<ObAggregatedStore *>(block_row_store_);
    agg_row_store->reuse_aggregated_row();
    if (OB_NOT_NULL(access_param_->get_op())) {
      int64_t batch_size = max(1, access_param_->get_op()->get_batch_size());
      access_param_->get_op()->get_eval_ctx().reuse(batch_size);
    }
    reuse_lob_locator();
    while (OB_SUCC(ret) && !agg_row_store->is_end()) {
      bool can_batch = false;
      // clear evaluated flag for every row
      // all rows will be touched in this loop
      if (NULL != access_param_->get_op()) {
        access_param_->get_op()->clear_datum_eval_flag();
      }
      if (OB_FAIL(refresh_table_on_demand())) {
        LOG_WARN("fail to refresh table on demand", K(ret));
      } else if (OB_FAIL(can_batch_scan(can_batch))) {
        LOG_WARN("fail to check can batch scan", K(ret));
      } else if (can_batch) {
        scan_state_ = ScanState::BATCH;
        if (OB_FAIL(inner_get_next_rows())) {
          if (OB_LIKELY(OB_PUSHDOWN_STATUS_CHANGED == ret || OB_ITER_END == ret)) {
            // OB_ITER_END should use fuse to make sure no greater key in dynamic data
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next aggregate row fast", K(ret));
          }
        } else {
          continue;
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        if (OB_FAIL(inner_get_next_row(unprojected_row_))) {
          if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            ret = OB_SUCCESS;
            scan_state_ = ScanState::BATCH;
            continue;
          } else if (OB_ITER_END != ret) {
            LOG_WARN("Fail to inner get next row, ", K(ret), KP(this));
          }
        } else if (need_read_lob_columns(unprojected_row_)) {
          if (OB_FAIL(handle_lob_before_fuse_row())) {
            LOG_WARN("Fail to handle lobs, ", K(ret), KP(this));
          }
        }
        if (OB_SUCC(ret)) {
          scan_state_ = ScanState::SINGLE_ROW;
        }

        if (OB_SUCC(ret)) {
          ObDatumRow *out_row = nullptr;
          if (OB_FAIL(process_fuse_row(
                      false,
                      unprojected_row_,
                      out_row))) {
            LOG_WARN("get row from fuse failed", K(ret));
          } else if (nullptr != out_row) {
            if (OB_FAIL(agg_row_store->fill_row(unprojected_row_))) {
              LOG_WARN("fail to aggregate row", K(ret));
            }
          }
          if (nullptr != access_ctx_->table_scan_stat_) {
            access_ctx_->table_scan_stat_->access_row_cnt_++;
          }
        }
      }
    }

    // second time: ret = OB_ITER_END
    if (agg_row_store->is_end()) {
      ret = OB_ITER_END;
    }
    // first time: aggregated row, ret = OB_SUCCESS
    if (OB_ITER_END == ret && !agg_row_store->is_end()) {
      if (OB_FAIL(agg_row_store->collect_aggregated_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to collect aggregate row", K(ret));
        }
      }
      agg_row_store->set_end();
    }

    if (nullptr != access_ctx_->table_scan_stat_) {
      access_ctx_->table_scan_stat_->out_row_cnt_++;
    }
  }
  if (OB_ITER_END == ret) {
    update_and_report_tablet_stat();
    scan_state_ = ScanState::NONE;
  }
  return ret;
}

void ObMultipleMerge::report_tablet_stat()
{
  if (OB_ISNULL(access_ctx_) || OB_ISNULL(access_param_)) {
  } else if (0 == access_ctx_->table_store_stat_.physical_read_cnt_
      && (0 == access_ctx_->table_store_stat_.micro_access_cnt_ || !access_param_->iter_param_.enable_pd_blockscan())) {
    // empty query, ignore it
  } else {
    int tmp_ret = OB_SUCCESS;
    bool report_succ = false; /*placeholder*/
    storage::ObTabletStat tablet_stat;
    tablet_stat.ls_id_ = access_ctx_->table_store_stat_.ls_id_.id();
    tablet_stat.tablet_id_ = access_ctx_->table_store_stat_.tablet_id_.id();
    tablet_stat.query_cnt_ = 1;
    tablet_stat.scan_logical_row_cnt_ = access_ctx_->table_store_stat_.logical_read_cnt_;
    tablet_stat.scan_physical_row_cnt_ = access_ctx_->table_store_stat_.physical_read_cnt_;
    tablet_stat.scan_micro_block_cnt_ = access_param_->iter_param_.enable_pd_blockscan() ? access_ctx_->table_store_stat_.micro_access_cnt_ : 0;
    tablet_stat.pushdown_micro_block_cnt_ = access_ctx_->table_store_stat_.pushdown_micro_access_cnt_;
    if (OB_TMP_FAIL(MTL(storage::ObTenantTabletStatMgr *)->report_stat(tablet_stat, report_succ))) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to report tablet stat", K(tmp_ret), K(tablet_stat));
    }
  }
}

int ObMultipleMerge::process_fuse_row(const bool not_using_static_engine,
                                      ObDatumRow &in_row,
                                      ObDatumRow *&out_row)
{
  int ret = OB_SUCCESS;
  bool need_skip = false;
  bool is_filter_filtered = false;
  out_row = nullptr;
  if (OB_FAIL((not_using_static_engine)
          ?  project_row(in_row,
                         access_param_->iter_param_.out_cols_project_,
                         range_idx_delta_,
                         cur_row_)
          : project2output_exprs(in_row, cur_row_))) {
    LOG_WARN("fail to project row", K(ret));
  } else if (need_fill_default_ && nop_pos_.count() > 0 && OB_FAIL(fuse_default(cur_row_))) {
    LOG_WARN("Fail to fuse default row, ", K(ret));
  } else if (!need_fill_default_ && !need_output_row_with_nop_ && nop_pos_.count() > 0) {
    // this is for sample scan on increment data, we only output one row if increment data
    // has all the column data needed by the sample scan
    need_skip = true;
  } else if (!not_using_static_engine
             && OB_NOT_NULL(access_ctx_->lob_locator_helper_)
             && access_ctx_->lob_locator_helper_->enable_lob_locator_v2() == false
             && OB_FAIL(fill_lob_locator(cur_row_))) {
    LOG_WARN("fill lob locator v1 failed", K(ret));
  } else if (need_padding_ && OB_FAIL(pad_columns(cur_row_))) {
    LOG_WARN("Fail to padding columns, ", K(ret));
  } else if (need_fill_virtual_columns_ && OB_FAIL(fill_virtual_columns(cur_row_))) {
    LOG_WARN("Fail to fill virtual columns, ", K(ret));
  }
  if (OB_FAIL(ret) || need_skip) {
  } else{
    if (in_row.fast_filter_skipped_) {
      in_row.fast_filter_skipped_ = false;
    } else if (OB_FAIL(check_filtered(cur_row_, is_filter_filtered))) {
      LOG_WARN("fail to check row filtered", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (is_filter_filtered) {
      LOG_DEBUG("store row is filtered", K(in_row), K(cur_row_));
    } else if (nullptr != access_ctx_->limit_param_
               && access_ctx_->out_cnt_ < access_ctx_->limit_param_->offset_) {
      // clear evaluated flag for next row.
      if (NULL != access_param_->get_op()) {
        access_param_->get_op()->clear_datum_eval_flag();
      }
      ++access_ctx_->out_cnt_;
    } else {
      out_row = &cur_row_;
      ++access_ctx_->out_cnt_;
    }
  }
  LOG_DEBUG("multiple merge process fuse row", K(ret), K(cur_row_), K(need_skip), K(is_filter_filtered));
  return ret;
}

void ObMultipleMerge::reset()
{
  ObStoreRowIterator *iter = NULL;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != (iter = iters_.at(i))) {
      iter->~ObStoreRowIterator();
      if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
        access_ctx_->stmt_allocator_->free(iter);
      }
      iter = NULL;
    }
  }
  if (nullptr != block_row_store_) {
    block_row_store_->~ObBlockRowStore();
    if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
      access_ctx_->stmt_allocator_->free(block_row_store_);
    }
    block_row_store_ = nullptr;
  }
  if (nullptr != iter_pool_) {
    iter_pool_->reset();
    if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
      access_ctx_->stmt_allocator_->free(iter_pool_);
    }
    iter_pool_ = nullptr;
  }
  padding_allocator_.reset();
  iters_.reset();
  access_param_ = NULL;
  access_ctx_ = NULL;
  tables_.reset();
  nop_pos_.reset();
  row_stat_.reset();
  scan_cnt_ = 0;
  need_padding_ = false;
  need_fill_default_ = false;
  need_fill_virtual_columns_ = false;
  need_output_row_with_nop_ = false;
  inited_ = false;
  range_idx_delta_ = 0;
  out_cols_projector_ = NULL;
  read_memtable_only_ = false;
  out_project_cols_.reset();
  lob_reader_.reset();
  scan_state_ = ScanState::NONE;
}

void ObMultipleMerge::reuse()
{
  reuse_iter_array();
  row_stat_.reset();
  range_idx_delta_ = 0;
  unprojected_row_.row_flag_.reset();
  if (nullptr != block_row_store_) {
    block_row_store_->reuse();
  }
  lob_reader_.reuse();
  scan_state_ = ScanState::NONE;
}

void ObMultipleMerge::reset_iter_array()
{
  ObStoreRowIterator *iter = NULL;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != (iter = iters_.at(i))) {
      iter->~ObStoreRowIterator();
      if (OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
        access_ctx_->stmt_allocator_->free(iter);
      }
      iter = NULL;
    }
  }
  iters_.reuse();
}

void ObMultipleMerge::reuse_iter_array()
{
  ObStoreRowIterator *iter = NULL;
  if (nullptr != iter_pool_) {
    for (int64_t i = 0; i <  iters_.count(); ++i) {
      if (nullptr != (iter = iters_.at(i))) {
        iter->reuse();
        iter_pool_->return_iter(iter);
      }
    }
    iters_.reuse();
  } else {
    for (int64_t i = 0; i < iters_.count(); ++i) {
      if (NULL != (iter = iters_.at(i))) {
        iter->reuse();
      }
    }
  }
}

int ObMultipleMerge::open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(nullptr == access_param_ || nullptr == access_ctx_ || nullptr == access_ctx_->stmt_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null access info", K(ret), KP(access_param_), KP(access_ctx_));
  } else if (OB_FAIL(cur_row_.reserve(access_param_->get_max_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(*access_ctx_->stmt_allocator_, access_param_->get_max_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Fail to init nop pos, ", K(ret));
  } else {
    if (access_param_->iter_param_.is_use_iter_pool()) {
      if (OB_FAIL(alloc_iter_pool(*access_ctx_->stmt_allocator_))) {
        LOG_WARN("Failed to init iter pool", K(ret));
      } else {
        access_ctx_->iter_pool_ = iter_pool_;
      }
    }
    if (OB_SUCC(ret)) {
      access_ctx_->block_row_store_ = block_row_store_;
      ObMultipleMerge::reuse();
      if (nullptr != block_row_store_ && OB_FAIL(block_row_store_->open())) {
        LOG_WARN("fail to open block_row_store", K(ret));
      } else if (nullptr != iter_pool_ && 0 != iters_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected state, all the iters in iters_ have must be returned to iter_pool_", K(ret), K(iters_.count()));
      } else {
        scan_cnt_ = 0;
      }
    }
  }
  return ret;
}

int ObMultipleMerge::alloc_row_store(ObTableAccessContext &context, const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (param.iter_param_.enable_pd_aggregate()) {
    if (OB_ISNULL(buf = context.stmt_allocator_->alloc(sizeof(ObAggregatedStore)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc aggregated store", K(ret));
    } else {
      block_row_store_ = new (buf) ObAggregatedStore(
          param.iter_param_.vectorized_enabled_ ? param.get_op()->get_batch_size() : ObAggregatedStore::BATCH_SIZE,
          param.get_op()->get_eval_ctx(),
          context);
    }
  } else if (ObQRIterType::T_SINGLE_GET != get_type()) {
    if (param.iter_param_.vectorized_enabled_) {
      if (OB_ISNULL(buf = context.stmt_allocator_->alloc(sizeof(ObVectorStore)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector store", K(ret));
      } else {
        block_row_store_ = new (buf) ObVectorStore(
            param.get_op()->get_batch_size(),
            param.get_op()->get_eval_ctx(),
            context);
      }
    } else if (param.iter_param_.enable_pd_blockscan()) {
      if (OB_ISNULL(buf = context.stmt_allocator_->alloc(sizeof(ObBlockRowStore)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc row store");
      } else {
        block_row_store_ = new (buf) ObBlockRowStore(context);
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != block_row_store_) {
    if (OB_FAIL(block_row_store_->init(param))) {
      LOG_WARN("fail to init block row store", K(ret), K(block_row_store_));
    }
  }
  return ret;
}

int ObMultipleMerge::alloc_iter_pool(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == iter_pool_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStoreRowIterPool)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc row iter pool", K(ret));
    } else {
      iter_pool_ = new(buf) ObStoreRowIterPool(allocator);
    }
  }
  return ret;
}

int ObMultipleMerge::fuse_default(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t idx = 0;
  const ObIArray<ObColumnParam *> *out_cols_param = access_param_->iter_param_.get_col_params();
  if (OB_ISNULL(out_cols_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null cols param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); ++i) {
    if (OB_FAIL(nop_pos_.get_nop_pos(i, pos))) {
      STORAGE_LOG(WARN, "Fail to get nop pos, ", K(ret));
    } else {
      idx = access_param_->iter_param_.out_cols_project_->at(pos);
      ObObj def_cell(out_cols_param->at(idx)->get_orig_default_value());
      if (is_lob_storage(def_cell.get_type())
          && OB_FAIL(fuse_lob_default(def_cell, out_cols_param->at(idx)->get_column_id()))) {
        STORAGE_LOG(WARN, "Fail to fuse lob default, ", K(ret));
      } else if (NULL == access_param_->output_exprs_) {
        if (OB_FAIL(row.storage_datums_[pos].from_obj(def_cell))) {
          STORAGE_LOG(WARN, "Failed to transform obj to datum", K(ret));
        }
      } else {
        // skip virtual column which has nop default value.
        if (!def_cell.is_nop_value()) {
          sql::ObExpr *expr = access_param_->output_exprs_->at(pos);
          sql::ObDatum &datum = expr->locate_datum_for_write(
              access_param_->get_op()->get_eval_ctx());
          sql::ObEvalInfo &eval_info = expr->get_eval_info(
              access_param_->get_op()->get_eval_ctx());
          if (OB_FAIL(datum.from_obj(def_cell, expr->obj_datum_map_))) {
            LOG_WARN("convert obj to datum failed", K(ret));
          } else if (is_lob_storage(def_cell.get_type()) &&
                     OB_FAIL(sql::ob_adjust_lob_datum(def_cell, expr->obj_meta_, expr->obj_datum_map_,
                                                      lob_reader_.get_allocator(), datum))) {
            LOG_WARN("adjust lob datum failed", K(ret), K(def_cell.get_meta()), K(expr->obj_meta_));
          } else {
            eval_info.evaluated_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::fill_lob_locator(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL != access_ctx_->lob_locator_helper_) {
    if (!access_ctx_->lob_locator_helper_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected lob locator helper", K(ret),
                  KPC(access_ctx_->lob_locator_helper_));
    } else if (access_ctx_->lob_locator_helper_->enable_lob_locator_v2()) {
      if (OB_FAIL(access_ctx_->lob_locator_helper_->fill_lob_locator_v2(row,
                                                                        *access_ctx_,
                                                                        *access_param_))) {
        STORAGE_LOG(WARN, "fill lob locator v2 failed", K(ret));
      }
    } else { // locator v1
      if (OB_FAIL(access_ctx_->lob_locator_helper_->fill_lob_locator(unprojected_row_,
                                                                     false,
                                                                     *access_param_))) {
        STORAGE_LOG(WARN, "fill lob locator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMultipleMerge::pad_columns(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<int32_t> *padding_cols = access_param_->padding_cols_;
  int32_t idx = 0;
  const ObIArray<ObColumnParam *> *out_cols_param = access_param_->iter_param_.get_col_params();
  if (OB_ISNULL(out_cols_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null cols param", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != padding_cols && i < padding_cols->count(); ++i) {
    idx = padding_cols->at(i);
    const int64_t col_idx = access_param_->iter_param_.out_cols_project_->at(idx);
    share::schema::ObColumnParam *col_param = out_cols_param->at(col_idx);
    if (NULL == access_param_->output_exprs_) {
      if (OB_FAIL(pad_column(col_param->get_meta_type(), col_param->get_accuracy(), padding_allocator_, row.storage_datums_[idx]))) {
        STORAGE_LOG(WARN, "Fail to pad column", K(*col_param), K(ret));
      }
    } else {
      sql::ObExpr *e = access_param_->output_exprs_->at(idx);
      if (e->arg_cnt_ > 0 && unprojected_row_.storage_datums_[col_idx].is_nop()) {
        // do nothing for virtual column with no data read,
        // datum is filled && padded in fill_virtual_columns().
      } else {
        if (OB_ISNULL(access_param_->get_op())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected access param: null op", K(ret));
        } else if (OB_FAIL(pad_column(col_param->get_accuracy(), access_param_->get_op()->get_eval_ctx(), *e))) {
          LOG_WARN("pad column failed", K(ret), K(*col_param));
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::fill_virtual_columns(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL != access_param_->get_op()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); i++) {
      int64_t pos = 0;
      if (OB_FAIL(nop_pos_.get_nop_pos(i, pos))) {
        LOG_WARN("get position failed", K(ret));
      } else {
        sql::ObExpr *expr = access_param_->output_exprs_->at(pos);
        // table scan access exprs is column reference expr, only virtual column has argument.
        if (expr->arg_cnt_ > 0) {
          ObDatum *datum = NULL;
          access_param_->get_op()->clear_datum_eval_flag();
          if (OB_FAIL(expr->eval(access_param_->get_op()->get_eval_ctx(), datum))) {
            LOG_WARN("evaluate virtual column failed", K(ret));
          } else if (need_padding_ && expr->obj_meta_.is_fixed_len_char_type()) {
            const int64_t col_idx = access_param_->iter_param_.out_cols_project_->at(pos);
            if (OB_ISNULL(access_param_->get_op())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected access param: null op", K(ret));
            } else if (OB_FAIL(pad_column(access_param_->iter_param_.get_col_params()->at(col_idx)->get_accuracy(),
                                          access_param_->get_op()->get_eval_ctx(),
                                          *expr))) {
              LOG_WARN("pad column failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::check_filtered(const ObDatumRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  // check if timeout or if transaction status every 10000 rows, which should be within 10ms
  if (0 == (++scan_cnt_ % 10000) && !access_ctx_->query_flag_.is_daily_merge()) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      STORAGE_LOG(WARN, "query interrupt, ", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && NULL != access_param_->op_filters_
      && !access_param_->op_filters_->empty()) {
    // Execute filter in sql static typing engine.
    // %row is already projected to output expressions for main table scan.
    if (OB_FAIL(access_param_->get_op()->filter_row_outside(*access_param_->op_filters_, filtered))) {
      LOG_WARN("filter row failed", K(ret));
    }
  }
  return ret;
}

int ObMultipleMerge::add_iterator(ObStoreRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iters_.push_back(&iter))) {
    STORAGE_LOG(WARN, "fail to push back iterator", K(ret));
  }
  return ret;
}

const ObTableIterParam * ObMultipleMerge::get_actual_iter_param(const ObITable *table) const
{
  const ObTableIterParam *ptr = NULL;
  if (OB_ISNULL(table)) {
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "input table is NULL", KP(table));
  } else{
    ptr = &access_param_->iter_param_;
  }
  return ptr;
}

int ObMultipleMerge::prepare_read_tables(bool refresh)
{
  int ret = OB_SUCCESS;
  tables_.reset();
  if (OB_UNLIKELY(!get_table_param_.is_valid() || !access_param_->is_valid() || NULL == access_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultipleMerge has not been inited", K(ret), K_(get_table_param), KP_(access_param),
        KP_(access_ctx));
  } else if (OB_UNLIKELY(!access_ctx_->query_flag_.is_whole_macro_scan() &&
                         0 != access_ctx_->trans_version_range_.base_version_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("base version should be 0", K(ret), K(access_ctx_->trans_version_range_.base_version_));
  } else if (!refresh && get_table_param_.tablet_iter_.table_iter()->is_valid()) {
    if (OB_FAIL(prepare_tables_from_iterator(*get_table_param_.tablet_iter_.table_iter()))) {
      LOG_WARN("prepare tables fail", K(ret), K(get_table_param_.tablet_iter_.table_iter()));
    }
  } else if (OB_FAIL(FALSE_IT(get_table_param_.tablet_iter_.table_iter()->reset()))) {
  } else {
    if (OB_UNLIKELY(get_table_param_.frozen_version_ != -1)) {
      if (!get_table_param_.sample_info_.is_no_sample()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("sample query does not support frozen_version", K(ret), K_(get_table_param), KP_(access_param));
      } else if (OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(
          get_table_param_.frozen_version_, false/*allow_not_ready*/, true/*major only*/))) {
        LOG_WARN("get table iterator fail", K(ret), K_(get_table_param), KP_(access_param));
      }
    } else if (OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(
        get_table_param_.sample_info_.is_no_sample()
          ? access_ctx_->store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx()
          : INT64_MAX,
        false/*allow_not_ready*/))) {
      LOG_WARN("get table iterator fail", K(ret), K_(get_table_param), KP_(access_param));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_tables_from_iterator(*get_table_param_.tablet_iter_.table_iter(), &get_table_param_.sample_info_))) {
        LOG_WARN("failed to prepare tables from iter", K(ret), K(get_table_param_.tablet_iter_.table_iter()));
      }
    }
  }
  LOG_DEBUG("prepare read tables", K(ret), K(refresh), K_(get_table_param), K_(tables));
  return ret;
}

int ObMultipleMerge::prepare_tables_from_iterator(ObTableStoreIterator &table_iter, const common::SampleInfo *sample_info)
{
  int ret = OB_SUCCESS;
  table_iter.resume();
  int64_t memtable_cnt = 0;
  read_memtable_only_ = false;
  bool read_released_memtable = false;
  while (OB_SUCC(ret)) {
    ObITable *table_ptr = nullptr;
    bool need_table = true;
    if (OB_FAIL(table_iter.get_next(table_ptr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next tables", K(ret));
      }
    } else if (OB_ISNULL(table_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must not be null", K(ret), K(table_iter));
    } else if (nullptr != sample_info && !sample_info->is_no_sample()) {
      need_table = false;
      if (SampleInfo::SAMPLE_ALL_DATA == sample_info->scope_) {
        need_table = true;
      } else if (SampleInfo::SAMPLE_BASE_DATA == sample_info->scope_) {
        need_table = table_ptr->is_major_sstable();
      } else if (SampleInfo::SAMPLE_INCR_DATA == sample_info->scope_) {
        need_table = !table_ptr->is_major_sstable();
      }
    }
    if (OB_SUCC(ret) && need_table) {
      if (table_ptr->is_empty()) {
        LOG_DEBUG("cur table is empty", K(ret), KPC(table_ptr));
        continue;
      } else if (table_ptr->is_memtable()) {
        read_released_memtable = read_released_memtable ||
            memtable::ObMemtableFreezeState::RELEASED == (static_cast<memtable::ObMemtable*>(table_ptr))->get_freeze_state();
        ++memtable_cnt;
      }
      if (OB_FAIL(tables_.push_back(table_ptr))) {
        LOG_WARN("add table fail", K(ret), K(*table_ptr));
      }
    }
  } // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (tables_.count() > common::MAX_TABLE_CNT_IN_STORAGE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table cnt", K(ret), K(memtable_cnt), K(tables_.count()), K(table_iter), K(tables_));
    }
  }
  if (OB_SUCC(ret) && memtable_cnt == tables_.count()) {
    read_memtable_only_ = true;
  }
  #ifdef ENABLE_DEBUG_LOG
  if (GCONF.enable_defensive_check() && read_released_memtable) {
    for (int64_t i = 0; i < tables_.count(); ++i) {
      LOG_INFO("dump read tables", KPC(tables_.at(i)));
    }
  }
  #endif
  return ret;
}

int ObMultipleMerge::refresh_table_on_demand()
{
  int ret = OB_SUCCESS;
  bool need_refresh = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMultipleMerge has not been inited", K(ret));
  } else if (ScanState::NONE == scan_state_) {
    STORAGE_LOG(DEBUG, "skip refresh table");
  } else if (OB_FAIL(check_need_refresh_table(need_refresh))) {
    STORAGE_LOG(WARN, "fail to check need refresh table", K(ret));
  } else if (need_refresh) {
    if (ScanState::BATCH == scan_state_) {
      STORAGE_LOG(INFO, "in vectorize batch scan, do refresh at next time");
      if (OB_NOT_NULL(block_row_store_)) {
        block_row_store_->disable();
      }
    } else if (OB_FAIL(save_curr_rowkey())) {
      STORAGE_LOG(WARN, "fail to save current rowkey", K(ret));
    } else if (FALSE_IT(reset_iter_array())) {
    } else if (OB_FAIL(refresh_tablet_iter())) {
      STORAGE_LOG(WARN, "fail to refresh tablet iter", K(ret));
    } else if (OB_FAIL(prepare_read_tables(true))) {
      STORAGE_LOG(WARN, "fail to prepare read tables", K(ret));
    } else if (OB_FAIL(reset_tables())) {
      STORAGE_LOG(WARN, "fail to reset tables", K(ret));
    } else if (nullptr != block_row_store_) {
      block_row_store_->reuse();
    }
    STORAGE_LOG(INFO, "table need to be refresh", "table_id", access_param_->iter_param_.table_id_,
        K(*access_param_), K(curr_scan_index_), K(scan_state_));
  }
  return ret;
}

int ObMultipleMerge::refresh_tablet_iter()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!get_table_param_.tablet_iter_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet iter is invalid", K(ret), K(get_table_param_.tablet_iter_));
  } else {
    const common::ObTabletID tablet_id = get_table_param_.tablet_iter_.get_tablet()->get_tablet_meta().tablet_id_;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(access_ctx_->ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_handle));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_read_tables(
        tablet_id,
        get_table_param_.sample_info_.is_no_sample()
          ? access_ctx_->store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx()
          : INT64_MAX,
        get_table_param_.tablet_iter_,
        false/*allow_not_ready*/))) {
      LOG_WARN("failed to refresh tablet iterator", K(ret), K_(get_table_param), KP_(access_param));
    }
  }
  return ret;
}

int ObMultipleMerge::init_lob_reader(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (iter_param.has_lob_column_out_) {
    lob_reader_.reset();
    if (OB_FAIL(lob_reader_.init(iter_param, access_ctx))) {
      LOG_WARN("[LOB] fail to init lob reader", K(access_ctx.query_flag_), K(iter_param));
    }
  }
  return ret;
}

int ObMultipleMerge::fill_group_idx_if_need(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (access_param_->iter_param_.need_fill_group_idx()) {
    const uint64_t col_idx = access_param_->iter_param_.get_group_idx_col_index();
    if (col_idx < 0 || col_idx >= row.get_column_count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid iter idx col", K(ret), K(col_idx), K(row.get_column_count()));
    } else {
      row.storage_datums_[col_idx].reuse();
      row.storage_datums_[col_idx].set_int(row.group_idx_);
    }
    STORAGE_LOG(DEBUG, "fill group idx", K(ret), K(col_idx), K(row.group_idx_));
  }
  return ret;
}

int ObMultipleMerge::read_lob_columns_full_data(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnParam *> *out_cols_param = access_param_->iter_param_.get_col_params();
  if (!lob_reader_.is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobDataReader not init", K(ret));
  } else if (OB_ISNULL(out_cols_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null cols param", K(ret));
  } else if (out_cols_param->count() != row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid col count", K(row), KPC(out_cols_param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
      blocksstable::ObStorageDatum &datum = row.storage_datums_[i];
      if (out_cols_param->at(i)->get_meta_type().is_lob_storage()) {
        if (OB_FAIL(lob_reader_.read_lob_data(datum, out_cols_param->at(i)->get_meta_type().get_collation_type()))) {
          STORAGE_LOG(WARN, "Failed to read lob obj", K(ret), K(i), K(datum));
        } else {
          STORAGE_LOG(DEBUG, "[LOB] Succeed to load lob obj", K(datum), K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObMultipleMerge::need_read_lob_columns(const blocksstable::ObDatumRow &row)
{
  return (access_param_->iter_param_.has_lob_column_out_ && row.row_flag_.is_exist());
}

// handle lobs before process_fuse_row
// 1. if query flag is skip_read_lob do nothing (LocalScan)
// 2. if use lob locator v2, fill lob header
// 3. if use lob locator v1, read full lob data (the same as 4.0)
int ObMultipleMerge::handle_lob_before_fuse_row()
{
  int ret = OB_SUCCESS;
  // reuse lob locator every row because output filter shared common expr is already fixed
  reuse_lob_locator();
  // Notice: should not  change behavior dynamicly by min cluster version:
  // for example, while running in 4.0 compat mode, the min cluster version changes to 4.1
  // but lob_locator_helper is not initialized.
  if (OB_NOT_NULL(access_ctx_->lob_locator_helper_)
            && access_ctx_->lob_locator_helper_->enable_lob_locator_v2()) {
    if (OB_FAIL(fill_lob_locator(unprojected_row_))) {
      LOG_WARN("Failed to read lob columns from store row with locator v2",
        K(ret), K(access_param_->iter_param_.tablet_id_), K(unprojected_row_));
    }
  } else {
    // no lob locator v2, read full lob data without header
    if (OB_FAIL(read_lob_columns_full_data(unprojected_row_))) {
      LOG_WARN("Failed to read lob columns full data from store row", K(ret),
        K(access_param_->iter_param_.tablet_id_), K(unprojected_row_));
    }
  }
  return ret;
}

int ObMultipleMerge::fuse_lob_default(ObObj &def_cell, const uint64_t col_id)
{
  // Notice: should not change behavior dynamicly by min cluster version
  // for example, while running in 4.0 compat mode, the min cluster version changes to 4.1
  // but lob_locator_helper is not initialized.
  int ret = OB_SUCCESS;
  ObLobLocatorHelper *lob_locator_helper = access_ctx_->lob_locator_helper_;
  if (access_ctx_->query_flag_.is_skip_read_lob()) { // skip means skip read full lob data
    if (OB_FAIL(lob_reader_.fuse_disk_lob_header(def_cell))) { // fuse disk locator
      STORAGE_LOG(WARN, "Failed to fuse lob header for nop val", K(ret));
    }
  } else if (OB_NOT_NULL(lob_locator_helper)) {
    if (nullptr == access_param_->output_exprs_) {
      if (OB_FAIL(lob_reader_.fuse_disk_lob_header(def_cell))) { // fuse disk locator
        STORAGE_LOG(WARN, "Failed to fuse lob header for nop val", K(ret));
      }
    } else { // fuse memory locator
      if (!lob_locator_helper->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected lob locator helper", K(ret),
                    KPC(access_ctx_->lob_locator_helper_));
      } else if (access_ctx_->lob_locator_helper_->fuse_mem_lob_header(def_cell,
                  col_id, is_sys_table(access_param_->iter_param_.table_id_))) {
        STORAGE_LOG(WARN, "fuse lob default with locator v2 failed", K(ret));
      }
    }
  } else {
    // not skip read lob, no lob locator helper, the result is full default value without header
  }
  return ret;
}

void ObMultipleMerge::reuse_lob_locator()
{
  if (NULL != access_ctx_->lob_locator_helper_) {
    access_ctx_->lob_locator_helper_->reuse();
  }
  lob_reader_.reuse();
}

void ObMultipleMerge::dump_tx_statistic_for_4377(ObStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  LOG_ERROR("==================== Start trx info ====================");

  if (NULL != store_ctx) {
    store_ctx->force_print_trace_log();
    if (NULL != store_ctx->mvcc_acc_ctx_.tx_ctx_) {
      LOG_ERROR("Dump trx info", K(ret), KPC(store_ctx->mvcc_acc_ctx_.tx_ctx_));
      if (NULL != store_ctx->mvcc_acc_ctx_.mem_ctx_) {
        // TODO(handora.qc): Shall we dump the row?
        store_ctx->mvcc_acc_ctx_.mem_ctx_->print_callbacks();
      }
    } else {
      LOG_ERROR("no some of the trx info", K(ret), KPC(store_ctx));
    }
  } else {
    LOG_ERROR("no trx info completely", K(ret));
  }

  LOG_ERROR("==================== End trx info ====================");
}

void ObMultipleMerge::dump_table_statistic_for_4377()
{
  int ret = OB_SUCCESS;
  int64_t table_idx = -1;

  LOG_ERROR("==================== Start table info ====================");
  for (table_idx = tables_.count() - 1; table_idx >= 0; --table_idx) {
    ObITable *table = tables_.at(table_idx);
    LOG_ERROR("Dump table info", K(ret), KPC(table));
  }
  LOG_ERROR("==================== End table info ====================");
}
}
}
