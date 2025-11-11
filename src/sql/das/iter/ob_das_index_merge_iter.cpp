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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_index_merge_iter.h"
#include "src/sql/das/ob_das_attach_define.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeIter::IndexMergeRowStore::init(common::ObIAllocator &allocator,
                                                  const common::ObIArray<ObExpr*> *exprs,
                                                  ObEvalCtx *eval_ctx,
                                                  int64_t max_size,
                                                  bool is_reverse,
                                                  bool rowkey_is_uint64,
                                                  ObBitVector *mock_skip)
{
  int ret = OB_SUCCESS;
  rowids_.set_allocator(&allocator);
  if (OB_ISNULL(exprs)
      || OB_ISNULL(eval_ctx)
      || OB_ISNULL(mock_skip)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr for init index merge row store", K(ret));
  } else if (OB_UNLIKELY(max_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max_size for init index merge row store", K(ret), K(max_size));
  } else if (OB_ISNULL(stored_rows_ =
      static_cast<StoredRow **>(allocator.alloc((max_size) * sizeof(StoredRow *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(max_size), K(ret));
  } else if (OB_ISNULL(row_store_ = OB_NEWx(ObChunkDatumStore, &allocator, "IndexMergeStore"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate row store", K(ret));
  } else if (OB_FAIL(row_store_->init(UINT64_MAX, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID, "IndexMergeStore"))) {
    LOG_WARN("failed to init row store", K(ret));
  } else if (rowkey_is_uint64 && OB_FAIL(rowids_.prepare_allocate(max_size))) {
    LOG_WARN("failed to prepare allocate rowids", K(ret));
  } else if (OB_ISNULL(relevances_ =
    static_cast<double*>(allocator.alloc((max_size) * sizeof(double))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(max_size), K(ret));
  } else {
    row_store_->set_allocator(allocator);
    count_ = 0;
    idx_ = 0;
    mock_skip_ = mock_skip;
    iter_end_ = false;
    drained_ = false;
    exprs_ = exprs;
    eval_ctx_ = eval_ctx;
    capacity_ = max_size;
    is_reverse_ = is_reverse;
    rowkey_is_uint64_ = rowkey_is_uint64;

    for (int64_t i = 0; i < max_size; i++) {
      relevances_[i] = 0.0;
    }
  }
  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::save_distance(bool is_vectorized, int64_t size, ObExpr *relevance_expr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0 || size > capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K(size), K(capacity_));
  } else if (OB_ISNULL(relevance_expr)) {
    // do nothing
  } else if (!is_vectorized) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(1);
    batch_info_guard.set_batch_idx(0);
    ObDatum &datum = relevance_expr->locate_expr_datum(*eval_ctx_);
    relevances_[0] = datum.get_double();
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(size);
    ObDatum *datum = relevance_expr->locate_batch_datums(*eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      relevances_[i] = datum[i].get_double();
    }
  }

  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::save(bool is_vectorized, int64_t size, ObExpr *relevance_expr)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_count = 0;
  row_store_->remove_added_blocks();
  if (OB_UNLIKELY(size <= 0 || size > capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K(size), K(capacity_));
  } else if (!is_vectorized) {
    // not vectorized, no need to prepare rowids
    if (OB_FAIL(row_store_->add_row(*exprs_, eval_ctx_, stored_rows_))) {
      LOG_WARN("failed to add row to row store", K(ret));
    } else if (OB_FAIL(save_distance(false, size, relevance_expr))) {
      LOG_WARN("failed to save distance", K(ret));
    }
  } else if (OB_FAIL(row_store_->add_batch(*exprs_, *eval_ctx_, *mock_skip_, size, stored_rows_count, stored_rows_))) {
    LOG_WARN("failed to add batch rows", K(ret));
  } else if (OB_FAIL(save_distance(true, size, relevance_expr))) {
    LOG_WARN("failed to save distance", K(ret));
  } else if (rowkey_is_uint64_) {
    for (int64_t i = 0; i < size; i++) {
      rowids_[i] = stored_rows_[i]->cells()[0].get_uint64();
    }
  }
  if (OB_SUCC(ret)) {
    count_ = size;
    idx_ = 0;
  }
  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::to_expr(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0 || idx_ + size > count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid size for to expr", K(ret), K(size), K(idx_));
  } else if (OB_ISNULL(stored_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for store rows to expr", K(ret), K(size), K(idx_));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(size);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(stored_rows_[idx_ + i]->to_expr<true>(*exprs_, *eval_ctx_))) {
        LOG_WARN("index merge iter failed to store rows", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      idx_ += size;
    }
  }
  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::lower_bound(uint64_t target, uint64_t &lower_bound)
{
  int ret = OB_SUCCESS;
  idx_ = std::lower_bound(rowids_.begin() + idx_, rowids_.begin() + count_, target) - rowids_.begin();
  if (idx_ >= count_) {
    ret = OB_ITER_END;
  } else {
    lower_bound = rowids_[idx_];
  }
  return ret;
}

void ObDASIndexMergeIter::IndexMergeRowStore::reuse()
{
  count_ = 0;
  idx_ = 0;
  iter_end_ = false;
  drained_ = false;
  if (OB_NOT_NULL(relevances_)) {
    for (int64_t i = 0; i < count_; i++) {
      relevances_[i] = 0.0;
    }
  }
}

void ObDASIndexMergeIter::IndexMergeRowStore::reset()
{
  rowids_.reset();
  if (OB_NOT_NULL(row_store_)) {
    row_store_->reset();
  }

  if (OB_NOT_NULL(relevances_)) {
    for (int64_t i = 0; i < capacity_; i++) {
      relevances_[i] = 0.0;
    }
    relevances_ = nullptr;
  }
  exprs_ = nullptr;
  eval_ctx_ = nullptr;
  capacity_ = 0;
  count_ = 0;
  idx_ = 0;
  iter_end_ = false;
  drained_ = false;
}

int ObDASIndexMergeIter::MergeResultBuffer::init(int64_t max_size,
                                                 ObEvalCtx *eval_ctx,
                                                 const common::ObIArray<ObExpr*> *exprs,
                                                 common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(eval_ctx), K(exprs), K(ret));
  } else if (OB_FAIL(result_store_.init(UINT64_MAX, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID, "DASIndexMerge"))) {
    LOG_WARN("failed to init result store", K(ret));
  } else if (OB_FAIL(result_store_.begin(result_store_iter_))) {
    LOG_WARN("failed to begin iterate result store", K(ret));
  } else {
    result_store_.set_allocator(alloc);
    max_size_ = max_size;
    row_cnt_ = 0;
    eval_ctx_ = eval_ctx;
    exprs_ = exprs;
  }
  return ret;
}

int ObDASIndexMergeIter::MergeResultBuffer::add_rows(int64_t size)
{
  return add_rows_by_exprs(size, exprs_);
}

int ObDASIndexMergeIter::MergeResultBuffer::add_rows_by_exprs(int64_t size, const common::ObIArray<ObExpr*> *exprs)
{
  int ret = OB_SUCCESS;
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid size for add rows", K(ret), K(size));
  } else if (size == 1) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(1);
    batch_info_guard.set_batch_idx(0);
    if (OB_FAIL(result_store_.add_row(*exprs, eval_ctx_))) {
      LOG_WARN("failed to add row to result store", K(ret));
    } else {
      row_cnt_ ++;
    }
  } else {
    bool added = false;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(size);
    if (OB_FAIL(result_store_.try_add_batch(*exprs, eval_ctx_, size, INT64_MAX, added))) {
      LOG_WARN("failed to try add batch to result store", K(ret));
    } else if (!added) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch wasn't added to result store", K(ret));
    } else {
      row_cnt_ += size;
    }
  }
  return ret;
}

int ObDASIndexMergeIter::MergeResultBuffer::to_expr(int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (OB_UNLIKELY(row_cnt_ <= 0 || !(result_store_iter_.is_valid() && result_store_iter_.has_next()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no available rows", K(ret));
  } else if (OB_FAIL(result_store_iter_.get_next_batch<true>(*exprs_, *eval_ctx_, size, read_size))) {
    LOG_WARN("failed to get next batch from result store", K(ret));
  } else if (OB_UNLIKELY(size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read size not equal to actually size", K(ret), K(size), K(read_size), K(get_row_cnt()));
  } else {
    row_cnt_ -= size;
  }
  return ret;
}

int ObDASIndexMergeIter::MergeResultBuffer::reuse()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(result_store_.begin(result_store_iter_))) {
    LOG_WARN("failed to begin iterate result store", K(ret));
  }
  return ret;
}

void ObDASIndexMergeIter::MergeResultBuffer::reset()
{
  row_cnt_ = 0;
  result_store_iter_.reset();
  result_store_.reset();
}

int ObDASIndexMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_ && ObDASIterType::DAS_ITER_TWO_PHASE_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    merge_type_ = index_merge_param.merge_type_;
    rowkey_exprs_ = index_merge_param.rowkey_exprs_;
    merge_ctdef_ = index_merge_param.ctdef_;
    merge_rtdef_ = index_merge_param.rtdef_;
    tx_desc_ = index_merge_param.tx_desc_;
    snapshot_ = index_merge_param.snapshot_;
    is_reverse_ = index_merge_param.is_reverse_;
    child_empty_count_ = 0;
    iter_end_count_=  0;
    force_merge_mode_ = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_INDEX_MERGE_MODE);
    force_merge_mode_ = 1;
    int64_t skip_id_threshold = - EVENT_CALL(EventTable::EN_INDEX_MERGE_SKIP_ID_THRESHOLD);
    skip_id_threshold_ = skip_id_threshold > 0 ? skip_id_threshold : SKIP_ID_THRESHOLD;
    current_skip_id_ = 0;
    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASIndexMerge", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_ctx_, context_param))) {
      LOG_WARN("failed to create index merge memctx", K(ret));
    } else if (OB_FAIL(check_rowkey_is_uint64())) {
      LOG_WARN("failed to check rowkey type", K(ret));
    } else if (OB_FAIL(check_disable_bitmap())) {
      LOG_WARN("failed to disable bitmap", K(ret));
    } else {
      common::ObArenaAllocator &alloc = mem_ctx_->get_arena_allocator();
      child_iters_.set_allocator(&alloc);
      child_stores_.set_allocator(&alloc);
      child_scan_rtdefs_.set_allocator(&alloc);
      child_scan_params_.set_allocator(&alloc);
      child_tablet_ids_.set_allocator(&alloc);
      int64_t child_cnt = index_merge_param.child_iters_->count();
      if (OB_FAIL(init_mock_skip(alloc, max_size_))) {
        LOG_WARN("failed to init mock skip", K(ret));
      } else if (OB_FAIL(child_iters_.assign(*index_merge_param.child_iters_))) {
        LOG_WARN("failed to assign child iters", K(ret));
      } else if (OB_FAIL(child_scan_rtdefs_.assign(*index_merge_param.child_scan_rtdefs_))) {
        LOG_WARN("failed to assign child scan rtdefs", K(ret));
      } else if (OB_FAIL(child_stores_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child stores", K(ret));
      } else if (OB_FAIL(child_scan_params_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child scan params", K(ret));
      } else if (OB_FAIL(child_tablet_ids_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child tablet ids", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
          ObDASIter *child = child_iters_.at(i);
          IndexMergeRowStore &row_store = child_stores_.at(i);
          ObDASScanRtDef *child_scan_rtdef = child_scan_rtdefs_.at(i);
          ObTableScanParam *&child_scan_param = child_scan_params_.at(i);
          if (OB_ISNULL(child) || OB_ISNULL(child->get_output())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid child iter", K(i), K(child), K(ret));
          } else if (OB_FAIL(row_store.init(
              alloc, child->get_output(), eval_ctx_, max_size_, is_reverse_, rowkey_is_uint64_, mock_skip_))) {
            LOG_WARN("failed to init row store", K(ret));
          } else if (child_scan_rtdef != nullptr) {
            // need to prepare scan param for normal scan node
            if (OB_ISNULL(child_scan_param = OB_NEWx(ObTableScanParam, &alloc))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate child scan param", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(result_buffer_.init(max_size_, eval_ctx_, output_, mem_ctx_->get_malloc_allocator()))) {
            LOG_WARN("failed to init merge result buffer", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::init_scan_param(const share::ObLSID &ls_id,
                                         const common::ObTabletID &tablet_id,
                                         const sql::ObDASScanCtDef *ctdef,
                                         sql::ObDASScanRtDef *rtdef,
                                         ObTableScanParam &scan_param) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.scan_tasks_.set_attr(ObMemAttr(tenant_id, "ScanParamET"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = ctdef->is_get_;
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    scan_param.enable_new_false_range_ = ctdef->enable_new_false_range_;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc_)) {
      scan_param.tx_id_ = tx_desc_->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot_)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot_))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KPC_(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    } else if (OB_FAIL(prepare_scan_ranges(scan_param, rtdef))) {
      LOG_WARN("failed to prepare scan ranges", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::prepare_scan_ranges(ObTableScanParam &scan_param, const ObDASScanRtDef *rtdef) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan rtdef", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.assign(rtdef->key_ranges_))) {
    LOG_WARN("failed to assign key ranges", K(ret));
  } else if (OB_FAIL(scan_param.ss_key_ranges_.assign(rtdef->ss_key_ranges_))) {
    LOG_WARN("failed to assign ss key ranges", K(ret));
  } else if (OB_FAIL(scan_param.mbr_filters_.assign(rtdef->mbr_filters_))) {
    LOG_WARN("failed to assign mbr filters", K(ret));
  }

  LOG_TRACE("index merge iter prepare scan ranges", K(scan_param), KPC(rtdef), K(ret));
  return ret;
}

int ObDASIndexMergeIter::save_row_to_result_buffer(int64_t size)
{
  return result_buffer_.add_rows(size);
}

int ObDASIndexMergeIter::result_buffer_rows_to_expr(int64_t expect_size)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = result_buffer_.get_row_cnt();
  if (OB_UNLIKELY(expect_size != row_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index merge iter output all row to expr failed, size not equal to row cnt", K(ret), K(expect_size), K(row_cnt));
  } else if (row_cnt == 0) {
  } else if (OB_FAIL(result_buffer_.to_expr(row_cnt))) {
    LOG_WARN("failed to expr", K(ret), K(expect_size), K(row_cnt));
  }
  return ret;
}

int ObDASIndexMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else if (OB_FAIL(init_scan_param(ls_id_, child_tablet_ids_.at(i), scan_ctdef, scan_rtdef, *scan_param))) {
        LOG_WARN("failed to init child scan param", K(ret));
      } else if (OB_FAIL(iter->do_table_scan())) {
        LOG_WARN("child iter failed to do table scan", K(ret));
      }
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("child iter failed to do table scan", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else {
        scan_param->tablet_id_ = child_tablet_ids_.at(i);
        scan_param->ls_id_ = ls_id_;
        if (OB_FAIL(prepare_scan_ranges(*scan_param, scan_rtdef))) {
          LOG_WARN("failed to prepare scan ranges", K(ret));
        } else if (OB_FAIL(iter->rescan())) {
          LOG_WARN("child iter failed to rescan", K(ret));
        }
      }
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("child iter failed to rescan", K(ret));
    }
  }
  return ret;
}

void ObDASIndexMergeIter::clear_evaluated_flag()
{
  for (int64_t i = 0; i < child_iters_.count(); ++i) {
    if (OB_NOT_NULL(child_iters_.at(i))) {
      child_iters_.at(i)->clear_evaluated_flag();
    }
  }
}

int ObDASIndexMergeIter::set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  main_scan_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  const ObIArray<ObTabletID> &index_merge_tablet_ids = related_tablet_ids.index_merge_tablet_ids_;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      if (OB_ISNULL(scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr scan ctdef", K(ret));
      } else {
        child_tablet_ids_.at(i) = index_merge_tablet_ids.at(scan_ctdef->index_merge_idx_);
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else {
        const ObTabletID &new_tablet_id = child_tablet_ids_.at(i);
        const ObTabletID &old_tablet_id = scan_param->tablet_id_;
        scan_param->need_switch_param_ = scan_param->need_switch_param_ ||
            (old_tablet_id.is_valid() && old_tablet_id != new_tablet_id);
        scan_param->key_ranges_.reuse();
        scan_param->ss_key_ranges_.reuse();
        scan_param->mbr_filters_.reuse();
        scan_param->scan_tasks_.reuse();
        if (OB_FAIL(iter->reuse())) {
          LOG_WARN("child iter failed to reuse", K(ret));
        }
      }
    } else if (OB_FAIL(iter->reuse())) {
      LOG_WARN("child iter failed to reuse", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); ++i) {
    child_stores_.at(i).reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_bitmaps_.count(); ++i) {
    if (OB_ISNULL(child_bitmaps_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr bitmap", K(ret), K(i));
    } else {
      child_bitmaps_.at(i)->set_empty();
    }
  }
  if (OB_NOT_NULL(result_bitmap_iter_)) {
    result_bitmap_iter_->deinit();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result_buffer_.reuse())) {
      LOG_WARN("result buffer failed to reuse", K(ret));
    }
  }
  current_skip_id_ = 0;
  child_empty_count_ = 0;
  iter_end_count_= 0;
  return ret;
}

int ObDASIndexMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  child_iters_.reset();
  child_scan_rtdefs_.reset();
  child_tablet_ids_.reset();
  for (int64_t i = 0; i < child_stores_.count(); ++i) {
    child_stores_.at(i).reset();
  }
  child_stores_.reset();
  for (int64_t i = 0; i < child_scan_params_.count(); ++i) {
    ObTableScanParam *scan_param = child_scan_params_.at(i);
    if (scan_param != nullptr) {
      scan_param->destroy_schema_guard();
      scan_param->snapshot_.reset();
      scan_param->destroy();
    }
  }
  for (int64_t i = 0; i < child_bitmaps_.count(); ++i) {
    if (OB_ISNULL(child_bitmaps_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr bitmap", K(ret), K(i));
    } else {
      child_bitmaps_.at(i)->set_empty();
    }
  }
  if (OB_NOT_NULL(result_bitmap_iter_)) {
    result_bitmap_iter_->deinit();
  }

  result_buffer_.reset();
  if (OB_NOT_NULL(mem_ctx_)) {
    mem_ctx_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = nullptr;
  }
  child_empty_count_  = 0;
  iter_end_count_ = 0;
  return ret;
}

int ObDASIndexMergeIter::inner_get_next_row()
{
  // implement by index merge and & or iter
  return OB_NOT_IMPLEMENT;
}
int ObDASIndexMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  // implement by index merge and & or iter
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeIter::compare(IndexMergeRowStore &cur_store, IndexMergeRowStore &cmp_store, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (OB_UNLIKELY(cur_store.is_empty() || cmp_store.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cur_store), K(cmp_store));
  } else {
    const IndexMergeRowStore::StoredRow *cur_row = cur_store.first_row();
    const IndexMergeRowStore::StoredRow *cmp_row = cmp_store.first_row();
    if (OB_ISNULL(cur_row) || OB_ISNULL(cmp_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KPC(this), K(cur_row), K(cmp_row));
    } else {
      const ObDatum *cur_datums = cur_row->cells();
      const ObDatum *cmp_datums = cmp_row->cells();
      if (OB_ISNULL(cur_datums) || OB_ISNULL(cmp_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(cur_datums), K(cmp_datums));
      } else if (OB_UNLIKELY(cur_datums->is_null() || cmp_datums->is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), KPC(cur_datums), KPC(cmp_datums));
      } else {
        ObObj cur_obj;
        ObObj cmp_obj;
        for (int64_t i = 0; (cmp_ret == 0) && OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
          const ObExpr *expr = rowkey_exprs_->at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else if (OB_FAIL(cur_datums[i].to_obj(cur_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert left datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cmp_datums[i].to_obj(cmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert right datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cur_obj.check_collation_free_and_compare(cmp_obj, cmp_ret))) {
            LOG_WARN("failed to compare cur obj with output obj", K(ret));
          } else {
            cmp_ret = OB_UNLIKELY(is_reverse_) ? -cmp_ret : cmp_ret;
          }
        } // end for
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::update_skip_id(int64_t child_idx, uint64_t *skip_id)
{
  int ret = OB_SUCCESS;
  ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(child_idx);
  if (nullptr != scan_rtdef) {
    scan_rtdef->local_dynamic_filter_params_.reuse();
    // now we can ensure that each child_store is either empty or its idx is already equal to count
    const IndexMergeRowStore &child_store = child_stores_.at(child_idx);
    uint64_t current_id = child_store.count_ > 0 ? child_store.rowids_[child_store.count_ - 1] : 0;
    if (*skip_id > current_id + skip_id_threshold_) {
      scan_rtdef->p_pd_expr_op_->set_local_dynamic_filter_params(scan_rtdef->local_dynamic_filter_params_);
      if (OB_FAIL(scan_rtdef->local_dynamic_filter_params_.push_back(
          ObDatum(reinterpret_cast<const char*>(skip_id), sizeof(uint64_t), false)))) {
        LOG_WARN("failed to push back datum", K(ret), K(*skip_id));
      }
    }
    LOG_TRACE("update skip id", K(child_idx), K(*skip_id), K(current_id), K(skip_id_threshold_));
  }
  return ret;
}

int ObDASIndexMergeIter::fill_child_stores(int64_t capacity, common::ObIArray<ObExpr*> *relevance_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    if (child_store.iter_end_) {
      // no more rows for child iter
    } else if (!child_store.is_empty()) {
    } else {
      ObDASIter *child_iter = child_iters_.at(i);
      int64_t child_count = 0;
      int64_t child_capacity = std::min(capacity, child_store.capacity_);
      if (OB_ISNULL(child_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(i));
      } else if (OB_UNLIKELY(child_capacity <= 0)) {
        // do nothing
      } else {
        int64_t max_child_count = 0;
        if (merge_ctdef_->has_dynamic_id_filter_ && OB_FAIL(update_skip_id(i, &current_skip_id_))) {
          LOG_WARN("failed to update skip id", K(ret));
        } else {
          while (OB_SUCC(ret) && !child_store.iter_end_ && child_count == 0) {
            if (OB_FAIL(child_iter->get_next_rows(child_count, child_capacity))) {
              if (ret == OB_ITER_END) {
                ret = OB_SUCCESS;
                child_store.iter_end_ = true;
                iter_end_count_ ++;
              } else {
                LOG_WARN("failed to get next rows from child iter", K(ret), K(capacity), K(child_count), K(child_capacity));
              }
            }
            max_child_count = std::max(max_child_count, child_count);
            if (OB_FAIL(ret) || child_count <= 0) {
            } else if (rowkey_is_uint64_ && !is_reverse_) {
              // check read rows whether match skip id
              if (OB_ISNULL(rowkey_exprs_) ||
                  OB_UNLIKELY(rowkey_exprs_->count() != 1) ||
                  OB_ISNULL(rowkey_exprs_->at(0))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected rowkey exprs", K(ret));
              } else {
                ObDatum *datums = rowkey_exprs_->at(0)->locate_batch_datums(*eval_ctx_);
                if (datums[child_count - 1].get_uint64() < current_skip_id_) {
                  child_count = 0;
                }
              }
              LOG_TRACE("check read rows whether match skip id", K(ret), K(child_count), K(current_skip_id_));
            }
          }
        }
        ObExpr *relevance_expr = relevance_exprs != nullptr ? relevance_exprs->at(i) : nullptr;
        if (OB_FAIL(ret) || child_count <= 0) {
        } else if (OB_FAIL(child_store.save(true, child_count, relevance_expr))) {
          LOG_WARN("failed to save child rows", K(ret), K(child_count));
        } else if (merge_ctdef_->has_dynamic_id_filter_) {
          current_skip_id_ = std::max(current_skip_id_, child_store.rowids_[0]);
        }
        LOG_TRACE("fill child store", K(ret), K(child_count), K(child_store), K(i));

        // we need to reset datum ptr for sort iter
        if (OB_SUCC(ret) && child_iter->get_type() == DAS_ITER_SORT && max_child_count > 0) {
          reset_datum_ptr(child_iter->get_output(), max_child_count);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (child_store.iter_end_ && !child_store.drained_ && child_store.is_empty()) {
      child_empty_count_ ++;
      child_store.drained_ = true;
    }
  } // end for
  return ret;
}

int ObDASIndexMergeIter::fill_child_bitmaps()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(disable_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is disabled", K(ret), K(disable_bitmap_));
  } else if (OB_UNLIKELY(!rowkey_is_uint64_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey is not uint64", K(ret), K(rowkey_is_uint64_));
  } else if (OB_ISNULL(result_bitmap_iter_)) {
    if (OB_UNLIKELY(child_bitmaps_.count() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child bitmaps is not empty", K(ret), K(child_bitmaps_.count()));
    } else {
      common::ObArenaAllocator &allocator = mem_ctx_->get_arena_allocator();
      int64_t child_cnt = child_stores_.count();
      if (FALSE_IT(child_bitmaps_.set_allocator(&allocator))) {
      } else if (OB_FAIL(child_bitmaps_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child bitmaps", K(ret), K(child_cnt));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
          ObRoaringBitmap *&child_bitmap = child_bitmaps_.at(i);
          if (OB_ISNULL(child_bitmap = OB_NEWx(ObRoaringBitmap, &allocator, &allocator))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc child bitmap", K(ret), K(child_cnt), K(sizeof(ObRoaringBitmap)));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(child_bitmaps_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child bitmap is nullptr", K(ret), K(child_bitmaps_.at(0)));
      } else if (OB_ISNULL(result_bitmap_iter_ = static_cast<ObRoaringBitmapIter*>(allocator.alloc(sizeof(ObRoaringBitmapIter))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc result bitmap iter", K(ret), K(sizeof(ObRoaringBitmapIter)));
      } else {
        new (result_bitmap_iter_) ObRoaringBitmapIter(child_bitmaps_.at(0));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    ObRoaringBitmap *child_bitmap = child_bitmaps_.at(i);
    if (OB_ISNULL(child_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child bitmap is nullptr", K(ret), K(i), K(child_bitmap));
    } else if (FALSE_IT(child_bitmap->set_empty())) {
    } else if (child_store.is_empty()) {
      // skip
      if (OB_UNLIKELY(merge_type_ == INDEX_MERGE_INTERSECT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected child_store is empty when merge type is INTERSECT", K(ret), K(i),
                                                                                   K(child_store.count()),
                                                                                   K(child_store));
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < child_store.count(); j++) {
        const IndexMergeRowStore::StoredRow *cur_row = child_store.at(j);
        if (OB_ISNULL(cur_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret), K(i), K(j), K(cur_row));
        } else {
          uint64_t cur_rowkey = 0;
          const ObDatum *datums = cur_row->cells();
          if (OB_ISNULL(datums)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret), K(i), K(j), K(datums));
          } else if (FALSE_IT(cur_rowkey = datums->get_uint64())) {
          } else if (OB_FAIL(child_bitmap->value_add(cur_rowkey))) {
            LOG_WARN("failed to add value to bitmap", K(ret), K(i), K(j), K(cur_rowkey));
          }
        }
      } // end for
    }
  } // end for
  return ret;
}

void ObDASIndexMergeIter::reset_datum_ptr(const common::ObIArray<ObExpr*> *exprs, int64_t size) const
{
  if (OB_NOT_NULL(exprs) && size > 0) {
    for (int64_t i = 0; i < exprs->count(); i++) {
      ObExpr *expr = exprs->at(i);
      if (OB_NOT_NULL(expr)) {
        expr->locate_datums_for_update(*eval_ctx_, size);
        ObEvalInfo &info = expr->get_eval_info(*eval_ctx_);
        info.point_to_frame_ = true;
      }
    }
  }
}

int ObDASIndexMergeIter::check_disable_bitmap()
{
  int ret = OB_SUCCESS;
  disable_bitmap_ = false;
  if (OB_LIKELY(!rowkey_is_uint64_)) {
    disable_bitmap_ = true;
  }
  return ret;
}

int ObDASIndexMergeIter::check_rowkey_is_uint64()
{
  int ret = OB_SUCCESS;
  rowkey_is_uint64_ = false;
  if (OB_ISNULL(rowkey_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey exprs is null", K(ret));
  } else if (OB_UNLIKELY(rowkey_exprs_->count() != 1)) {
  } else {
    const ObExpr *expr = rowkey_exprs_->at(0);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (OB_LIKELY(expr->obj_meta_.is_unsigned_integer())) {
      rowkey_is_uint64_ = true;
    }
  }
  return ret;
}

int ObDASIndexMergeIter::init_mock_skip(common::ObIAllocator &alloc, int64_t max_size)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = alloc.alloc(ObBitVector::memory_size(max_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc datum", K(ret), K(max_size));
  } else {
    mock_skip_ = to_bit_vector(buf);
    mock_skip_->init(max_size);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
