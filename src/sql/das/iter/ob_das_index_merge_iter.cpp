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
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeIter::RowStore::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_rows_ =
      static_cast<LastDASStoreRow*>(allocator.alloc(max_size_ * sizeof(LastDASStoreRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K_(max_size), K(ret));
  } else {
    for (int64_t i = 0; i < max_size_; i++) {
      new (store_rows_ + i) LastDASStoreRow(allocator);
      store_rows_[i].reuse_ = true;
    }
  }

  return ret;
}

int ObDASIndexMergeIter::RowStore::save(bool is_vectorized, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > max_size_) || OB_ISNULL(store_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for save store rows", K(size), K_(max_size), K_(store_rows), K(ret));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(store_rows_[i].save_store_row(*exprs_, *eval_ctx_))) {
          LOG_WARN("index merge iter failed to store rows", K(ret));
        }
      }
    } else if (OB_FAIL(store_rows_[0].save_store_row(*exprs_, *eval_ctx_))) {
      LOG_WARN("index merge iter failed to store rows", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    saved_size_ = size;
  }

  return ret;
}

int ObDASIndexMergeIter::RowStore::to_expr(bool is_vectorized, int64_t size)
{
  int ret = OB_SUCCESS;
  if (is_vectorized) {
    if (cur_idx_ + size > saved_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, exceeds saved size", K_(cur_idx), K(size), K_(saved_size), K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        OZ(store_rows_[cur_idx_ + i].store_row_->to_expr<true>(*exprs_, *eval_ctx_));
      }
      cur_idx_ += size;
    }
  } else {
    OZ(store_rows_[cur_idx_].store_row_->to_expr<false>(*exprs_, *eval_ctx_));
    cur_idx_++;
  }

  return ret;
}

const ObDatum *ObDASIndexMergeIter::RowStore::cur_datums()
{
  OB_ASSERT(cur_idx_ < saved_size_);
  return store_rows_ != nullptr ? store_rows_[cur_idx_].store_row_->cells() : nullptr;
}

void ObDASIndexMergeIter::RowStore::reuse()
{
  cur_idx_ = OB_INVALID_INDEX;
  saved_size_ = 0;
}

void ObDASIndexMergeIter::RowStore::reset()
{
  if (OB_NOT_NULL(store_rows_)) {
    for (int64_t i = 0; i < max_size_; i++) {
      store_rows_[i].~LastDASStoreRow();
    }
    store_rows_ = nullptr;
  }
  exprs_ = nullptr;
  eval_ctx_ = nullptr;
  max_size_ = 1;
  saved_size_ = 0;
  cur_idx_ = OB_INVALID_INDEX;
}

int ObDASIndexMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    merge_type_ = index_merge_param.merge_type_;
    left_iter_ = index_merge_param.left_iter_;
    right_iter_ = index_merge_param.right_iter_;
    rowkey_exprs_ = index_merge_param.rowkey_exprs_;
    left_output_ = index_merge_param.left_output_;
    right_output_ = index_merge_param.right_output_;
    get_next_row_ = (merge_type_ == INDEX_MERGE_UNION) ?
        &ObDASIndexMergeIter::union_get_next_row : &ObDASIndexMergeIter::intersect_get_next_row;
    get_next_rows_ = (merge_type_ == INDEX_MERGE_UNION) ?
        &ObDASIndexMergeIter::union_get_next_rows : &ObDASIndexMergeIter::intersect_get_next_rows;
    merge_ctdef_ = index_merge_param.ctdef_;
    merge_rtdef_ = index_merge_param.rtdef_;
    tx_desc_ = index_merge_param.tx_desc_;
    snapshot_ = index_merge_param.snapshot_;
    is_reverse_ = index_merge_param.is_reverse_;
    if (merge_ctdef_->is_left_child_leaf_node_) {
      OB_ASSERT(merge_rtdef_->children_[0]->op_type_ == DAS_OP_TABLE_SCAN ||
                (merge_rtdef_->children_[0]->op_type_ == DAS_OP_SORT &&
                 merge_rtdef_->children_[0]->children_[0]->op_type_ == DAS_OP_TABLE_SCAN));
      left_scan_rtdef_ = merge_rtdef_->children_[0]->op_type_ == DAS_OP_TABLE_SCAN ?
          static_cast<ObDASScanRtDef*>(merge_rtdef_->children_[0]) :
          static_cast<ObDASScanRtDef*>(merge_rtdef_->children_[0]->children_[0]);
      OB_ASSERT(left_scan_rtdef_ != nullptr);
      left_scan_ctdef_ = static_cast<const ObDASScanCtDef*>(left_scan_rtdef_->ctdef_);
    }
    OB_ASSERT(merge_rtdef_->children_[1]->op_type_ == DAS_OP_TABLE_SCAN ||
              (merge_rtdef_->children_[1]->op_type_ == DAS_OP_SORT &&
               merge_rtdef_->children_[1]->children_[0]->op_type_ == DAS_OP_TABLE_SCAN));
    right_scan_rtdef_ = merge_rtdef_->children_[1]->op_type_ == DAS_OP_TABLE_SCAN ?
        static_cast<ObDASScanRtDef*>(merge_rtdef_->children_[1]) :
        static_cast<ObDASScanRtDef*>(merge_rtdef_->children_[1]->children_[0]);
    OB_ASSERT(right_scan_rtdef_ != nullptr);
    right_scan_ctdef_ = static_cast<const ObDASScanCtDef*>(right_scan_rtdef_->ctdef_);

    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASIndexMerge", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_ctx_, context_param))) {
      LOG_WARN("failed to create index merge memctx", K(ret));
    } else {
      const common::ObIArray<ObExpr*> *left_exprs = (merge_type_ == INDEX_MERGE_UNION) ?
          rowkey_exprs_ : left_output_;
      const common::ObIArray<ObExpr*> *right_exprs = (merge_type_ == INDEX_MERGE_UNION) ?
          rowkey_exprs_ : right_output_;
      common::ObArenaAllocator &alloc = mem_ctx_->get_arena_allocator();
      if (OB_ISNULL(left_row_store_ = OB_NEWx(RowStore, &alloc, left_exprs, eval_ctx_, max_size_)) ||
          OB_ISNULL(right_row_store_ = OB_NEWx(RowStore, &alloc, right_exprs, eval_ctx_, max_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate row store", K(max_size_), K(ret));
      } else if (OB_FAIL(left_row_store_->init(alloc)) || OB_FAIL(right_row_store_->init(alloc))) {
        LOG_WARN("failed to init row store", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::init_scan_param(const share::ObLSID &ls_id,
                                         const common::ObTabletID &tablet_id,
                                         const sql::ObDASScanCtDef *ctdef,
                                         sql::ObDASScanRtDef *rtdef,
                                         ObTableScanParam &scan_param)
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
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
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

int ObDASIndexMergeIter::prepare_scan_ranges(ObTableScanParam &scan_param, const ObDASScanRtDef *rtdef)
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

void ObDASIndexMergeIter::reset_datum_ptr(const common::ObIArray<ObExpr*> *exprs, int64_t size)
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

int ObDASIndexMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (merge_ctdef_->is_left_child_leaf_node_) {
    OB_ASSERT(left_scan_ctdef_ != nullptr && left_scan_rtdef_ != nullptr);
    if (OB_FAIL(init_scan_param(ls_id_, left_tablet_id_, left_scan_ctdef_, left_scan_rtdef_, left_scan_param_))) {
      LOG_WARN("failed to init left scan param", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    OB_ASSERT(right_scan_ctdef_ != nullptr && right_scan_rtdef_ != nullptr);
    if (OB_FAIL(init_scan_param(ls_id_, right_tablet_id_, right_scan_ctdef_, right_scan_rtdef_, right_scan_param_))) {
      LOG_WARN("failed to init right scan param", K(ret));
    } else if (OB_FAIL(left_iter_->do_table_scan())) {
      LOG_WARN("left iter failed to do table scan", K(ret));
    } else if (OB_FAIL(right_iter_->do_table_scan())) {
      LOG_WARN("right iter failed to do table scan", K(ret));
    }
  }

  return ret;
}

int ObDASIndexMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (merge_ctdef_->is_left_child_leaf_node_) {
    left_scan_param_.tablet_id_ = left_tablet_id_;
    left_scan_param_.ls_id_ = ls_id_;
    if (OB_FAIL(prepare_scan_ranges(left_scan_param_, left_scan_rtdef_))) {
      LOG_WARN("failed to prepare left rescan ranges", K(ret));
    }
  }
  right_scan_param_.tablet_id_ = right_tablet_id_;
  right_scan_param_.ls_id_ = ls_id_;
  if (FAILEDx(prepare_scan_ranges(right_scan_param_, right_scan_rtdef_))) {
    LOG_WARN("failed to prepare right rescan ranges", K(ret));
  } else if (OB_FAIL(left_iter_->rescan())) {
    LOG_WARN("left iter failed to rescan", K(ret));
  } else if (OB_FAIL(right_iter_->rescan())) {
    LOG_WARN("left iter failed to rescan", K(ret));
  }
  return ret;
}

void ObDASIndexMergeIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(left_iter_)) {
    left_iter_->clear_evaluated_flag();
  }
  if (OB_NOT_NULL(right_iter_)) {
    right_iter_->clear_evaluated_flag();
  }
}

int ObDASIndexMergeIter::set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  if (merge_ctdef_->is_left_child_leaf_node_) {
    OB_ASSERT(left_scan_ctdef_ != nullptr);
    left_tablet_id_ = related_tablet_ids.index_merge_tablet_ids_.at(left_scan_ctdef_->index_merge_idx_);
  }
  OB_ASSERT(right_scan_ctdef_ != nullptr);
  right_tablet_id_ = related_tablet_ids.index_merge_tablet_ids_.at(right_scan_ctdef_->index_merge_idx_);
  return ret;
}

int ObDASIndexMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (merge_ctdef_->is_left_child_leaf_node_) {
    const ObTabletID &old_left_tablet_id = left_scan_param_.tablet_id_;
    left_scan_param_.need_switch_param_ = left_scan_param_.need_switch_param_ ||
        (old_left_tablet_id.is_valid() && old_left_tablet_id != left_tablet_id_);
    left_scan_param_.key_ranges_.reuse();
    left_scan_param_.ss_key_ranges_.reuse();
    left_scan_param_.mbr_filters_.reuse();
  }
  const ObTabletID &old_right_tablet_id = right_scan_param_.tablet_id_;
  right_scan_param_.need_switch_param_ = right_scan_param_.need_switch_param_ ||
      (old_right_tablet_id.is_valid() && old_right_tablet_id != right_tablet_id_);
  right_scan_param_.key_ranges_.reuse();
  right_scan_param_.ss_key_ranges_.reuse();
  right_scan_param_.mbr_filters_.reuse();

  if (OB_FAIL(left_iter_->reuse())) {
    LOG_WARN("index merge iter failed to reuse left iter", K(ret));
  } else if (OB_FAIL(right_iter_->reuse())) {
    LOG_WARN("index merge iter failed to reuse right iter", K(ret));
  }
  if (OB_NOT_NULL(left_row_store_)) {
    left_row_store_->reuse();
  }
  if (OB_NOT_NULL(right_row_store_)) {
    right_row_store_->reuse();
  }
  left_iter_end_ = false;
  right_iter_end_ = false;
  state_ = FILL_LEFT_ROW;

  return ret;
}

int ObDASIndexMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(left_row_store_)) {
    left_row_store_->reset();
    left_row_store_ = nullptr;
  }
  if (OB_NOT_NULL(right_row_store_)) {
    right_row_store_->reset();
    right_row_store_ = nullptr;
  }
  left_iter_end_ = false;
  right_iter_end_ = false;
  left_scan_param_.destroy_schema_guard();
  left_scan_param_.snapshot_.reset();
  left_scan_param_.destroy();
  right_scan_param_.destroy_schema_guard();
  right_scan_param_.snapshot_.reset();
  right_scan_param_.destroy();
  if (OB_NOT_NULL(mem_ctx_))  {
    mem_ctx_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = nullptr;
  }
  return ret;
}

int ObDASIndexMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL((this->*get_next_row_)())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("index merge iter failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL((this->*get_next_rows_)(count, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("index merge iter failed to get next rows", K(ret));
    }
  }
  LOG_TRACE("[DAS ITER] index merge iter get next rows", K(count), K(capacity), K(ret));
  const ObBitVector *skip = nullptr;
  PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);

  return ret;
}

int ObDASIndexMergeIter::compare(int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_row_store_) ||
      OB_ISNULL(right_row_store_) ||
      !left_row_store_->have_data() ||
      !right_row_store_->have_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KPC_(left_row_store), KPC_(right_row_store));
  } else {
    const ObDatum *left_datums = left_row_store_->cur_datums();
    const ObDatum *right_datums = right_row_store_->cur_datums();
    if (OB_ISNULL(left_datums) || OB_ISNULL(right_datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(left_datums), K(right_datums));
    }
    bool end_compare = false;
    ObObj left_obj;
    ObObj right_obj;
    for (int64_t i = 0; !end_compare && OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
      const ObExpr *expr = rowkey_exprs_->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(left_datums[i].to_obj(left_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert left datum to obj", K(i), KPC(expr), K(ret));
      } else if (OB_FAIL(right_datums[i].to_obj(right_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert right datum to obj", K(i), KPC(expr), K(ret));
      } else if (OB_FAIL(left_obj.check_collation_free_and_compare(right_obj, cmp_ret))) {
          LOG_WARN("failed to compare cur obj with output obj", K(ret));
      } else if (cmp_ret != 0) {
        end_compare =  true;
        cmp_ret = OB_UNLIKELY(is_reverse_) ? -cmp_ret : cmp_ret;
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::intersect_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  do {
    switch(state_) {
      case FILL_LEFT_ROW: {
        while (OB_SUCC(ret) && !left_iter_end_ && !left_row_store_->have_data()) {
          if (OB_FAIL(left_iter_->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from left iter", K(ret));
            } else {
              left_iter_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(left_row_store_->save(false, 1))) {
            LOG_WARN("failed to save left row", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = left_iter_end_ ? FINISHED : FILL_RIGHT_ROW;
        }
        break;
      }

      case FILL_RIGHT_ROW: {
        while (OB_SUCC(ret) && !right_iter_end_ && !right_row_store_->have_data()) {
          if (OB_FAIL(right_iter_->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from right iter", K(ret));
            } else {
              right_iter_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(right_row_store_->save(false, 1))) {
            LOG_WARN("failed to save right row", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = right_iter_end_ ? FINISHED : MERGE_AND_OUTPUT;
        }
        break;
      }

      case MERGE_AND_OUTPUT: {
        if (OB_SUCC(ret)) {
          int cmp_ret = 0;
          if (!left_row_store_->have_data() || !left_row_store_->have_data()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected no data in left_row or right_row", K_(left_row_store), K_(right_row_store));
          } else if (OB_FAIL(compare(cmp_ret))) {
            LOG_WARN("failed to compare left row and right row", K(ret));
          } else if (cmp_ret > 0) {
            state_ = FILL_RIGHT_ROW;
          } else if (cmp_ret < 0) {
            state_ = FILL_LEFT_ROW;
          } else {
            if (OB_FAIL(left_row_store_->to_expr(false, 1))) {
              LOG_WARN("failed to convert left store row to expr", K(ret));
            } else if (OB_FAIL(right_row_store_->to_expr(false, 1))) {
              LOG_WARN("failed to convert right store row to expr", K(ret));
            } else {
              state_ = FILL_LEFT_ROW;
              got_next_row = true;
            }
          }
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index merge state", K_(state));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObDASIndexMergeIter::intersect_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_next_rows = false;
  int64_t left_count = 0;
  int64_t right_count = 0;
  do {
    switch(state_) {
      case FILL_LEFT_ROW: {
        while (OB_SUCC(ret) && !left_iter_end_ && !left_row_store_->have_data()) {
          if (OB_FAIL(left_iter_->get_next_rows(left_count, capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from left iter", K(ret));
            } else {
              ret = OB_SUCCESS;
              if (left_count == 0) {
                left_iter_end_ = true;
              }
            }
          }
          if (OB_SUCC(ret) && !left_iter_end_ && OB_FAIL(left_row_store_->save(true, left_count))) {
            LOG_WARN("failed to save left rows", K(left_count), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = left_iter_end_ ? FINISHED : FILL_RIGHT_ROW;
        }
        break;
      }

      case FILL_RIGHT_ROW: {
        while (OB_SUCC(ret) && !right_iter_end_ && !right_row_store_->have_data()) {
          if (OB_FAIL(right_iter_->get_next_rows(right_count, capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from right iter", K(ret));
            } else {
              ret = OB_SUCCESS;
              if (right_count == 0) {
                right_iter_end_ = true;
              }
            }
          }
          if (OB_SUCC(ret) && !right_iter_end_ && OB_FAIL(right_row_store_->save(true, right_count))) {
            LOG_WARN("failed to save right rows", K(right_count), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = right_iter_end_ ? FINISHED : MERGE_AND_OUTPUT;
        }
        break;
      }

      case MERGE_AND_OUTPUT: {
        if (OB_SUCC(ret)) {
          int cmp_ret = 0;
          if (!left_row_store_->have_data()) {
            state_ = FILL_LEFT_ROW;
          } else if (!right_row_store_->have_data()) {
            state_ = FILL_RIGHT_ROW;
          } else if (OB_FAIL(compare(cmp_ret))) {
            LOG_WARN("failed to compare left row and right row", K(ret));
          } else if (cmp_ret > 0) {
            state_ = FILL_RIGHT_ROW;
          } else if (cmp_ret < 0) {
            state_ = FILL_LEFT_ROW;
          } else {
            if (OB_FAIL(left_row_store_->to_expr(false, 1))) {
              LOG_WARN("failed to convert left store row to expr", K(ret));
            } else if (OB_FAIL(right_row_store_->to_expr(false, 1))) {
              LOG_WARN("failed to convert right store row to expr", K(ret));
            } else {
              count = 1;
              got_next_rows = true;
            }
          }
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index merge state", K_(state));
      }
    }
  } while (!got_next_rows && OB_SUCC(ret));

  return ret;
}

int ObDASIndexMergeIter::union_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  do {
    switch(state_) {
      case FILL_LEFT_ROW: {
        while (OB_SUCC(ret) && !left_iter_end_ && !left_row_store_->have_data()) {
          if (OB_FAIL(left_iter_->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from left iter", K(ret));
            } else {
              left_iter_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(left_row_store_->save(false, 1))) {
            LOG_WARN("failed to save left row", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = FILL_RIGHT_ROW;
        }
        break;
      }

      case FILL_RIGHT_ROW: {
        while (OB_SUCC(ret) && !right_iter_end_ && !right_row_store_->have_data()) {
          if (OB_FAIL(right_iter_->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from right iter", K(ret));
            } else {
              right_iter_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(right_row_store_->save(false, 1))) {
            LOG_WARN("failed to save right row", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          state_ = MERGE_AND_OUTPUT;
        }
        break;
      }

      case MERGE_AND_OUTPUT: {
        if (OB_SUCC(ret)) {
          if (left_iter_end_ && right_iter_end_) {
            state_ = FINISHED;
          } else if (left_iter_end_) {
            if (right_row_store_->have_data()) {
              if (OB_FAIL(right_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to convert right store row to expr", K(ret));
              } else {
                got_next_row = true;
                state_ = FILL_RIGHT_ROW;
              }
            } else {
              state_ = FILL_RIGHT_ROW;
            }
          } else if (right_iter_end_) {
            if (left_row_store_->have_data()) {
              if (OB_FAIL(left_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to convert left store row to expr", K(ret));
              } else {
                got_next_row = true;
                state_ = FILL_LEFT_ROW;
              }
            } else {
              state_ = FILL_LEFT_ROW;
            }
          } else {
            int cmp_ret = 0;
            if (OB_FAIL(compare(cmp_ret))) {
              LOG_WARN("failed to compare left row and right row", K(ret));
            } else if (cmp_ret < 0) {
              if (OB_FAIL(left_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to convert left store row to expr", K(ret));
              } else {
                got_next_row = true;
                state_ = FILL_LEFT_ROW;
              }
            } else if (cmp_ret > 0) {
              if (OB_FAIL(right_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to convert right store row to expr", K(ret));
              } else {
                got_next_row = true;
                state_ = FILL_RIGHT_ROW;
              }
            } else {
              if (OB_FAIL(left_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to convert store row to expr", K(ret));
              } else if (OB_FAIL(right_row_store_->to_expr(false, 1))) {
                LOG_WARN("failed to ");
              } else {
                got_next_row = true;
                state_ = FILL_LEFT_ROW;
              }
            }
          }
        }

        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index merge state", K_(state));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObDASIndexMergeIter::union_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_next_rows = false;
  int64_t left_count = 0;
  int64_t right_count = 0;
  do {
    switch(state_) {
      case FILL_LEFT_ROW: {
        while (OB_SUCC(ret) && !left_iter_end_ && !left_row_store_->have_data()) {
          if (OB_FAIL(left_iter_->get_next_rows(left_count, capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from left iter", K(ret));
            } else {
              ret = OB_SUCCESS;
              if (left_count == 0) {
                left_iter_end_ = true;
              }
            }
          }
          if (OB_SUCC(ret) && !left_iter_end_ && OB_FAIL(left_row_store_->save(true, left_count))) {
            LOG_WARN("failed to save left rows", K(left_count), K(ret));
          }
          if (left_iter_->get_type() == DAS_ITER_SORT) {
            reset_datum_ptr(left_output_, left_count);
          }
        }

        if (OB_SUCC(ret)) {
          state_ = FILL_RIGHT_ROW;
        }
        break;
      }

      case FILL_RIGHT_ROW: {
        while (OB_SUCC(ret) && !right_iter_end_ && !right_row_store_->have_data()) {
          if (OB_FAIL(right_iter_->get_next_rows(right_count, capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from right iter", K(ret));
            } else {
              ret = OB_SUCCESS;
              if (right_count == 0) {
                right_iter_end_ = true;
              }
            }
          }
          if (OB_SUCC(ret) && !right_iter_end_ && OB_FAIL(right_row_store_->save(true, right_count))) {
            LOG_WARN("failed to save right rows", K(right_count), K(ret));
          }
          if (right_iter_->get_type() == DAS_ITER_SORT) {
            reset_datum_ptr(right_output_, right_count);
          }
        }

        if (OB_SUCC(ret)) {
          state_ = MERGE_AND_OUTPUT;
        }
        break;
      }

      case MERGE_AND_OUTPUT: {
        if (OB_SUCC(ret)) {
          if (left_iter_end_ && right_iter_end_) {
            state_ = FINISHED;
          } else if (left_iter_end_) {
            if (right_row_store_->have_data()) {
              int64_t right_rows_cnt = right_row_store_->rows_cnt();
              if (OB_FAIL(right_row_store_->to_expr(true, right_rows_cnt))) {
                LOG_WARN("failed to convert right store rows to expr", K(right_rows_cnt), K(ret));
              } else {
                count = right_rows_cnt;
                got_next_rows = true;
                state_ = FILL_RIGHT_ROW;
              }
            } else {
              state_ = FILL_RIGHT_ROW;
            }
          } else if (right_iter_end_) {
            if (left_row_store_->have_data()) {
              int64_t left_rows_cnt = left_row_store_->rows_cnt();
              if (OB_FAIL(left_row_store_->to_expr(true, left_rows_cnt))) {
                LOG_WARN("failed to convert left store rows to expr", K(left_rows_cnt), K(ret));
              } else {
                count = left_rows_cnt;
                got_next_rows = true;
                state_ = FILL_LEFT_ROW;
              }
            } else {
              state_ = FILL_LEFT_ROW;
            }
          } else {
            int cmp_ret = 0;
            if (OB_FAIL(compare(cmp_ret))) {
              LOG_WARN("failed to compare left row and right row", K(ret));
            } else if (cmp_ret < 0) {
              if (OB_FAIL(left_row_store_->to_expr(true, 1))) {
                LOG_WARN("failed to convert left store row to expr", K(ret));
              } else {
                got_next_rows = true;
                state_ = FILL_LEFT_ROW;
              }
            } else if (cmp_ret > 0) {
              if (OB_FAIL(right_row_store_->to_expr(true, 1))) {
                LOG_WARN("failed to convert right store row to expr", K(ret));
              } else {
                got_next_rows = true;
                state_ = FILL_RIGHT_ROW;
              }
            } else {
              if (OB_FAIL(left_row_store_->to_expr(true, 1))) {
                LOG_WARN("failed to convert left store row to expr", K(ret));
              } else if (OB_FAIL(right_row_store_->to_expr(true, 1))) {
                LOG_WARN("failed to convert right store row to expr", K(ret));
              } else {
                got_next_rows = true;
                state_ = FILL_LEFT_ROW;
              }
            }
            if (OB_SUCC(ret) && got_next_rows) {
              count = 1;
            }
          }
        }

        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index merge state", K_(state));
      }
    }
  } while (!got_next_rows && OB_SUCC(ret));

  return ret;
}


}  // namespace sql
}  // namespace oceanbase
