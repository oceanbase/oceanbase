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
#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/das/ob_data_access_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int MergeStoreRows::init(common::ObIAllocator &allocator)
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

int MergeStoreRows::save(bool is_vectorized, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > max_size_) || OB_ISNULL(store_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for save store rows", K(size), K_(max_size), K(store_rows_), K(ret));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(store_rows_[i].save_store_row(*exprs_, *eval_ctx_))) {
          LOG_WARN("das merge iter failed to store rows", K(ret));
        }
      }
    } else if (OB_FAIL(store_rows_[0].save_store_row(*exprs_, *eval_ctx_))) {
      LOG_WARN("das merge iter failed to store rows", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    saved_size_ = size;
  }
  return ret;
}

int MergeStoreRows::to_expr(bool is_vectorized, int64_t size)
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

int64_t MergeStoreRows::get_group_idx(int64_t idx)
{
  OB_ASSERT(idx < saved_size_);
  return ObNewRange::get_group_idx(store_rows_[idx].store_row_->cells()[group_id_idx_].get_int());
}

int64_t MergeStoreRows::cur_group_idx()
{
  return get_group_idx(cur_idx_);
}

int64_t MergeStoreRows::row_cnt_with_cur_group_idx()
{
  OB_ASSERT(cur_idx_ < saved_size_);
  int64_t group_idx = cur_group_idx();
  // index of first row with greater group idx
  int64_t end_idx = cur_idx_ + 1;
  while (end_idx < saved_size_ && get_group_idx(end_idx) == group_idx) {
     end_idx++;
  }
  return end_idx - cur_idx_;
}

const ObDatum *MergeStoreRows::cur_datums()
{
  OB_ASSERT(cur_idx_ < saved_size_);
  return store_rows_[cur_idx_].store_row_->cells();
}

void MergeStoreRows::reuse()
{
  cur_idx_ = OB_INVALID_INDEX;
  saved_size_ = 0;
}

void MergeStoreRows::reset()
{
  exprs_ = nullptr;
  eval_ctx_ = nullptr;
  group_id_idx_ = OB_INVALID_INDEX;
  max_size_ = 1;
  saved_size_ = 0;
  cur_idx_ = OB_INVALID_INDEX;
  if (OB_NOT_NULL(store_rows_)) {
    for (int64_t i = 0; i < max_size_; i++) {
      store_rows_[i].~LastDASStoreRow();
    }
    store_rows_ = nullptr;
  }
}

int ObDASMergeIter::set_merge_status(MergeType merge_type)
{
  int ret = OB_SUCCESS;
  merge_type_ = used_for_keep_order_ ? MergeType::SORT_MERGE : merge_type;
  if (merge_type == MergeType::SEQUENTIAL_MERGE) {
    get_next_row_ = &ObDASMergeIter::get_next_seq_row;
    get_next_rows_ = &ObDASMergeIter::get_next_seq_rows;
    seq_task_idx_ = 0;
    DASTaskIter task_iter = das_ref_->begin_task_iter();
    if (need_update_partition_id_) {
      if (OB_FAIL(update_output_tablet_id(*task_iter))) {
        LOG_WARN("failed to update output tablet id", K(ret), K((*task_iter)->get_tablet_loc()->tablet_id_));
      }
    }
  } else {
    get_next_row_ = &ObDASMergeIter::get_next_sorted_row;
    get_next_rows_ = &ObDASMergeIter::get_next_sorted_rows;
    need_prepare_sort_merge_info_ = true;
  }

  return ret;
}

void ObDASMergeIter::set_global_lookup_iter(ObDASMergeIter *global_lookup_iter)
{
  wild_datum_info_.global_lookup_iter_ = global_lookup_iter;
}

common::ObIAllocator *ObDASMergeIter::get_das_alloc()
{
  common::ObIAllocator *alloc = nullptr;
  if (OB_NOT_NULL(das_ref_)) {
    alloc = &das_ref_->get_das_alloc();
  }
  return alloc;
}

int ObDASMergeIter::create_das_task(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&scan_op, bool &reuse_op)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = nullptr;
  // when the cluster version is less than 4.3.1, a DAS_OP_TABLE_BATCH_SCAN task is sent on group rescan situtations
  // for compatibility considerations.
  ObDASOpType op_type = (nullptr != group_id_expr_ && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_1_0) ? DAS_OP_TABLE_BATCH_SCAN : DAS_OP_TABLE_SCAN;
  reuse_op = false;
  if (OB_ISNULL(das_ref_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr das ref", K(ret));
  } else if (OB_NOT_NULL(task_op = das_ref_->find_das_task(tablet_loc, op_type))) {
    // reuse scan op
    reuse_op = true;
  } else if (OB_FAIL(das_ref_->create_das_task(tablet_loc, op_type, task_op))) {
    LOG_WARN("das ref failed to create das task", K(ret));
  }
  if (OB_SUCC(ret)) {
    scan_op = static_cast<ObDASScanOp*>(task_op);
  }
  return ret;
}

bool ObDASMergeIter::has_task() const
{
  bool bret = false;
  if (OB_NOT_NULL(das_ref_)) {
    bret = das_ref_->has_task();
  }
  return bret;
}

int32_t ObDASMergeIter::get_das_task_cnt() const
{
  int32_t cnt = 0;
  if (OB_NOT_NULL(das_ref_)) {
    cnt = das_ref_->get_das_task_cnt();
  }
  return cnt;
}

DASTaskIter ObDASMergeIter::begin_task_iter()
{
  DASTaskIter task_iter;
  if (OB_NOT_NULL(das_ref_)) {
    task_iter = das_ref_->begin_task_iter();
  }
  return task_iter;
}

bool ObDASMergeIter::is_all_local_task() const
{
  bool bret = false;
  if (OB_NOT_NULL(das_ref_)) {
    bret = das_ref_->is_all_local_task();
  }
  return bret;
}

int ObDASMergeIter::rescan_das_task(ObDASScanOp *scan_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(das_ref_) || OB_ISNULL(scan_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(das_ref_), K(scan_op), K(ret));
  } else if (OB_FAIL(MTL(ObDataAccessService*)->rescan_das_task(*das_ref_, *scan_op))) {
    LOG_WARN("failed to rescan das task", K(ret));
  }
  return ret;
}

int ObDASMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(das_ref_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr das ref", K(das_ref_), K(ret));
  } else if (OB_FAIL(das_ref_->execute_all_task())) {
    LOG_WARN("failed to execute all das task", K(ret));
  } else {
    DASTaskIter task_iter = das_ref_->begin_task_iter();
    for (; OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
      ObIDASTaskOp *das_task_ptr = task_iter.get_item();
      if (OB_ISNULL(das_task_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected das task nullptr", K(ret));
      } else if (OB_FAIL(das_tasks_arr_.push_back(das_task_ptr))) {
        LOG_WARN("failed to push back das task ptr", K(ret));
      }
    } // for end
    LOG_DEBUG("[DAS ITER] do table scan", K(ref_table_id_), K(das_tasks_arr_.count()));
  }
  return ret;
}

int ObDASMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_MERGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASMergeIterParam &merge_param = static_cast<ObDASMergeIterParam&>(param);
    eval_infos_ = merge_param.eval_infos_;
    need_update_partition_id_ = merge_param.need_update_partition_id_;
    pdml_partition_id_ = merge_param.pdml_partition_id_;
    partition_id_calc_type_ = merge_param.partition_id_calc_type_;
    ref_table_id_ = merge_param.ref_table_id_;
    should_scan_index_ = merge_param.should_scan_index_;
    is_vectorized_ = merge_param.is_vectorized_;
    iter_alloc_ = new (iter_alloc_buf_) common::ObArenaAllocator();
    iter_alloc_->set_attr(ObMemAttr(MTL_ID(), "ScanDASCtx"));
    das_ref_ = new (das_ref_buf_) ObDASRef(*eval_ctx_, *exec_ctx_);
    das_ref_->set_mem_attr(ObMemAttr(MTL_ID(), "ScanDASCtx"));
    das_ref_->set_expr_frame_info(merge_param.frame_info_);
    das_ref_->set_execute_directly(merge_param.execute_das_directly_);
    das_ref_->set_enable_rich_format(merge_param.enable_rich_format_);
    used_for_keep_order_ = merge_param.used_for_keep_order_;
    merge_type_ = used_for_keep_order_ ? SORT_MERGE : SEQUENTIAL_MERGE;

    if (group_id_expr_ != nullptr) {
      for (int64_t i = 0; i < output_->count(); i++) {
        if (output_->at(i) == group_id_expr_) {
          group_id_idx_ = i;
          break;
        }
      }
      if (group_id_idx_ == OB_INVALID_INDEX) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to get group id idx", K(ret), KPC_(group_id_expr), KPC_(output));
      }
    }
  }
  return ret;
}

int ObDASMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  seq_task_idx_ = OB_INVALID_INDEX;
  for (int64_t i = 0; i < merge_store_rows_arr_.count(); i++) {
    merge_store_rows_arr_.at(i).reset();
  }
  merge_store_rows_arr_.reuse();
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset_remain_one_page();
  }
  if (OB_NOT_NULL(das_ref_)) {
    if (OB_FAIL(das_ref_->close_all_task())) {
      LOG_WARN("das ref failed to close das task", K(ret));
    }
    das_ref_->reuse();
  }
  das_tasks_arr_.reuse();
  merge_state_arr_.reuse();
  return ret;
}

int ObDASMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < merge_store_rows_arr_.count(); i++) {
    merge_store_rows_arr_.at(i).reset();
  }
  merge_store_rows_arr_.reset();
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset();
    iter_alloc_->~ObArenaAllocator();
    iter_alloc_ = nullptr;
  }
  if (OB_NOT_NULL(das_ref_)) {
    if (OB_FAIL(das_ref_->close_all_task())) {
      LOG_WARN("das ref failed to close das task", K(ret));
    }
    das_ref_->reset();
    das_ref_->~ObDASRef();
    das_ref_ = nullptr;
  }
  das_tasks_arr_.reset();
  merge_state_arr_.reset();
  return ret;
}

int ObDASMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL((this->*get_next_row_)())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("das iter failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL((this->*get_next_rows_)(count, capacity))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("das merge iter failed to get next rows", K(ret));
    }
  }
  LOG_DEBUG("das merge iter get next rows end", K(count), K(merge_type_), K(merge_state_arr_), K(ret));
  const ObBitVector *skip = nullptr;
  PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);
  return ret;
}

void ObDASMergeIter::reset_datum_ptr(ObDASScanOp *scan_op, int64_t &capacity)
{
  ObDASCtx &das_ctx = scan_op->get_rtdef()->eval_ctx_->exec_ctx_.get_das_ctx();
  if (das_ctx.in_das_group_scan_) {
    int64_t simulate_max_rowsets = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_MAX_ROWSETS);
    capacity = (simulate_max_rowsets > 0 && simulate_max_rowsets < capacity) ? simulate_max_rowsets : capacity;
    scan_op->reset_access_datums_ptr(capacity);
  } else {
    reset_wild_datum_ptr();
  }
}

void ObDASMergeIter::reset_wild_datum_ptr()
{
  if (OB_NOT_NULL(wild_datum_info_.exprs_) && wild_datum_info_.max_output_rows_ > 0) {
    FOREACH_CNT(e, *wild_datum_info_.exprs_)
    {
      (*e)->locate_datums_for_update(*eval_ctx_, wild_datum_info_.max_output_rows_);
      ObEvalInfo &info = (*e)->get_eval_info(*eval_ctx_);
      info.point_to_frame_ = true;
    }
    wild_datum_info_.exprs_ = nullptr;
    wild_datum_info_.max_output_rows_ = 0;
  }

  // global index scan and its lookup maybe share some expr,
  // so remote lookup task change its datum ptr,
  // and also lead index scan touch the wild datum ptr
  // so need to associate the result iterator of scan and lookup
  // resetting the index scan result datum ptr will also reset the lookup result datum ptr
  if (OB_NOT_NULL(wild_datum_info_.global_lookup_iter_)) {
    wild_datum_info_.global_lookup_iter_->reset_wild_datum_ptr();
  }
}

void ObDASMergeIter::update_wild_datum_ptr(int64_t rows_count)
{
  wild_datum_info_.exprs_ = output_;
  wild_datum_info_.max_output_rows_ = std::max(wild_datum_info_.max_output_rows_, rows_count);
}

void ObDASMergeIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(eval_infos_)) {
    for (int64_t i = 0; i < eval_infos_->count(); i++) {
      eval_infos_->at(i)->clear_evaluated_flag();
    }
  }
}

int ObDASMergeIter::update_output_tablet_id(ObIDASTaskOp *output_das_task)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pdml_partition_id_) && OB_NOT_NULL(eval_ctx_) && OB_NOT_NULL(output_das_task)) {
    const ObDASTabletLoc *tablet_loc = nullptr;
    int64_t output_id = OB_INVALID_ID;
    if (partition_id_calc_type_ > 0) {
      tablet_loc = output_das_task->get_tablet_loc();
    } else if (should_scan_index_) {
      tablet_loc = ObDASUtils::get_related_tablet_loc(*output_das_task->get_tablet_loc(), ref_table_id_);
    } else {
      tablet_loc = output_das_task->get_tablet_loc();
    }

    if (OB_ISNULL(tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr tablet loc", K(ret));
    } else {
      if (partition_id_calc_type_ == 0) {
        output_id = tablet_loc->tablet_id_.id();
      } else if (partition_id_calc_type_ == 1) {
        output_id = tablet_loc->first_level_part_id_ != OB_INVALID_ID ?
                    tablet_loc->first_level_part_id_ : tablet_loc->partition_id_;
      } else if (partition_id_calc_type_ == 2) {
        output_id = tablet_loc->partition_id_;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("got invalid partition id calc type", K(partition_id_calc_type_), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const ObExpr *expr = pdml_partition_id_;
      if (is_vectorized_) {
        ObDatum *datums = expr->locate_datums_for_update(*eval_ctx_, max_size_);
        for (int64_t i = 0; i < max_size_; i++) {
          datums[i].set_int(output_id);
        }
      } else {
        expr->locate_datum_for_write(*eval_ctx_).set_int(output_id);
      }
      expr->set_evaluated_projected(*eval_ctx_);
      LOG_TRACE("find the partition id expr in pdml table scan", K(ret), K(output_id), K(expr), KPC(tablet_loc));
    }
  }
  return ret;
}

int ObDASMergeIter::get_next_seq_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (OB_UNLIKELY(seq_task_idx_ == OB_INVALID_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid index", K(ret));
  } else if (OB_UNLIKELY(seq_task_idx_ == das_tasks_arr_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !got_row) {
      clear_evaluated_flag();
      ObDASScanOp *scan_op = DAS_SCAN_OP(das_tasks_arr_.at(seq_task_idx_));
      if (OB_ISNULL(scan_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected das task op type", K(ret));
      } else {
        if (OB_SUCC(scan_op->get_output_result_iter()->get_next_row())) {
          got_row = true;
        } else if (OB_ITER_END == ret) {
          ++seq_task_idx_;
          if (seq_task_idx_ == das_tasks_arr_.count()) {
            // keep the ret = OB_ITER_END
          } else {
            ret = OB_SUCCESS;
            scan_op = DAS_SCAN_OP(das_tasks_arr_.at(seq_task_idx_));
            if (need_update_partition_id_) {
              if (OB_FAIL(update_output_tablet_id(scan_op))) {
                LOG_WARN("failed to update output tablet id", K(ret), K(scan_op->get_tablet_loc()->tablet_id_));
              }
            }
          }
        } else {
          LOG_WARN("das iter failed to get next row", K(ret));
        }
      }
    } // while end
  }
  return ret;
}

int ObDASMergeIter::get_next_seq_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_rows = false;
  if (OB_UNLIKELY(seq_task_idx_ == OB_INVALID_INDEX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid index", K(ret));
  } else if (OB_UNLIKELY(seq_task_idx_ == das_tasks_arr_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !got_rows) {
      clear_evaluated_flag();
      ObDASScanOp *scan_op = DAS_SCAN_OP(das_tasks_arr_.at(seq_task_idx_));
      if (OB_ISNULL(scan_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected das task op type", K(ret));
      } else {
        if (scan_op->is_local_task()) {
          reset_datum_ptr(scan_op, capacity);
        }
        count = 0;
        ret = scan_op->get_output_result_iter()->get_next_rows(count, capacity);
        if (OB_ITER_END == ret && count > 0) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          got_rows = true;
          if (!scan_op->is_local_task()) {
            update_wild_datum_ptr(count);
          }
        } else if (OB_ITER_END == ret) {
          ++seq_task_idx_;
          if (seq_task_idx_ == das_tasks_arr_.count()) {
            // keep the ret = OB_ITER_END
          } else {
            ret = OB_SUCCESS;
            scan_op = DAS_SCAN_OP(das_tasks_arr_.at(seq_task_idx_));
            if (need_update_partition_id_) {
              if (OB_FAIL(update_output_tablet_id(scan_op))) {
                LOG_WARN("update output tablet id failed", K(ret), K(scan_op->get_tablet_loc()->tablet_id_));
              }
            }
          }
        } else {
          LOG_WARN("das iter failed to get next rows", K(ret));
        }
      }
    } // while end
  }
  return ret;
}

int ObDASMergeIter::get_next_sorted_row()
{
  int ret = OB_SUCCESS;
  int64_t output_idx = OB_INVALID_INDEX;
  if (OB_FAIL(prepare_sort_merge_info())) {
    LOG_WARN("failed to prepare sort merge info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < das_tasks_arr_.count(); i++) {
    if (!merge_state_arr_[i].das_task_iter_end_) {
      if (!merge_state_arr_[i].row_store_have_data_) {
        clear_evaluated_flag();
        ObDASScanOp *scan_op = DAS_SCAN_OP(das_tasks_arr_[i]);
        if (OB_ISNULL(scan_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected das task op type", K(ret), KPC(das_tasks_arr_[i]));
        } else if (OB_SUCC(scan_op->get_output_result_iter()->get_next_row())) {
          if (OB_FAIL(merge_store_rows_arr_[i].save(false, 1))) {
            LOG_WARN("failed to save store row", K(ret));
          } else {
            merge_state_arr_[i].row_store_have_data_ = true;
            if (OB_FAIL(compare(i, output_idx))) {
              LOG_WARN("failed to compare two rows", K(ret));
            }
          }
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          merge_state_arr_[i].das_task_iter_end_ = true;
        } else {
          LOG_WARN("das iter failed to get next row", K(ret));
        }
      } else if (OB_FAIL(compare(i, output_idx))) {
        LOG_WARN("failed to compare two rows", K(ret));
      }
    }
  } // for end

  if (OB_SUCC(ret)) {
    if (output_idx == OB_INVALID_INDEX) {
      ret = OB_ITER_END;
    } else {
      if (need_update_partition_id_) {
        if (OB_FAIL(update_output_tablet_id(das_tasks_arr_[output_idx]))) {
          ObTabletID tablet_id = das_tasks_arr_[output_idx]->get_tablet_loc()->tablet_id_;
          LOG_WARN("failed to update output tablet id", K(ret), K(tablet_id));
        }
      }
      ret = merge_store_rows_arr_[output_idx].to_expr(false, 1);
      if (OB_SUCC(ret)) {
        merge_state_arr_[output_idx].row_store_have_data_ = merge_store_rows_arr_[output_idx].have_data();
      } else {
        LOG_WARN("failed to convert store row to expr", K(output_idx), K(ret));
      }
    }
  }
  return ret;
}

int ObDASMergeIter::get_next_sorted_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (das_tasks_arr_.count() == 1) {
    // only one das task, no need to compare
    clear_evaluated_flag();
    ObDASScanOp *scan_op = DAS_SCAN_OP(das_tasks_arr_[0]);
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected das task op type", K(ret), KPC(scan_op));
    } else {
      if (scan_op->is_local_task()) {
        reset_datum_ptr(scan_op, capacity);
      }
      count = 0;
      ret = scan_op->get_output_result_iter()->get_next_rows(count, capacity);
      if (OB_ITER_END == ret && count > 0) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (!scan_op->is_local_task()) {
          update_wild_datum_ptr(count);
        }
      }
    }
  } else {
    int64_t output_idx = OB_INVALID_INDEX;
    if (OB_FAIL(prepare_sort_merge_info())) {
      LOG_WARN("failed to prepare sort merge info", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < das_tasks_arr_.count(); i++) {
      if (!merge_state_arr_[i].das_task_iter_end_) {
        if (!merge_state_arr_[i].row_store_have_data_) {
          clear_evaluated_flag();
          ObDASScanOp *scan_op = DAS_SCAN_OP(das_tasks_arr_[i]);
          if (OB_ISNULL(scan_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected das task op type", K(ret), KPC(das_tasks_arr_[i]));
          } else {
            if (scan_op->is_local_task()) {
              reset_datum_ptr(scan_op, capacity);
            }
            count = 0;
            ret = scan_op->get_output_result_iter()->get_next_rows(count, capacity);
            if (OB_ITER_END == ret && count > 0) {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              if (!scan_op->is_local_task()) {
                update_wild_datum_ptr(count);
              }
              if (OB_FAIL(merge_store_rows_arr_[i].save(true, count))) {
                LOG_WARN("failed to save store row", K(ret));
              } else {
                merge_state_arr_[i].row_store_have_data_ = true;
                if (OB_FAIL(compare(i, output_idx))) {
                  LOG_WARN("failed to compare two rows", K(ret));
                }
              }
            } else if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              merge_state_arr_[i].das_task_iter_end_ = true;
            } else {
              LOG_WARN("das iter failed to get next rows", K(ret));
            }
          }
        } else {
          if (OB_FAIL(compare(i, output_idx))) {
            LOG_WARN("failed to compare two rows", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (output_idx == OB_INVALID_INDEX) {
        count = 0;
        ret = OB_ITER_END;
      } else {
        // We need keep the datum points to the frame.
        reset_wild_datum_ptr();
        if (need_update_partition_id_) {
          if (OB_FAIL(update_output_tablet_id(das_tasks_arr_[output_idx]))) {
            ObTabletID tablet_id = das_tasks_arr_[output_idx]->get_tablet_loc()->tablet_id_;
            LOG_WARN("failed to update output tablet id", K(ret), K(tablet_id));
          }
        }
        MergeStoreRows &store_rows = merge_store_rows_arr_.at(output_idx);
        int64_t ret_count = used_for_keep_order_ ? 1 : store_rows.row_cnt_with_cur_group_idx();
        ret = store_rows.to_expr(true, ret_count);
        if (OB_SUCC(ret)) {
          count = ret_count;
          merge_state_arr_[output_idx].row_store_have_data_ = merge_store_rows_arr_[output_idx].have_data();
        } else {
          LOG_WARN("failed to convert store row to expr", K(output_idx), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASMergeIter::prepare_sort_merge_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(need_prepare_sort_merge_info_)) {
    if (das_tasks_arr_.count() > 0) {
      // init merge state for each das task
      if (merge_state_arr_.empty()) {
        if (OB_FAIL(merge_state_arr_.reserve(das_tasks_arr_.count()))) {
          LOG_WARN("failed to reserve merge state array", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < das_tasks_arr_.count(); i++) {
            if (OB_FAIL(merge_state_arr_.push_back(MergeState()))) {
              LOG_WARN("failed to push back merge state", K(ret));
            }
          }
        }
      } else {
        for (int64_t i = 0; i < merge_state_arr_.count(); i++) {
          merge_state_arr_.at(i).reuse();
        }
      }

      // init store rows for each das task
      if (merge_store_rows_arr_.empty()) {
        if (OB_FAIL(merge_store_rows_arr_.reserve(das_tasks_arr_.count()))) {
          LOG_WARN("failed to reserve merge store rows array", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < das_tasks_arr_.count(); i++) {
            if (OB_FAIL(merge_store_rows_arr_.push_back(
                MergeStoreRows(output_, eval_ctx_, group_id_idx_, max_size_)))) {
              LOG_WARN("failed to push back merge store rows", K(ret));
            } else if (OB_FAIL(merge_store_rows_arr_.at(i).init(*iter_alloc_))) {
              LOG_WARN("failed to init merge store rows", K(ret));
            }
          }
        }
      } else {
        for (int64_t i = 0; i < merge_store_rows_arr_.count(); i++) {
          merge_store_rows_arr_.at(i).reuse();
        }
      }
    }
    need_prepare_sort_merge_info_ = false;
  }
  return ret;
}


// [GROUP_ID] is composed of group_idx and index_ordered_idx now,
// we should compare group_idx first and then index_ordered_idx,
// group_idx and index_ordered_idx should always be sorted in ascending order.
int ObDASMergeIter::compare(int64_t cur_idx, int64_t &output_idx)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == output_idx) {
    output_idx = cur_idx;
  } else {
    const ObDatum *cur_datums = merge_store_rows_arr_[cur_idx].cur_datums();
    const ObDatum *output_datums = merge_store_rows_arr_[output_idx].cur_datums();
    if (nullptr != group_id_expr_) {
      int64_t cur_group_idx = ObNewRange::get_group_idx(cur_datums[group_id_idx_].get_int());
      int64_t output_group_idx = ObNewRange::get_group_idx(output_datums[group_id_idx_].get_int());
      if (cur_group_idx != output_group_idx) {
        output_idx = cur_group_idx < output_group_idx ? cur_idx : output_idx;
      } else {
        int64_t cur_order_idx = ObNewRange::get_index_ordered_idx(cur_datums[group_id_idx_].get_int());
        int64_t output_order_idx = ObNewRange::get_index_ordered_idx(output_datums[group_id_idx_].get_int());
        if (cur_order_idx != output_order_idx) {
          output_idx = cur_order_idx < output_order_idx ? cur_idx : output_idx;
        }
      }
    }
  }
  LOG_DEBUG("das merge iter compare finished", K(cur_idx), K(output_idx), K(used_for_keep_order_));
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
