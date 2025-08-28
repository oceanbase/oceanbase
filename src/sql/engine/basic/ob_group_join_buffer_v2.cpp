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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_group_join_buffer_v2.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDriverRowBuffer::ObDriverRowBuffer()
  :op_(nullptr), left_(nullptr), left_brs_(), spec_(nullptr), ctx_(nullptr), eval_ctx_(nullptr),
   rescan_params_(nullptr), left_store_(), left_store_iter_(), mem_context_(),
   cur_group_idx_(-1), group_rescan_cnt_(0), group_params_(), last_batch_(),
   max_group_size_(0), group_scan_size_(0), left_store_read_(0), flags_(0) 
{

}

int ObDriverRowBuffer::init(ObOperator *op,
                             const int64_t max_group_size,
                             const int64_t group_scan_size,
                             const common::ObIArray<ObDynamicParamSetter> *rescan_params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("batch info was already inited", KR(ret));
  } else if (OB_UNLIKELY(NULL != mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem context should be null", KR(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op is null", KR(ret), KP(op));
  } else if (OB_UNLIKELY(op->get_child_cnt() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op should have at least 2 children", KR(ret), K(op->get_child_cnt()));
  } else if (max_group_size < group_scan_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("max group size is less than group scan size", K(max_group_size), K(group_scan_size));
  } else {
    op_ = op;
    spec_ = &op_->get_spec();
    ctx_ = &op_->get_exec_ctx();
    eval_ctx_ = &op_->get_eval_ctx();
    left_ = op_->get_child();
    max_group_size_ = max_group_size;
    group_scan_size_ = group_scan_size;
    rescan_params_ = rescan_params;
    ObMemAttr mem_attr(ctx_->get_my_session()->get_effective_tenant_id(), ObModIds::OB_SQL_NLJ_CACHE, ObCtxIds::WORK_AREA);
    lib::ContextParam param;
    ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
    bool reorder_fixed_expr = false;
    bool enable_trunc = false;
    param.set_mem_attr(mem_attr)
            .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", KR(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity", KR(ret));
    } else if (OB_FAIL(left_store_.init(left_->get_spec().output_, eval_ctx_->max_batch_size_, mem_attr, UINT64_MAX, true, 0, compressor_type, false, false))) {
      LOG_WARN("init row store failed", KR(ret));
    } else if (FALSE_IT(left_store_.set_allocator(mem_context_->get_malloc_allocator()))) {
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(last_batch_.init(left_->get_spec().output_,
                                   *eval_ctx_))) {
        LOG_WARN("init batch failed", KR(ret));
      } else if (OB_FAIL(init_left_batch_rows())) {
        LOG_WARN("failed to init left batch rows", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObDriverRowBuffer::init_left_batch_rows()
{
  int ret = OB_SUCCESS;
  if (op_->is_vectorized()) {
    if (OB_ISNULL(ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec ctx is nullptr", K(ret));
    } else {
      int batch_size = eval_ctx_->max_batch_size_;
      void *mem =ctx_->get_allocator().alloc(ObBitVector::memory_size(batch_size));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(batch_size));
      } else {
        left_brs_.skip_ = to_bit_vector(mem);
        left_brs_.skip_->init(batch_size);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parent op is not vectorized", K(ret));
  }
  return ret;
}

void ObDriverRowBuffer::bind_group_params_to_das_ctx(GroupParamBackupGuard &guard)
{
  guard.bind_batch_rescan_params(cur_group_idx_, group_rescan_cnt_, &rescan_params_info_);
}

int ObDriverRowBuffer::rescan_left()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_->rescan())) {
    LOG_WARN("rescan left failed", KR(ret),
              K(left_->get_spec().get_id()), K(left_->op_name()));
  }
  return ret;
}

void ObDriverRowBuffer::destroy()
{
  left_store_iter_.reset();
  left_store_.reset();
  last_batch_.reset();
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

void ObDriverRowBuffer::reset_buffer_state()
{
  cur_group_idx_ = -1;
  left_store_iter_.reset();
  left_store_.reset();
  left_store_read_ = 0;
  last_batch_.clear_saved_size();
  mem_context_->get_arena_allocator().reset();
}

void ObDriverRowBuffer::reset()
{
  left_store_iter_.reset();
  left_store_.reset();
  save_last_batch_ = false;
  last_batch_.reset();
  cur_group_idx_ = -1;
  left_store_read_ = 0;
  is_left_end_ = false;
}

int ObDriverRowBuffer::add_row_to_store()
{
  int ret = OB_SUCCESS;
  ObCompactRow *compact_row = nullptr;
  if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, *eval_ctx_, compact_row))) {
    LOG_WARN("add row failed", KR(ret));
  }
  return ret;
}

int ObDriverRowBuffer::init_group_params()
{
  int ret = OB_SUCCESS;
  if (!group_params_.empty()) {
    for (int64_t i = 0; i < group_params_.count(); ++i) {
      group_params_.at(i).count_ = 0;
    }
  } else if (OB_FAIL(group_params_.allocate_array(ctx_->get_allocator(),
                                                  rescan_params_->count()))) {
    LOG_WARN("allocate group params array failed", KR(ret), K(rescan_params_->count()));
  } else {
    int64_t obj_buf_size = sizeof(ObObjParam) * max_group_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_params_.count(); ++i) {
      ObExpr *dst_expr = rescan_params_->at(i).dst_;
      void *buf = ctx_->get_allocator().alloc(obj_buf_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", KR(ret), K(obj_buf_size));
      } else {
        group_params_.at(i).data_ = reinterpret_cast<ObObjParam*>(buf);
        group_params_.at(i).count_ = 0;
        group_params_.at(i).element_.set_meta_type(dst_expr->obj_meta_);
      }
    }
  }

  // collect batch nlj params needed by rescan right op
  if (OB_FAIL(ret)) {
  } else if (group_params_.empty()) {
    // do nothing
  } else if (rescan_params_info_.empty()) { // only perform once
    int64_t rescan_params_info_cnt = group_params_.count();
    if (OB_FAIL(rescan_params_info_.allocate_array(ctx_->get_allocator(),rescan_params_info_cnt))) {
      LOG_WARN("failed to allocate group param info", K(ret), K(rescan_params_info_cnt));
    } else {
      // collect rescan params of current nlj op
      int64_t j = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < group_params_.count() && j < rescan_params_info_.count(); ++i, ++j) {
        int64_t param_idx = rescan_params_->at(i).param_idx_;
        rescan_params_info_.at(j).param_idx_ = param_idx;
        rescan_params_info_.at(j).gr_param_ = &group_params_.at(i);
      }
    }
  }
  return ret;
}

// Optimize: without copy the value to paramstore, and copy from paramstore to objarray
int ObDriverRowBuffer::deep_copy_dynamic_obj()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
  ParamStore &param_store = plan_ctx->get_param_store_for_update();
  if (OB_ISNULL(mem_context_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem entity is not inited", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rescan_params_->count(); ++i) {
    const ObDynamicParamSetter &rescan_param = rescan_params_->at(i);
    int64_t param_idx = rescan_param.param_idx_;
    ObExpr* src_expr = rescan_param.src_;
    ObObj tmp_obj;
    ObDatum* res_datum = nullptr;
    // NOTE: use eval_vector here
    if (OB_FAIL(src_expr->eval(*eval_ctx_, res_datum))) {
      LOG_WARN("failed to eval src expr", K(ret));
    } else if (OB_ISNULL(res_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the result ObDatum of res expr is nullptr", K(ret));
    } else if (OB_FAIL(res_datum->to_obj(tmp_obj, src_expr->obj_meta_, src_expr->obj_datum_map_))) {
      LOG_WARN("failed to convert ObDatum to ObObj", K(ret));
    } else if (OB_FAIL(ob_write_obj(mem_context_->get_arena_allocator(),
                             tmp_obj,
                             group_params_.at(i).data_[group_params_.at(i).count_]))) {
      LOG_WARN("deep copy dynamic param failed", KR(ret), K(i), K(param_idx));
    } else {
      group_params_.at(i).count_++;
    }
  }
  return ret;
}

int ObDriverRowBuffer::fill_cur_row_group_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_->get_physical_plan_ctx();
  if (group_params_.empty() || cur_group_idx_ >= group_params_.at(0).count_) {
    ret = OB_ERR_UNEXPECTED;
    if (group_params_.empty()) {
      LOG_WARN("empty group params", KR(ret), K(cur_group_idx_), K(group_params_.empty()));
    } else {
      LOG_WARN("row idx is unexpected", KR(ret),
               K(cur_group_idx_), K(group_params_.count()));
    }
  } else {
    cur_group_idx_++;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_params_.count(); i++) {
      const ObDynamicParamSetter &rescan_param = rescan_params_->at(i);
      int64_t param_idx = rescan_param.param_idx_;
      ObExpr *dst = rescan_param.dst_;
      OZ(dst->init_vector(*eval_ctx_, VEC_UNIFORM_CONST, 1));
      ObDatum &param_datum = dst->locate_datum_for_write(*eval_ctx_);
      ObSqlArrayObj &arr = group_params_.at(i);
      dst->get_eval_info(*eval_ctx_).clear_evaluated_flag();
      ObDynamicParamSetter::clear_parent_evaluated_flag(*eval_ctx_, *dst);
      if (OB_FAIL(param_datum.from_obj(arr.data_[cur_group_idx_], dst->obj_datum_map_))) {
        LOG_WARN("fail to cast datum", K(ret));
      } else {
        plan_ctx->get_param_store_for_update().at(param_idx) = arr.data_[cur_group_idx_];
        dst->set_evaluated_projected(*eval_ctx_);
      }
    }
  }
  return ret;
}

int ObDriverRowBuffer::batch_fill_group_buffer(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (!is_left_end_ && need_fill_group_buffer()) {
    group_rescan_cnt_++;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    if (save_last_batch_) {
      last_batch_.restore();
      save_last_batch_ = false;
    }
    const ObBatchRows *batch_rows = &left_->get_brs();
    reset_buffer_state();
    if (OB_FAIL(init_group_params())) {
      LOG_WARN("init group params failed", KR(ret));
    }
    while (OB_SUCC(ret) && !is_full() && !batch_rows->end_) {
      op_->clear_evaluated_flag();
      if (!rescan_params_->empty()) {
        op_->set_pushdown_param_null(*rescan_params_);
      }
      if (OB_FAIL(left_->get_next_batch(max_row_cnt, batch_rows))) {
        LOG_WARN("get next batch from left failed", KR(ret));
      } else if (batch_rows->end_) {
        is_left_end_ = true;
      } else {
        for (int64_t l_idx = 0;  OB_SUCC(ret) && l_idx < batch_rows->size_; l_idx++) {
          if (batch_rows->skip_->exist(l_idx)) {
            // do nothing
          } else {
            batch_info_guard.set_batch_idx(l_idx);
            batch_info_guard.set_batch_size(batch_rows->size_);
            if (OB_FAIL(add_row_to_store())) {
              LOG_WARN("store left row failed", KR(ret));
            } else if (OB_FAIL(deep_copy_dynamic_obj())) {
              LOG_WARN("deep copy dynamic obj failed", KR(ret));
            }
          }
        }
      } // end add a batch rows to store
    } // end fill group join buffer

    if (OB_SUCC(ret)) {
      if (!rescan_params_->empty()) {
        op_->set_pushdown_param_null(*rescan_params_);
      }

      if (batch_rows->size_ == 0 && batch_rows->end_) {
        // do nothing
      } else if (OB_FAIL(last_batch_.save(spec_->max_batch_size_))) {
        LOG_WARN("failed to save last batch", K(ret));
      } else {
        save_last_batch_ = true;
      }

      if (OB_SUCC(ret)) {
        if (left_store_.get_row_cnt() <= 0) {
          // this could happen if we have skipped all rows
          ret = OB_ITER_END;
        } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
          LOG_WARN("begin iterator for chunk row store failed", KR(ret));
        }
      }
    }
  }
  return ret;
}


int ObDriverRowBuffer::get_next_left_batch(int64_t max_rows, const ObBatchRows *&batch_rows)
{
  int ret = OB_SUCCESS;
  // if joinbuffer is empty, fill joinbuffer first, and then get next batch from joinbuffer
  int64_t read_size = 0;
  batch_rows = &left_brs_;
  if (OB_FAIL(batch_fill_group_buffer(max_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to batch fill group join buffer", KR(ret));
    } else {
      left_brs_.size_ = 0;
      left_brs_.end_ = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_next_batch_from_store(max_rows, read_size))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next batch from store failed", KR(ret));
    } else {
      left_brs_.size_ = 0;
      left_brs_.end_ = true;
      ret = OB_SUCCESS;
    }
  } else {
    left_brs_.skip_->reset(read_size);
    left_brs_.size_ = read_size;
    left_brs_.end_ = false;
  }
  return ret;
}

int ObDriverRowBuffer::get_next_batch_from_store(int64_t max_rows, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  // when disable vectorization, at least read one row
  max_rows = max_rows > 0 ? max_rows : 1;
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    if (OB_FAIL(left_store_iter_.get_next_batch(left_->get_spec().output_, *eval_ctx_, max_rows, read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from iter failed", KR(ret));
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

ObDriverRowIterator::ObDriverRowIterator():
  left_brs_(nullptr), l_idx_(0), op_(nullptr), left_(nullptr),
  join_buffer_(), left_batch_(), rescan_params_(nullptr), is_group_rescan_(false),
  eval_ctx_(nullptr), op_max_batch_size_(0), need_backup_left_(false), left_expr_extend_size_(0), ctx_(nullptr)
{

}

int ObDriverRowIterator::init(ObOperator *op, const int64_t op_group_scan_size,
                              const common::ObIArray<ObDynamicParamSetter> *rescan_params, bool is_group_rescan,
                              bool need_backup_left)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op is null", KR(ret), KP(op));
  } else if (OB_UNLIKELY(op->get_child_cnt() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op should have at least 2 children", KR(ret), K(op->get_child_cnt()));
  } else if (OB_ISNULL(rescan_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rescan params is nullptr", K(ret));
  } else {
    l_idx_ = 0;
    op_ = op;
    left_ = op->get_child();
    eval_ctx_ = &op->get_eval_ctx();
    is_group_rescan_ = is_group_rescan;
    op_max_batch_size_ = left_->get_spec().max_batch_size_;
    need_backup_left_ = need_backup_left;
    left_expr_extend_size_ = 0;
    rescan_params_ = rescan_params;
    ctx_ = &op->get_exec_ctx();
    if (OB_SUCC(ret) && op_->is_vectorized()) {
      if (OB_FAIL(left_batch_.init(left_->get_spec().output_, *eval_ctx_))) {
        LOG_WARN("failed to init left batch result holder", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_group_rescan_) {
      int64_t simulate_group_size = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_GROUP_SIZE);
      int64_t group_scan_size = 0;
      int64_t max_group_size = 0;
      if (simulate_group_size > 0) {
        max_group_size = simulate_group_size + op_->get_spec().plan_->get_batch_size();
        group_scan_size = simulate_group_size;
      } else {
        max_group_size = OB_MAX_BULK_JOIN_ROWS + op_->get_spec().plan_->get_batch_size();
        group_scan_size = op_group_scan_size;
      }

      if (OB_FAIL(join_buffer_.init(op, max_group_size, group_scan_size, rescan_params))) {
        LOG_WARN("failed to init group join buffer for group rescan", K(ret));
      }
    }
  }
  return ret;
}

void ObDriverRowIterator::destroy()
{
  if (op_->is_vectorized()) {
    left_batch_.reset();
  }
  if (is_group_rescan_) {
    join_buffer_.destroy();
  }
}


int ObDriverRowIterator::get_next_left_batch(int64_t max_rows, const ObBatchRows *&batch_rows)
{
  int ret = OB_SUCCESS;
  DASGroupScanMarkGuard mark_guard(ctx_->get_das_ctx(), is_group_rescan_);
  if (!is_group_rescan_) {
    op_->set_pushdown_param_null(*rescan_params_);
    if (OB_FAIL(left_->get_next_batch(max_rows, batch_rows))) {
      LOG_WARN("failed to get batch from left child", K(ret));
    }
  } else {
    if (OB_FAIL(join_buffer_.get_next_left_batch(max_rows, batch_rows))) {
      LOG_WARN("failed to get next batch from join buffer", K(ret));
    }
  }
  left_brs_ = batch_rows;
  return ret;
}

int ObDriverRowIterator::fill_cur_row_group_param()
{
  int ret = OB_SUCCESS;
  if (is_group_rescan_) {
    if (OB_FAIL(join_buffer_.fill_cur_row_group_param())) {
      LOG_WARN("failed to fill group param from join buffer", K(ret));
    }
  } else {
    int64_t param_cnt = rescan_params_->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      const ObDynamicParamSetter &rescan_param = rescan_params_->at(i);
      if (OB_FAIL(rescan_param.set_dynamic_param_vec2(*eval_ctx_, *(left_brs_->skip_)))) {
        LOG_WARN("fail to set dynamic param", K(ret), K(l_idx_));
      }
    }
  }
  return ret;
}

int ObDriverRowIterator::get_next_left_row()
{
  int ret = OB_SUCCESS;
  bool get_row = false;
  op_->set_pushdown_param_null(*rescan_params_);
  left_expr_extend_size_ = 0;
  l_idx_++;
  while (!get_row && OB_SUCC(ret)) {
    // current batch has left row, no need to get left batch
    if (OB_NOT_NULL(left_brs_) && l_idx_ < left_brs_->size_) {
      for (int i = l_idx_; i < left_brs_->size_ && !get_row; i++) {
        if (!left_brs_->skip_->exist(l_idx_)) {
          get_row = true;
        } else {
          l_idx_ = i + 1;
        }
      }
      LOG_TRACE("get a row from left batch", K(l_idx_), K(get_row), K(left_brs_->size_));
    } else {
      // get a new batch
      LOG_TRACE("start get next batch from left op");
      if (need_backup_left_ && OB_FAIL(left_batch_.restore())) {
        LOG_WARN("failed to restore left batch rows", K(ret));
      } else if (OB_FAIL(get_next_left_batch(op_max_batch_size_, left_brs_))) {
        LOG_WARN("failed to get next left batch", K(ret));
      } else if (left_brs_->end_) {
        ret = OB_ITER_END;
      } else if (need_backup_left_ && OB_FAIL(left_batch_.save(left_->is_vectorized() ? op_max_batch_size_ : 1))) { // backup left_batch for NLJ
        LOG_WARN("failed to get backup left batch rows", K(ret));
      }
      l_idx_ = 0;
      LOG_TRACE("end get next batch from left", K(left_brs_->size_), K(left_brs_->end_));
    }
  }
  return ret;
}

int ObDriverRowIterator::drive_row_extend(int size)
{
  int ret = OB_SUCCESS;
  int min_vec_size = 0;
  if (OB_FAIL(get_min_vec_size_from_drive_row(min_vec_size))) {
    LOG_WARN("failed to get min vector size of drive row", K(ret));
  } else if (left_expr_extend_size_ < size || min_vec_size < size) {
    left_batch_.drive_row_extended(l_idx_, 0, size);
    left_expr_extend_size_ = size;
  }
  LOG_TRACE("drive_row extended", K(l_idx_), K(size), K(left_expr_extend_size_), K(min_vec_size), K(op_->get_spec().id_));
  return ret;
}

int ObDriverRowIterator::restore_drive_row(int from_idx, int to_idx)
{
  return left_batch_.restore_single_row(from_idx, to_idx);
}

int ObDriverRowIterator::rescan_left()
{
  int ret = OB_SUCCESS;
  if (is_group_rescan_) {
    if (OB_FAIL(join_buffer_.rescan_left())) {
      LOG_WARN("failed to rescan left in group rescan", K(ret));
    }
  } else {
    if (OB_FAIL(left_->rescan())) {
      LOG_WARN("failed to rescan left op", K(ret));
    }
  }
  return ret;
}

void ObDriverRowIterator::bind_group_params_to_das_ctx(GroupParamBackupGuard &guard)
{
  if (is_group_rescan_) {
    join_buffer_.bind_group_params_to_das_ctx(guard);
  }
}

void ObDriverRowIterator::reset()
{
  l_idx_ = 0;
  left_expr_extend_size_ = 0;
  left_batch_.clear_saved_size();
  if (is_group_rescan_) {
    join_buffer_.reset();
  }
  left_brs_ = nullptr;
}

int ObDriverRowIterator::get_min_vec_size_from_drive_row(int &min_vec_size) {
  int ret = OB_SUCCESS;
  min_vec_size = left_expr_extend_size_;
  if (OB_ISNULL(left_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drive operator of NLJ is null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < left_->get_spec().output_.count(); i++) {
      const ObExpr *expr = left_->get_spec().output_.at(i);
      if (OB_ISNULL(expr) || OB_ISNULL(expr->get_vector(*eval_ctx_))) {
         ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of drive operator for NLJ is null", K(ret), K(i));
      } else {
        int tmp_vec_size = static_cast<ObVectorBase *>(expr->get_vector(*eval_ctx_))->get_max_row_cnt();
        if (tmp_vec_size < left_expr_extend_size_) {
          LOG_TRACE("the vector size is less than left expr extended size", K(expr), K(i));
        }
        if (tmp_vec_size < min_vec_size) {
          min_vec_size = tmp_vec_size;
        }
      }
    }
  }
  return ret;
}

} // end anmespace oceanbase
} // end naemespace sql