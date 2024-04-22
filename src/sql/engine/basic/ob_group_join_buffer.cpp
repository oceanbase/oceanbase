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

#include "ob_group_join_buffer.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObBatchRowDatums::init(const ObExprPtrIArray *exprs, ObIAllocator *alloc, int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc) || OB_ISNULL(exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(alloc), KP(exprs));
  } else {
    char *buf= (char *)alloc->alloc(ObBitVector::memory_size(batch_size)
                                    + sizeof(ObDatum) * batch_size * exprs->count());
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", KR(ret));
    } else {
      MEMSET(buf, 0, ObBitVector::memory_size(batch_size));
      skip_ = to_bit_vector(buf);
      alloc_ = alloc;
      exprs_ = exprs;
      datums_ = reinterpret_cast<ObDatum *>(buf + ObBitVector::memory_size(batch_size));
      batch_size_ = batch_size;
      size_ = 0;
      saved_size_ = 0;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObBatchRowDatums::from_exprs(ObEvalCtx &ctx, ObBitVector *skip, int64_t size)
{
  OB_ASSERT(size <= batch_size_);
  OB_ASSERT(OB_NOT_NULL(skip) && OB_NOT_NULL(exprs_));
  for (int64_t i = 0; i < exprs_->count(); i++) {
    ObExpr *expr = exprs_->at(i);
    ObDatum *datums = expr->locate_batch_datums(ctx);
    int64_t copy_size = (expr->is_batch_result() ? size: 1) * sizeof(ObDatum);
    MEMCPY(datums_ + i * batch_size_, datums, copy_size);
  }
  size_ = size;
  saved_size_ = size;
  skip_->deep_copy(*skip, size);
}

void ObBatchRowDatums::extend_save(ObEvalCtx &ctx, int64_t size)
{
  if (size > saved_size_) {
    for (int64_t i = 0; i < exprs_->count(); i++) {
      ObExpr *expr = exprs_->at(i);
      if (expr->is_batch_result()) {
        ObDatum *datums = expr->locate_batch_datums(ctx);
        int64_t copy_size = (size - saved_size_) * sizeof(ObDatum);
        MEMCPY(datums_ + i * batch_size_ + saved_size_, datums + saved_size_, copy_size);
      }
    }
    saved_size_ = size;
  }
}

void ObBatchRowDatums::to_exprs(ObEvalCtx &ctx)
{
  if (saved_size_ > 0) {
    for (int64_t i = 0; i < exprs_->count(); i++) {
      ObExpr *expr = exprs_->at(i);
      ObDatum *datums = expr->locate_batch_datums(ctx);
      int64_t copy_size = (expr->is_batch_result() ? saved_size_: 1) * sizeof(ObDatum);
      MEMCPY(datums, datums_ + i * batch_size_, copy_size);
    }
  }
}

void ObBatchRowDatums::to_exprs(ObEvalCtx &ctx, int64_t from_idx, int64_t to_idx)
{
  OB_ASSERT(from_idx <= size_ && to_idx <= batch_size_);
  OB_ASSERT(!skip_->exist(from_idx));
  for (int64_t i = 0; i < exprs_->count(); i++) {
    ObExpr *expr = exprs_->at(i);
    ObDatum *datums = expr->locate_batch_datums(ctx);
    if (!expr->is_batch_result()) {
      *datums = *(datums_ + i * batch_size_);
    } else {
      *(datums + to_idx) = *(datums_ + i * batch_size_ + from_idx) ;
    }
  }
}

ObGroupJoinBufffer::ObGroupJoinBufffer()
  : op_(NULL), spec_(NULL), ctx_(NULL), eval_ctx_(NULL),
    left_(NULL), right_(NULL), rescan_params_(NULL),
    left_rescan_params_(NULL), right_rescan_params_(NULL), mem_context_(NULL),
    left_store_("JoinBufStore"), left_store_iter_(), left_store_group_idx_(),
    above_left_group_params_(), above_right_group_params_(),
    group_params_(), above_group_params_(),
    last_row_(), last_batch_(),
    right_cnt_(0), cur_group_idx_(-1), left_store_read_(0),
    above_group_idx_for_expand_(0), above_group_idx_for_read_(0),
    above_group_size_(0), max_group_size_(0),
    group_scan_size_(0), group_rescan_cnt_(0), rescan_params_info_(), flags_(0)
{
  need_check_above_ = true;
}

int ObGroupJoinBufffer::init(ObOperator *op,
                             const int64_t max_group_size,
                             const int64_t group_scan_size,
                             const common::ObIArray<ObDynamicParamSetter> *rescan_params,
                             const common::ObIArray<ObDynamicParamSetter> *left_rescan_params,
                             const common::ObIArray<ObDynamicParamSetter> *right_rescan_params)
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
    right_ = op_->get_child(1);
    right_cnt_ = op_->get_child_cnt() - 1;
    max_group_size_ = max_group_size;
    group_scan_size_ = group_scan_size;
    rescan_params_ = rescan_params;
    left_rescan_params_ = left_rescan_params;
    right_rescan_params_ = right_rescan_params;
    ObSQLSessionInfo *session = ctx_->get_my_session();
    uint64_t tenant_id =session->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id,
                       ObModIds::OB_SQL_NLJ_CACHE,
                       ObCtxIds::WORK_AREA)
            .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", KR(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity", KR(ret));
    } else if (OB_FAIL(left_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
      LOG_WARN("init row store failed", KR(ret));
    } else if (FALSE_IT(left_store_.set_allocator(mem_context_->get_malloc_allocator()))) {
    }
    if (OB_SUCC(ret) && op_->is_vectorized()) {
      if (OB_FAIL(last_batch_.init(&left_->get_spec().output_,
                                   &ctx_->get_allocator(),
                                   spec_->max_batch_size_))) {
        LOG_WARN("init batch failed", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObGroupJoinBufffer::has_next_left_row(bool &has_next)
{
  int ret = OB_SUCCESS;
  has_next = false;
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    if (OB_UNLIKELY(left_store_read_ >= left_store_group_idx_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row read and group idx do not match", KR(ret),
               K(left_store_read_), K(left_store_group_idx_.count()));
    } else if (above_group_idx_for_read_ == left_store_group_idx_.at(left_store_read_)) {
      // above_group_idx_for_read_ < left_store_group_idx_.at(left_store_read_) means the row of above_group_idx_ is end,
      // need to return iter_end, and rescan current NLJ operator
      // we are still reading results for the current rescan param, need to rescan right child
      has_next = true;
    }
  } else {
    LOG_TRACE("Left child operator has no left rows for read,and the join buffer has no left row, needs to return iter_end_");
  }
  return ret;
}

int ObGroupJoinBufffer::init_above_group_params()
{
  int ret = OB_SUCCESS;
  if (need_check_above_) {
    need_check_above_ = false;
    int64_t left_group_size = 0;
    int64_t right_group_size = 0;
    if (OB_FAIL(build_above_group_params(*left_rescan_params_,
                                         above_left_group_params_,
                                         left_group_size))) {
      LOG_WARN("build above group params failed", KR(ret));
    } else if (OB_FAIL(build_above_group_params(*right_rescan_params_,
                                                above_right_group_params_,
                                                right_group_size))) {
      LOG_WARN("build above group params failed", KR(ret));
    } else if (left_group_size > 0) {
      // if above op only fills group params for our right child,
      // then it is just single level group rescan.
      if (OB_UNLIKELY(left_group_size != right_group_size && right_group_size > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left and right group sizes do not match", KR(ret),
                 K(left_group_size), K(right_group_size));
      } else {
        is_multi_level_ = true;
      }
    }
  }
  if (is_multi_level_) {
    LOG_TRACE("multi level group rescan", KR(ret),
              K(spec_->get_id()), K(spec_->get_type()), K(is_multi_level_));
  }
  return ret;
}

int ObGroupJoinBufffer::fill_cur_row_group_param()
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
    for (int64_t i = 0; OB_SUCC(ret) && i < group_params_.count(); i++) {
      const ObDynamicParamSetter &rescan_param = rescan_params_->at(i);
      int64_t param_idx = rescan_param.param_idx_;
      ObExpr *dst = rescan_param.dst_;
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
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_multi_level_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < above_right_group_params_.count(); i++) {
      if (NULL == above_right_group_params_.at(i)) {
        // skip
      } else {
        // we need to fill group rescan params according to our group step
        const ObDynamicParamSetter &rescan_param = right_rescan_params_->at(i);
        int64_t param_idx = rescan_param.param_idx_;
        ObExpr *dst = rescan_param.dst_;
        ObDatum &param_datum = dst->locate_datum_for_write(*eval_ctx_);
        ObSqlArrayObj *arr = above_right_group_params_.at(i);
        dst->get_eval_info(*eval_ctx_).clear_evaluated_flag();
        ObDynamicParamSetter::clear_parent_evaluated_flag(*eval_ctx_, *dst);
        if (OB_UNLIKELY(above_group_idx_for_read_ >= arr->count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected group idx", KR(ret), K(above_group_idx_for_read_), K(arr->count_));
        } else if (OB_FAIL(param_datum.from_obj(arr->data_[above_group_idx_for_read_], dst->obj_datum_map_))) {
          LOG_WARN("cast datum failed", KR(ret));
        } else {
          plan_ctx->get_param_store_for_update().at(param_idx) = arr->data_[above_group_idx_for_read_];
          dst->set_evaluated_projected(*eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::get_next_left_iter()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_multi_level_ || ((above_group_idx_for_expand_ + 1) >= above_group_size_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left op does not have another iterator", KR(ret));
  } else {
    // for multi level group rescan, left_ may output more than 1 iterators,
    // and we need to call left_->rescan() to switch to next iterator
    above_group_idx_for_expand_++;
    ObPhysicalPlanCtx *plan_ctx = ctx_->get_physical_plan_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < above_left_group_params_.count(); ++i) {
      ObSqlArrayObj *array_obj = above_left_group_params_.at(i);
      if (NULL == array_obj) {
        // skip
      } else {
        const ObDynamicParamSetter &rescan_param = left_rescan_params_->at(i);
        int64_t param_idx = rescan_param.param_idx_;
        ObExpr *dst = rescan_param.dst_;
        ObDatum &param_datum = dst->locate_datum_for_write(*eval_ctx_);
        dst->get_eval_info(*eval_ctx_).clear_evaluated_flag();
        ObDynamicParamSetter::clear_parent_evaluated_flag(*eval_ctx_, *dst);
        if (OB_FAIL(param_datum.from_obj(array_obj->data_[above_group_idx_for_expand_], dst->obj_datum_map_))) {
          LOG_WARN("cast datum failed", KR(ret));
        } else {
          plan_ctx->get_param_store_for_update().at(param_idx) = array_obj->data_[above_group_idx_for_expand_];
          dst->set_evaluated_projected(*eval_ctx_);
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(left_->rescan())) {
      ret = (OB_ITER_END == ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("rescan left failed", KR(ret));
    }
  }
  return ret;
}

int ObGroupJoinBufffer::drain_left()
{
  int ret = OB_SUCCESS;
  bool need_drain = !left_store_group_idx_.empty();
  const ObChunkDatumStore::StoredRow *row = NULL;
  // drain old rows from left store
  for (int64_t i = left_store_read_; OB_SUCC(ret) && need_drain && i < left_store_group_idx_.count(); ++i) {
    // In addition to the lines of the current group (above_group_idx_for_read_), the buffer also caches rows of subsequent groups
    if (above_group_idx_for_read_ != left_store_group_idx_.at(i)) {
      need_drain = false;
    } else if (OB_FAIL(left_store_iter_.get_next_row(row))) {
      ret = (OB_ITER_END == ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("get next row failed", KR(ret));
    } else {
      ++left_store_read_;
    }
  }
  // Only rows of the current group are left in the buffer, and there are rows of the current group that have not been added to the buffer
  // discard unread rows from left op
  if (OB_SUCC(ret) && need_drain && !is_left_end_) {
    if (!spec_->is_vectorized()) {
      while (OB_SUCC(ret)) {
        op_->clear_evaluated_flag();
        if (!rescan_params_->empty()) {
          op_->set_pushdown_param_null(*rescan_params_);
        }
        ret = left_->get_next_row();
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_left_end_ = true;
      } else {
        LOG_WARN("get next left row failed", KR(ret));
      }
    } else {
      const ObBatchRows *batch_rows = NULL;
      int64_t max_row_cnt = min(max_group_size_, spec_->max_batch_size_);
      if (save_last_batch_) {
        last_batch_.to_exprs(*eval_ctx_);
        save_last_batch_ = false;
      }
      batch_rows = &left_->get_brs();
      while (OB_SUCC(ret) && !batch_rows->end_) {
        if (!rescan_params_->empty()) {
          op_->set_pushdown_param_null(*rescan_params_);
        }
        if (OB_FAIL(left_->get_next_batch(max_row_cnt, batch_rows))) {
          LOG_WARN("get next batch from left failed", KR(ret));
        }
      }
      if (OB_SUCC(ret) && batch_rows->end_) {
        is_left_end_ = true;
      }
      if (OB_SUCC(ret)) {
        if (!(batch_rows->size_ == 0 && batch_rows->end_)) {
          last_batch_.from_exprs(*eval_ctx_, batch_rows->skip_, spec_->max_batch_size_);
          save_last_batch_ = true;
        }
        const_cast<ObBatchRows *&>(batch_rows)->end_ = false;
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::rescan_left()
{
  int ret = OB_SUCCESS;
  bool need_rescan = false;
  const ObChunkDatumStore::StoredRow *row = NULL;
  if (OB_SUCC(ret)) {
    if (is_multi_level_) {
      // for multi level group rescan, we only rescan left if
      // there is a new group of params for left child.
      // note that if op_ is a child of SPF, param store can be
      // filled with a new group before we finish processing the
      // current group. in that case, we need to discard the old
      // group and switch to the new batch.
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
      for (int64_t i = 0; !need_rescan && i < left_rescan_params_->count(); i++) {
        int64_t param_idx = left_rescan_params_->at(i).param_idx_;
        if (plan_ctx->get_param_store().at(param_idx).is_ext_sql_array()) {
          need_rescan = true;
        }
      }
    } else {
      need_rescan = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (need_rescan) {
      if (OB_FAIL(set_above_group_size())) {
        LOG_WARN("set above group size failed", KR(ret));
      } else if (OB_FAIL(left_->rescan())) {
        LOG_WARN("rescan left failed", KR(ret),
                 K(left_->get_spec().get_id()), K(left_->op_name()));
      } else {
        is_left_end_ = false;
        above_group_idx_for_expand_ = 0;
        above_group_idx_for_read_ = 0;
        reset_buffer_state();
      }
    } else {
      if (OB_FAIL(drain_left())) {
        LOG_WARN("drain left failed", KR(ret));
      } else {
        is_left_end_ = false;
        above_group_idx_for_read_++;
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::rescan_right()
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  if (skip_rescan_right_) {
    skip_rescan_right_ = false;
  } else {
    cur_group_idx_++;
    if (OB_FAIL(fill_cur_row_group_param())) {
      LOG_WARN("failed to fill cur row group param");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_cnt_; i++) {
      GroupParamBackupGuard guard(right_[i].get_exec_ctx().get_das_ctx());
      guard.bind_batch_rescan_params(cur_group_idx_, group_rescan_cnt_, &rescan_params_info_);
      int cur_ret = right_[i].rescan();
      if (OB_SUCC(cur_ret) || OB_ITER_END == cur_ret) {
        if (0 == i) {
          save_ret = cur_ret;
        }
        if (cur_ret != save_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rescan right children returned different codes", KR(ret),
                   KR(cur_ret), KR(save_ret), K(i), K(right_cnt_));
        }
      } else {
        ret = cur_ret;
        LOG_WARN("rescan right failed", KR(ret), K(i), K(right_cnt_));
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::fill_group_buffer()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObObjParam, 1> left_params_backup;
  common::ObSEArray<ObObjParam, 1> right_params_backup;
  if (!is_left_end_ && need_fill_group_buffer()) {
    if (OB_FAIL(backup_above_params(left_params_backup, right_params_backup))) {
      LOG_WARN("backup above params failed", KR(ret));
    } else if (OB_FAIL(init_group_params())) {
      LOG_WARN("init group params failed", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (save_last_row_) {
        if (OB_ISNULL(last_row_.get_store_row())) {
          ret = OB_NOT_INIT;
          LOG_WARN("store row is null", KR(ret),
                   K(save_last_row_), KP(last_row_.get_store_row()));
        } else if (OB_FAIL(last_row_.restore(left_->get_spec().output_, *eval_ctx_))) {
          LOG_WARN("restore last row failed", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      reset_buffer_state();
      group_rescan_cnt_++;
      if (OB_FAIL(last_row_.init(
              mem_context_->get_malloc_allocator(), left_->get_spec().output_.count()))) {
        LOG_WARN("failed to init right last row", KR(ret));
      }
    }
    bool ignore_end = false;
    while (OB_SUCC(ret) && !is_full()) {
      op_->clear_evaluated_flag();
      if (!rescan_params_->empty()) {
        op_->set_pushdown_param_null(*rescan_params_);
      }
      if (OB_FAIL(left_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next left row failed", KR(ret));
        } else {
          is_left_end_ = true;
          if (is_multi_level_ && ((above_group_idx_for_expand_ + 1) < above_group_size_)) {
            ret = OB_SUCCESS;
            if (OB_FAIL(get_next_left_iter())) {
              LOG_WARN("get next iter failed", KR(ret));
            }
          }
        }
      } else if (OB_FAIL(add_row_to_store())) {
        LOG_WARN("add row to store failed", KR(ret));
      } else if (OB_FAIL(prepare_rescan_params())) {
        LOG_WARN("prepare rescan params failed", KR(ret));
      } else if (OB_FAIL(deep_copy_dynamic_obj())) {
        LOG_WARN("deep copy dynamic obj failed", KR(ret));
      } else {
        ignore_end = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (!rescan_params_->empty()) {
        op_->set_pushdown_param_null(*rescan_params_);
      }
      if (OB_FAIL(last_row_.shadow_copy(left_->get_spec().output_, *eval_ctx_))) {
        LOG_WARN("shadow copy last left row failed", KR(ret));
      } else {
        save_last_row_ = true;
      }
    }
    if (OB_SUCC(ret) || (ignore_end && OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
      if (OB_FAIL(left_store_.finish_add_row(false))) {
        LOG_WARN("finish add row to row store failed", KR(ret));
      } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
        LOG_WARN("begin iterator for chunk row store failed", KR(ret));
      } else if (OB_FAIL(rescan_right())) {
        ret = (OB_ITER_END == ret) ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("rescan right failed", KR(ret));
      } else {
        skip_rescan_right_ = true;
      }
    }
    int save_ret = ret;
    ret = OB_SUCCESS;
    if (OB_FAIL(restore_above_params(left_params_backup,
                                     right_params_backup))) {
      LOG_WARN("restore above params failed", KR(ret), KR(save_ret));
    } else {
      ret = save_ret;
    }
  }
  return ret;
}

int ObGroupJoinBufffer::batch_fill_group_buffer(const int64_t max_row_cnt,
                                                const ObBatchRows *&batch_rows)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObObjParam, 1> left_params_backup;
  common::ObSEArray<ObObjParam, 1> right_params_backup;
  if (!is_left_end_ && need_fill_group_buffer()) {
    if (OB_FAIL(init_group_params())) {
      LOG_WARN("init group params failed", KR(ret));
    } else if (OB_FAIL(backup_above_params(left_params_backup, right_params_backup))) {
      LOG_WARN("backup above params failed", KR(ret));
    }

    // fill group join buffer of current op untill join buffer is full
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      if (save_last_batch_) {
        last_batch_.to_exprs(*eval_ctx_);
        save_last_batch_ = false;
      }
      batch_rows = &left_->get_brs();
      reset_buffer_state();
      group_rescan_cnt_++;
      while (OB_SUCC(ret) && !is_full() && !batch_rows->end_) {
        op_->clear_evaluated_flag();
        if (!rescan_params_->empty()) {
          op_->set_pushdown_param_null(*rescan_params_);
        }
        DASGroupScanMarkGuard mark_guard(ctx_->get_das_ctx(), true);
        if (OB_FAIL(left_->get_next_batch(max_row_cnt, batch_rows))) {
          LOG_WARN("get next batch from left failed", KR(ret));
        }
        for (int64_t l_idx = 0;  OB_SUCC(ret) && l_idx < batch_rows->size_; l_idx++) {
          if (batch_rows->skip_->exist(l_idx)) {
            // do nothing
          } else {
            batch_info_guard.set_batch_idx(l_idx);
            batch_info_guard.set_batch_size(batch_rows->size_);
            if (OB_FAIL(add_row_to_store())) {
              LOG_WARN("store left row failed", KR(ret));
            } else if (OB_FAIL(prepare_rescan_params())) {
              LOG_WARN("prepare rescan params failed", KR(ret));
            } else if (OB_FAIL(deep_copy_dynamic_obj())) {
              LOG_WARN("deep copy dynamic obj failed", KR(ret));
            }
          }
        }
        // rescan left op, switch to next iter of left child op
        if (OB_SUCC(ret) && batch_rows->end_) {
          is_left_end_ = true;
          if (is_multi_level_) {
            if ((above_group_idx_for_expand_ + 1) >= above_group_size_) {
              // wait for parent op to fill next group params
            } else if (FALSE_IT(const_cast<ObBatchRows *&>(batch_rows)->end_ = false)) {
            } else if (OB_FAIL(get_next_left_iter())) {
              LOG_WARN("get next iter failed", KR(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!rescan_params_->empty()) {
        op_->set_pushdown_param_null(*rescan_params_);
      }
      if (batch_rows->size_ == 0 && batch_rows->end_) {
        // do nothing
      } else {
        // if buffer is full ,but the last batch rows of left op is not added, save them to last batch
        last_batch_.from_exprs(*eval_ctx_, batch_rows->skip_, spec_->max_batch_size_);
        save_last_batch_ = true;
      }

      op_->clear_evaluated_flag();
      if (left_store_.get_row_cnt() <= 0) {
        // this could happen if we have skipped all rows
        ret = OB_ITER_END;
      } else if (OB_FAIL(left_store_.finish_add_row(false))) {
        LOG_WARN("finish add row to row store failed", KR(ret));
      } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
        LOG_WARN("begin iterator for chunk row store failed", KR(ret));
      } else if (OB_FAIL(rescan_right())) {
        ret = (OB_ITER_END == ret) ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("rescan right failed", KR(ret));
      } else {
        skip_rescan_right_ = true;
      }
    }
    int save_ret = ret;
    ret = OB_SUCCESS;
    if (OB_FAIL(restore_above_params(left_params_backup,
                                     right_params_backup))) {
      LOG_WARN("restore above params failed", KR(ret), KR(save_ret));
    } else {
      ret = save_ret;
    }
  }
  return ret;
}

int ObGroupJoinBufffer::get_next_row_from_store()
{
  int ret = OB_SUCCESS;
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    if (OB_UNLIKELY(left_store_read_ >= left_store_group_idx_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row read and group idx do not match", KR(ret),
               K(left_store_read_), K(left_store_group_idx_.count()));
    } else if (above_group_idx_for_read_ == left_store_group_idx_.at(left_store_read_)) {
      // we are still reading results for the current rescan param,
      // need to rescan right child
      if (OB_FAIL(left_store_iter_.get_next_row(left_->get_spec().output_, *eval_ctx_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from iter failed", KR(ret));
        }
      } else {
        left_store_read_++;
      }
    } else {
      // we have finished reading results for the current rescan param
      ret = OB_ITER_END;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObGroupJoinBufffer::get_next_batch_from_store(int64_t max_rows, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    if (OB_UNLIKELY(left_store_read_ >= left_store_group_idx_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row read and group idx do not match", KR(ret),
               K(left_store_read_), K(left_store_group_idx_.count()));
    } else {
      if (is_multi_level_) {
        // left_store_iter_      = [1, 2, 3, 4, 5, 6, 7, 8]
        // left_store_batch_idx_ = [0, 0, 0, 0, 1, 1, 1, 1]
        // left_store_read_      = 0
        // max_rows = 5
        //
        // As shown above, get next batch from left_store_iter_ with max_rows=5
        // can return rows 1 ~ 5 where row 5 does not belong to current rescan.
        // Thus, we need to check and rewrite max_rows to 4 in this case.
        int64_t tmp_max_rows = 0;
        if (above_group_idx_for_read_ == left_store_group_idx_.at(left_store_read_)) {
          for (int64_t i = left_store_read_; i < left_store_group_idx_.count(); i++) {
            if (tmp_max_rows >= max_rows) {
              break;
            } else if (above_group_idx_for_read_ != left_store_group_idx_.at(i)) {
              break;
            } else {
              tmp_max_rows++;
            }
          }
        }
        max_rows = tmp_max_rows;
      }
      if (max_rows > 0) {
        // since we do not know how many rows we got last time, we need to save all possible
        // batch datums and restore them before getting next batch from left child
        last_batch_.extend_save(*eval_ctx_, spec_->max_batch_size_);
        // we are still reading results for the current rescan param,
        // need to rescan right child
        if (OB_FAIL(left_store_iter_.get_next_batch(left_->get_spec().output_,
                                                    *eval_ctx_,
                                                    max_rows,
                                                    read_rows))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next batch from iter failed", KR(ret));
          }
        } else {
          left_store_read_ += read_rows;
        }
      } else {
        ret = OB_ITER_END;
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObGroupJoinBufffer::destroy()
{
  left_store_iter_.reset();
  left_store_.reset();
  left_store_group_idx_.destroy();
  above_left_group_params_.destroy();
  above_right_group_params_.destroy();
  last_row_.reset();
  last_batch_.reset();
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

int ObGroupJoinBufffer::init_group_params()
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
  if (OB_FAIL(ret) || 0 == right_rescan_params_->count()) {
    // do nothing
  } else if (!above_group_params_.empty()) {
    for (int64_t i = 0; i < above_group_params_.count(); i++) {
      above_group_params_.at(i).count_ = 0;
    }
  } else if (OB_FAIL(above_group_params_.allocate_array(
          ctx_->get_allocator(), right_rescan_params_->count()))) {
    LOG_WARN("allocate above group params array failed", KR(ret),
             K(right_rescan_params_->count()));
  } else {
    int64_t obj_buf_size = sizeof(ObObjParam) * max_group_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < above_group_params_.count(); ++i) {
      ObExpr *dst_expr = right_rescan_params_->at(i).dst_;
      void *buf = ctx_->get_allocator().alloc(obj_buf_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", KR(ret), K(obj_buf_size));
      } else {
        above_group_params_.at(i).data_ = reinterpret_cast<ObObjParam*>(buf);
        above_group_params_.at(i).count_ = 0;
        above_group_params_.at(i).element_.set_meta_type(dst_expr->obj_meta_);
      }
    }
  }

  // collect batch nlj params needed by rescan right op
  if (OB_FAIL(ret) || (group_params_.empty())) {
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

int ObGroupJoinBufffer::deep_copy_dynamic_obj()
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
    if (OB_FAIL(ob_write_obj(mem_context_->get_arena_allocator(),
                             param_store.at(param_idx),
                             group_params_.at(i).data_[group_params_.at(i).count_]))) {
      LOG_WARN("deep copy dynamic param failed", KR(ret), K(i), K(param_idx));
    } else {
      group_params_.at(i).count_++;
    }
  }
  if (OB_FAIL(ret) || !is_multi_level_) {
    // do nothing
  } else {
    if (OB_UNLIKELY(above_group_params_.count() != above_right_group_params_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected group params count", KR(ret),
               K(above_group_params_.count()),
               K(above_right_group_params_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < above_group_params_.count(); i++) {
      ObSqlArrayObj *arr = above_right_group_params_.at(i);
      if (NULL == arr) {
        // skip
      } else if (OB_FAIL(ob_write_obj(
              mem_context_->get_arena_allocator(),
              arr->data_[above_group_idx_for_expand_],
              above_group_params_.at(i).data_[above_group_params_.at(i).count_]))) {
        LOG_WARN("deep copy dynamic param failed", KR(ret), K(i), K(above_group_idx_for_expand_));
      } else {
        ++above_group_params_.at(i).count_;
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::prepare_rescan_params()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rescan_params_->count(); i++) {
    if (OB_FAIL(rescan_params_->at(i).set_dynamic_param(*eval_ctx_))) {
      LOG_WARN("set dynamic param failed", KR(ret));
    }
  }
  return ret;
}

int ObGroupJoinBufffer::add_row_to_store()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, eval_ctx_))) {
    LOG_WARN("add row failed", KR(ret));
  } else if (OB_FAIL(left_store_group_idx_.push_back(above_group_idx_for_expand_))) {
    LOG_WARN("add index failed", KR(ret));
  }
  return ret;
}

int ObGroupJoinBufffer::build_above_group_params(
        const common::ObIArray<ObDynamicParamSetter> &above_rescan_params,
        common::ObIArray<ObSqlArrayObj *> &above_group_params,
        int64_t &group_size)
{
  int ret = OB_SUCCESS;
  group_size = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
  const GroupParamArray* group_params_above = nullptr;
  if (OB_ISNULL(group_params_above = ctx_->get_das_ctx().get_group_params())) {
    // the above operator of this nlj don't use batch rescan, do nothing
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is nullptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < above_rescan_params.count(); i++) {
      int64_t param_idx = above_rescan_params.at(i).param_idx_;
      ObSqlArrayObj *array_obj = NULL;
      uint64_t array_idx = OB_INVALID_ID;
      bool exist = false;
      if (OB_FAIL(ctx_->get_das_ctx().find_group_param_by_param_idx(param_idx, exist, array_idx))) {
        LOG_WARN("failed to find group param by param idx", K(ret), K(i), K(param_idx));
      } else if (!exist || array_idx == OB_INVALID_ID || array_idx > group_params_above->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find group param", K(ret), K(exist), K(i), K(array_idx));
      } else {
        const GroupRescanParam &group_param = group_params_above->at(array_idx);
        array_obj = group_param.gr_param_;
        if (0 == group_size) {
          group_size = array_obj->count_;
        } else if (OB_UNLIKELY(group_size != array_obj->count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group sizes do not match", KR(ret),
                  K(group_size), K(array_obj->count_));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(above_group_params.push_back(array_obj))) {
        LOG_WARN("push array obj failed", KR(ret), K(i), KP(array_obj));
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::set_above_group_size() {
  int ret = OB_SUCCESS;
  if (is_multi_level_) {
    above_group_size_ = 0;
    for (int i = 0; i < above_left_group_params_.count(); i++) {
      if (NULL == above_left_group_params_.at(i)) {
        // skip
      } else {
        above_group_size_ = above_left_group_params_.at(i)->count_;
        break;
      }
    }
    if (0 == above_group_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("above group size is invalid", KR(ret), K(above_group_size_));
    }
  }
  return ret;
}

void ObGroupJoinBufffer::reset_buffer_state()
{
  cur_group_idx_ = -1;
  left_store_read_ = 0;
  left_store_iter_.reset();
  left_store_.reset();
  left_store_read_ = 0;
  left_store_group_idx_.reuse();
  last_row_.reset();
  last_batch_.clear_saved_size();
  save_last_row_ = false;
  mem_context_->get_arena_allocator().reset();
}

int ObGroupJoinBufffer::backup_above_params(common::ObIArray<ObObjParam> &left_params_backup,
                                            common::ObIArray<ObObjParam> &right_params_backup)
{
  int ret = OB_SUCCESS;
  if (is_multi_level_) {
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
    left_params_backup.reuse();
    right_params_backup.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rescan_params_->count(); i++) {
      int64_t param_idx = left_rescan_params_->at(i).param_idx_;
      const ObObjParam &obj_param = plan_ctx->get_param_store().at(param_idx);
      if (OB_FAIL(left_params_backup.push_back(obj_param))) {
        LOG_WARN("push obj param failed", KR(ret), K(param_idx), K(obj_param));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_rescan_params_->count(); i++) {
      int64_t param_idx = right_rescan_params_->at(i).param_idx_;
      const ObObjParam &obj_param = plan_ctx->get_param_store().at(param_idx);
      if (OB_FAIL(right_params_backup.push_back(obj_param))) {
        LOG_WARN("push obj param failed", KR(ret), K(param_idx), K(obj_param));
      }
    }
  }
  return ret;
}

int ObGroupJoinBufffer::restore_above_params(common::ObIArray<ObObjParam> &left_params_backup,
                                             common::ObIArray<ObObjParam> &right_params_backup)
{
  int ret = OB_SUCCESS;
  if (is_multi_level_) {
    if (OB_UNLIKELY(left_rescan_params_->count() != left_params_backup.count()
                    || right_rescan_params_->count() != right_params_backup.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup params count do not match", KR(ret),
               K(left_rescan_params_->count()), K(left_params_backup.count()),
               K(right_rescan_params_->count()), K(right_params_backup.count()));
    }
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rescan_params_->count(); i++) {
      int64_t param_idx = left_rescan_params_->at(i).param_idx_;
      plan_ctx->get_param_store_for_update().at(param_idx) = left_params_backup.at(i);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_rescan_params_->count(); i++) {
      int64_t param_idx = right_rescan_params_->at(i).param_idx_;
      plan_ctx->get_param_store_for_update().at(param_idx) = right_params_backup.at(i);
    }
  }
  return ret;
}

int ObGroupJoinBufffer::get_next_batch_from_right(int64_t max_batch_size, const ObBatchRows *brs)
{
  int ret = OB_SUCCESS;
  if (right_cnt_ == 1) {
    GroupParamBackupGuard guard(right_[0].get_exec_ctx().get_das_ctx());
    guard.bind_batch_rescan_params(cur_group_idx_, group_rescan_cnt_, &rescan_params_info_);
    if (OB_FAIL(right_[0].get_next_batch(max_batch_size, brs))) {
      LOG_WARN("failed to get next batch from right op in batch NLJ", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the right child cnt of NLJ is not 1", K(ret));
  }
  return ret;
}

int ObGroupJoinBufffer::get_next_row_from_right()
{
  int ret = OB_SUCCESS;
  if (right_cnt_ == 1) {
    GroupParamBackupGuard guard(right_[0].get_exec_ctx().get_das_ctx());
    guard.bind_batch_rescan_params(cur_group_idx_, group_rescan_cnt_, &rescan_params_info_);
    if (OB_FAIL(right_[0].get_next_row())) {
      LOG_WARN("failed to get next row from right op in batch NLJ", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the right child cnt of NLJ is not 1", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
