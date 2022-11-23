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

#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObNestedLoopJoinSpec, ObBasicNestedLoopJoinSpec),
                    use_group_, left_group_size_, left_expr_ids_in_other_cond_);

ObNestedLoopJoinOp::ObNestedLoopJoinOp(ObExecContext &exec_ctx,
                                       const ObOpSpec &spec,
                                       ObOpInput *input)
  : ObBasicNestedLoopJoinOp(exec_ctx, spec, input),
    state_(JS_READ_LEFT), mem_context_(nullptr), is_left_end_(false),
    last_store_row_(), save_last_row_(false), defered_right_rescan_(false),
    batch_rescan_ctl_(),
    batch_state_(JS_FILL_LEFT), save_last_batch_(false),
    batch_mem_ctx_(NULL), stored_rows_(NULL), left_brs_(NULL), left_matched_(NULL),
    need_switch_iter_(false), iter_end_(false), op_max_batch_size_(0),
    max_group_size_(BNLJ_DEFAULT_GROUP_SIZE),
    bnlj_cur_idx_(0)
{
  state_operation_func_[JS_JOIN_END] = &ObNestedLoopJoinOp::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObNestedLoopJoinOp::join_end_func_end;

  state_operation_func_[JS_READ_LEFT] = &ObNestedLoopJoinOp::read_left_operate;
  state_function_func_[JS_READ_LEFT][FT_ITER_GOING] = &ObNestedLoopJoinOp::read_left_func_going;
  state_function_func_[JS_READ_LEFT][FT_ITER_END] = &ObNestedLoopJoinOp::read_left_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObNestedLoopJoinOp::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObNestedLoopJoinOp::read_right_func_going;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObNestedLoopJoinOp::read_right_func_end;
}

int ObNestedLoopJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nlp_op child is null", KP(left_), KP(right_), K(ret));
  } else if (OB_FAIL(ObBasicNestedLoopJoinOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  }
  if (OB_SUCC(ret) && is_vectorized()) {
    if (MY_SPEC.use_group_) {
      max_group_size_ = BNLJ_DEFAULT_GROUP_SIZE + MY_SPEC.plan_->get_batch_size();
    }
    if (OB_ISNULL(batch_mem_ctx_)) {
      ObSQLSessionInfo *session = ctx_.get_my_session();
      uint64_t tenant_id =session->get_effective_tenant_id();
      lib::ContextParam param;
      const int64_t mem_limit = 8 * 1024 * 1024; //8M;
      param.set_mem_attr(tenant_id,
                         ObModIds::OB_SQL_NLJ_CACHE,
                         ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(batch_mem_ctx_, param))) {
        LOG_WARN("create entity failed", K(ret));
      } else if (OB_ISNULL(batch_mem_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null memory entity returned", K(ret));
      } else if (OB_FAIL(right_store_.init(mem_limit, tenant_id, ObCtxIds::WORK_AREA,
              common::ObModIds::OB_SQL_NLJ_CACHE, true, sizeof(int64_t)))) {
        LOG_WARN("init row store failed", K(ret));
      } else if (OB_FAIL(right_store_.alloc_dir_id())) {
        LOG_WARN("failed to alloc dir id", K(ret));
      } else {
        right_store_.set_allocator(batch_mem_ctx_->get_malloc_allocator());
        right_store_.set_io_event_observer(&io_event_observer_);
      }
    }
    if (OB_SUCC(ret)) {
      stored_rows_ = static_cast<ObChunkDatumStore::StoredRow **>(batch_mem_ctx_
                     ->get_arena_allocator().alloc(
                       sizeof(ObChunkDatumStore::StoredRow *) * MY_SPEC.max_batch_size_));
      if (OB_ISNULL(stored_rows_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      char *buf = (char *)batch_mem_ctx_->get_arena_allocator()
                          .alloc(ObBitVector::memory_size(MY_SPEC.max_batch_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc", K(ret));
      } else {
        MEMSET(buf, 0, ObBitVector::memory_size(MY_SPEC.max_batch_size_));
        left_matched_ = to_bit_vector(buf);
      }
    }
    if (OB_SUCC(ret)) {
      // if join type is left semi or anti join, we don't have to get batch from right_store_ when output.
      // That means datums of exprs in right output_ won't be overwrited.
      if (!IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)
          && OB_FAIL(brs_holder_.init(right_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("fail to init brs_holder_", K(ret));
      } else if (OB_FAIL(left_batch_.init(&(left_->get_spec().output_),
                                   &(batch_mem_ctx_->get_arena_allocator()),
                                   MY_SPEC.max_batch_size_))) {
        LOG_WARN("fail to init batch", K(ret));
      } else if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
        if (OB_FAIL(last_save_batch_.init(&left_->get_spec().output_,
                                     &batch_mem_ctx_->get_arena_allocator(),
                                     MY_SPEC.max_batch_size_))) {
          LOG_WARN("fail to init batch", K(ret));
        }
      }
    }
  }
  return ret;
}
//NLJ has its own switch_iterator
int ObNestedLoopJoinOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to inner switch iterator", K(ret));
  } else if (OB_FAIL(left_->switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch left child iterator failed", K(ret));
    }
  } else {
    reset_buf_state();
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObNestedLoopJoinOp::rescan()
{
  int ret = OB_SUCCESS;
  //NLJ's rescan should only drive left child's rescan,
  //the right child's rescan is defer to rescan_right_operator() driven by get_next_row();
  defered_right_rescan_ = true;
  if (OB_FAIL(left_->rescan())) {
    LOG_WARN("rescan left child operator failed", K(ret), "child op_type", left_->op_name());
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_rescan())) {
      LOG_WARN("failed to inner rescan", K(ret));
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObNestedLoopJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset_buf_state();
  set_param_null();
  if (OB_FAIL(ObBasicNestedLoopJoinOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }

  return ret;
}

int ObNestedLoopJoinOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_)) {
    if (OB_FAIL(join_row_with_semi_join())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to join row with semi join", K(ret));
      }
    }
  } else {
    state_operation_func_type state_operation = NULL;
    state_function_func_type state_function = NULL;
    int func = -1;
    output_row_produced_  = false;
    while (OB_SUCC(ret) && !output_row_produced_) {
      state_operation = this->ObNestedLoopJoinOp::state_operation_func_[state_];
      if (OB_ITER_END == (ret = (this->*state_operation)())) {
        func = FT_ITER_END;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed state operation", K(ret), K(state_));
      } else {
        func = FT_ITER_GOING;
      }
      if (OB_SUCC(ret)) {
        state_function = this->ObNestedLoopJoinOp::state_function_func_[state_][func];
        if (OB_FAIL((this->*state_function)()) && OB_ITER_END != ret) {
          LOG_WARN("failed state function", K(ret), K(state_), K(func));
        }
      }
    } // while end
  }
  if (OB_ITER_END == ret) {
    set_param_null();
  }

  return ret;
}

void ObNestedLoopJoinOp::reset_buf_state()
{
  state_ = JS_READ_LEFT;
  left_store_iter_.reset();
  left_store_.reset();
  is_left_end_ = false;
  last_store_row_.reset();
  save_last_row_ = false;
  batch_rescan_ctl_.reset();
  batch_state_ = JS_FILL_LEFT;
  save_last_batch_ = false;
  need_switch_iter_ = false;
  iter_end_ = false;
  left_batch_.saved_size_ = 0;
  last_save_batch_.saved_size_ = 0;
}

int ObNestedLoopJoinOp::fill_cur_row_rescan_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else if (batch_rescan_ctl_.cur_idx_ >= batch_rescan_ctl_.params_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx is unexpected", K(ret),
             K(batch_rescan_ctl_.cur_idx_), K(batch_rescan_ctl_.params_.get_count()));
  } else {
    common::ObIArray<common::ObObjParam>& params =
        batch_rescan_ctl_.params_.get_one_batch_params(batch_rescan_ctl_.cur_idx_);
    int64_t param_cnt = params.count();
    int64_t idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      idx = batch_rescan_ctl_.params_.get_param_idx(i);
      plan_ctx->get_param_store_for_update().at(idx) = params.at(i);
      const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
      if (OB_FAIL(rescan_param.set_dynamic_param(eval_ctx_))) {
        LOG_WARN("fail to set dynamic param", K(ret));
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::fill_cur_row_bnlj_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (bnlj_params_.empty() || bnlj_cur_idx_ >= bnlj_params_.at(0).count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx is unexpected", K(ret),
             K(bnlj_cur_idx_), K(bnlj_params_.at(0).count_));
  } else {
    int64_t param_cnt = bnlj_params_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
      int64_t param_idx = rescan_param.param_idx_;
      ObExpr *dst = rescan_param.dst_;
      ObDatum &param_datum = dst->locate_datum_for_write(eval_ctx_);
      ObSqlArrayObj &arr = bnlj_params_.at(i);
      if (OB_FAIL(param_datum.from_obj(arr.data_[bnlj_cur_idx_], dst->obj_datum_map_))) {
        LOG_WARN("fail to cast datum", K(ret));
      } else {
        plan_ctx->get_param_store_for_update().at(param_idx) = arr.data_[bnlj_cur_idx_];
        dst->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::join_row_with_semi_join()
{
  int ret = OB_SUCCESS;
  const bool is_anti = (LEFT_ANTI_JOIN == MY_SPEC.join_type_);
  while (OB_SUCC(ret) && OB_SUCC(get_next_left_row())) {
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("prepare right child rescan param failed", K(ret));
    } else if (OB_FAIL(rescan_right_operator())) {
      LOG_WARN("rescan right child failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      // 迭代右表
      bool is_matched = false;
      while (OB_SUCC(ret)
             && !is_matched
             && OB_SUCC(right_->get_next_row())) {
        clear_evaluated_flag();
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(calc_other_conds(is_matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS; // 右表不存在和左表匹配的行，所以迭代左表下一行
      }
      if (OB_SUCC(ret) && is_anti != is_matched) {
        // 1. the exit conditions of semi and anti semi are different,
        // 2. they share the same outer while loop,
        // 3. is_matched must init to false for inner while loop,
        // so we need explicitly break.
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::join_end_func_end()
{
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::read_left_operate()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
    if (OB_FAIL(group_read_left_operate()) && OB_ITER_END != ret) {
      LOG_WARN("failed to read left group", K(ret));
    }
  } else if (FALSE_IT(set_param_null())) {
  } else if (OB_FAIL(get_next_left_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }

  return ret;
}


int ObNestedLoopJoinOp::rescan_params_batch_one(int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  left_batch_.to_exprs(eval_ctx_, batch_idx, batch_idx);
  if (OB_FAIL(prepare_rescan_params())) {
    LOG_WARN("failed to prepare rescan params", K(ret));
  } else if (OB_FAIL(rescan_right_operator())) {
    LOG_WARN("failed to rescan right op", K(ret));
  }

  return ret;
}

int ObNestedLoopJoinOp::rescan_right_operator()
{
  int ret = OB_SUCCESS;
  bool do_rescan = false;
  if (defered_right_rescan_) {
    do_rescan = true;
    defered_right_rescan_ = false;
  } else {
    // FIXME bin.lb: handle monitor dump + material ?
    if (PHY_MATERIAL == right_->get_spec().type_) {
      if (OB_FAIL(static_cast<ObMaterialOp*>(right_)->rewind())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("rewind failed", K(ret));
        }
      }
    } else {
      do_rescan = true;
    }
  }
  if (OB_SUCC(ret) && do_rescan) {
    if (OB_FAIL(right_->rescan())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("rescan right failed", K(ret));
      }
    } else {
      brs_holder_.reset();
      if (MY_SPEC.use_group_ && !MY_SPEC.enable_px_batch_rescan_) {
        if (OB_FAIL(fill_cur_row_bnlj_param())) {
          LOG_WARN("fill bnlj param failed", K(ret));
        } else {
          bnlj_cur_idx_++;
        }
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::init_bnlj_params()
{
  int ret = OB_SUCCESS;
  if (!bnlj_params_.empty()) {
    //to reuse bnlj param buffer
    for (int64_t i = 0; i < bnlj_params_.count(); ++i) {
      bnlj_params_.at(i).count_ = 0;
    }
  } else if (OB_FAIL(bnlj_params_.allocate_array(ctx_.get_allocator(),
                                                 get_spec().rescan_params_.count()))) {
    LOG_WARN("allocate bnlj params failed", K(ret), K(get_spec().rescan_params_.count()));
  } else {
    int64_t obj_buf_size = sizeof(ObObjParam) * max_group_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < bnlj_params_.count(); ++i) {
      ObExpr *dst_expr = get_spec().rescan_params_.at(i).dst_;
      void *buf = ctx_.get_allocator().alloc(obj_buf_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(obj_buf_size));
      } else {
        bnlj_params_.at(i).data_ = reinterpret_cast<ObObjParam*>(buf);
        bnlj_params_.at(i).count_ = 0;
        bnlj_params_.at(i).element_.set_meta_type(dst_expr->obj_meta_);
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::bind_bnlj_param_to_store()
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = get_spec().rescan_params_.count();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ParamStore &param_store = plan_ctx->get_param_store_for_update();
  if (OB_UNLIKELY(param_cnt != bnlj_params_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bnlj param count is invalid", K(ret), K(param_cnt), K(bnlj_params_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
    int64_t param_idx = rescan_param.param_idx_;
    int64_t array_obj_addr = reinterpret_cast<int64_t>(&bnlj_params_.at(i));
    param_store.at(param_idx).set_extend(array_obj_addr, T_EXT_SQL_ARRAY);
  }
  return ret;
}

int ObNestedLoopJoinOp::group_read_left_operate()
{
  int ret = OB_SUCCESS;
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    // 重新设置右表 table scan result, result 为下一个 cache
    if (MY_SPEC.enable_px_batch_rescan_) {
      batch_rescan_ctl_.cur_idx_++;
    } else if (OB_FAIL(rescan_right_operator())) {
      // 这里是不期望有 OB_ITER_END
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to get next right row from group", K(ret));
    }
  } else {
    // 当前 row 对应的 cache 读完了, 左表拿新 cache, 设置右表 result_iter
    if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(init_bnlj_params())) {
      LOG_WARN("Failed to init group rescan", K(ret));
    } else if (is_left_end_) {
      ret = OB_ITER_END;
    } else {
      if (OB_ISNULL(mem_context_)) {
        ObSQLSessionInfo *session = ctx_.get_my_session();
        uint64_t tenant_id =session->get_effective_tenant_id();
        lib::ContextParam param;
        param.set_mem_attr(tenant_id,
                           ObModIds::OB_SQL_NLJ_CACHE,
                           ObCtxIds::WORK_AREA)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL);
        if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
          LOG_WARN("create entity failed", K(ret));
        } else if (OB_ISNULL(mem_context_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null memory entity returned", K(ret));
        } else if (OB_FAIL(left_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
          LOG_WARN("init row store failed", K(ret));
        } else {
          left_store_.set_allocator(mem_context_->get_malloc_allocator());
        }
      }

      bool ignore_end = false;
      if (OB_SUCC(ret)) {
        // 没有下一个了, 尝试填充 cache.
        batch_rescan_ctl_.reuse();
        bnlj_cur_idx_ = 0;
        left_store_iter_.reset();
        left_store_.reset();
        mem_context_->get_arena_allocator().reset();
        if (OB_ISNULL(last_store_row_.get_store_row())) {
          if (save_last_row_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: store row is null", K(ret));
          } else if (OB_FAIL(last_store_row_.init(
              mem_context_->get_malloc_allocator(), left_->get_spec().output_.count()))) {
            LOG_WARN("failed to init right last row", K(ret));
          }
        } else if (save_last_row_) {
          if (OB_FAIL(last_store_row_.restore(left_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("failed to restore left row", K(ret));
          }
        }
        save_last_row_ = false;
        set_param_null();
        while (OB_SUCC(ret) && !is_full()) {
          // need clear evaluated flag, since prepare_rescan_params() will evaluate expression.
          clear_evaluated_flag();
          if (OB_FAIL(get_next_left_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next left row", K(ret));
            } else {
              is_left_end_ = true;
            }
          } else if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, &eval_ctx_))) {
            LOG_WARN("failed to store left row", K(ret));
            // do nothing
          } else if (OB_FAIL(prepare_rescan_params(true/*is_group*/))) {
            LOG_WARN("failed to prepare rescan params", K(ret));
          // 下压参数数据是由被换的原始表达式计算生成, 比如c1 = c2 + 1--> c1 = ?;
          // 下压参数?的值, 由c2+1计算而来, c2+1的内存是复用的, 如果此时不深拷贝
          // 计算query range的下压param, 则可能导致后面query range的结果和
          // 前面query range的obobj对应的ptr(string/number类型在obj中ptr)使用相同指针;
          } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(deep_copy_dynamic_obj())) {
            LOG_WARN("fail to deep copy dynamic obj", K(ret));
          } else {
            ignore_end = true;
          }
        }
        if (OB_SUCC(ret)) {
          // here need to set param null, because dynamic datum ptr
          // which from last batch row may invalid
          set_param_null();
          if (OB_FAIL(last_store_row_.shadow_copy(left_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("failed to shadow copy last left row", K(ret));
          } else {
            save_last_row_ = true;
          }
        }
      }

      if (OB_SUCC(ret) || (ignore_end && OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
        if (OB_FAIL(left_store_.finish_add_row(false))) {
          LOG_WARN("failed to finish add row to row store", K(ret));
        } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(bind_bnlj_param_to_store())) {
          LOG_WARN("bind bnlj param to store failed", K(ret));
        } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(rescan_right_operator())) {
          LOG_WARN("failed to rescan right op", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 拿到下一行 ret = OB_SUCCESS;
    clear_evaluated_flag();
    if (OB_FAIL(left_store_iter_.get_next_row(left_->get_spec().output_,
                                              eval_ctx_))) {
      LOG_WARN("Failed to get next row", K(ret));
    } else if (MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(fill_cur_row_rescan_param())) {
      LOG_WARN("fail to fill cur row rescan param", K(ret));
    } else if (MY_SPEC.enable_px_batch_rescan_) {
      OZ(right_->rescan());
      OX(brs_holder_.reset());
    }
    if (OB_SUCC(ret)) {
      left_row_joined_ = false;
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::deep_copy_dynamic_obj()
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = get_spec().rescan_params_.count();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ParamStore &param_store = plan_ctx->get_param_store_for_update();
  if (OB_ISNULL(mem_context_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem entity not init", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
    int64_t param_idx = rescan_param.param_idx_;
    if (OB_FAIL(ob_write_obj(mem_context_->get_arena_allocator(),
                             param_store.at(param_idx),
                             bnlj_params_.at(i).data_[bnlj_params_.at(i).count_]))) {
      LOG_WARN("deep copy dynamic param", K(ret));
    } else {
      ++bnlj_params_.at(i).count_;
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::read_left_func_going()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
    // do nothing
    // group nested loop join 已经做过 rescan 了
  } else if (OB_FAIL(prepare_rescan_params())) {
    LOG_WARN("failed to prepare rescan params", K(ret));
  } else if (OB_FAIL(rescan_right_operator())) {
    LOG_WARN("rescan right operator failed", K(ret));
  }
  state_ = JS_READ_RIGHT;

  return ret;
}

int ObNestedLoopJoinOp::read_left_func_end()
{
  state_ = JS_JOIN_END;
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_right_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  } else {
    clear_evaluated_flag();
  }

  return ret;
}

int ObNestedLoopJoinOp::read_right_func_going()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_other_conds(is_match))) {
      LOG_WARN("failed to compare left and right row on other join conds", K(ret));
    } else if (is_match) {
      output_row_produced_ = true;
      left_row_joined_ = true;     // left row joined sign.
    } else {}
  }

  return ret;
}

int ObNestedLoopJoinOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (need_left_join() && !left_row_joined_) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_row(right_->get_spec().output_))) {
      LOG_WARN("failed to blank right row", K(ret));
    }
  }
  state_ = JS_READ_LEFT;

  return ret;
}

bool ObNestedLoopJoinOp::is_full() const
{
  return left_store_.get_row_cnt() >= MY_SPEC.left_group_size_;
}

int ObNestedLoopJoinOp::get_left_batch()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
    if (OB_FAIL(group_get_left_batch(left_brs_)) && OB_ITER_END != ret) {
      LOG_WARN("fail to get left batch", K(ret));
    }
  } else {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    set_param_null();
    if (is_left_end_) {
      // do nothing
      ret = OB_ITER_END;
    } else if (!IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_) // semi/anti has to_exprs in out process
               && FALSE_IT(left_batch_.to_exprs(eval_ctx_))) {
    } else if (OB_FAIL(left_->get_next_batch(op_max_batch_size_, left_brs_))) {
      LOG_WARN("fail to get next batch", K(ret));
    } else if (left_brs_->end_) {
      is_left_end_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (left_brs_->end_ && left_brs_->size_ == 0) {
    ret = OB_ITER_END;
  } else {
    left_batch_.from_exprs(eval_ctx_, left_brs_->skip_, left_brs_->size_);
  }

  return ret;
}

int ObNestedLoopJoinOp::group_get_left_batch(const ObBatchRows *&left_brs)
{
  int ret = OB_SUCCESS;
  left_brs = &left_->get_brs();
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    // do nothing
  } else {
    if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(init_bnlj_params())) {
      LOG_WARN("Failed to init bnlj params", K(ret));
    } else if (is_left_end_) {
      ret = OB_ITER_END;
      // do nothing
    } else {
      if (OB_ISNULL(mem_context_)) {
        ObSQLSessionInfo *session = ctx_.get_my_session();
        uint64_t tenant_id =session->get_effective_tenant_id();
        lib::ContextParam param;
        param.set_mem_attr(tenant_id,
                           ObModIds::OB_SQL_NLJ_CACHE,
                           ObCtxIds::WORK_AREA)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL);
        if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
          LOG_WARN("create entity failed", K(ret));
        } else if (OB_ISNULL(mem_context_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null memory entity returned", K(ret));
        } else if (OB_FAIL(left_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
          LOG_WARN("init row store failed", K(ret));
        } else {
          left_store_.set_allocator(mem_context_->get_malloc_allocator());
        }
      }
      if (OB_SUCC(ret)) {
        // 没有下一个了, 尝试填充 cache.
        batch_rescan_ctl_.reuse();
        bnlj_cur_idx_ = 0;
        left_store_iter_.reset();
        left_store_.reset();
        mem_context_->get_arena_allocator().reset();
        save_last_row_ = false;
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
        while (OB_SUCC(ret) && continue_fetching()) {
          // need clear evaluated flag, since prepare_rescan_params() will evaluate expression.
          clear_evaluated_flag();
          if (save_last_batch_) {
            last_save_batch_.to_exprs(eval_ctx_);
            save_last_batch_ = false;
          }
          set_param_null();
          if (OB_FAIL(left_->get_next_batch(op_max_batch_size_, left_brs_))) {
            LOG_WARN("failed to get next left row", K(ret));
          } else if (left_brs_->end_) {
            is_left_end_ = true;
          }
          for (int64_t l_idx = 0;  OB_SUCC(ret) && l_idx < left_brs_->size_; l_idx++) {
            if (left_brs_->skip_->exist(l_idx)) { continue; }
            batch_info_guard.set_batch_idx(l_idx);
            batch_info_guard.set_batch_size(left_brs_->size_);
            if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, &eval_ctx_))) {
              LOG_WARN("failed to store left row", K(ret));
              // do nothing
            } else if (OB_FAIL(prepare_rescan_params(true))) {
              LOG_WARN("failed to prepare rescan params", K(ret));
            // 下压参数数据是由被换的原始表达式计算生成, 比如c1 = c2 + 1--> c1 = ?;
            // 下压参数?的值, 由c2+1计算而来, c2+1的内存是复用的, 如果此时不深拷贝
            // 计算query range的下压param, 则可能导致后面query range的结果和
            // 前面query range的obobj对应的ptr(string/number类型在obj中ptr)使用相同指针;
            } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(deep_copy_dynamic_obj())) {
              LOG_WARN("fail to deep copy dynamic obj", K(ret));
            }
          } // for end
        }
        if (OB_SUCC(ret)) {
          set_param_null();
          if (left_brs_->size_ == 0 && left_brs_->end_) {
            // do nothing
          } else {
            last_save_batch_.from_exprs(eval_ctx_, left_brs_->skip_, left_brs_->size_);
            save_last_batch_ = true;
          }
        }
        clear_evaluated_flag();
      }
      if (OB_SUCC(ret) ) {
        if (left_store_.get_row_cnt() <= 0) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(left_store_.finish_add_row(false))) {
          LOG_WARN("failed to finish add row to row store", K(ret));
        } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(bind_bnlj_param_to_store())) {
          LOG_WARN("bind bnlj param to store failed", K(ret));
        } else if (!MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(rescan_right_operator())) {
          LOG_WARN("failed to rescan right op", K(ret));
        } else {
          need_switch_iter_ = false;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t read_size = 0;
    int64_t max_size = MY_SPEC.max_batch_size_;
    last_save_batch_.extend_save(eval_ctx_, max_size);
    if (OB_FAIL(left_store_iter_.get_next_batch(left_->get_spec().output_,
                                                eval_ctx_,
                                                max_size,
                                                read_size))) {
      if (OB_ITER_END == ret) {
        // do nothing
      } else {
        LOG_WARN("Failed to get next row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const_cast<ObBatchRows *>(left_brs)->skip_->reset(read_size);
      const_cast<ObBatchRows *>(left_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(left_brs)->end_ = false;
      left_row_joined_ = false;
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::process_left_batch()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(left_brs_->size_);
  for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < left_brs_->size_; l_idx++) {
    if (!MY_SPEC.use_group_ && !MY_SPEC.enable_px_batch_rescan_) {
      batch_info_guard.set_batch_idx(l_idx);
      if (left_brs_->skip_->exist(l_idx)) { continue; }
      if (OB_FAIL(rescan_params_batch_one(l_idx))) {
        LOG_WARN("fail to rescan params", K(ret));
      }
    } else if (MY_SPEC.use_group_ && !MY_SPEC.enable_px_batch_rescan_) {
      // after group rescan, first left row not need switch iter
      if (!need_switch_iter_) {
        need_switch_iter_ = true;
      } else if (OB_FAIL(rescan_right_operator())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("fail to switch iterator", K(ret));
      }
    } else if (MY_SPEC.enable_px_batch_rescan_) {
      // NOTE: left batch is ALWAYS continous, NO need to check skip for
      // left_brs under px batch rescan
      batch_info_guard.set_batch_idx(l_idx);
      if (OB_FAIL(fill_cur_row_rescan_param())) {
        LOG_WARN("fail to fill cur row rescan param", K(ret));
      } else if (OB_FAIL(right_->rescan())) {
        LOG_WARN("failed to rescan right", K(ret));
      } else {
        brs_holder_.reset();
      }
    }
    int64_t stored_rows_count = 0;
    bool match_right_batch_end = false;
    bool no_match_row_found = true;
    LOG_DEBUG("ObNestedLoopJoinOp::process_left_batch: after rescan", K(l_idx),
            K(batch_rescan_ctl_.param_version_), K(batch_rescan_ctl_.cur_idx_));
    while (OB_SUCC(ret) && !match_right_batch_end) {
      // get and calc right batch from right child until right operator reach end
      if (OB_FAIL(calc_right_batch_matched_result(l_idx, match_right_batch_end,
                                                  batch_info_guard))) {
        LOG_WARN("fail to get right batch", K(ret));
      } else {
        if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
          // do nothing
        } else if (OB_FAIL(right_store_.add_batch(right_->get_spec().output_,
                                           eval_ctx_, *brs_.skip_,
                                           brs_.size_,
                                           stored_rows_count,
                                           stored_rows_))) {
          LOG_WARN("fail to add rows", K(ret));
        } else if (stored_rows_count > 0) {
          no_match_row_found = false;
          for (int64_t i = 0; i < stored_rows_count; i++) {
            *(reinterpret_cast<int64_t *>(stored_rows_[i]->get_extra_payload())) = l_idx;
          }
        }
      }
      reset_batchrows();
    } // while right batch end

    if (MY_SPEC.enable_px_batch_rescan_) {
      batch_rescan_ctl_.cur_idx_++;
    }
    // outer join: generate a blank row for LEFT OUTER JOIN
    // Note: optimizer guarantee there is NO RIGHT/FULL OUTER JOIN for NLJ
    if (OB_SUCC(ret) && no_match_row_found && need_left_join()) {
      brs_.size_ = 1;
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_idx(0);
      blank_row_batch_one(right_->get_spec().output_);
      // Note: no need to call brs_.skip_->unset(0) as brs_.skip_->reset()
      // already unset that row
      if (OB_FAIL(right_store_.add_batch(right_->get_spec().output_, eval_ctx_,
                                         *brs_.skip_, brs_.size_,
                                         stored_rows_count, stored_rows_))) {
        LOG_WARN("fail to add rows", K(ret));
      } else if (stored_rows_count != 1) {
        LOG_WARN("failed to generate a blank row");
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t i = 0; i < stored_rows_count; i++) {
          *(reinterpret_cast<int64_t *>(stored_rows_[i]->get_extra_payload())) = l_idx;
          LOG_DEBUG("left out join: generated a blank row", K(l_idx));
        }
      }
      reset_batchrows();
    }
  } // for end
  clear_evaluated_flag();

  return ret;
}

int ObNestedLoopJoinOp::calc_right_batch_matched_result(
    int64_t l_idx, bool &match_right_batch_end,
    ObEvalCtx::BatchInfoScopeGuard &batch_info_guard)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *right_brs = &right_->get_brs();
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  clear_evaluated_flag();
  if (!IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_) &&
      OB_FAIL(restore_right_child_exprs())) {
    LOG_WARN("restore expr datums failed", K(ret));
  } else if (OB_FAIL(right_->get_next_batch(op_max_batch_size_, right_brs))) {
    LOG_WARN("fail to get next right batch", K(ret), K(MY_SPEC));
  } else if (0 == right_brs->size_ && right_brs->end_) {
    match_right_batch_end = true;
  } else {
    if (0 == conds.count()) {
      brs_.skip_->deep_copy(*right_brs->skip_, right_brs->size_);
    } else {
      if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
        last_save_batch_.extend_save(eval_ctx_, right_brs->size_);
      } else {
        left_batch_.extend_save(eval_ctx_, right_brs->size_);
      }
      batch_info_guard.set_batch_size(right_brs->size_);
      bool is_match = false;
      for (int64_t r_idx = 0; OB_SUCC(ret) && r_idx < right_brs->size_; r_idx ++) {
        batch_info_guard.set_batch_idx(r_idx);
        if (right_brs->skip_->exist(r_idx)) {
          brs_.skip_->set(r_idx);
        } else if (OB_FAIL(calc_other_conds_with_update_left_expr(
                       is_match, left_batch_, l_idx))) {
          LOG_WARN("calc_other_conds failed", K(ret), K(r_idx),
                   K(right_brs->size_));
        } else if (!is_match) {
          brs_.skip_->set(r_idx);
        } else { /*do nothing*/
        }
        LOG_DEBUG("cal_other_conds finished ", K(is_match), K(l_idx), K(r_idx));
      } // for conds end
    }

    if (OB_SUCC(ret)) {
      brs_.size_ = right_brs->size_;
      if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
        int64_t skip_cnt = brs_.skip_->accumulate_bit_cnt(right_brs->size_);
        if (right_brs->size_ - skip_cnt > 0) {
          left_matched_->set(l_idx);
          match_right_batch_end = true;
        }
      }
      match_right_batch_end = match_right_batch_end || right_brs->end_;
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::output()
{
  int ret = OB_SUCCESS;
  if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
    if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
      brs_.skip_->bit_calculate(*left_batch_.skip_, *left_matched_, left_batch_.size_,
                                [](const uint64_t l, const uint64_t r) { return (l | (~r)); });
    } else if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      brs_.skip_->bit_calculate(*left_batch_.skip_, *left_matched_, left_batch_.size_,
                                [](const uint64_t l, const uint64_t r) { return (l | r); });
    }
    if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
      last_save_batch_.extend_save(eval_ctx_, left_batch_.size_);
    }
    left_batch_.to_exprs(eval_ctx_);
    brs_.size_ = left_batch_.size_;
    left_matched_->reset(left_batch_.size_);
  } else {
    reset_batchrows();
    if (right_store_iter_.is_valid() && right_store_iter_.has_next()) {
      int64_t read_rows = 0;
      if (OB_FAIL(right_store_iter_.get_next_batch(right_->get_spec().output_, eval_ctx_,
                              op_max_batch_size_, read_rows,
                              const_cast<const ObChunkDatumStore::StoredRow **>(stored_rows_)))) {
        if (OB_ITER_END == ret) {
          OB_ASSERT(read_rows == 0);
          // do nothing
        } else {
          LOG_WARN("fail to get next batch", K(ret));
        }
      } else {
        if (MY_SPEC.use_group_ || MY_SPEC.enable_px_batch_rescan_) {
          last_save_batch_.extend_save(eval_ctx_, read_rows);
        } else {
          left_batch_.extend_save(eval_ctx_, read_rows);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < read_rows; i++) {
          int64_t left_batch_idx = *(reinterpret_cast<int64_t *>(
                                     stored_rows_[i]->get_extra_payload()));
          left_batch_.to_exprs(eval_ctx_, left_batch_idx, i);
        }
      }
      brs_.size_ = read_rows;
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  }
  op_max_batch_size_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
  while (!iter_end_ && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (JS_FILL_LEFT == batch_state_) {
      if (OB_FAIL(get_left_batch())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          brs_.size_ = 0;
          brs_.end_ = true;
          iter_end_ = true;
        } else {
          LOG_WARN("fail to get left batch", K(ret));
        }
      } else {
        batch_state_ = JS_PROCESS;
      }
    }
    if (JS_PROCESS == batch_state_) {
      right_store_iter_.reset();
      right_store_.reset();
      if (OB_FAIL(process_left_batch())) {
        LOG_WARN("fail to process", K(ret));
      } else {
        if (OB_FAIL(right_store_.finish_add_row(false))) {
          LOG_WARN("failed to finish add row to row store", K(ret));
        } else if (OB_FAIL(right_store_.begin(right_store_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else {
          batch_state_ = JS_OUTPUT;
          if (!IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)
              && OB_FAIL(backup_right_child_exprs())) {
            LOG_WARN("backup expr datums failed", K(ret));
          }
        }
      }
    }
    if (JS_OUTPUT == batch_state_) {
      if (OB_FAIL(output())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          batch_state_ = JS_FILL_LEFT;
        } else {
          LOG_WARN("fail to output", K(ret));
        }
      } else {
        if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
          batch_state_ = JS_FILL_LEFT;
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && iter_end_) {
    set_param_null();
  }

  return ret;
}


// update datums from left child expr before calc other conditions
int ObNestedLoopJoinOp::calc_other_conds_with_update_left_expr(bool &is_match,
                               ObBatchRowDatums &left_batch,
                               const int64_t l_idx)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = MY_SPEC.other_join_conds_;
  const auto &left_output = left_->get_spec().output_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    auto cond = conds.at(i);
    // update datums from condition args when it is from left child
    const ObIArray<int> &left_expr_ids =
        MY_SPEC.left_expr_ids_in_other_cond_.at(i);
    for (auto j = 0; j < left_expr_ids.count();
         j++) {
      auto left_output_expr_id =
          left_expr_ids.at(j);
      auto &datum = left_output.at(left_output_expr_id)
                        ->locate_datum_for_write(eval_ctx_);
      datum = left_batch.get_datum(
          // always fetch datum 0 if expr is NOT batch result
          left_output.at(left_output_expr_id)->is_batch_result() ? l_idx : 0,
          left_output_expr_id);
    }
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }

  return ret;
}

int ObBatchRowDatums::init(const ObExprPtrIArray *exprs, ObIAllocator *alloc, int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc) || OB_ISNULL(exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(alloc), KP(exprs));
  } else {
    char *buf= (char *)alloc->alloc(ObBitVector::memory_size(batch_size)
                                    + sizeof(ObDatum) * batch_size * exprs->count());
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      MEMSET(buf, 0, ObBitVector::memory_size(batch_size));
      skip_ = to_bit_vector(buf);
      alloc_ = alloc;
      exprs_ = exprs;
      datums_ = reinterpret_cast<ObDatum *>(buf + ObBitVector::memory_size(batch_size));
      batch_size_ = batch_size;
      size_ = 0;
      saved_size_ = 0;
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

} // end namespace sql
} // end namespace oceanbase
