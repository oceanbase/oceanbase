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

#include "sql/engine/join/ob_nested_loop_join_vec_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/expr/ob_expr.h"
#include <algorithm>

namespace oceanbase
{
using namespace commom;
namespace sql
{

OB_SERIALIZE_MEMBER((ObNestedLoopJoinVecSpec, ObJoinVecSpec),
                    rescan_params_,
                    gi_partition_id_expr_,
                    enable_gi_partition_pruning_,
                    enable_px_batch_rescan_,
                    group_rescan_, group_size_,
                    left_expr_ids_in_other_cond_,
                    left_rescan_params_,
                    right_rescan_params_);

ObNestedLoopJoinVecOp::ObNestedLoopJoinVecOp(ObExecContext &exec_ctx,
                                                 const ObOpSpec &spec,
                                                 ObOpInput *input)
  : ObJoinVecOp(exec_ctx, spec, input),
    batch_state_(JS_GET_LEFT_ROW),
    is_left_end_(false), left_brs_(nullptr),
    iter_end_(false), op_max_batch_size_(0),
    drive_iter_(), match_right_batch_end_(false),
    no_match_row_found_(true), need_output_row_(false),
    defered_right_rescan_(false), is_cartesian_(false), cartesian_opt_(false), right_total_row_cnt_(0)
{
}

int ObNestedLoopJoinVecOp::inner_open()
{
  LOG_TRACE("open ObNestedLoopJoinVecOp", K(MY_SPEC.join_type_));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nlp_op child is null", KP(left_), KP(right_), K(ret));
  } else if (OB_FAIL(ObJoinVecOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(drive_iter_.init(this,
                                      MY_SPEC.group_size_,
                                      &MY_SPEC.rescan_params_,
                                      MY_SPEC.group_rescan_,
                                      true
                                      ))) {
    LOG_WARN("failed to init drive iterator for NLJ", KR(ret));
  } else {
    is_cartesian_ = (MY_SPEC.rescan_params_.count() == 0 &&
                     MY_SPEC.other_join_conds_.count() == 0) ||
                    (MY_SPEC.rescan_params_.count() == 0 &&
                     INNER_JOIN == MY_SPEC.join_type_);
  }
  return ret;
}

int ObNestedLoopJoinVecOp::rescan()
{
  int ret = OB_SUCCESS;
  //NLJ's rescan should only drive left child's rescan,
  //the right child's rescan is defer to rescan_right_operator() driven by get_next_row();
  defered_right_rescan_ = true;
  if (OB_FAIL(drive_iter_.rescan_left())) {
    LOG_WARN("rescan left child operator failed", KR(ret), "child op_type", left_->op_name());
  } else if (OB_FAIL(inner_rescan())) {
    LOG_WARN("failed to inner rescan", KR(ret));
  }
#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif
  return ret;
}

int ObNestedLoopJoinVecOp::do_drain_exch()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.group_rescan_) {
    if (OB_FAIL( ObOperator::do_drain_exch())) {
      LOG_WARN("failed to drain NLJ operator", K(ret));
    }
  } else if (!drive_iter_.is_multi_level_group_rescan()) {
    if (OB_FAIL( ObOperator::do_drain_exch())) {
      LOG_WARN("failed to drain NLJ operator", K(ret));
    }
  } else {
    if (!is_operator_end()) {
      // the drain request is triggered by parent operator
      // NLJ needs to pass the drain request to it's child operator
      LOG_TRACE("The drain request is passed by parent operator");
      if (OB_FAIL( ObOperator::do_drain_exch())) {
        LOG_WARN("failed to drain normal NLJ operator", K(ret));
      }
    } else if (OB_FAIL(do_drain_exch_multi_lvel_bnlj())) {
      LOG_WARN("failed to drain multi level NLJ operator", K(ret));
    }
  }
  return ret;
}

int ObNestedLoopJoinVecOp::do_drain_exch_multi_lvel_bnlj()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_open())) {
    LOG_WARN("fail to open operator", K(ret));
  } else if (!exch_drained_) {
    // the drain request is triggered by current NLJ operator, and current NLJ is a multi level Batch NLJ
    // It will block rescan request for it's child operator, if the drain request is passed to it's child operator
    // The child operators will be marked as iter-end_, and will not get any row if rescan is blocked
    // So we block the drain request here; Only set current operator to end;
    int tmp_ret = inner_drain_exch();
    exch_drained_ = true;
    brs_.end_ = true;
    batch_reach_end_ = true;
    row_reach_end_ = true;
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

void ObNestedLoopJoinVecOp::reset_buf_state()
{
  is_left_end_ = false;
  batch_state_ = JS_GET_LEFT_ROW;
  match_right_batch_end_ = false;
  no_match_row_found_ = true;
  need_output_row_ = false;
  drive_iter_.reset();
  iter_end_ = false;
  cartesian_opt_ = false;
}
int ObNestedLoopJoinVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset_buf_state();
  set_param_null();
  if (OB_FAIL(ObJoinVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObNestedLoopJoinVecOp::inner_close()
{
  drive_iter_.reset();
  return OB_SUCCESS;
}

int ObNestedLoopJoinVecOp::get_next_left_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drive_iter_.get_next_left_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next left row from driver iterator", K(ret));
    }
  }
  return ret;
}


int ObNestedLoopJoinVecOp::perform_gi_partition_prunig()
{
  int ret = OB_SUCCESS;
  // 左边每一行出来后，去通知右侧 GI 实施 part id 过滤，避免 PKEY NLJ 场景下扫不必要分区
  if (OB_SUCC(ret) && !get_spec().enable_px_batch_rescan_ && !get_spec().group_rescan_ && get_spec().enable_gi_partition_pruning_) {
    ObDatum *datum = nullptr;
    if (OB_FAIL(get_spec().gi_partition_id_expr_->eval(eval_ctx_, datum))) {
      LOG_WARN("fail eval value", K(ret));
    } else {
      // NOTE: 如果右侧对应多张表，这里的逻辑也没有问题
      // 如 A REPART TO NLJ (B JOIN C) 的场景
      // 此时 GI 在 B 和 C 的上面
      int64_t part_id = datum->get_int();
      ctx_.get_gi_pruning_info().set_part_id(part_id);
    }
  }
  return ret;
}

int ObNestedLoopJoinVecOp::rescan_right_operator()
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
    } else if (PHY_VEC_MATERIAL == right_->get_spec().type_) {
      if (OB_FAIL(static_cast<ObMaterialVecOp*>(right_)->rewind())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("rewind failed", K(ret));
        }
      }
    } else {
      do_rescan = true;
    }
  }
  if (OB_SUCC(ret) && do_rescan) {
    GroupParamBackupGuard guard(right_->get_exec_ctx().get_das_ctx());
    if (MY_SPEC.group_rescan_) {
      drive_iter_.bind_group_params_to_das_ctx(guard);
    }
    if (OB_FAIL(right_->rescan())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("rescan right failed", K(ret));
      }
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObNestedLoopJoinVecOp::rescan_right_op()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(drive_iter_.get_left_batch_size());
  batch_info_guard.set_batch_idx(drive_iter_.get_left_batch_idx());
  if (OB_FAIL(drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), drive_iter_.get_left_batch_idx()))) {
    LOG_WARN("failed to restore single row", K(ret), K(drive_iter_.get_left_batch_idx()));
  } else if (OB_FAIL(drive_iter_.fill_cur_row_group_param())) {
    LOG_WARN("failed to prepare rescan params for NLJ", K(ret));
  } else if (OB_FAIL(perform_gi_partition_prunig())) {
    LOG_WARN("failed perform gi partition pruning", K(ret));
  } else if (OB_FAIL(rescan_right_operator())) {
    LOG_WARN("failed to rescan right operator", K(ret));
  }
  return ret;
}

int ObNestedLoopJoinVecOp::get_next_batch_from_right(const ObBatchRows *right_brs)
{
  int ret = OB_SUCCESS;
  GroupParamBackupGuard guard(right_->get_exec_ctx().get_das_ctx());
  if (MY_SPEC.group_rescan_) {
    drive_iter_.bind_group_params_to_das_ctx(guard);
  }
  ret = right_->get_next_batch(op_max_batch_size_, right_brs);
  return ret;
}

int ObNestedLoopJoinVecOp::process_right_batch()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(drive_iter_.get_left_batch_size());
  reset_batchrows();
  const ObBatchRows *right_brs = &right_->get_brs();
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  clear_evaluated_flag();
  DASGroupScanMarkGuard mark_guard(ctx_.get_das_ctx(), MY_SPEC.group_rescan_);
  if (OB_FAIL(get_next_batch_from_right(right_brs))) {
    LOG_WARN("fail to get next right batch", K(ret), K(MY_SPEC));
  } else if (is_cartesian_ && (PHY_MATERIAL == right_->get_spec().type_ ||
                               PHY_VEC_MATERIAL == right_->get_spec().type_)) {
    if (PHY_MATERIAL == right_->get_spec().type_ &&
        OB_FAIL(static_cast<ObMaterialOp *>(right_)->get_material_row_count(
            right_total_row_cnt_))) {
      LOG_WARN("failed to get material row count in right side", K(ret),
               K(right_->get_spec().type_));
    } else if (PHY_VEC_MATERIAL == right_->get_spec().type_ &&
               OB_FAIL(static_cast<ObMaterialVecOp *>(right_)
                           ->get_material_row_count(right_total_row_cnt_))) {
      LOG_WARN("failed to get material row count in right side", K(ret),
               K(right_->get_spec().type_));
    } else if (right_brs->skip_->accumulate_bit_cnt(right_brs->size_) == 0 &&
               right_total_row_cnt_ == right_brs->size_) {
      if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
        cartesian_opt_ = true;
      } else if ((INNER_JOIN == MY_SPEC.join_type_ ||
                  LEFT_OUTER_JOIN == MY_SPEC.join_type_) &&
                 right_total_row_cnt_ * 2 <= op_max_batch_size_) {
        if (OB_FAIL(drive_iter_.save_right_batch(right_->get_spec().output_))) {
          LOG_WARN("failed to save right batch", K(ret));
        } else {
          cartesian_opt_ = true;
        }
      }
    }
  } else if (0 == right_brs->size_ && right_brs->end_) {
    match_right_batch_end_ = true;
    LOG_DEBUG("right rows iter end");
  }

  if (OB_FAIL(ret) || cartesian_opt_) {
  } else if (OB_FAIL(drive_iter_.drive_row_extend(right_brs->size_))) {
    LOG_WARN("failed to extend drive row", K(ret));
  } else {
    if (0 == conds.count()) {
      brs_.skip_->deep_copy(*right_brs->skip_, right_brs->size_);
    } else {
      if (right_brs->size_ == 0) {
      } else if (OB_FAIL(brs_.copy(right_brs))) {
      } else if (OB_FALSE_IT(brs_.end_ = false)) {
        LOG_WARN("copy from right brs failed", K(ret));
      } else if (OB_FAIL(batch_calc_other_conds(brs_))) {
        LOG_WARN("batch calc other conditions failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // if is not semi/anti-join, the output of NLJ is the join result of right batches
      brs_.size_ = right_brs->size_;
      int64_t skip_cnt = brs_.skip_->accumulate_bit_cnt(right_brs->size_);
      if (MY_SPEC.join_type_ == LEFT_SEMI_JOIN) {
        if (right_brs->size_ - skip_cnt > 0) {
          match_right_batch_end_ = true;
          no_match_row_found_ = false;
          need_output_row_ = true;
        }
      } else if (MY_SPEC.join_type_ == LEFT_ANTI_JOIN) {
        if (right_brs->size_ - skip_cnt > 0) {
          no_match_row_found_ = false;
          match_right_batch_end_ = true;
        }
      } else {
        if (right_brs->size_ - skip_cnt > 0) {
          no_match_row_found_ = false;
          need_output_row_ = true;
        }
      }
      match_right_batch_end_ = match_right_batch_end_ || right_brs->end_;
    }
  }
  // outer join or anti-join
  if (OB_SUCC(ret)) {
    if (match_right_batch_end_ && no_match_row_found_) {
      if (need_left_join() || MY_SPEC.join_type_ == LEFT_ANTI_JOIN) {
        need_output_row_ = true;
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinVecOp::cartesian_optimized_process()
{
  int ret = OB_SUCCESS;
  if (INNER_JOIN == MY_SPEC.join_type_ || LEFT_OUTER_JOIN == MY_SPEC.join_type_) {
    if (right_total_row_cnt_ > 0) {
      int64_t right_extend_times = std::max(1L, std::min(op_max_batch_size_ / right_total_row_cnt_,
                                            drive_iter_.get_left_valid_rows_cnt()));
      if (OB_FAIL(drive_iter_.right_rows_extend(right_total_row_cnt_, right_extend_times))) {
        LOG_WARN("failed to extend right rows", K(ret));
      } else if (OB_FAIL(drive_iter_.extend_left_next_batch_rows(right_extend_times, right_total_row_cnt_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to extend left rows", K(ret));
        }
      } else {
        drive_iter_.set_real_ouptut_right_batch_times(right_extend_times);
        brs_.size_ = right_total_row_cnt_ * right_extend_times;
        brs_.reset_skip(brs_.size_);
        if (INNER_JOIN == MY_SPEC.join_type_ && OB_FAIL(batch_calc_other_conds(brs_))) {
          LOG_WARN("batch calc other conditions failed", K(ret));
        }
      }
    } else {
      if (INNER_JOIN == MY_SPEC.join_type_) {
        brs_.size_ = 0;
        ret = OB_ITER_END;
      } else {
        int64_t left_rows_cnt = drive_iter_.get_left_batch_size();
        if (OB_FAIL(blank_row_batch(right_->get_spec().output_, left_rows_cnt))) {
          LOG_WARN("failed to blank right rows", K(ret));
        } else if (OB_FAIL(drive_iter_.extend_left_next_batch_rows(left_rows_cnt, 1))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to extend left rows", K(ret));
          }
        } else {
          brs_.size_ = left_rows_cnt;
          brs_.reset_skip(brs_.size_);
        }
      }
    }
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    if ((right_total_row_cnt_ > 0 && LEFT_SEMI_JOIN == MY_SPEC.join_type_) ||
        (right_total_row_cnt_ == 0 && LEFT_ANTI_JOIN == MY_SPEC.join_type_)) {
      int64_t left_rows_cnt = drive_iter_.get_left_batch_size();
      if (OB_FAIL(drive_iter_.extend_left_next_batch_rows(left_rows_cnt, 1))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to extend left rows", K(ret));
        }
      } else {
        brs_.size_ = left_rows_cnt;
        brs_.reset_skip(brs_.size_);
      }
    } else {
      brs_.size_ = 0;
      ret = OB_ITER_END;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObNestedLoopJoinVecOp::output()
{
  int ret = OB_SUCCESS;
  if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
    reset_batchrows();
    brs_.size_ = 1;
    drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), 0);
  }

  if (OB_SUCC(ret) && need_left_join() && match_right_batch_end_ && no_match_row_found_) {
    reset_batchrows();
    brs_.size_ = 1;
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    blank_row_batch_one(right_->get_spec().output_);
    drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), 0);
  } else {
    // do nothing
  }
  return ret;
}


void ObNestedLoopJoinVecOp::reset_right_batch_state()
{
  match_right_batch_end_ = false;
  no_match_row_found_ = true;
}

int ObNestedLoopJoinVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  }
  op_max_batch_size_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
  while (!iter_end_ && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (JS_GET_LEFT_ROW == batch_state_) {
      LOG_DEBUG("start get left row", K(spec_.id_));
      reset_batchrows();
      if (OB_FAIL(get_next_left_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          brs_.size_ = 0;
          brs_.end_ = true;
          iter_end_ = true;
        } else {
          LOG_WARN("fail to get left batch", K(ret));
        }
      } else {
        batch_state_ = JS_RESCAN_RIGHT_OP;
      }
    }

    if (OB_SUCC(ret) && JS_RESCAN_RIGHT_OP == batch_state_) {
      LOG_DEBUG("start rescan right op", K(spec_.id_), K(drive_iter_.get_left_batch_idx()));
      if (OB_FAIL(rescan_right_op())) {
        LOG_WARN("failed to rescan right", K(ret));
      } else {
        batch_state_ = JS_PROCESS_RIGHT_BATCH;
      }
    }
    // process right batch
    if (OB_SUCC(ret) && JS_PROCESS_RIGHT_BATCH == batch_state_) {
      LOG_DEBUG("start process right batch", K(spec_.id_),K(drive_iter_.get_left_batch_idx()));
      if (OB_FAIL(process_right_batch())) {
        LOG_WARN("fail to process right batch", K(ret));
      } else if (cartesian_opt_) {
        batch_state_ = JS_CARTESIAN_OPTIMIZED_PROCESS;
      } else {
        if (need_output_row_) {
          batch_state_ = JS_OUTPUT;
          need_output_row_ = false;
        } else {
          if (match_right_batch_end_) {
            batch_state_ = JS_GET_LEFT_ROW;
            reset_right_batch_state();
          } else {
            batch_state_ = JS_PROCESS_RIGHT_BATCH;
          }
        }
      }
    } // end process right state

    if (OB_SUCC(ret) && JS_CARTESIAN_OPTIMIZED_PROCESS == batch_state_) {
      if (OB_FAIL(cartesian_optimized_process())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          brs_.size_ = 0;
          brs_.end_ = true;
          iter_end_ = true;
        } else {
          LOG_WARN("failed to process cartesian optimization path", K(ret));
        }
      } else {
        break;
      }
    }

    // start output state
    if (OB_SUCC(ret) && JS_OUTPUT == batch_state_) {
      LOG_DEBUG("start output", K(spec_.id_), K(drive_iter_.get_left_batch_idx()));
      if (OB_FAIL(output())) {
        LOG_WARN("fail to output", K(ret));
      } else {
        if (match_right_batch_end_) {
          batch_state_ = JS_GET_LEFT_ROW;
          reset_right_batch_state();
        } else {
          batch_state_ = JS_PROCESS_RIGHT_BATCH;
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

} // end namespace sql
} // end namespace oceanbase
