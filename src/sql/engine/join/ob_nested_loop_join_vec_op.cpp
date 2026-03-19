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
    defered_right_rescan_(false), is_cartesian_(false), cartesian_opt_(false), right_total_row_cnt_(0),
    need_restore_drive_row_(false),

    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_),
    right_hldr_(),
    left_row_store_(), right_row_store_(),
    left_row_reader_(), right_row_reader_(), output_pairs_(), left_rows_(nullptr), right_rows_(nullptr),
    cur_output_idx_(0), need_store_drive_row_(true), end_after_cache_output_(false), drive_row_idx_(0),
    mocked_null_row_(nullptr), cache_mem_context_(nullptr)
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
  } else if (OB_FAIL(right_hldr_.init(right_->get_spec().output_, eval_ctx_))) {
  } else if (OB_FAIL(init_output_cache())) {
    LOG_WARN("failed to init output cache for NLJ", KR(ret));
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
  reset_output_cache();
}
int ObNestedLoopJoinVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset_buf_state();
  set_param_null();
  right_hldr_.reset();
  if (OB_FAIL(ObJoinVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  need_store_drive_row_ = true;
  end_after_cache_output_= false;
  need_restore_drive_row_ = false;
  drive_row_idx_ = 0;
  return ret;
}

int ObNestedLoopJoinVecOp::inner_close()
{
  drive_iter_.reset();
  sql_mem_processor_.unregister_profile();
  left_row_reader_.reset();
  right_row_reader_.reset();
  left_row_store_.destroy();
  right_row_store_.destroy();
  output_pairs_.reset();
  right_hldr_.reset();
  if (nullptr != cache_mem_context_) {
    if (OB_NOT_NULL(left_rows_)) {
      cache_mem_context_->get_malloc_allocator().free(left_rows_);
      left_rows_ = nullptr;
    }
    if (OB_NOT_NULL(right_rows_)) {
      cache_mem_context_->get_malloc_allocator().free(right_rows_);
      right_rows_ = nullptr;
    }
    if (OB_NOT_NULL(mocked_null_row_)) {
      cache_mem_context_->get_malloc_allocator().free(mocked_null_row_);
      mocked_null_row_ = nullptr;
    }
    DESTROY_CONTEXT(cache_mem_context_);
    cache_mem_context_ = nullptr;
  }
  return OB_SUCCESS;
}

int ObNestedLoopJoinVecOp::get_next_left_row()
{
  int ret = OB_SUCCESS;
  bool next_batch = false;
  if (OB_FAIL(drive_iter_.get_next_left_row(next_batch))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next left row from driver iterator", K(ret));
    }
  } else {
    need_store_drive_row_ = true;
    need_restore_drive_row_ = next_batch ? false : need_restore_drive_row_;
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
  drive_row_idx_ = drive_iter_.get_left_batch_idx();
  if (OB_FAIL(need_restore_drive_row_ &&
          drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(),
                                        drive_iter_.get_left_batch_idx()))) {
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

int ObNestedLoopJoinVecOp::is_cache_full(bool &is_full)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  is_full = false;
  ObNLJVecMemChecker checker(right_row_store_.get_row_cnt());
  if (output_pairs_.count() >= op_max_batch_size_) {
    is_full = true;
  } else if (output_pairs_.count() * 2 >= op_max_batch_size_) {
      if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
                &(cache_mem_context_->get_malloc_allocator()),
                checker,
                updated))) {
      LOG_WARN("failed to update max available memory size periodically", K(ret));
    } else if (updated && sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound()
              && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
              &cache_mem_context_->get_malloc_allocator(),
              [&](int64_t max_memory_size) {
                return sql_mem_processor_.get_data_size() > max_memory_size * 0.8;
              },
              is_full, sql_mem_processor_.get_data_size()))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
  }
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
  if (OB_FAIL(right_hldr_.restore())) {
    LOG_WARN("failed to restore right batch", K(ret));
  } else if (OB_FAIL(get_next_batch_from_right(right_brs))) {
    LOG_WARN("fail to get next right batch", K(ret), K(MY_SPEC));
  } else if (is_cartesian_ && output_pairs_.count() == 0 &&
                (PHY_MATERIAL == right_->get_spec().type_ ||
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
  } else {
    int64_t right_skip_cnt = right_brs->skip_->accumulate_bit_cnt(right_brs->size_);
    if (right_brs->size_ > 0 &&
        ((((right_brs->size_ - right_skip_cnt) * 2 >= op_max_batch_size_) && output_pairs_.count() == 0)
          || 0 != conds.count())) {
      if (OB_FAIL(drive_iter_.drive_row_extend(right_brs->size_))) {
        LOG_WARN("failed to extend drive row", K(ret));
      } else {
        need_restore_drive_row_ = true;
        drive_row_idx_ = 0;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (right_brs->size_ == 0) {
      brs_.size_ = 0;
    } else if (OB_FAIL(brs_.copy(right_brs))) {
      LOG_WARN("copy from right brs failed", K(ret));
    } else if (OB_FALSE_IT(brs_.end_ = false)) {
    } else if (OB_FAIL(batch_calc_other_conds(brs_))) {
      LOG_WARN("batch calc other conditions failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t skip_cnt = brs_.size_ == 0 ? 0 : (0 == conds.count() ? right_skip_cnt : brs_.skip_->accumulate_bit_cnt(brs_.size_));
      // if is not semi/anti-join, the output of NLJ is the join result of right batches
      if (MY_SPEC.join_type_ == LEFT_SEMI_JOIN) {
        if (brs_.size_ - skip_cnt > 0) {
          match_right_batch_end_ = true;
          no_match_row_found_ = false;
          int64_t l_row_id = -1;
          int64_t r_start_row_id = -1;
          int64_t r_end_row_id = -1;
          if (OB_FAIL(store_child_batch<false>(drive_row_idx_, &brs_, l_row_id, r_start_row_id, r_end_row_id))) {
            LOG_WARN("failed to cache left row.", K(ret), K(l_row_id));
          } else if (OB_FAIL(output_pairs_.push_back(RowPair(l_row_id, -1)))) {
            LOG_WARN("failed to push RowPair to output_pairs_.", K(ret), K(l_row_id));
          } else if (OB_FAIL(is_cache_full(need_output_row_))) {
            LOG_WARN("failed to check output cache.", K(ret), K(need_output_row_));
          }
        }
      } else if (MY_SPEC.join_type_ == LEFT_ANTI_JOIN) {
        if (brs_.size_ - skip_cnt > 0) {
          no_match_row_found_ = false;
          match_right_batch_end_ = true;
        }
      } else {
        if (brs_.size_ - skip_cnt > 0) {
          no_match_row_found_ = false;
          if ((brs_.size_ - skip_cnt) * 2 < op_max_batch_size_ || output_pairs_.count() > 0) {
            int64_t l_row_id = -1;
            int64_t r_start_row_id = -1;
            int64_t r_end_row_id = -1;
            if (OB_FAIL(store_child_batch<true>(drive_row_idx_, &brs_, l_row_id, r_start_row_id, r_end_row_id))) {
              LOG_WARN("failed to cache children row.", K(ret), K(l_row_id), K(r_start_row_id), K(r_end_row_id));
            } else {
              for (int64_t i = r_start_row_id; OB_SUCC(ret) && i < r_end_row_id; ++i) {
                if (OB_FAIL(output_pairs_.push_back(RowPair(l_row_id, i)))) {
                  LOG_WARN("failed to push RowPair to output_pairs_.", K(ret), K(l_row_id), K(i));
                }
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(is_cache_full(need_output_row_))) {
              LOG_WARN("failed to check output cache.", K(ret), K(need_output_row_));
            }
          } else {
            need_output_row_ = true;
          }
        }
      }
      match_right_batch_end_ = match_right_batch_end_ || right_brs->end_;
    }
    // outer join or anti-join
    if (OB_SUCC(ret)) {
      if (match_right_batch_end_ && no_match_row_found_) {
        if (need_left_join() || MY_SPEC.join_type_ == LEFT_ANTI_JOIN) {
          int64_t l_row_id = -1;
          int64_t r_start_row_id = -1;
          int64_t r_end_row_id = -1;
          if (OB_FAIL(store_child_batch<false>(drive_row_idx_, &brs_, l_row_id, r_start_row_id, r_end_row_id))) {
            LOG_WARN("failed to cache left row.", K(ret), K(l_row_id));
          } else if (OB_FAIL(output_pairs_.push_back(RowPair(l_row_id, -1)))) {
            LOG_WARN("failed to push RowPair to output_pairs_.", K(ret), K(l_row_id));
          } else if (OB_FAIL(is_cache_full(need_output_row_))) {
            LOG_WARN("failed to check output cache.", K(ret), K(need_output_row_));
          }
        }
      }
    }

  }
  return ret;
}

int ObNestedLoopJoinVecOp::cartesian_optimized_process()
{
  int ret = OB_SUCCESS;
  need_restore_drive_row_ = true;
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
  if (end_after_cache_output_ || output_pairs_.count() > 0) {
    clear_evaluated_flag();
    drive_iter_.reset_left_expr_extend_size();
    int output_cnt = 0;
    if (output_pairs_.count() <= cur_output_idx_) {
      reset_output_cache();
      drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), drive_iter_.get_left_batch_idx());
      drive_row_idx_ = drive_iter_.get_left_batch_idx();
    } else {
      const ObCompactRow **left_rows = left_rows_;
      const ObCompactRow **right_rows = right_rows_;
      set_row_store_it_age(&rows_it_age_);
      while (OB_SUCC(ret) && output_cnt < op_max_batch_size_ && cur_output_idx_ < output_pairs_.count()) {
        const ObCompactRow *left_row = nullptr;
        const ObCompactRow *right_row = nullptr;
        if (OB_FAIL(left_row_reader_.get_row(output_pairs_.at(cur_output_idx_).first, left_row))) {
          LOG_WARN("get left row from store failed", K(ret), K(output_pairs_.at(cur_output_idx_).first));
        } else if (output_pairs_.at(cur_output_idx_).second == -1) {
          right_row = mocked_null_row_;
        } else if (OB_FAIL(right_row_reader_.get_row(output_pairs_.at(cur_output_idx_).second, right_row))) {
          LOG_WARN("get left row from store failed", K(ret), K(output_pairs_.at(cur_output_idx_).second));
        }
        left_rows[output_cnt] = left_row;
        right_rows[output_cnt] = right_row;
        output_cnt++;
        cur_output_idx_++;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(right_hldr_.save(right_->get_brs().size_))) {
        LOG_WARN("failed to save right batch", K(ret), K(right_->get_brs().size_));
      } else {
        const RowMeta &left_row_meta = left_row_reader_.get_row_meta();
        const RowMeta &right_row_meta = right_row_reader_.get_row_meta();
        if (OB_FAIL(compact_row_to_vector(left_row_meta, left_rows_, left_->get_spec().output_, output_cnt))) {
          LOG_WARN("failed to project rows of left child", K(ret), K(output_cnt));
        } else if (OB_FAIL(compact_row_to_vector(right_row_meta, right_rows_, right_->get_spec().output_, output_cnt))) {
          LOG_WARN("failed to project rows of right child", K(ret), K(output_cnt));
        }
      }
      set_row_store_it_age(nullptr);
    }
    brs_.size_ = output_cnt;
    brs_.reset_skip(output_cnt);
  } else {
    if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
      reset_batchrows();
      brs_.size_ = 1;
      drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), 0);
      drive_row_idx_ = 0;
    }

    if (OB_SUCC(ret) && need_left_join() && match_right_batch_end_ && no_match_row_found_) {
      reset_batchrows();
      brs_.size_ = 1;
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_idx(0);
      blank_row_batch_one(right_->get_spec().output_);
      drive_iter_.restore_drive_row(drive_iter_.get_left_batch_idx(), 0);
      drive_row_idx_ = 0;
    } else {
      // do nothing
    }
  }
  need_restore_drive_row_ = true;
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
          if (output_pairs_.count() > 0) {
            batch_state_ = JS_OUTPUT;
            end_after_cache_output_ = true;
            need_output_row_ = false;
          } else {
            brs_.size_ = 0;
            brs_.end_ = true;
            iter_end_ = true;
          }
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
      } else if (end_after_cache_output_) {
        brs_.end_ = brs_.size_ == 0;
        iter_end_ = brs_.size_ == 0;
      } else if (output_pairs_.count() > 0) {
      } else if (match_right_batch_end_) {
        batch_state_ = JS_GET_LEFT_ROW;
        reset_right_batch_state();
      } else {
        batch_state_ = JS_PROCESS_RIGHT_BATCH;
      }
      break;
    }
  }
  need_restore_drive_row_ = true;
  if (OB_SUCC(ret) && iter_end_) {
    set_param_null();
  }
  return ret;
}

int ObNestedLoopJoinVecOp::init_output_cache()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left or right child is null", KP(left_), KP(right_), K(ret));
  } else {
    const int64_t left_width = left_->get_spec().width_;
    const int64_t right_width = right_->get_spec().width_;
    const int64_t cache_size = MY_SPEC.max_batch_size_ * (left_width + right_width) * 2;
    ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_NLJ_CACHE, ObCtxIds::WORK_AREA);
    lib::ContextParam param;
    param.set_mem_attr(mem_attr).set_properties(lib::USE_TL_PAGE_OPTIONAL);

    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(cache_mem_context_, param))) {
      LOG_WARN("create memory context failed", KR(ret));
    } else if (OB_ISNULL(cache_mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memory context is null", KR(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(&(cache_mem_context_->get_malloc_allocator()),
                                        tenant_id,
                                        std::max(2L << 20, cache_size),
                                        MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    } else {
      ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
      if (OB_FAIL(left_row_store_.init(left_->get_spec().output_,
                                       eval_ctx_.max_batch_size_,
                                       mem_attr,
                                       UINT64_MAX,
                                       true,  // enable_dump
                                       0,     // row_extra_size
                                       compressor_type,
                                       false, // reorder_fixed_expr
                                       false))) {  // enable_trunc
        LOG_WARN("init left row store failed", KR(ret));
      } else if (OB_FAIL(right_row_store_.init(right_->get_spec().output_,
                                                eval_ctx_.max_batch_size_,
                                                mem_attr,
                                                UINT64_MAX,
                                                true,  // enable_dump
                                                0,     // row_extra_size
                                                compressor_type,
                                                false, // reorder_fixed_expr
                                                false))) {  // enable_trunc
        LOG_WARN("init right row store failed", KR(ret));
      } else if (OB_FAIL(left_row_reader_.init(&left_row_store_))) {
        LOG_WARN("begin left row reader failed", KR(ret));
      } else if (OB_FAIL(right_row_reader_.init(&right_row_store_))) {
        LOG_WARN("begin right row reader failed", KR(ret));
      } else {
        left_row_store_.set_allocator(cache_mem_context_->get_malloc_allocator());
        left_row_store_.set_mem_stat(&sql_mem_processor_);
        left_row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        left_row_store_.set_io_event_observer(&io_event_observer_);

        right_row_store_.set_allocator(cache_mem_context_->get_malloc_allocator());
        right_row_store_.set_mem_stat(&sql_mem_processor_);
        right_row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        right_row_store_.set_io_event_observer(&io_event_observer_);

        left_rows_ =
            static_cast<const ObCompactRow **>(cache_mem_context_->get_malloc_allocator().alloc(
                sizeof(ObCompactRow *) * MY_SPEC.max_batch_size_));
        right_rows_ =
            static_cast<const ObCompactRow **>(cache_mem_context_->get_malloc_allocator().alloc(
                sizeof(ObCompactRow *) * MY_SPEC.max_batch_size_));

        void* ptr = nullptr;
        int64_t memory_size = sizeof(ObCompactRow) + ObTinyBitVector::memory_size(right_->get_spec().output_.count());
        if (OB_ISNULL(ptr = cache_mem_context_->get_malloc_allocator().alloc(memory_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(memory_size));
        } else {
          mocked_null_row_ = new (ptr) ObCompactRow();
          mocked_null_row_->nulls()->set_all(right_->get_spec().output_.count());
        }

        if (OB_ISNULL(left_rows_) || OB_ISNULL(right_rows_) || OB_ISNULL(mocked_null_row_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(left_rows_), K(right_rows_));
        }
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinVecOp::compact_row_to_vector(const RowMeta &row_meta,
                                                 const ObCompactRow **compact_rows,
                                                 const common::ObIArray<ObExpr *> &exprs,
                                                 int64_t rows_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObExpr *expr = exprs.at(i);
    if (OB_FAIL(expr->init_vector(eval_ctx_, expr->get_default_res_format(), rows_cnt, false))) {
      LOG_WARN("init vector failed", K(ret), KPC(expr));
    } else if (OB_FAIL(expr->get_vector(eval_ctx_)->from_rows(row_meta,
                                                              compact_rows,
                                                              rows_cnt,
                                                              i))) {
      LOG_WARN("from_rows failed", K(ret), KPC(expr));
    } else {
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

template<bool need_store_right>
int ObNestedLoopJoinVecOp::store_child_batch(const int64_t l_idx, const ObBatchRows *right_brs, int64_t &l_id, int64_t &r_start_id, int64_t &r_end_id)
{
  int ret = OB_SUCCESS;
  l_id = need_store_drive_row_ ? left_row_store_.get_row_cnt() : left_row_store_.get_row_cnt() - 1;
  // 缓存左侧行
  ObCompactRow *stored_row = nullptr;
  if (need_store_drive_row_ && OB_FAIL(left_row_store_.add_row(left_->get_spec().output_,
                                       l_idx,
                                       eval_ctx_,
                                       stored_row))) {
    LOG_WARN("add left row to store failed", K(ret));
  } else if (l_id < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store row id", K(ret), K(l_id), K(need_store_drive_row_), K(MY_SPEC.join_type_));
  } else if (OB_FALSE_IT(need_store_drive_row_ = false)) {
  } else if (need_store_right) {
    // 缓存右侧批次（只缓存有效的行，跳过被 skip 的行）
    int64_t stored_rows_count = 0;
    // 只添加有效行到存储
    r_start_id = right_row_store_.get_row_cnt();
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(right_brs->size_);
    for (int i = 0; i < right_brs->size_; ++i) {
      if (OB_LIKELY(!right_brs->skip_->exist(i))) {
         batch_info_guard.set_batch_idx(i);
         if (OB_FAIL(right_row_store_.add_row(right_->get_spec().output_,
                                          eval_ctx_,
                                          stored_row))) {
          LOG_WARN("add right row to store failed", K(ret));
        }
      }
    }
    r_end_id = right_row_store_.get_row_cnt();
  }
  return ret;
}

void ObNestedLoopJoinVecOp::reset_output_cache()
{
  cur_output_idx_ = 0;
  left_row_reader_.init(&left_row_store_);
  right_row_reader_.init(&right_row_store_);
  left_row_store_.reuse();
  right_row_store_.reuse();
  output_pairs_.reuse();
  need_store_drive_row_ = true;
}

} // end namespace sql
} // end namespace oceanbase
