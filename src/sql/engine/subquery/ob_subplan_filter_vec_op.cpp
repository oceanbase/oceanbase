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

#include "ob_subplan_filter_vec_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObSubPlanFilterVecSpec::ObSubPlanFilterVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObSubPlanFilterSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObSubPlanFilterVecSpec, ObSubPlanFilterSpec));

DEF_TO_STRING(ObSubPlanFilterVecSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(rescan_params),
       K_(onetime_exprs),
       K_(init_plan_idxs),
       K_(one_time_idxs),
       K_(update_set),
       K_(exec_param_idxs_inited));
  J_OBJ_END();
  return pos;
}


ObSubPlanFilterVecOp::ObSubPlanFilterVecOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObSubPlanFilterOp(exec_ctx, spec, input),
    drive_iter_()
{
}


ObSubPlanFilterVecOp::~ObSubPlanFilterVecOp()
{
  destroy_subplan_iters();
  drive_iter_.reset();
}

void ObSubPlanFilterVecOp::destroy_subplan_iters()
{
  FOREACH_CNT(it, subplan_iters_) {
    if (NULL != *it) {
      (*it)->~Iterator();
      *it = NULL;
    }
  }
  subplan_iters_.reset();
}

void ObSubPlanFilterVecOp::destroy()
{
  destroy_subplan_iters();
  drive_iter_.reset();
  // vectorization not supported update set, no need destroy update set memory
  // destroy_update_set_mem();
  ObOperator::destroy();
}

int ObSubPlanFilterVecOp::rescan()
{
  int ret = OB_SUCCESS;
  brs_.end_ = false;
  iter_end_ = false;
  clear_evaluated_flag();
  set_param_null();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to inner rescan", K(ret));
  }
  drive_iter_.reset();

  if (!MY_SPEC.enable_das_group_rescan_) {
    // call each child's rescan when not batch rescan
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      if (OB_FAIL(children_[i]->rescan())) {
        LOG_WARN("rescan child operator failed", K(ret),
                 "op", op_name(), "child", children_[i]->op_name());
      }
    }
  } else {
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      if (MY_SPEC.init_plan_idxs_.has_member(i) || MY_SPEC.one_time_idxs_.has_member(i)) {
        // rescan for init plan and onetime expr when batch rescan
        if (OB_FAIL(children_[i]->rescan())) {
          LOG_WARN("rescan child operator failed", K(ret),
                  "op", op_name(), "child", children_[i]->op_name());
        }
      }
    }
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    Iterator *iter = subplan_iters_.at(i - 1);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan_iter is null", K(ret));
    } else if (MY_SPEC.init_plan_idxs_.has_member(i)) {
      iter->reuse();
      if (OB_FAIL(iter->prepare_init_plan())) {
        LOG_WARN("prepare init plan failed", K(ret), K(i));
      }
    } else if (OB_FAIL(iter->reset_hash_map())) {
      LOG_WARN("failed to reset hash map", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_onetime_exprs())) {
      LOG_WARN("prepare onetime exprs failed", K(ret));
    } else if (OB_FAIL(drive_iter_.rescan_left())) { // use drive_iter_ to rescan drive child operator of SPF
      LOG_WARN("failed to do rescan", K(ret));
    } else {
      startup_passed_ = spec_.startup_filters_.empty();
    }
  }
  need_init_before_get_row_ = false;
#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObSubPlanFilterVecOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to inner switch iterator", K(ret));
  } else if (OB_FAIL(child_->switch_iterator())) {
    //TODO: 目前只支持对非相关子查询做多组迭代器切换，只切换主表
    if (OB_ITER_END != ret) {
      LOG_WARN("switch child operator iterator failed", K(ret));
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObSubPlanFilterVecOp::init_subplan_iters()
{
  int ret = OB_SUCCESS;
  CK(child_cnt_ >= 2);
  LOG_TRACE("init subplan iters in ObSubPlanFilterVecOp", K(child_cnt_));
  if (OB_SUCC(ret)) {
    OZ(subplan_iters_.prepare_allocate(child_cnt_ - 1));
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      void *ptr = ctx_.get_allocator().alloc(sizeof(Iterator));
      Iterator *&iter = subplan_iters_.at(i - 1);
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc subplan iterator failed", K(ret), "size", sizeof(Iterator));
      } else {
        iter = new(ptr) Iterator(*children_[i]);
        iter->set_iter_id(i);
        iter->set_parent(this);
        if (MY_SPEC.init_plan_idxs_.has_member(i)) {
          iter->set_init_plan();
          //init plan 移到get_next_row之后
        } else if (MY_SPEC.one_time_idxs_.has_member(i)) {
          iter->set_onetime_plan();
        } else if (!MY_SPEC.enable_px_batch_rescans_.empty() &&
            MY_SPEC.enable_px_batch_rescans_.at(i)) {
          // enable_left_px_batch_ = true;
        }
        if (!MY_SPEC.exec_param_idxs_inited_) {
          //unittest or old version, do not init hashmap
        } else if (OB_FAIL(iter->init_mem_entity())) {
          LOG_WARN("failed to init mem_entity", K(ret));
        } else if (MY_SPEC.exec_param_array_[i - 1].count() > 0) {
          //min of buckets is 16,
          //max will not exceed card of left_child and HASH_MAP_MEMORY_LIMIT/ObObj
          if (OB_FAIL(iter->init_hashmap(max(
                  16/*hard code*/, min(get_child(0)->get_spec().get_rows(),
                      iter->HASH_MAP_MEMORY_LIMIT / static_cast<int64_t>(sizeof(ObDatum))))))) {
            LOG_WARN("failed to init hash map for idx", K(i), K(ret));
          } else if (OB_FAIL(iter->init_probe_row(MY_SPEC.exec_param_array_[i - 1].count()))) {
            LOG_WARN("failed to init probe row", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (children_[i]->is_vectorized() && OB_FAIL(iter->init_batch_rows_holder(children_[i]->get_spec().output_, children_[i]->get_eval_ctx()))) {
            LOG_WARN("failed to init batch rows holder", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSubPlanFilterVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  max_group_size_ = OB_MAX_BULK_JOIN_ROWS;
  LOG_TRACE("max group size of SPF is", K(max_group_size_));
  if (OB_FAIL(drive_iter_.init(this,
                              max_group_size_,
                              &MY_SPEC.rescan_params_,
                              MY_SPEC.enable_das_group_rescan_,
                              false
                              ))) {
    LOG_WARN("failed to init drive iterator for SPF", KR(ret));
  } else if (OB_FAIL(init_subplan_iters())) {
    LOG_WARN("failed to init sub plan iters for SPF", K(ret));
  }
  return ret;
}

int ObSubPlanFilterVecOp::inner_close()
{
  destroy_subplan_iters();
  // destroy_update_set_mem();
  drive_iter_.reset();
  return OB_SUCCESS;
}



int ObSubPlanFilterVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t op_max_batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t params_size = 0;
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (need_init_before_get_row_) {
    OZ(prepare_onetime_exprs());
  }
 
  clear_evaluated_flag();
  DASGroupScanMarkGuard mark_guard(ctx_.get_das_ctx(), MY_SPEC.enable_das_group_rescan_);
  while (OB_SUCC(ret) && !iter_end_) {
    const ObBatchRows *child_brs = NULL;
    set_param_null();
    if (OB_FAIL(drive_iter_.get_next_left_batch(op_max_batch_size, child_brs))) {
      LOG_WARN("fail to get next batch", K(ret));
    } else if (child_brs->end_) {
      iter_end_ = true;
    }
    LOG_TRACE("child_brs size", K(child_brs->size_), K(spec_.id_));
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(child_brs->size_);
    brs_.size_ = child_brs->size_;
    bool all_filtered = true;
    brs_.skip_->deep_copy(*child_brs->skip_, child_brs->size_);
    clear_evaluated_flag();

    for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < child_brs->size_ && !iter_end_; l_idx++) {
      if (child_brs->skip_->exist(l_idx)) { continue; }
      guard.set_batch_idx(l_idx);
      if (OB_FAIL(drive_iter_.fill_cur_row_group_param())) {
        LOG_WARN("prepare rescan params failed", K(ret));
      } else {
        if (need_init_before_get_row_) {
          for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
            Iterator *&iter = subplan_iters_.at(i - 1);
            if (MY_SPEC.init_plan_idxs_.has_member(i)) {
              OZ(iter->prepare_init_plan());
            }
          }
          need_init_before_get_row_ = false;
        }
      }
      if (OB_SUCC(ret))  {
        bool filtered = false;
        if (OB_FAIL(filter_row_vector(eval_ctx_, MY_SPEC.filter_exprs_, *(child_brs->skip_), filtered))) {
          LOG_WARN("fail to filter row", K(ret), K(l_idx), K(spec_.id_));
        } else if (filtered) {
          brs_.skip_->set(l_idx);
          LOG_TRACE("left rows is filterd", K(l_idx), K(spec_.id_));
        } else {
          LOG_TRACE("left rows is not filterd", K(l_idx), K(spec_.id_));
          all_filtered = false;
          // ObDatum *datum = NULL;
          EvalBound eval_bound(eval_ctx_.get_batch_size(), l_idx, l_idx + 1, false);
          FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
            if (OB_FAIL((*e)->eval_vector(eval_ctx_, *(child_brs->skip_), eval_bound))) {
              LOG_WARN("expr evaluate failed", K(ret), K(*e));
            }
          }
        }
      }
    } // for end

    if (OB_SUCC(ret) && all_filtered) {
      reset_batchrows();
      continue;
    }
    FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
      (*e)->get_eval_info(eval_ctx_).projected_ = true;
    }
    break;
  } // end while

  if (OB_SUCC(ret) && iter_end_) {
    set_param_null();
  }
  return ret;
}

int ObSubPlanFilterVecOp::prepare_onetime_exprs()
{
  int ret = OB_SUCCESS;
  if (is_vectorized()) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(MY_SPEC.max_batch_size_);
    ret = prepare_onetime_exprs_inner();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SPF operator is not vectorized", K(ret));
  }

  for (int64_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    Iterator *iter = subplan_iters_.at(i - 1);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan_iter is null", K(ret));
    } else if (MY_SPEC.one_time_idxs_.has_member(i)) {
      iter->drain_exch();
    }
  }
  return ret;
}

int ObSubPlanFilterVecOp::prepare_onetime_exprs_inner()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.onetime_exprs_.count(); ++i) {
    char mock_skip_data[1] = {0};
    sql::ObBitVector &skip = *sql::to_bit_vector(mock_skip_data);
    const ObDynamicParamSetter &setter = MY_SPEC.onetime_exprs_.at(i);
    if (OB_FAIL(setter.set_dynamic_param_vec2(eval_ctx_, skip))) {
      LOG_WARN("failed to prepare onetime expr", K(ret), K(i));
    }
  }
  return ret;
}

} // end sql
} // oceanbase