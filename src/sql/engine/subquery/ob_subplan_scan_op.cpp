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

#include "ob_subplan_scan_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObSubPlanScanSpec::ObSubPlanScanSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), projector_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObSubPlanScanSpec, ObOpSpec), projector_);


ObSubPlanScanOp::ObSubPlanScanOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input)
{
}

int ObSubPlanScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no child", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.projector_.count() % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("projector array size should be multiples of 2", K(ret));
  } else if (OB_FAIL(init_monitor_info())) {
    LOG_WARN("init monitor info failed", K(ret));
  }
  return ret;
}

int ObSubPlanScanOp::inner_rescan()
{
  return ObOperator::inner_rescan();
}

int ObSubPlanScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from child failed", K(ret));
    }
  } else {
    // eval child's output expr
    // For some expression in the subquery, we must eval, even if it not output.
    // e.g.
    //      select 1 from (select @a=3);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_->get_spec().output_.count(); i++) {
      ObExpr *expr = child_->get_spec().output_[i];
      ObDatum *datum = NULL;
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expr evaluate failed", K(ret), K(*expr));
      }
    }


    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.projector_.count(); i += 2) {
      ObExpr *from = MY_SPEC.projector_[i];
      ObExpr *to = MY_SPEC.projector_[i + 1];
      ObDatum *datum = NULL;
      if (OB_FAIL(from->eval(eval_ctx_, datum))) {
        LOG_WARN("expr evaluate failed", K(ret), K(*from));
      } else {
        to->locate_expr_datum(eval_ctx_) = *datum;
        to->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObSubPlanScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  return MY_SPEC.use_rich_format_ ? next_vector(max_row_cnt) : next_batch(max_row_cnt);
}

int ObSubPlanScanOp::next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    LOG_WARN("get child next batch failed", K(ret));
  } else if (child_brs->end_ && 0 == child_brs->size_) {
    brs_.copy(child_brs);
  } else {
    brs_.copy(child_brs);
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.projector_.count(); i += 2) {
      ObExpr *from = MY_SPEC.projector_[i];
      ObExpr *to = MY_SPEC.projector_[i + 1];
      if (OB_FAIL(from->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        ObDatum *from_datums = from->locate_batch_datums(eval_ctx_);
        ObDatum *to_datums = to->locate_batch_datums(eval_ctx_);
        const ObEvalInfo &from_info = from->get_eval_info(eval_ctx_);
        ObEvalInfo &to_info = to->get_eval_info(eval_ctx_);
        if (OB_UNLIKELY(!to->is_batch_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("output of subplan scan should be batch result", K(ret), KPC(to));
        } else if (from->is_batch_result()) {
          MEMCPY(to_datums, from_datums, brs_.size_ * sizeof(ObDatum));
          to_info = from_info;
          to_info.projected_ = true;
          to_info.point_to_frame_ = false;
        } else {
          for (int64_t j = 0; j < brs_.size_; j++) {
            to_datums[j] = *from_datums;
          }
          to_info = from_info;
          to_info.projected_ = true;
          to_info.point_to_frame_ = false;
          to_info.cnt_ = brs_.size_;
        }
      }
    }
  }
  return ret;
}

int ObSubPlanScanOp::next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("subplan scan next vector");
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    LOG_WARN("get child next batch failed", K(ret));
  } else if (child_brs->end_ && 0 == child_brs->size_) {
    brs_.copy(child_brs);
  } else {
    brs_.copy(child_brs);
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.projector_.count(); i += 2) {
      ObExpr *from = MY_SPEC.projector_[i];
      ObExpr *to = MY_SPEC.projector_[i + 1];
      if (OB_FAIL(from->eval_vector(eval_ctx_, brs_))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx_);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx_);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx_))->get_datums();
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, brs_.size_));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx_));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < brs_.size_ && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx_));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx_);
          if (src != dst) {
            MEMCPY(dst, src, brs_.size_ * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, brs_.size_));
        } else {
          to_vec_header = from_vec_header;
        }
        // init eval info
        if (OB_SUCC(ret)) {
          const ObEvalInfo &from_info = from->get_eval_info(eval_ctx_);
          ObEvalInfo &to_info = to->get_eval_info(eval_ctx_);
          to_info = from_info;
          to_info.projected_ = true;
          to_info.cnt_ = brs_.size_;
        }
      }
    } // for end
  }

  return ret;
}

int ObSubPlanScanOp::init_monitor_info()
{
  int ret = OB_SUCCESS;
  if (spec_.plan_->get_phy_plan_hint().monitor_) {
    ObPhysicalPlanCtx *plan_ctx = NULL;
    const ObPhysicalPlan *phy_plan = nullptr;
    if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialized exec ctx without phy plan ctx set. Unexpected", K(ret));
    } else if (OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, phy plan must not be nullptr", K(ret));
    } else if (phy_plan->get_ddl_task_id() > 0) {
      op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
      op_monitor_info_.otherstat_5_value_ = phy_plan->get_ddl_task_id();
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
