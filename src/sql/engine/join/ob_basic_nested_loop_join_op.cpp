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

#include "sql/engine/join/ob_basic_nested_loop_join_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObBasicNestedLoopJoinSpec, ObJoinSpec),
                    rescan_params_,
                    gi_partition_id_expr_,
                    enable_gi_partition_pruning_,
                    enable_px_batch_rescan_);

ObBasicNestedLoopJoinOp::ObBasicNestedLoopJoinOp(ObExecContext &exec_ctx,
                                                 const ObOpSpec &spec,
                                                 ObOpInput *input)
  : ObJoinOp(exec_ctx, spec, input)
  {}

int ObBasicNestedLoopJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to inner open join", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::inner_rescan())) {
    LOG_WARN("failed to call parent rescan", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoinOp::inner_close()
{
  return OB_SUCCESS;
}


int ObBasicNestedLoopJoinOp::get_next_left_row()
{
  if (!get_spec().rescan_params_.empty()) {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    set_param_null();
  }
  return ObJoinOp::get_next_left_row();
}

int ObBasicNestedLoopJoinOp::prepare_rescan_params(bool is_group)
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = get_spec().rescan_params_.count();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  CK(OB_NOT_NULL(plan_ctx));
  ObObjParam *param = NULL;
  sql::ObTMArray<common::ObObjParam> params;
  common::ObSArray<int64_t> param_idxs;
  common::ObSArray<int64_t> param_expr_idxs;
  ObBatchRescanCtl *batch_rescan_ctl = (get_spec().enable_px_batch_rescan_ && is_group)
      ? &static_cast<ObNestedLoopJoinOp*>(this)->batch_rescan_ctl_
      : NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
    if (OB_FAIL(rescan_param.set_dynamic_param(eval_ctx_, param))) {
      LOG_WARN("fail to set dynamic param", K(ret));
    } else if (NULL != batch_rescan_ctl) {
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param", K(ret));
      } else {
        ObObjParam copy_res;
        int64_t expr_idx = 0;
        OZ(batch_rescan_ctl->params_.deep_copy_param(*param, copy_res));
        OZ(params.push_back(copy_res));
        OZ(param_idxs.push_back(rescan_param.param_idx_));
        CK(OB_NOT_NULL(plan_ctx->get_phy_plan()));
        OZ(plan_ctx->get_phy_plan()->get_expr_frame_info().get_expr_idx_in_frame(
            rescan_param.dst_, expr_idx));
        OZ(param_expr_idxs.push_back(expr_idx));
        param = NULL;
      }
    }
  }
  if (OB_SUCC(ret) && NULL != batch_rescan_ctl) {
    batch_rescan_ctl->param_version_ += 1;
    OZ(batch_rescan_ctl->params_.append_batch_rescan_param(param_idxs, params, param_expr_idxs));
  }

  // 左边每一行出来后，去通知右侧 GI 实施 part id 过滤，避免 PKEY NLJ 场景下扫不必要分区
  if (OB_SUCC(ret) && !get_spec().enable_px_batch_rescan_ && get_spec().enable_gi_partition_pruning_) {
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

void ObBasicNestedLoopJoinOp::set_param_null()
{
  set_pushdown_param_null(get_spec().rescan_params_);
}

} // end namespace sql
} // end namespace oceanbase
