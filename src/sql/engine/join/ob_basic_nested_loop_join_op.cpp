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

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(
    (ObBasicNestedLoopJoinSpec, ObJoinSpec), rescan_params_, gi_partition_id_expr_, enable_gi_partition_pruning_);

ObBasicNestedLoopJoinOp::ObBasicNestedLoopJoinOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObJoinOp(exec_ctx, spec, input)
{}

int ObBasicNestedLoopJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t left_output_cnt = spec_.get_left()->output_.count();
  if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to inner open join", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoinOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::rescan())) {
    LOG_WARN("failed to call parent rescan", K(ret));
  }

  return ret;
}

int ObBasicNestedLoopJoinOp::inner_close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObBasicNestedLoopJoinOp::get_next_left_row()
{
  if (!get_spec().rescan_params_.empty()) {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    ParamStore& param_store = plan_ctx->get_param_store_for_update();
    FOREACH_CNT(param, get_spec().rescan_params_)
    {
      param_store.at(param->param_idx_).set_null();
      param->dst_->locate_expr_datum(eval_ctx_).set_null();
    }
  }
  return ObJoinOp::get_next_left_row();
}

int ObBasicNestedLoopJoinOp::prepare_rescan_params(bool is_group)
{
  int ret = OB_SUCCESS;
  UNUSED(is_group);
  int64_t param_cnt = get_spec().rescan_params_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter& rescan_param = get_spec().rescan_params_.at(i);
    if (OB_FAIL(rescan_param.set_dynamic_param(eval_ctx_))) {
      LOG_WARN("fail to set dynamic param", K(ret));
    }
  }

  if (OB_SUCC(ret) && get_spec().enable_gi_partition_pruning_) {
    ObDatum* datum = nullptr;
    if (OB_FAIL(get_spec().gi_partition_id_expr_->eval(eval_ctx_, datum))) {
      LOG_WARN("fail eval value", K(ret));
    } else {
      int64_t part_id = datum->get_int();
      ctx_.get_gi_pruning_info().set_part_id(part_id);
    }
  }

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
