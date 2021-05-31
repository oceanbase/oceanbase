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
#include "sql/engine/join/ob_basic_nested_loop_join.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObBasicNestedLoopJoin::ObBasicNestedLoopJoin(common::ObIAllocator& alloc)
    : ObJoin(alloc), rescan_params_(alloc), left_scan_index_(alloc), is_inner_get_(false), is_self_join_(false)
{
  // TODO Auto-generated constructor stub
}

ObBasicNestedLoopJoin::~ObBasicNestedLoopJoin()
{
  // TODO Auto-generated destructor stub
}

void ObBasicNestedLoopJoin::reset()
{
  ObJoin::reset();
  left_scan_index_.reset();
  rescan_params_.reset();
  is_inner_get_ = false;
  is_self_join_ = false;
}
void ObBasicNestedLoopJoin::reuse()
{
  ObJoin::reuse();
  left_scan_index_.reuse();
  rescan_params_.reuse();
  is_inner_get_ = false;
  is_self_join_ = false;
}

int ObBasicNestedLoopJoin::prepare_rescan_params(ObBasicNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjParam res_obj;
  ObPhysicalPlanCtx* plan_ctx = join_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(join_ctx.left_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else {
    int64_t param_cnt = rescan_params_.count();
    const ObSqlExpression* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      if (OB_ISNULL(expr = rescan_params_.at(i).expr_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("rescan param expr is null", K(ret), K(i));
      } else if (OB_FAIL(expr->calc(join_ctx.expr_ctx_, *join_ctx.left_row_, res_obj))) {
        LOG_WARN("failed to calc expr for rescan param", K(ret), K(i));
      } else {
        int64_t idx = rescan_params_.at(i).param_idx_;
        res_obj.set_param_meta();
        plan_ctx->get_param_store_for_update().at(idx) = res_obj;
        LOG_DEBUG("prepare_rescan_params",
            K(ret),
            K(i),
            K(res_obj),
            K(idx),
            K(expr),
            K(plan_ctx),
            K(*join_ctx.left_row_),
            K(*expr),
            K(join_ctx.expr_ctx_.phy_plan_ctx_));
      }
    }
  }

  // try to open the right child
  if (OB_SUCC(ret)) {
    if (OB_FAIL(open_right_child(join_ctx))) {
      LOG_WARN("failed to open right child", K(ret));
    }
  }

  return ret;
}

int ObBasicNestedLoopJoin::get_next_left_row(ObJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (!rescan_params_.empty()) {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    ObPhysicalPlanCtx* plan_ctx = static_cast<ObBasicNestedLoopJoinCtx&>(join_ctx).exec_ctx_.get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan context is null", K(ret));
    } else {
      ParamStore& param_store = plan_ctx->get_param_store_for_update();
      FOREACH_CNT(param, rescan_params_)
      {
        param_store.at(param->param_idx_).set_null();
      }
    }
  }
  return OB_SUCCESS != ret ? ret : ObJoin::get_next_left_row(join_ctx);
}

int ObBasicNestedLoopJoin::open_right_child(ObBasicNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_ptr = nullptr;
  ObPhyOperatorCtx* child_op_ctx = nullptr;
  if (join_ctx.open_right_child()) {
    // do nothing
  } else if (OB_ISNULL(child_ptr = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get right child", K(ret));
  } else if (OB_NOT_NULL(
                 child_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, join_ctx.exec_ctx_, child_ptr->get_id()))) {
    // the drain stage will open this operator, and no record in join_ctx.
    join_ctx.set_open_right_child();
  } else if (OB_FAIL(open_child(join_ctx.exec_ctx_, SECOND_CHILD))) {
    LOG_WARN("fail to open right child", K(ret));
  } else {
    join_ctx.set_open_right_child();
  }
  return ret;
}

int ObBasicNestedLoopJoin::open_left_child(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_child(ctx, FIRST_CHILD))) {
    LOG_WARN("fail to open left child", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoin::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoin::inner_open(ctx))) {
    LOG_WARN("failed to inner open join", K(ret));
  } else if (OB_FAIL(open_left_child(ctx))) {
    LOG_WARN("failed to open left child", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoin::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int child_ret = OB_SUCCESS;
  // first call close of all children
  ObPhyOperator* child_ptr = NULL;
  for (int32_t i = 0; i < get_child_num(); ++i) {
    if (OB_ISNULL(child_ptr = get_child(i))) {
      ret = OB_NOT_INIT;
      LOG_WARN("failed to get child",
          K(i),
          K(child_ptr),
          K(get_child_num()),
          K(ret),
          "op_type",
          ob_phy_operator_type_str(get_type()));
    } else {
      int tmp_ret = child_ptr->close(ctx);
      if (OB_SUCCESS != tmp_ret) {
        child_ret = tmp_ret;
        LOG_WARN("Close child operator failed", K(child_ret), "op_type", ob_phy_operator_type_str(get_type()));
      }
    }
  }

  // no matter what, must call operatoris close function, then close this operator
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    LOG_DEBUG("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    // op_ctx->op_monitor_info_.set_value(CLOSE_TIME, common::ObTimeUtility::current_time());
  }

  if (OB_SUCC(ret)) {
    // Can only preserve one error code
    ret = child_ret;
  }
  return ret;
}

int ObBasicNestedLoopJoin::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObBasicNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObBasicNestedLoopJoinCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get join ctx", K(ret));
  } else if (OB_FAIL(open_right_child(*join_ctx))) {
    LOG_WARN("failed to open right child", K(ret));
  } else if (OB_FAIL(ObJoin::rescan(ctx))) {
    LOG_WARN("failed to call parent rescan", K(ret));
  } else {
  }
  return ret;
}

OB_DEF_SERIALIZE(ObBasicNestedLoopJoin)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObBasicNestedLoopJoin, ObJoin));
  OB_UNIS_ENCODE(left_scan_index_);
  OB_UNIS_ENCODE(rescan_params_);
  OB_UNIS_ENCODE(is_inner_get_);
  OB_UNIS_ENCODE(is_self_join_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicNestedLoopJoin)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObBasicNestedLoopJoin, ObJoin));
  OB_UNIS_DECODE(left_scan_index_);
  // set physical plan before deserialize param
  // OB_UNIS_ENCODE(rescan_params_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    OB_UNIS_DECODE(count);
    rescan_params_.reset();
    rescan_params_.init(count);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      RescanParam item;
      item.my_phy_plan_ = my_phy_plan_;
      if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
        LOG_WARN("failed to decode rescan_param", K(ret));
      } else if (OB_SUCCESS != (ret = rescan_params_.push_back(item))) {
        LOG_WARN("failed to push rescan_param", K(ret));
      }
    }  // end for
  }    // end if
  OB_UNIS_DECODE(is_inner_get_);
  OB_UNIS_DECODE(is_self_join_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicNestedLoopJoin)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObBasicNestedLoopJoin, ObJoin));
  OB_UNIS_ADD_LEN(left_scan_index_);
  OB_UNIS_ADD_LEN(rescan_params_);
  OB_UNIS_ADD_LEN(is_inner_get_);
  OB_UNIS_ADD_LEN(is_self_join_);
  return len;
}

OB_DEF_SERIALIZE(ObBasicNestedLoopJoin::RescanParam)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("expr is null", K(ret));
  } else {
    OB_UNIS_ENCODE(*expr_);
    OB_UNIS_ENCODE(param_idx_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicNestedLoopJoin::RescanParam)
{
  int ret = OB_SUCCESS;
  expr_ = NULL;
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("my phy plan is null", K(ret));
  } else if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr_))) {
    LOG_WARN("make sql expr failed", K(ret));
  } else if (OB_ISNULL(expr_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to make sql expr", K(ret));
  } else if (OB_FAIL(expr_->deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize expr failed", K(ret));
  } else {
    OB_UNIS_DECODE(param_idx_);
  }
  if (OB_SUCCESS != ret && NULL != expr_) {
    expr_ = NULL;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicNestedLoopJoin::RescanParam)
{
  int64_t len = 0;
  if (!OB_ISNULL(expr_)) {
    OB_UNIS_ADD_LEN(*expr_);
  }
  OB_UNIS_ADD_LEN(param_idx_);
  return len;
}

} /* namespace sql */
} /* namespace oceanbase */
