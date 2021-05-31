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
#include "sql/engine/expr/ob_expr_prior.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"

namespace oceanbase {
using namespace common;
using namespace sql;
namespace sql {

ObExprPrior::ObExprPrior(ObIAllocator& alloc) : ObExprOperator(alloc, T_OP_PRIOR, N_NEG, 1, NOT_ROW_DIMENSION){};

int ObExprPrior::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type = type1;
  int ret = OB_SUCCESS;
  return ret;
}

int ObExprPrior::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Got a null phy plan ctx", K(ret));
  } else {
    ObExecContext& exec_ctx = *expr_ctx.exec_ctx_;
    ObPhyOperator::ObPhyOperatorCtx* op_ctx = NULL;
    ObConnectByBase::ObConnectByBaseCtx* connect_by_ctx = NULL;
    int64_t level = 0;
    if (OB_ISNULL(
            op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperator::ObPhyOperatorCtx, exec_ctx, expr_ctx.phy_operator_ctx_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get nested loop connect by ctx", K(ret));
    } else if (PHY_NESTED_LOOP_CONNECT_BY != op_ctx->get_op_type() &&
               PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX != op_ctx->get_op_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys connect path can only generate by nestloop connect by join", K(ret), K(op_ctx->get_op_type()));
    } else if (FALSE_IT(connect_by_ctx = static_cast<ObConnectByBase::ObConnectByBaseCtx*>(op_ctx))) {
    } else if (FALSE_IT(level = connect_by_ctx->get_current_level())) {
    } else {
      result = obj;
      if (1 == level) {
        result.set_null();
      }
    }
  }
  return ret;
}
int ObExprPrior::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg num", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = calc_prior_expr;
  }
  return ret;
}

int ObExprPrior::calc_prior_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  uint64_t operator_id = expr.extra_;
  ObDatum* arg_datum = nullptr;
  ObOperatorKit* kit = ctx.exec_ctx_.get_operator_kit(operator_id);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is NULL", K(ret), K(operator_id), KP(kit));
  } else if (OB_UNLIKELY(PHY_NESTED_LOOP_CONNECT_BY != kit->op_->get_spec().type_ &&
                         PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX != kit->op_->get_spec().type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not connect by operator", K(ret), K(operator_id), "spec", kit->op_->get_spec());
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("failed to eval expr", K(ret));
  } else {
    int64_t level = 0;
    if (PHY_NESTED_LOOP_CONNECT_BY == kit->op_->get_spec().type_) {
      ObNLConnectByOp* cnntby_op = static_cast<ObNLConnectByOp*>(kit->op_);
      level = cnntby_op->connect_by_pump_.get_current_level();
    } else {
      ObNLConnectByWithIndexOp* cnntby_op = static_cast<ObNLConnectByWithIndexOp*>(kit->op_);
      level = cnntby_op->connect_by_pump_.get_current_level();
    }
    if (1 == level) {
      res.set_null();
    } else {
      res.set_datum(*arg_datum);
    }
    LOG_DEBUG("trace prior level", K(level), K(res));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
