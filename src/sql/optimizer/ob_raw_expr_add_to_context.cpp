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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_raw_expr_add_to_context.h"

/// interface of ObRawExprVisitor
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObRawExprAddToContext::add_to_context(ObRawExpr &expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprAddToContext::visit(ObConstRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprAddToContext::visit(ObVarRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprAddToContext::visit(ObOpPseudoColumnRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprAddToContext::visit(ObQueryRefRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObPlQueryRefRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObColumnRefRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.has_generalized_column() && T_OP_ROW != expr.get_expr_type()) {
    ret = add_expr(expr);
  }
  return ret;
}

int ObRawExprAddToContext::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.has_generalized_column()) {
    ret = add_expr(expr);
  }
  return ret;
}

int ObRawExprAddToContext::visit(ObAggFunRawExpr &expr)
{
//  /* When we recursively add the expressions to the list, it is because the expression
//     can only be produced by us - ie. the aggregation function of group by. In this case
//     we would want to skip adding the aggregation funtion to the request list */
//  int ret = OB_SUCCESS;
////  UNUSED(expr);
////  return ret;
//  if (expr.has_flag(CNT_COLUMN) || IS_COUNT_STAR(&expr)) {
//    return add_expr(expr);
//  }
//
//  return ret;
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.has_generalized_column() || expr.has_flag(CNT_RAND_FUNC)) {
    ret = add_expr(expr);
  }
  return ret;
}

int ObRawExprAddToContext::visit(ObSetOpRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObWinFunRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObPseudoColumnRawExpr &expr)
{
  return add_expr(expr);
}

int ObRawExprAddToContext::visit(ObMatchFunRawExpr &expr)
{
  return add_expr(expr);
}

/**
 *  TODO(jiuman): the complexity of the algorithm used in add_expr is quite high. *  We may need to revisit it later if it turns out to be an optimization we have
 *  to do.
 */
int ObRawExprAddToContext::add_expr(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ExprProducer *existing_producer = NULL;
    if (!ObOptimizerUtil::find_expr(ctx_, expr, existing_producer)) {
      ExprProducer expr_producer(&expr, consumer_id_);
      if (OB_FAIL(ctx_->push_back(expr_producer))) {
        LOG_PRINT_EXPR(WARN, "failed to add expr to request list", &expr,
                       "# of expr", ctx_->count());
      } else {
        LOG_PRINT_EXPR(DEBUG, "add expr to request list",  &expr,
                       "# of expr", ctx_->count());
      }
    } else if (OB_ISNULL(existing_producer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("existing_producer is unexpected null", K(ret));
    } else {
      //if we need the same expr below the predetermined producer
      //restore predetermined_producer_ so it can be produced here
      LOG_TRACE("restore predetermined_producer", K(existing_producer->expr_),
                K(existing_producer->producer_id_), K(existing_producer->consumer_id_));
      existing_producer->producer_id_ = OB_INVALID_ID;
    }
  }

  return ret;
}
