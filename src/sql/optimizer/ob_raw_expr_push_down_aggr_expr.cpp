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
#include "sql/optimizer/ob_raw_expr_push_down_aggr_expr.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"

/// interface of ObRawExprVisitor
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObRawExprPushDownAggrExpr::analyze(ObRawExpr& expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprPushDownAggrExpr::visit(ObConstRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPushDownAggrExpr::visit(ObVarRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPushDownAggrExpr::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}
int ObRawExprPushDownAggrExpr::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}
int ObRawExprPushDownAggrExpr::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}
int ObRawExprPushDownAggrExpr::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}
int ObRawExprPushDownAggrExpr::visit(ObAggFunRawExpr& expr)
{

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 != new_exprs_.count())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Nested avg push-down is not supported at the moment", "# of exprs", new_exprs_.count(), K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (T_FUN_AVG == expr.get_expr_type()) {
      ObAggFunRawExpr* sum = NULL;
      ObAggFunRawExpr* count = NULL;
      ObOpRawExpr* div_expr = NULL;
      if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SUM, sum))) {
        LOG_WARN("failed to create new expr", K(ret));
      } else if (OB_FAIL(new_exprs_.push_back(sum))) {
        LOG_WARN("failed to add new expr");
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_COUNT, count))) {
        LOG_WARN("failed to create new expr", K(ret));
      } else if (OB_FAIL(new_exprs_.push_back(count))) {
        LOG_WARN("failed to add new expr", K(ret));
      } else if (OB_FAIL(expr_factory_.create_raw_expr(T_OP_AGG_DIV, div_expr))) {
        LOG_WARN("failed to create div op to replace average", K(ret));
      } else if (OB_ISNULL(sum) || OB_ISNULL(count) || OB_ISNULL(div_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", KP(sum), KP(count), KP(div_expr), K(ret));
      } else {
        // set properties
        if (expr.is_param_distinct()) {
          sum->set_param_distinct(true);
          count->set_param_distinct(true);
        }
        if (1 != expr.get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_OPT_LOG(ERROR, "avg param count must be 1", K(ret), K(expr.get_param_count()));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
            if (OB_FAIL(sum->add_real_param_expr(expr.get_param_expr(i)))) {
              SQL_OPT_LOG(WARN, "fail to add real param expr", K(ret));
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
            if (OB_FAIL(count->add_real_param_expr(expr.get_param_expr(i)))) {
              SQL_OPT_LOG(WARN, "fail to add real param expr", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(div_expr->set_param_exprs(sum, count))) {
              LOG_WARN("failed to set DIV params", K(ret));
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(div_expr->formalize(session_info_))) {
          SQL_OPT_LOG(WARN, "fail to formalize div expr", K(ret));
        } else {
          // remember the newly generated exprs
          expr.get_push_down_sum_expr() = sum;
          expr.get_push_down_count_expr() = count;
          push_down_avg_expr_ = div_expr;
          LOG_PRINT_EXPR(DEBUG, "create new expressions for avg push-down", sum, "# of new exprs", new_exprs_.count());
          LOG_PRINT_EXPR(
              DEBUG, "create new expressions for avg push-down", count, "# of new exprs", new_exprs_.count());
        }
      }
    } else if (T_FUN_APPROX_COUNT_DISTINCT == expr.get_expr_type() ||
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == expr.get_expr_type()) {
      ObAggFunRawExpr* synopsis = NULL;
      if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, synopsis))) {
        LOG_WARN("failed to create new expr", K(ret));
      } else if (OB_FAIL(new_exprs_.push_back(synopsis))) {
        LOG_WARN("failed to add new expr");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
          if (OB_FAIL(synopsis->add_real_param_expr(expr.get_param_expr(i)))) {
            SQL_OPT_LOG(WARN, "fail to add real param expr", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          expr.get_push_down_synopsis_expr() = synopsis;
        }
      }
    } else {
      LOG_PRINT_EXPR(DEBUG, "Aggregation function push-down is supported", &expr, K(&expr));
    }
  }
  return ret;
}

int ObRawExprPushDownAggrExpr::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  // not sure what to do
  UNUSED(expr);
  return ret;
}

int ObRawExprPushDownAggrExpr::visit(ObSetOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}
