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
#include "ob_raw_expr_pull_up_aggr_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"

/// interface of ObRawExprVisitor
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObRawExprPullUpAggrExpr::analyze(ObRawExpr& expr)
{
  return expr.postorder_replace(*this);
}

int ObRawExprPullUpAggrExpr::visit(ObConstRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}

int ObRawExprPullUpAggrExpr::visit(ObVarRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}

int ObRawExprPullUpAggrExpr::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}
int ObRawExprPullUpAggrExpr::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}
int ObRawExprPullUpAggrExpr::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}
int ObRawExprPullUpAggrExpr::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}

int ObRawExprPullUpAggrExpr::visit(ObAggFunRawExpr& expr)
{

  int ret = OB_SUCCESS;

  // For avg(), the expr will be created later by itself
  if (T_FUN_AVG != expr.get_expr_type() && T_FUN_APPROX_COUNT_DISTINCT != expr.get_expr_type()) {
    ObAggFunRawExpr* aggr_expr = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(expr.get_expr_type(), aggr_expr))) {
      LOG_WARN("failed to create new expresssion", K(ret));
    } else if (OB_ISNULL(new_expr_ = aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_expr_ is null", K(ret));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    switch (expr.get_expr_type()) {
      case T_FUN_GROUPING:
        new_expr_->set_expr_type(expr.get_expr_type());
        if (1 == expr.get_real_param_count()) {
          ObRawExpr* real_group_by_expr = (expr.get_real_param_exprs()).at(0);
          if (OB_FAIL(static_cast<ObAggFunRawExpr*>(new_expr_)->add_real_param_expr(real_group_by_expr))) {
            LOG_PRINT_EXPR(WARN, "failed to add real param expr", &expr, K(ret));
          } else if (OB_FAIL(new_expr_->formalize(session_info_))) {
            LOG_PRINT_EXPR(WARN, "failed to formalized expr", new_expr_, K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the grouping must has only one real param expr", K(expr.get_real_param_count()));
        }
        break;
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_SUM:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        // MAX(a) -> MAX(MAX(a)), MIN(a) -> MIN(MIN(a)), SUM(a) -> SUM(SUM(a))

        // same type
        new_expr_->set_expr_type(expr.get_expr_type());
        // if (expr.is_param_distinct()) {
        //  new_expr_->set_param_distinct();
        //}
        if (OB_FAIL(static_cast<ObAggFunRawExpr*>(new_expr_)->add_real_param_expr(&expr))) {
          LOG_PRINT_EXPR(WARN, "failed to add real param expr", &expr, K(ret));
        } else if (OB_FAIL(new_expr_->formalize(session_info_))) {
          LOG_PRINT_EXPR(WARN, "failed to formalized expr", new_expr_, K(ret));
        }
      } break;
      case T_FUN_COUNT_SUM: {
        new_expr_->set_expr_type(T_FUN_COUNT_SUM);
        if (OB_FAIL(static_cast<ObAggFunRawExpr*>(new_expr_)->add_real_param_expr(&expr))) {
          LOG_PRINT_EXPR(WARN, "failed to add real param expr", &expr, K(ret));
        } else if (OB_FAIL(new_expr_->formalize(session_info_))) {
          LOG_PRINT_EXPR(WARN, "failed to formalized expr", new_expr_, K(ret));
        }
      } break;
      case T_FUN_GROUP_CONCAT:
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_AVG:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_STDDEV:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_VARIANCE:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_WM_CONCAT: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Aggregation is not supported.", K(ret));
      } break;
      case T_FUN_COUNT: {
        // COUNT(a) -> SUM(COUNT(*))
        new_expr_->set_expr_type(T_FUN_COUNT_SUM);
        // static_cast<ObAggFunRawExpr *>(new_expr_)->set_param_expr(&expr);
        // if (OB_FAIL(new_expr_->formalize())) {
        if (OB_FAIL(static_cast<ObAggFunRawExpr*>(new_expr_)->add_real_param_expr(&expr))) {
          LOG_PRINT_EXPR(WARN, "failed to add real param expr", &expr, K(ret));
        } else if (OB_FAIL(new_expr_->formalize(session_info_))) {
          LOG_PRINT_EXPR(WARN, "failed to formalized expr", new_expr_, K(ret));
        }
      } break;
      case T_FUN_AVG: {
        /*
         *  OK, this is tricky... what we want is the following
         *
         *          AVG(c1)   --->      SUM(SUM(c1)) / SUM(COUNT(c1))
         *                                           |
         *                                   SUM(c1), COUNT(c1)
         *
         *  Both the parent and child operator are pointed to the same operator and at
         *  the child group-by, we change it to
         *
         *    parent                            child(SUM(c1), COUNT(c1))
         *         \                           /
         *          \                         /
         *          AVG(SUM(C1), COUNT(C1))
         *                ^         ^
         *                |         |
         *          first param   second parm
         *
         *  And then at the parent we create SUM(SUM(c1)) / SUM(COUNT(c1))
         */

        // this is not true for regular AVG
        if (NULL == expr.get_push_down_sum_expr()) {
          LOG_PRINT_EXPR(DEBUG, "AVG function is not pushed down", &expr, "param_count", expr.get_param_count());
        } else {
          ObOpRawExpr* op_expr = NULL;
          if (OB_FAIL(expr_factory_.create_raw_expr(T_OP_AGG_DIV, op_expr))) {
            LOG_WARN("failed to create div op to replace average", K(ret));
          } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SUM, sum_sum_))) {
            LOG_WARN("create raw expr failed", K(ret));
          } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SUM, sum_count_))) {
            LOG_WARN("create sum count failed", K(ret));
          } else if (OB_ISNULL(new_expr_ = op_expr) || OB_ISNULL(sum_sum_) || OB_ISNULL(sum_count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K_(new_expr), K_(sum_sum), K_(sum_count));
          } else {
            // set properties
            if (OB_FAIL(new_expr_->add_flag(IS_AGG))) {  // this is to silence CG
              LOG_WARN("failed to add flag IS_AGG", K(ret));
            } else if (OB_FAIL(sum_sum_->add_flag(IS_NEW_AGG_EXPR))) {
              LOG_WARN("failed to add flag IS_NEW_AGG_EXPR", K(ret));
            } else if (OB_FAIL(sum_count_->add_flag(IS_NEW_AGG_EXPR))) {
              LOG_WARN("failed to add flag IS_NEW_AGG_EXPR", K(ret));
            } else {
            }
            if (OB_FAIL(ret)) {
              // do nothing
              // SUM(SUM(c1))
            } else if (OB_FAIL(sum_sum_->add_real_param_expr(expr.get_push_down_sum_expr()))) {
              LOG_PRINT_EXPR(WARN, "failed to add real param expr", expr.get_push_down_sum_expr(), K(ret));
            }
            // SUM(COUNT(c1))
            else if (OB_FAIL(sum_count_->add_real_param_expr(expr.get_push_down_count_expr()))) {
              LOG_PRINT_EXPR(WARN, "failed to add real param expr", expr.get_push_down_count_expr(), K(ret));
            } else {
              ObOpRawExpr* new_expr = static_cast<ObOpRawExpr*>(new_expr_);
              if (OB_FAIL(new_expr->set_param_exprs(sum_sum_, sum_count_))) {
                LOG_WARN("failed to set DIV params", K(ret));
              } else if (OB_FAIL(new_expr->formalize(session_info_))) {
                LOG_WARN("failed to formalize div operator", K(ret));
              } else {
                LOG_PRINT_EXPR(DEBUG, "succ to set DIV params", new_expr_, K(ret));
                LOG_PRINT_EXPR(DEBUG, "succ to set sum_sum params", sum_sum_, K(ret));
                LOG_PRINT_EXPR(DEBUG, "succ to set sum_count params", sum_count_, K(ret));
              }
            }
          }
        }
      } break;
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
        /**
         * APPROX_COUNT_DISTINCT_SYNOPSIS(c1,c2) ---> APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE(...)
         *                                                               |
         *                                              APPROX_COUNT_DISTINCT_SYNOPSIS(c1,c2)
         */
        new_expr_->set_expr_type(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE);
        if (OB_FAIL(
                static_cast<ObAggFunRawExpr*>(new_expr_)->add_real_param_expr(expr.get_push_down_synopsis_expr()))) {
          LOG_PRINT_EXPR(WARN, "failed to add real param expr", expr.get_push_down_synopsis_expr(), K(ret));
        } else if (OB_FAIL(new_expr_->formalize(session_info_))) {
          LOG_PRINT_EXPR(WARN, "failed to formalized expr", new_expr_, K(ret));
        }
      } break;
      case T_FUN_APPROX_COUNT_DISTINCT: {
        /**
         * APPROX_COUNT_DISTINCT(c1,c2) ---> ESTIMATE_NDV(APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE(...))
         *                                                                |
         *                                               APPROX_COUNT_DISTINCT_SYNOPSIS(c1,c2)
         */
        ObSysFunRawExpr* sys_fun_expr = NULL;
        ObString func_name;

        if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_ESTIMATE_NDV, sys_fun_expr))) {
          LOG_WARN("failed to create estimate_div to replace approx_count_distinct", K(ret));
        } else if (OB_FAIL(ob_write_string(expr_factory_.get_allocator(), "ESTIMATE_NDV", func_name))) {
          LOG_WARN("failed to allocate string", K(ret));
        } else if (OB_FAIL(
                       expr_factory_.create_raw_expr(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, merge_synopsis_))) {
          LOG_WARN("failed to create approx_count_distinct_merge", K(ret));
        } else if (OB_ISNULL(new_expr_ = sys_fun_expr) || OB_ISNULL(merge_synopsis_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K_(new_expr), K_(merge_synopsis));
        } else {
          // set function name
          sys_fun_expr->set_func_name(func_name);
          // set properties
          if (OB_FAIL(new_expr_->add_flag(IS_AGG))) {  // this is to silence CG
            LOG_WARN("failed to add flag IS_AGG", K(ret));
          } else if (OB_FAIL(merge_synopsis_->add_flag(IS_NEW_AGG_EXPR))) {
            LOG_WARN("failed to add flag IS_NEW_AGG_EXPR", K(ret));
          } else {
            // do nothing
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(merge_synopsis_->add_real_param_expr(expr.get_push_down_synopsis_expr()))) {
            LOG_PRINT_EXPR(WARN, "failed to add real param expr", expr.get_push_down_synopsis_expr(), K(ret));
          } else {
            ObSysFunRawExpr* new_expr = static_cast<ObSysFunRawExpr*>(new_expr_);
            if (OB_FAIL(new_expr->set_param_expr(merge_synopsis_))) {
              LOG_WARN("failed to set esitmate_ndv param", K(ret));
            } else if (OB_FAIL(new_expr->formalize(session_info_))) {
              LOG_WARN("failed to formalize estimate_ndv function", K(ret));
            } else {
              // do nothing
            }
          }
        }
      } break;
      default:
        break;
    }

    if (OB_SUCC(ret) && OB_LIKELY(NULL != new_expr_)) {
      if (OB_FAIL(new_expr_->add_flag(IS_NEW_AGG_EXPR))) {
        LOG_WARN("failed to add flag IS_NEW_AGG_EXPR", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprPullUpAggrExpr::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  // not sure what to do
  new_expr_ = &expr;
  return ret;
}

int ObRawExprPullUpAggrExpr::visit(ObSetOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  new_expr_ = &expr;
  return ret;
}
