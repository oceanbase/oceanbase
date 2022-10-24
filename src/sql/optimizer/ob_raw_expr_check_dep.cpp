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
#include "ob_raw_expr_check_dep.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "ob_optimizer_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObRawExprCheckDep::check_expr(const ObRawExpr &expr, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  int64_t idx = OB_INVALID;
  if (ObOptimizerUtil::find_item(dep_exprs_, &expr, &idx) && OB_INVALID_INDEX != idx) {
    if (OB_ISNULL(dep_indices_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dep_indices_ is NULL", K(ret));
    } else if (!dep_indices_->has_member(idx)) {
      if (OB_FAIL(dep_indices_->add_member(idx))) {
        LOG_WARN("failed to add member", K(ret));
      }
    } else {}
    // mark as found
    found = true;
  }
  return ret;
}

int ObRawExprCheckDep::check(const ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    switch (expr.get_expr_class()) {
    case ObRawExpr::EXPR_CONST:
    case ObRawExpr::EXPR_EXEC_PARAM:
    case ObRawExpr::EXPR_PSEUDO_COLUMN:
    case ObRawExpr::EXPR_OP_PSEUDO_COLUMN: {
      if (OB_FAIL(check_expr(expr, found))) {
        LOG_WARN("failed to check expr", K(expr), K(ret));
      }
      break;
    }
    case ObRawExpr::EXPR_COLUMN_REF: {
      if (OB_FAIL(check_expr(expr, found))) {
        LOG_WARN("failed to check expr", K(expr), K(ret));
      } else if (found && !is_access_) {
        /*do nothing*/
      } else {
        const ObColumnRefRawExpr *column = static_cast<const ObColumnRefRawExpr*>(&expr);
        if (column->is_generated_column()) {
          if (OB_ISNULL(column->get_dependant_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("virtual generated column has NULL dependant expr", K(expr), K(ret));
          } else if (OB_FAIL(SMART_CALL(check(*column->get_dependant_expr())))) {
            LOG_WARN("failed to check expr", K(expr), K(ret));
          } else { /*do nothing*/ }
        }
      }
      break;
    }
    case ObRawExpr::EXPR_QUERY_REF:
    case ObRawExpr::EXPR_SYS_FUNC:
    case ObRawExpr::EXPR_UDF:
    case ObRawExpr::EXPR_AGGR:
    case ObRawExpr::EXPR_OPERATOR:
    case ObRawExpr::EXPR_SET_OP:
    case ObRawExpr::EXPR_CASE_OPERATOR:
    case ObRawExpr::EXPR_WINDOW: {
      if (OB_FAIL(check_expr(expr, found))) {
        LOG_WARN("failed to check expr", K(expr), K(ret));
      } else if (!found) {
        /*
         * We only need to check child exprs if the parent cannot be produced directly
         */
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
          if (OB_ISNULL(expr.get_param_expr(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get_param_expr is NULL", K(i), K(ret));
          } else if (OB_FAIL(SMART_CALL(check(*expr.get_param_expr(i))))) {
            LOG_WARN("failed to check expr", K(*expr.get_param_expr(i)), K(ret));
          } else {}
        }
      }
      break;
    }
    default:
      break;
    }
  }
  return ret;
}

int ObRawExprCheckDep::check(const ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(check(*exprs.at(i)))) {
      LOG_WARN("failed to check exprs", K(ret));
    }
  }
  return ret;
}

