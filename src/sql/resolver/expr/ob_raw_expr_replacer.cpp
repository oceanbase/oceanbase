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

#define USING_LOG_PREFIX SQL_RESV
#include "ob_raw_expr_replacer.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObRawExprReplacer::ObRawExprReplacer(ObRawExpr *old_expr,
                                     ObRawExpr *new_expr)
  : old_expr_(old_expr), new_expr_(new_expr)
{}

ObRawExprReplacer::~ObRawExprReplacer()
{}

int ObRawExprReplacer::replace(ObRawExpr &expr)
{
  return expr.preorder_accept(*this);
}

int ObRawExprReplacer::visit(ObConstRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    if (OB_ISNULL(expr.get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref expr is null", K(ret));
    } else if (expr.get_ref_expr() == old_expr_) {
      expr.set_ref_expr(new_expr_);
    } else if (OB_FAIL(expr.get_ref_expr()->preorder_accept(*this))) {
      LOG_WARN("failed to visit ref expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObVarRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObOpPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObQueryRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObPlQueryRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObColumnRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    int64_t count = expr.get_param_count();
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (expr.get_param_expr(i) == old_expr_) {
        ret = expr.replace_param_expr(i, new_expr_);
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_when_expr_size() != expr.get_then_expr_size()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (&expr != new_expr_) {
      if (expr.get_arg_param_expr() == old_expr_) {
        expr.set_arg_param_expr(new_expr_);
      }

      int64_t count = expr.get_when_expr_size();
      for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (expr.get_when_param_expr(i) == old_expr_) {
          ret = expr.replace_when_param_expr(i, new_expr_);
        }

        if (OB_SUCCESS == ret ) {
          if (expr.get_then_param_expr(i) == old_expr_) {
            ret = expr.replace_then_param_expr(i, new_expr_);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (expr.get_default_param_expr() == old_expr_) {
          expr.set_default_param_expr(new_expr_);
        }
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    ObIArray<ObRawExpr*> &real_param_exprs = expr.get_real_param_exprs_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
      if (real_param_exprs.at(i) == old_expr_) {
        real_param_exprs.at(i) = new_expr_;
      }
    }
    if (OB_SUCC(ret)) {
      ObIArray<OrderItem> &order_items = expr.get_order_items_for_update();
      for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
        if (order_items.at(i).expr_ == old_expr_) {
          order_items.at(i).expr_ = new_expr_;
        }
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    int64_t count = expr.get_param_count();
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (expr.get_param_expr(i) == old_expr_) {
        ret = expr.replace_param_expr(i, new_expr_);
      }
    }
  }
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObSetOpRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObWinFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    int64_t count = expr.get_param_count();
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (expr.get_param_expr(i) == old_expr_) {
        expr.get_param_expr(i) = new_expr_;
      }
    }
  }
  return ret;
}

bool ObRawExprReplacer::skip_child(ObRawExpr &expr)
{
  return &expr == new_expr_;
}

}//end of namespace sql
}//end of namespace oceanbase
