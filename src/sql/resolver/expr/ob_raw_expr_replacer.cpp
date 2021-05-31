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

#include "ob_raw_expr_replacer.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
ObRawExprReplacer::ObRawExprReplacer(ObRawExpr* old_expr, ObRawExpr* new_expr)
    : old_expr_(old_expr), new_expr_(new_expr)
{}

ObRawExprReplacer::~ObRawExprReplacer()
{}

int ObRawExprReplacer::replace(ObRawExpr& expr)
{
  return expr.preorder_accept(*this);
}

int ObRawExprReplacer::visit(ObConstRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObVarRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObQueryRefRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObColumnRefRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObOpRawExpr& expr)
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

int ObRawExprReplacer::visit(ObCaseOpRawExpr& expr)
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

        if (OB_SUCCESS == ret) {
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

int ObRawExprReplacer::visit(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (&expr != new_expr_) {
    ObIArray<ObRawExpr*>& real_param_exprs = expr.get_real_param_exprs_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
      if (real_param_exprs.at(i) == old_expr_) {
        real_param_exprs.at(i) = new_expr_;
      }
    }
    if (OB_SUCC(ret)) {
      if (expr.get_push_down_sum_expr() == old_expr_) {
        expr.set_push_down_sum_expr(new_expr_);
      }
      if (expr.get_push_down_count_expr() == old_expr_) {
        expr.set_push_down_count_expr(new_expr_);
      }
      ObIArray<OrderItem>& order_items = expr.get_order_items_for_update();
      for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
        if (order_items.at(i).expr_ == old_expr_) {
          order_items.at(i).expr_ = new_expr_;
        }
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObSysFunRawExpr& expr)
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

int ObRawExprReplacer::visit(ObSetOpRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObWinFunRawExpr& expr)
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

}  // end of namespace sql
}  // end of namespace oceanbase
