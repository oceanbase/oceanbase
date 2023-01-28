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
#include "sql/resolver/dml/ob_any_value_checker.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "lib/oblog/ob_log_module.h"
//#include "lib/utility/ob_macro_utils.h"
namespace oceanbase
{
namespace sql
{

int ObAnyValueChecker::visit(ObSysFunRawExpr &expr)
{
  if (T_FUN_SYS_ANY_VALUE == expr.get_expr_type()) {
    set_skip_expr(&expr);
  }
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObColumnRefRawExpr &expr)
{
  if (undefined_column_ == &expr) {
    is_pass_ = false;
  }
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObConstRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.get_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is invalid", K(ret));
  } else if (OB_FAIL(expr.get_ref_expr()->preorder_accept(*this))) {
    LOG_WARN("failed to visit child", K(ret));
  }
  return ret;
}

int ObAnyValueChecker::visit(ObVarRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObOpPseudoColumnRawExpr &expr)
{
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObQueryRefRawExpr &expr)
{
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObPlQueryRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObOpRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObCaseOpRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObAggFunRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObSetOpRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObAliasRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObWinFunRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::check_any_value(const ObRawExpr *expr, const ObColumnRefRawExpr * undefined_column)
{
  int ret = OB_SUCCESS;
  undefined_column_ = undefined_column;
  ObRawExpr *expr_ = const_cast<ObRawExpr *>(expr);
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(expr_->preorder_accept(*this))) {
    LOG_WARN("check any value expr fail", K(ret));
  }
  return ret;
}

bool ObAnyValueChecker::is_pass_after_check()
{
  return is_pass_;
}

} // namespace sql
} // namespace oceanbase
