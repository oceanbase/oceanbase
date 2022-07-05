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
namespace oceanbase {
namespace sql {

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

int ObAnyValueChecker::visit(ObVarRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObAnyValueChecker::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *ref_stmt = expr.get_ref_stmt();
  if (OB_FAIL(check_select_stmt(ref_stmt))) {
    LOG_WARN("failed to check select stmt", K(ret));
  }
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

int ObAnyValueChecker::visit(ObFunMatchAgainst &expr)
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

int ObAnyValueChecker::check_select_stmt(const ObSelectStmt *ref_stmt)
{
  int ret = OB_SUCCESS;
  bool ref_query = false;
  LOG_DEBUG("check any value start stmt", K(ret));

  if (OB_ISNULL(ref_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref_stmt should not be NULL", K(ret));
  } else if (OB_FAIL(
                 ObTransformUtils::is_ref_outer_block_relation(ref_stmt, ref_stmt->get_current_level(), ref_query))) {
    LOG_WARN("failed to get ref stmt", K(ret));
  } else if (!ref_query) {
    // non ref query
  } else {
    int32_t ignore_scope = 0;
    if (ref_stmt->is_order_siblings()) {
      ignore_scope |= RelExprCheckerBase::ORDER_SCOPE;
    }
    ObArray<ObRawExpr *> relation_expr_pointers;
    if (OB_FAIL(ref_stmt->get_relation_exprs(relation_expr_pointers, ignore_scope))) {
      LOG_WARN("get stmt relation exprs fail", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
      ObRawExpr *expr = relation_expr_pointers.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr in relation_expr_pointers is null", K(i), K(relation_expr_pointers));
      } else if (OB_FAIL(expr->preorder_accept(*this))) {
        LOG_WARN("fail to check group by", K(i), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const ObIArray<ObSelectStmt *> &child_stmts = ref_stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ret = check_select_stmt(child_stmts.at(i));
      }
    }
  }
  LOG_DEBUG("check any value end stmt", K(ret));
  return ret;
}

int ObAnyValueChecker::check_any_value(const ObRawExpr *expr, const ObColumnRefRawExpr *undefined_column)
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

}  // namespace sql
}  // namespace oceanbase