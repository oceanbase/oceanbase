/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/expr/ob_raw_expr_modify_column_name.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObRawExprModifyColumnName::modifyColumnName(ObRawExpr &expr) {
  return expr.postorder_accept(*this);
}

int ObRawExprModifyColumnName::visit(ObConstRawExpr &expr) {
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObExecParamRawExpr &expr)
{
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObVarRawExpr &expr) {
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObOpPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObQueryRefRawExpr &expr) {
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObPlQueryRefRawExpr &expr) {
  UNUSED(expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObColumnRefRawExpr &expr) {
  int ret = OB_SUCCESS;
  ObString column_name = expr.get_column_name();
  lib::CompatModeGuard compat_guard(compat_mode_);
  if (ObColumnNameHashWrapper(column_name) == ObColumnNameHashWrapper(orig_column_name_)) {
    expr.set_column_name(new_column_name_);
  }
  return ret;
}

int ObRawExprModifyColumnName::visit(ObOpRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObCaseOpRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObAggFunRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObMatchFunRawExpr &expr)
{
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObUnpivotRawExpr &expr)
{
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObSysFunRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObSetOpRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObAliasRefRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObWinFunRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}

int ObRawExprModifyColumnName::visit(ObPseudoColumnRawExpr &expr) {
  UNUSED (expr);
  return common::OB_SUCCESS;
}
}
}
