/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_ANALYZER_H
#define _OB_RAW_EXPR_ANALYZER_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase
{
namespace sql
{
class ObRawExprInfoExtractor : public ObRawExprVisitor
{
public:
ObRawExprInfoExtractor()
    : ObRawExprVisitor() {}
  virtual ~ObRawExprInfoExtractor() {}

  int analyze(ObRawExpr &expr);

  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);
  virtual int visit(ObUnpivotRawExpr &expr);

  static ObItemType get_subquery_comparison_type(ObItemType cmp_type);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprInfoExtractor);
  // function members
  int visit_interm_node(ObRawExpr &expr);
  int visit_subquery_node(ObOpRawExpr &expr);
  int visit_left_param(ObRawExpr &expr);
  //需要根据右边的操作符来修改根节点信息，所以需要传入最上层根节点
  int visit_right_param(ObOpRawExpr &expr);
  int clear_info(ObRawExpr &expr);
  int pull_info(ObRawExpr &expr);
  int add_const(ObRawExpr &expr);
  int add_deterministic(ObRawExpr &expr);
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_ANALYZER_H */
