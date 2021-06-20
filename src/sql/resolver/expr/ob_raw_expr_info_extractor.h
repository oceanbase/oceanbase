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

#ifndef _OB_RAW_EXPR_ANALYZER_H
#define _OB_RAW_EXPR_ANALYZER_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase {
namespace sql {
class ObRawExprInfoExtractor : public ObRawExprVisitor {
public:
  ObRawExprInfoExtractor() : ObRawExprVisitor()
  {}
  virtual ~ObRawExprInfoExtractor()
  {}

  int analyze(ObRawExpr& expr);

  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr& expr);
  virtual int visit(ObVarRawExpr& expr);
  virtual int visit(ObQueryRefRawExpr& expr);
  virtual int visit(ObColumnRefRawExpr& expr);
  virtual int visit(ObOpRawExpr& expr);
  virtual int visit(ObCaseOpRawExpr& expr);
  virtual int visit(ObAggFunRawExpr& expr);
  virtual int visit(ObSysFunRawExpr& expr);
  virtual int visit(ObSetOpRawExpr& expr);
  virtual int visit(ObAliasRefRawExpr& expr);
  virtual int visit(ObFunMatchAgainst& expr);
  virtual int visit(ObWinFunRawExpr& expr);
  virtual int visit(ObPseudoColumnRawExpr& expr);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprInfoExtractor);
  // function members
  int visit_interm_node(ObRawExpr& expr);
  int visit_subquery_node(ObOpRawExpr& expr);
  int visit_left_param(ObRawExpr& expr);
  int visit_right_param(ObOpRawExpr& expr);
  void clear_info(ObRawExpr& expr);
  int pull_info(ObRawExpr& expr);
  int add_const(ObRawExpr& expr);
  int add_calculable(ObOpRawExpr& expr);
  bool not_calculable_expr(const ObRawExpr& expr);

  ObItemType get_subquery_comparison_type(ObItemType cmp_type) const;
  // data members
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_RAW_EXPR_ANALYZER_H */
