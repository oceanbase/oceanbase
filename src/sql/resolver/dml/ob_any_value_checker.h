/**
 * Copyright 2014-2021 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *
 * Date: 2021/07/14
 *
 * ob_any_value_checker.h is for checking any_value expr in mysql's only_full_group_by mode compatible
 *
 * Authors:
 *     ailing.lcq<ailing.lcq@alibaba-inc.com>
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {

class ObAnyValueChecker : public ObRawExprVisitor {
public:
  ObAnyValueChecker() : ObRawExprVisitor(), skip_expr_(nullptr), is_pass_(true)
  {}
  virtual ~ObAnyValueChecker()
  {}
  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObFunMatchAgainst &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);

  // set expr skip
  virtual bool skip_child(ObRawExpr &expr)
  {
    return skip_expr_ == &expr;
  }

  int check_select_stmt(const ObSelectStmt *ref_stmt);
  int check_any_value(const ObRawExpr *expr, const ObColumnRefRawExpr *undefined_column);
  bool is_pass_after_check();

private:
  void set_skip_expr(ObRawExpr *expr)
  {
    skip_expr_ = expr;
  }
  const ObColumnRefRawExpr *undefined_column_;
  ObRawExpr *skip_expr_;
  bool is_pass_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAnyValueChecker);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
