/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_PRINT_VISITOR_H
#define _OB_RAW_EXPR_PRINT_VISITOR_H
#include "lib/utility/ob_print_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase
{
namespace sql
{
class ObRawExprPrintVisitor: public ObRawExprVisitor
{
public:
  explicit ObRawExprPrintVisitor(ObRawExpr &expr_root);
  virtual ~ObRawExprPrintVisitor();

  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);
  virtual int visit(ObUnpivotRawExpr &expr);
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprPrintVisitor);
  // function members
private:
  // data members
  ObRawExpr &expr_root_;
  mutable char* buf_;
  mutable int64_t buf_len_;
  mutable int64_t pos_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_PRINT_VISITOR_H */
