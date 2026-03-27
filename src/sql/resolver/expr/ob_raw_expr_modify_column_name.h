/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_MODIFY_COLUMN_NAME_H
#define _OB_RAW_EXPR_MODIFY_COLUMN_NAME_H 1

#include "lib/string/ob_string.h"
#include "lib/worker.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
class ObRawExprModifyColumnName : public ObRawExprVisitor
{
public:
  ObRawExprModifyColumnName(
      common::ObString new_column_name,
      common::ObString orig_column_name,
      const lib::Worker::CompatMode compat_mode)
    : ObRawExprVisitor() {
    orig_column_name_ = orig_column_name;
    new_column_name_ = new_column_name;
    compat_mode_ = compat_mode;
  }
  virtual ~ObRawExprModifyColumnName() {}

  int modifyColumnName(ObRawExpr &expr);

  // interface of ObRawExprVisitor
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
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);
  virtual int visit(ObUnpivotRawExpr &expr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExprModifyColumnName);
  common::ObString orig_column_name_;
  common::ObString new_column_name_;
  lib::Worker::CompatMode compat_mode_;
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_MODIFY_COLUMN_NAME_H */
