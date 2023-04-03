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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObAnyValueChecker: public ObRawExprVisitor
{
public:
  ObAnyValueChecker() : ObRawExprVisitor(), skip_expr_(nullptr), is_pass_(true)
  {}
  virtual ~ObAnyValueChecker()
  {}
  /// interface of ObRawExprVisitor
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

  // set expr skip
  virtual bool skip_child(ObRawExpr &expr) { return skip_expr_ == &expr; }

  int check_any_value(const ObRawExpr *expr, const ObColumnRefRawExpr * undefined_column);
  bool is_pass_after_check();
private:
  void set_skip_expr(ObRawExpr *expr) { skip_expr_ = expr; }
  const ObColumnRefRawExpr * undefined_column_;
  ObRawExpr *skip_expr_;
  bool is_pass_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAnyValueChecker);
};

}
}
#endif //OCEANBASE_SRC_SQL_RESOLVER_DML_OB_ANY_VALUE_CHECKER_H_
