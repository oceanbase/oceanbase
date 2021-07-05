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

#ifndef _OB_RAW_EXPR_REPLACER_H
#define _OB_RAW_EXPR_REPLACER_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase {
namespace sql {
class ObRawExprReplacer : public ObRawExprVisitor {
public:
  ObRawExprReplacer(ObRawExpr* old_expr, ObRawExpr* new_expr);
  virtual ~ObRawExprReplacer();

  int replace(ObRawExpr& expr);
  virtual int visit(ObConstRawExpr& expr);
  virtual int visit(ObVarRawExpr& expr);
  virtual int visit(ObQueryRefRawExpr& expr);
  virtual int visit(ObColumnRefRawExpr& expr);
  virtual int visit(ObOpRawExpr& expr);
  virtual int visit(ObCaseOpRawExpr& expr);
  virtual int visit(ObAggFunRawExpr& expr);
  virtual int visit(ObSysFunRawExpr& expr);
  virtual int visit(ObSetOpRawExpr& expr);
  virtual int visit(ObWinFunRawExpr& expr);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprReplacer);
  // function members
private:
  ObRawExpr* old_expr_;
  ObRawExpr* new_expr_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_RAW_EXPR_REPLACER_H */
