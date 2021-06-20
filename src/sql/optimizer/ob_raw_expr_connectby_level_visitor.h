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

#ifndef OB_RAW_EXPR_CONNECTBY_LEVEL_VISITOR_H_
#define OB_RAW_EXPR_CONNECTBY_LEVEL_VISITOR_H_
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {

class ObRawConnectByLevelVisitor : public ObRawExprVisitor {
public:
  ObRawConnectByLevelVisitor() : level_exprs_(nullptr){};
  virtual ~ObRawConnectByLevelVisitor() = default;

  int check_connectby_filter(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& level_exprs);
  // OP types: constants, ? etc.
  virtual int visit(ObConstRawExpr& expr);
  // OP types: ObObjtypes
  virtual int visit(ObVarRawExpr& expr);
  // OP types: subquery, cell index
  virtual int visit(ObQueryRefRawExpr& expr);
  // OP types: identify, table.column
  virtual int visit(ObColumnRefRawExpr& expr);
  // unary OP types: exists, not, negative, positive
  // binary OP types: +, -, *, /, >, <, =, <=>, IS, IN etc.
  // triple OP types: like, not like, btw, not btw
  // multi OP types: and, or, ROW
  virtual int visit(ObOpRawExpr& expr);
  // OP types: case, arg case
  virtual int visit(ObCaseOpRawExpr& expr);
  // OP types: aggregate functions e.g. max, min, avg, count, sum
  virtual int visit(ObAggFunRawExpr& expr);
  // OP types: system functions
  virtual int visit(ObSysFunRawExpr& expr);
  virtual int visit(ObSetOpRawExpr& expr);
  virtual int visit(ObAliasRefRawExpr& expr);
  virtual int visit(ObFunMatchAgainst& expr);
  virtual int visit(ObSetIterRawExpr& expr);
  virtual int visit(ObRowIterRawExpr& expr);
  virtual int visit(ObWinFunRawExpr& expr);
  virtual int visit(ObPseudoColumnRawExpr& expr);

private:
  common::ObIArray<ObRawExpr*>* level_exprs_;
};

}  // namespace sql
}  // namespace oceanbase

#endif
