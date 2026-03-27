/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_GET_HASH_VALUE_H
#define _OB_RAW_EXPR_GET_HASH_VALUE_H 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
  /**
   *  This class is used to traverse an expression tree in a post-order fashion to
   *  add all required expressions into the context data structure, which is used
   *  during output expr allocation.
   */
  class ObRawExprGetHashValue: public ObRawExprVisitor
  {
  public:
    ObRawExprGetHashValue(uint64_t seed) : seed_(seed) {}
    virtual ~ObRawExprGetHashValue() {}

    /**
     *  The starting point
     */
    int get_hash_value(ObRawExpr &expr);

    /// interface of ObRawExprVisitor
    virtual int visit(ObConstRawExpr &expr);
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
  private:
    int add_expr(ObRawExpr &expr);

  private:
    uint64_t seed_;
    // disallow copy
    DISALLOW_COPY_AND_ASSIGN(ObRawExprGetHashValue);
  };
}
}

#endif // _OB_RAW_EXPR_GET_HASH_VALUE_H
