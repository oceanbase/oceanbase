/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_ADD_TO_CONTEXT_H
#define _OB_RAW_EXPR_ADD_TO_CONTEXT_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
  /**
   *  This class is used to traverse an expression tree in a post-order fashion to
   *  add all required expressions into the context data structure, which is used
   *  during output expr allocation.
   */
  class ObRawExprAddToContext: public ObRawExprVisitor
  {
  public:
  ObRawExprAddToContext(common::ObIArray<ExprProducer> *ctx, uint64_t id)
    : ctx_(ctx), consumer_id_(id), keep_working_(true)
    {}
    virtual ~ObRawExprAddToContext() {}

    /**
     *  The starting point
     */
    int add_to_context(ObRawExpr &expr);

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
    virtual int visit(ObWinFunRawExpr &expr);
    virtual int visit(ObPseudoColumnRawExpr &expr);
    virtual int visit(ObPlQueryRefRawExpr &expr);
    virtual int visit(ObMatchFunRawExpr &expr);
    virtual int visit(ObUnpivotRawExpr &expr);
  private:
    int add_expr(ObRawExpr &expr);
    // types and constants

  private:
    // function members
  private:
    common::ObIArray<ExprProducer> *ctx_;
    uint64_t consumer_id_;
    bool keep_working_;
    // disallow copy
    DISALLOW_COPY_AND_ASSIGN(ObRawExprAddToContext);
  };
}
}

#endif // _OB_RAW_EXPR_ADD_TO_CONTEXT_H
