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

#ifndef _OB_RAW_EXPR_PULL_UP_AGGR_EXPR_H
#define _OB_RAW_EXPR_PULL_UP_AGGR_EXPR_H 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
/**
 *  This class is used to traverse an expression tree in a post-order fashion to
 *  add all required expressions into the context data structure, which is used
 *  during output expr allocation.
 */
class ObRawExprPullUpAggrExpr : public ObRawExprVisitor {
public:
  ObRawExprPullUpAggrExpr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info)
      : expr_factory_(expr_factory),
        new_expr_(NULL),
        sum_sum_(NULL),
        sum_count_(NULL),
        merge_synopsis_(NULL),
        session_info_(session_info)
  {}
  virtual ~ObRawExprPullUpAggrExpr()
  {}

  /**
   *  The starting point
   */
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

  inline ObRawExpr* get_new_expr() const
  {
    return new_expr_;
  }
  inline ObRawExpr* get_sum_sum_expr() const
  {
    return sum_sum_;
  }
  inline ObRawExpr* get_sum_count_expr() const
  {
    return sum_count_;
  }
  inline ObRawExpr* get_merge_synopsis_expr() const
  {
    return merge_synopsis_;
  }

private:
  // ObIArray<ExprProducer> *ctx_;
  // uint64_t consumer_id_;
  // disallow copy
  ObRawExprFactory& expr_factory_;
  ObRawExpr* new_expr_;
  ObAggFunRawExpr* sum_sum_;
  ObAggFunRawExpr* sum_count_;
  ObAggFunRawExpr* merge_synopsis_;
  const ObSQLSessionInfo* session_info_;
  DISALLOW_COPY_AND_ASSIGN(ObRawExprPullUpAggrExpr);
};
}  // namespace sql
}  // namespace oceanbase

#endif  // _OB_RAW_EXPR_PULL_UP_AGGR_EXPR_H
