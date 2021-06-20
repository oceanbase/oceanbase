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

#ifndef _OB_RAW_EXPR_PUSH_DOWN_AGGR_EXPR_H
#define _OB_RAW_EXPR_PUSH_DOWN_AGGR_EXPR_H 1
#include "ob_logical_operator.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
class ObStmt;
/**
 *  This class is used to traverse an expression tree in a post-order fashion to
 *  add all required expressions into the context data structure, which is used
 *  during output expr allocation.
 */
class ObRawExprPushDownAggrExpr : public ObRawExprVisitor {
public:
  ObRawExprPushDownAggrExpr(const ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory)
      : session_info_(session_info), expr_factory_(expr_factory), push_down_avg_expr_(NULL)
  {}
  virtual ~ObRawExprPushDownAggrExpr()
  {}

  inline const common::ObIArray<ObRawExpr*>& get_new_exprs() const
  {
    return new_exprs_;
  }

  inline ObOpRawExpr* get_push_down_avg_expr() const
  {
    return push_down_avg_expr_;
  }
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
  int64_t get_exprs_count() const
  {
    return new_exprs_.count();
  }

private:
  const ObSQLSessionInfo* session_info_;
  ObRawExprFactory& expr_factory_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> new_exprs_;
  ObOpRawExpr* push_down_avg_expr_;
  DISALLOW_COPY_AND_ASSIGN(ObRawExprPushDownAggrExpr);
};
}  // namespace sql
}  // namespace oceanbase

#endif  // _OB_RAW_EXPR_PUSH_DOWN_AGGR_EXPR_H
