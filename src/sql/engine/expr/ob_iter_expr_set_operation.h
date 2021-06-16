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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_SET_OPERATION_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_SET_OPERATION_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase {
namespace sql {
class ObSetOpIterExpr : public ObIterExprOperator {
private:
  class ObSetOpIterExprCtx : public ObExprOperatorCtx {
  public:
    ObSetOpIterExprCtx() : left_row_(NULL), right_row_(NULL), left_is_end_(false), right_is_end_(false)
    {}

  private:
    const common::ObNewRow* left_row_;
    const common::ObNewRow* right_row_;
    bool left_is_end_;
    bool right_is_end_;
    friend class ObSetOpIterExpr;
  };

public:
  ObSetOpIterExpr() : ObIterExprOperator(), left_iter_(NULL), right_iter_(NULL)
  {}

  inline void set_left_iter(const ObIterExprOperator* left_iter)
  {
    left_iter_ = left_iter;
  }
  inline void set_right_iter(const ObIterExprOperator* right_iter)
  {
    right_iter_ = right_iter;
  }
  int get_next_row(ObIterExprCtx& expr_ctx, const common::ObNewRow*& result) const;

protected:
  inline int get_left_row(ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx) const;
  inline int get_right_row(ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx) const;
  inline int compare_row(const common::ObNewRow& left_row, const common::ObNewRow& right_row, int& result) const;
  inline int get_union_next_row(
      ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const common::ObNewRow*& result) const;
  inline int get_intersect_next_row(
      ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const common::ObNewRow*& result) const;
  inline int get_except_next_row(
      ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const common::ObNewRow*& result) const;

protected:
  const ObIterExprOperator* left_iter_;
  const ObIterExprOperator* right_iter_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_SET_OPERATION_H_ */
