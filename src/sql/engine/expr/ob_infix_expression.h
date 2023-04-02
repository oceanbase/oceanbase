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

#ifndef OCEANBASE_EXPR_OB_INFIX_EXPRESSION_H_
#define OCEANBASE_EXPR_OB_INFIX_EXPRESSION_H_

#include "sql/engine/expr/ob_postfix_expression.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{

class ObInfixExprItem : public ObPostExprItem
{
  OB_UNIS_VERSION(1);
public:
  ObInfixExprItem()
      : ObPostExprItem(),
        param_idx_(0), param_num_(0), param_lazy_eval_(false),
        is_boolean_(false)
  {
  }

  uint16_t get_param_idx() const { return param_idx_; }
  uint16_t get_param_num() const { return param_num_; }
  bool is_param_lazy_eval() const { return param_lazy_eval_; }

  void set_param_idx(const uint16_t idx) { param_idx_ = idx; }
  void set_param_num(const uint16_t num) { param_num_ = num; }
  void set_param_lazy_eval() { param_lazy_eval_ = true; }
  void set_is_boolean(bool is_boolean) { is_boolean_ = is_boolean; }
  bool is_boolean() const { return is_boolean_; }

  // deep copy self with allocator
  int deep_copy(common::ObIAllocator &alloc, const bool only_obj = false);

  INHERIT_TO_STRING_KV("parent", ObPostExprItem,
      K(param_idx_), K(param_num_), K(param_lazy_eval_));

protected:
  // index of first parameter
  uint16_t param_idx_;
  // number of parameter
  uint16_t param_num_;
  // parameters are evaluated by expression
  bool param_lazy_eval_;
  bool is_boolean_;
};

// Infix expression, expr stored in %exprs_, expr's child stored in
// ObInfixExprItem::param_idx_ position.
//
// e.g.:
//     a * b + c / d
// exprs_ will be: + * / a b c d
//
class ObInfixExpression
{
  OB_UNIS_VERSION(1);
public:
  const static int64_t TSI_STACK_IDENTIFIER = 3; // 1 and 2 used in ob_postfix_expression.cpp
  const static int64_t STACK_ALLOC_STACK_SIZE = 128;

  typedef ObSqlFixedArray<ObInfixExprItem> ExprItemArray;

  ObInfixExpression(common::ObIAllocator &alloc, const int64_t expr_cnt);
  ~ObInfixExpression();

  void reset();
  int assign(const ObInfixExpression &other);

  int set_item_count(const int64_t count);
  // add item to %exprs_, only %item.get_obj() deep copied
  int add_expr_item(const ObInfixExprItem &item);

  int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row, common::ObObj &val) const;
  int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1, const common::ObNewRow &row2,
           common::ObObj &val) const;

  // Only called in ObAggregateExpression, because it try to evaluate
  // T_OP_AGG_PARAM_LIST which return multi values.
  int calc_row(common::ObExprCtx &expr_ctx, const common::ObNewRow &row, ObItemType aggr_func,
               const ObExprResType &res_type, common::ObNewRow &res_row) const;

  int generate_idx_for_regexp_ops(int16_t &cur_regexp_op_count);

  ExprItemArray &get_exprs() { return exprs_; }
  const ExprItemArray &get_exprs() const { return exprs_; }

  OB_INLINE bool is_empty() const { return 0 == exprs_.count(); }
  bool is_equijoin_cond(int64_t &c1, int64_t &c2,
      common::ObObjType &cmp_type, common::ObCollationType &cmp_cs_type,
      bool &is_null_safe) const;

  int uk_fast_project(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
      common::ObObj &result_val) const;

  OB_INLINE int param_eval(common::ObExprCtx &expr_ctx, const common::ObObj &param) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = &param - expr_ctx.stack_;
    if (OB_ISNULL(expr_ctx.stack_) || OB_ISNULL(expr_ctx.row1_)
        || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= exprs_.count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "some ctx is invalid", K(ret), KP(expr_ctx.stack_),
          KP(expr_ctx.row1_), K(pos));
    } else {
      ret = eval(expr_ctx, *expr_ctx.row1_, expr_ctx.stack_, pos);
    }
    return ret;
  }

  OB_INLINE int get_param_type(common::ObExprCtx &expr_ctx,
                               const common::ObObj &param,
                               ObItemType &param_type) const
  {
    int ret = common::OB_SUCCESS;
    param_type = T_INVALID;
    int64_t pos = &param - expr_ctx.stack_;
    if (OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= exprs_.count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "some ctx is invalid", K(ret), K(pos));
    } else {
      param_type = exprs_.at(pos).get_item_type();
    }
    return ret;
  }

  OB_INLINE int get_param_is_boolean(common::ObExprCtx &expr_ctx,
                                         const common::ObObj &param,
                                         bool &is_boolean) const
  {
    int ret = common::OB_SUCCESS;
    is_boolean = false;
    int64_t pos = &param - expr_ctx.stack_;
    if (OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= exprs_.count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "some ctx is invalid", K(ret), K(pos));
    } else {
      is_boolean = exprs_.at(pos).is_boolean();
    }
    return ret;
  }

  TO_STRING_KV(K_(exprs));

private:

  OB_INLINE static int64_t &global_stack_top() { RLOCAL(int64_t, val); return val; }
  OB_INLINE static ObPostfixExpressionCalcStack *global_stack()
  { return GET_TSI_MULT(ObPostfixExpressionCalcStack, TSI_STACK_IDENTIFIER); }

  int eval(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
      common::ObObj *stack, const int64_t pos) const;

private:
  common::ObIAllocator &alloc_;
  ExprItemArray exprs_;
  // output_column_count_ ?

  DISALLOW_COPY_AND_ASSIGN(ObInfixExpression);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_EXPR_OB_INFIX_EXPRESSION_H_
