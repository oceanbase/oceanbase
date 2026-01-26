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

#ifndef OCEANBASE_SQL_EXPR_FUNC_RIGHT_
#define OCEANBASE_SQL_EXPR_FUNC_RIGHT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRight : public ObStringExprOperator
{
public:
  explicit  ObExprRight(common::ObIAllocator &alloc);
  virtual ~ObExprRight();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &text,
                                ObExprResType &start_pos,
                                common::ObExprTypeCtx &type_ctx) const;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;

  static int calc_right_vector(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const ObBitVector &skip,
                               const EvalBound &bound);

  template <typename ArgVec, typename ResVec, bool IsAscii, bool CanDoAsciiOptimize, bool HasNull>
  static int calc_right_vector_const_inner(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           const ObBitVector &skip,
                                           const EvalBound &bound,
                                           int64_t const_len);

  template <typename ArgVec, typename ResVec>
  static int calc_right_vector_const(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const EvalBound &bound,
                                     int64_t const_len);

  static int calc_right_vector_non_const(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound);

  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRight);
};

}
}

#endif //OCEANBASE_SQL_EXPR_FUNC_RIGHT_
