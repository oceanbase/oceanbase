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

#ifndef _OB_EXPR_AND_H_
#define _OB_EXPR_AND_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprAnd : public ObLogicalExprOperator
{
public:
  explicit  ObExprAnd(common::ObIAllocator &alloc);
  virtual ~ObExprAnd() {};

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_and_batch_exprN(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size);
  static int eval_and_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound);

  enum EvalAndStage {
    FIRST,
    MIDDLE,
    LAST
  };

private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAnd);
};
}
}
#endif  /* _OB_EXPR_AND_H_ */
