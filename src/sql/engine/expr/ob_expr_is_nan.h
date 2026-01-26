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
* This file contains implementation for json_valid.
*/

#ifndef OCEANBASE_SQL_OB_EXPR_IS_NAN_H_
#define OCEANBASE_SQL_OB_EXPR_IS_NAN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprIsNan : public ObFuncExprOperator
{
public:
  explicit ObExprIsNan(common::ObIAllocator &alloc);
  virtual ~ObExprIsNan();
  virtual int calc_result_type1(ObExprResType& type,
                                ObExprResType& type1,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_is_nan(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_is_nan_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template <typename ArgVec, typename ResVec>
  static int vector_isnan(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsNan);
};
}
}
#endif // OCEANBASE_SQL_OB_EXPR_IS_NAN_H_