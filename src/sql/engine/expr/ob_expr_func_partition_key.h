/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprFuncPartKey : public ObFuncExprOperator
{
public:
  explicit  ObExprFuncPartKey(common::ObIAllocator &alloc);
  virtual ~ObExprFuncPartKey();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int calc_partition_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_partition_key_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int calc_partition_key_vector(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncPartKey);

};

}  // namespace sql
}  // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_
