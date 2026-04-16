/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AI_COMPLETE_H_
#define OCEANBASE_SQL_OB_EXPR_AI_COMPLETE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprAIComplete : public ObFuncExprOperator
{
public:
  explicit ObExprAIComplete(common::ObIAllocator &alloc);
  virtual ~ObExprAIComplete();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_complete(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_ai_complete_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int pack_complete_response_to_indices(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               common::ObIAllocator &allocator,
                                               common::ObJsonObject *response,
                                               const common::ObArray<int64_t> &row_indices,
                                               const share::ObAiModelEndpointInfo &endpoint_info,
                                               common::ObIVector *res_vec);
  static constexpr int MODEL_IDX = 0;
  static constexpr int PROMPT_IDX = 1;
  static constexpr int CONFIG_IDX = 2;
  DISALLOW_COPY_AND_ASSIGN(ObExprAIComplete);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_COMPLETE_H_
