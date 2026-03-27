/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AI_PROMPT_H_
#define OCEANBASE_SQL_OB_EXPR_AI_PROMPT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprAIPrompt : public ObFuncExprOperator
{
public:
  explicit ObExprAIPrompt(common::ObIAllocator &alloc);
  virtual ~ObExprAIPrompt();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types_array,
                              int64_t param_num,
                              common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_prompt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAIPrompt);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_PROMPT_H_
