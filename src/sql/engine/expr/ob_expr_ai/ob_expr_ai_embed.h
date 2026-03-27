/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_
#define OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprAIEmbed : public ObFuncExprOperator
{
public:
  explicit ObExprAIEmbed(common::ObIAllocator &alloc);
  virtual ~ObExprAIEmbed();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  // batch method1:json array,multi-request,one request contains one content
  static int eval_ai_embed_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  // batch method2:json object,one requset contains batch contents
  static int eval_ai_embed_vector_v2(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int get_vector_params(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const EvalBound &bound,
                              ObString &model_id,
                              ObArray<ObString> &contents,
                              int64_t &dim);
  static int pack_json_array_to_res_vector(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObIAllocator &allocator,
                                          ObArray<ObJsonObject *> &responses,
                                          const ObBitVector &skip,
                                          const EvalBound &bound,
                                          const ObAiModelEndpointInfo &endpoint_info,
                                          ObIVector *res_vec);
  static int pack_json_object_to_res_vector(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObJsonObject *response,
                                            const ObBitVector &skip,
                                            const EvalBound &bound,
                                            const ObAiModelEndpointInfo &endpoint_info,
                                            ObIVector *res_vec);

  static constexpr int MODEL_IDX = 0;
  static constexpr int CONTENT_IDX = 1;
  static constexpr int DIM_IDX = 2;
  DISALLOW_COPY_AND_ASSIGN(ObExprAIEmbed);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_
