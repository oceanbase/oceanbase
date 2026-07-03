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
#include "observer/omt/ob_tenant_ai_service.h"

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
  // batch method2:json object,one requset contains batch contents
  static int eval_ai_embed_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
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
  static int pack_embed_response_to_indices(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObJsonObject *response,
                                            const ObArray<int64_t> &row_indices,
                                            const ObAiModelEndpointInfo &endpoint_info,
                                            ObIVector *res_vec);

  static int enqueue_group_to_pending(ObIAllocator &allocator,
                                      const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      omt::ObAiServiceGuard &ai_service_guard,
                                      ObIVector *res_vec,
                                      ObBitVector &eval_flags,
                                      const ObString &grp_model_id,
                                      int64_t grp_dim,
                                      ObArray<ObString> &grp_contents,
                                      ObArray<ObString> &grp_input_types,
                                      const ObArray<int64_t> &grp_row_idxs,
                                      common::ObAIFuncBatchState &pending,
                                      int64_t &pending_dim);

  enum class ObAiEmbedInputType {
    INPUT_TYPE_UNKNOWN = 0,
    INPUT_TYPE_TEXT = 1,
    INPUT_TYPE_IMAGE = 2
  };

  static int parse_embed_options(ObIAllocator &allocator,
                                 const ObString &options_str,
                                 ObAiEmbedInputType &input_type);
  static int process_image_input(ObIAllocator &allocator,
                                const ObString &image_input,
                                bool is_url,
                                ObString &processed_content);

  static constexpr int MODEL_IDX = 0;
  static constexpr int CONTENT_IDX = 1;
  static constexpr int DIM_IDX = 2;
  static constexpr int OPTIONS_IDX = 3;
  // default size of a single request
  static constexpr int64_t AI_EMBED_DEFAULT_BATCH_SIZE = 1;
  // max total bytes per batch
  static constexpr int64_t AI_EMBED_BATCH_MAX_BYTES = 100 * 1024 * 1024;  // 100MB
  DISALLOW_COPY_AND_ASSIGN(ObExprAIEmbed);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_
