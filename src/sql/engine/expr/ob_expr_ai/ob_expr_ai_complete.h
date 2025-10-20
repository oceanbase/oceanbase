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

  static int eval_ai_complete_vector_v2(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int get_prompt_and_contents_contact_str(ObIAllocator &allocator,
                                      ObString &prompt,
                                      ObArray<ObString> &contents,
                                      ObString &prompt_and_contents);
  static int construct_tuple_str(ObIAllocator &allocator, ObArray<ObString> &contents, ObString &content_str);
  static int get_tuple_str(ObIAllocator &allocator, ObString &content, ObString &tuple_str);
  static int get_config_with_json_response_format(ObIAllocator &allocator, ObJsonObject *config);
  static int get_vector_params(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const EvalBound &bound,
                              ObString &model_id,
                              ObArray<ObString> &prompts,
                              ObJsonObject *&config);
  static int pack_json_array_to_res_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                      ObIAllocator &allocator,
                                      ObArray<ObJsonObject *> &responses,
                                      const ObBitVector &skip,
                                      const EvalBound &bound,
                                      const ObAiModelEndpointInfo &endpoint_info,
                                      ObIVector *res_vec);
  static int pack_json_string_to_res_vector(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObIJsonBase *response,
                                            const ObBitVector &skip,
                                            const EvalBound &bound,
                                            ObIVector *res_vec);
  static constexpr int MODEL_IDX = 0;
  static constexpr int PROMPT_IDX = 1;
  static constexpr int CONFIG_IDX = 2;
  static constexpr char PROMPT_META_PROMPT[20] = "{{USER_PROMPT}}";
  static constexpr char TUPLES_META_PROMPT[20] = "{{TUPLES}}";
  static constexpr char meta_prompt_[1024] = "You are OceanBase semantic analysis tool for DBMS. "
    "You will analyze each tuple in the provided data and respond to the user prompt. "
    "User Prompt:{{USER_PROMPT}}\n"
    "Tuples Table:{{TUPLES}}\n"
    "Instructions:The response should be directly relevant to each tuple without additional formatting, purely answering the user prompt as if each tuple were a standalone entity. Use clear, context-relevant language to generate a meaningful and concise answer for each tuple. "
    "Expected Response Format:You should return the responses to the user's prompt for each tuple in json array format. Ensure no tuple is missed.\n"
    "Example: {\"item\":[\"The response for the first tuple\", \"The response for the second tuple\"]}";
  DISALLOW_COPY_AND_ASSIGN(ObExprAIComplete);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_COMPLETE_H_
