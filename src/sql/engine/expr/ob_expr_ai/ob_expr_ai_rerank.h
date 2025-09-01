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

#ifndef OCEANBASE_SQL_OB_EXPR_AI_RERANK_H_
#define OCEANBASE_SQL_OB_EXPR_AI_RERANK_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprAIRerank : public ObFuncExprOperator
{
public:
  explicit ObExprAIRerank(common::ObIAllocator &alloc);
  virtual ~ObExprAIRerank();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }

private:
  static int inner_eval_ai_rerank(common::ObIAllocator &allocator,
                                  common::ObAIFuncExprInfo *info,
                                  common::ObArray<common::ObString> &header_array,
                                  common::ObString &query,
                                  common::ObJsonArray *document_array,
                                  common::ObJsonArray *&result_array);
  static int construct_config_json(common::ObIAllocator &allocator, int64_t top_k, int64_t return_doc, common::ObJsonObject *&config_json);
  static int construct_batch_document_array(common::ObIAllocator &allocator, common::ObJsonArray *document_array, int64_t start_idx, int64_t end_idx, common::ObJsonArray *&batch_document_array);
  static int batch_result_add_base(common::ObIAllocator &allocator, common::ObJsonArray *array, int64_t start_idx);
  static int compact_json_array_by_key(common::ObIAllocator &allocator, common::ObJsonArray *array1, common::ObJsonArray *array2, common::ObString &score_key, common::ObJsonArray *&compact_array);
  static constexpr int MODEL_IDX = 0;
  static constexpr int QUERY_IDX = 1;
  static constexpr int DOCUMENTS_IDX = 2;
  static constexpr char INDEX_KEY[20] = "index";
  static constexpr char SCORE_KEY[20] = "relevance_score";
  DISALLOW_COPY_AND_ASSIGN(ObExprAIRerank);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_RERANK_H_
