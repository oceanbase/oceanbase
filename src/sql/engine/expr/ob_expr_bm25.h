/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_EXPR_BM25_H_
#define _OB_EXPR_BM25_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

/**
 * An implementation of Okapi BM25 relevance estimation / ranking algorithm
 *
 * Params:
 *    token document count: count of documents contains query token
 *    total document count: count of all documents in retrieval domain
 *    document token count: count of tokens in specific document
 *    average document token count: average count of tokens in document in retrieval domain
 *    related token count: count of query token in specific document
 *
 *    p_k1, p_b, p_epsilon: parameters to tune bm25 score, hard coded for now.
 */
class ObExprBM25 : public ObFuncExprOperator
{
public:
  explicit ObExprBM25(common::ObIAllocator &alloc);
  virtual ~ObExprBM25() {}

  virtual int calc_result_typeN(
      ObExprResType &result_type,
      ObExprResType *types,
      int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;

  static int eval_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
public:
  static constexpr int TOKEN_DOC_CNT_PARAM_IDX = 0;
  static constexpr int TOTAL_DOC_CNT_PARAM_IDX = 1;
  static constexpr int DOC_TOKEN_CNT_PARAM_IDX = 2;
  static constexpr int AVG_DOC_CNT_PARAM_IDX = 3;
  static constexpr int RELATED_TOKEN_CNT_PARAM_IDX = 4;
private:
  static double doc_token_weight(const int64_t token_freq, const double norm_len);
  static double query_token_weight(const int64_t doc_freq, const int64_t doc_cnt);
  static constexpr double p_k1 = 1.2;
  static constexpr double p_b = 0.75;
  static constexpr double p_epsilon = 0.25;
  DISALLOW_COPY_AND_ASSIGN(ObExprBM25);
};

} // namespace sql
} // namespace oceanbase

#endif