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

#define USING_LOG_PREFIX SQL_ENG
#include <cmath>
#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
ObExprBM25::ObExprBM25(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_BM25, N_BM25, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprBM25::calc_result_typeN(
    ObExprResType &result_type,
    ObExprResType *types,
    int64_t param_num,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num != 5)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("BM25 expr should have 4 parameters", K(ret), K(param_num));
  } else {
    types[TOKEN_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[TOTAL_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[DOC_TOKEN_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[AVG_DOC_CNT_PARAM_IDX].set_calc_type(ObDoubleType);
    types[RELATED_TOKEN_CNT_PARAM_IDX].set_calc_type(ObUInt64Type);
    result_type.set_double();
  }
  return ret;
}

int ObExprBM25::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK(5 == raw_expr.get_param_count());
  rt_expr.eval_func_ = eval_bm25_relevance_expr;
  return ret;
}

int ObExprBM25::eval_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *token_doc_cnt_datum = nullptr;
  ObDatum *total_doc_cnt_datum = nullptr;
  ObDatum *doc_token_cnt_datum = nullptr;
  ObDatum *avg_doc_token_cnt_datum = nullptr;
  ObDatum *related_token_cnt_datum = nullptr;
  if (OB_FAIL(expr.eval_param_value(
      ctx,
      token_doc_cnt_datum,
      total_doc_cnt_datum,
      doc_token_cnt_datum,
      avg_doc_token_cnt_datum,
      related_token_cnt_datum))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum->is_null() || total_doc_cnt_datum->is_null()
      || doc_token_cnt_datum->is_null() || avg_doc_token_cnt_datum->is_null() || related_token_cnt_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum), KPC(total_doc_cnt_datum),
        KPC(doc_token_cnt_datum), KPC(avg_doc_token_cnt_datum), KPC(related_token_cnt_datum));
  } else {
    const int64_t token_doc_cnt = token_doc_cnt_datum->get_int();
    const int64_t total_doc_cnt = total_doc_cnt_datum->get_int();
    const int64_t related_token_cnt = related_token_cnt_datum->get_uint();
    const int64_t doc_token_cnt = doc_token_cnt_datum->get_int();
    const double avg_doc_token_cnt = avg_doc_token_cnt_datum->get_double();
    const double norm_len = doc_token_cnt / avg_doc_token_cnt;
    const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
    const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
    const double relevance = token_weight * doc_weight;
    res_datum.set_double(relevance);
    LOG_DEBUG("show bm25 parameters for current document",
        K(token_doc_cnt), K(total_doc_cnt), K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
        K(norm_len), K(token_weight), K(doc_weight), K(relevance));
  }
  return ret;
}

double ObExprBM25::doc_token_weight(const int64_t token_freq, const double norm_len)
{
  const double tf = static_cast<double>(token_freq);
  return tf / (tf + p_k1 * (1.0 - p_b + p_b * norm_len));
}

double ObExprBM25::query_token_weight(const int64_t doc_freq, const int64_t doc_cnt)
{
  const double df = static_cast<double>(doc_freq);
  const double len = static_cast<double>(doc_cnt);
  // Since we might use approximate count statistic for total doc cnt, possibilities there are
  //   document frequencies larger than total doc cnt
  const double diff = (len - df) > 0 ? (len - df) : 0;
  const double idf = std::log((diff + 0.5) / (df + 0.5));
  return MAX(p_epsilon, idf) * (1.0 + p_k1);
}


} // namespace sql
} // namespace oceanbase
