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
#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_util.h"

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
  } else if (lib::is_oracle_mode()) {
    // Oracle mode: parameters are ObNumberType
    types[TOKEN_DOC_CNT_PARAM_IDX].set_calc_type(ObNumberType);
    types[TOTAL_DOC_CNT_PARAM_IDX].set_calc_type(ObNumberType);
    types[DOC_TOKEN_CNT_PARAM_IDX].set_calc_type(ObNumberType);
    types[AVG_DOC_CNT_PARAM_IDX].set_calc_type(ObDoubleType);
    types[RELATED_TOKEN_CNT_PARAM_IDX].set_calc_type(ObNumberType);
    result_type.set_double();
  } else {
    // MySQL mode: parameters are ObIntType
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
  rt_expr.eval_batch_func_ = eval_batch_bm25_relevance_expr;
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
    // Safe conversion: support both INT and NUMBER types
    int64_t token_doc_cnt = 0;
    int64_t total_doc_cnt = 0;
    int64_t related_token_cnt = 0;
    int64_t doc_token_cnt = 0;
    const bool token_doc_cnt_is_decint = expr.args_[TOKEN_DOC_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool total_doc_cnt_is_decint = expr.args_[TOTAL_DOC_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool related_token_cnt_is_decint = expr.args_[RELATED_TOKEN_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool doc_token_cnt_is_decint = expr.args_[DOC_TOKEN_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    if (OB_FAIL(ObExprUtil::get_int_param_val(token_doc_cnt_datum, token_doc_cnt_is_decint, token_doc_cnt))) {
      LOG_WARN("failed to get token_doc_cnt", K(ret));
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(total_doc_cnt_datum, total_doc_cnt_is_decint, total_doc_cnt))) {
      LOG_WARN("failed to get total_doc_cnt", K(ret));
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(related_token_cnt_datum, related_token_cnt_is_decint, related_token_cnt))) {
      LOG_WARN("failed to get related_token_cnt", K(ret));
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(doc_token_cnt_datum, doc_token_cnt_is_decint, doc_token_cnt))) {
      LOG_WARN("failed to get doc_token_cnt", K(ret));
    }
    if (OB_SUCC(ret)) {
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
  }
  return ret;
}

int ObExprBM25::eval_batch_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDatumVector token_doc_cnt_datum;
  ObDatumVector total_doc_cnt_datum;
  ObDatumVector doc_token_cnt_datum;
  ObDatumVector avg_doc_token_cnt_datum;
  ObDatumVector related_token_cnt_datum;
  if (OB_FAIL(expr.eval_batch_param_value(
    ctx,
    skip,
    size,
    token_doc_cnt_datum,
    total_doc_cnt_datum,
    doc_token_cnt_datum,
    avg_doc_token_cnt_datum,
    related_token_cnt_datum))) {
      LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum.at(0)->is_null() || total_doc_cnt_datum.at(0)->is_null()
      || avg_doc_token_cnt_datum.at(0)->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum.at(0)), KPC(total_doc_cnt_datum.at(0)),
         KPC(avg_doc_token_cnt_datum.at(0)));
  } else {
    // Safe conversion: support both INT and NUMBER types
    int64_t token_doc_cnt = 0;
    int64_t total_doc_cnt = 0;
    const bool token_doc_cnt_is_decint = expr.args_[TOKEN_DOC_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool total_doc_cnt_is_decint = expr.args_[TOTAL_DOC_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool related_token_cnt_is_decint = expr.args_[RELATED_TOKEN_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    const bool doc_token_cnt_is_decint = expr.args_[DOC_TOKEN_CNT_PARAM_IDX]->obj_meta_.is_decimal_int();
    if (OB_FAIL(ObExprUtil::get_int_param_val(token_doc_cnt_datum.at(0), token_doc_cnt_is_decint, token_doc_cnt))) {
      LOG_WARN("failed to get token_doc_cnt", K(ret));
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(total_doc_cnt_datum.at(0), total_doc_cnt_is_decint, total_doc_cnt))) {
      LOG_WARN("failed to get total_doc_cnt", K(ret));
    }
    if (OB_SUCC(ret)) {
      const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
      const double avg_doc_token_cnt = avg_doc_token_cnt_datum.at(0)->get_double();
      ObDatum *res_datum = expr.locate_batch_datums(ctx);
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      for(int64_t i = 0; OB_SUCC(ret) && i < size; ++i)
      {
        if (OB_UNLIKELY(doc_token_cnt_datum.at(i)->is_null() || related_token_cnt_datum.at(i)->is_null())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null datum", K(ret), KPC(doc_token_cnt_datum.at(i)), KPC(related_token_cnt_datum.at(i)));
        } else if (!skip.contain(i) && !eval_flags.at(i)) {
          int64_t related_token_cnt = 0;
          int64_t doc_token_cnt = 0;
          if (OB_FAIL(ObExprUtil::get_int_param_val(related_token_cnt_datum.at(i), related_token_cnt_is_decint, related_token_cnt))) {
            LOG_WARN("failed to get related_token_cnt", K(ret));
          } else if (OB_FAIL(ObExprUtil::get_int_param_val(doc_token_cnt_datum.at(i), doc_token_cnt_is_decint, doc_token_cnt))) {
            LOG_WARN("failed to get doc_token_cnt", K(ret));
          } else {
            const double norm_len = doc_token_cnt / avg_doc_token_cnt;
            const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
            const double relevance = token_weight * doc_weight;
            res_datum[i].set_double(relevance);
            eval_flags.set(i);
            LOG_DEBUG("show bm25 parameters for current document",
                      K(token_doc_cnt), K(total_doc_cnt), K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
                      K(norm_len), K(token_weight), K(doc_weight), K(relevance));
          }
        }
      }
    }
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
