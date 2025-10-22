/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "ob_block_stat_iter.h"
#include "ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace storage
{

int ObTextAvgDocLenEstimator::estimate_avg_doc_len(
    sql::ObExpr &avg_doc_token_cnt_expr,
    sql::ObEvalCtx &eval_ctx,
    double &result)
{
  int ret = OB_SUCCESS;
  result = 0.0;
  ObBlockStatIterator stat_iter;
  ObStorageDatum tmp_result;
  number::ObNumber zero_num;
  zero_num.set_zero();
  tmp_result.set_number(zero_num);
  if (OB_FAIL(MTL(ObAccessService *)->scan_block_stat(doc_length_est_param_, stat_iter))) {
    LOG_WARN("failed to scan block stat", K(ret));
  }
  sql::ObNumStackAllocator<2> tmp_alloc;

  while (OB_SUCC(ret)) {
    tmp_alloc.free();
    const ObDatumRow *agg_row = nullptr;
    const ObDatumRowkey *endkey = nullptr;
    if (OB_FAIL(stat_iter.get_next(agg_row, endkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next agg row", K(ret));
      }
    } else if (OB_ISNULL(agg_row)
        || OB_UNLIKELY(agg_row->get_column_count() != 1)
        || OB_UNLIKELY(agg_row->storage_datums_[0].is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg row", K(ret), KPC(agg_row));
    } else {
      const ObStorageDatum &datum = agg_row->storage_datums_[0];
      number::ObNumber left_num(tmp_result.get_number());
      number::ObNumber right_num(datum.get_number());
      number::ObNumber result_num;
      if (OB_FAIL(left_num.add_v3(right_num, result_num, tmp_alloc))) {
        LOG_WARN("failed to add numbers", K(ret), K(left_num), K(right_num), K(result_num));
      } else {
        tmp_result.set_number(result_num);
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    tmp_alloc.free();
    number::ObNumber doc_len_num(tmp_result.get_number());
    number::ObNumber doc_cnt_num;
    number::ObNumber result_num;
    double avg_doc_token_cnt = 0.0;
    if (OB_UNLIKELY(0 == total_doc_cnt_)) {
      // use DEFAULT_AVG_DOC_TOKEN_CNT
    } else if (OB_FAIL(doc_cnt_num.from(total_doc_cnt_, tmp_alloc))) {
      LOG_WARN("failed to create number from int", K(ret), K(total_doc_cnt_));
    } else if (OB_FAIL(doc_len_num.div_v3(doc_cnt_num, result_num, tmp_alloc))) {
      LOG_WARN("failed to divide numbers", K(ret), K(total_doc_cnt_), K(doc_cnt_num), K(tmp_result.get_number()));
    } else if (OB_UNLIKELY(avg_doc_token_cnt_expr.datum_meta_.get_type() != ObDoubleType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected avg doc token cnt expr type", K(ret), K(avg_doc_token_cnt_expr.datum_meta_));
    } else if (OB_FAIL(cast_number_to_double(result_num, avg_doc_token_cnt))) {
      LOG_WARN("failed to cast number to double", K(ret), K(result_num));
    }

    if (OB_SUCC(ret)) {
      const double default_avg_doc_token_cnt = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
      result = avg_doc_token_cnt > default_avg_doc_token_cnt ? avg_doc_token_cnt : default_avg_doc_token_cnt;
      avg_doc_token_cnt_expr.locate_datum_for_write(eval_ctx).set_double(result);
      LOG_DEBUG("[Sparse Retrieval] estimated avg doc token cnt",
          K(doc_len_num), K(doc_cnt_num), K(result_num), K(avg_doc_token_cnt), K(result));
    }
  }

  return ret;
}

int ObTextAvgDocLenEstimator::cast_number_to_double(const number::ObNumber &num, double &result)
{
  int ret = OB_SUCCESS;
  result = 0.0;
  ObObj src_obj, dest_obj;
  src_obj.set_number(num);

  ObArenaAllocator alloc("TxtNum2Dbl", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObCastCtx cast_ctx(&alloc, nullptr, CM_NONE, ObCharset::get_system_collation());
  if (OB_FAIL(ObObjCaster::to_type(ObDoubleType, cast_ctx, src_obj, dest_obj))) {
    LOG_WARN("failed to cast number to double", K(ret));
  } else if (OB_FAIL(dest_obj.get_double(result))) {
    LOG_WARN("failed to get double from casted obj", K(ret));
  }
  return ret;
}

ObBM25ParamEstCtx::ObBM25ParamEstCtx()
  : estimated_total_doc_cnt_(0),
    total_doc_cnt_iter_(nullptr),
    total_doc_cnt_expr_(nullptr),
    avg_doc_token_cnt_expr_(nullptr),
    doc_length_est_param_(nullptr),
    can_est_by_sum_skip_index_(false),
    need_est_avg_doc_token_cnt_(false)
{
}

bool ObBM25ParamEstCtx::is_valid() const
{
  bool bret = (estimated_total_doc_cnt_ > 0 || total_doc_cnt_expr_ != nullptr)
      && (!need_est_avg_doc_token_cnt_ || avg_doc_token_cnt_expr_ != nullptr)
      && doc_length_est_param_ != nullptr;
  return bret;
}

int ObBM25ParamEstCtx::assign(const ObBM25ParamEstCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(other));
  } else {
    estimated_total_doc_cnt_ = other.estimated_total_doc_cnt_;
    total_doc_cnt_iter_ = other.total_doc_cnt_iter_;
    total_doc_cnt_expr_ = other.total_doc_cnt_expr_;
    avg_doc_token_cnt_expr_ = other.avg_doc_token_cnt_expr_;
    doc_length_est_param_ = other.doc_length_est_param_;
    can_est_by_sum_skip_index_ = other.can_est_by_sum_skip_index_;
    need_est_avg_doc_token_cnt_ = other.need_est_avg_doc_token_cnt_;
  }
  return ret;
}

void ObBM25ParamEstCtx::reset()
{
  estimated_total_doc_cnt_ = 0;
  total_doc_cnt_iter_ = nullptr;
  total_doc_cnt_expr_ = nullptr;
  avg_doc_token_cnt_expr_ = nullptr;
  doc_length_est_param_ = nullptr;
  can_est_by_sum_skip_index_ = false;
  need_est_avg_doc_token_cnt_ = false;
}

ObBM25ParamEstimator::ObBM25ParamEstimator()
  : est_ctx_(),
    total_doc_cnt_(0),
    avg_doc_token_cnt_(0.0),
    estimated_(false),
    is_inited_(false)
{
}

void ObBM25ParamEstimator::reset()
{
  est_ctx_.reset();
  total_doc_cnt_ = 0;
  avg_doc_token_cnt_ = 0.0;
  estimated_ = false;
  is_inited_ = false;
}

void ObBM25ParamEstimator::reuse(const bool switch_tablet)
{
  // only re-estimate when switch scanning inverted index tablet
  if (switch_tablet) {
    total_doc_cnt_ = 0;
    avg_doc_token_cnt_ = 0.0;
    estimated_ = false;
  }
}

int ObBM25ParamEstimator::init(const ObBM25ParamEstCtx &est_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!est_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid est ctx", K(ret), K(est_ctx));
  } else if (OB_UNLIKELY(est_ctx.need_est_avg_doc_token_cnt_
      && est_ctx.avg_doc_token_cnt_expr_->type_ != T_PSEUDO_COLUMN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected avg doc token cnt expr type", K(ret), K(est_ctx), K(est_ctx.avg_doc_token_cnt_expr_->type_));
  } else if (OB_FAIL(est_ctx_.assign(est_ctx))) {
    LOG_WARN("failed to assign est ctx", K(ret), K(est_ctx));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBM25ParamEstimator::do_estimation(sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  guard.set_batch_idx(0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!estimated_) {
    // skip repeated estimation
    if (est_ctx_.estimated_total_doc_cnt_ <= 0) {
      if (OB_ISNULL(est_ctx_.total_doc_cnt_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null total doc cnt expr", K(ret));
      } else if (OB_FAIL(est_ctx_.total_doc_cnt_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from total doc cnt iter", K(ret));
        } else {
          total_doc_cnt_ = 0;
        }
      } else {
        if (est_ctx_.total_doc_cnt_expr_->enable_rich_format()
            && is_valid_format(est_ctx_.total_doc_cnt_expr_->get_format(eval_ctx))) {
          total_doc_cnt_ = est_ctx_.total_doc_cnt_expr_->get_vector(eval_ctx)->get_int(0);
        } else {
          total_doc_cnt_ = est_ctx_.total_doc_cnt_expr_->locate_expr_datum(eval_ctx, 0).get_int();
        }
      }
    } else if (OB_ISNULL(est_ctx_.total_doc_cnt_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else {
      ObDatum &total_doc_cnt = est_ctx_.total_doc_cnt_expr_->locate_datum_for_write(eval_ctx);
      total_doc_cnt.set_int(est_ctx_.estimated_total_doc_cnt_);
      total_doc_cnt_ = est_ctx_.estimated_total_doc_cnt_;
    }

    if (OB_FAIL(ret)) {
    } else if (est_ctx_.need_est_avg_doc_token_cnt_) {
      if (est_ctx_.can_est_by_sum_skip_index_) {
        ObTextAvgDocLenEstimator avg_doc_len_estimator(total_doc_cnt_, *est_ctx_.doc_length_est_param_);
        if (OB_FAIL(avg_doc_len_estimator.estimate_avg_doc_len(*est_ctx_.avg_doc_token_cnt_expr_, eval_ctx, avg_doc_token_cnt_))) {
          LOG_WARN("failed to estimate avg doc len", K(ret));
        }
      } else {
        est_ctx_.avg_doc_token_cnt_expr_->locate_datum_for_write(eval_ctx).set_double(sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT);
        avg_doc_token_cnt_ = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
      }
    } else {
      avg_doc_token_cnt_ = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
    }

    if (OB_SUCC(ret)) {
      estimated_ = true;
      LOG_TRACE("[Sparse Retrieval] estimated total doc cnt for bm25 param", K(ret), K_(total_doc_cnt), K_(avg_doc_token_cnt));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
