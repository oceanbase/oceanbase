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
#include "sql/das/search/ob_i_das_search_op.h" // TODO: remove this dependency when dependency problem of ob_das_search_context.h is solved
#include "sql/das/search/ob_das_search_context.h"
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

ObBM25IndexParamEstimator::ObBM25IndexParamEstimator()
  : ir_scan_ctdef_(nullptr),
    tablet_id_(),
    inv_scan_param_(nullptr),
    param_cache_(nullptr),
    can_est_by_sum_skip_index_(false),
    need_est_avg_doc_token_cnt_(false),
    estimated_(false),
    is_inited_(false)
{
}

void ObBM25IndexParamEstimator::reset()
{
  ir_scan_ctdef_ = nullptr;
  inv_scan_param_ = nullptr;
  param_cache_ = nullptr;
  can_est_by_sum_skip_index_ = false;
  need_est_avg_doc_token_cnt_ = false;
  estimated_ = false;
  is_inited_ = false;
}

int ObBM25IndexParamEstimator::init(
    const sql::ObDASIRScanCtDef &ir_scan_ctdef,
    const common::ObTabletID &inv_idx_tablet_id,
    ObDASIRScanRtDef &ir_scan_rtdef,
    ObTableScanParam &inv_scan_param,
    ObInvIdxParamCache &param_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ir_scan_ctdef.need_calc_relevance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to estimate bm25 param when not needed to calc relevance", K(ret));
  } else {
    ir_scan_ctdef_ = &ir_scan_ctdef;
    ir_scan_rtdef_ = &ir_scan_rtdef;
    tablet_id_ = inv_idx_tablet_id;
    inv_scan_param_ = &inv_scan_param;
    param_cache_ = &param_cache;
    estimated_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObBM25IndexParamEstimator::do_estimation(sql::ObDASSearchCtx &search_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(param_cache_->get_total_doc_cnt(total_doc_cnt_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("failed to get total doc cnt", K(ret));
    } else if (OB_FAIL(do_total_doc_cnt_estimation_on_demand(search_ctx))) {
      LOG_WARN("failed to do total doc cnt estimation on demand", K(ret));
    }
  }

  if (FAILEDx(param_cache_->get_avg_doc_token_cnt(tablet_id_, avg_doc_token_cnt_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret && OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("failed to get avg doc token cnt", K(ret));
    } else if (OB_FAIL(do_avg_doc_token_cnt_estimation_on_demand(*ir_scan_rtdef_->eval_ctx_))) {
      LOG_WARN("failed to do avg doc token cnt estimation on demand", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    estimated_ = true;
  }
  return ret;
}

int ObBM25IndexParamEstimator::get_bm25_param(uint64_t &total_doc_cnt, double &avg_doc_token_cnt) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!estimated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param not estimated yet", K(ret));
  } else {
    total_doc_cnt = total_doc_cnt_;
    avg_doc_token_cnt = avg_doc_token_cnt_;
  }
  return ret;
}

int ObBM25IndexParamEstimator::do_total_doc_cnt_estimation_on_demand(sql::ObDASSearchCtx &search_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t estimated_total_doc_cnt = ir_scan_ctdef_->estimated_total_doc_cnt_;
  if (estimated_total_doc_cnt > 0) {
    total_doc_cnt_ = estimated_total_doc_cnt;
  } else {
    // get total doc cnt by aggregation
    ObTableScanParam total_doc_cnt_scan_param;
    ObTabletID total_doc_cnt_tablet_id;
    const ObDASScalarScanCtDef *total_doc_cnt_ctdef = ir_scan_ctdef_->get_doc_agg_scalar_ctdef();
    ObDASScalarScanRtDef *total_doc_cnt_rtdef = ir_scan_rtdef_->get_doc_agg_scalar_rtdef();
    ObExpr *total_doc_cnt_expr = nullptr;
    ObNewRowIterator *total_doc_cnt_iter = nullptr;
    if (OB_ISNULL(total_doc_cnt_ctdef) || OB_ISNULL(total_doc_cnt_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret), KP(total_doc_cnt_ctdef), KP(total_doc_cnt_rtdef));
    } else if (OB_ISNULL(total_doc_cnt_expr = total_doc_cnt_ctdef->pd_expr_spec_.pd_storage_aggregate_output_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else if (OB_FAIL(search_ctx.get_related_tablet_id(total_doc_cnt_ctdef, total_doc_cnt_tablet_id))) {
      LOG_WARN("failed to get related tablet id", K(ret));
    } else if (OB_FAIL(search_ctx.init_scan_param(
        total_doc_cnt_tablet_id, total_doc_cnt_ctdef, total_doc_cnt_rtdef, total_doc_cnt_scan_param))) {
      LOG_WARN("failed to init total doc cnt scan param", K(ret));
    } else if (OB_FAIL(MTL(ObAccessService *)->table_scan(total_doc_cnt_scan_param, total_doc_cnt_iter))) {
      LOG_WARN("failed to table scan", K(ret));
    } else if (OB_ISNULL(total_doc_cnt_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt iter", K(ret));
    } else if (OB_FAIL(total_doc_cnt_iter->get_next_row())) {
      LOG_WARN("failed to get next row", K(ret));
    } else {
      ObEvalCtx *eval_ctx = total_doc_cnt_rtdef->eval_ctx_;
      if (total_doc_cnt_expr->enable_rich_format()
          && is_valid_format(total_doc_cnt_expr->get_format(*eval_ctx))) {
        total_doc_cnt_ = total_doc_cnt_expr->get_vector(*eval_ctx)->get_int(0);
      } else {
        total_doc_cnt_ = total_doc_cnt_expr->locate_expr_datum(*eval_ctx, 0).get_int();
      }
    }

    if (nullptr != total_doc_cnt_iter) {
      MTL(ObAccessService *)->revert_scan_iter(total_doc_cnt_iter);
    }
  }

  if (FAILEDx(param_cache_->set_total_doc_cnt(total_doc_cnt_))) {
    LOG_WARN("failed to set total doc cnt", K(ret));
  }
  return ret;
}

int ObBM25IndexParamEstimator::do_avg_doc_token_cnt_estimation_on_demand(sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const ObTextAvgDocLenEstSpec &doc_len_est_spec = ir_scan_ctdef_->avg_doc_len_est_spec_;
  const bool need_est_avg_doc_token_cnt = ir_scan_ctdef_->need_avg_doc_len_est();
  const bool can_est_by_sum_skip_index = doc_len_est_spec.can_est_by_sum_skip_index_;
  if (need_est_avg_doc_token_cnt && OB_UNLIKELY(!doc_len_est_spec.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid doc len est spec", K(ret));
  } else if (!need_est_avg_doc_token_cnt || !can_est_by_sum_skip_index) {
    avg_doc_token_cnt_ = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
  } else {
    ObBlockStatScanParam doc_length_est_param;
    ObSEArray<ObSkipIndexColMeta, 1> skip_index_cols;
    if (OB_FAIL(skip_index_cols.push_back(
        ObSkipIndexColMeta(doc_len_est_spec.col_store_idxes_.at(0), doc_len_est_spec.col_types_.at(0))))) {
      LOG_WARN("failed to push back skip index col meta", K(ret));
    } else if (OB_FAIL(doc_length_est_param.init(
        skip_index_cols, doc_len_est_spec.scan_col_proj_, *inv_scan_param_, true, true, true))) {
      LOG_WARN("failed to init doc length est param", K(ret));
    } else {
      ObTextAvgDocLenEstimator avg_doc_len_estimator(total_doc_cnt_, doc_length_est_param);
      if (OB_FAIL(avg_doc_len_estimator.estimate_avg_doc_len(
          *ir_scan_ctdef_->avg_doc_token_cnt_expr_, eval_ctx, avg_doc_token_cnt_))) {
        LOG_WARN("failed to estimate avg doc len", K(ret));
      }
    }
  }

  if (FAILEDx(param_cache_->set_avg_doc_token_cnt(tablet_id_, avg_doc_token_cnt_))) {
    LOG_WARN("failed to set avg doc token cnt", K(ret));
  }
  return ret;
}

ObBM25ParamMultiEstCtx::ObBM25ParamMultiEstCtx()
  : estimated_total_doc_cnt_(0),
    total_doc_cnt_iter_(nullptr),
    total_doc_cnt_exprs_(),
    avg_doc_token_cnt_exprs_(),
    doc_length_est_params_(),
    doc_length_est_stat_cols_(),
    can_est_by_sum_skip_index_(),
    need_est_avg_doc_token_cnt_(false)
{
}

int ObBM25ParamMultiEstCtx::init(const ObIArray<const sql::ObDASIRScanCtDef *> &ir_scan_ctdefs,
                            ObIArray<ObTableScanParam *> &inv_scan_params,
                            ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  const int64_t field_cnt = ir_scan_ctdefs.count();
  total_doc_cnt_exprs_.set_allocator(alloc);
  avg_doc_token_cnt_exprs_.set_allocator(alloc);
  doc_length_est_params_.set_allocator(alloc);
  doc_length_est_stat_cols_.set_allocator(alloc);
  can_est_by_sum_skip_index_.set_allocator(alloc);
  if (OB_UNLIKELY(ir_scan_ctdefs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no ir scan ctdefs", K(ret));
  } else if (OB_FAIL(total_doc_cnt_exprs_.init(field_cnt))) {
    LOG_WARN("failed to init total doc cnt exprs", K(ret));
  } else if (OB_FAIL(total_doc_cnt_exprs_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate total doc cnt exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      if (OB_ISNULL(ir_scan_ctdefs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ir scan ctdef", K(ret));
      } else {
        total_doc_cnt_exprs_.at(i) = ir_scan_ctdefs.at(i)->get_doc_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
        if (!inv_scan_params.empty() && OB_ISNULL(total_doc_cnt_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null total doc cnt exprs", K(ret));
        }
      }
    }
    estimated_total_doc_cnt_ = ir_scan_ctdefs.at(0)->estimated_total_doc_cnt_;
    need_est_avg_doc_token_cnt_ = ir_scan_ctdefs.at(0)->need_avg_doc_len_est();
  }
  if (OB_FAIL(ret)) {
  } else if (!need_est_avg_doc_token_cnt_) {
    // skip the following
  } else if (OB_FAIL(avg_doc_token_cnt_exprs_.init(field_cnt))) {
    LOG_WARN("failed to init avg doc token cnt exprs", K(ret));
  } else if (OB_FAIL(avg_doc_token_cnt_exprs_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate avg doc token cnt exprs", K(ret));
  } else if (OB_FAIL(doc_length_est_params_.init(field_cnt))) {
    LOG_WARN("failed to init doc length est params", K(ret));
  } else if (OB_FAIL(doc_length_est_params_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate doc length est params", K(ret));
  } else if (OB_FAIL(doc_length_est_stat_cols_.init(field_cnt))) {
    LOG_WARN("failed to init doc length est stat cols", K(ret));
  } else if (OB_FAIL(doc_length_est_stat_cols_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate doc length est stat cols", K(ret));
  } else if (OB_FAIL(can_est_by_sum_skip_index_.init(field_cnt))) {
    LOG_WARN("failed to init can est by num skip index", K(ret));
  } else if (OB_FAIL(can_est_by_sum_skip_index_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate can est by num skip index", K(ret));
  } else {
    const int64_t token_cnt = inv_scan_params.count() / field_cnt;
    for (int64_t i = 0; i < field_cnt; ++i) {
      const ObTextAvgDocLenEstSpec &doc_len_est_spec = ir_scan_ctdefs.at(i)->avg_doc_len_est_spec_;
      avg_doc_token_cnt_exprs_.at(i) = ir_scan_ctdefs.at(i)->avg_doc_token_cnt_expr_;
      can_est_by_sum_skip_index_.at(i) = doc_len_est_spec.can_est_by_sum_skip_index_;
      if (OB_ISNULL(avg_doc_token_cnt_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null avg doc token cnt exprs", K(ret));
      } else if (inv_scan_params.empty() || !can_est_by_sum_skip_index_.at(i)) {
        // skip the following
      } else if (OB_UNLIKELY(!doc_len_est_spec.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid doc len est spec", K(ret));
      } else if (OB_FAIL(doc_length_est_stat_cols_.at(i).push_back(ObSkipIndexColMeta(
          doc_len_est_spec.col_store_idxes_.at(0), doc_len_est_spec.col_types_.at(0))))) {
        LOG_WARN("failed to append skip index col meta", K(ret));
      } else if (OB_FAIL(doc_length_est_params_.at(i).init(
          doc_length_est_stat_cols_.at(i), doc_len_est_spec.scan_col_proj_,
          *inv_scan_params.at(i * token_cnt), true, true, true))) {
        LOG_WARN("failed to init doc length est param", K(ret));
      }
    }
  }
  return ret;
}

void ObBM25ParamMultiEstCtx::reset()
{
  estimated_total_doc_cnt_ = 0;
  total_doc_cnt_iter_ = nullptr;
  total_doc_cnt_exprs_.reset();
  avg_doc_token_cnt_exprs_.reset();
  doc_length_est_params_.reset();
  doc_length_est_stat_cols_.reset();
  can_est_by_sum_skip_index_.reset();
  need_est_avg_doc_token_cnt_ = false;
}

ObBM25ParamMultiEstimator::ObBM25ParamMultiEstimator()
  : est_ctx_(),
    total_doc_cnt_(0),
    avg_doc_token_cnts_(),
    estimated_(false),
    is_inited_(false)
{
}

void ObBM25ParamMultiEstimator::reset()
{
  est_ctx_->reset();
  total_doc_cnt_ = 0;
  avg_doc_token_cnts_.reset();
  estimated_ = false;
  is_inited_ = false;
}

void ObBM25ParamMultiEstimator::reuse(const bool switch_tablet)
{
  // only re-estimate when switch scanning inverted index tablet
  if (switch_tablet) {
    total_doc_cnt_ = 0;
    for (int64_t i = 0; i < avg_doc_token_cnts_.count(); ++i) {
      avg_doc_token_cnts_.at(i) = 0.0;
    }
    estimated_ = false;
  }
}

int ObBM25ParamMultiEstimator::init(ObBM25ParamMultiEstCtx *est_ctx, ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(est_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null est ctx", K(ret), KP(est_ctx));
  } else if (est_ctx->need_est_avg_doc_token_cnt_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < est_ctx->avg_doc_token_cnt_exprs_.count(); ++i) {
      if (OB_ISNULL(est_ctx->avg_doc_token_cnt_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null avg doc token cnt expr", K(ret));
      } else if (OB_UNLIKELY(T_PSEUDO_COLUMN != est_ctx->avg_doc_token_cnt_exprs_.at(i)->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected avg doc token cnt expr type", K(ret),
                 KP(est_ctx), K(est_ctx->avg_doc_token_cnt_exprs_.at(i)->type_));
      }
    }
  }
  const int64_t size = est_ctx->total_doc_cnt_exprs_.count();
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(avg_doc_token_cnts_.set_allocator(alloc))) {
  } else if (OB_FAIL(avg_doc_token_cnts_.init(size))) {
    LOG_WARN("failed to init avg doc token cnts", K(ret));
  } else if (OB_FAIL(avg_doc_token_cnts_.prepare_allocate(size))) {
    LOG_WARN("failed to prepare allocate avg doc token cnts", K(ret));
  } else {
    est_ctx_ = est_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBM25ParamMultiEstimator::do_estimation(sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  guard.set_batch_idx(0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!estimated_) {
    // skip repeated estimation
    if (est_ctx_->estimated_total_doc_cnt_ <= 0) {
      if (OB_ISNULL(est_ctx_->total_doc_cnt_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null total doc cnt expr", K(ret));
      } else if (OB_FAIL(est_ctx_->total_doc_cnt_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from total doc cnt iter", K(ret));
        } else {
          total_doc_cnt_ = 0;
        }
      } else if (OB_ISNULL(est_ctx_->total_doc_cnt_exprs_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null total doc cnt expr", K(ret));
      } else {
        if (est_ctx_->total_doc_cnt_exprs_.at(0)->enable_rich_format()
            && is_valid_format(est_ctx_->total_doc_cnt_exprs_.at(0)->get_format(eval_ctx))) {
          total_doc_cnt_ = est_ctx_->total_doc_cnt_exprs_.at(0)->get_vector(eval_ctx)->get_int(0);
        } else {
          total_doc_cnt_ = est_ctx_->total_doc_cnt_exprs_.at(0)->locate_expr_datum(eval_ctx, 0)
              .get_int();
        }
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < est_ctx_->total_doc_cnt_exprs_.count(); ++i) {
        if (OB_ISNULL(est_ctx_->total_doc_cnt_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null total doc cnt expr", K(ret));
        } else {
          ObDatum &datum = est_ctx_->total_doc_cnt_exprs_.at(i)->locate_datum_for_write(eval_ctx);
          datum.set_int(total_doc_cnt_);
        }
      }
    } else if (OB_UNLIKELY(est_ctx_->total_doc_cnt_exprs_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty total doc cnt exprs", K(ret));
    } else {
      total_doc_cnt_ = est_ctx_->estimated_total_doc_cnt_;
      for (int64_t i = 0; OB_SUCC(ret) && i < est_ctx_->total_doc_cnt_exprs_.count(); ++i) {
        ObDatum &datum = est_ctx_->total_doc_cnt_exprs_.at(i)->locate_datum_for_write(eval_ctx);
        datum.set_int(total_doc_cnt_);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (est_ctx_->need_est_avg_doc_token_cnt_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < est_ctx_->doc_length_est_params_.count(); ++i) {
        if (OB_ISNULL(est_ctx_->avg_doc_token_cnt_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null avg doc token cnt expr", K(ret));
        } else if (est_ctx_->can_est_by_sum_skip_index_.at(i)) {
          ObTextAvgDocLenEstimator avg_doc_len_estimator(
              total_doc_cnt_, est_ctx_->doc_length_est_params_.at(i));
          if (OB_FAIL(avg_doc_len_estimator.estimate_avg_doc_len(
              *est_ctx_->avg_doc_token_cnt_exprs_.at(i), eval_ctx, avg_doc_token_cnts_.at(i)))) {
            LOG_WARN("failed to estimate avg doc len", K(ret));
          }
        } else {
          est_ctx_->avg_doc_token_cnt_exprs_.at(i)->locate_datum_for_write(eval_ctx)
              .set_double(sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT);
          avg_doc_token_cnts_.at(i) = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
        }
      }
    } else {
      for (int64_t i = 0; i < avg_doc_token_cnts_.count(); ++i) {
        avg_doc_token_cnts_.at(i) = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
      }
    }

    if (OB_SUCC(ret)) {
      estimated_ = true;
      LOG_TRACE("[Sparse Retrieval] estimated total doc cnt for bm25 param", K(ret),
                K_(total_doc_cnt), K_(avg_doc_token_cnts));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
