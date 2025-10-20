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

#define USING_LOG_PREFIX STORAGE

#include "ob_text_retrieval_token_iter.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/das/iter/sparse_retrieval/ob_das_tr_merge_iter.h"

namespace oceanbase
{
namespace storage
{
ObTextRetrievalTokenIter::ObTextRetrievalTokenIter()
  : ObISparseRetrievalDimIter(),
    mem_context_(nullptr),
    allocator_(nullptr),
    inv_idx_scan_param_(nullptr),
    inv_idx_agg_param_(nullptr),
    fwd_idx_scan_param_(nullptr),
    inv_idx_scan_iter_(nullptr),
    inv_idx_agg_iter_(nullptr),
    fwd_idx_agg_iter_(nullptr),
    inv_idx_agg_expr_(nullptr),
    fwd_idx_agg_expr_(nullptr),
    eval_ctx_(nullptr),
    relevance_expr_(nullptr),
    inv_scan_doc_length_col_(nullptr),
    inv_scan_domain_id_col_(nullptr),
    doc_token_cnt_expr_(nullptr),
    relevance_calc_exprs_(),
    skip_(nullptr),
    fwd_range_objs_(nullptr),
    max_batch_size_(0),
    token_doc_cnt_(0),
    max_token_relevance_(-1.0),
    advance_doc_id_(),
    token_doc_cnt_calculated_(false),
    inv_idx_agg_cache_mode_(false),
    is_inited_(false)
{
}

int ObTextRetrievalTokenIter::init(const ObTextRetrievalScanIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  allocator_ = iter_param.allocator_;
  mem_context_ = iter_param.mem_context_;
  inv_idx_scan_param_ = iter_param.inv_idx_scan_param_;
  inv_idx_agg_param_ = iter_param.inv_idx_agg_param_;
  fwd_idx_scan_param_ = iter_param.fwd_idx_scan_param_;
  inv_idx_scan_iter_ = iter_param.inv_idx_scan_iter_;
  inv_idx_agg_iter_ = iter_param.inv_idx_agg_iter_;
  inv_idx_agg_expr_ = iter_param.inv_idx_agg_expr_;
  fwd_idx_agg_iter_ = iter_param.fwd_idx_agg_iter_;
  fwd_idx_agg_expr_ = iter_param.fwd_idx_agg_expr_;
  eval_ctx_ = iter_param.eval_ctx_;
  relevance_expr_ = iter_param.relevance_expr_;
  inv_scan_doc_length_col_ = iter_param.inv_scan_doc_length_col_;
  inv_scan_domain_id_col_ = iter_param.inv_scan_domain_id_col_;
  max_batch_size_ = OB_MAX(iter_param.eval_ctx_->max_batch_size_, 1);
  inv_idx_agg_cache_mode_ = iter_param.inv_idx_agg_cache_mode_;

  if (OB_ISNULL(inv_idx_scan_iter_) || OB_ISNULL(eval_ctx_) || OB_ISNULL(inv_idx_scan_param_) ||
      OB_ISNULL(inv_scan_domain_id_col_) || OB_ISNULL(allocator_) || OB_ISNULL(mem_context_.ref_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_iter or eval_ctx is NULL", K(ret), K_(inv_idx_scan_iter), KPC_(eval_ctx), KPC_(inv_idx_scan_param), KP(allocator_), KP(mem_context_.ref_context()));
  } else if (!need_calc_relevance()) {
  } else if (OB_ISNULL(relevance_expr_) || OB_ISNULL(inv_idx_agg_expr_) || OB_ISNULL(inv_idx_agg_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null relevance expr", K(ret), KPC_(relevance_expr), KPC_(inv_idx_agg_expr), KPC_(inv_idx_agg_iter));
  } else if (!need_fwd_idx_agg() && (OB_ISNULL(inv_scan_doc_length_col_))) {
  } else if (need_fwd_idx_agg() && (OB_ISNULL(fwd_idx_agg_iter_) || OB_ISNULL(fwd_idx_agg_expr_) || OB_ISNULL(fwd_idx_scan_param_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fwd_idx_agg_iter or fwd_idx_agg_expr is NULL", K(ret), K_(fwd_idx_agg_iter), KPC_(fwd_idx_agg_expr), KPC_(fwd_idx_scan_param));
  } else if (OB_FAIL(init_calc_exprs_in_relevance_expr())) {
    LOG_WARN("failed to init row-wise calc exprs", K(ret));
  } else if (OB_ISNULL(doc_token_cnt_expr_) || OB_UNLIKELY(doc_token_cnt_expr_->datum_meta_.get_type() != ObDecimalIntType && doc_token_cnt_expr_->datum_meta_.get_type() != ObNumberType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KPC_(doc_token_cnt_expr), KPC_(inv_scan_doc_length_col), KPC_(eval_ctx));
  } else if (OB_ISNULL(skip_ = to_bit_vector(allocator_->alloc(ObBitVector::memory_size(max_batch_size_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate skip bit vector", K(ret));
  } else {
    skip_->init(max_batch_size_);
  }
  is_inited_ = true;
  return ret;
}

void ObTextRetrievalTokenIter::reset()
{
  relevance_calc_exprs_.reset();
  token_doc_cnt_calculated_ = false;
}

void ObTextRetrievalTokenIter::reuse()
{
  if (inv_idx_agg_cache_mode_ && !inv_idx_agg_param_->need_switch_param_) {
    // do nothing
  } else {
    token_doc_cnt_calculated_ = false;
  }
}

int ObTextRetrievalTokenIter::init_calc_exprs_in_relevance_expr()
{
  int ret = OB_SUCCESS;
  if (need_calc_relevance()) {
    if (OB_ISNULL(relevance_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null relevance expr", K(ret));
    } else if (OB_FAIL(relevance_calc_exprs_.push_back(relevance_expr_))) {
      LOG_WARN("failed to append relevance expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < relevance_expr_->arg_cnt_; ++i) {
      sql::ObExpr *arg_expr = relevance_expr_->args_[i];
      if (OB_ISNULL(arg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null arg expr", K(ret));
      } else if (T_FUN_SYS_CAST == arg_expr->type_) {
        // cast expr is evaluated with relevance expr
        if (OB_FAIL(relevance_calc_exprs_.push_back(arg_expr))) {
          LOG_WARN("failed to append cast expr", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      sql::ObExpr *doc_token_cnt_param_expr = relevance_expr_->args_[sql::ObExprBM25::DOC_TOKEN_CNT_PARAM_IDX];
      if (T_FUN_SYS_CAST == doc_token_cnt_param_expr->type_) {
        doc_token_cnt_param_expr = doc_token_cnt_param_expr->args_[0];
      }
      if (OB_UNLIKELY(doc_token_cnt_param_expr->type_ != T_FUN_SUM)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected doc token cnt expr type", K(ret), KPC(doc_token_cnt_param_expr));
      } else {
        doc_token_cnt_expr_ = doc_token_cnt_param_expr;
        // update the locate datums
        if (max_batch_size_ > 0) {
          doc_token_cnt_expr_->locate_datums_for_update(*eval_ctx_, max_batch_size_);
        }
      }
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::get_token_doc_cnt(int64_t &token_doc_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!token_doc_cnt_calculated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get token doc cnt before inv_idx_agg evaluated", K(ret));
  } else {
    token_doc_cnt = token_doc_cnt_;
  }
  return ret;
}

void ObTextRetrievalTokenIter::clear_row_wise_evaluated_flag()
{
  for (int64_t i = 0; i < relevance_calc_exprs_.count(); ++i) {
    sql::ObExpr *expr = relevance_calc_exprs_.at(i);
    expr->clear_evaluated_flag(*eval_ctx_);
  }
}

int ObTextRetrievalTokenIter::get_next_doc_token_cnt(const bool use_fwd_idx_agg)
{
  int ret = OB_SUCCESS;
  if (use_fwd_idx_agg) {
    sql::ObDocIdExt cur_doc_id;
    if (OB_FAIL(get_inv_idx_scan_doc_id(cur_doc_id))) {
      LOG_WARN("failed to get current doc id", K(ret));
    } else if (OB_FAIL(do_token_cnt_agg(cur_doc_id))) {
      LOG_WARN("failed to do token count agg on fwd index", K(ret));
    }
  } else {
    if (OB_FAIL(fill_token_cnt_with_doc_len())) {
      LOG_WARN("failed to fill token cnt with document length", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::get_inv_idx_scan_doc_id(ObDocIdExt &doc_id)
{
  int ret = OB_SUCCESS;
  ObDatum &doc_id_datum = inv_scan_domain_id_col_->locate_expr_datum(*eval_ctx_);
  if (OB_FAIL(doc_id.from_datum(doc_id_datum))) {
    LOG_WARN("failed to get doc id", K(ret), K(doc_id_datum));
  }

  return ret;
}

int ObTextRetrievalTokenIter::gen_fwd_idx_scan_range(const ObDocIdExt &doc_id, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  if (nullptr == fwd_range_objs_) {
    common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
    void *buf = nullptr;
    constexpr int64_t obj_cnt = FWD_IDX_ROWKEY_COL_CNT * 2;
    if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
    } else if (OB_ISNULL(fwd_range_objs_ = new (buf) ObObj[obj_cnt])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(doc_id.get_datum().to_obj(fwd_range_objs_[0], inv_scan_domain_id_col_->obj_meta_))) {
    LOG_WARN("failed to set obj", K(ret));
  } else if (OB_FAIL(doc_id.get_datum().to_obj(fwd_range_objs_[2], inv_scan_domain_id_col_->obj_meta_))) {
    LOG_WARN("failed to set obj", K(ret));
  } else if (OB_UNLIKELY(fwd_idx_scan_param_->key_ranges_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key range count", K(ret), K(fwd_idx_scan_param_->key_ranges_.count()));
  } else {
    fwd_range_objs_[1].set_min_value();
    fwd_range_objs_[3].set_max_value();
    scan_range.table_id_ = fwd_idx_scan_param_->key_ranges_.at(0).table_id_;
    scan_range.start_key_.assign(fwd_range_objs_, FWD_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&fwd_range_objs_[2], FWD_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObTextRetrievalTokenIter::do_token_cnt_agg(const ObDocIdExt &doc_id)
{
  int ret = OB_SUCCESS;
  int64_t token_count = 0;
  ObNewRange scan_range;
  if (OB_FAIL(gen_fwd_idx_scan_range(doc_id, scan_range))) {
    LOG_WARN("failed to generate forward index scan range", K(ret));
  } else if (OB_FAIL(fwd_idx_agg_iter_->reuse())) {
    LOG_WARN("failed to reuse forward index iter", K(ret));
  } else if (FALSE_IT(fwd_idx_scan_param_->key_ranges_.reuse())) {
    LOG_WARN("failed to reuse forward index scan range", K(ret));
  } else if (OB_FAIL(fwd_idx_scan_param_->key_ranges_.push_back(scan_range))){
    LOG_WARN("failed to add forward index scan range", K(ret), K(scan_range));
  } else if (OB_FAIL(fwd_idx_agg_iter_->rescan())) {
    LOG_WARN("failed to rescan forward index", K(ret));
  } else if (OB_FAIL(fwd_idx_agg_iter_->get_next_row())) {
    LOG_WARN("failed to get next row from forward index iterator", K(ret));
  } else {
    if (is_valid_format(fwd_idx_agg_expr_->get_format(*eval_ctx_))) {
      token_count = fwd_idx_agg_expr_->get_vector(*eval_ctx_)->get_int(0);
    } else {
      token_count = fwd_idx_agg_expr_->locate_expr_datum(*eval_ctx_).get_int();
    }
    LOG_DEBUG("retrieval iterator get token cnt for doc", K(ret), K(doc_id), K(token_count));
  }
  return ret;
}

int ObTextRetrievalTokenIter::fill_token_cnt_with_doc_len()
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *agg_expr = doc_token_cnt_expr_;
  const sql::ObExpr *doc_length_expr = inv_scan_doc_length_col_;
  ObDatum *doc_length_datum = nullptr;
  if (OB_ISNULL(agg_expr) || OB_ISNULL(doc_length_expr) || OB_ISNULL(eval_ctx_)
      || OB_UNLIKELY(agg_expr->datum_meta_.get_type() != ObDecimalIntType && agg_expr->datum_meta_.get_type() != ObNumberType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KPC(agg_expr), KP(doc_length_expr), KP(eval_ctx_));
  } else if (OB_FAIL(doc_length_expr->eval(*eval_ctx_, doc_length_datum))) {
    LOG_WARN("failed to evaluate document length expr", K(ret));
  } else {
    ObDatum &agg_datum = agg_expr->locate_datum_for_write(*eval_ctx_);
    if (agg_expr->datum_meta_.get_type() == ObDecimalIntType) {
      if(OB_FAIL(set_decimal_int_by_precision(agg_datum, doc_length_datum->get_uint(), agg_expr->datum_meta_.precision_))) {
        LOG_WARN("fail to set decimal int", K(ret));
      }
    } else {
      const int64_t in_val = doc_length_datum->get_uint64();
      number::ObNumber nmb;
      if (OB_FAIL(nmb.from(in_val, mem_context_->get_arena_allocator()))) {
        LOG_WARN("fail to int_number", K(ret), K(in_val));
      } else {
        agg_datum.set_number(nmb);
      }
    }

  }
  return ret;
}

int ObTextRetrievalTokenIter::fill_token_doc_cnt()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inv_idx_agg_expr_->datum_meta_.get_type() != ObIntType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KP_(inv_idx_agg_expr), KP_(eval_ctx));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
    guard.set_batch_idx(0);
    ObDatum &doc_cnt_datum = inv_idx_agg_expr_->locate_datum_for_write(*eval_ctx_);
    doc_cnt_datum.set_int(token_doc_cnt_);
  }
  return ret;
}

int ObTextRetrievalTokenIter::eval_relevance_expr()
{
  int ret = OB_SUCCESS;
  ObDatum *relevance_datum = nullptr;
  if (OB_FAIL(relevance_expr_->eval(*eval_ctx_, relevance_datum))) {
    LOG_WARN("failed to evaluate relevance", K(ret));
  }
  return ret;
}

int ObTextRetrievalTokenIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retrieval token iterator not inited", K(ret));
  } else if (!token_doc_cnt_calculated_ && OB_FAIL(estimate_token_doc_cnt())) {
    LOG_WARN("failed to estimate token doc cnt", K(ret));
  } else if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from inverted index", K(ret), K_(inv_idx_scan_param), KPC_(inv_idx_scan_iter));
    }
  } else {
    if (need_calc_relevance()) {
      clear_row_wise_evaluated_flag();
      if (OB_FAIL(get_next_doc_token_cnt(need_fwd_idx_agg()))) {
        LOG_WARN("failed to get next doc token count", K(ret));
      } else if (OB_FAIL(fill_token_doc_cnt())) {
        LOG_WARN("failed to get token doc cnt", K(ret));
      } else if (OB_FAIL(eval_relevance_expr())) {
        LOG_WARN("failed to evaluate simarity expr", K(ret));
      }
    }
  }
  return ret;
}

void ObTextRetrievalTokenIter::clear_batch_wise_evaluated_flag(const int64_t count)
{
  int64_t max_size = OB_MAX(max_batch_size_, count);
  for (int64_t i = 0; i < relevance_calc_exprs_.count(); ++i) {
    sql::ObExpr *expr = relevance_calc_exprs_.at(i);
    expr->get_evaluated_flags(*eval_ctx_).reset(count);
  }
}

int ObTextRetrievalTokenIter::batch_fill_token_cnt_with_doc_len(const int64_t count)
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *agg_expr = doc_token_cnt_expr_;
  const sql::ObExpr *doc_length_expr = inv_scan_doc_length_col_;
  if (need_fwd_idx_agg()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported fwd_idx_agg", K(ret));
  } else if (OB_FAIL(doc_length_expr->eval_batch(*eval_ctx_, *skip_, count))) {
    LOG_WARN("failed to evaluate document length expr", K(ret));
  } else if (OB_UNLIKELY(!doc_length_expr->is_batch_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no batch expr", K(ret), KP(doc_length_expr));
  } else {
    const ObDatum *datums = doc_length_expr->locate_batch_datums(*eval_ctx_);
    ObDatum *agg_datum = agg_expr->locate_batch_datums(*eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_LIKELY(!skip_->at(i))) {
        if (agg_expr->datum_meta_.get_type() == ObDecimalIntType) {
          if (OB_FAIL(set_decimal_int_by_precision(agg_datum[i], datums[i].get_uint(), agg_expr->datum_meta_.precision_))) {
            LOG_WARN("fail to set decimal int", K(ret));
          }
        } else {
          const int64_t in_val = datums[i].get_uint64();
          number::ObNumber nmb;
          if (OB_FAIL(nmb.from(in_val, mem_context_->get_arena_allocator()))) {
            LOG_WARN("fail to int_number", K(ret), K(in_val));
          } else {
            agg_datum[i].set_number(nmb);
          }
        }
      }
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::batch_eval_relevance_expr(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(relevance_expr_->eval_batch(*eval_ctx_, *skip_, count))) {
    LOG_WARN("failed to evaluate relevance", K(ret));
  }
  return ret;
}

int ObTextRetrievalTokenIter::get_next_batch(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retrieval token iterator not inited", K(ret));
  } else if (!token_doc_cnt_calculated_ && OB_FAIL(estimate_token_doc_cnt())) {
    LOG_WARN("failed to estimate token doc cnt", K(ret));
  } else if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(count, OB_MIN(max_batch_size_, capacity)))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows from inverted index", K(ret), KPC_(inv_idx_scan_param), KPC_(inv_idx_scan_iter));
    } else if (count != 0) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_calc_relevance()) {
    const ObBitVector *skip = NULL;
    PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *inv_idx_scan_param_->output_exprs_, count, skip);
    clear_batch_wise_evaluated_flag(count);
    if (OB_FAIL(batch_fill_token_cnt_with_doc_len(count))) {
      LOG_WARN("failed to fill batch token cnt with document length", K(ret));
    } else if (OB_FAIL(fill_token_doc_cnt())) {
      LOG_WARN("failed to get token doc cnt", K(ret));
    } else if (OB_FAIL(batch_eval_relevance_expr(count))) {
      LOG_WARN("failed to evaluate simarity expr", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::advance_to(const ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(advance_doc_id_.from_datum(id_datum))) {
    LOG_WARN("failed to get doc id", K(ret), K(id_datum));
  } else if (OB_UNLIKELY(inv_idx_scan_param_->key_ranges_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key range count", K(ret), K(inv_idx_scan_param_->key_ranges_.count()));
  } else {
    ObRowkey start_rowkey = inv_idx_scan_param_->key_ranges_.at(0).start_key_;
    ObRowkey end_rowkey = inv_idx_scan_param_->key_ranges_.at(0).end_key_;
    if (&start_rowkey.get_obj_ptr()[2] != &end_rowkey.get_obj_ptr()[0]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey", K(ret), K(start_rowkey), K(end_rowkey));
    }
    // obj_ptr[0] is token, obj_ptr[1] is docid
    ObObj *obj_ptr = start_rowkey.get_obj_ptr();
    ObNewRange scan_range;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(advance_doc_id_.get_datum().to_obj(obj_ptr[1], inv_scan_domain_id_col_->obj_meta_))) {
      LOG_WARN("failed to set obj", K(ret));
    } else {
      scan_range.table_id_ = inv_idx_scan_param_->key_ranges_.at(0).table_id_;
      scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
      scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
      scan_range.border_flag_.set_inclusive_start();
      scan_range.border_flag_.set_inclusive_end();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inv_idx_scan_iter_->reuse())) {
      LOG_WARN("failed to reuse inverted index scan iterator", K(ret));
    } else if (OB_UNLIKELY(!inv_idx_scan_param_->key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret), K(inv_idx_scan_param_->key_ranges_));
    } else if (OB_FAIL(inv_idx_scan_param_->key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (OB_FAIL(inv_idx_scan_iter_->rescan())) {
      LOG_WARN("failed to rescan inverted index", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::update_scan_param(const ObString &token, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inv_idx_agg_param_->key_ranges_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key range count", K(ret), K(inv_idx_agg_param_->key_ranges_.count()));
  } else {
    ObNewRange scan_range = inv_idx_agg_param_->key_ranges_.at(0);
    ObObj tmp_obj;
    tmp_obj.set_string(ObVarcharType, token);
    tmp_obj.set_meta_type(scan_range.start_key_.get_obj_ptr()->meta_);
    if (scan_range.start_key_.get_obj_ptr() + 2 != scan_range.end_key_.get_obj_ptr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey", K(ret), K_(scan_range.start_key), K_(scan_range.end_key));
    } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, *scan_range.start_key_.get_obj_ptr()))) {
      LOG_WARN("failed to write obj", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, *scan_range.end_key_.get_obj_ptr()))) {
      LOG_WARN("failed to write obj", K(ret));
    } else if (!need_inv_idx_agg()) {
      // skip inverted index aggregate iterator
    } else if (OB_FAIL(inv_idx_agg_iter_->reuse())) {
      LOG_WARN("failed to reuse inverted index aggregate iterator", K(ret));
    } else if (OB_UNLIKELY(!inv_idx_agg_param_->key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret));
    } else if (OB_FAIL(inv_idx_agg_param_->key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (OB_FAIL(inv_idx_agg_iter_->rescan())) {
      LOG_WARN("failed to rescan inverted aggregate index", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(inv_idx_scan_param_->key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty key ranges", K(ret));
    } else if (inv_idx_scan_param_->key_ranges_.at(0).start_key_.get_obj_ptr()
        == inv_idx_scan_param_->key_ranges_.at(0).end_key_.get_obj_ptr()) {
      // function lookup mode
      ObSEArray<ObNewRange, 4> scan_ranges;
      scan_ranges.assign(inv_idx_scan_param_->key_ranges_);
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count(); ++i) {
        if (OB_FAIL(ob_write_obj(allocator, tmp_obj, *scan_ranges.at(i).start_key_.get_obj_ptr()))) {
          LOG_WARN("failed to write obj", K(ret));
        }
      }
      if (FAILEDx(inv_idx_scan_iter_->reuse())) {
        LOG_WARN("failed to reuse inverted index scan iterator", K(ret));
      } else if (OB_UNLIKELY(!inv_idx_scan_param_->key_ranges_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-empty scan range", K(ret));
      } else if (OB_FAIL(inv_idx_scan_param_->key_ranges_.assign(scan_ranges))) {
        LOG_WARN("failed to push back scan range", K(ret));
      } else if (OB_FAIL(inv_idx_scan_iter_->rescan())) {
        LOG_WARN("failed to rescan inverted index", K(ret));
      }
    } else {
      // non-function lookup mode
      if (OB_FAIL(inv_idx_scan_iter_->reuse())) {
        LOG_WARN("failed to reuse inverted index scan iterator", K(ret));
      } else if (OB_UNLIKELY(!inv_idx_scan_param_->key_ranges_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-empty scan range", K(ret));
      } else if (OB_FAIL(inv_idx_scan_param_->key_ranges_.push_back(scan_range))) {
        LOG_WARN("failed to push back scan range", K(ret));
      } else if (OB_FAIL(inv_idx_scan_iter_->rescan())) {
        LOG_WARN("failed to rescan inverted index", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      reuse();
    }
  }
  return ret;
}

int ObTextRetrievalTokenIter::set_decimal_int_by_precision(ObDatum &result_datum,
                                                           const uint64_t decint,
                                                           const ObPrecision precision)
{
  int ret = OB_SUCCESS;
  if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected precision, precision is too short", K(ret), K(precision));
  } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
    const int128_t result = decint;
    result_datum.set_decimal_int(result);
  } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
    const int256_t result = decint;
    result_datum.set_decimal_int(result);
  } else {
    const int512_t result = decint;
    result_datum.set_decimal_int(result);
  }
  return ret;
}

int ObTextRetrievalTokenIter::estimate_token_doc_cnt()
{
  int ret = OB_SUCCESS;
  int64_t logical_row_cnt = 0;
  int64_t physical_row_cnt = 0;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObArenaAllocator allocator;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  ObAccessService *access_service = NULL;
  storage::ObTableScanRange table_scan_range;
  ObSimpleBatch batch;
  batch.type_ = ObSimpleBatch::T_SCAN;
  batch.range_ = &inv_idx_agg_param_->key_ranges_.at(0);
  ObTableScanParam est_param;
  est_param.index_id_ = inv_idx_agg_param_->index_id_;
  est_param.scan_flag_ = inv_idx_agg_param_->scan_flag_;
  est_param.tablet_id_ = inv_idx_agg_param_->tablet_id_;
  est_param.ls_id_ = inv_idx_agg_param_->ls_id_;
  est_param.tx_id_ = inv_idx_agg_param_->tx_id_;
  est_param.schema_version_ = inv_idx_agg_param_->schema_version_;
  est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
  if (OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(access_service));
  } else if (OB_FAIL(table_scan_range.init(*inv_idx_agg_param_, batch, allocator))) {
    STORAGE_LOG(WARN, "Failed to init table scan range", K(ret), K(batch));
  } else if (OB_FAIL(access_service->estimate_row_count(est_param,
                                                        table_scan_range,
                                                        timeout_us,
                                                        est_records,
                                                        logical_row_cnt,
                                                        physical_row_cnt))) {
    LOG_TRACE("OPT:[STORAGE EST FAILED, USE STAT EST]", "storage_ret", ret);
  } else {
    token_doc_cnt_ = logical_row_cnt;
    token_doc_cnt_calculated_ = true;
    sql::ObExpr *total_doc_cnt_param_expr = relevance_expr_->args_[sql::ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX];
    if (OB_ISNULL(total_doc_cnt_param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else {
      int64_t total_doc_cnt = 0;
      if (is_valid_format(total_doc_cnt_param_expr->get_format(*eval_ctx_))) {
        total_doc_cnt = total_doc_cnt_param_expr->get_vector(*eval_ctx_)->get_int(0);
      } else {
        total_doc_cnt = total_doc_cnt_param_expr->locate_expr_datum(*eval_ctx_, 0).get_int();
      }
      max_token_relevance_ = sql::ObExprBM25::query_token_weight(token_doc_cnt_, total_doc_cnt);
    }
  }
  return ret;
}

ObTextRetrievalDaaTTokenIter::ObTextRetrievalDaaTTokenIter()
  : ObISRDaaTDimIter(),
    allocator_(nullptr),
    token_iter_(nullptr),
    eval_ctx_(nullptr),
    relevance_expr_(nullptr),
    inv_scan_domain_id_col_(nullptr),
    max_batch_size_(1),
    cur_idx_(-1),
    count_(0),
    relevance_(),
    doc_id_(),
    cmp_func_(nullptr),
    is_inited_(false)
{
}

int ObTextRetrievalDaaTTokenIter::init(const ObTextRetrievalScanIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  allocator_ = iter_param.allocator_;
  if (OB_ISNULL(iter_param.eval_ctx_) || OB_ISNULL(iter_param.inv_scan_domain_id_col_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(iter_param.eval_ctx_), KP(iter_param.inv_scan_domain_id_col_), KP(allocator_));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObTextRetrievalTokenIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    token_iter_ = new (buf) ObTextRetrievalTokenIter();
    if (OB_FAIL(token_iter_->init(iter_param))) {
      LOG_WARN("failed to init token iter", K(ret));
    } else {
      eval_ctx_ = iter_param.eval_ctx_;
      relevance_expr_ = iter_param.relevance_expr_;
      inv_scan_domain_id_col_ = iter_param.inv_scan_domain_id_col_;
      max_batch_size_ = OB_MAX(iter_param.eval_ctx_->max_batch_size_, 1);
      sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(inv_scan_domain_id_col_->datum_meta_.type_, CS_TYPE_BINARY);
      cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      if (OB_ISNULL(cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to init IRIterLoserTreeCmp", K(ret));
      } else if (FALSE_IT(relevance_.set_allocator(allocator_))) {
      } else if (OB_FAIL(relevance_.init(max_batch_size_))) {
        LOG_WARN("failed to init next batch iter idxes array", K(ret));
      } else if (OB_FAIL(relevance_.prepare_allocate(max_batch_size_))) {
        LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
      } else if (FALSE_IT(doc_id_.set_allocator(allocator_))) {
      } else if (OB_FAIL(doc_id_.init(max_batch_size_))) {
        LOG_WARN("failed to init next batch iter idxes array", K(ret));
      } else if (OB_FAIL(doc_id_.prepare_allocate(max_batch_size_))) {
        LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObTextRetrievalDaaTTokenIter::reuse()
{
  cur_idx_ = -1;
  count_ = 0;
  token_iter_->reuse();
}

void ObTextRetrievalDaaTTokenIter::reset()
{
  relevance_.reset();
  doc_id_.reset();
  token_iter_->reset();
  cur_idx_ = -1;
  count_ = 0;
}

// Sparse Retrieval Dimension Iter Interfaces
int ObTextRetrievalDaaTTokenIter::get_next_row() {
  int ret = OB_SUCCESS;
  bool need_load = false;
  if (OB_LIKELY((++cur_idx_) < count_)) {
  } else if (!eval_ctx_->is_vectorized() && (OB_FAIL(token_iter_->get_next_row()))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get row from inverted index", K(ret));
    }
  } else if (!eval_ctx_->is_vectorized() && FALSE_IT(count_ = 1)) {
  } else if (eval_ctx_->is_vectorized() && OB_FAIL(token_iter_->get_next_batch(max_batch_size_, count_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get batch rows from inverted index", K(ret));
    } else if (count_ != 0) {
      ret = OB_SUCCESS;
      need_load = true;
    }
  } else {
    need_load = true;
  }
  if (OB_SUCC(ret) && need_load) {
    if (OB_NOT_NULL(relevance_expr_)) {
      if (OB_FAIL(save_relevances_and_docids())) {
        LOG_WARN("failed to evaluate simarity expr", K(ret));
      }
    } else if (OB_FAIL(save_docids())) {
        LOG_WARN("failed to save doc ids", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalDaaTTokenIter::save_relevances_and_docids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(relevance_expr_) || OB_ISNULL(inv_scan_domain_id_col_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance or doc id expr", K(ret));
  } else if (OB_FAIL(relevance_expr_->eval_batch(*eval_ctx_, *token_iter_->get_skip(), count_))) {
    LOG_WARN("failed to evaluate relevance", K(ret));
  } else {
    cur_idx_ = 0;
    const ObDatumVector &relevance_datum = relevance_expr_->locate_expr_datumvector(*eval_ctx_);
    const ObDatumVector &doc_id_datum = inv_scan_domain_id_col_->locate_expr_datumvector(*eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_LIKELY(!token_iter_->get_skip()->at(i))) {
        relevance_[i] = relevance_datum.at(i)->get_double();
        if (OB_FAIL(doc_id_[i].from_datum(*doc_id_datum.at(i)))) {
          LOG_WARN("failed to get doc id", K(ret), K(doc_id_datum.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObTextRetrievalDaaTTokenIter::save_docids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inv_scan_domain_id_col_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance or doc id expr", K(ret));
  } else {
    cur_idx_ = 0;
    const ObDatumVector &doc_id_datum = inv_scan_domain_id_col_->locate_expr_datumvector(*eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_LIKELY(!token_iter_->get_skip()->at(i))) {
        if (OB_FAIL(doc_id_[i].from_datum(*doc_id_datum.at(i)))) {
          LOG_WARN("failed to get doc id", K(ret));
        };
      }
    }
  }
  return ret;
}

int ObTextRetrievalDaaTTokenIter::get_next_batch(const int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalDaaTTokenIter::advance_to(const ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  int result = 0;
  bool find = false;

  if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cur_idx", K(ret), K(cur_idx_));
  }
  while (OB_SUCC(ret) && !find) {
    if (cur_idx_ < count_) {
      if (OB_FAIL(cmp_func_(id_datum, doc_id_[cur_idx_].get_datum(), result))) {
        LOG_WARN("failed to compare datum", K(ret));
      } else if (result <= 0) {
        find = true;
      } else {
        ++cur_idx_;
      }
    } else if (OB_FAIL(token_iter_->advance_to(id_datum))) {
      LOG_WARN("failed to advance token iter", K(ret));
    } else if (OB_FAIL(get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get batch rows from inverted index", K(ret));
      } else {
        find = true;
      }
    } else if (cur_idx_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result", K(ret), K(result));
    } else if (OB_FAIL(cmp_func_(id_datum, doc_id_[cur_idx_].get_datum(), result))) {
      LOG_WARN("failed to compare datum", K(ret));
    } else if (result <= 0) {
      find = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result", K(ret), K(result));
    }
  }
  return ret;
}

int ObTextRetrievalDaaTTokenIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_idx_ >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of bounds", K(ret), K_(cur_idx), K_(count));
  } else if (relevance_expr_) {
    score = relevance_[cur_idx_];
  }
  return ret;
}

int ObTextRetrievalDaaTTokenIter::get_curr_id(const ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_idx_ >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of bounds", K(ret), K_(cur_idx), K_(count));
  } else {
    id_datum = &doc_id_[cur_idx_].get_datum();
  }
  return ret;
}

ObTextRetrievalBlockMaxIter::ObTextRetrievalBlockMaxIter()
  : ObISRDimBlockMaxIter(),
    token_iter_(),
    block_max_iter_(),
    block_max_iter_param_(nullptr),
    block_max_scan_param_(nullptr),
    ranking_param_(),
    curr_id_(nullptr),
    max_score_tuple_(nullptr),
    dim_max_score_(0),
    block_max_inited_(false),
    in_shallow_status_(false),
    is_inited_(false)
{
}

int ObTextRetrievalBlockMaxIter::init(
    const ObTextRetrievalScanIterParam &iter_param,
    const ObBlockMaxScoreIterParam &block_max_iter_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!block_max_iter_param.is_valid() || !scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter param", K(ret), K(block_max_iter_param), K(scan_param));
  } else if (OB_FAIL(token_iter_.init(iter_param))) {
    LOG_WARN("failed to init token iter", K(ret));
  } else {
    curr_id_ = nullptr;
    block_max_iter_param_ = &block_max_iter_param;
    block_max_scan_param_ = &scan_param;
    max_score_tuple_ = nullptr;
    dim_max_score_ = 0;
    in_shallow_status_ = false;
    ranking_param_.token_freq_col_idx_ = block_max_iter_param.token_freq_col_idx_;
    ranking_param_.doc_length_col_idx_ = block_max_iter_param.doc_length_col_idx_;
    is_inited_ = true;
  }
  return ret;
}

void ObTextRetrievalBlockMaxIter::reset()
{
  token_iter_.reset();
  block_max_iter_.reset();
  curr_id_ = nullptr;
  block_max_iter_param_ = nullptr;
  block_max_scan_param_ = nullptr;
  max_score_tuple_ = nullptr;
  dim_max_score_ = 0;
  in_shallow_status_ = false;
  block_max_inited_ = false;
  is_inited_ = false;
}

void ObTextRetrievalBlockMaxIter::reuse()
{
  token_iter_.reuse();
  block_max_iter_.reset();
  curr_id_ = nullptr;
  max_score_tuple_ = nullptr;
  in_shallow_status_ = false;
  block_max_inited_ = false;
  dim_max_score_ = 0;
}

int ObTextRetrievalBlockMaxIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get next row after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_FAIL(token_iter_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(token_iter_.get_curr_id(curr_id_))) {
    LOG_WARN("failed to get curr id", K(ret));
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::get_next_batch(const int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalBlockMaxIter::advance_to(const ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(token_iter_.advance_to(id_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to id datum", K(ret));
    }
  } else if (OB_FAIL(token_iter_.get_curr_id(curr_id_))) {
    LOG_WARN("failed to get curr id", K(ret));
  } else {
    in_shallow_status_ = false;
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::advance_shallow(const ObDatum &id_datum, const bool inclusive)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("[Sparse Retrieval] advance shallow", K(ret), K(id_datum), K(inclusive));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!block_max_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block max iter not calculated", K(ret), K_(block_max_inited));
  } else if (OB_FAIL(block_max_iter_.advance_to(id_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to id datum", K(ret));
    }
  } else if (OB_FAIL(block_max_iter_.get_curr_max_score_tuple(max_score_tuple_))) {
    LOG_WARN("failed to get next max score tuple", K(ret));
  } else {
    // max_score_tuple_->min_domain_id_ should not be smaller than $id_datum
    curr_id_ = max_score_tuple_->min_domain_id_;
    in_shallow_status_ = true;
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get curr score after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_FAIL(token_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score", K(ret));
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::get_curr_id(const ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_ISNULL(curr_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to curr id", K(ret), KP_(curr_id));
  } else {
    id_datum = curr_id_;
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::get_dim_max_score(double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!block_max_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block max iter not calculated", K(ret), K_(block_max_inited));
  } else {
    score = dim_max_score_;
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!block_max_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block max iter not calculated", K(ret), K_(block_max_inited));
  } else if (OB_ISNULL(max_score_tuple_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to max score tuple", K(ret), KP_(max_score_tuple));
  } else {
    max_score_tuple = max_score_tuple_;
  }
  return ret;
}

bool ObTextRetrievalBlockMaxIter::in_shallow_status() const
{
  return in_shallow_status_;
}

int ObTextRetrievalBlockMaxIter::calc_dim_max_score(
    const ObBlockMaxScoreIterParam &block_max_iter_param,
    const ObBlockMaxBM25RankingParam &ranking_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  // Maybe a specialized interface to calculate dimension max score based on statistics is more efficient
  if (OB_FAIL(block_max_iter_.init(ranking_param, block_max_iter_param, scan_param))) {
    LOG_WARN("failed to init block max iter", K(ret));
  }

  while (OB_SUCC(ret)) {
    const ObMaxScoreTuple *max_score_tuple = nullptr;
    if (OB_FAIL(block_max_iter_.get_next(max_score_tuple))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next max score tuple", K(ret));
      }
    } else if (OB_ISNULL(max_score_tuple)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to max score tuple", K(ret), KP_(max_score_tuple));
    } else {
      dim_max_score_ = std::max(dim_max_score_, max_score_tuple->max_score_);
      LOG_DEBUG("[Text Retrieval] calc dim max score", K(ret), K(dim_max_score_), K(max_score_tuple->max_score_),
        KPC(max_score_tuple->max_domain_id_), KPC(max_score_tuple->min_domain_id_));
    }
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
    block_max_iter_.reset(); // TODO: reuse or rewind iter
  } else {
    LOG_WARN("failed to calc dim max score", K(ret));
  }
  return ret;
}

int ObTextRetrievalBlockMaxIter::init_block_max_iter()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(block_max_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("block max iter already initalized", K(ret));
  } else if (OB_FAIL(token_iter_.get_token_doc_cnt(ranking_param_.doc_freq_))) {
    LOG_WARN("failed to get token doc cnt", K(ret));
  } else if (OB_FAIL(calc_dim_max_score(*block_max_iter_param_, ranking_param_, *block_max_scan_param_))) {
    LOG_WARN("failed to calc dim max score", K(ret));
  } else if (OB_FAIL(block_max_iter_.init(ranking_param_, *block_max_iter_param_, *block_max_scan_param_))) {
    LOG_WARN("failed to init block max iter", K(ret));
  } else {
    block_max_inited_ = true;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase