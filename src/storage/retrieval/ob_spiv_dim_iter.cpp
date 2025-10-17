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

#include "ob_spiv_dim_iter.h"

namespace oceanbase
{
namespace storage
{
// ------------------ ObSPIVDaaTDimIter implement ------------------
int ObSPIVDaaTDimIter::init(const ObSPIVDimIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  mem_context_ = iter_param.mem_context_;
  allocator_ = iter_param.allocator_;
  inv_idx_scan_param_ = iter_param.inv_idx_scan_param_;
  inv_idx_agg_param_ = iter_param.inv_idx_agg_param_;
  inv_idx_scan_iter_ = iter_param.inv_idx_scan_iter_;
  inv_idx_agg_iter_ = iter_param.inv_idx_agg_iter_;
  inv_idx_agg_expr_ = iter_param.inv_idx_agg_expr_;
  inv_scan_domain_id_expr_ = iter_param.inv_scan_domain_id_expr_;
  inv_scan_score_expr_ = iter_param.inv_scan_score_expr_;
  inv_idx_scan_ctdef_ = iter_param.inv_idx_scan_ctdef_;
  inv_idx_scan_rtdef_ = iter_param.inv_idx_scan_rtdef_;
  max_batch_size_ = OB_MAX(iter_param.eval_ctx_->max_batch_size_, 1);
  eval_ctx_ = iter_param.eval_ctx_;
  ls_id_ = iter_param.ls_id_;
  dim_docid_value_tablet_id_ = iter_param.dim_docid_value_tablet_id_;
  dim_ = iter_param.dim_;
  cur_idx_ = -1;
  count_ = 0;
  query_value_ = iter_param.query_value_;
  if (OB_ISNULL(inv_idx_scan_param_) || OB_ISNULL(inv_idx_scan_iter_) || OB_ISNULL(inv_scan_domain_id_expr_) ||
      OB_ISNULL(eval_ctx_) || OB_ISNULL(mem_context_.ref_context()) || OB_ISNULL(inv_scan_score_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_iter or eval_ctx is NULL",
        K(ret),
        KP(inv_idx_scan_iter_),
        KPC_(eval_ctx),
        KPC_(inv_idx_scan_param),
        KPC_(inv_scan_domain_id_expr),
        KP(mem_context_.ref_context()));
  } else if (need_inv_agg() && (OB_ISNULL(inv_idx_agg_param_) || OB_ISNULL(inv_idx_agg_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_param or inv_idx_agg_expr is NULL", KPC_(inv_idx_agg_param), KPC_(inv_idx_agg_expr));
  } else {
    sql::ObExprBasicFuncs *basic_funcs =
        ObDatumFuncs::get_basic_func(inv_scan_domain_id_expr_->datum_meta_.type_, CS_TYPE_BINARY);
    cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
    if (OB_ISNULL(cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp_func is NULL", K(ret));
    } else if (FALSE_IT(scores_.set_allocator(allocator_))) {
    } else if (OB_FAIL(scores_.init(max_batch_size_))) {
      LOG_WARN("failed to init relevance array", K(ret));
    } else if (OB_FAIL(scores_.prepare_allocate(max_batch_size_))) {
      LOG_WARN("failed to prepare allocate relevance array", K(ret));
    } else if (FALSE_IT(doc_ids_.set_allocator(allocator_))) {
    } else if (OB_FAIL(doc_ids_.init(max_batch_size_))) {
      LOG_WARN("failed to init docid array", K(ret));
    } else if (OB_FAIL(doc_ids_.prepare_allocate(max_batch_size_))) {
      LOG_WARN("failed to prepare allocate docid array", K(ret));
    } else {
      inv_idx_scan_iter_->set_scan_param(*inv_idx_scan_param_);
      max_score_cached_ = false;
      is_inited_ = true;
      iter_end_ = false;
    }
  }
  return ret;
}

void ObSPIVDaaTDimIter::reset()
{
  count_ = 0;
  cur_idx_ = -1;
  scores_.reset();
  doc_ids_.reset();
  if (OB_NOT_NULL(inv_idx_scan_iter_)) {
    inv_idx_scan_iter_->reset();
  }
  if (OB_NOT_NULL(inv_idx_agg_iter_)) {
    inv_idx_agg_iter_->reset();
  }
  iter_end_ = false;
}

void ObSPIVDaaTDimIter::reuse()
{
  count_ = 0;
  cur_idx_ = -1;
  // inv_idx_scan_iter_->reuse() in ob_das_spiv_merge_iter.cpp
  iter_end_ = false;
}

int ObSPIVDaaTDimIter::save_docids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inv_scan_score_expr_) || OB_ISNULL(inv_scan_domain_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid score or doc id expr", K(ret));
  } else {
    const ObDatumVector &score_datum = inv_scan_score_expr_->locate_expr_datumvector(*eval_ctx_);
    const ObDatumVector &doc_id_datum = inv_scan_domain_id_expr_->locate_expr_datumvector(*eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      scores_[i] = score_datum.at(i)->get_float();
      if (OB_FAIL(doc_ids_[i].from_datum(*doc_id_datum.at(i)))) {
        LOG_WARN("failed to get doc id", K(ret), K(doc_id_datum.at(i)));
      }
    }
  }
  return ret;
}

int ObSPIVDaaTDimIter::get_next_row()
{
  int ret = OB_SUCCESS;
  bool need_save = false;
  cur_idx_++;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_LIKELY(cur_idx_ < count_)) {
  } else if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_iter is null", K(ret));
  } else {
    if (max_batch_size_ > 1) {
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(count_, max_batch_size_))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next rows", K(ret));
        } else if (count_ != 0) {
          ret = OB_SUCCESS;
          need_save = true;
        } else {
          iter_end_ = true;
        }
      } else {
        need_save = true;
      }
    } else {
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret));
        } else {
          iter_end_ = true;
        }
      } else {
        need_save = true;
        count_ = 1;
      }
    }
  }
  if (OB_SUCC(ret) && need_save) {
    if (OB_FAIL(save_docids())) {
      LOG_WARN("failed to save docids", K(ret));
    } else {
      cur_idx_ = 0;
    }
  }
  return ret;
}

int ObSPIVDaaTDimIter::update_scan_param(const ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inv_idx_scan_param_->key_ranges_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key range count", K(ret), K(inv_idx_scan_param_->key_ranges_.count()));
  } else {
    ObRowkey start_rowkey = inv_idx_scan_param_->key_ranges_.at(0).start_key_;
    ObRowkey end_rowkey = inv_idx_scan_param_->key_ranges_.at(0).end_key_;
    if (start_rowkey.get_obj_ptr()[0].get_uint32() != end_rowkey.get_obj_ptr()[0].get_uint32()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey", K(ret), K(start_rowkey), K(end_rowkey));
    }
    // ob_ptr[0] is dim, ob_ptr[1] is docid
    ObObj *obj_ptr = start_rowkey.get_obj_ptr();
    ObNewRange scan_range;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(id_datum.to_obj(obj_ptr[1], inv_scan_domain_id_expr_->obj_meta_))) {
      LOG_WARN("failed to set obj", K(ret));
    } else {
      scan_range.table_id_ = inv_idx_scan_param_->key_ranges_.at(0).table_id_;
      scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
      scan_range.end_key_.assign(end_rowkey.get_obj_ptr(), INV_IDX_ROWKEY_COL_CNT);
      scan_range.border_flag_.set_inclusive_start();
      scan_range.border_flag_.set_inclusive_end();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inv_idx_scan_iter_->reuse())) {
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
  return ret;
}

int ObSPIVDaaTDimIter::advance_to(const ObDatum &id_datum)
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
      if (OB_FAIL(cmp_func_(id_datum, doc_ids_[cur_idx_].get_datum(), result))) {
        LOG_WARN("failed to compare datum", K(ret));
      } else if (result <= 0) {
        find = true;
      } else {
        ++cur_idx_;
      }
    } else if (OB_FAIL(update_scan_param(id_datum))) {
      LOG_WARN("failed to update scan param", K(ret));
    } else if (OB_FAIL(get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get batch rows from inverted index", K(ret));
      } else {
        find = true;
      }
    } else if (cur_idx_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result", K(ret), K(result));
    } else if (OB_FAIL(cmp_func_(id_datum, doc_ids_[cur_idx_].get_datum(), result))) {
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

int ObSPIVDaaTDimIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_idx_ >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of bounds", K(ret), K_(cur_idx), K_(count));
  } else {
    score = scores_[cur_idx_] * query_value_;
  }
  return ret;
}

int ObSPIVDaaTDimIter::get_curr_id(const ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_idx_ >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of bounds", K(ret), K_(cur_idx), K_(count));
  } else {
    id_datum = &doc_ids_[cur_idx_].get_datum();
  }
  return ret;
}

// TODO: implement agg_iter_
int ObSPIVDaaTDimIter::get_dim_max_score(double &max_score)
{
  int ret = OB_SUCCESS;
  if (max_score_cached_) {
    max_score = max_score_;
  } else {
    if (OB_FAIL(inv_idx_agg_iter_->get_next_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next row", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      const ObDatum &score_datum = inv_idx_agg_expr_->locate_expr_datum(*eval_ctx_);
      max_score_ = score_datum.get_float() * query_value_;
      max_score_cached_ = true;
    }
  }
  return ret;
}

int ObSPIVBlockMaxDimIter::init(
    const ObSPIVDimIterParam &iter_param,
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
  } else if (OB_FAIL(dim_iter_.init(iter_param))) {
    LOG_WARN("failed to init token iter", K(ret));
  } else {
    curr_id_ = nullptr;
    block_max_iter_param_ = &block_max_iter_param;
    block_max_scan_param_ = &scan_param;
    max_score_tuple_ = nullptr;
    dim_max_score_ = 0;
    in_shallow_status_ = false;
    ranking_param_.query_value_ = iter_param.query_value_;
    ranking_param_.score_col_idx_ = block_max_iter_param.score_col_idx_;
    is_inited_ = true;
  }
  return ret;
}

void ObSPIVBlockMaxDimIter::reset()
{
  dim_iter_.reset();
  block_max_iter_.reset();
  curr_id_ = nullptr;
  block_max_iter_param_ = nullptr;
  block_max_scan_param_ = nullptr;
  max_score_tuple_ = nullptr;
  dim_max_score_ = 0;
  in_shallow_status_ = false;
  block_max_inited_ = false;
  block_max_iter_end_ = false;
  is_inited_ = false;
}

void ObSPIVBlockMaxDimIter::reuse()
{
  dim_iter_.reuse();
  block_max_iter_.reuse();
  curr_id_ = nullptr;
  block_max_iter_param_ = nullptr;
  block_max_scan_param_ = nullptr;
  max_score_tuple_ = nullptr;
  in_shallow_status_ = false;
  block_max_inited_ = false;
  block_max_iter_end_ = false;
  dim_max_score_ = 0;
}

int ObSPIVBlockMaxDimIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get next row after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_FAIL(dim_iter_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(dim_iter_.get_curr_id(curr_id_))) {
    LOG_WARN("failed to get curr id", K(ret));
  }
  return ret;
}

int ObSPIVBlockMaxDimIter::get_next_batch(const int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

int ObSPIVBlockMaxDimIter::advance_to(const ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dim_iter_.advance_to(id_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to id datum", K(ret));
    }
  } else if (OB_FAIL(dim_iter_.get_curr_id(curr_id_))) {
    LOG_WARN("failed to get curr id", K(ret));
  } else {
    in_shallow_status_ = false;
  }
  return ret;
}

int ObSPIVBlockMaxDimIter::advance_shallow(const ObDatum &id_datum, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!block_max_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block max iter not calculated", K(ret), K_(block_max_inited));
  } else if (OB_FAIL(block_max_iter_.advance_to(id_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to id datum", K(ret));
    } else {
      block_max_iter_end_ = true;
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

int ObSPIVBlockMaxDimIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get curr score after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_FAIL(dim_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score", K(ret));
  }
  return ret;
}

int ObSPIVBlockMaxDimIter::get_curr_id(const ObDatum *&id_datum) const
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

int ObSPIVBlockMaxDimIter::get_dim_max_score(double &score)
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

int ObSPIVBlockMaxDimIter::get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple)
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

bool ObSPIVBlockMaxDimIter::in_shallow_status() const
{
  return in_shallow_status_;
}

int ObSPIVBlockMaxDimIter::calc_dim_max_score(
    const ObBlockMaxScoreIterParam &block_max_iter_param,
    const ObBlockMaxIPRankingParam &ranking_param,
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
      dim_max_score_ = OB_MAX(dim_max_score_, max_score_tuple->max_score_);
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

int ObSPIVBlockMaxDimIter::init_block_max_iter()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(block_max_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("block max iter already initalized", K(ret));
  } else if (OB_FAIL(calc_dim_max_score(*block_max_iter_param_, ranking_param_, *block_max_scan_param_))) {
    LOG_WARN("failed to calc dim max score", K(ret));
  } else if (OB_FAIL(block_max_iter_.init(ranking_param_, *block_max_iter_param_, *block_max_scan_param_))) {
    LOG_WARN("failed to init block max iter", K(ret));
  } else {
    block_max_inited_ = true;
  }
  return ret;
}

}  // namespace storage

}  // namespace oceanbase
