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

#include "ob_text_daat_iter.h"

namespace oceanbase
{
namespace storage
{
int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null pointer in param", K(ret), KP_(param.base_param),
             KP_(param.dim_iters), KP_(param.allocator), KP_(param.relevance_collector));
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
    LOG_WARN("failed to init sr daat iter", K(ret));
  } else {
    total_doc_cnt_scan_param_ = param.total_doc_cnt_scan_param_;
    estimated_total_doc_cnt_ = param.estimated_total_doc_cnt_;
    total_doc_cnt_iter_ = param.total_doc_cnt_iter_;
    total_doc_cnt_expr_ = param.total_doc_cnt_expr_;
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  total_doc_cnt_calculated_ = false;
  estimated_total_doc_cnt_ = 0;
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse()
{
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count()) || estimated_total_doc_cnt_ > 0) {
    // do nothing
  } else if (function_lookup_mode_ && OB_NOT_NULL(total_doc_cnt_scan_param_)
      && !total_doc_cnt_scan_param_->need_switch_param_) {
    // do nothing
  } else {
    total_doc_cnt_calculated_ = false;
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRDaaTIterImpl::reuse();
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (iter_param_->need_project_relevance()) {
    if (total_doc_cnt_calculated_) {
    } else if (OB_FAIL(do_total_doc_cnt())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do total document count", K(ret));
      }
    } else {
      total_doc_cnt_calculated_ = true;
    }
  }
  return ret;
}

int ObTextDaaTIter::do_total_doc_cnt()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*iter_param_->eval_ctx_);
  guard.set_batch_idx(0);
  if (estimated_total_doc_cnt_ <= 0) {
    if (OB_ISNULL(total_doc_cnt_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt iter", K(ret));
    } else if (OB_FAIL(total_doc_cnt_iter_->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get aggregated row from iter", K(ret));
      }
    }
  } else if (OB_ISNULL(total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null total doc cnt expr", K(ret));
  } else {
    // try to do this in das iter
    ObDatum &total_doc_cnt = total_doc_cnt_expr_->locate_datum_for_write(*iter_param_->eval_ctx_);
    total_doc_cnt.set_int(estimated_total_doc_cnt_);
    LOG_TRACE("use estimated row count as partition document count", K(ret), K(total_doc_cnt));
  }
  return ret;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    estimated_total_doc_cnt_(0),
    total_doc_cnt_scan_param_(nullptr),
    whole_doc_cnt_iter_(nullptr),
    total_doc_cnt_expr_(nullptr),
    total_doc_cnt_calculated_(false)
{}

void ObTextBMWIter::reuse()
{
  total_doc_cnt_calculated_ = false;
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse();
}

void ObTextBMWIter::reset()
{
  total_doc_cnt_calculated_ = false;
  estimated_total_doc_cnt_ = 0;
  total_doc_cnt_scan_param_ = nullptr;
  whole_doc_cnt_iter_ = nullptr;
  total_doc_cnt_expr_ = nullptr;
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null pointer in param", K(ret), KP_(param.base_param),
             KP_(param.dim_iters), KP_(param.allocator), KP_(param.relevance_collector));
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
    LOG_WARN("failed to init sr bmw iter", K(ret));
  } else {
    total_doc_cnt_scan_param_ = param.total_doc_cnt_scan_param_;
    estimated_total_doc_cnt_ = param.estimated_total_doc_cnt_;
    whole_doc_cnt_iter_ = param.total_doc_cnt_iter_;
    total_doc_cnt_expr_ = param.total_doc_cnt_expr_;
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (total_doc_cnt_calculated_) {
  } else if (OB_FAIL(do_total_doc_cnt())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to do total document count", K(ret));
    }
  } else {
    total_doc_cnt_calculated_ = true;
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows", K(ret));
    }
  }
  return ret;
}

int ObTextBMWIter::do_total_doc_cnt()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*iter_param_->eval_ctx_);
  guard.set_batch_idx(0);
  if (estimated_total_doc_cnt_ <= 0) {
    if (OB_ISNULL(whole_doc_cnt_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null whole doc cnt iter", K(ret));
    } else if (OB_FAIL(whole_doc_cnt_iter_->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get aggregated row from iter", K(ret));
      }
    }
  } else if (OB_ISNULL(total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null total doc cnt expr", K(ret));
  } else {
    ObDatum &total_doc_cnt_datum = total_doc_cnt_expr_->locate_datum_for_write(*iter_param_->eval_ctx_);
    total_doc_cnt_datum.set_int(estimated_total_doc_cnt_);
    LOG_TRACE("use estimated row count as partition document count", K(ret), K(total_doc_cnt_datum));
  }

  if (OB_SUCC(ret)) {
    int64_t total_doc_cnt = 0;
    if (is_valid_format(total_doc_cnt_expr_->get_format(*iter_param_->eval_ctx_))) {
      total_doc_cnt = total_doc_cnt_expr_->get_vector(*iter_param_->eval_ctx_)->get_int(0);
    } else {
      total_doc_cnt = total_doc_cnt_expr_->locate_expr_datum(*iter_param_->eval_ctx_).get_int();
    }
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->set_total_doc_cnt(total_doc_cnt);
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter())) {
      LOG_WARN("failed to init block max iter", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase