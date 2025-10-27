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

#include "ob_text_taat_iter.h"

namespace oceanbase
{
namespace storage
{

int ObTextTaaTIter::init(const ObTextTaaTParam &param)
{
  int ret = OB_SUCCESS;
  lib::ContextParam mem_param;
  mem_param.set_mem_attr(MTL_ID(), "TextTaaTIter", ObCtxIds::DEFAULT_CTX_ID);
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iter_) || OB_ISNULL(param.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null pointer in param", K(ret), KP_(param.base_param),
             KP_(param.dim_iter), KP_(param.allocator));
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
    LOG_WARN("failed to create text taat iter memory context", K(ret));
  } else if (OB_FAIL(ObSRTaaTIterImpl::init(*param.base_param_, *param.dim_iter_, *param.allocator_))) {
    LOG_WARN("failed to init sr taat iter", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  } else {
    query_tokens_ = param.query_tokens_;
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
  }
  return ret;
}

void ObTextTaaTIter::reset()
{
  query_tokens_ = nullptr;
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iter_)) {
    static_cast<ObTextRetrievalTokenIter *>(dim_iter_)->reset();
  }
  ObSRTaaTIterImpl::reset();
}

void ObTextTaaTIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iter_)) {
    static_cast<ObTextRetrievalTokenIter *>(dim_iter_)->reuse();
  }
  ObSRTaaTIterImpl::reuse(switch_tablet);
}

int ObTextTaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (iter_param_->need_project_relevance()) {
    const bool is_first_estimation = !bm25_param_estimator_.is_estimated();
    if (!is_first_estimation) {
      // skip
    } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
      LOG_WARN("failed to do bm25 param estimation", K(ret));
    } else {
      const int64_t total_doc_cnt = bm25_param_estimator_.get_total_doc_cnt();
      partition_cnt_ = MIN((total_doc_cnt-1) / OB_HASHMAP_DEFAULT_SIZE + 1, OB_MAX_HASHMAP_COUNT);
    }
  }
  return ret;
}

int ObTextTaaTIter::update_dim_iter(const int64_t dim_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dim_idx > query_tokens_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dim idx", K(ret), K(dim_idx), K(query_tokens_->count()));
  } else if (dim_idx == query_tokens_->count()) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(mem_context_->reset_remain_one_page())) {
  } else if (OB_FAIL(static_cast<ObTextRetrievalTokenIter *>(dim_iter_)
      ->update_scan_param(query_tokens_->at(dim_idx), mem_context_->get_arena_allocator()))) {
    LOG_WARN("failed to update scan param of dim iter", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase