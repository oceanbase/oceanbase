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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_sort_iter.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASSortIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_SORT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASSortIter", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(sort_memctx_, context_param))) {
      LOG_WARN("failed to create lookup memctx", K(ret));
    } else {
      ObDASSortIterParam &sort_param = static_cast<ObDASSortIterParam&>(param);
      sort_ctdef_ = sort_param.sort_ctdef_;
      child_ = sort_param.child_;
      // init top-n parameter
      if ((nullptr != sort_ctdef_->limit_expr_ || nullptr != sort_ctdef_->offset_expr_)
          && sort_param.limit_param_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected both limit offset expr and limit param", K(ret));
      } else if (sort_param.limit_param_.is_valid()) {
        limit_param_ = sort_param.limit_param_;
      } else {
        if (nullptr != sort_ctdef_->limit_expr_) {
          ObDatum *limit_datum = nullptr;
          if (OB_FAIL(sort_ctdef_->limit_expr_->eval(*eval_ctx_, limit_datum))) {
            LOG_WARN("failed to eval limit expr", K(ret));
          } else {
            limit_param_.limit_ = (limit_datum->is_null() || limit_datum->get_int() < 0) ? 0 : limit_datum->get_int();
          }
        }
        if (nullptr != sort_ctdef_->offset_expr_) {
          ObDatum *offset_datum = nullptr;
          if (OB_FAIL(sort_ctdef_->offset_expr_->eval(*eval_ctx_, offset_datum))) {
            LOG_WARN("failed to eval limit expr", K(ret));
          } else if (offset_datum->is_null()) {
            limit_param_.limit_ = 0;
            limit_param_.offset_ = 0;
          } else {
            limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
          }
        }
      }

      if (OB_SUCC(ret)) {
        const bool top_k_overflow = INT64_MAX - limit_param_.offset_ < limit_param_.limit_;
        const int64_t top_k = (limit_param_.is_valid() && !top_k_overflow)
            ? (limit_param_.limit_ + limit_param_.offset_) : INT64_MAX;
        if (OB_FAIL(sort_impl_.init(MTL_ID(),
                                    &sort_ctdef_->sort_collations_,
                                    &sort_ctdef_->sort_cmp_funcs_,
                                    eval_ctx_,
                                    exec_ctx_,
                                    false,  // enable encode sort key
                                    false,  // is local order
                                    false,  // need rewind
                                    0,      // part cnt
                                    top_k,
                                    sort_ctdef_->fetch_with_ties_))) {
          LOG_WARN("failed to init sort impl", K(ret));
        } else if (OB_FAIL(append(sort_row_, sort_ctdef_->sort_exprs_))) {
          LOG_WARN("failed to append sort exprs", K(ret));
        } else if (OB_FAIL(append_array_no_dup(sort_row_, *child_->get_output()))) {
          LOG_WARN("failed to append sort rows", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASSortIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(child_)) {
    if (OB_FAIL(child_->reuse())) {
      LOG_WARN("failed to reuse child iter", K(ret));
    }
  }
  if (OB_NOT_NULL(sort_memctx_))  {
    sort_memctx_->reset_remain_one_page();
  }
  sort_finished_ = false;
  output_row_cnt_ = 0;
  input_row_cnt_ = 0;
  // TODO: check if we can reuse sort impl here
  // reset sort impl here since ObSortOpImpl::outputted_rows_cnt_ is not reset in reuse()
  sort_impl_.reset();
  fake_skip_ = nullptr;
  return ret;
}

int ObDASSortIter::inner_release()
{
  int ret = OB_SUCCESS;
  sort_impl_.reset();
  if (OB_NOT_NULL(sort_memctx_))  {
    sort_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(sort_memctx_);
    sort_memctx_ = nullptr;
  }
  sort_row_.reset();
  return ret;
}

int ObDASSortIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_->do_table_scan())) {
    LOG_WARN("failed to do table scan", K(ret));
  }
  return ret;
}

int ObDASSortIter::rescan()
{
  int ret = OB_SUCCESS;
  const bool top_k_overflow = INT64_MAX - limit_param_.offset_ < limit_param_.limit_;
  const int64_t top_k = (limit_param_.is_valid() && !top_k_overflow)
      ? (limit_param_.limit_ + limit_param_.offset_) : INT64_MAX;
  if (OB_FAIL(child_->rescan())) {
    LOG_WARN("failed to rescan child", K(ret));
  } else if (OB_FAIL(sort_impl_.init(MTL_ID(),
                                     &sort_ctdef_->sort_collations_,
                                     &sort_ctdef_->sort_cmp_funcs_,
                                     eval_ctx_,
                                     exec_ctx_,
                                     false,  // enable encode sort key
                                     false,  // is local order
                                     false,  // need rewind
                                     0,      // part cnt
                                     top_k,
                                     sort_ctdef_->fetch_with_ties_))) {
    LOG_WARN("failed to init sort impl", K(ret));
  }
  return ret;
}

int ObDASSortIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
    LOG_DEBUG("das sort iter got enough rows", K_(limit_param), K_(output_row_cnt), K_(input_row_cnt), K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort(false))) {
    LOG_WARN("failed to do sort", K(ret));
  } else {
    bool got_row = false;
    while (OB_SUCC(ret) && !got_row) {
      if (OB_FAIL(sort_impl_.get_next_row(sort_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from sort impl", K(ret));
        }
      } else {
        ++input_row_cnt_;
        if (input_row_cnt_ > limit_param_.offset_) {
          got_row = true;
          ++output_row_cnt_;
        }
      }
    }
  }
  return ret;
}

int ObDASSortIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
    LOG_DEBUG("das sort iter got enough rows", K_(limit_param), K_(output_row_cnt), K_(input_row_cnt), K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort(true))) {
    LOG_WARN("failed to do sort", K(ret));
  } else {
    bool got_rows = false;
    // TODO: @bingfan use vectorized interface instead.
    while (OB_SUCC(ret) && !got_rows) {
      if (OB_FAIL(sort_impl_.get_next_row(sort_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from sort impl", K(ret));
        }
      } else {
        ++input_row_cnt_;
        if (input_row_cnt_ > limit_param_.offset_) {
          got_rows = true;
          count = 1;
          ++output_row_cnt_;
        }
      }
    }
  }
  return ret;
}

int ObDASSortIter::do_sort(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_vectorized)) {
    int64_t read_size = 0;
    fake_skip_ = to_bit_vector(sort_memctx_->get_arena_allocator().alloc(ObBitVector::memory_size(max_size_)));
    fake_skip_->init(max_size_);
    while (OB_SUCC(ret)) {
      read_size = 0;
      if (OB_FAIL(child_->get_next_rows(read_size, max_size_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed ro get next rows from child iter", K(ret));
        } else if (OB_FAIL(sort_impl_.add_batch(sort_row_, *fake_skip_, read_size, 0, nullptr))) {
          LOG_WARN("failed to add batch to sort impl", K(ret));
        } else {
          ret = OB_ITER_END;
        }
      } else if (OB_FAIL(sort_impl_.add_batch(sort_row_, *fake_skip_, read_size, 0, nullptr))) {
        LOG_WARN("failed to add batch to sort impl", K(ret));
      }
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed ro get next rows from child iter", K(ret));
        }
      } else if (OB_FAIL(sort_impl_.add_row(sort_row_))) {
        LOG_WARN("failed to add row to sort impl", K(ret));
      }
    }
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
    if (OB_FAIL(sort_impl_.sort())) {
      LOG_WARN("failed to do sort", K(ret));
    } else {
      sort_finished_ = true;
    }
  }

  return ret;
}

void ObDASSortIter::clear_evaluated_flag()
{
  OB_ASSERT(nullptr != child_);
  child_->clear_evaluated_flag();
}

}  // namespace sql
}  // namespace oceanbase
