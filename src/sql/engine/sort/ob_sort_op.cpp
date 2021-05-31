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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"

namespace oceanbase {
namespace sql {

ObSortSpec::ObSortSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      topn_expr_(nullptr),
      topk_limit_expr_(nullptr),
      topk_offset_expr_(nullptr),
      all_exprs_(alloc),
      sort_collations_(alloc),
      sort_cmp_funs_(alloc),
      minimum_row_count_(0),
      topk_precision_(0),
      prefix_pos_(0),
      is_local_merge_sort_(false),
      is_fetch_with_ties_(false)
{}

OB_SERIALIZE_MEMBER((ObSortSpec, ObOpSpec), topn_expr_, topk_limit_expr_, topk_offset_expr_, all_exprs_,
    sort_collations_, sort_cmp_funs_, minimum_row_count_, topk_precision_, prefix_pos_, is_local_merge_sort_,
    is_fetch_with_ties_);

ObSortOp::ObSortOp(ObExecContext& ctx_, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(ctx_, spec, input),
      sort_impl_(),
      prefix_sort_impl_(),
      topn_sort_(),
      read_func_(&ObSortOp::sort_impl_next),
      sort_row_count_(0),
      is_first_(true),
      ret_row_count_(0),
      iter_end_(false)
{}

int ObSortOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  }
  return OB_SUCCESS;
}

int ObSortOp::rescan()
{
  reset();
  iter_end_ = false;
  return ObOperator::rescan();
}

void ObSortOp::reset()
{
  sort_impl_.reset();
  prefix_sort_impl_.reset();
  topn_sort_.reset();
  read_func_ = &ObSortOp::sort_impl_next;
  sort_row_count_ = 0;
  ret_row_count_ = 0;
  is_first_ = true;
}

void ObSortOp::destroy()
{
  sort_impl_.~ObSortOpImpl();
  prefix_sort_impl_.~ObPrefixSortImpl();
  topn_sort_.~ObInMemoryTopnSortImpl();
  read_func_ = nullptr;
  sort_row_count_ = 0;
  is_first_ = true;
  ret_row_count_ = 0;
  ObOperator::destroy();
}

int ObSortOp::inner_close()
{
  sort_impl_.unregister_profile();
  prefix_sort_impl_.unregister_profile();
  return OB_SUCCESS;
}

int ObSortOp::get_int_value(const ObExpr* in_val, int64_t& out_val)
{
  int ret = OB_SUCCESS;
  ObDatum* datum = NULL;
  if (NULL != in_val) {
    if (OB_FAIL(in_val->eval(eval_ctx_, datum))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (OB_ISNULL(datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: datum is null", K(ret));
    } else {
      out_val = *datum->int_;
    }
  }
  return ret;
}

int ObSortOp::get_topn_count(int64_t& topn_cnt)
{
  int ret = OB_SUCCESS;
  topn_cnt = INT64_MAX;
  if ((OB_ISNULL(MY_SPEC.topn_expr_) && OB_ISNULL(MY_SPEC.topk_limit_expr_)) ||
      ((NULL != MY_SPEC.topn_expr_) && (NULL != MY_SPEC.topk_limit_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid topn_expr or topk_limit_expr", K(MY_SPEC.topn_expr_), K(MY_SPEC.topk_limit_expr_), K(ret));
  } else if (NULL != MY_SPEC.topn_expr_) {
    if (OB_FAIL(get_int_value(MY_SPEC.topn_expr_, topn_cnt))) {
      LOG_WARN("failed to get int value", K(ret), K(MY_SPEC.topn_expr_));
    } else {
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, topn_cnt);
    }
  } else if (NULL != MY_SPEC.topk_limit_expr_) {
    int64_t limit = -1;
    int64_t offset = 0;
    if ((OB_FAIL(get_int_value(MY_SPEC.topk_limit_expr_, limit)) ||
            OB_FAIL(get_int_value(MY_SPEC.topk_offset_expr_, offset)))) {
      LOG_WARN("Get limit/offset value failed", K(ret));
    } else if (OB_UNLIKELY(limit < 0 || offset < 0)) {
      ret = OB_ERR_ILLEGAL_VALUE;
      LOG_WARN("Invalid limit/offset value", K(limit), K(offset), K(ret));
    } else {
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, limit + offset);
      int64_t row_count = 0;
      ObPhyOperatorType op_type = child_->get_spec().type_;
      if (PHY_HASH_GROUP_BY != op_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid child_op_", K(op_type), K(ret));
      } else {
        ObHashGroupByOp* hash_groupby_op = static_cast<ObHashGroupByOp*>(child_);
        row_count = hash_groupby_op->get_hash_groupby_row_count();
      }
      if (OB_SUCC(ret)) {
        topn_cnt = std::max(topn_cnt, static_cast<int64_t>(row_count * MY_SPEC.topk_precision_ / 100));
        if (topn_cnt >= row_count) {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(true);
        } else {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(false);
        }
      }
    }
  }
  return ret;
}

int ObSortOp::process_sort()
{
  int ret = OB_SUCCESS;
  if (read_func_ == &ObSortOp::prefix_sort_impl_next) {
    // prefix sort get child row in it's own wrap, do nothing here
  } else if (read_func_ == &ObSortOp::topn_sort_next) {
    bool need_sort = false;
    if (0 == sort_row_count_) {
      int64_t topn_cnt = INT64_MAX;
      if (OB_FAIL(get_topn_count(topn_cnt))) {
        LOG_WARN("failed to get topn count", K(ret));
      } else {
        topn_sort_.set_topn(topn_cnt);
        if (topn_cnt <= 0) {
          ret = OB_ITER_END;
        }
      }
    }
    if (OB_SUCC(ret) && topn_sort_.get_topn_cnt() > 0) {
      while (OB_SUCC(ret) && !need_sort) {
        clear_evaluated_flag();
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("failed to check status", K(ret));
        } else if (OB_FAIL(child_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          sort_row_count_++;
          OZ(topn_sort_.add_row(MY_SPEC.all_exprs_, need_sort));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        topn_sort_.set_iter_end();
      }
      OZ(topn_sort_.sort_rows());
    }
  } else if (read_func_ == &ObSortOp::sort_impl_next) {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        sort_row_count_++;
        OZ(sort_impl_.add_row(MY_SPEC.all_exprs_));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    OZ(sort_impl_.sort());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid read function pointer", K(ret), K(*reinterpret_cast<int64_t*>(&read_func_)));
  }
  return ret;
}

int ObSortOp::topn_sort_next()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_fetch_with_ties_ && ret_row_count_ >= topn_sort_.get_topn_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(topn_sort_.get_next_row(MY_SPEC.all_exprs_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("topn sort get next row failed", K(ret));
    } else if (MY_SPEC.prefix_pos_ > 0 && !topn_sort_.is_iter_end()) {
      // topn prefix sort, iterate one prefix at a time, need to process_sort() again.
      if (OB_FAIL(process_sort())) {
        LOG_WARN("process sort failed", K(ret));
      } else if (OB_FAIL(topn_sort_.get_next_row(MY_SPEC.all_exprs_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("topn sort get next row failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSortOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (is_first_) {
    // The name 'get_effective_tenant_id()' is really confusing. Here what we want is to account
    // the resource usage(memory usage in this case) to a 'real' tenant rather than billing
    // the innocent DEFAULT tenant. We should think about changing the name of this function.
    int64_t row_count = MY_SPEC.rows_;
    const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    is_first_ = false;
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, MY_SPEC.rows_, row_count))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (NULL != MY_SPEC.topn_expr_ || NULL != MY_SPEC.topk_limit_expr_) {  // topn sort
      OZ(topn_sort_.init(
          tenant_id, MY_SPEC.prefix_pos_, &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_, &eval_ctx_));
      read_func_ = &ObSortOp::topn_sort_next;
      topn_sort_.set_fetch_with_ties(MY_SPEC.is_fetch_with_ties_);
    } else if (MY_SPEC.prefix_pos_ > 0) {
      OZ(prefix_sort_impl_.init(tenant_id,
          MY_SPEC.prefix_pos_,
          MY_SPEC.all_exprs_,
          &MY_SPEC.sort_collations_,
          &MY_SPEC.sort_cmp_funs_,
          &eval_ctx_,
          child_,
          this,
          ctx_,
          sort_row_count_));
      read_func_ = &ObSortOp::prefix_sort_impl_next;
      prefix_sort_impl_.set_input_rows(row_count);
      prefix_sort_impl_.set_input_width(MY_SPEC.width_);
      prefix_sort_impl_.set_operator_type(MY_SPEC.type_);
      prefix_sort_impl_.set_operator_id(MY_SPEC.id_);
      prefix_sort_impl_.set_exec_ctx(&ctx_);
    } else {
      OZ(sort_impl_.init(
          tenant_id, &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_, &eval_ctx_, MY_SPEC.is_local_merge_sort_));
      read_func_ = &ObSortOp::sort_impl_next;
      sort_impl_.set_input_rows(row_count);
      sort_impl_.set_input_width(MY_SPEC.width_);
      sort_impl_.set_operator_type(MY_SPEC.type_);
      sort_impl_.set_operator_id(MY_SPEC.id_);
      sort_impl_.set_exec_ctx(&ctx_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(process_sort())) {  // process sort
        if (OB_ITER_END != ret) {
          LOG_WARN("process sort failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL((this->*read_func_)())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      } else {
        iter_end_ = true;
        reset();
      }
    } else {
      ++ret_row_count_;
      LOG_DEBUG("finish ObSortOp::inner_get_next_row", K(MY_SPEC.output_), K(ret_row_count_));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
