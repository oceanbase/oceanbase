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
#include "mock_child.h"
#include <algorithm>
#include <vector>
#include <cstdint>
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
int MockChildOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  ObEvalCtx &ctx = (eval_ctx_ref_ != nullptr) ? *eval_ctx_ref_ : eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(max_row_cnt);
  batch_info_guard.set_batch_idx(0);
  // Keep shared exec_ctx frame pointer aligned to the generation context.
  // Limit and child may both reference frames through exec_ctx in UT setup.
  ctx_.set_frames(ctx.frames_);
  // Keep parent operator eval_ctx aligned as well so parent save_store_row
  // reads vectors from the same frame set written by mock child generation.
  if (parent_eval_ctx_ref_ != nullptr) {
    parent_eval_ctx_ref_->frames_ = ctx.frames_;
    parent_eval_ctx_ref_->max_batch_size_ = ctx.max_batch_size_;
  }
  // ObOperator::get_next_batch() performs output projection with operator eval_ctx_.
  // Keep operator eval_ctx_ aligned to generation ctx so projection doesn't read
  // another frame set and accidentally cast output exprs from stale memory.
  eval_ctx_.frames_ = ctx.frames_;
  eval_ctx_.max_batch_size_ = ctx.max_batch_size_;
  skip_words_.resize(ObBitVector::word_count(max_row_cnt));
  std::fill(skip_words_.begin(), skip_words_.end(), 0);
  brs_.size_ = max_row_cnt;
  brs_.skip_ = to_bit_vector(reinterpret_cast<char *>(skip_words_.data()));
  for (int64_t col = 0; col < get_spec().output_.count(); ++col) {
    ObExpr *expr = get_spec().output_.at(col);
    // Reset full batch evaluation state on the same ctx where data is generated.
    expr->get_eval_info(ctx).clear_evaluated_flag();
    expr->get_evaluated_flags(ctx).reset(max_row_cnt);
    gen_expr_makers_[col]->gen(*expr, brs_, ctx);
  }
  return OB_SUCCESS;
}

void MockChildUtils::init(const int64_t batch_size,
                          const std::vector<GenExprMaker *> &gen_expr_makers)
{
  common::ObIAllocator &alloc = exec_ctx_.get_allocator();
  spec_.max_batch_size_ = batch_size;
  spec_.output_.set_allocator(&alloc);
  ASSERT_EQ(OB_SUCCESS, spec_.output_.init(gen_expr_makers.size()));
  for (int64_t i = 0; i < gen_expr_makers.size(); ++i) {
    ObExpr *child_output_expr = static_cast<ObExpr *>(alloc.alloc(sizeof(ObExpr)));
    ASSERT_NE(nullptr, child_output_expr);
    new (child_output_expr) ObExpr();
    gen_expr_makers.at(i)->make(*child_output_expr, eval_ctx_);
    ASSERT_EQ(OB_SUCCESS, spec_.output_.push_back(child_output_expr));
  }
  op_ = new (alloc.alloc(sizeof(MockChildOp)))
      MockChildOp(exec_ctx_, spec_, nullptr, gen_expr_makers, &eval_ctx_);
  ASSERT_NE(nullptr, op_);
}

void MockChildUtils::attach_to_parent(ObOpSpec &parent_spec, ObOperator &parent_op)
{
  ASSERT_NE(nullptr, op_);
  op_->set_parent_eval_ctx(&parent_op.get_eval_ctx());
  // Keep parent/child operator eval contexts aligned to fixture frames.
  // This centralizes frame-sync wiring in mock-child utility instead of test cases.
  parent_op.get_eval_ctx().frames_ = exec_ctx_.get_frames();
  parent_op.get_eval_ctx().max_batch_size_ = spec_.max_batch_size_;
  op_->get_eval_ctx().frames_ = exec_ctx_.get_frames();
  op_->get_eval_ctx().max_batch_size_ = spec_.max_batch_size_;
  child_spec_arr_[0] = &spec_;
  child_op_arr_[0] = op_;
  ASSERT_EQ(OB_SUCCESS, parent_spec.set_children_pointer(child_spec_arr_, 1));
  ASSERT_EQ(OB_SUCCESS, parent_op.set_children_pointer(child_op_arr_, 1));
}

} // namespace sql
} // namespace oceanbase
