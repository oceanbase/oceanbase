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

#include "ob_merge_except_op.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObMergeExceptSpec::ObMergeExceptSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObMergeSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeExceptSpec, ObMergeSetSpec));

ObMergeExceptOp::ObMergeExceptOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObMergeSetOp(exec_ctx, spec, input), right_iter_end_(false), first_got_right_row_(true)
{}

int ObMergeExceptOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == left_ || nullptr == right_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left or right is null", K(ret));
  } else if (OB_FAIL(ObMergeSetOp::inner_open())) {
    LOG_WARN("failed to init open", K(ret));
  } else {
    need_skip_init_row_ = true;
  }
  return ret;
}

int ObMergeExceptOp::inner_close()
{
  return ObMergeSetOp::inner_close();
}

int ObMergeExceptOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMergeSetOp::rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    right_iter_end_ = false;
    first_got_right_row_ = true;
    need_skip_init_row_ = true;
  }
  return ret;
}

void ObMergeExceptOp::destroy()
{
  ObMergeSetOp::destroy();
}

int ObMergeExceptOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  bool break_outer_loop = false;
  const ObIArray<ObExpr*>* left_row = NULL;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && OB_SUCC(do_strict_distinct(*left_, last_row_.store_row_, left_row))) {
    break_outer_loop = right_iter_end_;
    while (OB_SUCC(ret) && !right_iter_end_) {
      if (!first_got_right_row_) {
        if (OB_FAIL(cmp_(right_->get_spec().output_, *left_row, eval_ctx_, cmp))) {
          LOG_WARN("cmp_ input_row with right_row failed", K(*left_row), K(ret));
        } else if (cmp > 0) {
          // input_row is not in the right set, output input_row
          break_outer_loop = true;
          break;
        } else if (0 == cmp) {
          break;
        } else {
          if (OB_FAIL(right_->get_next_row())) {
            if (OB_ITER_END == ret) {
              right_iter_end_ = true;
              break_outer_loop = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get right operator row", K(ret));
            }
          } else {
          }
        }
      } else {
        // get first row
        first_got_right_row_ = false;
        if (OB_FAIL(right_->get_next_row())) {
          if (OB_ITER_END == ret) {
            right_iter_end_ = true;
            break_outer_loop = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get right operator row", K(ret));
          }
        }
      }
    }
    if (break_outer_loop) {
      break;  // cmp < 0 implement that the left_row is not in the right set, so output it
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_row(*left_row, MY_SPEC.output_))) {
      LOG_WARN("failed to convert row", K(ret));
    } else if (OB_FAIL(last_row_.save_store_row(*left_row, eval_ctx_, 0))) {
      LOG_WARN("failed to save right row", K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
