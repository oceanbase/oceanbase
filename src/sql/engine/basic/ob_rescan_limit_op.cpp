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

#include "sql/engine/basic/ob_rescan_limit_op.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObRescanLimitSpec, ObOpSpec));

// int ObRescanLimitOp::inner_open()
// {
//   int ret = OB_SUCCESS;
//   return ret;
// }

// void ObRescanLimitOp::destroy()
// {
//   ObOperator::destroy();
// }

// int ObRescanLimitOp::rescan()
// {
//   int ret = OB_SUCCESS;
// #ifndef NDEBUG
//   OX(OB_ASSERT(1 == child_cnt_));
// #endif
//   LOG_DEBUG("rescan limit", K(current_rescan_count_), K(MY_SPEC.rescan_limit_));
//   if (-1 == MY_SPEC.rescan_limit_ && OB_FAIL(ObOperator::rescan())) {
//   } else if (current_rescan_count_++ < MY_SPEC.rescan_limit_ && OB_FAIL(ObOperator::rescan())) {
//     LOG_WARN("rescan child operator failed",
//               K(ret), K(current_rescan_count_), K(MY_SPEC.rescan_limit_),
//               "op_type", op_name(), "child op_type", children_[0]->op_name());
//   }

//   return ret;
// }

int ObRescanLimitOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next batch", K(ret));
    }
  }
  brs_.copy(child_brs);
  if ((OB_ITER_END == ret or brs_.end_) && current_rescan_count_++ < MY_SPEC.rescan_limit_) {
    iter_end_ = true;
    brs_.end_ = false;
    child_->rescan();
  }
  // only output the rows in the last rescan
  // if (current_rescan_count_ != MY_SPEC.rescan_limit_) {
  //   brs_.size_ = 0;
  // }
  return ret;
}

// int ObRescanLimitOp::inner_close()
// {
//   int ret = OB_SUCCESS;
//   return ret;
// }

// int ObRescanLimitOp::inner_get_next_batch(int64_t max_row_cnt)
// {
//   int ret = OB_SUCCESS;
//   return ret;
// }

} // end namespace sql
} // end namespace oceanbase
