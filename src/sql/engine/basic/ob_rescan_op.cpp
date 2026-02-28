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

#include "sql/engine/basic/ob_rescan_op.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObRescanSpec, ObOpSpec), rescan_cnt_);

int ObRescanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memory_usages_ = static_cast<int64_t *>(ctx_.get_allocator().alloc((MY_SPEC.rescan_cnt_ + 1) * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }
  return ret;
}

int ObRescanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else {
    brs_.copy(child_brs);
    if (current_rescan_cnt_ < MY_SPEC.rescan_cnt_) {
      brs_.size_ = 0;
    }
  }
  // Last time rescan don't record memory usage
  // because it may be have extra memory used in ez_buffer.
  if (OB_SUCC(ret) && brs_.end_ && current_rescan_cnt_ < MY_SPEC.rescan_cnt_) {
    // record memory usage
    int64_t memory_usage = CURRENT_CONTEXT.tree_mem_hold();
    peak_memory_usage_ = max(peak_memory_usage_, memory_usage);
    memory_usages_[current_rescan_cnt_] = memory_usage;
    if (current_rescan_cnt_ == MY_SPEC.rescan_cnt_ - 1 && OB_FAIL(check_memory_usages())) {
      ObArray<int64_t> memory_usage_arr;
      for (int i = 0; i <= current_rescan_cnt_; i++) {
        memory_usage_arr.push_back(memory_usages_[i]);
      }
      LOG_WARN("there is a memory issue", K(ret), K(memory_usage_arr));
    }
  }
  if ((OB_SUCC(ret) && brs_.end_) && current_rescan_cnt_ < MY_SPEC.rescan_cnt_) {
    current_rescan_cnt_++;
    iter_end_ = false;
    brs_.end_ = false;
    child_->rescan();
  }
  return ret;
}

int ObRescanOp::check_memory_usages()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.rescan_cnt_ == 1) {
    // skip
  } else {
    // baseline_memory = memory_usages[0]
    // peak_memory is the maximum memory usage
    // peak_memory > 1.5 * baseline_memory
    int64_t baseline_memory = memory_usages_[0];
    if (peak_memory_usage_ > 1.5 * baseline_memory) {
      ret = OB_MEMORY_LEAK;
      LOG_WARN("there is a memory issue", K(ret), K(baseline_memory), K(peak_memory_usage_));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase