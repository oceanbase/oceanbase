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

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_rollback_section.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

RollbackNode::RollbackNode(const transaction::ObTxSEQ &rollback_from_seq, const transaction::ObTxSEQ &rollback_to_seq)
  : from_seq_(rollback_from_seq),
    to_seq_(rollback_to_seq)
{
}

RollbackNode::~RollbackNode()
{
  from_seq_.reset();
  to_seq_.reset();
}

bool RollbackNode::is_valid() const
{
  return from_seq_.is_valid() && to_seq_.is_valid() && from_seq_ > to_seq_;
}

bool RollbackNode::should_rollback_stmt(const transaction::ObTxSEQ &stmt_seq_no) const
{
  bool need_rollback = false;
  const int64_t rollback_branch_id = from_seq_.get_branch();
  const int64_t stmt_branch_id = stmt_seq_no.get_branch();
  const bool is_branch_rollback = (rollback_branch_id != 0); // branch_id should large than 0 if rollback in branch
  const bool need_check_rollback = (! is_branch_rollback) || rollback_branch_id == stmt_branch_id;

  if (need_check_rollback) {
    // note: from_seq is large than to_seq
    need_rollback = from_seq_ >= stmt_seq_no && to_seq_ < stmt_seq_no;
    if (need_rollback) {
      LOG_DEBUG("ROLLBACK_STMT", K(is_branch_rollback), K(stmt_seq_no), K_(from_seq), K_(to_seq));
    }
  }

  return need_rollback;
}

///////////////////////////////////////////////////////////////////////////////
// RollbackList
///////////////////////////////////////////////////////////////////////////////

void RollbackList::reset()
{
  rollback_node_list_.reset();
  optimized_ = false;
  intervals_.reset();
  min_rollback_seq_ = INT64_MAX;
  max_rollback_seq_ = 0;
  has_global_rollback_ = false;
}

int RollbackList::add(RollbackNode *node)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(optimized_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("cannot add to optimized rollback list", KR(ret), KPC(node), KPC(this));
  } else if (OB_ISNULL(node) || OB_UNLIKELY(! node->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid rollback node", KR(ret), KPC(node));
  } else if (OB_FAIL(rollback_node_list_.add(node))) {
    LOG_ERROR("add rollback node failed", KR(ret), KPC(node));
  }

  return ret;
}

int RollbackList::optimize()
{
  int ret = OB_SUCCESS;

  if (optimized_) {
    // already done
  } else if (rollback_node_list_.num_ == 0) {
    optimized_ = true;
  } else {
    // Step 1: collect intervals from linked list into a temporary sortable array
    ObSEArray<RollbackInterval, INLINE_INTERVAL_COUNT> temp;
    const RollbackNode *node = rollback_node_list_.head_;

    while (OB_SUCC(ret) && OB_NOT_NULL(node)) {
      if (node->is_valid()) {
        if (OB_FAIL(temp.push_back(RollbackInterval(node)))) {
          LOG_ERROR("push_back interval failed", KR(ret));
        }
      }
      node = node->get_next();
    }

    if (OB_FAIL(ret) || temp.count() == 0) {
      if (OB_SUCC(ret)) { optimized_ = true; }
      return ret;
    }

    // Step 2: sort by (branch_id, from_seq)
    lib::ob_sort(temp.begin(), temp.end());

    // Step 3: merge overlapping / adjacent intervals within the same branch
    RollbackInterval merged = temp.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < temp.count(); ++i) {
      const RollbackInterval &cur = temp.at(i);
      if (merged.can_merge_with(cur)) {
        merged.merge_with(cur);
      } else {
        if (OB_FAIL(intervals_.push_back(merged))) {
          LOG_ERROR("push_back merged interval failed", KR(ret));
        }
        merged = cur;
      }
    }
    if (FAILEDx(intervals_.push_back(merged))) {
      LOG_ERROR("push_back last interval failed", KR(ret));
    }

    // Step 4: compute global bounds for fast O(1) rejection
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < intervals_.count(); ++i) {
        const RollbackInterval &iv = intervals_.at(i);
        const int64_t to_seq = iv.to_seq_.get_seq();
        const int64_t from_seq = iv.from_seq_.get_seq();
        if (to_seq < min_rollback_seq_) { min_rollback_seq_ = to_seq; }
        if (from_seq > max_rollback_seq_) { max_rollback_seq_ = from_seq; }
        if (0 == iv.get_branch_id()) { has_global_rollback_ = true; }
      }
      optimized_ = true;
      LOG_DEBUG("RollbackList optimized",
                "original_count", rollback_node_list_.num_,
                "merged_count", intervals_.count(),
                K_(min_rollback_seq), K_(max_rollback_seq),
                K_(has_global_rollback));
    } else {
      intervals_.reset();
    }
  }

  return ret;
}

bool RollbackList::should_rollback_stmt(const transaction::ObTxSEQ &stmt_seq_no) const
{
  if (OB_UNLIKELY(! optimized_)) {
    return should_rollback_stmt_fallback_(stmt_seq_no);
  }

  if (intervals_.count() == 0) {
    return false;
  }

  // O(1) bounds check: stmt_seq must be in (min_to_seq, max_from_seq] across all branches.
  // Rows clearly outside every rollback range are rejected without binary search.
  const int64_t stmt_seq = stmt_seq_no.get_seq();
  if (stmt_seq > max_rollback_seq_ || stmt_seq <= min_rollback_seq_) {
    return false;
  }

  // Check global (branch_id=0) intervals — they apply to rows of any branch
  if (has_global_rollback_ && binary_search_in_branch_(0, stmt_seq_no)) {
    return true;
  }

  // Check branch-specific intervals
  const int16_t stmt_branch = stmt_seq_no.get_branch();
  if (stmt_branch != 0 && binary_search_in_branch_(stmt_branch, stmt_seq_no)) {
    return true;
  }

  return false;
}

// Binary search within the sorted intervals_ array for target_branch.
//
// Because intervals_ is sorted by (branch_id, from_seq) and intervals within
// the same branch are non-overlapping, we can find the unique candidate with
// a single lower-bound search:
//   - find the first interval with (branch >= target_branch, from_seq >= stmt_seq)
//   - if it's in target_branch and contains stmt_seq, return true
//
bool RollbackList::binary_search_in_branch_(
    const int16_t target_branch,
    const transaction::ObTxSEQ &stmt_seq) const
{
  int64_t left = 0;
  int64_t right = intervals_.count() - 1;

  while (left <= right) {
    const int64_t mid = left + (right - left) / 2;
    const RollbackInterval &iv = intervals_.at(mid);
    const int16_t b = iv.get_branch_id();

    if (b < target_branch) {
      left = mid + 1;
    } else if (b > target_branch) {
      right = mid - 1;
    } else {
      // same branch — narrow by from_seq
      if (iv.from_seq_ < stmt_seq) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
  }

  // left is the lower-bound position: first interval with
  // (branch > target_branch) OR (branch == target_branch && from_seq >= stmt_seq)
  if (left < intervals_.count()) {
    const RollbackInterval &iv = intervals_.at(left);
    if (iv.get_branch_id() == target_branch && iv.contains_seq(stmt_seq)) {
      return true;
    }
  }

  return false;
}

bool RollbackList::should_rollback_stmt_fallback_(const transaction::ObTxSEQ &stmt_seq_no) const
{
  const RollbackNode *node = rollback_node_list_.head_;

  while (OB_NOT_NULL(node)) {
    if (node->should_rollback_stmt(stmt_seq_no)) {
      return true;
    }
    node = node->get_next();
  }

  return false;
}

}
}
