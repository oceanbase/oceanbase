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

#ifndef OCEANBASE_LIBOBCDC_ROLLBACK_SECTION_H_
#define OCEANBASE_LIBOBCDC_ROLLBACK_SECTION_H_

#include "ob_log_lighty_list.h"                     // LightyList
#include "lib/queue/ob_link.h"                      // ObLink
#include "lib/container/ob_se_array.h"              // ObSEArray
#include "storage/tx/ob_trans_define.h"             // ObTxSEQ

namespace oceanbase
{
namespace libobcdc
{
// One RollbackToLog correspond to one RollbackNode
class RollbackNode : public common::ObLink
{
public:
  RollbackNode(const transaction::ObTxSEQ &rollback_from_seq, const transaction::ObTxSEQ &rollback_to_seq);
  ~RollbackNode();

public:
  bool is_valid() const;
  void set_next(RollbackNode* next) { next_ = next; }
  const RollbackNode *get_next() const { return static_cast<RollbackNode*>(next_); }
  const transaction::ObTxSEQ &get_rollback_from_seq() const { return from_seq_; }
  const transaction::ObTxSEQ &get_rollback_to_seq() const { return to_seq_; }
  bool should_rollback_stmt(const transaction::ObTxSEQ &stmt_seq_no) const;

  TO_STRING_KV(K_(from_seq), K_(to_seq));
private:
  // from_seq_ > to_seq_
  transaction::ObTxSEQ from_seq_;
  transaction::ObTxSEQ to_seq_;
};

// Rollback algorithm:
// We will rollback statements based the total rollback to savepoint seq through traversal the all RollbackNode.
//
// EG: as following, one Participant have 4 RedoLog, and the third/fourth of them exist rollback to savepoint
// 1. We should find all RollbackNodes that are greater than the current log when DmlParser handle the RedoLog,
//    eg, for log_id=1, [1, 100]
//    RollbackNode (150) and (350, 380) all meet the conditions, so we can rollback statement through traversal the
//    all rollback to savepoint seq:
//    (150) -> do nothing
//    (350, 380) -> do nothing
//
//    for log_id=5, [101, 200]
//    (150) -> rollback, [101, 150)
//    (350, 380) -> do nothing
// 2. We should deal specifically with logs that contain rollback to savepoint, rollback statements based on the information contained in the log.
//    eg, log_id=15 [300, 400]
//    (350, 380) -> [300, 350) and (380, 400]
// 3. RollbackList will sort by LogId to increase the rollback speed
//
// ParticipantRedoLog:    [log_id=1] -----> [log_id=5] -----> [log_id=10] -----> [log_id=15] -----> NULL
// StmtSeqRange:          [1, 100]          [101, 200]        [201, 300]         [300, 400]
// rollback to savepoint:   None              None             150                350, 380
// RollbackList:          (150) -----> (350, 380) -----> NULL

typedef LightyList<RollbackNode> RollbackNodeList;

// Merged, non-overlapping interval for fast rollback lookup.
// Represents one or more RollbackNodes within the same branch after merging.
// Interval semantics: (to_seq_, from_seq_] — from_seq_ > to_seq_ always holds.
struct RollbackInterval
{
  transaction::ObTxSEQ from_seq_;  // upper bound (inclusive)
  transaction::ObTxSEQ to_seq_;    // lower bound (exclusive)

  RollbackInterval() {}
  RollbackInterval(const transaction::ObTxSEQ &from, const transaction::ObTxSEQ &to)
    : from_seq_(from), to_seq_(to) {}
  explicit RollbackInterval(const RollbackNode *node)
    : from_seq_(node->get_rollback_from_seq()), to_seq_(node->get_rollback_to_seq()) {}

  int16_t get_branch_id() const { return from_seq_.get_branch(); }

  // Check if stmt_seq is in (to_seq_, from_seq_]. Caller ensures branch match.
  bool contains_seq(const transaction::ObTxSEQ &stmt_seq) const
  {
    return from_seq_ >= stmt_seq && to_seq_ < stmt_seq;
  }

  bool can_merge_with(const RollbackInterval &other) const
  {
    if (get_branch_id() != other.get_branch_id()) { return false; }
    return to_seq_ <= other.from_seq_ && other.to_seq_ <= from_seq_;
  }

  void merge_with(const RollbackInterval &other)
  {
    if (other.from_seq_ > from_seq_) { from_seq_ = other.from_seq_; }
    if (other.to_seq_ < to_seq_) { to_seq_ = other.to_seq_; }
  }

  bool operator<(const RollbackInterval &other) const
  {
    const int16_t b = get_branch_id();
    const int16_t ob = other.get_branch_id();
    if (b != ob) { return b < ob; }
    return from_seq_ < other.from_seq_;
  }

  TO_STRING_KV(K_(from_seq), K_(to_seq), "branch_id", get_branch_id());
};

// Optimized rollback list.
//
// Accumulation phase (log parsing): nodes are appended to a linked list via add().
// Optimization phase (PartTransTask::commit): optimize() merges overlapping intervals,
// sorts them, and computes global seq bounds for O(1) rejection.
//
// Lookup: should_rollback_stmt() performs:
//   1. O(1) global bounds check — rejects rows outside all rollback ranges
//   2. O(log M) binary search within the applicable branch intervals
// Falls back to O(N) linked list scan if optimize() has not been called.
class RollbackList
{
  static const int64_t INLINE_INTERVAL_COUNT = 16;

public:
  RollbackList()
    : rollback_node_list_(),
      optimized_(false),
      intervals_(),
      min_rollback_seq_(INT64_MAX),
      max_rollback_seq_(0),
      has_global_rollback_(false) {}
  ~RollbackList() { reset(); }

  void reset();
  int add(RollbackNode *node);

  // Merge and sort intervals for fast lookup. Call once after all nodes are added.
  int optimize();
  bool is_optimized() const { return optimized_; }

  bool should_rollback_stmt(const transaction::ObTxSEQ &stmt_seq_no) const;

  int64_t get_node_count() const { return rollback_node_list_.num_; }

  TO_STRING_KV(K_(optimized),
               "node_num", rollback_node_list_.num_,
               "interval_num", intervals_.count(),
               K_(has_global_rollback),
               K_(min_rollback_seq),
               K_(max_rollback_seq));

private:
  // Binary search for the first interval in target_branch with from_seq >= stmt_seq,
  // then check if that interval contains stmt_seq.
  bool binary_search_in_branch_(int16_t target_branch, const transaction::ObTxSEQ &stmt_seq) const;

  bool should_rollback_stmt_fallback_(const transaction::ObTxSEQ &stmt_seq_no) const;

private:
  RollbackNodeList rollback_node_list_;
  bool optimized_;
  common::ObSEArray<RollbackInterval, INLINE_INTERVAL_COUNT> intervals_;
  int64_t min_rollback_seq_;    // min to_seq.get_seq() across all intervals
  int64_t max_rollback_seq_;    // max from_seq.get_seq() across all intervals
  bool has_global_rollback_;    // true if any interval has branch_id == 0
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
