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
#include "lib/container/ob_array_iterator.h"        // ObArray
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
//
typedef LightyList<RollbackNode> RollbackList;

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
