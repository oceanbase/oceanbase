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

}
}
