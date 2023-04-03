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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_rollback_section.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

RollbackNode::RollbackNode(const int64_t rollback_from_seq, const int64_t rollback_to_seq)
  : from_seq_(rollback_from_seq),
    to_seq_(rollback_to_seq)
{
}

RollbackNode::~RollbackNode()
{
  from_seq_ = -1;
  to_seq_ = -1;
}

bool RollbackNode::is_valid() const
{
  return from_seq_ >= 0 && to_seq_ >= 0 && from_seq_ >= to_seq_;
}

bool RollbackNode::should_rollback_stmt(const int64_t stmt_seq_no) const
{
  // note: from_seq is large than to_seq
  return from_seq_ >= stmt_seq_no && to_seq_ < stmt_seq_no;
}

}
}
