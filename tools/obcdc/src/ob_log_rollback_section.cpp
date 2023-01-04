/**
 * Copyright (c) 2022 OceanBase
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
namespace liboblog
{
using namespace oceanbase::common;

RollbackNode::RollbackNode(uint64_t log_id)
  : log_id_(log_id),
    rollback_seqs_()
{
}

RollbackNode::~RollbackNode()
{
  log_id_ = OB_INVALID_ID;
  rollback_seqs_.destroy();
}

int RollbackNode::push(int64_t rollback_seq)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(rollback_seqs_.push_back(rollback_seq))) {
    LOG_ERROR("rollback_seqs_ push_back fail", KR(ret), K(rollback_seq));
  }

  return ret;
}

int64_t RollbackNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t size = rollback_seqs_.size();

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "lod_id=[%ld] ", log_id_);

    (void)common::databuff_printf(buf, buf_len, pos, "rollback_seq=[");
    for (int64_t idx = 0; idx < size; ++idx) {
      int64_t rollback_seq = rollback_seqs_[idx];
      if (idx == size - 1) {
        (void)common::databuff_printf(buf, buf_len, pos, "%ld", rollback_seq);
      } else {
        (void)common::databuff_printf(buf, buf_len, pos, "%ld ", rollback_seq);
      }
    }
    (void)common::databuff_printf(buf, buf_len, pos, "]");
  }

  return pos;
}

}
}
