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

#include "ob_lock_wait_node.h"

namespace oceanbase {
namespace rpc {

ObLockWaitNode::ObLockWaitNode()
    : hold_key_(0),
      need_wait_(false),
      addr_(NULL),
      recv_ts_(0),
      lock_ts_(0),
      lock_seq_(0),
      abs_timeout_(0),
      table_id_(common::OB_INVALID_ID),
      try_lock_times_(0),
      sessid_(0),
      block_sessid_(0),
      sessid_version_(0),
      ctx_desc_(0),
      run_ts_(0),
      is_standalone_task_(false),
      total_update_cnt_(0)
{}

void ObLockWaitNode::set(void* addr, int64_t hash, int64_t lock_seq, int64_t timeout, uint64_t table_id,
    const int64_t total_trans_node_cnt, const char* key, uint32_t ctx_desc)
{
  hash_ = hash | 1;
  addr_ = addr;
  lock_ts_ = common::ObTimeUtil::current_time();
  lock_seq_ = lock_seq;
  abs_timeout_ = timeout;
  table_id_ = table_id;  // used for gv$lock_wait_stat
  ctx_desc_ = ctx_desc;  // used for deadlock detection
  total_update_cnt_ = total_trans_node_cnt;

  snprintf(key_, sizeof(key_), "%s", key);
}

void ObLockWaitNode::change_hash(const int64_t hash, const int64_t lock_seq)
{
  hash_ = hash | 1;
  lock_seq_ = lock_seq;
}

int ObLockWaitNode::compare(ObLockWaitNode* that)
{
  int ret = 0;
  if (this->hash_ > that->hash_) {
    ret = 1;
  } else if (this->hash_ < that->hash_) {
    ret = -1;
  } else if (this->is_dummy()) {
    ret = 0;
  } else if (this->abs_timeout_ > that->abs_timeout_) {
    ret = 1;
  } else if (this->abs_timeout_ < that->abs_timeout_) {
    ret = -1;
  } else if ((uint64_t)this->addr_ > (uint64_t)that->addr_) {
    ret = 1;
  } else if ((uint64_t)this->addr_ < (uint64_t)that->addr_) {
    ret = -1;
  } else {
    ret = 0;
  }
  return ret;
}

}  // namespace rpc
}  // namespace oceanbase
