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

namespace oceanbase
{
namespace rpc
{

ObLockWaitNode::ObLockWaitNode() :
  hold_key_(0), need_wait_(false), request_stat_(), addr_(NULL), recv_ts_(0), lock_ts_(0), lock_seq_(0),
  abs_timeout_(0), tablet_id_(common::OB_INVALID_ID), try_lock_times_(0), sessid_(0),
  holder_sessid_(0), block_sessid_(0), tx_id_(0), holder_tx_id_(0), run_ts_(0),
  is_standalone_task_(false), last_compact_cnt_(0), total_update_cnt_(0) {}

void ObLockWaitNode::set(void *addr,
                         int64_t hash,
                         int64_t lock_seq,
                         int64_t timeout,
                         uint64_t tablet_id,
                         const int64_t last_compact_cnt,
                         const int64_t total_trans_node_cnt,
                         const char *key,
                         const uint32_t sess_id,
                         const uint32_t holder_sess_id,
                         int64_t tx_id,
                         int64_t holder_tx_id,
                         const share::ObLSID &ls_id)
{
  hash_ = hash | 1;
  addr_ = addr;
  lock_ts_ = common::ObTimeUtil::current_time();
  lock_seq_ = lock_seq;
  abs_timeout_ = timeout;
  tablet_id_ = tablet_id;//used for gv$lock_wait_stat
  sessid_ = sess_id;
  holder_sessid_ = holder_sess_id;
  tx_id_ = tx_id;//requester used for deadlock detection
  holder_tx_id_ = holder_tx_id; // txn id of lock holder
  last_compact_cnt_ = last_compact_cnt,
  total_update_cnt_ = total_trans_node_cnt;
  run_ts_ = 0;
  snprintf(key_, sizeof(key_), "%s", key);
  reset_need_wait();
}

void ObLockWaitNode::change_hash(const int64_t hash, const int64_t lock_seq)
{
  hash_ = hash | 1;
  lock_seq_ = lock_seq;
}

int ObLockWaitNode::compare(ObLockWaitNode* that) {
  int ret = 0;
  if (this->hash_ > that->hash_) {
    ret = 1;
  } else if (this->hash_ < that->hash_) {
    ret = -1;
  } else if (this->is_dummy()) {
    ret = 0;
  } else if(this->recv_ts_ > that->recv_ts_) {
    ret = 1;
  } else if(this->recv_ts_ < that->recv_ts_) {
    ret = -1;
  } else if(this->abs_timeout_ > that->abs_timeout_) {
    ret = 1;
  } else if(this->abs_timeout_ < that->abs_timeout_) {
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

} // namespace rpc
} // namespace oceanbase
