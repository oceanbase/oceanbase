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
  last_wait_hash_(0), need_wait_(false), request_stat_(), addr_(NULL), recv_ts_(0), lock_ts_(0), lock_seq_(0),
  lock_wait_expire_ts_(0), ls_id_(0), tablet_id_(common::OB_INVALID_ID), try_lock_times_(0), sessid_(0), assoc_sess_id_(0),
  holder_sessid_(), block_sessid_(0), tx_id_(0), holder_tx_id_(0), holder_tx_hold_seq_value_(0), last_compact_cnt_(0),
  total_update_cnt_(0), is_placeholder_(false), wait_timeout_ts_(0), tx_active_ts_(0), node_id_(0),
  exec_addr_(), node_type_(NODE_TYPE::LOCAL), wait_row_hash_(0), last_touched_thread_id_(0) {
    memset(key_, '\0', sizeof(key_));
  }

void ObLockWaitNode::set(void *addr,
                         int64_t hash,
                         int64_t lock_seq,
                         int64_t ls_id,
                         uint64_t tablet_id,
                         const int64_t last_compact_cnt,
                         const int64_t total_trans_node_cnt,
                         const char *key,
                         uint64_t key_size,
                         const uint32_t client_sid,
                         const uint32_t holder_sess_id,
                         int64_t tx_id,
                         int64_t holder_tx_id,
                         int64_t holder_tx_hold_seq_value,
                         int64_t tx_active_ts,
                         uint32_t assoc_sess_id,
                         const NODE_TYPE node_type,
                         const ObAddr &exec_addr) {
  hash_ = hash | 1;
  addr_ = addr;
  lock_ts_ = common::ObTimeUtil::current_time();
  lock_seq_ = lock_seq;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;//used for gv$lock_wait_stat
  client_sid_ = client_sid;
  holder_sessid_ = holder_sess_id;
  tx_id_ = tx_id;//requester used for deadlock detection
  holder_tx_id_ = holder_tx_id; // txn id of lock holder
  holder_tx_hold_seq_value_ = holder_tx_hold_seq_value;// holder txn lock row by SQL identified by this seq
  last_compact_cnt_ = last_compact_cnt,
  total_update_cnt_ = total_trans_node_cnt;
  if (0 != assoc_sess_id) {
    sessid_ = assoc_sess_id;
  }
  if (NULL != key) {
    uint64_t copy_size = (key_size + 1) > KEY_BUFFER_SIZE ? KEY_BUFFER_SIZE : (key_size + 1);
    snprintf(key_, copy_size, "%s", key);
  }
  tx_active_ts_ = tx_active_ts;
  assoc_sess_id_ = assoc_sess_id;
  node_type_ = node_type;
  exec_addr_ = exec_addr;
  need_wait_ = true;
}

void ObLockWaitNode::change_hash(const int64_t hash, const int64_t lock_seq)
{
  // used for remote wake up
  wait_row_hash_ = hash_;
  hash_ = hash | 1;
  lock_seq_ = lock_seq;
}

int ObLockWaitNode::compare(ObLockWaitNode* that) {
  int ret = 0;
  int from = 0;
  if (this->hash_ > that->hash_) {
    ret = 1;
  } else if (this->hash_ < that->hash_) {
    ret = -1;
  } else if (this->is_dummy()) {
    ret = 0;
  } else if (this->is_placeholder_ && !that->is_placeholder_) {
    ret = -1;
  } else if (that->is_placeholder_ && !this->is_placeholder_) {
    ret = 1;
  } else if(this->recv_ts_ > that->recv_ts_) {
    ret = 1;
  } else if(this->recv_ts_ < that->recv_ts_) {
    ret = -1;
  } else if(this->lock_wait_expire_ts_ > that->lock_wait_expire_ts_) {
    ret = 1;
  } else if(this->lock_wait_expire_ts_ < that->lock_wait_expire_ts_) {
    ret = -1;
  } else if (this->lock_ts_ > that->lock_ts_) {
    ret = 1;
  } else if (this->lock_ts_ < that->lock_ts_) {
    ret = -1;
  } else if ((uint64_t)this->addr_ > (uint64_t)that->addr_) {
    ret = 1;
  } else if ((uint64_t)this->addr_ < (uint64_t)that->addr_) {
    ret = -1;
  } else {
    ret = 0;
  }
  // RPC_LOG(INFO, "compare node", K(ret), KP(this), KP(that), K(from));
  return ret;
}

} // namespace rpc
} // namespace oceanbase
