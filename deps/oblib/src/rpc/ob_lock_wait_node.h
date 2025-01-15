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

#ifndef OCEANBASE_RPC_OB_LOCK_WAIT_NODE_
#define OCEANBASE_RPC_OB_LOCK_WAIT_NODE_

#include "lib/hash/ob_fixed_hash2.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/net/ob_addr.h"                                                                  // ObAddr

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace rpc
{

typedef uint64_t NodeID;

struct ObLockWaitNode: public common::SpHashNode
{
  static constexpr int64_t KEY_BUFFER_SIZE = 320;
  ObLockWaitNode();
  ~ObLockWaitNode() {}
  ObLockWaitNode &operator=(const ObLockWaitNode &rhs)
  {
    hash_ = rhs.hash_;
    last_wait_hash_ = rhs.last_wait_hash_;
    retire_link_ = rhs.retire_link_;
    need_wait_ = rhs.need_wait_;
    addr_ = rhs.addr_;
    recv_ts_ = rhs.recv_ts_;
    lock_ts_ = rhs.lock_ts_;
    lock_seq_ = rhs.lock_seq_;
    abs_timeout_ = rhs.abs_timeout_;
    tablet_id_ = rhs.tablet_id_;
    try_lock_times_ = rhs.try_lock_times_;
    sessid_ = rhs.sessid_;
    holder_sessid_ = rhs.holder_sessid_;
    block_sessid_ = rhs.block_sessid_;
    tx_id_ = rhs.tx_id_;
    holder_tx_id_ = rhs.holder_tx_id_;
    snprintf(key_, sizeof(rhs.key_), "%s", rhs.key_);
    lock_mode_ = rhs.lock_mode_;
    last_compact_cnt_ = rhs.last_compact_cnt_;
    total_update_cnt_ = rhs.total_update_cnt_;
    wait_timeout_ts_ = rhs.wait_timeout_ts_;
    node_id_ = rhs.node_id_;
    exec_addr_ = rhs.exec_addr_;
    node_type_ = rhs.node_type_;
    is_placeholder_ = rhs.is_placeholder_;
    tx_active_ts_ = rhs.tx_active_ts_;
    holder_tx_hold_seq_value_ = rhs.holder_tx_hold_seq_value_;
    ls_id_ = rhs.ls_id_;
    wait_row_hash_ = rhs.wait_row_hash_;
    last_touched_thread_id_ = rhs.last_touched_thread_id_;
    next_ = rhs.next_;
    return *this;
  }
  enum NODE_TYPE {
    // used for local execution, and is a memaber of a local processed request
    LOCAL = 0,
    // used for remote execution, control side lock wait node which is a memaber of a local processed request
    REMOTE_CTRL_SIDE = 1,
    // used for remote execution, execution side lock wait node which is dynamically allocated
    // and no corresponding request in this side
    REMOTE_EXEC_SIDE = 2,
    MAX
  };
  void reset_need_wait() { need_wait_ = false; }
  void set(void *addr,
           int64_t hash,
           int64_t lock_seq,
           int64_t timeout,
           int64_t ls_id,
           uint64_t tablet_id,
           const int64_t last_compact_cnt,
           const int64_t total_trans_node_cnt,
           const char *key,
           uint64_t key_size,
           const uint32_t holder_sess_id,
           int64_t tx_id,
           int64_t holder_tx_id,
           int64_t holder_tx_hold_seq_value,
           int64_t tx_active_ts,
           uint32_t assoc_sess_id,
           const NODE_TYPE node_type,
           const ObAddr &exec_addr);
  void change_hash(const int64_t hash, const int64_t lock_seq);
  int compare(ObLockWaitNode* that);
  bool is_timeout() { return common::ObTimeUtil::current_time() >= abs_timeout_; }
  int64_t get_abs_timeout() const { return abs_timeout_; }
  void set_need_wait() { need_wait_ = true; }
  void set_lock_mode(uint8_t lock_mode) { lock_mode_ = lock_mode; }
  bool need_wait() { return need_wait_; }
  void on_retry_lock(uint64_t hash) { last_wait_hash_ = hash; }
  void set_session_info(uint32_t sessid) {
    int ret = common::OB_SUCCESS;
    if (0 == sessid) {
      ret = common::OB_INVALID_ARGUMENT;
      RPC_LOG(WARN, "unexpected sessid", K(ret), K(sessid));
    } else {
      sessid_ = sessid;
    }
    UNUSED(ret);
  }
  uint32_t get_sess_id() const { return sessid_; }
  uint32_t get_assoc_session_id() const { return assoc_sess_id_; }
  void set_assoc_sess_id(uint32_t assoc_sess_id) { assoc_sess_id_ = assoc_sess_id; }
  void set_block_sessid(const uint32_t block_sessid) { block_sessid_ = block_sessid; }
  bool is_placeholder() { return is_placeholder_; }
  void set_node_type(const NODE_TYPE node_type) { node_type_ = node_type; }
  NODE_TYPE get_node_type() const { return node_type_; }
  void set_exec_addr(const ObAddr &addr) { exec_addr_ = addr; }
  ObAddr &get_exec_addr() { return exec_addr_; }
  const ObAddr &get_exec_addr() const { return exec_addr_; }
  ObAddr &get_ctrl_addr() { return ctrl_addr_; }
  const ObAddr &get_ctrl_addr() const { return ctrl_addr_; }
  NodeID get_node_id() const { return node_id_; }
  bool is_node_id_valid() const { return node_id_ > 0; }
  void set_node_id(const NodeID node_id) { node_id_ = node_id; }
  int64_t get_wait_timeout_ts() const { return ATOMIC_LOAD(&wait_timeout_ts_); }
  void set_wait_timeout_ts(const int64_t wait_timeout_ts) { ATOMIC_SET(&wait_timeout_ts_, wait_timeout_ts); }
  void reset_wait_timeout_ts() { ATOMIC_SET(&wait_timeout_ts_, 0); }
  bool is_wait_timeout() {
    int64_t timeout_ts = ATOMIC_LOAD(&wait_timeout_ts_);
    return timeout_ts != 0 && common::ObTimeUtil::current_time() > timeout_ts;
  }
  int64_t get_lock_seq() const { return lock_seq_; }
  int64_t get_lock_ts() const { return lock_ts_; }
  int64_t get_tx_id() const { return tx_id_; }
  int64_t get_holder_tx_id() const { return holder_tx_id_; }
  int64_t get_recv_ts() const { return recv_ts_; }
  int64_t get_tx_active_ts() const { return tx_active_ts_; }
  int64_t get_holder_tx_hold_seq_value() const { return holder_tx_hold_seq_value_; }
  const char* get_lock_key() const { return key_; }
  uint64_t get_wait_row_hash() const { return wait_row_hash_; }
  void set_wait_row_hash(uint64_t wait_row_hash) { wait_row_hash_ = wait_row_hash; }

  TO_STRING_KV(KP(this),
               KP_(addr),
               K_(last_wait_hash),
               K_(hash),
               K_(lock_ts),
               K_(lock_seq),
               K_(abs_timeout),
               K_(ls_id),
               K_(tablet_id),
               K_(try_lock_times),
               KCSTRING_(key),
               K_(sessid),
               K_(assoc_sess_id),
               K_(holder_sessid),
               K_(block_sessid),
               K_(lock_mode),
               "txid", tx_id_,
               K_(holder_tx_id),
               K_(holder_tx_hold_seq_value),
               K_(need_wait),
               K_(last_compact_cnt),
               K_(total_update_cnt),
               K_(is_placeholder),
               K_(exec_addr),
               K_(node_id),
               K_(node_type),
               K_(wait_timeout_ts),
               K_(recv_ts),
               K_(tx_active_ts),
               K_(wait_row_hash),
               K_(last_touched_thread_id));
  int64_t get_ls_id() const { return ls_id_; }
  uint64_t get_tablet_id() const { return tablet_id_; }
  uint64_t last_wait_hash_; // used to avoid missing wake up
  ObLink retire_link_;
  bool need_wait_;
  void* addr_;
  int64_t recv_ts_;
  int64_t lock_ts_;
  int64_t lock_seq_;
  int64_t abs_timeout_;
  int64_t ls_id_;
  uint64_t tablet_id_;
  int64_t try_lock_times_;
  uint32_t sessid_;
  uint32_t assoc_sess_id_;
  uint32_t holder_sessid_;
  uint32_t block_sessid_;
  int64_t tx_id_;
  int64_t holder_tx_id_;
  int64_t holder_tx_hold_seq_value_;
  char key_[KEY_BUFFER_SIZE];
  uint8_t lock_mode_;
  // There may be tasks that miss to wake up, called standalone tasks
  int64_t last_compact_cnt_;
  int64_t total_update_cnt_;
  // insert placeholder node into hash wait queue before request retries
  // ensures that only one request can retry on one same hash at a time
  // other requests observing the placeholder are not allowed to retry
  // and the placehodler node cannot be woken up
  bool is_placeholder_;
  // used for remote wakeup to avoid subsequent requests starve to death
  int64_t wait_timeout_ts_;
  // tx begin ts
  int64_t tx_active_ts_;
  // a unique node ID used to distinctly identify different nodes
  NodeID node_id_;
  union {
    common::ObAddr exec_addr_;
    common::ObAddr ctrl_addr_;
  }; // used for remote execution
  NODE_TYPE node_type_;
  // used to remote wakeup after wait on row to wait on trx transform happened
  // e.g: REMOTE_EXEC_SIDE node1 transform from waiting on row(hash1) to waiting on trx(hash2)
  // REMOTE_CTRL_SIDE node1' still wait on row hash1
  // when trx committed and wake up node1, node1 should be able to find REMOTE_CTRL_SIDE node1
  // use this hash to remote wake up
  uint64_t wait_row_hash_;
  // just for debug
  int64_t last_touched_thread_id_;
};


} // namespace rpc
} // namespace oceanbase

#endif // OCEANBASE_RPC_OB_LOCK_WAIT_NODE_


