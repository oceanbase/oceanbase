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

namespace oceanbase
{
namespace rpc
{

struct ObLockWaitNode: public common::SpHashNode
{
  ObLockWaitNode();
  ~ObLockWaitNode() {}
  void reset_need_wait() { need_wait_ = false; }
  void set(void* addr,
           int64_t hash,
           int64_t lock_seq,
           int64_t timeout,
           uint64_t tablet_id,
           const int64_t last_compact_cnt,
           const int64_t total_trans_node_cnt,
           const char* key,
           int64_t tx_id,
           int64_t holder_tx_id);
  void change_hash(const int64_t hash, const int64_t lock_seq);
  void update_run_ts(const int64_t run_ts) { run_ts_ = run_ts; }
  int64_t get_run_ts() const { return run_ts_; }
  int compare(ObLockWaitNode* that);
  bool is_timeout() { return common::ObTimeUtil::current_time() >= abs_timeout_; }
  void set_need_wait() { need_wait_ = true; }
  void set_standalone_task(const bool is_standalone_task) { is_standalone_task_ = is_standalone_task; }
  void set_lock_mode(uint8_t lock_mode) { lock_mode_ = lock_mode; }
  bool is_standalone_task() const { return is_standalone_task_; }
  bool need_wait() { return need_wait_; }
  void on_retry_lock(uint64_t hash) { hold_key_ = hash; }
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
  void set_block_sessid(const uint32_t block_sessid) { block_sessid_ = block_sessid; }

  TO_STRING_KV(KP(this),
               KP_(addr),
               K_(hash),
               K_(lock_ts),
               K_(lock_seq),
               K_(abs_timeout),
               K_(tablet_id),
               K_(try_lock_times),
               KCSTRING_(key),
               K_(sessid),
               K_(block_sessid),
               K_(run_ts),
               K_(lock_mode),
               K_(tx_id),
               K_(holder_tx_id),
               K_(need_wait),
               K_(is_standalone_task),
               K_(last_compact_cnt),
               K_(total_update_cnt));

  uint64_t hold_key_;
  ObLink retire_link_;
  bool need_wait_;
  void* addr_;
  int64_t recv_ts_;
  int64_t lock_ts_;
  uint64_t lock_seq_;
  int64_t abs_timeout_;
  uint64_t tablet_id_;
  int64_t try_lock_times_;
  uint32_t sessid_;
  uint32_t block_sessid_;
  int64_t tx_id_;
  int64_t holder_tx_id_;
  char key_[400];
  uint8_t lock_mode_;
  int64_t run_ts_;
  // There may be tasks that miss to wake up, called standalone tasks
  bool is_standalone_task_;
  int64_t last_compact_cnt_;
  int64_t total_update_cnt_;
};


} // namespace rpc
} // namespace oceanbase

#endif // OCEANBASE_RPC_OB_LOCK_WAIT_NODE_


