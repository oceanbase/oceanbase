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
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/time/ob_time_utility.h"
#include <cstring>

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace rpc
{

struct RequestLockWaitStat {
  enum RequestStat : uint8_t {
    DEFAULT = 0,
    EXECUTE = 1,
    START = 2,
    CONFLICTED = 3,
    END = 4,
    INQUEUE = 5,
    OUTQUEUE = 6,
    SIZE
  };
  const char *stat_to_string(RequestStat stat) const {
    const char *ret = nullptr;
    switch (state_) {
    case RequestStat::DEFAULT:
      ret = "DEFAULT";
      break;
    case RequestStat::EXECUTE:
      ret = "EXECUTE";
      break;
    case RequestStat::START:
      ret = "START";
      break;
    case RequestStat::CONFLICTED:
      ret = "CONFLICTED";
      break;
    case RequestStat::END:
      ret = "END";
      break;
    case RequestStat::INQUEUE:
      ret = "INQUEUE";
      break;
    case RequestStat::OUTQUEUE:
      ret = "OUTQUEUE";
      break;
    default:
      ret = "UNKNOWN";
      break;
    }
    return ret;
  }
  static constexpr bool ConvertMap[RequestStat::SIZE][RequestStat::SIZE] = {
                  /*DEFAULT*/ /*EXECUTE*/ /*START*/ /*CONFLICTED*/  /*END*/  /*INQUEUE*/ /*OUTQUEUE*/
  /*DEFAULT*/     {       1,          1,        0,             0,       0,           0,           0},// from DEFAULT, only allowed to EXECUTE
  /*EXECUTE*/     {       0,          1,        1,             1,       0,           0,           0},// from EXECUTE, only allowed to START, there maybe switch to CONFLICTED without START in test for some unknown reason
  /*START*/       {       0,          1,        1,             1,       1,           0,           0},// from START, only allowed to CONFLICTED/END, there maybe switch to START in test for some unknown reason
  /*CONFLICTED*/  {       0,          0,        0,             1,       1,           1,           0},// from CONFLICTED, only allowed to END, there maybe switch to INQUEUE in test for some unknown reason
  /*END*/         {       0,          1,        1,             1,       1,           1,           0},// from END, allowed to EXECUTE/START/INQUEUE, there maybe switch to CONFLICTED without START in test for some unknown reason
  /*INQUEUE*/     {       0,          0,        0,             0,       0,           1,           1},// from INQUEUE, only allowed to OUTQUEUE
  /*OUTQUEUE*/    {       0,          1,        0,             0,       0,           0,           1},// from UTQUEUE, only allowed to EXECUTE
  };
  RequestLockWaitStat() : state_(RequestStat::DEFAULT) {}
  void advance_to(RequestStat new_stat) {
    if (OB_UNLIKELY(!ConvertMap[state_][new_stat])) {
#ifdef OB_BUILD_PACKAGE // serious env, just WARN
      DETECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "stat advance unexpected", K(state_), K(new_stat), KP(this));
#else // test env, print ERROR and abort if necessary
      DETECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "stat advance unexpected", K(state_), K(new_stat), KP(this), K(lbt()));
      if (RequestStat::INQUEUE == state_) {
        ob_abort();// this is not expectecd, something is beyond control and break important assumption.
      }
#endif
    }
    state_ = new_stat;
  }
  int64_t to_string(char *buffer, const int64_t buffer_len) const {
    int64_t pos = 0;
    databuff_printf(buffer, buffer_len, pos, "%ld(%s)", (int64_t)state_, stat_to_string(state_));
    return pos;
  }
  RequestStat state_;
};

struct ObLockWaitNode: public common::SpHashNode
{
  ObLockWaitNode();
  ~ObLockWaitNode() {}
  void reset_need_wait() { need_wait_ = false; }
  void set(void *addr,
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
           const share::ObLSID &ls_id);
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
  void advance_stat(RequestLockWaitStat::RequestStat new_stat) {
    request_stat_.advance_to(new_stat);
  }

  TO_STRING_KV(KP(this),
               K_(request_stat),
               KP_(addr),
               K_(hash),
               K_(lock_ts),
               K_(lock_seq),
               K_(abs_timeout),
               K_(tablet_id),
               K_(try_lock_times),
               KCSTRING_(key),
               K_(sessid),
               K_(holder_sessid),
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
  RequestLockWaitStat request_stat_;
  void* addr_;
  int64_t recv_ts_;
  int64_t lock_ts_;
  uint64_t lock_seq_;
  int64_t abs_timeout_;
  uint64_t tablet_id_;
  int64_t try_lock_times_;
  uint32_t sessid_;
  uint32_t holder_sessid_;
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


