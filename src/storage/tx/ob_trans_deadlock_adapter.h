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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_DEADLOCK_ADAPTER_H_
#define OCEANBASE_TRANSACTION_OB_TRANS_DEADLOCK_ADAPTER_H_

#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "ob_trans_define_v4.h"
#include "storage/tx/deadlock_adapter/ob_session_id_pair.h"

namespace oceanbase
{
namespace storage
{
class ObRowConflictInfo;
}
namespace sql
{
class ObExecContext;
}
namespace transaction
{
using namespace share::detector;

class SessionGuard
{
public:
  SessionGuard();
  SessionGuard(const SessionGuard &) = delete;
  SessionGuard &operator=(const SessionGuard &) = delete;
  ~SessionGuard();
  void set_session(sql::ObSQLSessionInfo *session_ptr);
  bool is_valid() const;
  sql::ObSQLSessionInfo &get_session() const;
  sql::ObSQLSessionInfo *operator->() const;
  TO_STRING_KV(K_(sess_ptr));
private:
  void revert_session_();
private:
  sql::ObSQLSessionInfo *sess_ptr_;
};

// ObTransDeadlockDetectorAdapter is a helper class which provides utility
// functions for deadlock detector of transaction component
class ObTransDeadlockDetectorAdapter
{
  typedef share::detector::DetectCallBack DetectCallBack;
  typedef share::detector::BlockCallBack BlockCallBack;
  typedef share::detector::CollectCallBack CollectCallBack;
  typedef share::detector::FillVirtualInfoCallBack FillVirtualInfoCallBack;
 public:
  static const uint64_t INVALID_SESSION_ID = 0;
  enum class UnregisterPath {
    AUTONOMOUS_TRANS = 1,
    LOCK_WAIT_MGR_REPOST,
    LOCK_WAIT_MGR_WAIT_FAILED,
    LOCK_WAIT_MGR_TRANSFORM_WAITING_ROW_TO_TX,
    END_STMT,
    REPLACE_MEET_TOTAL_DIFFERENT_LIST,
    DO_END_TRANS,
    TX_ROLLBACK_IN_END_STMT,
    LOCK_WAIT_MGR_REGISTRE,
  };
  static const char* to_string(const UnregisterPath path)
  {
    switch (path) {
    case UnregisterPath::AUTONOMOUS_TRANS:
      return "AUTONOMOUS_TRANS";
    case UnregisterPath::LOCK_WAIT_MGR_REPOST:
      return "LOCK_WAIT_MGR_REPOST";
    case UnregisterPath::LOCK_WAIT_MGR_WAIT_FAILED:
      return "LOCK_WAIT_MGR_WAIT_FAILED";
    case UnregisterPath::LOCK_WAIT_MGR_TRANSFORM_WAITING_ROW_TO_TX:
      return "LOCK_WAIT_MGR_TRANSFORM_WAITING_ROW_TO_TX";
    case UnregisterPath::END_STMT:
      return "END_STMT";
    case UnregisterPath::REPLACE_MEET_TOTAL_DIFFERENT_LIST:
      return "REPLACE_MEET_TOTAL_DIFFERENT_LIST";
    case UnregisterPath::DO_END_TRANS:
      return "DO_END_TRANS";
    case UnregisterPath::TX_ROLLBACK_IN_END_STMT:
      return "TX_ROLLBACK_IN_END_STMT";
    default:
      return "UNKNOWN";
    }
  }
  /**********MAIN INTERFACE**********/
  // for local execution, call from lock wait mgr
  static int lock_wait_mgr_reconstruct_detector_waiting_for_row(CollectCallBack &on_collect_op,
                                                                const BlockCallBack &call_back,
                                                                const FillVirtualInfoCallBack &on_fill_op,
                                                                const ObTransID &self_trans_id,
                                                                const SessionIDPair sess_id_pair);
  static int lock_wait_mgr_reconstruct_detector_waiting_for_trans(CollectCallBack &on_collect_op,
                                                                  const FillVirtualInfoCallBack &on_fill_op,
                                                                  const ObTransID &conflict_trans_id,
                                                                  const ObTransID &self_trans_id,
                                                                  const SessionIDPair sess_id_pair);
  static int fetch_conflict_info_array(transaction::ObTxDesc &desc,
                                       ObArray<storage::ObRowConflictInfo> &conflict_info_array);
  // for remote execution, call from lock wait mgr
  // waiting for remote conflict tx
  static int lock_wait_mgr_reconstruct_detector_waiting_remote_self(CollectCallBack &on_collect_op,
                                                                    const ObTransID &self_trans_id,
                                                                    const ObAddr &remote_addr,
                                                                    const uint32_t sess_id);
  static int lock_wait_mgr_reconstruct_detector_waiting_remote_self_(const rpc::ObLockWaitNode& node);
  // for remote execution, call from lock wait mgr waiting without session info
  static int lock_wait_mgr_reconstruct_detector_waiting_for_row_without_session(CollectCallBack &on_collect_op,
                                                                                const FillVirtualInfoCallBack &on_fill_op,
                                                                                const BlockCallBack &call_back,
                                                                                const ObTransID &self_trans_id,
                                                                                const uint32_t sess_id,
                                                                                const int64_t trans_begin_ts,
                                                                                const int64_t query_timeout_us);
  static int lock_wait_mgr_reconstruct_detector_waiting_for_trans_without_session(CollectCallBack &on_collect_op,
                                                                                  const FillVirtualInfoCallBack &on_fill_op,
                                                                                  const ObTransID &conflict_trans_id,
                                                                                  const ObTransID &self_trans_id,
                                                                                  const uint32_t sess_id,
                                                                                  const int64_t trans_begin_ts,
                                                                                  const int64_t query_timeout_us);

  // for remote execution, call from sql trans control
  static int maintain_deadlock_info_when_end_stmt(sql::ObExecContext &exec_ctx, const bool is_rollback);
  // for autonomous trans
  static int autonomous_register_to_deadlock(const ObTransID last_trans_id,
                                             const ObTransID now_trans_id,
                                             const int64_t last_trans_active_ts,
                                             const int64_t query_timeout);
  // if trans node on row removed(for example:1, dump trans. 2, a trans write too many row.)
  // change the dependency relationship from row to trans
  static int change_detector_waiting_obj_from_row_to_trans(const ObTransID &self_trans_id,
                                                           const ObTransID &conflict_trans_id,
                                                           const ObAddr &scheduler_addr);
  // for all path
  static void unregister_from_deadlock_detector(const ObTransID &self_trans_id, const UnregisterPath path);
  /**********************************/
  static int get_conflict_trans_scheduler(const ObTransID &self_trans_id, ObAddr &scheduler_addr);
  static int kill_tx(const SessionIDPair sess_id_pair);
  static int get_session_info(const SessionIDPair sess_id_pair, SessionGuard &guard);
  static int get_session_info(const ObTxDesc &desc, SessionGuard &guard);
  static int get_trans_start_time_and_scheduler_from_session(const SessionIDPair sess_id_pair,
                                                             int64_t &trans_start_time,
                                                             ObAddr &scheduler_addr);
  static int get_trans_info_on_participant(const ObTransID trans_id,
                                           const share::ObLSID ls_id,
                                           ObAddr &scheduler_addr,
                                           SessionIDPair &sess_id_pair);
  static int register_or_replace_conflict_trans_ids(const ObTransID self_tx_id,
                                                    const SessionIDPair sess_id_pair,
                                                    const ObArray<storage::ObRowConflictInfo> &conflict_array,
                                                    const uint32_t count_down_allow_detect);
  static int lock_wait_mgr_reconstruct_conflict_trans_ids(const ObTransID self_tx_id,
                                                          const SessionIDPair sess_id_pair,
                                                          const ObArray<storage::ObRowConflictInfo> &conflict_array,
                                                          const uint32_t count_down_allow_detect);
  static int kill_stmt(const SessionIDPair sess_id_pair);
  static void copy_str_and_translate_apostrophe(const char *src_ptr,
                                                const int64_t src_len,
                                                char *dest_ptr,// C-style str, contain '\0'
                                                const int64_t dest_len);
private:
  static int register_to_deadlock_detector_(const SessionIDPair sess_id_pair,
                                            const ObTransID self_tx_id,
                                            const ObIArray<storage::ObRowConflictInfo> &conflict_array,
                                            SessionGuard &session_guard,
                                            const uint32_t count_down_allow_detect,
                                            const uint64_t start_delay,
                                            const ObDeadlockKeyType &detector_key_type);
  static int replace_conflict_trans_ids_(const SessionIDPair sess_id_pair,
                                         const ObTransID self_tx_id,
                                         const ObIArray<storage::ObRowConflictInfo> &conflict_array,
                                         SessionGuard &session_guard);
  static int create_detector_node_and_set_parent_if_needed_(CollectCallBack &on_collect_op,
                                                            const FillVirtualInfoCallBack &on_fill_op,
                                                            const ObTransID &self_trans_id,
                                                            const SessionIDPair sess_id_pair);
  template<typename KeyType>
  static int create_detector_node_without_session_and_block_row_(CollectCallBack &on_collect_op,
                                                             const FillVirtualInfoCallBack &on_fill_op,
                                                             const KeyType &self_trans_key,
                                                             const uint32_t sess_id,
                                                             const int64_t trans_begin_ts,
                                                             const int64_t query_timeout_us,
                                                             const BlockCallBack &func);
  template<typename KeyType>
  static int create_detector_node_without_session_and_block_trans_(CollectCallBack &on_collect_op,
                                                             const FillVirtualInfoCallBack &on_fill_op,
                                                             const KeyType &self_trans_key,
                                                             const KeyType &conflict_trans_key,
                                                             const uint32_t sess_id,
                                                             const int64_t trans_begin_ts,
                                                             const int64_t query_timeout_us,
                                                             const ObAddr &conflict_scheduler);
  template<typename KeyType>
  static int create_detector_node_without_session_(CollectCallBack &on_collect_op,
                                                  const FillVirtualInfoCallBack &on_fill_op,
                                                  const KeyType &self_trans_key,
                                                  const uint32_t sess_id,
                                                  const int64_t trans_begin_ts,
                                                  const int64_t query_timeout_us);
  static int get_session_related_info_(const SessionIDPair sess_id_pair, int64_t &query_timeout);
  static void try_unregister_deadlock_detector_(sql::ObSQLSessionInfo &session,
                                                const ObTransID &trans_id,
                                                UnregisterPath path);
  static int gen_dependency_resource_array_(const ObIArray<storage::ObRowConflictInfo> &blocked_trans_info_array,
                                            ObIArray<share::detector::ObDependencyHolder> &dependency_resources,
                                            const ObDeadlockKeyType &detector_key_type);
  static void check_if_needed_register_detector_when_end_stmt_(const bool is_rollback,
                                                               const int ret_code,
                                                               sql::ObSQLSessionInfo &session,
                                                               ObTxDesc &desc,
                                                               ObArray<storage::ObRowConflictInfo> &conflict_info_array);
};

template<typename KeyType>
class ObTransDummyOnDetectOperation
{
 public:
  ObTransDummyOnDetectOperation(const uint32_t sess_id, const KeyType &trans_key) :
    sess_id_(sess_id), trans_key_(trans_key) {}
  ~ObTransDummyOnDetectOperation() {}
  int operator()(const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
                 const int64_t self_idx)
  {
    // do nothing
    int ret = OB_SUCCESS;
    return ret;
  }
   TO_STRING_KV(KP(this), K_(sess_id), K_(trans_key));
 private:
   uint32_t sess_id_;
   KeyType trans_key_;
};

template<typename KeyType>
int ObTransDeadlockDetectorAdapter::create_detector_node_without_session_(CollectCallBack &on_collect_op,
                                                                         const FillVirtualInfoCallBack &on_fill_op,
                                                                         const KeyType &self_trans_key,
                                                                         const uint32_t sess_id,
                                                                         const int64_t trans_begin_ts,
                                                                         const int64_t query_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_key), K(sess_id), K(query_timeout_us), K(trans_begin_ts)
  int ret = OB_SUCCESS;
  // priority highest
  ObDetectorPriority priority(PRIORITY_RANGE::EXTREMELY_HIGH, 0);
  ObTransDummyOnDetectOperation<KeyType> on_detect_op(sess_id, self_trans_key);
  if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "deadlock detector mgr shuould not be null", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(self_trans_key,
                                                               on_detect_op,
                                                               on_collect_op,
                                                               on_fill_op,
                                                               trans_begin_ts,
                                                               priority))) {
    DETECT_LOG(WARN, "fail to register key", PRINT_WRAPPER);
  } else {
    MTL(ObDeadLockDetectorMgr*)->set_timeout(self_trans_key, query_timeout_us);
  }
  DETECT_LOG(TRACE, "create fake node detector node without session", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}

template<typename KeyType>
int ObTransDeadlockDetectorAdapter::create_detector_node_without_session_and_block_row_(CollectCallBack &on_collect_op,
                                                                         const FillVirtualInfoCallBack &on_fill_op,
                                                                         const KeyType &self_trans_key,
                                                                         const uint32_t sess_id,
                                                                         const int64_t trans_begin_ts,
                                                                         const int64_t query_timeout_us,
                                                                         const BlockCallBack &func)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_key), K(sess_id), K(query_timeout_us), K(trans_begin_ts)
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_detector_node_without_session_(on_collect_op,
                                                    on_fill_op,
                                                    self_trans_key,
                                                    sess_id,
                                                    trans_begin_ts,
                                                    query_timeout_us))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_key, func))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_key))) {
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
    DETECT_LOG(WARN, "fail to block on call back function", PRINT_WRAPPER);
  } else {
    DETECT_LOG(INFO, "local execution register to deadlock detector waiting for row success", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template<typename KeyType>
int ObTransDeadlockDetectorAdapter::create_detector_node_without_session_and_block_trans_(CollectCallBack &on_collect_op,
                                                                                          const FillVirtualInfoCallBack &on_fill_op,
                                                                                          const KeyType &self_trans_key,
                                                                                          const KeyType &conflict_trans_key,
                                                                                          const uint32_t sess_id,
                                                                                          const int64_t trans_begin_ts,
                                                                                          const int64_t query_timeout_us,
                                                                                          const ObAddr &conflict_scheduler)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_key), K(sess_id), K(query_timeout_us), K(trans_begin_ts), K(conflict_scheduler)
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_detector_node_without_session_(on_collect_op, on_fill_op, self_trans_key, sess_id, trans_begin_ts, query_timeout_us))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_key, conflict_scheduler, conflict_trans_key))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_key))) {
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
    DETECT_LOG(WARN, "fail to block on conflict trans", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "local execution register to deadlock detector waiting for trans success", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

} // namespace transaction
} // namespace oceanbase

#endif
