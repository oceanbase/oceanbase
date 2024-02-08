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

#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "ob_trans_define_v4.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
}
namespace transaction
{

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

class ObTransOnDetectOperation
{
public:
  ObTransOnDetectOperation(const uint32_t sess_id, const ObTransID &trans_id) :
    sess_id_(sess_id), trans_id_(trans_id) {}
  ~ObTransOnDetectOperation() {}
  int operator()(const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
                 const int64_t self_idx);
  TO_STRING_KV(KP(this), K_(sess_id), K_(trans_id));
private:
  uint32_t sess_id_;
  ObTransID trans_id_;
};

// ObTransDeadlockDetectorAdapter is a helper class which provides utility
// functions for deadlock detector of transaction component
class ObTransDeadlockDetectorAdapter
{
  typedef share::detector::DetectCallBack DetectCallBack;
  typedef share::detector::BlockCallBack BlockCallBack;
  typedef share::detector::CollectCallBack CollectCallBack;
 public:
  enum class UnregisterPath {
    AUTONOMOUS_TRANS = 1,
    LOCK_WAIT_MGR_REPOST,
    LOCK_WAIT_MGR_WAIT_FAILED,
    LOCK_WAIT_MGR_TRANSFORM_WAITING_ROW_TO_TX,
    END_STMT_DONE,
    END_STMT_OTHER_ERR,
    END_STMT_NO_CONFLICT,
    END_STMT_TIMEOUT,
    REPLACE_MEET_TOTAL_DIFFERENT_LIST,
    DO_END_TRANS,
    TX_ROLLBACK_IN_END_STMT,
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
    case UnregisterPath::END_STMT_DONE:
      return "END_STMT_DONE";
    case UnregisterPath::END_STMT_OTHER_ERR:
      return "END_STMT_OTHER_ERROR";
    case UnregisterPath::END_STMT_NO_CONFLICT:
      return "END_STMT_NO_CONFLICT";
    case UnregisterPath::END_STMT_TIMEOUT:
      return "END_STMT_TIMEOUT";
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
                                                                const ObTransID &self_trans_id,
                                                                const uint32_t sess_id);
  static int lock_wait_mgr_reconstruct_detector_waiting_for_trans(CollectCallBack &on_collect_op,
                                                                  const ObTransID &conflict_trans_id,
                                                                  const ObTransID &self_trans_id,
                                                                  const uint32_t sess_id);
  // for remote execution, call from sql trans control
  static int maintain_deadlock_info_when_end_stmt(sql::ObExecContext &exec_ctx, const bool is_rollback);
  // for autonomous trans
  static int autonomous_register_to_deadlock(const ObTransID last_trans_id,
                                             const ObTransID now_trans_id,
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
  static int kill_tx(const uint32_t sess_id);
  static int get_session_info(const uint32_t session_id, SessionGuard &guard);
  static int get_session_info(const ObTxDesc &desc, SessionGuard &guard);
  static int get_trans_start_time_and_scheduler_from_session(const uint32_t session_id,
                                                             int64_t &trans_start_time,
                                                             ObAddr &scheduler_addr);
  static int get_trans_scheduler_info_on_participant(const ObTransID trans_id,
                                                     const share::ObLSID ls_id, 
                                                     ObAddr &scheduler_addr);
  static int register_or_replace_conflict_trans_ids(const ObTransID self_tx_id,
                                                    const uint32_t self_session_id,
                                                    const ObArray<ObTransIDAndAddr> &conflict_tx_ids);
  static int kill_stmt(const uint32_t sess_id);
  static void copy_str_and_translate_apostrophe(const char *src_ptr,
                                                const int64_t src_len,
                                                char *dest_ptr,// C-style str, contain '\0'
                                                const int64_t dest_len);
private:
  static int register_to_deadlock_detector_(const ObTransID self_tx_id,
                                            const uint32_t self_session_id,
                                            const ObIArray<ObTransIDAndAddr> &conflict_tx_ids,
                                            SessionGuard &session_guard);
  static int replace_conflict_trans_ids_(const ObTransID self_tx_id,
                                         const ObIArray<ObTransIDAndAddr> &conflict_tx_ids,
                                         SessionGuard &session_guard);
  static int create_detector_node_and_set_parent_if_needed_(CollectCallBack &on_collect_op,
                                                            const ObTransID &self_trans_id,
                                                            const uint32_t sess_id);
  static int get_session_related_info_(const uint32_t sess_id, int64_t &query_timeout);
  static int gen_dependency_resource_array_(const ObIArray<ObTransIDAndAddr> &blocked_trans_ids_and_addrs,
                                            ObIArray<share::detector::ObDependencyResource> &dependency_resources);
  static void try_unregister_deadlock_detector_(sql::ObSQLSessionInfo &session, const ObTransID &trans_id, UnregisterPath path);
};

} // namespace transaction
} // namespace oceanbase

#endif
