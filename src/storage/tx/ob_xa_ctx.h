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

#ifndef OCEANBASE_TRANSACTION_OB_XA_CTX_
#define OCEANBASE_TRANSACTION_OB_XA_CTX_

#include "ob_trans_define.h"
#include "ob_xa_define.h"
#include "ob_trans_result.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_latch.h"
#include "lib/hash/ob_link_hashmap.h"
#include "storage/tx/ob_dblink_client.h"

namespace oceanbase
{

namespace obrpc
{
class ObXAStartRPCRequest;
class ObXAStartRPCResponse;
class ObXAEndRPCRequest;
class ObXAStartStmtRPCRequest;
class ObXAStartStmtRPCResponse;
class ObXAEndStmtRPCRequest;
}

namespace transaction
{

class ObXAService;
class ObXACtxMgr;
class ObXARpc;

typedef common::LinkHashValue<ObTransID> XACtxHashValue;

struct SyncXACb : public ObITxCallback
{
  void callback(int ret) { cond_.notify(ret); }
  int wait(const int64_t time_us, int &ret) {
    return cond_.wait(time_us, ret);
  }
  ObTransCond cond_;
};

class ObXACtx : public XACtxHashValue
{
public:
  ObXACtx();
  virtual ~ObXACtx() { destroy(); }
  int init(const ObXATransID &xid,
           const ObTransID &trans_id,
           const uint64_t tenant_id,
           const common::ObAddr &scheduler_addr,
           const bool is_tightly_coupled,
           ObXAService *xa_service,
           ObXACtxMgr *xa_ctx_mgr,
           ObXARpc *xa_rpc,
           ObITransTimer *timer);
  void reset();
  void destroy();

  int wait_xa_start_complete();
  int handle_timeout(const int64_t delay);

  //for timer use
  const ObTransID& get_trans_id() const { return trans_id_; }
  ObXACtxMgr* get_xa_ctx_mgr() { return xa_ctx_mgr_; }
  ObITransTimer* get_timer() { return timer_; }

  bool is_inited() const { return is_inited_; }
  bool is_tightly_coupled() const { return is_tightly_coupled_; }
  int stmt_lock_with_guard(const ObXATransID &xid);
  int stmt_unlock_with_guard(const ObXATransID &xid);
  bool is_terminated() { return is_terminated_; }
  int xa_scheduler_hb_req();
  common::ObAddr get_original_sche_addr() { return original_sche_addr_; }
  int kill();

  int process_xa_start(const obrpc::ObXAStartRPCRequest &req);
  int process_xa_start_response(const obrpc::ObXAStartRPCResponse &resp);
  int process_xa_end(const obrpc::ObXAEndRPCRequest &req);
  int process_start_stmt(const obrpc::ObXAStartStmtRPCRequest &req);
  int process_start_stmt_response(const obrpc::ObXAStartStmtRPCResponse &res);
  int process_end_stmt(const obrpc::ObXAEndStmtRPCRequest &req);
  int xa_start(const ObXATransID &xid,
               const int64_t flags,
               const int64_t timeout_seconds,
               ObTxDesc *tx_desc);
  int xa_start_second(const ObXATransID &xid,
                      const int64_t flags,
                      const int64_t timeout_seconds,
                      ObTxDesc *&tx_desc);
  int xa_start_remote_first(const ObXATransID &xid,
                            const int64_t flags,
                            const int64_t timeout_seconds,
                            ObTxDesc *&tx_desc);
  int xa_end(const ObXATransID &xid, const int64_t flags, ObTxDesc *&tx_desc);
  int start_stmt(const ObXATransID &xid, const uint32_t session_id);
  int wait_start_stmt(const uint32_t session_id);
  int end_stmt(const ObXATransID &xid);
  const ObXATransID &get_executing_xid() const { return executing_xid_; }
  int one_phase_end_trans(const ObXATransID &xid,
                          const bool is_rollback,
                          const int64_t timeout_us,
                          const int64_t request_id);
  // this is ONLY used for session terminate
  int xa_rollback_session_terminate(bool &is_first_terminate);
  // process terminate request from temporary scheduler
  int process_terminate(const ObXATransID &xid);
  void try_exit(const bool need_decrease_ref = false);
  // this is ONLY used for session terminate
  int clear_branch_for_xa_terminate(const ObXATransID &xid);
  int try_heartbeat();
  int response_for_heartbeat(const ObXATransID &xid, const ObAddr &original_addr);
  int update_xa_branch_for_heartbeat(const ObXATransID &xid);
  int check_for_execution(const ObXATransID &xid, const bool is_new_branch)
  {
    ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
    return check_for_execution_(xid, is_new_branch);
  }
  int xa_prepare(const ObXATransID &xid, const int64_t timeout_us);
  int two_phase_end_trans(const ObXATransID &xid,
                          const share::ObLSID &coord,
                          const bool is_rollback,
                          const int64_t timeout_us,
                          const int64_t request_id);
  int wait_two_phase_end_trans(const ObXATransID &xid,
                               const bool is_rollback,
                               const int64_t timeout_us);
  int wait_xa_prepare(const ObXATransID &xid,
                      const int64_t timeout_us);
  int wait_one_phase_end_trans(const bool is_rollback,
                              const int64_t timeout_us);
  int64_t get_xa_ref_count()
  {
    ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
    return xa_ref_count_;
  }
public:
  // for 4.0 dblink
  int xa_start_for_dblink(const ObXATransID &xid,
                          const int64_t flags,
                          const int64_t timeout_seconds,
                          const bool need_promote,
                          ObTxDesc *tx_desc);
  int get_dblink_client(const common::sqlclient::DblinkDriverProto dblink_type,
                        common::sqlclient::ObISQLConnection *dblink_conn,
                        ObDBLinkTransStatistics *dblink_statistics,
                        ObDBLinkClient *&client);
  int remove_dblink_client(ObDBLinkClient *client);
  ObDBLinkClientArray &get_dblink_client_array() { return dblink_client_array_; }
  int recover_tx_for_dblink_callback(ObTxDesc *&tx_desc);
  int revert_tx_for_dblink_callback(ObTxDesc *&tx_desc);
  bool has_tx_level_temp_table() { return has_tx_level_temp_table_; }
  int start_check_stmt_lock(const ObXATransID &xid);
  int stop_check_stmt_lock(const ObXATransID &xid);
  OB_INLINE bool is_executing() const { return is_executing_; }
  
  TO_STRING_KV(K_(is_inited), K_(xid), K_(original_sche_addr), K_(is_exiting),
               K_(trans_id), K_(is_executing), K_(is_xa_end_trans), K_(tenant_id), 
               K_(is_xa_readonly), K_(xa_trans_state), K_(is_xa_one_phase),
               K_(xa_branch_count), K_(xa_ref_count), K_(lock_grant),
               K_(is_tightly_coupled), K_(lock_xid), K_(xa_stmt_info),
               K_(is_terminated), K_(executing_xid), "uref", get_uref(),
               K_(has_tx_level_temp_table), K_(local_lock_level), K_(need_stmt_lock));
private:
  int register_timeout_task_(const int64_t interval_us);
  int unregister_timeout_task_();
  int register_xa_timeout_task_();
  void notify_xa_start_complete_(int ret_code);
  int wait_xa_sync_status_(const int64_t expired_time);
  int wait_sync_stmt_info_(const int64_t expired_time);
  int update_xa_branch_info_(const ObXATransID &xid,
                             const int64_t to_state,
                             const common::ObAddr &addr,
                             const int64_t timeout_seconds,
                             const int64_t end_flag);
  int check_join_(const ObXATransID &xid) const;
  int get_branch_info_(const ObXATransID &xid, ObXABranchInfo &info) const;
  int stmt_lock_(const ObXATransID &xid);
  int stmt_unlock_(const ObXATransID &xid);
  int update_xa_branch_hb_info_(const ObXATransID &xid);
  int is_one_phase_end_trans_allowed_(const ObXATransID &xid, const bool is_rollback);
  int init_xa_branch_info_();
  int check_terminated_() const;
  void set_terminated_();
  void print_branch_info_() const;
  // for 4.0
  int process_xa_start_tightly_(const obrpc::ObXAStartRPCRequest &req);
  int process_xa_start_loosely_(const obrpc::ObXAStartRPCRequest &req);
  int xa_start_(const ObXATransID &xid,
                const int64_t flags,
                const int64_t timeout_seconds,
                ObTxDesc *tx_desc);
  int xa_start_local_(const ObXATransID &xid,
                      const int64_t flags,
                      const int64_t timeout_seconds,
                      const bool is_new_branch,
                      ObTxDesc *&tx_desc);
  int xa_start_remote_first_(const ObXATransID &xid,
                             const int64_t flags,
                             const int64_t timeout_seconds,
                             const bool is_new_branch,
                             ObTxDesc *&tx_desc);
  int xa_start_remote_second_(const ObXATransID &xid,
                              const int64_t flags,
                              const int64_t timeout_seconds,
                              const bool is_new_branch,
                              ObTxDesc *&tx_desc);
  int xa_end_loose_local_(const ObXATransID &xid,
                          const int64_t flags,
                          ObTxDesc *&tx_desc);
  int xa_end_loose_remote_(const ObXATransID &xid,
                           const int64_t flags,
                           ObTxDesc *&tx_desc);
  int xa_end_tight_local_(const ObXATransID &xid,
                          const int64_t flags,
                          ObTxDesc *&tx_desc);
  int xa_end_tight_remote_(const ObXATransID &xid,
                           const int64_t flags,
                           ObTxDesc *&tx_desc);
  int one_phase_end_trans_(const bool is_rollback,
                           const int64_t timeout_us,
                           const int64_t request_id);
  int save_tx_desc_(ObTxDesc *tx_desc);
  int start_stmt_local_(const ObXATransID &xid);
  int start_stmt_remote_(const ObXATransID &xid);
  int end_stmt_local_(const ObXATransID &xid);
  int end_stmt_remote_(const ObXATransID &xid);
  bool check_response_(const int64_t response_id,
                       const bool is_stmt_response) const;
  int set_exiting_();
  void try_exit_();
  int check_for_execution_(const ObXATransID &xid, const bool is_new_branch);
  int xa_rollback_terminate_(const int cause);
  
  int xa_prepare_(const ObXATransID &xid, const int64_t timeout_us, bool &need_exit);
  int drive_prepare_(const ObXATransID &xid,
                     const int64_t timeout_us);
  int check_trans_state_(const bool is_rollback,
                         const int64_t request_id,
                         const bool is_xa_one_phase);
  int update_xa_stmt_info_(const ObXATransID &xid);
  int create_xa_savepoint_if_need_(const ObXATransID &xid,
                                   const uint32_t session_id);
  int remove_xa_stmt_info_(const ObXATransID &xid);
private:
  // for 4.0 dblink
  int get_dblink_client_(const common::sqlclient::DblinkDriverProto dblink_type,
                         common::sqlclient::ObISQLConnection *dblink_conn,
                         ObDBLinkTransStatistics *dblink_statistics,
                         ObDBLinkClient *&dblink_client);
private:
  static const int MIN_TX_REF_COUNT = 3;
private:
  bool is_inited_;
  ObXACtxMgr *xa_ctx_mgr_;
  common::ObLatch lock_;
  //Serveral branches may share one xa ctx, xid_ equals to the one which first creates it
  //in prepare phase, xid_ equals to the one which last do prepare
  ObXATransID xid_;
  ObXAService *xa_service_;
  common::ObAddr original_sche_addr_;
  bool is_exiting_;
  ObTransID trans_id_;
  //whether current ctx is executing stmt on behalf of some branch,
  //used to replace local lock only valid on tmp scheduler
  bool is_executing_;
  ObITransTimer *timer_;
  ObXATimeoutTask timeout_task_;
  uint64_t tenant_id_;
  //=========================================================================================
  // receive xa commit or xa rollback
  bool is_xa_end_trans_;
  // read only xa transaction
  bool is_xa_readonly_;
  // XA transaction state
  int32_t xa_trans_state_;
  // XA one phase commit flag
  bool is_xa_one_phase_;
  ObTxDesc *tx_desc_;
  ObXARpc *xa_rpc_;
  common::ObLatch xa_stmt_lock_;
  ObTransCond xa_start_cond_;
  ObTransCond xa_sync_status_cond_;
  int64_t xa_branch_count_;
  int64_t xa_ref_count_;
  //for debug, to be removed
  int64_t lock_grant_;
  // couple flag of XA trans, tightly bu default
  bool is_tightly_coupled_;
  ObXATransID lock_xid_;
  ObXABranchInfoArray *xa_branch_info_;
  ObXAStmtInfoArray xa_stmt_info_;
  bool is_terminated_;
  common::ObTraceEventRecorder tlog_;
  bool need_print_trace_log_;
  // for checking response
  int64_t request_id_;
  // record xa trans branch which is executing normal stmt
  // can be removed when xid is from session
  ObXATransID executing_xid_;
  ObTransCond start_stmt_cond_;
  ObTransCond sync_stmt_info_cond_;
  SyncXACb end_trans_cb_;
  // if dblink trans, record dblink client
  ObDBLinkClientArray dblink_client_array_;
  bool has_tx_level_temp_table_;
  // local_lock_level is used with is_executing_, executing_xid_
  // the rules are as follows:
  // 1. if there are no executing statements,
  //    is_executing is false
  //    executing_xid is empty
  //    local_lock_level is -1
  // 2. if temp scheduler acquires the global lock,
  //    |     | temp | original |
  //    | --- | --- | --- |
  //    | is_executing | true | true |
  //    | executing_xid | xid | xid |
  //    | local_lock_level | 0 | -1 |
  // 3. when try to acquire the global lock and find that the is_executing is true
  //    3.1 if executing_xid is not equal to the xid in session, return stmt_retry
  //    3.2 if executing_xid is equal to the xid in session, increment the local_lock_level
  //        and return success
  // 4. when release the global lock and find that is_executing is true
  //    4.1 if local_lock_level > 0, decrease the local_lock_level
  //    4.2 if local_lock_level == 0, execute the normal global lock release processing
  int64_t local_lock_level_;
  bool need_stmt_lock_;
};

}//transaction

}//oceanbaase

#endif
