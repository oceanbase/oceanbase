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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SCHE_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_SCHE_CTX_

#include "ob_trans_ctx.h"
#include "ob_ts_mgr.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
class ObMaskSet;
}  // namespace common

namespace transaction {
class ObITransCtxMgr;
class ObTransMsg;
}  // namespace transaction

namespace transaction {
class ObXARpc;
typedef common::ObList<common::ObPartitionKey, ObArenaAllocator> ObPartitionList;

struct ObXABranchInfo {
  ObXABranchInfo()
  {}
  virtual ~ObXABranchInfo()
  {}
  int init(const ObXATransID& xid, const int64_t state, const int64_t timeout_seconds, const int64_t abs_expired_time,
      const common::ObAddr& addr, const int64_t unrespond_msg_cnt, const int64_t last_hb_ts);
  TO_STRING_KV(K_(xid), K_(state), K_(timeout_seconds), K_(addr), K_(unrespond_msg_cnt), K_(last_hb_ts));
  ObXATransID xid_;
  int64_t state_;
  int64_t timeout_seconds_;
  int64_t abs_expired_time_;
  common::ObAddr addr_;
  int64_t unrespond_msg_cnt_;
  int64_t last_hb_ts_;
};

typedef common::ObSEArray<ObXABranchInfo, 4> ObXABranchInfoArray;

// one transaction include scheduler context, coordinator context and participant context
// scheduler transaction context
class ObScheTransCtx : public ObDistTransCtx, public ObTsCbTask {
public:
  ObScheTransCtx()
      : ObDistTransCtx("scheduler", ObTransCtxType::SCHEDULER),
        last_stmt_participants_(ObModIds::OB_TRANS_PARTITION_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        cur_stmt_unreachable_partitions_(ObModIds::OB_TRANS_PARTITION_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        stmt_rollback_participants_(ObModIds::OB_TRANS_PARTITION_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        trans_location_cache_(ObModIds::OB_TRANS_LOCATION_CACHE, OB_MALLOC_NORMAL_BLOCK_SIZE),
        xa_stmt_lock_(),
        xa_branch_info_(NULL)
  {
    reset();
  }
  virtual ~ObScheTransCtx()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service, const bool can_elr);
  virtual void destroy();
  void reset();

public:
  int handle_message(const ObTransMsg& msg);
  bool is_inited() const;
  int handle_timeout(const int64_t delay);
  int kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array);
  int leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array)
  {
    UNUSED(first_check);
    UNUSED(need_release);
    UNUSED(cb_array);
    return common::OB_ERR_UNEXPECTED;
  }
  int leader_takeover(const int64_t checkpoint)
  {
    UNUSED(checkpoint);
    return common::OB_ERR_UNEXPECTED;
  }
  int leader_active(const storage::LeaderActiveArg& arg)
  {
    UNUSED(arg);
    return common::OB_ERR_UNEXPECTED;
  }
  bool can_be_freezed() const
  {
    return true;
  }
  int inactive(ObEndTransCallbackArray& cb_array);

public:
  static bool need_retry_end_trans(const int retcode)
  {
    return OB_LOCATION_LEADER_NOT_EXIST == retcode || OB_LOCATION_NOT_EXIST == retcode || OB_NOT_MASTER == retcode;
  }

public:
  int start_trans(const ObTransDesc& trans_desc);
  int end_trans(const ObTransDesc& trans_desc, const bool is_rollback, sql::ObIEndTransCallback* cb,
      const int64_t trans_expired_time, const int64_t stmt_expired_time, const MonotonicTs commit_time);
  int kill_trans(const ObTransDesc& trans_desc);
  int mark_stmt_started();
  int start_stmt(
      const ObPartitionLeaderArray& partition_leader_arr, const ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc);
  int end_stmt(const bool is_rollback, ObTransDesc& trans_desc);
  int commit(const bool is_rollback, sql::ObIEndTransCallback* cb, bool is_readonly, const MonotonicTs commit_time,
      const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info,
      const common::ObString& app_trace_info, bool& need_convert_to_dist_trans)
  {
    UNUSED(is_rollback);
    UNUSED(cb);
    UNUSED(is_readonly);
    UNUSED(commit_time);
    UNUSED(stmt_expired_time);
    UNUSED(stmt_rollback_info);
    UNUSED(app_trace_info);
    UNUSED(need_convert_to_dist_trans);
    return OB_NOT_SUPPORTED;
  }
  int wait_start_stmt(ObTransDesc& trans_desc, const int64_t expired_time);
  int wait_end_stmt(const int64_t expired_time);
  int handle_err_response(const int64_t msg_type, const ObPartitionKey& partition, const ObAddr& sender_addr,
      const int status, const int64_t sql_no, const int64_t request_id);
  int kill_query_session(const int status);
  int handle_trans_msg_callback(const ObPartitionKey& partition, const int64_t msg_type, const int status,
      const ObAddr& sender_addr, const int64_t sql_no, const int64_t request_id);
  int merge_participants_pla();
  int merge_participants(const ObPartitionArray& participants);
  int merge_gc_participants(const ObPartitionArray& participants);
  int merge_gc_participants_(const ObPartitionArray& participants);
  int merge_participants_pla(const ObPartitionLeaderArray& participants_pla);
  void clear_stmt_participants();
  void clear_stmt_participants_pla();
  int set_sql_no(const int64_t sql_no);
  int get_cur_stmt_unreachable_partitions(ObPartitionArray& partitions) const;
  int merge_partition_leader_info(const ObPartitionLeaderArray& partition_leader_arr);
  int set_trans_app_trace_id_str(const ObString& app_trace_id_str);
  // for gts interface
  uint64_t hash() const
  {
    return trans_id_.hash();
  }
  int get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts);
  virtual int gts_elapse_callback(const MonotonicTs srr, const int64_t gts)
  {
    UNUSED(srr);
    UNUSED(gts);
    return OB_SUCCESS;
  }
  MonotonicTs get_stc() const
  {
    return stc_;
  }
  int64_t get_request_ts() const
  {
    return gts_request_ts_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int handle_trans_response(const int64_t msg_type, const int status, const ObPartitionKey& sender,
      const ObAddr& sender_addr, const int64_t need_wait_interval_us);
  int submit_stmt_rollback_request(const ObPartitionKey& receiver, const int64_t msg_type, const int64_t request_id,
      const int64_t sql_no, const int64_t msg_start_ts);
  int xa_end_trans(const bool is_rollback);
  int wait_xa_end_trans();
  void set_xa_readonly(const bool is_xa_readonly)
  {
    is_xa_readonly_ = is_xa_readonly;
  }
  int set_xa_trans_state(const int32_t state, const bool need_check = true);
  int get_xa_trans_state(int32_t& state) const;
  int xa_rollback_session_terminate();
  int xa_one_phase_end_trans(const bool is_rollback);
  int pre_xa_prepare(const ObXATransID& xid);
  int wait_savepoint_rollback(const int64_t expired_time);
  int start_savepoint_rollback(
      const ObTransDesc& trans_desc, const int64_t sql_no, const ObPartitionArray& rollback_partitions);
  int wait_xa_sync_status(const int64_t expired_time);
  int xa_sync_status(const common::ObAddr& sender, const bool is_stmt_pull);
  int xa_sync_status_response(const ObTransDesc& trans_desc, const bool is_stmt_pull);
  int xa_merge_status(const ObTransDesc& trans_desc, const bool is_stmt_push);
  ObTransDesc* get_trans_desc()
  {
    return trans_desc_;
  }
  int save_trans_desc(const ObTransDesc& trans_desc);
  void set_tmp_trans_desc(ObTransDesc& trans_desc);
  void set_exiting()
  {
    CtxLockGuard guard(lock_);
    (void)unregister_timeout_task_();
    set_exiting_();
  }
  int register_xa_timeout_task();
  int update_xa_branch_info(
      const ObXATransID& xid, const int64_t to_state, const ObAddr& addr, const int64_t timeout_seconds);
  int update_xa_branch_hb_info(const ObXATransID& xid);
  int xa_scheduler_hb_req();
  int xa_hb_resp(const ObXATransID& xid, const ObAddr& addr);
  int add_xa_branch_count();
  int64_t dec_and_get_xa_branch_count();
  int64_t get_total_branch_count() const
  {
    return xa_branch_count_;
  }
  int64_t add_and_get_xa_ref_count();
  int64_t dec_and_get_xa_ref_count();
  int64_t get_xa_ref_count() const
  {
    return xa_ref_count_;
  }
  bool is_xa_tightly_coupled() const
  {
    return is_tightly_coupled_;
  }
  void set_xa_tightly_coupled(const bool is_tightly_coupled)
  {
    is_tightly_coupled_ = is_tightly_coupled;
  }
  int xa_try_global_lock(const ObXATransID& xid);
  int xa_release_global_lock(const ObXATransID& xid);
  int xa_try_local_lock(const ObXATransID& xid);
  int xa_release_local_lock(const ObXATransID& xid);
  int set_trans_id(const ObTransID& trans_id);
  int trans_deep_copy(ObTransDesc& trans_desc) const;
  int wait_xa_start_complete();
  void notify_xa_start_complete(const int result);
  void set_terminated()
  {
    set_terminated_();
  }
  bool is_terminated()
  {
    return is_terminated_;
  }
  // orig scheduler need check state before handle xa_start, xa_end and stmt
  int check_for_xa_execution(const bool is_new_branch, const ObXATransID& xid);
  // this func can only be called in original sche (tighly coupled mode)
  int xa_end_stmt(const ObXATransID& xid, const ObTransDesc& trans_desc, const bool from_remote, const int64_t seq_no);

public:
  INHERIT_TO_STRING_KV("ObDistTransCtx", ObDistTransCtx, K_(state), K_(sql_no), K_(is_rollback), K_(session_id),
      K_(proxy_session_id), K_(cluster_id), K_(commit_times), K_(is_tenant_active), K_(app_trace_id_str),
      K_(is_gts_waiting), K_(can_elr), K_(savepoint_sql_no), K_(is_xa_end_trans), K_(is_xa_readonly),
      K_(xa_trans_state), K_(xa_branch_count), K_(xa_ref_count), K_(is_tightly_coupled), K_(lock_xid),
      K_(is_terminated), K_(stmt_rollback_req_timeout));
  static const int64_t OP_LOCAL_NUM = 16;

private:
  int merge_partition_leader_info_(const ObPartitionLeaderArray& partition_leader_arr);
  int merge_stmt_participants_pla_(const ObPartitionLeaderArray& pla);
  int submit_trans_request_(const int64_t msg_type);
  int submit_stmt_request_(
      const common::ObPartitionArray& participants, const int64_t msg_type, const ObTransDesc& trans_desc);
  int submit_bounded_staleness_request_(
      const common::ObPartitionLeaderArray& participants, const int64_t msg_type, ObTransDesc* trans_desc = NULL);
  bool can_local_trans_opt_(const int status);
  int handle_local_trans_commit_();
  int submit_request_(const common::ObPartitionKey& receiver, const int64_t msg_type,
      const bool need_create_ctx = false, const bool is_not_create_ctx_participant = false,
      const int64_t msg_start_ts = INT64_MAX / 2);
  int handle_trans_response_(const int64_t msg_type, const int status, const ObPartitionKey& sender,
      const ObAddr& sender_addr, const int64_t need_wait_interval_us);
  int handle_xa_trans_response_(const int64_t msg_type, const int status);
  int handle_stmt_response_(const ObTransMsg& msg);
  int handle_stmt_rollback_response_(const ObTransMsg& msg);
  int handle_start_stmt_response_(const ObTransMsg& msg);
  int handle_start_bounded_staleness_stmt_(
      const ObPartitionLeaderArray& partition_leader_arr, const ObTransDesc& trans_desc);
  int handle_savepoint_rollback_response_(const ObTransMsg& msg);
  int get_trans_location_(const ObPartitionKey& partition, ObAddr& addr);

private:
  int drive_();
  int drive_(bool& need_callback);
  int end_trans_(const bool is_rollback, bool& need_callback);
  int start_new_sql_(const int64_t sql_no);
  bool has_decided_() const
  {
    return State::END_TRANS == state_.get_state();
  }
  int end_trans_(
      const bool is_rollback, const int64_t trans_expired_time, const int64_t stmt_expired_time, bool& need_callback);
  int merge_participants_(const common::ObPartitionArray& participants);
  int merge_participants_pla_();
  int add_retry_stmt_rollback_task_(
      const ObPartitionKey& partition, const int64_t msg_type, const int64_t request_id, const int64_t task_start_ts);
  void check_inner_table_commit_();
  int xa_end_trans_(const bool is_rollback);
  int xa_one_phase_end_trans_(const bool is_rollback);
  int update_xa_branch_timeout_(
      const ObXATransID& xid, const int64_t timeout_seconds, const int64_t to_state, int64_t& target);
  int init_xa_branch_info_();
  int set_trans_app_trace_id_str_(const ObString& app_trace_id_str);
  int xa_rollback_session_terminate_();
  void set_terminated_();
  int xa_merge_status_(const ObTransDesc& trans_desc, const bool is_stmt_push);
  int xa_release_global_lock_(const ObXATransID& xid);
  int save_trans_desc_(const ObTransDesc& trans_desc);
  int update_xa_branch_hb_info_(const ObXATransID& xid);
  // get unprepared branch count for tightly coupled xa trans
  int64_t get_unprepared_branch_count_();

private:
  class State {
  public:
    static const int64_t INVALID = -1;
    static const int64_t UNKNOWN = 0;
    static const int64_t START_TRANS = 1;
    static const int64_t START_STMT = 2;
    static const int64_t END_STMT = 3;
    static const int64_t END_TRANS = 4;
    static const int64_t MAX = 5;

  public:
    static bool is_valid(const int64_t state)
    {
      return state > INVALID && state < MAX;
    }
  };
  class Ops {
  public:
    static const int64_t INVALID = -1;
    static const int64_t START_TRANS = 0;
    static const int64_t START_STMT = 1;
    static const int64_t END_STMT = 2;
    static const int64_t END_TRANS = 3;
    static const int64_t MAX = 4;

  public:
    static bool is_valid(const int64_t op)
    {
      return op > INVALID && op < MAX;
    }
  };
  class StateHelper {
  public:
    explicit StateHelper(int64_t& state) : state_(state), last_state_(State::INVALID), is_switching_(false)
    {}
    ~StateHelper()
    {}
    int switch_state(const int64_t op);
    void restore_state();

  private:
    int64_t& state_;
    int64_t last_state_;
    bool is_switching_;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObScheTransCtx);

private:
  // 0x0078746365686373 means reset schectx
  static const int64_t SCHE_CTX_MAGIC_NUM = 0x0078746365686373;
  static const int64_t RETRY_STMT_ROLLBACK_TASK_INTERVAL_US = 10 * 1000;

private:
  bool is_inited_;
  int64_t sql_no_;
  int64_t cur_stmt_snapshot_version_;
  bool is_rollback_;
  int64_t stmt_expired_time_;
  // for statement rollback
  common::ObPartitionArray last_stmt_participants_;
  common::ObPartitionArray cur_stmt_unreachable_partitions_;
  common::ObPartitionArray stmt_rollback_participants_;
  // participants leader of this transaction
  ObTransLocationCache trans_location_cache_;
  ObTransCond start_stmt_cond_;
  ObTransCond end_stmt_cond_;
  ObTransCond xa_end_trans_cond_;
  ObTransCond xa_sync_status_cond_;
  common::ObMaskSet msg_mask_set_;
  uint64_t cluster_id_;
  int64_t commit_times_;
  bool is_tenant_active_;
  common::ObPartitionLeaderArray last_stmt_participants_pla_;
  common::ObPartitionLeaderArray participants_pla_;
  common::ObPartitionArray gc_participants_;
  int snapshot_gene_type_;
  // trace_id of application
  char buffer_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  // we must hold a private copy of trace_id,
  // because session may already released
  ObString app_trace_id_str_;
  bool is_gts_waiting_;
  MonotonicTs rq_received_ts_;
  int64_t gts_request_ts_;
  bool can_elr_;
  bool multi_tenant_stmt_;
  int64_t savepoint_sql_no_;
  ObStmtRollbackInfo stmt_rollback_info_;
  int64_t last_commit_timestamp_;
  // has receive xa_commit / xa_rollback
  bool is_xa_end_trans_;
  bool is_xa_readonly_;
  int32_t xa_trans_state_;
  bool is_xa_one_phase_;
  // trans_desc of orig scheduler
  ObTransDesc* trans_desc_;
  // trans_desc of temp scheduler
  ObTransDesc* tmp_trans_desc_;
  ObXARpc* xa_rpc_;
  int64_t stmt_rollback_req_timeout_;
  common::ObLatch xa_stmt_lock_;
  ObTransCond xa_start_cond_;
  int64_t xa_branch_count_;
  // for debug, to be removed
  int64_t lock_grant_;
  bool is_tightly_coupled_;  // tightly is default
  ObXATransID lock_xid_;
  ObXABranchInfoArray* xa_branch_info_;
  bool is_terminated_;
};
}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_SCHE_CTX_
