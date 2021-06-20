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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_COORD_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_COORD_CTX_

#include "clog/ob_log_define.h"
#include "ob_trans_ctx.h"
#include "ob_trans_status.h"
#include "ob_ts_mgr.h"
#include "ob_trans_split_adapter.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
class ObMaskSet;
}  // namespace common

namespace transaction {
class ObITransCtxMgr;
class ObPartTransCtxMgr;
class ObTransMsg;
}  // namespace transaction

namespace transaction {
typedef ObSEArray<int64_t, OB_DEFAULT_PARTITION_KEY_COUNT> ObVersionArray;

// coordinator transaction context
class ObCoordTransCtx : public ObDistTransCtx, public ObTsCbTask {
public:
  ObCoordTransCtx()
      : ObDistTransCtx("coordinator", ObTransCtxType::COORDINATOR),
        partition_log_info_arr_(ObModIds::OB_TRANS_PARTITION_LOG_INFO_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        trans_location_cache_(ObModIds::OB_TRANS_LOCATION_CACHE, OB_MALLOC_NORMAL_BLOCK_SIZE)
  {
    reset();
  }
  ~ObCoordTransCtx()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service, const int64_t commit_times);
  virtual void destroy();
  void reset();
  void clear_part_ctx_arr_();
  int construct_context(const ObTransMsg& msg);
  int construct_context(const ObTrxMsgBase& msg, const int64_t msg_type);

public:
  int handle_message(const ObTransMsg& msg);
  bool is_inited() const;
  int handle_timeout(const int64_t delay);
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
  int get_end_trans_callback_item(ObEndTransCallbackArray& cb_array)
  {
    UNUSED(cb_array);
    return OB_SUCCESS;
  }
  int kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array);
  int leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array);
  int leader_takeover(const int64_t checkpoint)
  {
    UNUSED(checkpoint);
    return common::OB_SUCCESS;
  }
  int leader_active(const storage::LeaderActiveArg& arg)
  {
    UNUSED(arg);
    return common::OB_SUCCESS;
  }
  bool can_be_freezed() const
  {
    return true;
  }
  // ObTsCbTask
  uint64_t hash() const
  {
    return trans_id_.hash();
  }
  int get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts);
  int gts_elapse_callback(const MonotonicTs srr, const int64_t gts);
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
  int handle_batch_commit_succ();
  int handle_2pc_response(const ObTrxMsgBase& msg, const int64_t msg_type);
  /////////Performance optimization for single machine trans (START)////////////
  int handle_local_commit(const bool is_dup_table_trans, const int64_t commit_times);
  int construct_local_context(
      const ObAddr& scheduler, const ObPartitionKey& coordinator, const ObPartitionArray& participants);
  int handle_2pc_local_prepare_response(const int64_t msg_type, const ObPartitionKey& partition, const int32_t status,
      const int64_t partition_state, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr,
      const int64_t prepare_log_id, const int64_t prepare_log_timestamp, const int64_t publish_version,
      const int64_t request_id, const int64_t need_wait_interval_us, const ObTransSplitInfo& split_info,
      const bool is_xa_prepare);
  int handle_2pc_local_commit_response(
      const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type);
  int handle_2pc_local_clear_response(const ObPartitionKey& partition);
  /////////Performance optimization for single machine trans (END)////////////

public:
  INHERIT_TO_STRING_KV("ObDistTransCtx", ObDistTransCtx, K_(prepare_unknown_count), K_(prepare_error_count),
      K_(batch_commit_trans), K_(have_prev_trans), K_(trans_location_cache), K_(stmt_rollback_info), K_(split_info_arr),
      K_(is_waiting_xa_commit));
  static const int64_t OP_LOCAL_NUM = 16;

private:
  int drive_();
  int switch_state_(const int64_t state);
  int update_global_trans_version_(const int64_t trans_version);
  bool all_prepare_unknown_() const
  {
    return prepare_unknown_count_ == participants_.count();
  }
  bool has_decided_() const
  {
    return Ob2PCState::INIT != state_.get_state();
  }
  int check_and_response_scheduler_(const int64_t msg_type);
  void set_global_trans_version_(const int64_t trans_version)
  {
    global_trans_version_ = trans_version;
  }
  int64_t get_global_trans_version_() const
  {
    return global_trans_version_;
  }
  int handle_batch_commit_();
  int batch_submit_log_over_(const bool submit_log_succ);
  bool is_pre_preparing_() const;
  int collect_partition_log_info_(const ObPartitionKey& partition, const uint64_t log_id, const int64_t log_timestamp);
  bool can_batch_commit_();

private:
  // handle 2pc response from participants
  int handle_2pc_pre_prepare_response_(
      const ObPartitionKey& partition, const int status, const int64_t prepare_log_id, const int64_t prepare_log_ts);
  int handle_2pc_pre_prepare_action_(const int64_t prepare_version);
  int get_trans_mem_total_size_(int64_t& total_size) const;
  int handle_2pc_prepare_response_(const ObTransMsg& msg);
  int handle_2pc_commit_response_(const ObTransMsg& msg);
  int handle_2pc_abort_response_(const ObTransMsg& msg);
  int handle_2pc_commit_clear_response_(const ObTransMsg& msg);
  int handle_2pc_log_id_response_(const ObTransMsg& msg);
  int handle_2pc_pre_commit_response_(const ObTransMsg& msg);
  // optimization for local call
  int handle_2pc_local_prepare_action_();
  int handle_2pc_local_commit_request_();
  int handle_2pc_local_clear_request_();

private:
  // handle trans request from scheduler
  int handle_trans_request_(const ObTransLocationCache& trans_location, const ObStmtRollbackInfo& stmt_rollback_info,
      const bool is_dup_table_trans, const int64_t commit_times, const bool is_rollback);
  int handle_xa_trans_request_(const ObTransMsg& msg, const bool is_rollback);
  int handle_xa_trans_prepare_request_(const ObTransMsg& msg);
  int post_2pc_request_(const common::ObPartitionArray& pids, const int64_t msg_type);
  int generate_trans_msg_(const int64_t msg_type, const ObPartitionKey& receiver, ObTransMsgUnion& msg);
  int post_trans_response_(const common::ObAddr& server, const int64_t msg_type);
  int handle_partition_not_exist_(const ObPartitionKey& pkey, const ObTransMsgUnion& msg_union, const int64_t type);
  int post_batch_commit_request_(const int64_t msg_type, const ObPartitionKey& pkey, const int64_t key_index);
  int batch_post_2pc_request_(const ObPartitionArray& partitions, const int64_t msg_type);
  // raw interfaces for 2pc
  int handle_2pc_prepare_response_raw_(const int64_t msg_type, const ObPartitionKey& partition, const int32_t status,
      const int64_t partition_state, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr,
      const int64_t prepare_log_id, const int64_t prepare_log_timestamp, const int64_t publish_version,
      const ObTransSplitInfo& split_info);
  int handle_2pc_prepare_redo_response_raw_(
      const int64_t msg_type, const ObPartitionKey& partition, const int32_t status, const int64_t partition_state);
  int handle_2pc_abort_response_raw_(
      const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type);
  int handle_2pc_commit_response_raw_(
      const ObPartitionKey& partition, const ObPartitionArray& batch_same_leader_partitions, const int64_t msg_type);
  int handle_2pc_clear_response_raw_(const ObPartitionKey& partition, const ObPartitionArray* batch_partitions = NULL);
  int handle_2pc_pre_commit_response_raw_(const ObPartitionKey& partition, const int64_t commit_version,
      const ObPartitionArray& batch_same_leader_partitions);
  int construct_context_raw_(int64_t msg_type, const ObAddr& scheduler, const ObPartitionKey& coordinator,
      const ObPartitionArray& participants, int64_t status, int64_t trans_version,
      const PartitionLogInfoArray& partition_log_info_arr);
  // if need_full_precommt is ture, calling this function is not required.
  int add_participant_publish_version_(const ObPartitionKey& partition, const int64_t publish_version);
  void update_clear_log_base_ts_(const int64_t log_ts);
  int xa_drive_after_prepare_();
  int xa_drive_after_commit_();
  int xa_drive_after_rollback_();
  void DEBUG_SYNC_slow_txn_during_2pc_commit_phase_for_physical_backup_1055_();

private:
  DISALLOW_COPY_AND_ASSIGN(ObCoordTransCtx);

private:
  // 0x78746364726f6f63 means reset coordctx
  static const int64_t COORD_CTX_MAGIC_NUM = 0x78746364726f6f63;
  static const int64_t SAME_LEADER_PARTITION_BATCH_RPC_THRESHOLD = 3;

private:
  bool is_inited_;
  // check if response to scheduler or not
  bool already_response_;
  // 2pc message mask set
  common::ObMaskSet msg_mask_set_;
  // partition to prepare id
  PartitionLogInfoArray partition_log_info_arr_;
  // it is used to leader information of all participants for the trans
  ObTransLocationCache trans_location_cache_;
  int64_t prepare_unknown_count_;
  int64_t prepare_error_count_;
  int64_t global_trans_version_;
  bool batch_commit_trans_;
  bool enable_new_1pc_;
  ObPartTransCtxMgr* part_trans_ctx_mgr_;
  bool need_collect_logid_before_prepare_;
  bool is_gts_waiting_;
  int64_t gts_request_ts_;
  // 0 commit_times is used to indicate that the coord is constructed by parcicipant
  // positive commit_times is used to indicate that the coord id constructed by scheduler
  int64_t commit_times_;
  bool have_prev_trans_;
  ObTransStatusMgr* trans_status_mgr_;
  ObStmtRollbackInfo stmt_rollback_info_;
  ObSameLeaderPartitionArrMgr same_leader_partitions_mgr_;
  // These two varialbes are used to get the participants need precommit
  ObPartitionArray unconfirmed_participants_;
  ObVersionArray participant_publish_version_array_;
  ObTransSplitInfoArray split_info_arr_;
  // TRUE by default for xa trans, false is set after receiving xa commit
  bool is_waiting_xa_commit_;
  ObTransCtxArray part_ctx_arr_;
  // it is used to store the max commit_log_ts of participants
  int64_t clear_log_base_ts_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_COORD_CTX_
