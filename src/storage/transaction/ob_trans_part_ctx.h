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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_

#include "ob_trans_ctx.h"
#include "ob_trans_dependency.h"
#include "ob_ts_mgr.h"
#include "lib/container/ob_mask_set2.h"
#include "ob_dup_table.h"
#include "ob_trans_listener_handler.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
class ObMaskSet;
}  // namespace common

namespace storage {
struct ObStoreRowLockState;
}

namespace transaction {
class ObITransCtxMgr;
class ObTransMsg;
class ObRedoLogSyncResponseMsg;
class ObTransTaskWorker;
class ObTransStatusMgr;
class ObTransResultInfo;
class ObTransSplitInfo;
// class ObTransListenerHandler;
}  // namespace transaction

namespace memtable {
class ObIMemtableCtxFactory;
class ObIMemtableCtx;
};  // namespace memtable

namespace clog {
class ObLogMeta;
};

enum {
  USER_REQUEST_UNKNOWN = -1,
  USER_COMMIT = 0,
  USER_ABORT = 1,
};

namespace transaction {
class TransResultInfo {
public:
  TransResultInfo() : result_info_(NULL), registered_(false)
  {}
  ~TransResultInfo()
  {
    destroy();
  }
  void destroy()
  {
    reset();
  }
  void reset();
  int alloc_trans_result_info();
  ObTransResultInfo* get_trans_result_info()
  {
    return result_info_;
  }
  void set_registered(bool registered)
  {
    registered_ = registered;
  }

private:
  ObTransResultInfo* result_info_;
  bool registered_;
};

// participant transaction context
class ObPartTransCtx : public ObDistTransCtx, public ObTsCbTask {
  friend class IterateTransStatForKeyFunctor;

public:
  ObPartTransCtx()
      : ObDistTransCtx("participant", ObTransCtxType::PARTICIPANT),
        ObTsCbTask(),
        is_inited_(false),
        mt_ctx_(),
        prev_redo_log_ids_(ObModIds::OB_TRANS_REDO_LOG_ID_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        partition_log_info_arr_(ObModIds::OB_TRANS_PARTITION_LOG_INFO_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        listener_handler_(NULL)
  {
    reset();
  }
  ~ObPartTransCtx()
  {
    destroy();
  }
  // clog_adapter. the adapter belong transaction engine and clog engine
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service, const uint64_t cluster_id,
      const int64_t leader_epoch, const bool can_elr);
  virtual void destroy() override;
  void reset();
  int construct_context(const ObTransMsg& msg);

public:
  int start_trans();
  int start_task(const ObTransDesc& trans_desc, const int64_t snapshot_version, const bool need_update_gts,
      storage::ObIPartitionGroup* ob_partition);
  int end_task(
      const bool is_rollback, const ObTransDesc& trans_desc, const int64_t sql_no, const int64_t stmt_min_sql_no);
  int end_task_(
      const bool is_rollback, const ObTransDesc& trans_desc, const int64_t sql_no, const int64_t stmt_min_sql_no);
  int handle_message(const ObTransMsg& msg);
  bool is_inited() const override;
  int handle_timeout(const int64_t delay) override;
  int get_end_trans_callback_item(ObEndTransCallbackArray& cb_array);
  /*
   * graceful kill: wait trx finish logging
   */
  int kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array) override;
  int wait_1pc_trx_end_in_spliting(bool& trx_end);
  int check_cur_partition_split_(bool& is_split_partition);
  memtable::ObMemtableCtx* get_memtable_ctx()
  {
    return &mt_ctx_;
  }
  int set_memtable_ctx(memtable::ObIMemtableCtx* mt_ctx);
  int leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array) override;
  int leader_takeover(const int64_t checkpoint) override;
  int leader_active(const storage::LeaderActiveArg& arg) override;
  bool can_be_freezed() const override;
  int kill_trans(bool& need_convert_to_dist_trans);
  int commit(const bool is_rollback, sql::ObIEndTransCallback* cb, const bool is_readonly, const MonotonicTs commit_time,
      const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info,
      const common::ObString& app_trace_info, bool& need_convert_to_dist_trans) override;
  int set_stmt_info(const ObTransStmtInfo& stmt_info);
  const ObTransStmtInfo& get_stmt_info() const
  {
    return stmt_info_;
  }
  int recover_dist_trans(const ObAddr& scheduler);
  bool is_prepared() const;
  bool is_changing_leader() const
  {
    return is_changing_leader_;
  }
  int submit_log(const int64_t log_type);
  int callback_big_trans(
      const ObPartitionKey& pkey, const int64_t log_type, const int64_t log_id, const int64_t timestamp);
  int generate_redo_prepare_log_info(char* buf, const int64_t size, int64_t& pos, const int64_t request_id,
      const PartitionLogInfoArray& partition_log_info_arr, const int64_t commit_version, const bool have_prev_trans,
      clog::ObLogInfo& log_info, clog::ObISubmitLogCb*& cb);
  int get_prepare_version_if_prepared(bool& is_prepared, int64_t& prepare_version);
  int get_prepare_version_before_logts(const int64_t freeze_ts, bool& has_prepared, int64_t& prepare_version);
  int64_t get_snapshot_version() const;
  int64_t get_commit_version() const
  {
    return get_global_trans_version_();
  }
  uint64_t hash() const override
  {
    return trans_id_.hash();
  }
  int get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts) override;
  int gts_elapse_callback(const MonotonicTs srr, const int64_t gts) override;
  MonotonicTs get_stc() const override
  {
    return stc_;
  }
  int64_t get_request_ts() const override
  {
    return gts_request_ts_;
  }
  uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  int get_min_log(uint64_t& log_id, int64_t& log_ts) const;
  int elr_next_trans_callback(const int64_t task_type, const ObTransID& trans_id, int state);
  void check_memtable_ctx_ref();
  int64_t get_redo_log_no() const
  {
    return redo_log_no_;
  }
  int64_t get_mutator_log_no() const
  {
    return mutator_log_no_;
  }

public:
  int replay_sp_redo_log(
      const ObSpTransRedoLog& log, const int64_t timestamp, const uint64_t log_id, int64_t& log_table_version);
  int replay_sp_commit_log(const ObSpTransCommitLog& log, const int64_t timestamp, const uint64_t log_id);
  int replay_sp_abort_log(const ObSpTransAbortLog& log, const int64_t timestamp, const uint64_t log_id);
  int replay_redo_log(const ObTransRedoLog& log, const int64_t trans_version, const uint64_t log_id,
      const bool with_prepare, int64_t& log_table_version);
  int replay_record_log(const ObTransRecordLog &log, const int64_t trans_version, const uint64_t log_id);
  int replay_prepare_log(const ObTransPrepareLog& log, const int64_t trans_version, const uint64_t log_id,
      const bool batch_committed, const int64_t checkpoint);
  int replay_commit_log(const ObTransCommitLog& log, const int64_t trans_version, const uint64_t log_id);
  int replay_abort_log(const ObTransAbortLog& log, const int64_t trans_version, const uint64_t log_id);
  int replay_clear_log(const ObTransClearLog& log, const int64_t trans_version, const uint64_t log_id);
  int replay_trans_state_log(const ObTransStateLog& log, const int64_t timestamp, const uint64_t log_id);
  int replay_trans_mutator_log(
      const ObTransMutatorLog& log, const int64_t timestamp, const uint64_t log_id, int64_t& log_table_version);
  int replay_mutator_abort_log(const ObTransMutatorAbortLog& log, const int64_t timestamp, const uint64_t log_id);
  int replay_start_working_log(const int64_t timestamp, const uint64_t log_id);
  int replay_listener_commit_log(const ObTransCommitLog& log, const int64_t timestamp, const uint64_t log_id);
  int replay_listener_abort_log(const ObTransAbortLog& log, const int64_t timestamp, const uint64_t log_id);
  // log has persistented to abstract log-device (logic paxos log device)
  int on_sync_log_success(
      const int64_t log_type, const int64_t log_id, const int64_t version, const bool batch_committed);
  // a log written job was submitted successfully to logging layer
  int on_submit_log_success(
      const bool with_need_update_version, const uint64_t cur_log_id, const int64_t cur_log_timestamp);
  // submit log fail the log will not been written successfully
  int on_submit_log_fail(const int retcode);
  int set_commit_task_count(const int64_t commit_task_count);
  int set_trans_app_trace_id_str(const ObString& app_trace_id_str);
  const ObString& get_trans_app_trace_id_str() const
  {
    return trace_info_.get_app_trace_id();
  }
  int check_schema_version_elapsed(const int64_t schema_version, const int64_t refreshed_schema_ts);
  int check_ctx_create_timestamp_elapsed(const int64_t ts);
  int batch_submit_log_over(const bool submit_log_succ, const int64_t commit_version);
  int checkpoint(const int64_t checkpoint, const int64_t safe_slave_read_timestamp, bool& checkpoint_succ);
  int relocate_data(memtable::ObIMemtable* memtable);
  int get_memtable_key_arr(ObMemtableKeyArray& memtable_key_arr);
  uint64_t get_lock_for_read_retry_count() const
  {
    return mt_ctx_.get_lock_for_read_retry_count();
  }
  int prepare_changing_leader(const common::ObAddr& proposal_leader);
  int handle_2pc_pre_commit_request(const int64_t commit_version);
  int handle_2pc_commit_clear_request(const int64_t commit_version);
  int handle_2pc_pre_prepare_request(const int64_t prepare_version, const int64_t request_id, const ObAddr& scheduler,
      const ObPartitionKey& coordinator, const ObPartitionArray& participants, const common::ObString& app_trace_info,
      int& status, int64_t& prepare_log_id, int64_t& prepare_log_ts, bool& have_prev_trans);
  int handle_2pc_local_prepare_request(const int64_t request_id, const ObAddr& scheduler,
      const ObPartitionKey& coordinator, const ObPartitionArray& participants, const common::ObString& app_trace_info,
      const MonotonicTs stc, const int status, const bool is_xa_prepare);
  int handle_2pc_local_commit_request(
      const int64_t msg_type, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr);
  int handle_2pc_local_clear_request();
  int handle_2pc_local_msg_response(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t log_type);
  int before_prepare();
  int check_scheduler_status();
  int64_t get_cur_query_start_time() const
  {
    return cur_query_start_time_;
  }
  int set_cur_query_start_time(const int64_t cur_query_start_time);
  uint32_t get_ctx_id() const
  {
    return mt_ctx_.get_ctx_descriptor();
  }
  // register a callback task called after current transaction committed
  int submit_elr_callback_task(const int64_t callback_type, const ObTransID& trans_id, int state);
  ObPartTransCtxDependencyWrap& get_ctx_dependency_wrap()
  {
    return ctx_dependency_wrap_;
  }
  // check early lock release is prepared
  int check_elr_prepared(bool& elr_prepared, int64_t& elr_commit_version);
  int insert_prev_trans(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx);
  // void audit_partition(const bool is_rollback, const sql::stmt::StmtType stmt_type);
  int handle_redo_log_sync_response(const ObRedoLogSyncResponseMsg& msg);
  bool is_redo_log_sync_finish() const;
  bool is_prepare_leader_revoke() const;
  int retry_redo_sync_task(
      const uint64_t log_id, const int64_t log_type, const int64_t timestamp, const bool first_gen);
  bool need_to_post_redo_sync_task(const int64_t log_type) const;
  bool is_redo_log_replayed(const uint64_t log_id);
  // used by dead lock detector, maybe incorrect
  void set_local_trans(bool is_local_trans)
  {
    is_local_trans_ = is_local_trans;
  }
  int calculate_trans_ctx_cost(uint64_t& cost) const;
  int remove_callback_for_uncommited_txn(memtable::ObMemtable* mt);
  int remove_mem_ctx_for_trans_ctx(memtable::ObMemtable* mt);
  int handle_savepoint_rollback_request(const int64_t sql_no, const int64_t cur_sql_no, const bool need_response);
  int half_stmt_commit();
  int set_trans_table_status_info(const ObTransTableStatusInfo& trans_table_status_info);
  int get_trans_mem_total_size(int64_t& size) const;

  // dirty trans need to keep transaction state in memory to help decide
  // transaction modify-set accessbility
  // this contains two cases:
  // 1. trasaction persistented before commit/abort
  // 2. a transaction cross freeze_log_id whose modify-set locate in more than one memtable
  int mark_frozen_data(const memtable::ObMemtable* const frozen_memtable,
      const memtable::ObMemtable* const active_memtable, int64_t& cb_cnt);
  int submit_log_for_split(bool& log_finished);
  bool mark_dirty_trans();
  bool is_dirty_trans() const
  {
    return is_dirty_;
  }
  bool has_synced_log() const
  {
    return 0 != max_durable_log_ts_;
  }
  int64_t get_forbidden_sql_no() const
  {
    return ATOMIC_LOAD(&forbidden_sql_no_);
  }
  int set_forbidden_sql_no(const int64_t sql_no, bool& forbid_succ);
  void update_max_durable_sql_no(const int32_t sql_no)
  {
    inc_update(&max_durable_sql_no_, sql_no);
  }
  int lock_for_read(
      const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state);
  int get_transaction_status_with_log_ts(const int64_t log_ts, ObTransTableStatusType& status, int64_t& trans_version);
  int is_running(bool& is_running);
  int check_sql_sequence_can_read(const int64_t sql_sequence, bool& can_read);
  int check_row_locked(const ObStoreRowkey& key, memtable::ObIMvccCtx& ctx, const transaction::ObTransID& read_trans_id,
      const transaction::ObTransID& data_trans_id, const int64_t sql_sequence,
      storage::ObStoreRowLockState& lock_state);
  int cleanout_transnode(memtable::ObMvccTransNode& tnode, memtable::ObMvccRow& value, bool& cleanout_finish);
  int replay_rollback_to(const int64_t sql_no, const int64_t log_ts);
  int check_log_ts_and_get_trans_version(
      const int64_t log_ts, int64_t& trans_version, bool& is_related_trans, bool& is_rollback_trans);
  int get_trans_table_status_info(ObTransTableStatusInfo& trans_table_status_info);
  int check_if_terminated_in_given_log_range(const int64_t start_log_ts, const int64_t end_log_ts, bool& is_terminated);
  int get_trans_sstable_durable_ctx_info(const int64_t log_ts, ObTransSSTableDurableCtxInfo& info);
  int update_and_get_trans_table_with_minor_freeze(const int64_t log_ts, uint64_t& checksum);

  int recover_from_trans_sstable_durable_ctx_info(ObTransSSTableDurableCtxInfo& ctx_info);
  int64_t get_applying_log_ts() const;
  int64_t get_pending_log_size()
  {
    return mt_ctx_.get_pending_log_size();
  }
  int64_t get_flushed_log_size()
  {
    return mt_ctx_.get_flushed_log_size();
  }
  int submit_log_if_neccessary();
  // get transaction state, commit version and undo information
  int get_trans_state_and_version_without_lock(ObTransStatusInfo& trans_info);
  int handle_2pc_request(const ObTrxMsgBase& msg, const int64_t msg_type);
  int handle_listener_message(const ObTrxMsgBase& msg, const int64_t msg_type);
  int construct_listener_context(const ObTrxMsgBase& msg);
  int get_trans_state(int64_t& state);
  void get_audit_info(int64_t& lock_for_read_elapse) const;
  bool has_write_or_replay_mutator_redo_log() const
  {
    return has_write_or_replay_mutator_redo_log_;
  }
  int64_t get_max_durable_log_ts() const
  {
    return max_durable_log_ts_;
  }
  bool is_logging() const
  {
    return is_logging_();
  }
  bool is_listener() const
  {
    return is_listener_;
  }
  bool is_enable_new_1pc() const
  {
    return enable_new_1pc_;
  }
  bool is_task_match()
  {
    return stmt_info_.is_task_match();
  }
  void remove_trans_table();
  int clear_trans_after_restore(const int64_t restore_version, const uint64_t last_restore_log_id,
      const int64_t last_restore_log_ts, const int64_t fake_terminate_log_ts);
  bool is_in_trans_table_state();
  virtual int64_t get_part_trans_action() const override;
  int rollback_stmt(const int64_t from_sql_no, const int64_t to_sql_no);
  bool need_update_schema_version(const uint64_t log_id, const int64_t log_ts);
  int update_max_majority_log(const uint64_t log_id, const int64_t log_ts);
  void reset_prev_redo_log_ids();

public:
  INHERIT_TO_STRING_KV("ObDistTransCtx", ObDistTransCtx, K_(snapshot_version), K_(local_trans_version),
      K_(submit_log_pending_count), K_(submit_log_count), K_(stmt_info), K_(global_trans_version), K_(redo_log_no),
      K_(mutator_log_no), K_(session_id), K_(is_gts_waiting), K_(part_trans_action), K_(timeout_task),
      K_(batch_commit_trans), K_(batch_commit_state), K_(is_dup_table_trans), K_(last_ask_scheduler_status_ts),
      K_(last_ask_scheduler_status_response_ts), K_(ctx_dependency_wrap), K_(preassigned_log_meta),
      K_(is_dup_table_prepare), K_(dup_table_syncing_log_id), K_(is_prepare_leader_revoke), K_(is_local_trans),
      K_(forbidden_sql_no), K(is_dirty_), K_(undo_status), K_(max_durable_sql_no), K_(max_durable_log_ts),
      K(mt_ctx_.get_checksum_log_ts()), K_(is_changing_leader), K_(has_trans_state_log),
      K_(is_trans_state_sync_finished), K_(status), K_(same_leader_batch_partitions_count), K_(is_hazardous_ctx),
      K(mt_ctx_.get_callback_count()), K_(in_xa_prepare_state), K_(is_listener), K_(last_replayed_redo_log_id),
      K_(status), K_(is_xa_trans_prepared));

public:
  static const int64_t OP_LOCAL_NUM = 16;

private:
  int init_memtable_ctx_(ObTransService* txs, const uint64_t tenant_id);
  bool is_in_2pc_() const;
  bool is_logging_() const;
  bool has_logged_() const;
  bool is_pre_preparing_() const;
  bool need_record_log() const;
  int reserve_log_header_(char* buf, const int64_t size, int64_t& pos);
  int fill_sp_redo_log_(ObSpTransRedoLog& sp_redo_log, const int64_t available_capacity, int64_t& mutator_size);
  int fill_sp_commit_log_(const int real_log_type, char* buf, const int64_t size, int64_t& pos, int64_t& log_type,
      bool& has_redo_log, bool& need_submit_log, int64_t& mutator_size);
  int fill_sp_abort_log_(char* buf, const int64_t size, int64_t& pos, int64_t& log_type);
  int fill_redo_log_(char* buf, const int64_t size, int64_t& pos, int64_t& mutator_size);
  int fill_prepare_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_commit_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_record_log_(char *buf, const int64_t size, int64_t &pos);
  int fill_abort_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_clear_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_log_header_(char* buf, const int64_t size, int64_t& pos, const int64_t log_type, const int64_t idx);
  int fill_redo_prepare_log_(
      char* buf, const int64_t size, int64_t& pos, int64_t& redo_log_pos, int64_t& log_type, int64_t& mutator_size);
  int fill_redo_prepare_commit_log_(
      char* buf, const int64_t size, int64_t& pos, int64_t& redo_log_pos, int64_t& log_type, int64_t& mutator_size);
  int fill_redo_prepare_commit_clear_log_(
      char* buf, const int64_t size, int64_t& pos, int64_t& log_type, int64_t& mutator_size);

  int fill_trans_state_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_mutator_log_(char* buf, const int64_t size, int64_t& pos, int64_t& mutator_size);
  int fill_mutator_state_log_(char* buf, const int64_t size, int64_t& pos, int64_t& log_type);
  int fill_mutator_abort_log_(char* buf, const int64_t size, int64_t& pos);
  int fill_pre_commit_log_(char* buf, const int64_t size, int64_t& pos);

  int submit_log_impl_(const int64_t log_type, const bool pending, const bool sync, bool& has_redo_log);
  int submit_log_async_(const int64_t log_type, bool& has_redo_log)
  {
    return submit_log_impl_(log_type, false, false, has_redo_log);
  }
  int submit_log_task_(const int64_t log_type, bool& has_redo_log)
  {
    return submit_log_impl_(log_type, true, false, has_redo_log);
  }
  int submit_log_sync_(const int64_t log_type, bool& has_redo_log);

private:
  int handle_start_stmt_request_(const ObTransMsg& msg);
  int handle_stmt_rollback_request_(const ObTransMsg& msg);
  int handle_2pc_prepare_request_(const ObTransMsg& msg);
  int handle_2pc_log_id_request_(const ObTransMsg& msg);
  int handle_2pc_commit_request_(const ObTransMsg& msg);
  int handle_2pc_abort_request_(const ObTransMsg& msg);
  int handle_2pc_clear_request_(const ObTransMsg& msg);
  int handle_2pc_commit_clear_request_(const int64_t commit_version, const bool need_response);
  int handle_2pc_pre_commit_request_(const int64_t commit_version, const bool need_response);
  int handle_trans_ask_scheduler_status_response_(const ObTransMsg& msg);
  int handle_savepoint_rollback_request_(const int64_t sql_no, const int64_t cur_sql_no, const bool need_response);
  int handle_2pc_commit_request_raw_(
      const int64_t msg_type, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr);
  int handle_2pc_abort_request_raw_(const int64_t msg_type, const int64_t msg_status);
  int handle_2pc_clear_request_raw_(const int64_t msg_type);
  int handle_2pc_prepare_request_raw_(int status);
  int handle_2pc_prepare_redo_request_raw_(int status);

private:
  int drive_();
  void check_prev_trans_state_();
  int start_trans_();
  int trans_end_(const bool commit, const int64_t commit_version);
  int trans_clear_();
  int trans_kill_();
  int start_stmt_(const int64_t sql_no);
  int end_stmt_(const bool is_rollback, const int64_t sql_no, const int64_t stmt_min_sql_no, bool& need_response);
  int generate_snapshot_(storage::ObIPartitionGroup* partition);
  int compensate_prepare_no_log_();
  int compensate_mutator_abort_log_();
  int clear_ctx_(const bool commit);
  int post_2pc_response_(const common::ObPartitionKey& pid, const int64_t msg_type);
  int update_global_trans_version_(const int64_t trans_version);
  int post_stmt_response_(
      const int64_t msg_type, const int64_t sql_no, const int status, const int64_t msg_timeout = INT64_MAX / 2);
  int post_response_(
      const common::ObPartitionKey& receiver, const int64_t msg_type, const int64_t sql_no, bool& partition_exist);
  int post_trans_response_(const int64_t msg_type);
  int handle_trans_clear_request_(const ObTransMsg& msg);
  int handle_trans_discard_request_(const ObTransMsg& msg);
  int do_prepare_(const int status);  // before write redo/prepare log
  int do_dist_commit_(const int64_t trans_version, const PartitionLogInfoArray* partition_log_info_arr);
  int do_abort_();
  int do_clear_();
  int on_prepare_redo_();  // after redo/prepare log written
  int on_prepare_(const bool batch_committed, const int64_t timestamp);
  int on_sp_commit_(const bool commit, const int64_t timestamp = OB_INVALID_TIMESTAMP);
  int on_dist_commit_();
  int on_dist_abort_();
  int on_clear_(const bool need_response);
  int trans_sp_end_(const bool commit);
  void inc_submit_log_pending_count_();
  void dec_submit_log_pending_count_();
  void inc_submit_log_count_();
  void dec_submit_log_count_();
  int64_t get_global_trans_version_() const
  {
    return global_trans_version_;
  }
  void set_global_trans_version_(const int64_t trans_version)
  {
    global_trans_version_ = trans_version;
  }
  int alloc_local_trans_version_(const int64_t log_type);
  int prepare_sp_redolog_id_ts_(const int64_t log_type, bool& need_retry_alloc_id_ts);
  int retry_submit_log_(const int64_t log_type);
  int generate_local_trans_version_(const int64_t log_type);
  int generate_log_id_timestamp_(const int64_t log_type);
  int trans_replay_commit_(const int64_t commit_version, const int64_t checksum);
  int update_publish_version_(const int64_t publish_version, const bool for_replay);
  int calc_batch_commit_version_(int64_t& commit_version);
  int commit_by_checkpoint_(const int64_t commit_version);
  void update_last_checkpoint_(const int64_t checkpoint);
  int get_checkpoint_(int64_t& checkpoint);
  int checkpoint_(const int64_t checkpoint, const int64_t safe_slave_read_timestamp, bool& checkpoint_succ);
  bool in_changing_leader_windows_(const int64_t ts, common::ObTsWindows& changing_leader_windows)
  {
    return changing_leader_windows.contain(ts);
  }
  bool is_idential_tenant_();
  int prepare_changing_leader_(const common::ObAddr& proposal_leader);
  bool can_be_recycled_();
  bool need_to_ask_scheduler_status_();
  int check_rs_scheduler_is_alive_(bool& is_alive);
  int force_kill_();
  int post_ask_scheduler_status_msg_();
  // early lock release
  int drive_by_prev_trans_(int state);
  int check_and_early_release_lock_();
  int generate_sp_commit_log_type_(storage::ObStorageLogType& log_type);
  int pre_check_sp_trans_log_(const int log_type, ObSpTransCommitLog& commit_log, bool& need_wait_prev_trans);
  int register_trans_result_info_(const int state);
  int insert_all_prev_trans_(const bool for_replaying);
  bool bc_has_alloc_log_id_ts_()
  {
    return batch_commit_state_ >= ObBatchCommitState::ALLOC_LOG_ID_TS;
  }
  bool bc_has_generate_redo_log_()
  {
    return ObBatchCommitState::GENERATE_REDO_PREPARE_LOG == batch_commit_state_;
  }
  bool bc_has_generate_prepare_log_()
  {
    return batch_commit_state_ >= ObBatchCommitState::GENERATE_PREPARE_LOG;
  }
  int revert_self_();
  int get_compat_mode_(oceanbase::share::ObWorker::CompatMode& mode);
  int check_and_update_compact_mode_();
  bool in_pending_state_();
  bool has_write_data_();
  bool has_trans_version_();
  // duplicated table partition sync redo log
  int post_redo_log_sync_request_(
      const ObAddrLogIdArray& addr_logid_array, const uint64_t log_id, const int64_t log_ts, const int64_t log_type);
  int post_redo_log_sync_to_not_mask_addr_(const uint64_t log_id, const int64_t log_ts, const int64_t log_type);
  int post_redo_log_sync_response_(const uint64_t log_id);
  void update_max_submitted_log_timestamp_(const int64_t cur_log_timestamp);
  bool not_need_write_next_log_(const int64_t log_type);
  virtual void set_exiting_() override;
  virtual bool is_dirty() const override
  {
    return is_dirty_;
  }
  int submit_log_incrementally_(const bool need_state_log);
  int submit_log_incrementally_(const bool need_state_log, bool& has_redo_log);
  int submit_log_when_preparing_changing_leader_(const int64_t sql_no);
  int rollback_to_(const int32_t sql_no);
  int do_sp_trans_rollback_();
  int do_sp_trans_rollback_(const bool is_rollback, const int ret_code, sql::ObIEndTransCallback* cb);
  void debug_slow_on_sync_log_success_(const int64_t log_type);
  void DEBUG_SYNC_slow_txn_before_handle_message_(const int64_t msg_type);
  void DEBUG_SYNC_slow_txn_during_2pc_prepare_phase_for_physical_backup_1055_(const int64_t msg_type);
  int decide_log_type_for_mutator_and_record_(storage::ObStorageLogType& log_type);
  int decide_and_submit_next_log_(storage::ObStorageLogType& log_type, bool &has_redo_log, bool has_pending_cb);
  int get_trans_table_status_info_(ObTransTableStatusInfo& trans_table_status_info);
  int get_trans_table_status_info_(const int64_t log_ts, ObTransTableStatusInfo& trans_table_status_info);
  int get_callback_type_(const int64_t sql_sequence, memtable::TransCallbackType& cb_type);
  bool can_submit_log_(const int64_t log_type);
  void update_durable_log_id_ts_(const int64_t log_type, const uint64_t log_id, const int64_t log_ts);
  int set_trans_table_status_info_(const ObTransTableStatusInfo& trans_table_status_info);
  int submit_big_trans_callback_task_(const int64_t log_type, const int64_t log_id, const int64_t timestamp);
  void update_clear_log_base_ts_(const int64_t log_ts);
  bool need_rollback_when_restore_(const int64_t commit_version);
  int64_t decide_sstable_trans_state_();
  bool is_trans_valid_for_replay_(const storage::ObStorageLogType log_type, const int64_t log_ts);
  int check_need_notify_listener_(bool& need_notify_listener);
  int init_listener_handler_();
  int try_respond_coordinator_(const ObTransMsgType msg_type, const ListenerAction action);
  int get_prepare_ack_arg_(int& status, int64_t& state, int64_t& prepare_version, uint64_t& prepare_log_id,
      int64_t& prepare_log_ts, int64_t& request_id, int64_t& remain_wait_interval_us, bool& is_xa_prepare);
  int check_row_locked_(const ObTransStatusInfo& trans_info,
      const ObTransID& data_trans_id, const int64_t sql_sequence, storage::ObStoreRowLockState& lock_state);
  int lock_for_read_(const ObTransStatusInfo& trans_info, const ObLockForReadArg& lock_for_read_arg, bool& can_read,
      int64_t& trans_version, bool& is_determined_state);
  int check_sql_sequence_can_read_(const ObTransStatusInfo& trans_info, const int64_t sql_sequence, bool& can_read);
  int set_tmp_scheduler_(const common::ObAddr& scheduler);
  void try_restore_read_snapshot();
  bool is_xa_last_empty_redo_log_() const;
  int fake_kill_(const int64_t terminate_log_ts);
  int kill_v2_(const int64_t terminate_log_ts);
  int calc_serialize_size_and_set_participants_(const ObPartitionArray &participants);
  int do_calc_and_set_participants_(const ObPartitionArray &participants);
  int calc_serialize_size_and_set_undo_(const int64_t undo_to, const int64_t undo_from);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartTransCtx);

private:
  // 0x0078746374726170 means reset partctx
  static const int64_t PART_CTX_MAGIC_NUM = 0x0078746374726170;
  static const int64_t REPLAY_PRINT_TRACE_THRESHOLD = 10 * 1000;      // 10 ms
  static const int64_t REDO_SYNC_TASK_RETRY_INTERVAL_US = 10 * 1000;  // 10ms
  static const int64_t END_STMT_SLEEP_US = 10 * 1000;                 // 10ms
  static const int64_t MAX_END_STMT_RETRY_TIMES = 100;
  static const uint64_t MAX_PREV_LOG_IDS_COUNT = 80000;
private:
  bool is_inited_;
  ObIClogAdapter* clog_adapter_;
ObTransSubmitLogCb submit_log_cb_;
  memtable::ObMemtableCtx mt_ctx_;
  memtable::ObIMemtableCtxFactory* mt_ctx_factory_;
  ObTransTaskWorker* big_trans_worker_;
  int64_t redo_log_no_;
  int64_t mutator_log_no_;
  ObRedoLogIdArray prev_redo_log_ids_;
  // partition to prepare id
  PartitionLogInfoArray partition_log_info_arr_;
  storage::ObPartitionService* partition_service_;
  ObTransStatusMgr* trans_status_mgr_;
  int64_t snapshot_version_;
  // 2pc participant's prepare version
  // this acquired from gts or gts cache
  // it was not the final prepare version usually
  int64_t local_trans_version_;
  // number of successfully executed task
  // if equals to zero, part_ctx can be released directly
  int64_t commit_task_count_;
  // in threadpoll submit log task count
  int64_t submit_log_pending_count_;
  // in progress persistent log task count
  int64_t submit_log_count_;
  uint64_t cluster_id_;
  int64_t prepare_log_id_;
  int64_t prepare_log_timestamp_;
  int64_t global_trans_version_;
  ObTransStmtInfo stmt_info_;
  int64_t leader_epoch_;
  clog::ObLogMeta preassigned_log_meta_;
  // minimum log_id this partition has written
  uint64_t min_log_id_;
  int64_t min_log_ts_;
  int64_t end_log_ts_;
  int64_t max_submitted_log_timestamp_;
  // current log type need to be write
  int64_t log_type_;
  int64_t gts_request_ts_;
  int64_t stmt_expired_time_;
  int64_t quick_checkpoint_;
  int64_t commit_log_checksum_;
  CHANGING_LEADER_STATE prepare_changing_leader_state_;

  common::ObAddr proposal_leader_;

  int64_t last_ask_scheduler_status_ts_;
  int64_t last_ask_scheduler_status_response_ts_;
  int64_t cur_query_start_time_;
  // forbiden this sql's task
  int64_t forbidden_sql_no_;
  int batch_commit_state_;
  // ELR transaction dep relation
  ObPartTransCtxDependencyWrap ctx_dependency_wrap_;
  common::ObMaskSet2<ObAddrLogId> dup_table_msg_mask_set_;
  ObAddrLogIdArray dup_table_lease_addrs_;
  ObDupTableRedoSyncTask* redo_sync_task_;
  bool is_dup_table_prepare_;
  uint64_t dup_table_syncing_log_id_;
  int64_t dup_table_syncing_log_ts_;
  int64_t async_applying_log_ts_;
  ObTransUndoStatus undo_status_;
  int32_t max_durable_sql_no_;
  // uint64_t max_durable_log_id_;
  int64_t max_durable_log_ts_;
  // persistented transaction which uncommitted
  bool is_dirty_;
  mutable TransTableSeqLock trans_table_seqlock_;
  int sp_user_request_;
  int32_t same_leader_batch_partitions_count_;
  bool need_checksum_;
  bool is_prepared_;
  bool is_gts_waiting_;
  bool batch_commit_trans_;
  // Whether there exists a trans state log for the current leader transfer
  //
  // It is implemented as follow:
  // - For the New Leader:
  //   - we set the value to true when we replay the trans state log
  //     if the new leader is me
  //   - we reset the value when leader is active
  // - For the original Leader:
  //   - we reset the value before each leader transfer
  //   - we set the value to true when we synced the trans state log
  //   - we reset the value when leader is revoked if no on-the-fly log
  //     exist
  bool is_trans_state_sync_finished_;
  bool is_changing_leader_;
  bool can_rollback_stmt_;
  // this participant has switch leader out and write transaction state log
  bool has_trans_state_log_;
  bool waiting_next_trans_rollback_;
  bool is_prepare_leader_revoke_;
  bool is_local_trans_;
  // ctx's state can't be recovery, must reboot server
  bool is_hazardous_ctx_;
  // xa prepare_state indicate xa commit has not been received
  // it is used to distingush the requirement of prepare trx_prepare_version
  bool in_xa_prepare_state_;
  // only used by XA transaction
  // used to distingush redo log has been persistented
  bool is_redo_prepared_;
  bool has_gen_last_redo_log_;
  // this let clear_log's timestamp always
  // greater than commit timestamp of all participants
  int64_t clear_log_base_ts_;
  TransResultInfo result_info_;
  int64_t end_log_ts_for_batch_commit_;
  int64_t end_log_ts_for_elr_;
  bool is_listener_;
  ObTransListenerHandler* listener_handler_;
  storage::ObIPartitionGroup* pg_;
  bool enable_new_1pc_;
  ObTransCtx* coord_ctx_;
  uint64_t last_replayed_redo_log_id_;
  // used by XA response message to sender
  common::ObAddr tmp_scheduler_;
  int64_t last_redo_log_mutator_size_;
  bool is_xa_trans_prepared_;
  bool has_write_or_replay_mutator_redo_log_;
  bool is_in_redo_with_prepare_;
  int64_t redo_log_id_serialize_size_;
  int64_t participants_serialize_size_;
  int64_t undo_serialize_size_;
  // the log id of prev checkpoint log
  uint64_t prev_checkpoint_id_;
};

#if defined(__x86_64__)
STATIC_ASSERT(sizeof(ObPartTransCtx) < 8000, "ObPartTransCtx is too big");
#endif

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_
