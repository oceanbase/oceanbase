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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_
#define OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_

#include <stdlib.h>
#include <time.h>
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "common/ob_partition_key.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/utility/utility.h"
#include "ob_trans_define.h"
#include "ob_trans_timer.h"
#include "ob_location_adapter.h"
#include "ob_clog_adapter.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_rpc.h"
#include "ob_dup_table_rpc.h"
#include "ob_trans_memory_stat.h"
#include "ob_trans_event.h"
#include "ob_trans_audit_record_mgr.h"
#include "ob_trans_migrate_worker.h"
#include "ob_dup_table.h"
#include "ob_trans_task_worker.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_iarray.h"
#include "ob_trans_status.h"
#include "ob_trans_msg_handler.h"
#include "ob_xa_trans_heartbeat_worker.h"
#include "observer/ob_server_struct.h"
#include "ob_xa_rpc.h"
#include "ob_xa_inner_table_gc_worker.h"

namespace oceanbase {

namespace obrpc {
class ObTransRpcProxy;
class ObTransRpcResult;
}  // namespace obrpc

namespace common {
class ObAddr;
class ObPartitionKey;
}  // namespace common

namespace share {
class ObIPartitionLocationCache;
}

namespace obrpc {
class ObDupTableRpcProxy;
}

namespace storage {
class ObPartitionService;
class ObStoreCtx;
class ObIFreezeCb;
class ObIPartitionArrayGuard;
class LeaderActiveArg;
class ObIPartitionGroupGuard;
}  // namespace storage

namespace memtable {
class ObIMemtable;
class ObMemtable;
class ObIMemtableCtx;
class ObMemtableCtx;
class ObIMemtableCtxFactory;
}  // namespace memtable

namespace transaction {
class ObITsMgr;

// iterate transaction module memory usage status
typedef common::ObSimpleIterator<ObTransMemoryStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_MEMORY_STAT, 16>
    ObTransMemStatIterator;
// cache scaned duplicated table partition and its lease request task
typedef common::ObLinkHashMap<common::ObPartitionKey, ObDupTableLeaseTask> ObDupTableLeaseTaskMap;

class KillTransArg {
public:
  KillTransArg(const bool graceful, const bool ignore_ro_trans, const bool need_kill_coord_ctx = true)
      : graceful_(graceful), ignore_ro_trans_(ignore_ro_trans), need_kill_coord_ctx_(need_kill_coord_ctx)
  {}
  ~KillTransArg()
  {}
  TO_STRING_KV(K_(graceful), K_(ignore_ro_trans), K_(need_kill_coord_ctx));

public:
  bool graceful_;
  bool ignore_ro_trans_;
  bool need_kill_coord_ctx_;
};

enum class ObThreadLocalTransCtxState : int {
  OB_THREAD_LOCAL_CTX_INVALID,
  OB_THREAD_LOCAL_CTX_READY,
  OB_THREAD_LOCAL_CTX_RUNNING,
  OB_THREAD_LOCAL_CTX_BLOCKING
};

class ObThreadLocalTransCtx {
public:
  static const int64_t MAX_BIG_TRANS_WORKER = 8;
  static const int64_t MINI_MODE_MAX_BIG_TRANS_WORKER = 1;
  static const int64_t MAX_BIG_TRANS_TASK = 100 * 1000;

public:
  ObThreadLocalTransCtx() : state_(ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_READY)
  {}
  ~ObThreadLocalTransCtx()
  {
    destroy();
  }
  void reset()
  {
    state_ = ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_INVALID;
  }
  void destroy();

public:
  memtable::ObMemtableCtx memtable_ctx_;
  ObThreadLocalTransCtxState state_;
} CACHE_ALIGNED;

class ObTransService : public common::ObSimpleThreadPool {
public:
  ObTransService();
  virtual ~ObTransService()
  {
    destroy();
  }
  int init(const common::ObAddr& self, share::ObIPartitionLocationCache* location_cache,
      storage::ObPartitionService* partition_service, obrpc::ObTransRpcProxy* rpc_proxy, obrpc::ObBatchRpc* batch_rpc,
      obrpc::ObDupTableRpcProxy* dup_table_rpc_proxy, obrpc::ObXARpcProxy* xa_proxy,
      share::schema::ObMultiVersionSchemaService* schema_service, ObITsMgr* ts_mgr,
      share::ObAliveServerTracer& server_tracer);
  // just for test
  int init(const ObAddr& self, ObITransRpc* rpc, ObILocationAdapter* location_adapter, ObIClogAdapter* clog_adapter,
      storage::ObPartitionService* partition_service, share::schema::ObMultiVersionSchemaService* schema_service,
      ObITsMgr* ts_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  int push(void* task);
  virtual void handle(void* task) override;

public:
  memtable::ObIMemtableCtxFactory* get_mem_ctx_factory();
  // interfaces for SQL
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_IS_EXITING
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_INVALID_ARGUMENT
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   * timer error:
   * OB_CLOCK_OUT_OF_ORDER, OB_TIMER_TASK_HAS_SCHEDULER
   */
  int start_trans(const uint64_t tenant_id, const uint64_t cluster_id, const ObStartTransParam& req,
      const int64_t expired_time, const uint32_t session_id, const uint64_t proxy_session_id, ObTransDesc& trans_desc);
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_IS_EXITING
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   * RPC error: OB_INACTIVE_RPC_PROXY, OB_LIBEASY_ERROR, OB_RPC_POST_ERROR
   * cb: callback convention
   * if end_trans return OB_SUCC cb.callback must have been called
   * otherwise cb.callback must have not been called
   * */
  int end_trans(
      bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb, const int64_t stmt_expired_time);
  int internal_kill_trans(ObTransDesc& trans_desc);
  int kill_query_session(const ObTransDesc& trans_desc, const int status);
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_IS_EXITING
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   * RPC error: OB_INACTIVE_RPC_PROXY, OB_LIBEASY_ERROR, OB_RPC_POST_ERROR
   */
  int start_stmt(const ObStmtParam& stmt_param, ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla,
      common::ObPartitionArray& out_partitions);
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_IS_EXITING
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   * RPC error: OB_INACTIVE_RPC_PROXY, OB_LIBEASY_ERROR, OB_RPC_POST_ERROR
   */
  int end_stmt(bool is_rollback, bool is_incomplete, const ObPartitionArray& cur_stmt_all_participants,
      const ObPartitionEpochArray& epoch_arr, const ObPartitionArray& discard_participant,
      const ObPartitionLeaderArray& pla, ObTransDesc& trans_desc);
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_IS_EXITING
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   * RPC error: OB_INACTIVE_RPC_PROXY, OB_LIBEASY_ERROR, OB_RPC_POST_ERROR
   */
  int start_participant(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
      ObPartitionEpochArray& partition_epoch_arr, storage::ObIPartitionArrayGuard& pkey_guard_arr);
  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_TRANS_IS_EXITING
   *
   * error code:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   * OB_PARTITION_NOT_EXIST
   */
  int end_participant(bool is_rollback, const ObTransDesc& trans_desc, const ObPartitionArray& participants);
  int half_stmt_commit(const ObTransDesc& trans_desc, const ObPartitionKey& partition);
  /*
   * get statement snapshot info for nested sql
   */
  int get_stmt_snapshot_info(const bool is_cursor, ObTransDesc& trans_desc, ObTransSnapInfo& snapshot_info);
  int check_partition_status(const ObTransDesc& trans_desc, const ObPartitionKey& partition, int64_t& epoch);
  int check_partition_status(const ObPartitionKey& partition);
  int get_cached_pg_guard(const ObPartitionKey& partition, storage::ObIPartitionGroupGuard*& guard);
  int check_trans_partition_leader_unsafe(const ObPartitionKey& partition, bool& is_leader);
  /*
   * start a nested stmt
   * 1. generate snapshot
   * 2. generate sql_no for nested stmt
   */
  int start_nested_stmt(ObTransDesc& trans_desc);
  /*
   * at the end of a nested stmt
   */
  int end_nested_stmt(ObTransDesc& trans_desc, const ObPartitionArray& participants, const bool is_rollback);
  int start_cursor_stmt(ObTransDesc& trans_desc, const int64_t cur_stmt_expired_time);
  /*
   * get snapshot for read-only single stmt transaction
   */
  int get_stmt_snapshot_info(ObTransDesc& desc, const int64_t specified_snapshot_version);
  int replay(const common::ObPartitionKey& partition, const char* log, const int64_t size, const int64_t timestamp,
      const uint64_t log_id, const int64_t save_slave_read_timestamp, const bool batch_committed,
      const int64_t checkpoint, int64_t& log_table_version);
  int handle_trans_msg(const ObTransMsg& msg, obrpc::ObTransRpcResult& result);
  int handle_trans_response(const ObTransMsg& msg);
  int handle_trans_msg_callback(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t msg_type,
      const int status, const ObAddr& addr, const int64_t sql_no, const int64_t request_id);
  int handle_trx_req(int type, common::ObPartitionKey& pkey, const char* buf, int32_t size);
  int refresh_location_cache(const common::ObPartitionKey& partition, const bool need_clear_cache);
  int leader_revoke(const common::ObPartitionKey& partition);
  int leader_takeover(
      const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg, const int64_t checkpoint = 0);
  int leader_active(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg);
  int add_partition(const common::ObPartitionKey& partition);
  int block_partition(const common::ObPartitionKey& partition, bool& is_all_trans_clear);
  int kill_all_trans(const common::ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear);
  int calculate_trans_cost(const ObTransID& tid, uint64_t& cost);
  int wait_all_trans_clear(const common::ObPartitionKey& partition);
  int check_all_trans_in_trans_table_state(const common::ObPartitionKey& partition);
  int wait_1pc_trx_end(const common::ObPartitionKey& partition);
  int remove_partition(const common::ObPartitionKey& partition, const bool graceful);

  /*
   * mark transaction dirty which cross multiple memtable
   */
  int mark_dirty_trans(const common::ObPartitionKey& pkey, const memtable::ObMemtable* const frozen_memtable,
      const memtable::ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts);

  int get_applied_log_ts(const common::ObPartitionKey& pkey, int64_t& applied_log_ts);

  /*
   * return code:
   * OB_SUCCESS
   * OB_TRANS_KILLED
   * OB_TRANS_CTX_NOT_EXIST
   * OB_TRANS_TIMEOUT
   * OB_TRANS_STMT_TIMEOUT
   * OB_TRANS_NEED_ROLLBACK
   * OB_NOT_MASTER
   * OB_PARTITION_IS_FRONZE
   * OB_PARTITION_NOT_EXIST
   * OB_TRANS_IS_EXITING
   *
   * other error:
   * OB_NOT_INIT
   * OB_INIT_TWICE
   * OB_ERR_UNEXPECTED
   * OB_TRANS_CTX_ALLOC_ERROR
   */
  int get_store_ctx(
      const ObTransDesc& trans_desc, const common::ObPartitionKey& partition, storage::ObStoreCtx& store_ctx);
  int revert_store_ctx(
      const ObTransDesc& trans_desc, const common::ObPartitionKey& partition, storage::ObStoreCtx& store_ctx);
  int check_schema_version_elapsed(const ObPartitionKey& partition, const int64_t schema_version,
      const int64_t refreshed_schema_ts, int64_t& max_commit_version);
  int check_ctx_create_timestamp_elapsed(const common::ObPartitionKey& partition, const int64_t ts);
  /*
   * clean all partition ctx in replay state, used for replica state transfer from 'F' to 'L'
   */
  int clear_all_ctx(const common::ObPartitionKey& partition);
  int get_publish_version(const common::ObPartitionKey& pkey, int64_t& publish_version);
  int get_max_trans_version(const common::ObPartitionKey& pkey, int64_t& max_trans_version);
  int update_publish_version(
      const common::ObPartitionKey& partition, const int64_t publish_version, const bool for_replay);
  int get_min_uncommit_prepare_version(const ObPartitionKey& partition, int64_t& min_prepare_version);
  int get_min_uncommit_log(const ObPartitionKey& pkey, uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts);
  int get_min_prepare_version(const ObPartitionKey& partition, const int64_t log_ts, int64_t& min_prepare_version);
  int gc_trans_result_info(const ObPartitionKey& pkey, const int64_t checkpoint_ts);
  // get partition iterator
  int iterate_partition(ObPartitionIterator& partition_iter);
  int iterate_partition_mgr_stat(ObTransPartitionMgrStatIterator& partition_mgr_stat_iter);
  // get transaction stat iterator by partition
  int iterate_trans_stat(const common::ObPartitionKey& partition, ObTransStatIterator& trans_stat_iter);
  int print_all_trans_ctx(const common::ObPartitionKey& partition);
  // get the memory used condition of transaction module
  int iterate_trans_memory_stat(ObTransMemStatIterator& mem_stat_iter);
  // get transaction lock stat iterator by partition
  int iterate_trans_lock_stat(const common::ObPartitionKey& partition, ObTransLockStatIterator& trans_lock_stat_iter);
  int iterate_trans_result_info_in_TRIM(const common::ObPartitionKey& partition, ObTransResultInfoStatIterator& iter);

  int get_partition_audit_info(const common::ObPartitionKey& pkey, ObPartitionAuditInfo& info);
  int set_partition_audit_base_row_count(const ObPartitionKey& pkey, const int64_t count);
  int iterate_duplicate_partition_stat(const common::ObPartitionKey& partition, ObDuplicatePartitionStatIterator& iter);
  int iterate_trans_table(
      const ObPartitionKey& pg_key, const uint64_t end_log_id, blocksstable::ObMacroBlockWriter& writer);
  int update_dup_table_partition_info(const ObPartitionKey& pkey, const bool is_duplicated);
  int send_dup_table_lease_request_msg(const ObPartitionKey& pkey, ObDupTableLeaseTask* task);
  // savepoint
  int savepoint(ObTransDesc& trans_desc, const common::ObString& sp_id);
  int rollback_savepoint(ObTransDesc& trans_desc, const common::ObString& sp_id, const ObStmtParam& stmt_param);
  int release_savepoint(ObTransDesc& trans_desc, const common::ObString& sp_id);
  int xa_rollback_all_changes(ObTransDesc& trans_desc, const ObStmtParam& stmt_param);
  // elr statistic
  int dump_elr_statistic();
  int set_last_restore_log_id(const ObPartitionKey& pkey, const uint64_t last_restore_log_id);
  int set_restore_snapshot_version(const ObPartitionKey& pkey, const int64_t restore_snapshot_version);
  int update_restore_replay_info(
      const ObPartitionKey& partition, const int64_t restore_snapshot_version, const uint64_t last_restore_log_id);
  int xa_start_v2(
      const ObXATransID& xid, const int64_t flags, const int64_t xa_end_timeout_seconds, ObTransDesc& trans_desc);
  int xa_end_v2(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc);
  int xa_prepare_v2(const ObXATransID& xid, const uint64_t tenant_id, const int64_t stmt_expired_time);
  int local_xa_prepare(const ObXATransID& xid, const int64_t stmt_expired_time, ObScheTransCtx* sche_ctx);
  int xa_end_trans(const ObXATransID& xid, const bool is_rollback, ObTransDesc& trans_desc);
  int get_xa_trans_state(int32_t& state, ObTransDesc& trans_desc);
  int query_xa_state_and_flag(const uint64_t tenant_id, const ObXATransID& xid, int64_t& state, int64_t& end_flag);
  int update_xa_state(const uint64_t tenant_id, const int64_t state, const ObXATransID& xid, const bool one_phase,
      int64_t& affected_rows);
  int update_xa_state_and_flag(
      const uint64_t tenant_id, const int64_t state, const int64_t end_flag, const ObXATransID& xid);
  int query_xa_coordinator_trans_id(const uint64_t tenant_id, const ObXATransID& xid, ObPartitionKey& coordinator,
      ObTransID& trans_id, int64_t& state, bool& is_readonly, int64_t& end_flag);
  int query_xa_coordinator(const uint64_t tenant_id, const ObXATransID& xid, ObPartitionKey& coordinator,
      bool& is_readonly, int64_t& end_flag);
  int update_coordinator_and_state(const uint64_t tenant_id, const ObXATransID& xid, const ObPartitionKey& coordinator,
      const int64_t state, const bool is_readonly, int64_t& affected_rows);
  int query_xa_scheduler_trans_id(const uint64_t tenant_id, const ObXATransID& xid, ObAddr& scheduler_addr,
      ObTransID& trans_id, int64_t& state, int64_t& end_flag);
  int insert_xa_lock(ObISQLClient& client, const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id);
  int insert_xa_record(const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id,
      const ObAddr& sche_addr, const int64_t flag);
  int delete_xa_record(const uint64_t tenant_id, const ObXATransID& xid);
  int delete_xa_record_state(
      const uint64_t tenant_id, const ObXATransID& xid, const int32_t state, int64_t& affected_rows);
  int delete_xa_branch(const uint64_t tenant_id, const ObXATransID& xid, const bool is_tightly_coupled);
  int delete_xa_all_tightly_branch(const uint64_t tenant_id, const ObXATransID& xid);
  int xa_end_trans_v2(const ObXATransID& xid, const bool is_rollback, const int64_t flags, ObTransDesc& trans_desc);
  int gc_invalid_xa_record(const uint64_t tenant_id);

  int remove_callback_for_uncommited_txn(memtable::ObMemtable* mt);
  int remove_mem_ctx_for_trans_ctx(memtable::ObMemtable* mt);
  int mark_trans_forbidden_sql_no(
      const ObTransID& trans_id, const common::ObPartitionKey& partition, const int64_t sql_no, bool& forbid_succ);
  int is_trans_forbidden_sql_no(
      const ObTransID& trans_id, const common::ObPartitionKey& partition, const int64_t sql_no, bool& is_forbidden);
  const common::ObAddr& get_server()
  {
    return self_;
  }
  ObTransTimer& get_trans_timer()
  {
    return timer_;
  }
  ObITransRpc* get_trans_rpc()
  {
    return rpc_;
  }
  ObIDupTableRpc* get_dup_table_rpc()
  {
    return &dup_table_rpc_;
  }
  ObILocationAdapter* get_location_adapter()
  {
    return location_adapter_;
  }
  ObScheTransCtxMgr& get_sche_trans_ctx_mgr()
  {
    return sche_trans_ctx_mgr_;
  }
  ObCoordTransCtxMgr& get_coord_trans_ctx_mgr()
  {
    return coord_trans_ctx_mgr_;
  }
  ObPartTransCtxMgr& get_part_trans_ctx_mgr()
  {
    return part_trans_ctx_mgr_;
  }
  ObIClogAdapter* get_clog_adapter()
  {
    return clog_adapter_;
  }
  ObTransMigrateWorker* get_trans_migrate_worker()
  {
    return trans_migrate_worker_;
  }
  memtable::ObMemtableCtxFactory* get_memtable_ctx_factory()
  {
    return &mt_ctx_factory_;
  }
  storage::ObPartitionService* get_partition_service()
  {
    return partition_service_;
  }
  common::ObMySQLProxy* get_mysql_proxy()
  {
    return GCTX.sql_proxy_;
  }
  int inactive_tenant(const uint64_t tenant_id);
  int checkpoint(const common::ObPartitionKey& partition, const int64_t checkpoit_base_ts,
      storage::ObPartitionLoopWorker* lp_worker);
  int relocate_data(const ObPartitionKey& partition, memtable::ObIMemtable* memtable);
  bool is_running() const
  {
    return is_running_;
  }
  int handle_coordinator_orphan_msg(const ObTransMsg& msg, ObTransMsg& ret_msg);
  ObITsMgr* get_ts_mgr()
  {
    return ts_mgr_;
  }
  int register_prepare_changing_leader_task(
      const ObPartitionKey& partition, const ObAddr& proposal_leader, const int64_t ts);
  int prepare_changing_leader(
      const ObPartitionKey& partition, const ObAddr& proposal_leader, const int64_t round, const int64_t cnt);
  int check_scheduler_status(const ObPartitionKey& pkey);
  share::ObAliveServerTracer* get_server_tracer()
  {
    return server_tracer_;
  }
  int fetch_trans_ctx_by_ctx_id(const uint32_t ctx_id, ObTransCtx*& ctx);
  void revert_trans_ctx_by_ctx_id(const uint32_t ctx_id);
  int replay_start_working_log(const common::ObPartitionKey& pkey, const int64_t timestamp, const uint64_t log_id);
  int xa_scheduler_hb_req();
  int check_duplicated_partition(const ObPartitionKey& pkey, bool& is_duplicated_partition);
  share::schema::ObMultiVersionSchemaService* get_schema_service()
  {
    return schema_service_;
  }
  ObTransTaskWorker* get_big_trans_worker()
  {
    return &big_trans_worker_;
  }
  ObTransStatusMgr* get_trans_status_mgr()
  {
    return &trans_status_mgr_;
  }
  int get_max_trans_version_before_given_log_ts(
      const ObPartitionKey& pkey, const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans);
  int clear_unused_trans_status(const ObPartitionKey& pg_key, const int64_t max_cleanout_log_ts);
  virtual int has_terminated_trx_in_given_log_ts_range(
      const ObPartitionKey& pkey, const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx);
  ObPartTransSameLeaderBatchRpcMgr* get_part_trans_same_leader_batch_rpc_mgr()
  {
    return &part_trans_same_leader_batch_rpc_mgr_;
  }
  int flush_batch_clog_submit_wrapper(const ObPartitionKey& partition);
  int handle_batch_msg(const int type, const ObTrxMsgBase& base_msg);
  ObTransMsgHandler& get_trans_msg_handler()
  {
    return msg_handler_;
  }
  int kill_part_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id);
  int submit_log_for_split(const ObPartitionKey& partition, bool& log_finished);
  int copy_trans_table(const ObPartitionKey& partition, const ObIArray<ObPartitionKey>& dest_array);
  ObXARpc* get_xa_rpc()
  {
    return &xa_rpc_;
  }

private:
  int generate_transaction_snapshot_(ObTransDesc& trans_desc, int64_t& snapshot_version);
  int end_trans_(bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
      const int64_t stmt_expired_time, const MonotonicTs commit_time);
  void check_env_();
  bool need_handle_orphan_msg_(const int retcode, const ObTransMsg& msg);
  // orphan : a child who lost his or her parents
  int handle_orphan_msg_(const ObTransMsg& msg, const int ctx_ret);
  int post_trans_error_response_(const uint64_t tenant_id, ObTransMsg& msg, const ObAddr& server);
  int handle_trans_err_response_(const int64_t err_msg_type, const ObTransID& trans_id, const ObPartitionKey& partition,
      const ObAddr& sender_addr, const int status, const int64_t sql_no, const int64_t request_id);
  int end_trans_callback_(sql::ObIEndTransCallback& cb, const int cb_param, const uint64_t tenant_id);
  int init_memtable_ctx_(memtable::ObMemtableCtx* mem_ctx, const uint64_t tenant_id);

private:
  int recover_sche_ctx_(ObTransDesc& trans_desc, ObPartTransCtx* part_ctx);
  int handle_dist_start_stmt_(const ObStmtParam& stmt_param, const ObPartitionLeaderArray& pla,
      const ObStmtDesc& stmt_desc, ObTransDesc& trans_desc, ObPartitionArray& unreachable_partitions);
  int handle_end_stmt_(bool is_rollback, ObTransDesc& trans_desc);
  int handle_sp_trans_(const ObTransDesc& trans_desc, const ObPartitionArray& participants, const int64_t leader_epoch,
      storage::ObIPartitionArrayGuard& pkey_guard_arr);
  int handle_start_participant_(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
      const ObLeaderEpochArray& leader_epoch_arr, storage::ObIPartitionArrayGuard& pkey_guard_arr);
  int handle_end_participant_(bool is_rollback, const ObTransDesc& trans_desc, const ObPartitionArray& participants,
      const int64_t participant_cnt);
  int handle_sp_end_trans_(bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
      const int64_t stmt_expired_time, const MonotonicTs commit_time, bool& need_convert_to_dist_trans);
  int check_abort_participants_(ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx);
  int handle_sp_bounded_staleness_trans_(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
      storage::ObIPartitionArrayGuard& pkey_guard_arr);
  int handle_end_bounded_staleness_participant_(
      bool is_rollback, const ObTransDesc& trans_desc, const ObPartitionArray& participants);
  int check_stmt_participants_and_epoch_(const ObPartitionEpochArray& epoch_arr, ObPartitionArray& epoch_participants);
  bool check_is_multi_partition_update_stmt_(
      const ObTransDesc& trans_desc, const ObPartitionArray& paritions, const ObStmtDesc& stmt_desc);
  bool in_changing_leader_windows_(const int64_t ts, common::ObTsWindows& changing_leader_windows)
  {
    return changing_leader_windows.contain(ts);
  }
  bool can_create_ctx_(const int64_t trx_start_ts, const common::ObTsWindows& changing_leader_windows);
  int retry_trans_rpc_(const int64_t msg_type, const ObPartitionKey& partition, const ObTransID& trans_id,
      const int64_t request_id, const int64_t sql_no, const int64_t task_timeout);
  int handle_trans_ask_scheduler_status_request_(const ObTransMsg& msg, const int status);
  int query_xa_trans_(const ObXATransID& xid, const uint64_t tenant_id, ObPartitionKey& coordinator,
      ObTransID& trans_id, bool& is_xa_readonly);
  int xa_commit_(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc);
  int xa_rollback_(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc);
  int two_phase_rollback_(
      const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id, const ObTransDesc& trans_desc);
  int xa_recover_scheduler_v2_(const ObXATransID& xid, const ObPartitionKey& coordinator, const ObTransID& trans_id,
      const ObTransDesc& trans_desc, ObScheTransCtx*& sche_ctx);
  int prepare_scheduler_for_xa_start_resume_(const ObXATransID& xid, const ObTransID& trans_id,
      const ObAddr& scheduler_addr, ObTransDesc& trans_desc, const bool is_new_branch, const bool is_tightly_coupled);
  int xa_try_remote_lock_(ObTransDesc& trans_desc);
  int xa_release_remote_lock_(ObTransDesc& trans_desc);
  int xa_release_lock_(ObTransDesc& trans_desc);
  int handle_terminate_for_xa_branch_(ObTransDesc& trans_desc);
  int xa_init_sche_ctx_(ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx);
  int xa_start_local_resume_(const ObXATransID& xid, const ObTransID& trans_id, const ObAddr& scheduler_addr,
      const bool is_new_branch, const bool is_tightly_coupled, ObTransDesc& trans_desc);
  int xa_start_remote_resume_(const ObXATransID& xid, const ObTransID& trans_id, const ObAddr& scheduler_addr,
      const bool is_new_branch, const bool is_tightly_coupled, ObTransDesc& trans_desc);
  int clear_branch_for_xa_terminate_(
      ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx, const bool need_delete_xa_record);
  int set_trans_snapshot_version_for_serializable_(
      ObTransDesc& trans_desc, const int64_t stmt_snapshot_version, const bool is_stmt_snapshot_version_valid);

private:
  int check_and_handle_orphan_msg_(const int ret_code, const int64_t leader_epoch, const ObTransMsg& msg);
  int handle_participant_msg_(const ObTransMsg& msg, const common::ObPartitionKey& partition, const bool alloc);
  int handle_participant_bounded_staleness_msg_(const ObTransMsg& msg, const bool alloc);
  int set_replay_checkpoint_(const ObPartitionKey& pkey, const int64_t checkpoint);
  int handle_elr_callback_(const int64_t task_type, const ObPartitionKey& partition, const ObTransID& trans_id,
      const ObTransID& prev_trans_id, const int state);
  int handle_wait_trans_end_task_(const int64_t task_type, const ObPartitionKey& partition, const ObTransID& trans_id);
  int can_replay_redo_(
      const char* buf, const int64_t len, const bool is_xa_redo, ObPartTransCtx* part_ctx, bool& can_replay_redo);
  int can_replay_mutator_(const char* buf, const int64_t len, ObPartTransCtx* part_ctx, bool& can_replay_mutator);
  int can_replay_elr_log_(const int64_t tenant_id, bool& can_replay);
  bool multi_tenant_uncertain_phy_plan_(const ObStmtDesc& stmt_desc, const ObPartitionArray& participants);
  int handle_redo_sync_task_(ObDupTableRedoSyncTask* task, bool& need_release_task);
  bool verify_duplicate_partition_(const ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla);
  bool check_participant_epoch_exit_(const ObPartitionKey& pkey, const ObPartitionEpochArray& epoch_arr);
  bool need_switch_to_dist_trans_(const ObTransDesc& trans_desc, const ObPartitionKey& cur_pkey,
      const ObPartitionLeaderArray& pla, const bool is_external_consistent);
  int check_user_specified_snapshot_version(
      const ObTransDesc& trans_desc, const int64_t user_specified_snapshot_version, bool& is_snapshot_valid);
  int check_user_specified_snapshot_version(
      const ObStandaloneStmtDesc& desc, const int64_t user_specified_snapshot_version);
  int check_bounded_staleness_read_version_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& pg_key);
  int check_and_set_trans_consistency_type_(ObTransDesc& trans_desc);
  int decide_read_snapshot_(const ObStmtParam& stmt_param, const ObPartitionLeaderArray& pla,
      const ObStmtDesc& stmt_desc, const bool is_external_consistent, ObTransDesc& trans_desc,
      ObPartitionArray& unreachable_partitions);
  int decide_read_snapshot_for_serializable_trans_(const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc,
      int32_t& snapshot_gene_type, int64_t& snapshot_version);
  int decide_statement_snapshot_for_current_read_(const ObPartitionLeaderArray& pla, const ObStmtDesc& stmt_desc,
      ObTransDesc& trans_desc, const bool is_external_consistent, int32_t& snapshot_gene_type,
      int64_t& snapshot_version);
  int decide_statement_snapshot_for_bounded_staleness_read_(const ObStmtParam& stmt_param,
      const ObPartitionLeaderArray& pla, const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc,
      ObPartitionArray& unreachable_partitions, int32_t& snapshot_gene_type, int64_t& snapshot_version);
  int check_snapshot_version_for_bounded_staleness_read_(int64_t& cur_read_snapshot, const ObStmtParam& stmt_param,
      const ObPartitionLeaderArray& pla, const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc,
      ObPartitionArray& unreachable_partitions);
  int get_gts_(int64_t& snapshot_version, MonotonicTs& receive_gts_ts, const int64_t trans_expired_time,
      const int64_t stmt_expired_time, const uint64_t tenant_id);
  int snapshot_gene_type_compat_with_cluster_before_2200_(const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc,
      const int32_t consistency_type, const int32_t read_snapshot_type, int32_t& snapshot_gene_type,
      int64_t& snapshot_version);
  int handle_start_participant_for_bounded_staleness_read_(const ObTransDesc& trans_desc,
      const ObPartitionArray& participants, storage::ObIPartitionArrayGuard& pkey_guard_arr);
  int decide_participant_snapshot_version_(const ObTransDesc& trans_desc, const common::ObPartitionKey& pkey,
      storage::ObIPartitionGroup* ob_partition, const int64_t user_specified_snapshot, int64_t& part_snapshot_version,
      const char* module = "start_participant");
  int generate_part_snapshot_for_bounded_staleness_read_(const common::ObPartitionKey& pkey,
      storage::ObIPartitionGroup& ob_partition, int64_t& part_snapshot_version,
      common::ObPartitionArray& unreachable_partitions);
  int generate_part_snapshot_for_bounded_staleness_read_(const common::ObPartitionKey& pkey,
      int64_t& part_snapshot_version, common::ObPartitionArray& unreachable_partitions);
  int generate_part_snapshot_for_current_read_(const bool can_elr, const bool is_readonly, const int64_t stmt_expired,
      const int64_t trans_expired, const bool is_not_create_ctx_participant, const common::ObPartitionKey& pkey,
      storage::ObIPartitionGroup& ob_partition, int64_t& part_snapshot_version);
  int generate_part_snapshot_for_current_read_(const bool can_elr, const bool is_readonly, const int64_t stmt_expired,
      const int64_t trans_expired, const bool is_not_create_ctx_participant, const common::ObPartitionKey& pkey,
      int64_t& part_snapshot_version);
  int handle_snapshot_for_read_only_participant_(const ObTransDesc& trans_desc, const common::ObPartitionKey& pg_key,
      const int64_t user_specified_snapshot_version, int64_t& snapshot_version);
  int get_partition_group_(
      const common::ObPartitionKey& pkey, storage::ObIPartitionGroupGuard& guard, storage::ObIPartitionGroup*& pg);
  int check_snapshot_after_start_stmt_(const ObTransDesc& trans_desc);
  int get_gts_for_snapshot_version_(ObTransDesc& trans_desc, int64_t& snapshot_version,
      const int64_t trans_expired_time, const int64_t stmt_expire_time, const uint64_t tenant_id);
  int decide_trans_type_(ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc, const ObPartitionLeaderArray& pla,
      const bool is_external_consistent);
  int decide_trans_type_for_current_read_(ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc,
      const ObPartitionLeaderArray& pla, const bool is_external_consistent);
  int decide_trans_type_for_bounded_staleness_read_(
      ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc, const ObPartitionLeaderArray& pla);
  int convert_sp_trans_to_dist_trans_(ObTransDesc& trans_desc);
  int check_snapshot_for_start_stmt_(const ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla);
  memtable::ObMemtableCtx* alloc_tc_memtable_ctx_();
  int alloc_memtable_ctx_(const common::ObPartitionKey& pg_key, const bool is_fast_select, const uint64_t tenant_id,
      memtable::ObMemtableCtx*& mt_ctx);
  void release_memtable_ctx_(const common::ObPartitionKey& pg_key, memtable::ObMemtableCtx* mt_ctx);
  int handle_start_stmt_request_(const ObTransMsg& msg);
  int do_savepoint_rollback_(
      ObTransDesc& trans_desc, const int64_t sql_no, const common::ObPartitionArray& rollback_partitions);
  int check_duplicate_partition_status_(const ObPartitionKey& partition, int& status);
  int handle_batch_msg_(const int type, const char* buf, const int32_t size);
  int check_partition_status_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& partition, bool& is_dup_table,
      int64_t& leader_epoch, storage::ObIPartitionGroup* pg);
  int get_store_ctx_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& pg_key,
      const int64_t user_specified_snapshot, storage::ObStoreCtx& store_ctx);
  int revert_store_ctx_(const ObStandaloneStmtDesc& desc, const common::ObPartitionKey& partition,
      storage::ObStoreCtx& store_ctx, ObPartitionTransCtxMgr* part_mgr);

  int do_dist_rollback_(
      ObTransDesc& trans_desc, const int64_t sql_no, const common::ObPartitionArray& rollback_partitions);
  int alloc_tmp_sche_ctx_(ObTransDesc& trans_desc, bool& use_tmp_sche_ctx);
  void free_tmp_sche_ctx_(ObTransDesc& trans_desc);

private:
  static const int64_t END_STMT_MORE_TIME_US = 100 * 1000;
  // max task count in message process queue
  static const int64_t MAX_MSG_TASK = 10 * 1000 * 1000;
  static const int64_t MINI_MODE_MAX_MSG_TASK = 1 * 1000 * 1000;
  static const int64_t MAX_BIG_TRANS_WORKER = 8;
  static const int64_t MAX_BIG_TRANS_TASK = 100 * 1000;
  // max time bias between any two machine
  static const int64_t MAX_TIME_INTERVAL_BETWEEN_MACHINE_US = 200 * 1000;
  static const int64_t CHANGING_LEADER_TXN_PER_ROUND = 200;

public:
  // send lease renew request interval for duplicated table partition
  static const int64_t DUP_TABLE_LEASE_INTERVAL_US = 1 * 1000 * 1000;  // 1s
  // default duplicated table partition lease timeout
  static const int64_t DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US = 11 * 1000 * 1000;  // 11s
  static const int64_t DUP_TABLE_LEASE_START_RENEW_INTERVAL_US = 4 * 1000 * 1000;       // 4s
protected:
  bool is_inited_;
  bool is_running_;
  // for ObTransID
  common::ObAddr self_;
  ObTransBatchRpc rpc_def_;
  // ObTransRpc rpc_def_;
  ObLocationAdapter location_adapter_def_;
  ObClogAdapter clog_adapter_def_;
  ObTransMigrateWorker trans_migrate_worker_def_;
  ObXATransHeartbeatWorker xa_trans_heartbeat_worker_;
  ObXAInnerTableGCWorker xa_inner_table_gc_worker_;
  // memtable context factory
  memtable::ObMemtableCtxFactory mt_ctx_factory_;
  // transaction timer
  ObTransTimer timer_;
  // dup table lease timer
  ObDupTableLeaseTimer dup_table_lease_timer_;

protected:
  bool use_def_;
  // transaction communication on rpc
  ObITransRpc* rpc_;
  ObDupTableRpc dup_table_rpc_;
  // the adapter between transaction and location cache
  ObILocationAdapter* location_adapter_;
  ObTransMigrateWorker* trans_migrate_worker_;
  // the adapter between transaction and clog
  ObIClogAdapter* clog_adapter_;
  storage::ObPartitionService* partition_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

private:
  // scheduler transaction context manager
  ObScheTransCtxMgr sche_trans_ctx_mgr_;
  // coordinator transaction context manager
  ObCoordTransCtxMgr coord_trans_ctx_mgr_;
  // participant transaction context manager
  ObPartTransCtxMgr part_trans_ctx_mgr_;
  // for slave read
  ObPartTransCtxMgr slave_part_trans_ctx_mgr_;
  ObITsMgr* ts_mgr_;
  // server alive tracker
  share::ObAliveServerTracer* server_tracer_;
  // account task qeuue's inqueue and dequeue
  uint32_t input_queue_count_;
  uint32_t output_queue_count_;
  ObDupTableLeaseTaskMap dup_table_lease_task_map_;
  ObTransTaskWorker big_trans_worker_;
  // transaction status
  ObTransStatusMgr trans_status_mgr_;
  ObPartTransSameLeaderBatchRpcMgr part_trans_same_leader_batch_rpc_mgr_;
  ObTransMsgHandler msg_handler_;
  ObXARpc xa_rpc_;
  ObLightTransCtxMgr light_trans_ctx_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransService);
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_
