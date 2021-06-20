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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_SERVICE
#define OCEANBASE_STORAGE_OB_PARTITION_SERVICE

#include "lib/hash/ob_linear_hash_map.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/queue/ob_dedup_queue.h"
#include "share/stat/ob_stat_manager.h"
#include "share/ob_locality_info.h"
#include "share/ob_locality_info.h"
#include "share/ob_build_index_struct.h"
#include "share/ob_partition_modify.h"
#include "share/rpc/ob_batch_rpc.h"
#include "common/ob_i_rs_cb.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/ob_migrate_retry_queue_thread.h"
#include "storage/ob_callback_queue_thread.h"
#include "storage/ob_slog_writer_queue_thread.h"
#include "storage/ob_saved_storage_info.h"
#include "storage/ob_locality_manager.h"
#include "storage/ob_storage_log_type.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_log_rpc_proxy.h"
#include "clog/ob_i_submit_log_cb.h"
#include "clog/ob_log_replay_engine_wrapper.h"
#include "clog/ob_clog_aggre_runnable.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_rpc.h"
#include "sql/ob_end_trans_callback.h"
#include "ob_warm_up.h"
#include "ob_partition_component_factory.h"
#include "ob_partition_split_worker.h"
#include "ob_partition_worker.h"
#include "ob_trans_checkpoint_worker.h"
#include "storage/ob_garbage_collector.h"
#include "storage/ob_pg_index.h"
#include "storage/ob_pg_mgr.h"
#include "storage/gts/ob_ha_gts_mgr.h"
#include "storage/gts/ob_ha_gts_source.h"
#include "storage/ob_rebuild_scheduler.h"
#include "storage/ob_dup_replica_checker.h"
#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_freeze_async_task.h"
#include "storage/ob_sstable_garbage_collector.h"
#include "storage/ob_server_checkpoint_log_reader.h"
#include "storage/ob_auto_part_scheduler.h"
#include "storage/ob_clog_cb_async_worker.h"
#include "lib/container/ob_array_array.h"
#include "storage/ob_partition_group_create_checker.h"

namespace oceanbase {
namespace blocksstable {
class ObStorageEnv;
}

namespace share {
class ObSplitPartition;
class ObSplitPartitionPair;
}  // namespace share

namespace election {
class ObIElectionMgr;
}

namespace transaction {
enum ObPartitionAuditOperator;
}

namespace storage {
class ObPartitionService;
class ObAddPartitionToPGLog;

// tenant-related information in the storage layer
struct ObTenantStorageInfo {
  ObTenantStorageInfo() : part_cnt_(0), pg_cnt_(0)
  {}
  int64_t part_cnt_;
  int64_t pg_cnt_;
};

struct ObPartitionMigrationDataStatics {
  ObPartitionMigrationDataStatics();
  virtual ~ObPartitionMigrationDataStatics() = default;
  void reset();
  int64_t total_macro_block_;
  int64_t ready_macro_block_;
  int64_t major_count_;
  int64_t mini_minor_count_;
  int64_t normal_minor_count_;
  int64_t buf_minor_count_;
  int64_t reuse_count_;
  int64_t partition_count_;
  int64_t finish_partition_count_;
  // no need reset, cause has inner retry
  int64_t input_bytes_;
  int64_t output_bytes_;

  TO_STRING_KV(K_(total_macro_block), K_(ready_macro_block), K_(major_count), K_(mini_minor_count),
      K_(normal_minor_count), K_(buf_minor_count), K_(reuse_count), K_(partition_count), K_(finish_partition_count),
      K_(input_bytes), K_(output_bytes));
};

struct ObPartMigrationRes {
  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  share::ObPhysicalBackupArg backup_arg_;
  share::ObPhysicalValidateArg validate_arg_;
  ObPartitionMigrationDataStatics data_statics_;
  int64_t quorum_;
  int32_t result_;

  ObPartMigrationRes()
      : key_(),
        src_(),
        dst_(),
        data_src_(),
        backup_arg_(),
        validate_arg_(),
        data_statics_(),
        quorum_(-1),
        result_(OB_ERROR)
  {}
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(data_src), K_(backup_arg), K_(validate_arg), K_(data_statics), K_(quorum),
      K_(result));
};

class ObRestoreInfo {
public:
  typedef common::hash::ObHashMap<uint64_t, common::ObArray<blocksstable::ObSSTablePair>*,
      common::hash::NoPthreadDefendMode>
      SSTableInfoMap;
  ObRestoreInfo();
  virtual ~ObRestoreInfo();
  int init(const share::ObRestoreArgs& restore_args);
  bool is_inited() const
  {
    return is_inited_;
  }
  int add_sstable_info(const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& block_list);
  int get_backup_block_info(const uint64_t index_id, const int64_t macro_idx, uint64_t& backup_index_id,
      blocksstable::ObSSTablePair& backup_block_pair) const;
  const share::ObRestoreArgs& get_restore_args() const
  {
    return arg_;
  }
  TO_STRING_KV(K_(arg));

private:
  bool is_inited_;
  share::ObRestoreArgs arg_;
  SSTableInfoMap backup_sstable_info_map_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreInfo);
};

enum ObChangeMemberOption : int8_t {
  NORMAL_CHANGE_MEMBER_LIST = 0,
  SKIP_CHANGE_MEMBER_LIST,
};

struct ObReplicaOpArg {
  const static int64_t RESTORE_VERSION_0 = 0;  // old storage file format before 2.2.30
  const static int64_t RESTORE_VERSION_1 = 1;  // new storage file format
  common::ObPartitionKey key_;                 // TODO() change it to pg key
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  ObReplicaOpType type_;
  int64_t quorum_;
  common::ObVersion base_version_;
  share::ObRestoreArgs restore_arg_;
  share::ObPhysicalBackupArg backup_arg_;
  share::ObPhysicalRestoreArg phy_restore_arg_;
  share::ObPhysicalValidateArg validate_arg_;
  uint64_t index_id_;
  ObReplicaOpPriority priority_;
  int64_t cluster_id_;
  int64_t restore_version_;  // ObRestoreArgs:0, ObPhysicalRestoreArg:1
  ObChangeMemberOption change_member_option_;
  int64_t switch_epoch_;

  ObReplicaOpArg()
      : key_(),
        src_(),
        dst_(),
        data_src_(),
        type_(UNKNOWN_REPLICA_OP),
        quorum_(0),
        base_version_(),
        restore_arg_(),
        backup_arg_(),
        phy_restore_arg_(),
        validate_arg_(),
        index_id_(OB_INVALID_ID),
        priority_(ObReplicaOpPriority::PRIO_INVALID),
        cluster_id_(OB_INVALID_CLUSTER_ID),
        restore_version_(RESTORE_VERSION_0),
        change_member_option_(NORMAL_CHANGE_MEMBER_LIST),
        switch_epoch_(OB_INVALID_VERSION)
  {}

  bool is_valid() const
  {
    return (is_replica_op_valid(type_) && key_.is_valid() && (VALIDATE_BACKUP_OP == type_ || dst_.is_valid()) &&
               ((REMOVE_REPLICA_OP == type_ || RESTORE_REPLICA_OP == type_ || BACKUP_REPLICA_OP == type_ ||
                    RESTORE_STANDBY_OP == type_) ||
                   VALIDATE_BACKUP_OP == type_ || (src_.is_valid() && data_src_.is_valid())) &&
               (COPY_LOCAL_INDEX_OP != type_ || OB_INVALID_ID != index_id_)) &&
           is_replica_op_priority_valid(priority_) &&
           (NORMAL_CHANGE_MEMBER_LIST == change_member_option_ || ADD_REPLICA_OP == type_ ||
               MIGRATE_REPLICA_OP == type_ || REBUILD_REPLICA_OP == type_ || FAST_MIGRATE_REPLICA_OP == type_ ||
               CHANGE_REPLICA_OP == type_ || RESTORE_STANDBY_OP == type_);
  }
  void reset();
  bool is_physical_restore() const;
  bool is_physical_restore_leader() const;
  bool is_physical_restore_follower() const;
  bool is_standby_restore() const;
  const char* get_replica_op_type_str() const;
  TO_STRING_KV(K_(key), K_(dst), K_(src), K_(data_src), K_(quorum), "type", get_replica_op_type_str(), K_(base_version),
      K_(restore_arg), K_(validate_arg), K_(backup_arg), K_(phy_restore_arg), K_(index_id), K_(priority),
      K_(cluster_id), K_(restore_version), K_(change_member_option), K_(switch_epoch));
};

struct ObMigrateSrcInfo {
  ObMigrateSrcInfo() : src_addr_(), cluster_id_(-1)
  {}
  virtual ~ObMigrateSrcInfo() = default;
  bool is_valid() const
  {
    return src_addr_.is_valid() && -1 != cluster_id_;
  }

  void reset()
  {
    src_addr_.reset();
    cluster_id_ = -1;
  }

  uint64_t hash() const
  {
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&cluster_id_, sizeof(cluster_id_), hash_value);
    hash_value += src_addr_.hash();
    return hash_value;
  }
  bool operator==(const ObMigrateSrcInfo& src_info) const
  {
    return src_addr_ == src_info.src_addr_ && cluster_id_ == src_info.cluster_id_;
  }

  TO_STRING_KV(K_(src_addr), K_(cluster_id));
  common::ObAddr src_addr_;
  int64_t cluster_id_;
};

#define VIRTUAL_FOR_UNITTEST virtual

class ObPartitionService : public share::ObIPSCb,
                           public common::ObIDataAccessService,
                           public ObPartitionMetaRedoModule,
                           public common::ObTableStatDataService {
public:
  static const int64_t MC_WAIT_INTERVAL =
      200 * 1000;  // 200ms the interval of checking the completion of member change log syncing
  static const int64_t MC_SLEEP_TIME = 100000;                        // 100ms
  static const int64_t MC_WAIT_TIMEOUT = 6000000;                     // 6s the timeout for changing leader
  static const int64_t MC_TASK_TIMEOUT = 10000000;                    // 10s the timeout for writing member change log
  static const int64_t FETCH_LOG_TIMEOUT = 1000L * 1000L * 60L;       // 1min the timeout for catch up log
  static const int64_t GARBAGE_CLEAN_INTERVAL = 1000L * 1000L * 10L;  // 10s
  static const int64_t PURGE_RETIRE_MEMSTORE_INTERVAL = 1000L * 1000L * 5L;
  static const int64_t CLOG_REQUIRED_MINOR_FREEZE_INTERVAL = 1000L * 1000L * 20L;  // 20s
  static const int64_t RELOAD_LOCALITY_INTERVAL = 10 * 1000 * 1000L;               // 10S
  static const int64_t CREATE_TABLE_INTERVAL = 1000L * 1000L * 15L;                // 15s
  static const int64_t REFRESH_SCHEMA_INTERVAL = 600L * 1000L * 1000L;             // 10min
  static const int64_t OFFLINE_PARTITION_WAIT_INTERVAL = 2L * 1000L;               // 2ms
  static const int64_t MAX_RETRY_TIMES = 3;                                  // retry post result of migrate task to rs
  static const int64_t MC_INTERVAL_BEFORE_LEASE_EXPIRE = 2 * 1000 * 1000;    // 2s
  static const int64_t MIGRATE_RETRY_TIME_INTERVAL = 1 * 60 * 1000 * 1000L;  // retry interval for rebuild
  // estimated memory limit for single partition
  // 100kb(static) + 400kb(dynamic)/10 (assuming that one-tenth of the partition is active)
  static const int64_t SINGLE_PART_STATIC_MEM_COST = 100 * 1024L;
  static const int64_t SINGLE_PART_DYNAMIC_MEM_COST = 400 * 1024L;
  // estimated memory limit for single partition group
  static const int64_t SINGLE_PG_DYNAMIC_MEM_COST = SINGLE_PART_DYNAMIC_MEM_COST + 1 * 1024 * 1024L;
  static const int64_t TENANT_PART_NUM_THRESHOLD = 1000;
  static constexpr const char RPScanIteratorLabel[] = "RPScanIterator";

public:
  ObPartitionService();
  virtual ~ObPartitionService();
  static OB_INLINE ObPartitionService& get_instance();

public:
  virtual int init(const blocksstable::ObStorageEnv& env, const common::ObAddr& self_addr,
      ObPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
      share::ObIPartitionLocationCache* location_cache, share::ObRsMgr* rs_mgr, storage::ObIPartitionReport* rs_cb,
      rpc::frame::ObReqTransport* req_transport, obrpc::ObBatchRpc* batch_rpc, obrpc::ObCommonRpcProxy& rs_rpc_proxy,
      obrpc::ObSrvRpcProxy& srv_rpc_proxy, common::ObMySQLProxy& sql_proxy, share::ObRemoteSqlProxy& remote_sql_proxy,
      share::ObAliveServerTracer& server_tracer, common::ObInOutBandwidthThrottle& bandwidth_throttle);
  VIRTUAL_FOR_UNITTEST int start();
  VIRTUAL_FOR_UNITTEST int stop();
  VIRTUAL_FOR_UNITTEST int wait();
  VIRTUAL_FOR_UNITTEST int destroy();

  VIRTUAL_FOR_UNITTEST int wait_start_finish();

  VIRTUAL_FOR_UNITTEST int reload_config();

  VIRTUAL_FOR_UNITTEST int batch_check_leader_active(
      const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result);
  VIRTUAL_FOR_UNITTEST int batch_get_protection_level(
      const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result);
  VIRTUAL_FOR_UNITTEST int primary_process_protect_mode_switch();
  VIRTUAL_FOR_UNITTEST int standby_update_replica_protection_level();
  VIRTUAL_FOR_UNITTEST int create_batch_partition_groups(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res);
  VIRTUAL_FOR_UNITTEST int create_batch_pg_partitions(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res);
  VIRTUAL_FOR_UNITTEST int create_new_partition(const common::ObPartitionKey& key, ObIPartitionGroup*& partition);
  VIRTUAL_FOR_UNITTEST int add_new_partition(ObIPartitionGroupGuard& partition_guard);
  VIRTUAL_FOR_UNITTEST int log_new_partition(ObIPartitionGroup* partition, const int64_t publish_version);
  VIRTUAL_FOR_UNITTEST int remove_partition(const common::ObPartitionKey& key, const bool write_slog = true);
  VIRTUAL_FOR_UNITTEST int remove_partition_from_pg(
      const bool for_replay, const ObPartitionKey& pg_key, const ObPartitionKey& pkey, const uint64_t log_id);
  VIRTUAL_FOR_UNITTEST int online_partition(const common::ObPartitionKey& pkey, const int64_t publish_version,
      const int64_t restore_snapshot_version, const uint64_t last_restore_log_id);
  // before building the index, wait for all transactions with lower schema version to finish
  // max_commit_version is the max commit version of those transactions
  VIRTUAL_FOR_UNITTEST int check_schema_version_elapsed(const ObPartitionKey& partition, const int64_t schema_version,
      const uint64_t index_id, int64_t& max_commit_version);

  VIRTUAL_FOR_UNITTEST int check_ctx_create_timestamp_elapsed(const ObPartitionKey& partition, const int64_t ts);

  // transaction service interface
  VIRTUAL_FOR_UNITTEST int start_trans(const uint64_t tenant_id, const uint64_t cluster_id,
      const transaction::ObStartTransParam& req, const int64_t expired_time, const uint32_t session_id,
      const uint64_t proxy_session_id, transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int end_trans(
      bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb, const int64_t expired_time);

  VIRTUAL_FOR_UNITTEST int start_stmt(const transaction::ObStmtParam& stmt_param, transaction::ObTransDesc& trans_desc,
      const common::ObPartitionLeaderArray& pla, common::ObPartitionArray& participants);
  virtual int half_stmt_commit(const transaction::ObTransDesc& trans_desc, const common::ObPartitionKey& partition);
  VIRTUAL_FOR_UNITTEST int end_stmt(bool is_rollback, bool is_incomplete,
      const ObPartitionArray& cur_stmt_all_participants, const transaction::ObPartitionEpochArray& epoch_arr,
      const ObPartitionArray& discard_participant, const ObPartitionLeaderArray& pla,
      transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int start_nested_stmt(transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int end_nested_stmt(
      transaction::ObTransDesc& trans_desc, const ObPartitionArray& participants, const bool is_rollback);
  VIRTUAL_FOR_UNITTEST int start_participant(transaction::ObTransDesc& trans_desc,
      const common::ObPartitionArray& participants, transaction::ObPartitionEpochArray& partition_epoch_arr);
  VIRTUAL_FOR_UNITTEST int end_participant(
      bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants);
  VIRTUAL_FOR_UNITTEST int kill_query_session(const transaction::ObTransDesc& trans_desc, const int status);
  VIRTUAL_FOR_UNITTEST int internal_kill_trans(transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int savepoint(transaction::ObTransDesc& trans_desc, const common::ObString& sp_name);
  VIRTUAL_FOR_UNITTEST int rollback_savepoint(transaction::ObTransDesc& trans_desc, const common::ObString& sp_name,
      const transaction::ObStmtParam& stmt_param);
  VIRTUAL_FOR_UNITTEST int release_savepoint(transaction::ObTransDesc& trans_desc, const common::ObString& sp_name);
  VIRTUAL_FOR_UNITTEST int mark_trans_forbidden_sql_no(const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partitions, const int64_t sql_no, bool& forbid_succ);
  VIRTUAL_FOR_UNITTEST int is_trans_forbidden_sql_no(const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partitions, const int64_t sql_no, bool& is_forbidden);
  VIRTUAL_FOR_UNITTEST int xa_rollback_all_changes(
      transaction::ObTransDesc& trans_desc, const transaction::ObStmtParam& stmt_param);
  // xa transactions interfaces
  VIRTUAL_FOR_UNITTEST int xa_start(const transaction::ObXATransID& xid, const int64_t flags,
      const int64_t xa_end_timeout_seconds, transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int xa_end(
      const transaction::ObXATransID& xid, const int64_t flags, transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int xa_prepare(
      const transaction::ObXATransID& xid, const uint64_t tenant_id, const int64_t stmt_expired_time);
  VIRTUAL_FOR_UNITTEST int xa_end_trans(const transaction::ObXATransID& xid, const bool is_rollback,
      const int64_t flags, transaction::ObTransDesc& trans_desc);
  VIRTUAL_FOR_UNITTEST int get_xa_trans_state(int32_t& state, transaction::ObTransDesc& trans_desc);
  // partition storage interfaces
  virtual int table_scan(ObVTableScanParam& vparam, common::ObNewRowIterator*& result) override;
  virtual int table_scan(ObVTableScanParam& vparam, common::ObNewIterIterator*& result) override;
  virtual int revert_scan_iter(common::ObNewRowIterator* iter) override;
  virtual int revert_scan_iter(common::ObNewIterIterator* iter) override;
  VIRTUAL_FOR_UNITTEST int delete_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  VIRTUAL_FOR_UNITTEST int delete_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row);
  VIRTUAL_FOR_UNITTEST int put_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  VIRTUAL_FOR_UNITTEST int insert_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  VIRTUAL_FOR_UNITTEST int insert_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row);
  VIRTUAL_FOR_UNITTEST int insert_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
      int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows);
  VIRTUAL_FOR_UNITTEST int fetch_conflict_rows(const transaction::ObTransDesc& trans_desc,
      const ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey,
      const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
      common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters);
  VIRTUAL_FOR_UNITTEST int revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter);
  VIRTUAL_FOR_UNITTEST int update_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  VIRTUAL_FOR_UNITTEST int update_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, const common::ObNewRow& old_row,
      const common::ObNewRow& new_row);
  VIRTUAL_FOR_UNITTEST int lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, common::ObNewRowIterator* row_iter,
      const ObLockFlag lock_flag, int64_t& affected_rows);
  VIRTUAL_FOR_UNITTEST int lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
      const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, const common::ObNewRow& row,
      const ObLockFlag lock_flag);

  virtual int join_mv_scan(
      ObTableScanParam& left_param, ObTableScanParam& right_param, common::ObNewRowIterator*& result) override;

  // partition manager interface
  int get_table_store_cnt(int64_t& table_cnt);
  VIRTUAL_FOR_UNITTEST void revert_replay_status(ObReplayStatus* replay_status) const;
  VIRTUAL_FOR_UNITTEST int get_gts(const uint64_t tenant_id, const transaction::MonotonicTs stc, int64_t& gts) const;
  VIRTUAL_FOR_UNITTEST int trigger_gts();

  VIRTUAL_FOR_UNITTEST ObPGPartitionIterator* alloc_pg_partition_iter();
  VIRTUAL_FOR_UNITTEST ObSinglePGPartitionIterator* alloc_single_pg_partition_iter();
  VIRTUAL_FOR_UNITTEST void revert_pg_partition_iter(ObIPGPartitionIterator* iter);
  VIRTUAL_FOR_UNITTEST bool is_empty() const;
  VIRTUAL_FOR_UNITTEST int get_partition_count(int64_t& partition_count) const;
  VIRTUAL_FOR_UNITTEST int halt_all_prewarming(uint64_t tenant_id = OB_INVALID_TENANT_ID);
  VIRTUAL_FOR_UNITTEST int get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const;

  // replay interfaces
  VIRTUAL_FOR_UNITTEST int replay_redo_log(const common::ObPartitionKey& pkey, const ObStoreCtx& ctx,
      const int64_t log_timestamp, const int64_t log_id, const char* buf, const int64_t size, bool& replayed);

  // partition service callback function
  VIRTUAL_FOR_UNITTEST int push_callback_task(const ObCbTask& task);
  VIRTUAL_FOR_UNITTEST int push_into_migrate_retry_queue(const common::ObPartitionKey& pkey, const int32_t task_type);
  virtual int on_leader_revoke(const common::ObPartitionKey& pkey, const common::ObRole& role) override;
  virtual int on_leader_takeover(const common::ObPartitionKey& pkey, const common::ObRole& role,
      const bool is_elected_by_changing_leader) override;
  virtual int on_leader_active(const common::ObPartitionKey& pkey, const common::ObRole& role,
      const bool is_elected_by_changing_leader) override;
  VIRTUAL_FOR_UNITTEST int internal_leader_revoke(const ObCbTask& revoke_task);
  VIRTUAL_FOR_UNITTEST int internal_leader_takeover(const ObCbTask& takeover_task);
  VIRTUAL_FOR_UNITTEST int internal_leader_takeover_bottom_half(const ObCbTask& takeover_task);
  VIRTUAL_FOR_UNITTEST int internal_leader_active(const ObCbTask& active_task);
  virtual int async_leader_revoke(const common::ObPartitionKey& pkey, const uint32_t revoke_type);
  virtual bool is_take_over_done(const common::ObPartitionKey& pkey) const override;
  virtual bool is_revoke_done(const common::ObPartitionKey& pkey) const override;
  virtual int submit_ms_info_task(const common::ObPartitionKey& pkey, const common::ObAddr& server,
      const int64_t cluster_id, const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp,
      const int64_t replica_num, const common::ObMemberList& prev_member_list,
      const common::ObMemberList& curr_member_list, const common::ObProposalID& ms_proposal_id);
  virtual int process_ms_info_task(ObMsInfoTask& task);
  virtual int on_member_change_success(const common::ObPartitionKey& pkey, const clog::ObLogType log_type,
      const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
      const common::ObProposalID& ms_proposal_id) override;
  // used only by clog
  // set is_need_renew to refresh location cache
  virtual int nonblock_get_strong_leader_from_loc_cache(const common::ObPartitionKey& pkey, ObAddr& leader,
      int64_t& cluster_id, const bool is_need_renew = false) override;

  virtual int nonblock_get_leader_by_election_from_loc_cache(const common::ObPartitionKey& pkey, int64_t cluster_id,
      ObAddr& leader, const bool is_need_renew = false) override;
  virtual int get_restore_leader_from_loc_cache(
      const common::ObPartitionKey& pkey, ObAddr& restore_leader, const bool is_need_renew);
  virtual int nonblock_get_leader_by_election_from_loc_cache(
      const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew = false) override;
  virtual int nonblock_get_strong_leader_from_loc_cache(
      const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew = false) override;
  virtual int handle_log_missing(const common::ObPartitionKey& pkey, const common::ObAddr& server) override;
  virtual int restore_standby_replica(
      const common::ObPartitionKey& pkey, const common::ObAddr& server, const int64_t cluster_id);
  virtual bool is_service_started() const override;
  virtual int get_global_max_decided_trans_version(int64_t& max_decided_trans_version) const override;
  VIRTUAL_FOR_UNITTEST int handle_split_dest_partition_request(
      const obrpc::ObSplitDestPartitionRequestArg& arg, obrpc::ObSplitDestPartitionResult& result);
  VIRTUAL_FOR_UNITTEST int handle_split_dest_partition_result(const obrpc::ObSplitDestPartitionResult& result);

  VIRTUAL_FOR_UNITTEST int handle_replica_split_progress_request(
      const obrpc::ObReplicaSplitProgressRequest& arg, obrpc::ObReplicaSplitProgressResult& result);
  VIRTUAL_FOR_UNITTEST int handle_replica_split_progress_result(const obrpc::ObReplicaSplitProgressResult& result);

  VIRTUAL_FOR_UNITTEST int set_global_max_decided_trans_version(const int64_t trans_version);
  VIRTUAL_FOR_UNITTEST const common::ObAddr& get_self_addr() const
  {
    return self_addr_;
  }

  // expose component
  VIRTUAL_FOR_UNITTEST transaction::ObTransService* get_trans_service();
  VIRTUAL_FOR_UNITTEST memtable::ObIMemtableCtxFactory* get_mem_ctx_factory();
  VIRTUAL_FOR_UNITTEST clog::ObICLogMgr* get_clog_mgr();
  VIRTUAL_FOR_UNITTEST election::ObIElectionMgr* get_election_mgr();
  ObRebuildReplicaService& get_rebuild_replica_service()
  {
    return rebuild_replica_service_;
  }
  VIRTUAL_FOR_UNITTEST ObPartitionSplitWorker* get_split_worker();
  VIRTUAL_FOR_UNITTEST clog::ObClogAggreRunnable* get_clog_aggre_runnable();
  VIRTUAL_FOR_UNITTEST int get_locality_info(share::ObLocalityInfo& locality_info);
  VIRTUAL_FOR_UNITTEST const ObLocalityManager* get_locality_manager() const
  {
    return &locality_manager_;
  }
  VIRTUAL_FOR_UNITTEST ObLocalityManager* get_locality_manager()
  {
    return &locality_manager_;
  }

  VIRTUAL_FOR_UNITTEST int replay(const ObPartitionKey& partition, const char* log, const int64_t size,
      const uint64_t log_id, const int64_t log_ts);
  VIRTUAL_FOR_UNITTEST int minor_freeze(const uint64_t tenant_id);
  VIRTUAL_FOR_UNITTEST int minor_freeze(
      const common::ObPartitionKey& pkey, const bool emergency = false, const bool force = false);
  VIRTUAL_FOR_UNITTEST int freeze_partition(
      const ObPartitionKey& pkey, const bool emergency, const bool force, int64_t& freeze_snapshot);
  VIRTUAL_FOR_UNITTEST int check_dirty_txn(const ObPartitionKey& pkey, const int64_t min_log_ts,
      const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty);
  // query accum_checksum and submit_timestamp with pkey and log id
  // could cost a few seconds
  VIRTUAL_FOR_UNITTEST int query_log_info_with_log_id(const ObPartitionKey& pkey, const uint64_t log_id,
      const int64_t timeout, int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id);

  // misc functions
  VIRTUAL_FOR_UNITTEST int get_dup_replica_type(
      const common::ObPartitionKey& pkey, const common::ObAddr& server, DupReplicaType& dup_replica_type);
  VIRTUAL_FOR_UNITTEST int get_dup_replica_type(
      const uint64_t table_id, const common::ObAddr& server, DupReplicaType& dup_replica_type);
  VIRTUAL_FOR_UNITTEST int get_replica_status(const common::ObPartitionKey& pkey, share::ObReplicaStatus& status) const;
  VIRTUAL_FOR_UNITTEST int get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const;
  VIRTUAL_FOR_UNITTEST int get_role_for_partition_table(const common::ObPartitionKey& pkey, common::ObRole& role) const;
  VIRTUAL_FOR_UNITTEST int get_role_unsafe(const common::ObPartitionKey& pkey, common::ObRole& role) const;
  VIRTUAL_FOR_UNITTEST int get_leader_curr_member_list(
      const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const;
  VIRTUAL_FOR_UNITTEST int get_curr_member_list(
      const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const;
  VIRTUAL_FOR_UNITTEST int get_curr_leader_and_memberlist(const common::ObPartitionKey& pkey, common::ObAddr& leader,
      common::ObRole& role, common::ObMemberList& member_list, common::ObChildReplicaList& children_list,
      common::ObReplicaType& replica_type, common::ObReplicaProperty& property) const;
  VIRTUAL_FOR_UNITTEST int get_dst_leader_candidate(
      const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const;
  VIRTUAL_FOR_UNITTEST int get_dst_candidates_array(const common::ObPartitionIArray& pkey_array,
      const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_list_array,
      common::ObSArray<obrpc::CandidateStatusList>& candidate_status_array) const;
  VIRTUAL_FOR_UNITTEST int get_leader(const common::ObPartitionKey& pkey, common::ObAddr& leader) const;
  VIRTUAL_FOR_UNITTEST int change_leader(const common::ObPartitionKey& pkey, const common::ObAddr& leader);
  VIRTUAL_FOR_UNITTEST int batch_change_rs_leader(const common::ObAddr& leader);
  VIRTUAL_FOR_UNITTEST int auto_batch_change_rs_leader();
  VIRTUAL_FOR_UNITTEST bool is_partition_exist(const common::ObPartitionKey& pkey) const;
  VIRTUAL_FOR_UNITTEST bool is_scan_disk_finished();
  //  VIRTUAL_FOR_UNITTEST void garbage_clean();
  VIRTUAL_FOR_UNITTEST int activate_tenant(const uint64_t tenant_id);
  VIRTUAL_FOR_UNITTEST int inactivate_tenant(const uint64_t tenant_id);
  virtual bool is_tenant_active(const common::ObPartitionKey& pkey) const override;
  virtual bool is_tenant_active(const uint64_t tenant_id) const override;
  VIRTUAL_FOR_UNITTEST ObIPartitionComponentFactory* get_cp_fty();

  VIRTUAL_FOR_UNITTEST int get_server_locality_array(
      common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone) const;
  virtual int get_server_region_across_cluster(const common::ObAddr& server, common::ObRegion& region) const;

  virtual int get_server_region(const common::ObAddr& server, common::ObRegion& region) const override;
  virtual int record_server_region(const common::ObAddr& server, const common::ObRegion& region) override;
  virtual int get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const override;
  virtual int record_server_idc(const common::ObAddr& server, const common::ObIDC& idc) override;
  virtual int get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const override;
  virtual int record_server_cluster_id(const common::ObAddr& server, const int64_t cluster_id) override;
  VIRTUAL_FOR_UNITTEST int force_refresh_locality_info();
  int add_refresh_locality_task();
  ObCLogCallbackAsyncWorker& get_callback_async_worker()
  {
    return cb_async_worker_;
  }

  // batch migration
  VIRTUAL_FOR_UNITTEST int migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg);

  VIRTUAL_FOR_UNITTEST int restore_replica(const obrpc::ObRestoreReplicaArg& arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int physical_restore_replica(
      const obrpc::ObPhyRestoreReplicaArg& arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int restore_follower_replica(const obrpc::ObCopySSTableBatchArg& rpc_arg);
  VIRTUAL_FOR_UNITTEST int backup_replica_batch(const obrpc::ObBackupBatchArg& arg);
  VIRTUAL_FOR_UNITTEST int validate_backup_batch(const obrpc::ObValidateBatchArg& arg);

  virtual int get_tenant_log_archive_status(
      const share::ObGetTenantLogArchiveStatusArg& arg, share::ObTenantLogArchiveStatusWrapper& result);
  virtual int get_archive_pg_map(archive::PGArchiveMap*& map);
  int push_replica_task(
      const ObReplicaOpType& type, const obrpc::ObMigrateReplicaArg& migrate_arg, ObIArray<ObReplicaOpArg>& task_list);

  int check_add_or_migrate_replica_arg(const common::ObPartitionKey& pkey,
      const common::ObReplicaMember& dst, /* new replica */
      const common::ObReplicaMember& src,
      const common::ObReplicaMember& data_src, /* data source, if invalid, use leader instead */
      const int64_t quorum);

  // interface for online/offline
  VIRTUAL_FOR_UNITTEST int add_replica(const obrpc::ObAddReplicaArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int batch_add_replica(
      const obrpc::ObAddReplicaBatchArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int migrate_replica(const obrpc::ObMigrateReplicaArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int rebuild_replica(const obrpc::ObRebuildReplicaArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int schedule_standby_restore_task(
      const obrpc::ObRebuildReplicaArg& rpc_arg, const int64_t cluster_id, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int copy_global_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int copy_local_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int change_replica(const obrpc::ObChangeReplicaArg& rpc_arg, const share::ObTaskId& task_id);
  VIRTUAL_FOR_UNITTEST int batch_change_replica(const obrpc::ObChangeReplicaBatchArg& arg);
  VIRTUAL_FOR_UNITTEST int remove_replica(const common::ObPartitionKey& key, const common::ObReplicaMember& dst);
  VIRTUAL_FOR_UNITTEST int batch_remove_replica(const obrpc::ObRemoveReplicaArgs& args);
  VIRTUAL_FOR_UNITTEST int batch_remove_non_paxos_replica(
      const obrpc::ObRemoveNonPaxosReplicaBatchArg& args, obrpc::ObRemoveNonPaxosReplicaBatchResult& results);

  VIRTUAL_FOR_UNITTEST int is_member_change_done(
      const common::ObPartitionKey& key, const uint64_t log_id, const int64_t timestamp);
  VIRTUAL_FOR_UNITTEST int handle_member_change_callback(
      const ObReplicaOpArg& arg, const int result, bool& could_retry);
  VIRTUAL_FOR_UNITTEST int handle_report_meta_table_callback(
      const ObPartitionKey& pkey, const int result, const bool need_report_checksum);
  VIRTUAL_FOR_UNITTEST int report_migrate_in_indexes(const ObPartitionKey& pkey);
  VIRTUAL_FOR_UNITTEST int add_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  VIRTUAL_FOR_UNITTEST int remove_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  VIRTUAL_FOR_UNITTEST int batch_remove_replica_mc(
      const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result);
  VIRTUAL_FOR_UNITTEST int change_quorum_mc(const obrpc::ObModifyQuorumArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  int handle_ha_gts_ping_request(const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response);
  int handle_ha_gts_get_request(const obrpc::ObHaGtsGetRequest& request);
  int handle_ha_gts_get_response(const obrpc::ObHaGtsGetResponse& response);
  int handle_ha_gts_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat);
  int handle_ha_gts_update_meta(
      const obrpc::ObHaGtsUpdateMetaRequest& request, obrpc::ObHaGtsUpdateMetaResponse& response);
  int handle_ha_gts_change_member(
      const obrpc::ObHaGtsChangeMemberRequest& request, obrpc::ObHaGtsChangeMemberResponse& response);
  int send_ha_gts_get_request(const common::ObAddr& server, const obrpc::ObHaGtsGetRequest& request);

  VIRTUAL_FOR_UNITTEST int check_self_in_member_list(const common::ObPartitionKey& key, bool& in_member_list);

  // interface on ObTableDataService
  virtual int get_table_stat(const common::ObPartitionKey& key, common::ObTableStat& tstat) override;
  VIRTUAL_FOR_UNITTEST int schema_drop_partition(const ObCLogCallbackAsyncTask& offline_task);

  int replay_offline_partition_log(const ObPartitionKey& pkey, const uint64_t log_id, const bool is_physical_drop);

  int do_partition_loop_work(ObIPartitionGroup& partition);
  VIRTUAL_FOR_UNITTEST int do_warm_up_request(const obrpc::ObWarmUpRequestArg& arg, const int64_t recieve_ts);
  VIRTUAL_FOR_UNITTEST int generate_partition_weak_read_snapshot_version(ObIPartitionGroup& partition, bool& need_skip,
      bool& is_user_partition, int64_t& wrs_version, const int64_t max_stale_time);
  VIRTUAL_FOR_UNITTEST int check_can_start_service(
      bool& can_start_service, int64_t& safe_weak_read_snaspthot, ObPartitionKey& min_slave_read_ts_pkey);
  VIRTUAL_FOR_UNITTEST int admin_wash_ilog_cache(const clog::file_id_t file_id)
  {
    // Deprecated
    // return get_cursor_cache() == NULL ? OB_ERR_UNEXPECTED : get_cursor_cache()->admin_wash_ilog_cache(file_id);
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }
  VIRTUAL_FOR_UNITTEST int admin_wash_ilog_cache()
  {
    // Deprecated
    // return get_cursor_cache() == NULL ? OB_ERR_UNEXPECTED : get_cursor_cache()->admin_wash_ilog_cache();
    return common::OB_SUCCESS;
  }
  virtual int query_range_to_macros(common::ObIAllocator& allocator, const common::ObPartitionKey& pkey,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
      const int64_t* total_task_count, common::ObIArray<common::ObStoreRange>* splitted_ranges,
      common::ObIArray<int64_t>* split_index) override;

  virtual int get_multi_ranges_cost(
      const common::ObPartitionKey& pkey, const common::ObIArray<common::ObStoreRange>& ranges, int64_t& total_size);
  virtual int split_multi_ranges(const common::ObPartitionKey& pkey,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t expected_task_count,
      common::ObIAllocator& allocator, common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);
  VIRTUAL_FOR_UNITTEST int is_log_sync(
      const common::ObPartitionKey& key, bool& is_sync, uint64_t& max_confirmed_log_id);
  VIRTUAL_FOR_UNITTEST int set_region(const ObPartitionKey& key, clog::ObIPartitionLogService* pls);
  VIRTUAL_FOR_UNITTEST bool is_running() const
  {
    return is_running_;
  }

  int retry_post_operate_replica_res(const ObReplicaOpArg& arg, const int result);
  int retry_post_batch_migrate_replica_res(
      const ObReplicaOpType& type, const ObArray<ObPartMigrationRes>& report_res_list);
  bool is_working_partition(const common::ObPartitionKey& pkey);
  VIRTUAL_FOR_UNITTEST int is_local_zone_read_only(bool& is_read_only);
  share::ObIPartitionLocationCache* get_location_cache()
  {
    return location_cache_;
  }
  ObIPartitionServiceRpc& get_pts_rpc()
  {
    return pts_rpc_;
  }
  VIRTUAL_FOR_UNITTEST int append_local_sort_data(const common::ObPartitionKey& pkey,
      const share::ObBuildIndexAppendLocalDataParam& param, common::ObNewRowIterator& iter);
  VIRTUAL_FOR_UNITTEST int append_sstable(const common::ObPartitionKey& pkey,
      const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter);

  VIRTUAL_FOR_UNITTEST bool is_election_candidate(const common::ObPartitionKey& pkey);
  // for splitting partition
  VIRTUAL_FOR_UNITTEST int split_partition(
      const share::ObSplitPartition& split_info, common::ObIArray<share::ObPartitionSplitProgress>& result);
  VIRTUAL_FOR_UNITTEST int sync_split_source_log_success(
      const common::ObPartitionKey& pkey, const int64_t source_log_id, const int64_t source_log_ts);
  VIRTUAL_FOR_UNITTEST int sync_split_dest_log_success(const common::ObPartitionKey& pkey);
  VIRTUAL_FOR_UNITTEST int split_dest_partition(const common::ObPartitionKey& pkey,
      const ObPartitionSplitInfo& split_info, enum share::ObSplitProgress& progress);

  VIRTUAL_FOR_UNITTEST int calc_column_checksum(const common::ObPartitionKey& pkey, const uint64_t index_table_id,
      const int64_t schema_version, const uint64_t execution_id, const int64_t snapshot_version);
  VIRTUAL_FOR_UNITTEST int check_single_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id);
  VIRTUAL_FOR_UNITTEST int check_single_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& timestamp);
  VIRTUAL_FOR_UNITTEST int check_all_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id);
  VIRTUAL_FOR_UNITTEST int check_all_replica_major_sstable_exist(
      const common::ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& max_timestamp);
  VIRTUAL_FOR_UNITTEST int check_member_pg_major_sstable_enough(
      const common::ObPGKey& pg_key, const common::ObIArray<uint64_t>& table_ids);

  VIRTUAL_FOR_UNITTEST int check_all_partition_sync_state(const int64_t switchover_epoch);
  VIRTUAL_FOR_UNITTEST int send_leader_max_log_info();
  VIRTUAL_FOR_UNITTEST int get_role_and_leader_epoch(
      const common::ObPartitionKey& pkey, common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time) const;

  int turn_off_rebuild_flag(const ObReplicaOpArg& arg);

  int try_remove_from_member_list(const obrpc::ObMemberChangeArg& arg);
  // the destination partition of a split should not be removed before the source partition
  int check_split_dest_partition_can_remove(
      const common::ObPartitionKey& key, const common::ObPartitionKey& pg_key, bool& can_remove);
  int process_migrate_retry_task(const ObMigrateRetryTask& task);
  bool reach_tenant_partition_limit(const int64_t batch_cnt, const uint64_t tenant_id, const bool is_pg_arg);
  int retry_rebuild_loop();
  VIRTUAL_FOR_UNITTEST int get_pg_key(const ObPartitionKey& pkey, ObPGKey& pg_key);
  static int mtl_init(ObTenantStorageInfo*& tenant_store_info)
  {
    int ret = common::OB_SUCCESS;
    tenant_store_info = OB_NEW(ObTenantStorageInfo, ObModIds::OB_PARTITION_SERVICE);
    if (OB_ISNULL(tenant_store_info)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  static void mtl_destroy(ObTenantStorageInfo*& tenant_store_info)
  {
    common::ob_delete(tenant_store_info);
    tenant_store_info = nullptr;
  }

  VIRTUAL_FOR_UNITTEST ObPartitionGroupIndex& get_pg_index()
  {
    return pg_index_;
  }

  int set_restore_flag(const ObPartitionKey& pkey, const int16_t flag);
  int set_restore_snapshot_version_for_trans(const ObPartitionKey& pkey, const int64_t restore_snapshot_version);
  int get_restore_replay_info(
      const ObPartitionKey& pkey, uint64_t& restore_last_replay_log_id, int64_t& restore_snapshot_version);
  int wait_all_trans_clear(const ObPartitionKey& pkey);
  int check_all_trans_in_trans_table_state(const ObPartitionKey& pkey);

  int get_offline_log_id(const ObPartitionKey& pkey, uint64_t offline_log_id) const;
  int set_member_list(const obrpc::ObSetMemberListBatchArg& arg, obrpc::ObCreatePartitionBatchRes& result);
  int check_pg_partition_exist(const ObPGKey& pg_key, const ObPartitionKey& pkey);
  int check_split_source_partition_exist(const ObPGKey& pg_key, const int64_t table_id);
  int check_tenant_pg_exist(const uint64_t tenant_id, bool& is_exist);
  int acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle);
  int create_trans_table_if_needed();
  int check_physical_flashback_succ(
      const obrpc::ObCheckPhysicalFlashbackArg& arg, obrpc::ObPhysicalFlashbackResultArg& result);
  int nonblock_renew_loc_cache(const common::ObPartitionKey& pkey);
  int submit_pt_update_task(const ObPartitionKey& pkey);
  int submit_pt_update_role_task(const ObPartitionKey& pkey);
  int start_physical_flashback();
  int check_can_physical_flashback();
  int try_freeze_aggre_buffer(const common::ObPartitionKey& pkey);
  VIRTUAL_FOR_UNITTEST int check_has_need_offline_replica(
      const obrpc::ObTenantSchemaVersions& arg, obrpc::ObGetPartitionCountResult& result);
  int wait_schema_version(const int64_t tenant_id, int64_t schema_version, int64_t query_end_time);
  int fast_migrate_wait_batch_change_member_list_done(
      const ObAddr& leader_addr, obrpc::ObChangeMemberCtxs& change_member_info);
  int fast_migrate_try_remove_member(const ObAddr& leader_addr, const ObPGKey& pg_key, const ObMember& old_member,
      int64_t quorum, int64_t switch_epoch);
  int fast_migrate_try_add_member(const ObAddr& leader_addr, const ObPGKey& pg_key, const ObMember& new_member,
      int64_t quorum, int64_t switch_epoch);
  // for log_archive
  int mark_log_archive_encount_fatal_error(
      const common::ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round);
  // for migrate src
  int get_migrate_leader_and_parent(const common::ObPartitionKey& pkey, common::ObIArray<ObMigrateSrcInfo>& addr_array);
  int get_rebuild_src(const common::ObPartitionKey& pkey, const ObReplicaType& replica_type,
      common::ObIArray<ObMigrateSrcInfo>& addr_array);
  int get_migrate_member_list_src(const common::ObPartitionKey& pkey, common::ObIArray<ObMigrateSrcInfo>& addr_array);
  int mark_pg_creating(const ObPartitionKey& pkey);
  int mark_pg_created(const ObPartitionKey& pkey);
  int check_partition_exist(const common::ObPartitionKey& pkey, bool& exist) override;

  obrpc::ObCommonRpcProxy& get_rs_rpc_proxy();
  int inc_pending_batch_commit_count(const ObPartitionKey& pkey, memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);
  int inc_pending_elr_count(const ObPartitionKey& pkey, memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);
  int check_restore_point_complete(const ObPartitionKey& pkey, const int64_t snapshot_version, bool& is_complete);
  int try_advance_restoring_clog();
  int64_t get_total_partition_cnt()
  {
    return ATOMIC_LOAD(&total_partition_cnt_);
  }
  int get_partition_saved_last_log_info(
      const ObPartitionKey& pkey, uint64_t& last_replay_log_id, int64_t& last_replay_log_ts);
  int prepare_persist_log_archive_meta(ObIPartitionGroup& partition_group);

  // @brief: used for revoke all partition
  int try_revoke_all_leader(const election::ObElection::RevokeType& revoke_type);

private:
  class ObStoreCtxGuard {
  public:
    ObStoreCtxGuard() : is_inited_(false), trans_desc_(NULL), pkey_(), ctx_(), txs_(NULL), init_ts_(0)
    {}
    virtual ~ObStoreCtxGuard()
    {
      int err = common::OB_SUCCESS;
      static const int64_t WARN_TIME_US = 5 * 1000 * 1000;
      if (is_inited_) {
        if (OB_SUCCESS != (err = txs_->revert_store_ctx(*trans_desc_, pkey_, ctx_))) {
          STORAGE_LOG(ERROR,
              "fail to revert transaction context",
              "trans_desc",
              *trans_desc_,
              K_(pkey),
              "memtable context",
              ctx_.mem_ctx_,
              K(err));
        } else {
          is_inited_ = false;
        }
        const int64_t guard_ts = ObClockGenerator::getClock() - init_ts_;
        if (guard_ts >= WARN_TIME_US) {
          STORAGE_LOG(WARN, "guard too much time", K_(trans_desc), K_(pkey), K(guard_ts), "lbt", lbt());
        }
      }
    }
    int init(const transaction::ObTransDesc& trans_desc, const common::ObPartitionKey& pkey,
        transaction::ObTransService& txs);
    ObStoreCtx& get_store_ctx();

  private:
    bool is_inited_;
    transaction::ObTransDesc* trans_desc_;
    common::ObPartitionKey pkey_;
    ObStoreCtx ctx_;
    transaction::ObTransService* txs_;
    int64_t init_ts_;
  };

  struct ParitionRegisterStatus {
    bool in_tran_service_;
    bool in_election_;
    bool in_replay_engine_;
    ParitionRegisterStatus() : in_tran_service_(false), in_election_(false), in_replay_engine_(false)
    {}
    TO_STRING_KV(K_(in_tran_service), K_(in_election), K_(in_replay_engine));
  };

protected:
  virtual int init_partition_group(ObIPartitionGroup& pg, const common::ObPartitionKey& pkey) override;
  virtual int post_replay_remove_pg_partition(const ObChangePartitionLogEntry& log_entry) override;

private:
  int check_can_physical_flashback_();
  bool is_tenant_active_(const uint64_t tenant_id) const;
  int check_init(void* cp, const char* cp_name) const;
  int get_trans_ctx_for_dml(
      const common::ObPartitionKey& pkey, const transaction::ObTransDesc& trans_desc, ObStoreCtxGuard& ctx_guard);
  int check_query_allowed(const ObPartitionKey& pkey, const transaction::ObTransDesc& trans_desc,
      ObStoreCtxGuard& ctx_guard, ObIPartitionGroupGuard& guard);
  int replay_add_store(const int64_t log_seq_num, const char* buf, const int64_t buf_len);
  int replay_remove_store(const int64_t log_seq_num, const char* buf, const int64_t buf_len);
  int replay_add_partition_to_pg_clog(const ObCreatePartitionParam& arg, const uint64_t log_id, const int64_t log_ts);
  int replace_restore_info_(const uint64_t cur_tenant_id, const share::ObReplicaRestoreStatus is_restore,
      const int64_t create_frozen_version, ObCreatePartitionParam& create_param);
  int prepare_all_partitions();
  int remove_duplicate_partitions(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg);
  int add_partitions_to_mgr(common::ObIArray<ObIPartitionGroup*>& partitions);
  int add_partitions_to_replay_engine(const common::ObIArray<ObIPartitionGroup*>& partitions);
  int batch_register_trans_service(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg);
  int batch_register_election_mgr(const bool is_pg, const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg,
      common::ObIArray<ObIPartitionGroup*>& partitions,
      common::ObIArray<blocksstable::ObStorageFileHandle>& files_handle);
  int batch_start_partition_election(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<ObIPartitionGroup*>& partitions);
  int try_remove_from_member_list(ObIPartitionGroup& partition);
  int batch_prepare_splitting(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg);
  int try_add_to_member_list(const obrpc::ObMemberChangeArg& arg);
  int check_active_member(common::ObAddr& leader, const common::ObMember& member, const common::ObPartitionKey& key);
  int handle_add_replica_callback(const ObReplicaOpArg& arg, const int result);
  int handle_migrate_replica_callback(const ObReplicaOpArg& arg, const int result, bool& could_retry);
  int handle_rebuild_replica_callback(const ObReplicaOpArg& arg, const int result);
  int handle_change_replica_callback(const ObReplicaOpArg& arg, const int result);
  template <typename ResultT>
  int get_operate_replica_res(const ObReplicaOpArg& arg, const int result, ResultT& res);
  int retry_get_is_member_change_done(common::ObAddr& leader, obrpc::ObMCLogRpcInfo& mc_log_info);
  int retry_send_add_replica_mc_msg(common::ObAddr& server, const obrpc::ObMemberChangeArg& arg);
  int retry_send_remove_replica_mc_msg(common::ObAddr& server, const obrpc::ObMemberChangeArg& arg);
  int retry_post_add_replica_mc_msg(
      common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  int retry_post_remove_replica_mc_msg(
      common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  int retry_get_active_member(common::ObAddr& server, const common::ObPartitionKey& key, common::ObMemberList& mlist);

  int wait_clog_replay_over();
  int wait_fetch_log(const common::ObPartitionKey& key);

  bool dispatch_task(const ObCbTask& cb_task, ObIPartitionGroup* partition);
  bool is_role_change_done(const common::ObPartitionKey& pkey, const ObPartitionState& state) const;
  int check_tenant_out_of_memstore_limit(const uint64_t tenant_id, bool& is_out_of_mem);
  int save_base_schema_version(ObIPartitionGroupGuard& guard);
  int check_schema_version(const ObIPartitionArrayGuard& pkey_guard_arr);
  void init_tenant_bit_set();

  void rollback_partition_register(const common::ObPartitionKey& pkey, bool rb_txs, bool rb_rp_eg);
  int report_rs_to_rebuild_replica(const common::ObPartitionKey& pkey, const common::ObAddr& server);
  int check_mc_allowed_by_server_lease(bool& is_mc_allowed);
  int set_partition_region_priority(const ObPartitionKey& pkey, clog::ObIPartitionLogService* pls);
  static int build_migrate_replica_batch_res(
      const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObMigrateReplicaBatchRes& res);
  static int build_change_replica_batch_res(
      const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObChangeReplicaBatchRes& res);
  static int build_add_replica_batch_res(
      const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObAddReplicaBatchRes& res);
  static int build_backup_replica_batch_res(
      const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObBackupBatchRes& res);
  static int build_validate_backup_batch_res(
      const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObValidateBatchRes& res);

  int decode_log_type(const char* log, const int64_t size, int64_t& pos, ObStorageLogType& log_type);

  int split_source_partition_(const common::ObPartitionKey& pkey, const int64_t schema_version,
      const share::ObSplitPartitionPair& spp, enum share::ObSplitProgress& progress);
  int split_dest_partition_(const common::ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info,
      enum share::ObSplitProgress& progress);

  int remove_partition(ObIPartitionGroup* partition, const bool write_slog);
  int remove_pg_from_mgr(const ObIPartitionGroup* partition, const bool write_slog);
  int inner_add_partition(ObIPartitionGroup& partition, const bool need_check_tenant, const bool is_replay,
      const bool allow_multi_value) override;
  int inner_del_partition(const common::ObPartitionKey& pkey) override;
  int inner_del_partition_for_replay(const common::ObPartitionKey& pkey, const int64_t file_id) override;
  int inner_del_partition_impl(const common::ObPartitionKey& pkey, const int64_t* file_id);
  int create_sstables(const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
      ObIPartitionGroup& partition_group, ObTablesHandle& handle);
  int create_sstables(const ObCreatePartitionParam& arg, const bool in_slog_trans, ObIPartitionGroup& partition_group,
      ObTablesHandle& handle);
  int generate_task_pg_info_(const ObPartitionArray& pgs, ObIPartitionArrayGuard& out_pg_guard_arr);
  int do_remove_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst);
  int gen_rebuild_arg_(
      const common::ObPartitionKey& pkey, const common::ObReplicaType replica_type, obrpc::ObRebuildReplicaArg& arg);
  int gen_standby_restore_arg_(const common::ObPartitionKey& pkey, const common::ObReplicaType replica_type,
      const common::ObAddr& src_server, obrpc::ObRebuildReplicaArg& arg);
  int handle_rebuild_result_(
      const common::ObPartitionKey pkey, const common::ObReplicaType replica_type, const int ret_val);
  bool reach_tenant_partition_limit_(const int64_t batch_cnt, const uint64_t tenant_id, const bool is_pg_arg);
  int get_pg_key_(const ObPartitionKey& pkey, ObPGKey& pg_key);
  int get_pg_key_from_index_schema_(const ObPartitionKey& pkey, ObPGKey& pg_key);
  int submit_add_partition_to_pg_clog_(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg,
      const int64_t timeout, common::ObIArray<uint64_t>& log_id_arr);
  int write_partition_schema_version_change_clog_(const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey,
      const int64_t schema_version, const uint64_t index_id, const int64_t timeout, uint64_t& log_id, int64_t& log_ts);
  int check_partition_state_(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg,
      common::ObIArray<obrpc::ObCreatePartitionArg>& target_batch_arg, common::ObIArray<int>& batch_res);
  void free_partition_list(ObArray<ObIPartitionGroup*>& partition_list);
  void submit_pt_update_task_(const ObPartitionKey& pkey, const bool need_report_checksum = true);
  int try_inc_total_partition_cnt(const int64_t new_partition_cnt, const bool need_check);
  int physical_flashback();
  int clean_all_clog_files_();
  int report_pg_backup_task(const ObIArray<ObPartMigrationRes>& report_res_list);
  int get_create_pg_param(const obrpc::ObCreatePartitionArg& arg, const ObSavedStorageInfoV2& info,
      const int64_t data_version, const bool write_pg_slog, const ObPartitionSplitInfo& split_info,
      const int64_t split_state, blocksstable::ObStorageFileHandle* file_handle, ObBaseFileMgr* file_mgr,
      ObCreatePGParam& param);
  int check_flashback_need_remove_pg(const int64_t flashback_scn, ObIPartitionGroup* partition, bool& need_remve);
  int remove_flashback_unneed_pg(ObIPartitionGroup* partition);
  int check_condition_before_set_restore_flag_(
      const common::ObPartitionKey& pkey, const int16_t flag, const int64_t restore_snapshot_version);
  int get_primary_cluster_migrate_src(const common::ObPartitionKey& pkey, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int get_standby_cluster_migrate_src(const common::ObPartitionKey& pkey, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int get_primary_cluster_migrate_member_src(const common::ObPartitionKey& pkey, const ObReplicaType& replica_type,
      hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array);
  int get_standby_cluster_migrate_member_src(const common::ObPartitionKey& pkey, const ObReplicaType& replica_type,
      hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array);

  int add_migrate_member_list(const common::ObPartitionKey& pkey, const common::ObRole& role,
      const bool is_standby_cluster, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int add_current_migrate_member_list(const common::ObPartitionKey& pkey, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int add_meta_table_migrate_src(const common::ObPartitionKey& pkey, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int add_migrate_src(const ObMigrateSrcInfo& src_info, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int add_primary_meta_table_migrate_src(const common::ObPartitionKey& pkey, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int inner_add_meta_table_migrate_src(const common::ObIArray<share::ObPartitionReplica>& replicas,
      const int64_t cluster_id, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int extract_pkeys(
      const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<ObPartitionKey>& pkey_array);
  int add_location_cache_migrate_src(const common::ObPartitionKey& pkey, const common::ObRole& role,
      const bool is_standby_cluster, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);
  int inner_add_location_migrate_src(const common::ObIArray<share::ObReplicaLocation>& locations,
      const int64_t cluster_id, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
      common::ObIArray<ObMigrateSrcInfo>& src_array);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionService);

private:
  class ReloadLocalityTask : public common::ObTimerTask {
  public:
    ReloadLocalityTask();
    virtual ~ReloadLocalityTask()
    {}
    int init(ObPartitionService* ptt_svr);
    virtual void runTimerTask();
    void destroy();

  private:
    bool is_inited_;
    ObPartitionService* ptt_svr_;
  };

  class PurgeRetireMemstoreTask : public common::ObTimerTask {
  public:
    PurgeRetireMemstoreTask();
    virtual ~PurgeRetireMemstoreTask()
    {}

  public:
    int init();
    void destroy();
    virtual void runTimerTask();

  private:
    bool is_inited_;
  };

  class ClogRequiredMinorFreezeTask : public common::ObTimerTask {
  public:
    ClogRequiredMinorFreezeTask();
    virtual ~ClogRequiredMinorFreezeTask()
    {}

  public:
    int init(clog::ObICLogMgr* clog_mgr);
    void destroy();
    virtual void runTimerTask();

  private:
    bool is_inited_;
    clog::ObICLogMgr* clog_mgr_;
  };

  class ObRefreshLocalityTask : public common::IObDedupTask {
  public:
    explicit ObRefreshLocalityTask(ObPartitionService* partition_service);
    virtual ~ObRefreshLocalityTask();
    virtual int64_t hash() const;
    virtual bool operator==(const IObDedupTask& other) const;
    virtual int64_t get_deep_copy_size() const;
    virtual IObDedupTask* deep_copy(char* buffer, const int64_t buf_size) const;
    virtual int64_t get_abs_expired_time() const
    {
      return 0;
    }
    virtual int process();

  private:
    ObPartitionService* partition_service_;
  };

  struct PartitionSizePair {
    ObPartitionKey pkey_;
    int64_t size_;
  };
  class PrintHashedFreezeFunctor {
  public:
    PrintHashedFreezeFunctor()
    {}
    bool operator()(const common::ObPartitionKey& pkey, ObIPSFreezeCb* cb)
    {
      UNUSED(cb);
      bool bool_ret = true;
      STORAGE_LOG(WARN, "existing freeze partition", K(pkey));
      return bool_ret;
    }
  };

private:
  friend class ObPartitionIterator;
  static const int64_t BITS_PER_BITSETWORD = 32;
  static const int64_t BITSETWORD_SHIFT_NUM = 5;
  static const int64_t BITSETWORD_OFFSET_MASK = BITS_PER_BITSETWORD - 1;
  static const int64_t BITSET_BITS_NUM = (OB_DEFAULT_TENANT_COUNT + BITSETWORD_OFFSET_MASK) & (~BITSETWORD_OFFSET_MASK);
  static const int64_t BITSET_WORDS_NUM = BITSET_BITS_NUM / BITS_PER_BITSETWORD;
  typedef uint32_t BITSET_TYPE;
  static const int64_t WAIT_FREEZE_INTERVAL = 100000;  // 100ms
  static const int64_t ASSEMBLE_THREAD_NUM = 8;
  static const int64_t REFRESH_LOCALITY_TASK_NUM = 5;
  bool is_running_;
  obrpc::ObBatchRpc batch_rpc_;
  obrpc::ObTransRpcProxy tx_rpc_proxy_;
  obrpc::ObDupTableRpcProxy dup_table_rpc_proxy_;
  obrpc::ObLogRpcProxy clog_rpc_proxy_;
  obrpc::ObPartitionServiceRpcProxy pts_rpc_proxy_;
  obrpc::ObXARpcProxy xa_proxy_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  ObPartitionServiceRpc pts_rpc_;
  clog::ObICLogMgr* clog_mgr_;
  election::ObIElectionMgr* election_mgr_;
  clog::ObLogReplayEngineWrapper* rp_eg_wrapper_;
  ObIPartitionReport* rs_cb_;

  share::ObIPartitionLocationCache* location_cache_;
  share::ObRsMgr* rs_mgr_;
  ObCLogCallbackAsyncWorker cb_async_worker_;
  ObFreezeAsyncWorker freeze_async_worker_;
  ReloadLocalityTask reload_locality_task_;
  PurgeRetireMemstoreTask purge_retire_memstore_task_;
  ClogRequiredMinorFreezeTask clog_required_minor_freeze_task_;
  logservice::ObExtLogService::StreamTimerTask ext_log_service_stream_timer_task_;
  logservice::ObExtLogService::LineCacheTimerTask line_cache_timer_task_;
  common::ObAddr self_addr_;
  BITSET_TYPE tenant_bit_set_[BITSET_WORDS_NUM];

  ObMigrateRetryQueueThread migrate_retry_queue_thread_;
  ObCallbackQueueThread cb_queue_thread_;
  ObCallbackQueueThread large_cb_queue_thread_;
  ObSlogWriterQueueThread slog_writer_thread_pool_;
  common::ObDedupQueue refresh_locality_task_queue_;
  ObWarmUpService* warm_up_service_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObLocalityManager locality_manager_;
  ObPartitionSplitWorker split_worker_;
  mutable common::ObSpinLock trans_version_lock_;
  int64_t global_max_decided_trans_version_;
  ObGarbageCollector garbage_collector_;
  ObPartitionWorker partition_worker_;
  ObTransCheckpointWorker trans_checkpoint_worker_;
  clog::ObClogAggreRunnable clog_aggre_runnable_;
  ObDupReplicaChecker dup_replica_checker_;
  gts::ObHaGtsManager gts_mgr_;
  gts::ObHaGtsSource gts_source_;
  ObRebuildReplicaService rebuild_replica_service_;
  ObServerPGMetaSLogFilter slog_filter_;
  int64_t total_partition_cnt_;
  ObSSTableGarbageCollector sstable_garbage_collector_;
  ObAutoPartScheduler auto_part_scheduler_;
  ObPartitionGroupCreateChecker create_pg_checker_;
};

inline int ObPartitionService::ObStoreCtxGuard::init(
    const transaction::ObTransDesc& trans_desc, const common::ObPartitionKey& pkey, transaction::ObTransService& txs)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (!trans_desc.is_valid() || !pkey.is_valid() || !ctx_.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument(s)", K(trans_desc), K(pkey), "memtable context", ctx_.mem_ctx_, K(ret));
  } else {
    trans_desc_ = &const_cast<transaction::ObTransDesc&>(trans_desc);
    pkey_ = pkey;
    txs_ = &txs;
    is_inited_ = true;
    init_ts_ = ObClockGenerator::getClock();
  }
  return ret;
}

inline ObStoreCtx& ObPartitionService::ObStoreCtxGuard::get_store_ctx()
{
  return ctx_;
}

inline int ObPartitionService::check_init(void* cp, const char* cp_name) const
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_ || NULL == cp || NULL == cp_name) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "component does not exist", "component name", cp_name, K(ret));
  }
  return ret;
}

inline int ObPartitionService::internal_kill_trans(transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->internal_kill_trans(trans_desc);
  }
  return ret;
}

inline int ObPartitionService::kill_query_session(const transaction::ObTransDesc& trans_desc, const int status)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->kill_query_session(trans_desc, status);
  }

  return ret;
}

inline int ObPartitionService::get_trans_ctx_for_dml(
    const common::ObPartitionKey& pkey, const transaction::ObTransDesc& trans_desc, ObStoreCtxGuard& ctx_guard)
{
  int ret = common::OB_SUCCESS;
  // DML statement will always use invalid snapshot
  int64_t user_specified_snapshot = transaction::ObTransVersion::INVALID_TRANS_VERSION;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "transaction service does not exist", K(ret));
  } else if (OB_FAIL(txs_->get_store_ctx(trans_desc, pkey, ctx_guard.get_store_ctx()))) {
    STORAGE_LOG(WARN, "can not get transaction context", K(pkey), K(trans_desc), K(ret), K(user_specified_snapshot));
  } else {
    if (OB_FAIL(ctx_guard.init(trans_desc, pkey, *txs_))) {
      STORAGE_LOG(WARN, "fail to init transaction context guard", K(ret));
      ret = common::OB_SUCCESS;
      if (OB_FAIL(txs_->revert_store_ctx(trans_desc, pkey, ctx_guard.get_store_ctx()))) {
        STORAGE_LOG(WARN, "fail to revert transaction context", K(ret));
      }
    }
  }
  return ret;
}

inline bool ObPartitionService::is_empty() const
{
  return pg_mgr_.is_empty();
}

inline transaction::ObTransService* ObPartitionService::get_trans_service()
{
  return txs_;
}

inline memtable::ObIMemtableCtxFactory* ObPartitionService::get_mem_ctx_factory()
{
  memtable::ObIMemtableCtxFactory* factory = NULL;

  if (NULL != txs_) {
    factory = txs_->get_mem_ctx_factory();
  }
  return factory;
}

inline clog::ObICLogMgr* ObPartitionService::get_clog_mgr()
{
  return clog_mgr_;
}

inline election::ObIElectionMgr* ObPartitionService::get_election_mgr()
{
  return election_mgr_;
}

inline ObPartitionSplitWorker* ObPartitionService::get_split_worker()
{
  return &split_worker_;
}

inline clog::ObClogAggreRunnable* ObPartitionService::get_clog_aggre_runnable()
{
  return &clog_aggre_runnable_;
}

inline bool ObPartitionService::is_scan_disk_finished()
{
  return (is_inited_ && NULL != clog_mgr_ && clog_mgr_->is_scan_finished());
}

inline ObIPartitionComponentFactory* ObPartitionService::get_cp_fty()
{
  return cp_fty_;
}

OB_INLINE ObPartitionService& ObPartitionService::get_instance()
{
  static ObPartitionService instance;
  return instance;
}

OB_INLINE int ObPartitionService::xa_start(const transaction::ObXATransID& xid, const int64_t flags,
    const int64_t xa_end_timeout_seconds, transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->xa_start_v2(xid, flags, xa_end_timeout_seconds, trans_desc);
  }
  return ret;
}

OB_INLINE int ObPartitionService::xa_end(
    const transaction::ObXATransID& xid, const int64_t flags, transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->xa_end_v2(xid, flags, trans_desc);
  }
  return ret;
}

OB_INLINE int ObPartitionService::xa_prepare(
    const transaction::ObXATransID& xid, const uint64_t tenant_id, const int64_t stmt_expired_time)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->xa_prepare_v2(xid, tenant_id, stmt_expired_time);
  }
  return ret;
}

OB_INLINE int ObPartitionService::xa_end_trans(const transaction::ObXATransID& xid, const bool is_rollback,
    const int64_t flags, transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->xa_end_trans_v2(xid, is_rollback, flags, trans_desc);
  }
  return ret;
}

OB_INLINE int ObPartitionService::get_xa_trans_state(int32_t& state, transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "ObTransService check init error");
  } else {
    ret = txs_->get_xa_trans_state(state, trans_desc);
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_SERVICE
