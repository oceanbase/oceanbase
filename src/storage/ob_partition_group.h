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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_GROUP
#define OCEANBASE_STORAGE_OB_PARTITION_GROUP

#include "common/ob_partition_key.h"
#include "common/storage/ob_freeze_define.h"
#include "lib/container/ob_mask_set2.h"
#include "lib/hash/ob_dchash.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/metrics/ob_meter.h"
#include "ob_warm_up_request.h"
#include "share/ob_partition_modify.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_freeze_record.h"
#include "storage/ob_partition_group_lock.h"
#include "storage/ob_partition_loop_worker.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_wrs_utils.h"  // is_weak_readable_replica

namespace oceanbase {
namespace share {
class ObSplitPartitionPair;
}
namespace clog {
class ObIPartitionLogService;
class ObICLogMgr;
class ObISubmitLogCb;
}  // namespace clog
namespace storage {
class ObIPartitionComponentFactory;
class ObAddPartitionToPGLogCb;
class ObSchemaChangeClogCb;
typedef common::DCHash<common::ObPartitionKey> PMHash;
extern const char* OB_PARTITION_STATE_STR[INVALID_STATE + 1];

enum ObMigrateRetryFlag {
  NO_NEED_RETRY = 0,
  NEED_REBUILD,
  NEED_STANDBY_RESTORE,
};

class ObPartitionGroup : public ObIPartitionGroup {
public:
  ObPartitionGroup();
  virtual ~ObPartitionGroup();

  virtual int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      replayengine::ObILogReplayEngine* rp_eg, ObPartitionService* ps, ObPartitionGroupIndex* pg_index,
      ObPGPartitionMap* pg_partition_map) override;
  virtual void destroy();
  virtual void clear() override;

  virtual const clog::ObIPartitionLogService* get_log_service() const;
  virtual clog::ObIPartitionLogService* get_log_service();
  virtual ObIPartitionComponentFactory* get_cp_fty()
  {
    return cp_fty_;
  }

  virtual ObPGStorage& get_pg_storage()
  {
    return pg_storage_;
  }
  virtual ObPartitionService* get_partition_service();
  virtual ObPartitionGroupIndex* get_pg_index() override
  {
    return pg_index_;
  };
  virtual ObPGPartitionMap* get_pg_partition_map() override
  {
    return pg_partition_map_;
  };
  virtual transaction::ObTransService* get_trans_service();
  virtual const common::ObPartitionKey& get_partition_key() const;
  virtual int get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard);

  virtual int set_valid();
  virtual void set_invalid();
  virtual bool is_valid() const;
  virtual bool is_pg() const
  {
    return pkey_.is_pg();
  }

  virtual int table_scan(ObTableScanParam& param, common::ObNewRowIterator*& result);
  virtual int table_scan(ObTableScanParam& param, common::ObNewIterIterator*& result) override;
  virtual int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
      ObIPartitionGroup& right_partition, common::ObNewRowIterator*& result) override;
  virtual int delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) override;
  virtual int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows) override;
  virtual int insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) override;
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
      const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
      common::ObNewRowIterator*& duplicated_rows);
  // Used to check whether the specified row conflicts in the storage
  // @in_column_ids is used to specify the column of the row to be checked.
  // Rowkey must be first and local unique index must be included.
  // @out_column_ids is used to specify the column to be returned of the conflicting row.
  // @check_row_iter is the iterator of row to be checked.
  // @dup_row_iters is the iterator array of each row conflict. The number of iterators equals
  // to the number of rows to be checked
  virtual int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
      common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters);
  virtual int revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter);
  virtual int update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      const common::ObNewRow& old_row, const common::ObNewRow& new_row) override;
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      common::ObNewRowIterator* row_iter, ObLockFlag lock_flag, int64_t& affected_rows);
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const common::ObNewRow& row, ObLockFlag lock_flag);

  virtual int get_role(common::ObRole& role) const;
  virtual int get_role_for_partition_table(common::ObRole& role) const;
  virtual int get_role_unsafe(common::ObRole& role) const;
  virtual int get_leader_curr_member_list(common::ObMemberList& member_list) const;
  virtual int get_leader(common::ObAddr& leader) const;
  virtual int get_curr_member_list(common::ObMemberList& member_list) const;
  virtual int get_curr_member_list_for_report(common::ObMemberList& member_list) const;
  virtual int get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
      common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const;
  virtual int get_dst_leader_candidate(common::ObMemberList& member_list) const;
  virtual int get_log_archive_status(clog::ObPGLogArchiveStatus& status) const;
  virtual int change_leader(const common::ObAddr& leader);
  virtual int leader_takeover() override;
  virtual int leader_revoke();
  virtual int leader_active();
  virtual ObReplayStatus* get_replay_status();
  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size);

  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);

  virtual int report_clog_history_online() override;
  virtual int report_clog_history_offline() override;

  virtual int remove_election_from_mgr() override;
  virtual int pause() override;
  virtual int stop() override;
  virtual int offline() override;
  virtual int schema_drop(const bool for_replay, const uint64_t log_id, const bool is_physical_drop) override;

  // get create timestamp
  virtual int get_create_ts(int64_t& create_ts);

  // role change status
  virtual void replay_status_revoke();

  virtual int get_replica_state(ObPartitionReplicaState& state);

  virtual bool is_removed() const;
  virtual int check_is_in_member_list(bool& is_in_member_list) const;
  virtual int offline_itself(const bool is_physical_drop);
  virtual int submit_add_partition_to_pg_log(const obrpc::ObCreatePartitionArg& arg, ObPartitionService* ps,
      uint64_t& log_id, int64_t& log_timestamp, ObAddPartitionToPGLogCb*& out_cb);
  virtual int submit_remove_partition_from_pg_log(const ObPartitionKey& pkey);
  virtual int submit_partition_schema_change_log(const common::ObPartitionKey& pkey, const int64_t schema_version,
      const uint64_t index_id, ObPartitionService* ps, uint64_t& log_id, int64_t& log_ts,
      ObSchemaChangeClogCb*& out_cb);
  virtual int remove_partition_from_pg(
      const bool for_replay, const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id);

  virtual int replay_partition_meta_log(
      const ObStorageLogType log_type, const int64_t log_id, const char* buf, const int64_t size) override;
  virtual int set_wait_split();
  virtual int save_split_state(const bool write_slog);
  virtual int restore_split_state(const int state);
  virtual int restore_split_info(const ObPartitionSplitInfo& split_info);
  virtual int replay_split_source_log(
      const ObPartitionSplitSourceLog& log, const uint64_t log_id, const int64_t log_ts);
  virtual int replay_split_dest_log(const ObPartitionSplitDestLog& log);
  virtual int sync_split_source_log_success(const int64_t log_id, const int64_t log_ts);
  virtual int sync_split_dest_log_success();
  virtual int prepare_splitting(
      const ObPartitionSplitInfo& split_info, const common::ObMemberList& mlist, const common::ObAddr& leader);
  virtual int split_source_partition(const int64_t schema_version, const share::ObSplitPartitionPair& info,
      enum share::ObSplitProgress& partition_progress);
  virtual int split_dest_partition(const ObPartitionSplitInfo& split_info, enum share::ObSplitProgress& progress);
  virtual int push_reference_tables(
      const common::ObIArray<common::ObPartitionKey>& dest_array, const int64_t split_version);
  virtual int replay_split_state_slog(const ObSplitPartitionStateLogEntry& log_entry);
  virtual int replay_split_info_slog(const ObSplitPartitionInfoLogEntry& log_entry);
  virtual int set_dest_partition_split_progress(
      const int64_t schema_version, const common::ObPartitionKey& pkey, const int progress);
  virtual int get_all_table_ids(const ObPartitionKey& pkey, common::ObIArray<uint64_t>& index_tables);
  virtual int get_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);
  virtual int get_reference_memtables(ObTablesHandle& handle);
  virtual int set_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);
  virtual int set_split_version(const int64_t split_version);
  virtual int check_can_migrate(bool& can_migrate);
  virtual bool is_splitting() const;
  virtual bool is_split_source_partition() const;
  virtual bool is_in_dest_split() const;
  virtual bool is_dest_logical_split_finish() const;
  virtual int check_split_state() const;
  virtual int get_split_progress(const int64_t schema_version, int& progress);
  virtual int set_split_progress(const common::ObAddr& replica, const int progress);
  virtual int block_partition_split_by_mc();
  virtual int unblock_partition_split_by_mc();
  virtual int64_t get_freeze_snapshot_ts() const override;
  virtual ObPartitionState get_partition_state() const;
  virtual int switch_partition_state(const ObPartitionState state);

  virtual int try_switch_partition_state(const ObPartitionState state) override;
  virtual int get_latest_schema_version(share::schema::ObMultiVersionSchemaService* schema_service,
      const common::ObPartitionKey& pkey, int64_t& latest_schema_version);
  virtual int check_schema_version(share::schema::ObMultiVersionSchemaService* schema_service);
  virtual int set_base_schema_version(int64_t base_schema_version);
  virtual int do_warm_up_request(const ObIWarmUpRequest* request);
  virtual int check_can_do_merge(bool& can_merge, bool& need_merge);
  virtual int set_replica_type(const common::ObReplicaType& replica_type, const bool write_redo_log);
  virtual common::ObReplicaType get_replica_type() const;
  virtual common::ObReplicaProperty get_replica_property() const override;
  virtual int generate_weak_read_timestamp(const int64_t max_stale_time, int64_t& timestamp) override;
  virtual int do_partition_loop_work() override;
  virtual int get_weak_read_timestamp(int64_t& timestamp);
  virtual int update_last_checkpoint(const int64_t checkpoint);
  virtual int set_replay_checkpoint(const int64_t checkpoint);
  virtual int get_replay_checkpoint(int64_t& checkpoint);
  virtual int64_t get_cur_min_log_service_ts();
  virtual int64_t get_cur_min_trans_service_ts();
  virtual int64_t get_cur_min_replay_engine_ts();
  virtual void set_migrating_flag(const bool flag)
  {
    partition_loop_worker_.set_migrating_flag(flag);
  }
  virtual bool get_migrating_flag() const
  {
    return partition_loop_worker_.get_migrating_flag();
  }

  virtual int need_minor_freeze(const uint64_t& log_id, bool& need_freeze);
  virtual int set_emergency_release();

  virtual int freeze(const bool emergency, const bool force, int64_t& freeze_snapshot) override;
  // Mark dirty of transactions on the terminated memtable
  virtual int mark_dirty_trans(bool& cleared) override;

  virtual int get_curr_storage_info_for_migrate(const bool use_slave_safe_read_ts,
      const common::ObReplicaType replica_type, const int64_t src_cluster_id, ObSavedStorageInfoV2& info) override;

  virtual int check_is_from_restore(bool& is_from_restore) const;
  virtual int get_all_saved_info(ObSavedStorageInfoV2& info) const override;
  virtual int get_saved_clog_info(common::ObBaseStorageInfo& clog_info) const override;
  virtual int get_saved_data_info(ObDataStorageInfo& data_info) const override;
  virtual int get_saved_last_log_info(uint64_t& log_id, int64_t& submit_timestamp) const override;

  virtual int append_local_sort_data(const common::ObPartitionKey& pkey,
      const share::ObBuildIndexAppendLocalDataParam& param, common::ObNewRowIterator& iter) override;
  virtual int append_sstable(const common::ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param,
      common::ObNewRowIterator& iter) override;
  virtual const ObPartitionSplitInfo& get_split_info()
  {
    return split_info_;
  }
  virtual int check_cur_partition_split(bool& is_split_partition);
  virtual int get_trans_split_info(transaction::ObTransSplitInfo& split_info);
  virtual int check_single_replica_major_sstable_exist(
      const ObPartitionKey& pkey, const uint64_t index_table_id) override;
  virtual int get_max_decided_trans_version(int64_t& max_decided_trans_version) const;

  int create_memtable(const bool in_slog_trans = false, const bool is_replay = false,
      const bool ignore_memstore_percent = false) override;
  virtual int get_table_stat(const common::ObPartitionKey& pkey, common::ObTableStat& stat);
  // The following two interface are used by ObGarbageCollector
  virtual int allow_gc(bool& allow_gc);
  virtual int gc_check_valid_member(const bool is_valid, const int64_t gc_seq, bool& need_gc);
  virtual bool check_pg_partition_offline(const ObPartitionKey& pkey);
  virtual int check_offline_log_archived(
      const ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round, bool& has_archived) const;
  virtual int get_leader_epoch(int64_t& leader_epoch) const;
  virtual int get_replica_status(share::ObReplicaStatus& status) override;
  virtual int is_replica_need_gc(bool& is_offline) override;
  virtual int set_storage_info(const ObSavedStorageInfoV2& info) override;
  virtual int fill_pg_partition_replica(
      const ObPartitionKey& pkey, share::ObReplicaStatus& replica_status, ObReportStatus& report_status) override;
  virtual int fill_replica(share::ObPartitionReplica& replica) override;
  virtual int get_merge_priority_info(memtable::ObMergePriorityInfo& merge_priority_info) const override;
  virtual int64_t get_gc_schema_drop_ts() override;
  virtual void set_need_rebuild()
  {
    migrate_retry_flag_ = NEED_REBUILD;
  }
  virtual bool is_need_rebuild() const
  {
    return (NEED_REBUILD == migrate_retry_flag_);
  }
  virtual void set_need_standby_restore()
  {
    migrate_retry_flag_ = NEED_STANDBY_RESTORE;
  }
  virtual bool is_need_standby_restore() const
  {
    return (NEED_STANDBY_RESTORE == migrate_retry_flag_);
  }
  virtual void reset_migrate_retry_flag()
  {
    migrate_retry_flag_ = NO_NEED_RETRY;
  }
  virtual void set_need_gc()
  {
    ATOMIC_STORE(&need_gc_, true);
  }
  virtual bool is_need_gc() const
  {
    return ATOMIC_LOAD(&need_gc_);
  }
  virtual uint64_t get_offline_log_id() const
  {
    return ATOMIC_LOAD(&offline_log_id_);
  }
  virtual int set_offline_log_id(const uint64_t log_id);
  virtual int retire_warmup_store(const bool is_disk_full) override;
  int has_active_memtable(bool& found);
  virtual int enable_write_log(const bool is_replay_old) override;
  virtual uint64_t get_min_replayed_log_id() override;
  virtual void get_min_replayed_log(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts) override;
  virtual int get_min_replayed_log_with_keepalive(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts) override;
  virtual int check_dirty_txn(
      const int64_t min_log_ts, const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty) override;
  // Create Partition Group
  int create_partition_group(const ObCreatePGParam& param);
  int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle) override;
  int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay,
      const uint64_t log_id, ObTablesHandle& sstables_handle) override;
  int replay_pg_partition(const common::ObPartitionKey& pkey, const uint64_t log_id);
  int get_all_pg_partition_keys(ObPartitionArray& pkeys, const bool include_trans_table = false);
  int check_release_memtable();
  virtual int add_sstable_for_merge(const ObPartitionKey& pkey, storage::ObSSTable* sstable,
      const int64_t max_kept_major_version_number, ObIPartitionReport& report,
      ObSSTable* complement_minor_sstable = nullptr);
  virtual int check_replica_ready_for_bounded_staleness_read(const int64_t snapshot_version);
  virtual bool is_replica_using_remote_memstore() const
  {
    return pg_storage_.is_replica_with_remote_memstore() && F_WORKING == get_partition_state();
  }
  virtual bool is_share_major_in_zone() const
  {
    return pg_storage_.is_share_major_in_zone();
  }
  int feedback_scan_access_stat(const ObTableScanParam& param) override;
  virtual blocksstable::ObStorageFile* get_storage_file() override
  {
    return pg_storage_.get_storage_file();
  }
  virtual const blocksstable::ObStorageFile* get_storage_file() const override
  {
    return pg_storage_.get_storage_file();
  }
  virtual blocksstable::ObStorageFileHandle& get_storage_file_handle() override
  {
    return pg_storage_.get_storage_file_handle();
  }
  virtual int set_storage_file(blocksstable::ObStorageFileHandle& file_handle) override;
  virtual int create_sstables(const common::ObIArray<ObPGCreateSSTableParam>& create_sstable_params,
      ObTablesHandle& tables_handle, const bool in_slog_trans = false) override;
  virtual int create_sstable(
      const ObPGCreateSSTableParam& param, ObTableHandle& table_handle, const bool in_slog_trans = false) override;
  virtual int get_checkpoint_info(common::ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info) override;
  virtual int acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle) override;
  virtual int recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt) override;
  virtual int check_can_free(bool& can_free) override;

  virtual bool need_replay_redo() const;
  virtual int try_clear_split_info();
  virtual int check_complete(bool& is_complete);
  virtual int try_update_clog_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp,
      const int64_t replica_num, const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id);
  virtual int check_physical_flashback_succ(const obrpc::ObCheckPhysicalFlashbackArg& arg, const int64_t max_version,
      obrpc::ObPhysicalFlashbackResultArg& result) override;
  // Check if weak read is enabled.
  virtual bool can_weak_read() const override
  {
    return is_weak_readable_replica(partition_state_, get_replica_type());
  }
  ObPartitionLoopWorker* get_partition_loop_worker()
  {
    return &partition_loop_worker_;
  }
  virtual int save_split_info(const ObPartitionSplitInfo& split_info);
  virtual int save_split_state(const int64_t split_state);
  virtual int shutdown(const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version);
  virtual int physical_flashback(const int64_t flashback_scn);
  virtual int set_meta_block_list(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) override;
  virtual int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) const override;
  virtual int get_all_tables(ObTablesHandle& tables_handle) override;
  virtual int get_merge_log_ts(int64_t& freeze_ts) override;
  virtual int get_table_store_cnt(int64_t& table_cnt) const override;

  int check_can_physical_flashback(const int64_t flashback_scn);

  virtual int clear_trans_after_restore_log(const uint64_t last_restore_log_id);
  virtual int reset_for_replay();

  virtual int inc_pending_batch_commit_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);
  virtual int inc_pending_elr_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);

  virtual int register_txs_change_leader(const common::ObAddr& server, ObTsWindows& changing_leader_windows);
  virtual int check_physical_split(bool& finished);
  TO_STRING_KV(K_(pkey), K_(replay_status), K_(partition_state));

private:
  // this structure is introduced by major freeze refactoring
  // to break the circular dependency of waiting schema in the process
  // of leader takeover.
  // It will wait for schema forever in the process of leader takeover
  // before. We will just record schema version after introduce this
  // structure and delay the operation of comparing schema version to
  // the process of partition_service's start_participants.
  // In addition, schema version check is only limit to user tables, and
  // system or dummy tables will be skipped directly.
  class SchemaVersionContainer {
  public:
    SchemaVersionContainer() : schema_version_checked_(true), base_schema_version_(0)
    {}
    virtual ~SchemaVersionContainer()
    {}

  public:
    // This function is used to update schema_version. It will be called in the process
    // of leader_takeover
    int update_and_leader_takeover(int64_t base_schema_version);
    int check_base_schema_version(
        share::schema::ObMultiVersionSchemaService* schema_version, common::ObPartitionKey& pkey);

  private:
    static const int64_t INVALID_SCHEMA_RETRY_CNT = 10;

  private:
    // Used to identify whether the schema_version check has been completed.
    // It is set to false after leader takeover and set to true at the first
    // start_participants.
    // The follower will not check it and always true.
    bool schema_version_checked_;
    // Set with the value from memtable or base_storage_info in the process of
    // leader takeover.
    int64_t base_schema_version_;
  };

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroup);
  static const int64_t FREEZE_WAIT_TIME = 10L * 1000L;  // 10ms
  static const int64_t FREEZE_RETRY_CNT_LIMIT = 100L;
  static const int64_t FREEZE_CUT_RETRY_CNT_LIMIT = 10L;
  static const int64_t SPLIT_FREEZE_WAIT_TS = 1000L;
  static const int64_t FREEZE_WAIT_RETRY_SLEEP_TS = 1000L;  // 1ms
  static const int64_t FREEZE_WAIT_RETRY_SLEEP_CNT = 100L;
  static const int64_t MAX_FREEZE_WAIT_RETRY_SLEEP_CNT = 5000L;
  static const int64_t OB_SET_REPLAY_ENGINE_PENDING_TIMEOUT = 100L * 1000L;  // 100ms
  static const int64_t PG_QUERY_LOG_INFO_TIMEOUT = 5L * 1000L * 1000L;       // 5s
  static const int64_t WAIT_FREEZE_LOG_ELAPSE_CNT_LIMIT = 20L;               // 20
  static const int64_t WAIT_FREEZE_LOG_ELAPSE_SLEEP_TS = 50L * 1000L;        // 50ms

private:
  int check_init_(void* cp, const char* cp_name) const;
  // for clog history info
  int get_clog_service_range_for_clog_history_info_(
      uint64_t& start_log_id, int64_t& start_log_timestamp, uint64_t& end_log_id, int64_t& end_log_timestamp);
  int decide_split_version_(const int64_t base_ts, int64_t& split_version);
  int is_split_version_safe_(const int64_t split_version, bool& safe);
  int submit_split_source_log_(const ObPartitionSplitSourceLog& log, const int64_t base_ts);
  int submit_split_dest_log_(const ObPartitionSplitDestLog& log);
  int set_split_version_(const int64_t split_version);
  int push_split_task_(const int64_t schema_version, const share::ObSplitPartitionPair& info);
  int save_split_state_(const int64_t state, const bool write_slog);
  int save_split_info_(const ObPartitionSplitInfo& split_info, const bool write_slog);
  int split_dest_partitions_(bool& is_all_finished);
  int query_replica_split_progress_(const int64_t schema_version);
  int get_split_partition_member_list_(
      const common::ObIArray<common::ObPartitionKey>& pkey_array, common::ObMemberList& member_list);
  int get_all_table_ids_(const ObPartitionKey& pkey, common::ObIArray<uint64_t>& index_tables);
  int get_reference_tables_(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);
  int set_reference_tables_(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);
  int push_reference_tables_(const common::ObIArray<common::ObPartitionKey>& dest_array, const int64_t split_version);
  int check_if_dest_pg_ready_(const common::ObIArray<common::ObPartitionKey>& dest_pgs, bool& is_ready);
  int raise_memstore_if_needed_(const int64_t trans_version);
  int has_active_memtable_(bool& found);
  int check_split_state_() const;

  // freeze
  int freeze_log_(const bool force);
  int freeze_log_and_data_v2_(const bool emergency, const bool force, int64_t& snapshot_version);
  int cut_and_submit_freeze_(const bool is_leader, memtable::ObMemtable& memtable, int64_t& snapshot_version,
      uint64_t& freeze_id, int64_t& freeze_ts);
  int prepare_storage_info_(
      const int64_t snapshot_version, const uint64_t freeze_id, const int64_t freeze_ts, ObSavedStorageInfoV2& info);
  int check_range_changed_(ObTableHandle& handle, const bool is_leader, bool& changed);
  int wait_follower_no_pending_task_();
  int submit_freeze_and_effect_memstore_(const bool is_leader, const bool emergency,
      memtable::ObMemtable& frozen_memtable, bool& effected, int64_t& snapshot_version);
  int submit_freeze_and_effect_memstore_inner_(const bool is_leader, const bool emergency,
      memtable::ObMemtable& frozen_memtable, bool& effected, int64_t& snapshot_version);
  int get_clog_info_for_freeze_log_(ObSavedStorageInfoV2& info);
  bool is_splitting_() const;
  bool is_dest_splitting_() const;
  bool is_split_source_partition_() const;
  bool is_split_dest_partition_() const;
  bool is_physical_split_finished_() const;
  int check_physical_split_(bool& finished) const;
  int get_dest_split_progress_(int& progress);

  int wait_replay_();
  int clear_non_reused_stores_(const ObPartitionKey& pkey);
  bool with_data_();

  int get_curr_data_info_(
      const bool use_slave_safe_read_ts, const ObDataStorageInfo& saved_data_info, ObDataStorageInfo& data_info);
  int get_curr_clog_info_(const int64_t src_cluster_id, ObBaseStorageInfo& clog_info, bool& log_info_usable);
  int get_logical_read_data_info(ObDataStorageInfo& data_info);
  int set_max_passed_trans_version_(const int64_t trans_version);
  int get_saved_last_log_info_(uint64_t& log_id, int64_t& submit_timestamp) const;
  int shutdown_(const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version);
  /*
   * The constraints of freeze point:
   * 1. There should be no submit log that has been replayed or applied after this point(dependence of migration).
   * 2. The trans_node of the log before this point must be on the previous memtable(constraint of memtable dump).
   *   2.1 This log_id must be smaller than the point of the largest majority log(It is possible that the largest
   *   majority log be covered after the leader switch).
   * 3. There should be no trans node missing because of replaying out of order in the middle of the same line of
   * the follower(constraint of read sstable).
   */
  int get_freeze_cut_(memtable::ObMemtable& memtable, const bool is_leader, int64_t& snapshot_version,
      uint64_t& last_replay_log_id, int64_t& last_replay_log_ts);
  /*
   * 1. For the leader, get the max majority log's log id and log ts.
   * 2. For the follower, get the max replayed log's log id and log ts.
   */
  int get_freeze_log_(const bool is_leader, uint64_t& log_id, int64_t& log_ts);
  int get_freeze_snapshot_version_(const int64_t freeze_ts, int64_t& snapshot_version);
  int get_and_submit_freeze_(memtable::ObMemtable& memtable, bool is_leader, uint64_t& freeze_id, int64_t& freeze_ts);
  int wait_freeze_log_elapse_(const int64_t in_freeze_ts, int64_t& out_freeze_ts);
  int update_max_passed_trans_version_(const int64_t trans_version, const bool need_raise_memstore = true);
  int get_leader_curr_member_list_(ObMemberList& member_list) const;
  int create_memtable_(const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent);

  int pause_();
  int stop_();
  int offline_();
  void erase_pg_from_clog();
  void erase_pg_from_election();
  void erase_pg_from_replay_engine();
  void erase_pg_from_trans();
  void erase_pg_from_stat_cache();
  int set_replica_type_(const ObReplicaType& replica_type, const bool write_redo_log);
  int check_private_table_flashback_result_(const int64_t max_version, obrpc::ObPhysicalFlashbackResultArg& result);
  int check_non_private_table_flashback_result_(const int64_t flashback_scn, bool& result);
  int create_sstable_impl(const common::ObIArray<blocksstable::ObMacroBlocksWriteCtx*>& data_blocks,
      const common::ObIArray<blocksstable::ObMacroBlocksWriteCtx*>& lob_blocks,
      blocksstable::ObMacroBlocksWriteCtx* bloom_filter_block, const ObSSTableMergeInfo* sstable_merge_info,
      ObSSTable& sstable);
  int get_base_storage_info_(common::ObBaseStorageInfo& base_storage_info);

protected:
  bool is_inited_;
  common::ObPartitionKey pkey_;

  storage::SafeRef2 safe_ref_;  // The indirect reference of this partition.
  ObReplayStatus* replay_status_;
  ObIPartitionComponentFactory* cp_fty_;
  clog::ObIPartitionLogService* pls_;
  transaction::ObTransService* txs_;
  replayengine::ObILogReplayEngine* rp_eg_;
  ObPartitionService* ps_;
  ObPartitionGroupIndex* pg_index_;
  ObPGPartitionMap* pg_partition_map_;
  mutable ObPGStorage pg_storage_;
  // ATTENTION: use before reading the ObPartitionGroupLock guide
  // in ob_partition_group_lock.h
  ObPartitionGroupLock lock_;
  lib::ObMutex partition_state_lock_;
  ObPartitionState partition_state_;
  SchemaVersionContainer schema_version_container_;

  share::schema::ObMultiVersionSchemaService* schema_service_;

  ObPartitionSplitState split_state_;
  ObPartitionSplitInfo split_info_;
  transaction::MonotonicTs split_trans_clear_ts_;
  common::SpinRWLock split_lock_;
  // The target partition's leader maintains the split state of all replicas of the target partition
  ObReplicaSplitProgressArray replica_split_progress_array_;
  ObMemberList saved_member_list_;
  // The split state of all target partitions is maintained on the source partition's leader
  ObPartitionSplitProgressArray partition_split_progress_array_;
  bool is_split_blocked_by_mc_;

  int64_t max_passed_trans_version_ CACHE_ALIGNED;
  ObFreezeRecord freeze_record_ CACHE_ALIGNED;
  common::TCRWLock trans_version_lock_;

  // The leader will record the timestamp when all transactions on the dropped partition are finished.
  // It is used to solve the problem that one stage optimization cannot query historical log if the partition
  // is deleted.
  //
  //
  // this submission will be rolled back after the above plan is finished.
  int64_t gc_schema_drop_ts_;
  // The replica will record the gc_seq if it finds that it is not in the member list.
  // The replica will be GC if it is not in the member list two consecutive turns.
  int64_t gc_seq_check_valid_member_;
  // The log id of offline log, default is OB_INVALID_ID, used for gc validation.
  uint64_t offline_log_id_;
  uint32_t migrate_retry_flag_;  // PG migration task retry flag
  bool need_gc_;
  ObPartitionLoopWorker partition_loop_worker_;

  int64_t restore_task_cnt_;
  int64_t restore_task_ts_;
  bool has_clear_trans_after_restore_;

  common::SpinRWLock freeze_lock_;
  lib::ObMutex migrate_mutex_;  // todo@wenqu: remove after apply ofs file_lock
  ObAddr migrating_dst_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_GROUP
